package tlsutil

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func writeTempPEM(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func caPEM(ca *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw})
}

func mustCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, dns []string, ips []net.IP, usages []x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	t.Helper()
	certPEM, keyPEM, err := GenerateTestCert(ca, caKey, cn, "", dns, ips, usages)
	if err != nil {
		t.Fatalf("GenerateTestCert(%s): %v", cn, err)
	}
	return certPEM, keyPEM
}

// TestGenerateTestCerts verifies the in-memory PKI: a leaf signed by the CA
// verifies against the CA's pool, and a different CA rejects it.
func TestGenerateTestCerts(t *testing.T) {
	ca, caKey, pool, err := GenerateTestCA()
	if err != nil {
		t.Fatalf("GenerateTestCA: %v", err)
	}
	certPEM, _, err := GenerateTestCert(ca, caKey, "svc", "", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	if err != nil {
		t.Fatalf("GenerateTestCert: %v", err)
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("no PEM block")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := cert.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
		t.Fatalf("leaf does not verify against the CA: %v", err)
	}

	_, _, pool2, _ := GenerateTestCA()
	if _, err := cert.Verify(x509.VerifyOptions{Roots: pool2}); err == nil {
		t.Fatal("leaf verified against a different CA")
	}
}

// TestLoadCredentialsHandshake performs a real mTLS handshake over a pipe: the
// server requires and verifies the client cert; both sides present certs
// signed by the same CA.
func TestLoadCredentialsHandshake(t *testing.T) {
	ca, caKey, _, err := GenerateTestCA()
	if err != nil {
		t.Fatalf("GenerateTestCA: %v", err)
	}
	caFile := writeTempPEM(t, "ca.pem", caPEM(ca))
	serverCert, serverKey := mustCert(t, ca, caKey, "ppp-server", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCert, clientKey := mustCert(t, ca, caKey, "ppp-client", nil, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	serverCreds, err := LoadServerCredentials(caFile, writeTempPEM(t, "server.pem", serverCert), writeTempPEM(t, "server.key", serverKey))
	if err != nil {
		t.Fatalf("LoadServerCredentials: %v", err)
	}
	clientCreds, err := LoadClientCredentials(caFile, writeTempPEM(t, "client.pem", clientCert), writeTempPEM(t, "client.key", clientKey), "localhost")
	if err != nil {
		t.Fatalf("LoadClientCredentials: %v", err)
	}

	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()
	done := make(chan error, 1)
	go func() {
		_, _, err := serverCreds.ServerHandshake(serverSide)
		done <- err
	}()
	if _, _, err := clientCreds.ClientHandshake(context.Background(), "127.0.0.1", clientSide); err != nil {
		t.Fatalf("client handshake: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("server handshake: %v", err)
	}
}

// handshakePair runs a server and a client handshake over a real TCP
// connection (net.Pipe is synchronous and can deadlock a rejected handshake).
func handshakePair(t *testing.T, serverCreds, clientCreds credentials.TransportCredentials) (serverErr, clientErr error) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	serverDone := make(chan error, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		_, _, err = serverCreds.ServerHandshake(conn)
		serverDone <- err
	}()
	conn, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	// Pass the DIALED host as the authority, exactly like gRPC does; the
	// client creds' tls.Config.ServerName (when non-empty) overrides it.
	dialHost := "127.0.0.1"
	_, _, clientErr = clientCreds.ClientHandshake(context.Background(), dialHost, conn)
	serverErr = <-serverDone
	return serverErr, clientErr
}

// TestLoadCredentialsRejectsCertlessClient verifies the server-side mTLS
// rejects a client that does not present a certificate.
func TestLoadCredentialsRejectsCertlessClient(t *testing.T) {
	ca, caKey, _, err := GenerateTestCA()
	if err != nil {
		t.Fatalf("GenerateTestCA: %v", err)
	}
	caFile := writeTempPEM(t, "ca.pem", caPEM(ca))
	serverCert, serverKey := mustCert(t, ca, caKey, "ppp-server", []string{"localhost"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})

	serverCreds, err := LoadServerCredentials(caFile, writeTempPEM(t, "server.pem", serverCert), writeTempPEM(t, "server.key", serverKey))
	if err != nil {
		t.Fatalf("LoadServerCredentials: %v", err)
	}

	// A client that trusts the CA but presents no client certificate.
	certless := credentials.NewTLS(&tls.Config{
		RootCAs:    caPoolFromCert(ca),
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})
	serverErr, _ := handshakePair(t, serverCreds, certless)
	if serverErr == nil {
		t.Fatal("server accepted a certless client")
	}
}

// TestLoadClientCredentialsEmptyServerName locks the empty-serverName
// behavior: it does NOT disable hostname verification — gRPC verifies the
// DIALED address's hostname (so IP dialing requires the server certificate to
// contain the dialed IP as a SAN; a DNS-only cert is rejected).
func TestLoadClientCredentialsEmptyServerName(t *testing.T) {
	ca, caKey, _, err := GenerateTestCA()
	if err != nil {
		t.Fatalf("GenerateTestCA: %v", err)
	}
	caFile := writeTempPEM(t, "ca.pem", caPEM(ca))
	clientCert, clientKey := mustCert(t, ca, caKey, "ppp-client", nil, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	clientCreds, err := LoadClientCredentials(caFile, writeTempPEM(t, "client.pem", clientCert), writeTempPEM(t, "client.key", clientKey), "")
	if err != nil {
		t.Fatalf("LoadClientCredentials: %v", err)
	}

	// Case A: a DNS-only server cert (no IP SAN) dialed by 127.0.0.1 with an
	// empty serverName -> the dialed host is verified and NOT covered -> the
	// client rejects.
	dnsOnlyCert, dnsOnlyKey := mustCert(t, ca, caKey, "ppp-server", []string{"localhost"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	dnsOnlyCreds, err := LoadServerCredentials(caFile, writeTempPEM(t, "dns.pem", dnsOnlyCert), writeTempPEM(t, "dns.key", dnsOnlyKey))
	if err != nil {
		t.Fatalf("LoadServerCredentials(dns): %v", err)
	}
	_, clientErr := handshakePair(t, dnsOnlyCreds, clientCreds)
	if clientErr == nil {
		t.Fatal("client accepted a DNS-only server cert for an IP dial with empty serverName")
	}

	// Case B: a server cert that includes the dialed IP SAN is accepted with
	// an empty serverName.
	ipCert, ipKey := mustCert(t, ca, caKey, "ppp-server", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	ipCreds, err := LoadServerCredentials(caFile, writeTempPEM(t, "ip.pem", ipCert), writeTempPEM(t, "ip.key", ipKey))
	if err != nil {
		t.Fatalf("LoadServerCredentials(ip): %v", err)
	}
	serverErr, clientErr := handshakePair(t, ipCreds, clientCreds)
	if clientErr != nil || serverErr != nil {
		t.Fatalf("IP-SAN handshake with empty serverName failed: client=%v server=%v", clientErr, serverErr)
	}
}

// TestLoadCredentialsEmptyIsPlaintext verifies all-empty flags yield nil creds
// (the plaintext path is preserved).
func TestLoadCredentialsEmptyIsPlaintext(t *testing.T) {
	sc, err := LoadServerCredentials("", "", "")
	if err != nil || sc != nil {
		t.Fatalf("LoadServerCredentials(empty) = %v, %v; want nil, nil", sc, err)
	}
	cc, err := LoadClientCredentials("", "", "", "")
	if err != nil || cc != nil {
		t.Fatalf("LoadClientCredentials(empty) = %v, %v; want nil, nil", cc, err)
	}
}

func caPoolFromCert(ca *x509.Certificate) *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return pool
}

// TestCertRole verifies role extraction from the certificate Subject OU.
func TestCertRole(t *testing.T) {
	ca, caKey, _, err := GenerateTestCA()
	if err != nil {
		t.Fatalf("GenerateTestCA: %v", err)
	}
	certPEM, _, err := GenerateTestCert(ca, caKey, "node-a", RoleService, []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	if err != nil {
		t.Fatalf("GenerateTestCert(service): %v", err)
	}
	noOU, _, err := GenerateTestCert(ca, caKey, "node-b", "", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	if err != nil {
		t.Fatalf("GenerateTestCert(no OU): %v", err)
	}
	parse := func(der []byte) *x509.Certificate {
		t.Helper()
		block, _ := pem.Decode(der)
		if block == nil {
			t.Fatal("no PEM block")
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("ParseCertificate: %v", err)
		}
		return c
	}
	if got := CertRole(parse(certPEM)); got != RoleService {
		t.Fatalf("CertRole(service) = %q, want %q", got, RoleService)
	}
	if got := CertRole(parse(noOU)); got != "" {
		t.Fatalf("CertRole(no OU) = %q, want empty", got)
	}
	if got := CertRole(nil); got != "" {
		t.Fatalf("CertRole(nil) = %q, want empty", got)
	}
}

// TestRoleAuthInterceptors verifies the unary + stream role interceptors:
// allowed roles pass, disallowed roles get PermissionDenied, and plaintext
// (no TLS peer info) passes through.
func TestRoleAuthInterceptors(t *testing.T) {
	okUnary := func(ctx context.Context) error {
		_, err := RoleAuthUnaryInterceptor(RoleService, RoleClient)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/p.F/M"}, func(context.Context, interface{}) (interface{}, error) {
			return "ok", nil
		})
		return err
	}
	// Plaintext (no peer in the ctx) passes through.
	if err := okUnary(context.Background()); err != nil {
		t.Fatalf("plaintext unary = %v, want pass-through", err)
	}
	// A peer with a service-role cert passes.
	svcCtx := peerCtxWithRole(t, RoleService)
	if err := okUnary(svcCtx); err != nil {
		t.Fatalf("service unary = %v, want ok", err)
	}
	// A disallowed role is PermissionDenied (code 7).
	badCtx := peerCtxWithRole(t, "admin")
	err := okUnary(badCtx)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("admin unary code = %v, want PermissionDenied", status.Code(err))
	}
	// Streaming variant.
	streamCtx := peerCtxWithRole(t, RoleService)
	streamErr := RoleAuthStreamInterceptor(RoleService)(nil, &mockSS{ctx: streamCtx}, &grpc.StreamServerInfo{FullMethod: "/p.F/S"}, func(interface{}, grpc.ServerStream) error { return nil })
	if streamErr != nil {
		t.Fatalf("service stream = %v, want ok", streamErr)
	}
	badStream := RoleAuthStreamInterceptor(RoleService)(nil, &mockSS{ctx: peerCtxWithRole(t, "admin")}, &grpc.StreamServerInfo{FullMethod: "/p.F/S"}, func(interface{}, grpc.ServerStream) error { return nil })
	if status.Code(badStream) != codes.PermissionDenied {
		t.Fatalf("admin stream code = %v, want PermissionDenied", status.Code(badStream))
	}
}

// peerCtxWithRole builds a context carrying an mTLS peer whose certificate
// has the given OU role.
func peerCtxWithRole(t *testing.T, ou string) context.Context {
	t.Helper()
	ca, caKey, _, err := GenerateTestCA()
	if err != nil {
		t.Fatalf("GenerateTestCA: %v", err)
	}
	certPEM, _, err := GenerateTestCert(ca, caKey, "peer", ou, []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	if err != nil {
		t.Fatalf("GenerateTestCert: %v", err)
	}
	block, _ := pem.Decode(certPEM)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}},
		},
	})
}

// mockSS is a minimal ServerStream for the streaming interceptor test.
type mockSS struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockSS) Context() context.Context { return m.ctx }

// TestWarnIfRoleWithoutTLS verifies the misconfiguration warning: it fires
// only when -tls-require-role is set and the server has no TLS credentials
// (plaintext) — a deployment must not believe the gate is active when there
// is no peer identity to check.
func TestWarnIfRoleWithoutTLS(t *testing.T) {
	var warned []string
	capture := func(format string, args ...any) {
		warned = append(warned, fmt.Sprintf(format, args...))
	}
	if !WarnIfRoleWithoutTLS(capture, "service,client", false) {
		t.Fatal("expected a warning for a role flag without TLS creds")
	}
	if len(warned) != 1 || !strings.Contains(warned[0], "role authorization is NOT enforced") {
		t.Fatalf("warning text = %v", warned)
	}
	// No warning when TLS is enabled or the flag is empty.
	if WarnIfRoleWithoutTLS(capture, "service,client", true) {
		t.Fatal("unexpected warning with TLS enabled")
	}
	if WarnIfRoleWithoutTLS(capture, "", false) {
		t.Fatal("unexpected warning with no role flag")
	}
	if len(warned) != 1 {
		t.Fatalf("warned %d times, want 1", len(warned))
	}
}
