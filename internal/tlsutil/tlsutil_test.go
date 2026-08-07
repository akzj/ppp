package tlsutil

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/credentials"
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
	certPEM, keyPEM, err := GenerateTestCert(ca, caKey, cn, dns, ips, usages)
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
	certPEM, _, err := GenerateTestCert(ca, caKey, "svc", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
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
	if _, _, err := clientCreds.ClientHandshake(context.Background(), "localhost", clientSide); err != nil {
		t.Fatalf("client handshake: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("server handshake: %v", err)
	}
}

// handshakePair runs a server and a client handshake over a real TCP
// connection (net.Pipe is synchronous and can deadlock a rejected handshake).
func handshakePair(t *testing.T, serverCreds, clientCreds credentials.TransportCredentials, serverName string) (serverErr, clientErr error) {
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
	_, _, clientErr = clientCreds.ClientHandshake(context.Background(), serverName, conn)
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
	serverErr, _ := handshakePair(t, serverCreds, certless, "localhost")
	if serverErr == nil {
		t.Fatal("server accepted a certless client")
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
