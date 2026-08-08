// Package tlsutil builds gRPC TLS credentials for the ppp control and data
// planes. Both sides are configured from flags; when all TLS flags are empty
// the credentials are nil and callers keep the plaintext path (development
// compatibility). This phase does CA mutual verification (identity-based
// authorization is a later concern).
package tlsutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// LoadServerCredentials builds server-side mTLS credentials that require and
// verify a client certificate signed by the given CA. When caFile, certFile
// and keyFile are all empty it returns (nil, nil) so the caller keeps the
// plaintext path.
func LoadServerCredentials(caFile, certFile, keyFile string) (credentials.TransportCredentials, error) {
	if caFile == "" && certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, errors.New("tlsutil: -tls-cert and -tls-key are required with -tls-ca")
	}
	pool, err := loadCertPool(caFile)
	if err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("tlsutil: load server key pair: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}), nil
}

// LoadClientCredentials builds client-side mTLS credentials: it trusts the
// given CA, presents its own certificate, and verifies the server name. When
// caFile, certFile and keyFile are all empty it returns (nil, nil) for
// plaintext.
func LoadClientCredentials(caFile, certFile, keyFile, serverName string) (credentials.TransportCredentials, error) {
	if caFile == "" && certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, errors.New("tlsutil: -tls-cert and -tls-key are required with -tls-ca")
	}
	pool, err := loadCertPool(caFile)
	if err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("tlsutil: load client key pair: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS12,
	}), nil
}

// Role values carried by the certificate Subject OU field (identity
// authorization, deployment.md). An empty role means the certificate has no
// OU and therefore no declared identity role.
const (
	RoleCtl     = "ctl"     // control-plane operator/leader certificate
	RoleService = "service" // ppp agent/peer certificate
	RoleClient  = "client"  // orchestrator/leaf SDK certificate
)

// CertRole returns the role carried by the certificate's Subject OU (the
// first OU value, per the deployment convention). A certificate with no OU
// yields an empty role.
func CertRole(cert *x509.Certificate) string {
	if cert == nil || len(cert.Subject.OrganizationalUnit) == 0 {
		return ""
	}
	return cert.Subject.OrganizationalUnit[0]
}

// PeerRole extracts the peer certificate's role from a gRPC peer (an mTLS
// call). ok is false when the peer has no TLS auth info (plaintext), so a
// role check can skip — plaintext is the development path.
func PeerRole(p *peer.Peer) (role string, ok bool) {
	if p == nil || p.AuthInfo == nil {
		return "", false
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", false
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return "", true // TLS but no peer cert (should not happen with mTLS)
	}
	return CertRole(tlsInfo.State.PeerCertificates[0]), true
}

// roleAuth extracts the role from the caller context. Plaintext (no TLS)
// yields (role="", skip=true) — the interceptor passes the call through,
// because plaintext is the development mode and has no identity to check.
func roleAuth(ctx context.Context) (role string, skip bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", true
	}
	role, ok = PeerRole(p)
	if !ok {
		return "", true // no TLS auth info (plaintext)
	}
	return role, false
}

// RoleAuthUnaryInterceptor rejects unary calls whose peer certificate role is
// not in allowed with PermissionDenied (code 7; the message includes the
// caller's role). Calls without TLS (plaintext) are passed through unchanged.
func RoleAuthUnaryInterceptor(allowed ...string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		role, skip := roleAuth(ctx)
		if skip {
			return handler(ctx, req)
		}
		if !roleAllowed(role, allowed) {
			return nil, status.Errorf(codes.PermissionDenied, "tlsutil: role %q is not allowed for %s", role, info.FullMethod)
		}
		return handler(ctx, req)
	}
}

// RoleAuthStreamInterceptor is the streaming variant of
// RoleAuthUnaryInterceptor.
func RoleAuthStreamInterceptor(allowed ...string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		role, skip := roleAuth(ss.Context())
		if skip {
			return handler(srv, ss)
		}
		if !roleAllowed(role, allowed) {
			return status.Errorf(codes.PermissionDenied, "tlsutil: role %q is not allowed for %s", role, info.FullMethod)
		}
		return handler(srv, ss)
	}
}

// RoleAuthServerOptions returns gRPC server options that enforce the
// comma-separated allowed roles (e.g. "service,client"). An empty requireRoles
// yields no options — mTLS keeps working as before with no role check
// (compatibility with certificates that carry no OU). Plaintext calls pass
// through either way (the interceptors skip when there is no TLS peer info).
func RoleAuthServerOptions(requireRoles string) []grpc.ServerOption {
	roles := splitRoles(requireRoles)
	if len(roles) == 0 {
		return nil
	}
	return []grpc.ServerOption{
		grpc.UnaryInterceptor(RoleAuthUnaryInterceptor(roles...)),
		grpc.StreamInterceptor(RoleAuthStreamInterceptor(roles...)),
	}
}

func splitRoles(s string) []string {
	var out []string
	for _, r := range strings.Split(s, ",") {
		r = strings.TrimSpace(r)
		if r != "" {
			out = append(out, r)
		}
	}
	return out
}

func roleAllowed(role string, allowed []string) bool {
	for _, a := range allowed {
		if role == a {
			return true
		}
	}
	return false
}

func loadCertPool(caFile string) (*x509.CertPool, error) {
	pemData, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tlsutil: read CA %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemData) {
		return nil, fmt.Errorf("tlsutil: no CA certificates found in %s", caFile)
	}
	return pool, nil
}
