// Package tlsutil builds gRPC TLS credentials for the ppp control and data
// planes. Both sides are configured from flags; when all TLS flags are empty
// the credentials are nil and callers keep the plaintext path (development
// compatibility). This phase does CA mutual verification (identity-based
// authorization is a later concern).
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
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
