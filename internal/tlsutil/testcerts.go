// Test helpers for building in-memory PKI (crypto/x509 + ECDSA, no openssl).
// They are exported so internal integration/e2e tests can generate a CA and
// signed server/client certificates without external tooling.
package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"
)

// GenerateTestCA creates an in-memory self-signed CA (ECDSA P-256). It returns
// the CA certificate (for signing leaves), the CA private key, and a cert pool
// containing the CA (usable as a trust anchor / client CA).
func GenerateTestCA() (*x509.Certificate, *ecdsa.PrivateKey, *x509.CertPool, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("tlsutil: generate ca key: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ppp-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("tlsutil: create ca: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("tlsutil: parse ca: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return cert, key, pool, nil
}

// GenerateTestCert creates a leaf certificate signed by the given CA, for the
// given common name and SANs, with the given extended key usages (e.g.
// ExtKeyUsageServerAuth, ExtKeyUsageClientAuth, or both for a node that is
// both a server and a client). It returns the PEM-encoded certificate and
// private key (write them to temp files for LoadServerCredentials /
// LoadClientCredentials).
func GenerateTestCert(ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn, ou string, dnsNames []string, ips []net.IP, usages []x509.ExtKeyUsage) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("tlsutil: generate cert key: %w", err)
	}
	subject := pkix.Name{CommonName: cn}
	if ou != "" {
		// The OU carries the identity role (ctl/service/client, Phase 10).
		subject.OrganizationalUnit = []string{ou}
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      subject,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
		DNSNames:     dnsNames,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("tlsutil: create cert: %w", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("tlsutil: marshal key: %w", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}
