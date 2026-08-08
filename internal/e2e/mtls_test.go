package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"github.com/akzj/ppp/internal/tlsutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func writeMTLSPEM(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func genMTLSCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, dns []string, ips []net.IP, usages []x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	t.Helper()
	certPEM, keyPEM, err := tlsutil.GenerateTestCert(ca, caKey, cn, dns, ips, usages)
	if err != nil {
		t.Fatalf("GenerateTestCert(%s): %v", cn, err)
	}
	return certPEM, keyPEM
}

// TestE2EMTLS runs the single-root distribution scenario with REAL binaries
// over mTLS (ctl + services use -tls-* flags; the orchestrator client presents
// a client certificate). A certless client is rejected by the TLS Data server.
func TestE2EMTLS(t *testing.T) {
	truncateE2EPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// In-memory PKI + PEM files for the ctl, the nodes and the orchestrator.
	ca, caKey, _, err := tlsutil.GenerateTestCA()
	if err != nil {
		t.Fatalf("GenerateTestCA: %v", err)
	}
	logDir := t.TempDir()
	caFile := writeMTLSPEM(t, logDir, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}))
	sans := []string{"localhost"}
	ips := []net.IP{net.ParseIP("127.0.0.1")}
	ctlCert, ctlKey := genMTLSCert(t, ca, caKey, "ppp-ctl", sans, ips, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	ctlCertFile := writeMTLSPEM(t, logDir, "ctl.pem", ctlCert)
	ctlKeyFile := writeMTLSPEM(t, logDir, "ctl.key", ctlKey)
	nodeCert, nodeKey := genMTLSCert(t, ca, caKey, "ppp-node", sans, ips, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})
	nodeCertFile := writeMTLSPEM(t, logDir, "node.pem", nodeCert)
	nodeKeyFile := writeMTLSPEM(t, logDir, "node.key", nodeKey)
	orchCert, orchKey := genMTLSCert(t, ca, caKey, "ppp-orch", nil, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	orchCertFile := writeMTLSPEM(t, logDir, "orch.pem", orchCert)
	orchKeyFile := writeMTLSPEM(t, logDir, "orch.key", orchKey)

	// Deterministic 2-piece content served by an in-test HTTP source.
	content := make([]byte, contentLen)
	for i := range content {
		content[i] = byte(i * 53)
	}
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, filename, time.Time{}, bytes.NewReader(content))
	}))
	defer httpSrv.Close()

	// --- ctl over mTLS ---
	ctlPort := freePort(t)
	ctlAddr := fmt.Sprintf("127.0.0.1:%d", ctlPort)
	startProc(t, logDir, "ctl", ctlBin,
		"-addr", ctlAddr, "-pg-dsn", e2ePGDSN,
		"-tls-ca", caFile, "-tls-cert", ctlCertFile, "-tls-key", ctlKeyFile)
	waitPort(t, ctlAddr, 10*time.Second)

	orchCreds, err := tlsutil.LoadClientCredentials(caFile, orchCertFile, orchKeyFile, "localhost")
	if err != nil {
		t.Fatalf("LoadClientCredentials: %v", err)
	}
	ctlConn, err := grpc.NewClient(ctlAddr, grpc.WithTransportCredentials(orchCreds))
	if err != nil {
		t.Fatalf("dial ctl: %v", err)
	}
	defer ctlConn.Close()
	ctlClient := pppv1.NewControlClient(ctlConn)
	if _, err := ctlClient.CreateTree(ctx, &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{Id: treeID, RootCount: 1, GroupMembers: 2, GroupChildren: 2,
			Source: &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{httpSrv.URL}}},
	}); err != nil {
		t.Fatalf("CreateTree: %v", err)
	}

	// --- root + member services over mTLS ---
	tlsSvc := func(id, role, downloadPath string) string {
		addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
		startProc(t, logDir, "svc-"+id,
			svcBin,
			"-id", id, "-addr", addr, "-ctl-addr", ctlAddr,
			"-tree", treeID, "-role", role, "-download-path", downloadPath,
			"-heartbeat-interval", "200ms", "-lease-ttl", "2s",
			"-tls-ca", caFile, "-tls-cert", nodeCertFile, "-tls-key", nodeKeyFile,
			"-tls-server-name", "localhost")
		waitPort(t, addr, 10*time.Second)
		return addr
	}
	rootPath := filepath.Join(t.TempDir(), "root")
	memberPath := filepath.Join(t.TempDir(), "member")
	tlsSvc("root", "root", rootPath)
	memberAddr := tlsSvc("member", "member", memberPath)

	// CreateJob: the root pulls from the source (over mTLS to the ctl).
	if _, err := ctlClient.CreateJob(ctx, &pppv1.CreateJobRequest{TreeId: treeID, Filename: filename, Size: int64(len(content))}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	waitForFile(t, mmapFinalPath(rootPath, treeID, filename), content, 30*time.Second)

	// The member's GetPiece (orchestrator over mTLS) triggers its peer fetch
	// from the root over the encrypted link, then the file is persisted.
	dataConn, err := grpc.NewClient(memberAddr,
		grpc.WithTransportCredentials(orchCreds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
	if err != nil {
		t.Fatalf("dial data: %v", err)
	}
	defer dataConn.Close()
	dataClient := pppv1.NewDataClient(dataConn)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		info, err := dataClient.GetFileInfo(ctx, &pppv1.GetFileInfoRequest{
			Key: &pppv1.TreeKey{TreeId: treeID, Filename: filename},
		})
		if err != nil || info.GetInfo() == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		resp, err := dataClient.GetPiece(ctx, &pppv1.GetPieceRequest{
			Key: &pppv1.TreeKey{TreeId: treeID, Filename: filename}, Index: 0, Size: int64(len(content)), JobId: "e2e:tls", MetadataId: info.GetInfo().GetMetadataId(),
		})
		if err == nil && resp.GetError().GetCode() == pppv1.Error_ERROR_CODE_UNSPECIFIED {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	waitForFile(t, mmapFinalPath(memberPath, treeID, filename), content, 15*time.Second)

	// A certless client is rejected by the TLS Data server.
	certlessConn, err := grpc.NewClient(memberAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial certless: %v", err)
	}
	defer certlessConn.Close()
	if _, err := pppv1.NewDataClient(certlessConn).GetPiece(ctx, &pppv1.GetPieceRequest{
		Key: &pppv1.TreeKey{TreeId: treeID, Filename: filename}, Index: 0, Size: int64(len(content)), JobId: "e2e:tls", MetadataId: e2eFixedMetaID,
	}); err == nil {
		t.Fatal("certless client was not rejected by the TLS Data server")
	}
}
