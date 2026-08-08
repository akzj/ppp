package agent

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"github.com/akzj/ppp/internal/ctl"
	"github.com/akzj/ppp/internal/tlsutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func writeFilePEM(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func mustTLSFiles(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, dns []string, ips []net.IP, usages []x509.ExtKeyUsage) (certFile, keyFile string) {
	t.Helper()
	certPEM, keyPEM, err := tlsutil.GenerateTestCert(ca, caKey, cn, dns, ips, usages)
	if err != nil {
		t.Fatalf("GenerateTestCert(%s): %v", cn, err)
	}
	return writeFilePEM(t, cn+".pem", certPEM), writeFilePEM(t, cn+".key", keyPEM)
}

// writeMTLSFiles generates an in-memory PKI and writes the PEM files for the
// ctl server, an agent node (server+client) and a test orchestrator client.
// All certificates are signed by the same CA; SANs cover localhost/127.0.0.1.
func writeMTLSFiles(t *testing.T) (caFile, ctlCert, ctlKey, agentCert, agentKey, clientCert, clientKey string) {
	t.Helper()
	ca, caKey, _, err := tlsutil.GenerateTestCA()
	if err != nil {
		t.Fatalf("GenerateTestCA: %v", err)
	}
	caFile = writeFilePEM(t, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}))
	sans := []string{"localhost"}
	ips := []net.IP{net.ParseIP("127.0.0.1")}

	ctlCert, ctlKey = mustTLSFiles(t, ca, caKey, "ppp-ctl", sans, ips, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	// An agent node is both a server (Data service) and a client (ctl + peers).
	agentCert, agentKey = mustTLSFiles(t, ca, caKey, "ppp-node", sans, ips, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})
	clientCert, clientKey = mustTLSFiles(t, ca, caKey, "ppp-orchestrator", nil, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	return caFile, ctlCert, ctlKey, agentCert, agentKey, clientCert, clientKey
}

// tlsClientCreds builds the orchestrator's mTLS client credentials from the
// generated files.
func tlsClientCreds(t *testing.T, caFile, certFile, keyFile string) credentials.TransportCredentials {
	t.Helper()
	creds, err := tlsutil.LoadClientCredentials(caFile, certFile, keyFile, "localhost")
	if err != nil {
		t.Fatalf("LoadClientCredentials: %v", err)
	}
	return creds
}

// mtlsTestContentLen is a 2-piece file (matches the agent's 4 MiB PieceSize).
const mtlsTestContentLen = int(PieceSize) + 100

// TestAgentMTLSDistribution runs the root+member distribution scenario over
// real mTLS: an in-process ctl with TLS, a root and a member agent with TLS,
// and an orchestrator client with a client certificate. The member's on-demand
// GetPiece fetches from the root over the encrypted peer link. A certless
// client connecting to the TLS Data server is rejected.
func TestAgentMTLSDistribution(t *testing.T) {
	truncateCtlPG(t)
	caFile, ctlCert, ctlKey, agentCert, agentKey, clientCert, clientKey := writeMTLSFiles(t)
	orchestratorCreds := tlsClientCreds(t, caFile, clientCert, clientKey)

	// Deterministic 2-piece content served by an in-test HTTP source.
	content := make([]byte, mtlsTestContentLen)
	for i := range content {
		content[i] = byte(i * 41)
	}
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(content)
	}))
	defer httpSrv.Close()

	// --- in-process ctl over mTLS ---
	ctlCfg := ctl.DefaultConfig()
	ctlCfg.PGDSN = ctlTestPGDSN
	ctlCfg.TLSCA = caFile
	ctlCfg.TLSCert = ctlCert
	ctlCfg.TLSKey = ctlKey
	ctlCtx, ctlCancel := context.WithCancel(context.Background())
	defer ctlCancel()
	ctlLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ctl listen: %v", err)
	}
	_, ctlDone, err := ctl.ServeControl(ctlCtx, ctlCfg, ctlLis)
	if err != nil {
		t.Fatalf("ServeControl: %v", err)
	}
	defer func() { ctlCancel(); <-ctlDone }()

	// Orchestrator's ctl client (mTLS).
	ctlConn, err := grpc.NewClient(ctlLis.Addr().String(),
		grpc.WithTransportCredentials(orchestratorCreds))
	if err != nil {
		t.Fatalf("dial ctl: %v", err)
	}
	defer ctlConn.Close()
	ctlClient := pppv1.NewControlClient(ctlConn)
	if _, err := ctlClient.CreateTree(context.Background(), &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{
			Id: "t1", RootCount: 1, GroupMembers: 2, GroupChildren: 2,
			Source: &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{httpSrv.URL}},
		},
	}); err != nil {
		t.Fatalf("CreateTree: %v", err)
	}

	// --- root + member agents over mTLS ---
	start := func(id string, role pppv1.Node_Role) *Agent {
		cfg := DefaultConfig()
		cfg.ID = id
		cfg.Addr = "127.0.0.1:0"
		cfg.CtlAddr = ctlLis.Addr().String()
		cfg.Tree = "t1"
		cfg.Role = role
		cfg.DownloadPath = filepath.Join(t.TempDir(), "data-"+id)
		cfg.HeartbeatInterval = 200 * time.Millisecond
		cfg.TLSCA = caFile
		cfg.TLSCert = agentCert
		cfg.TLSKey = agentKey
		cfg.TLSServerName = "localhost"
		ag, err := NewAgent(cfg)
		if err != nil {
			t.Fatalf("NewAgent(%s): %v", id, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		if err := ag.Start(ctx); err != nil {
			t.Fatalf("Start(%s): %v", id, err)
		}
		t.Cleanup(func() { cancel(); ag.Stop() })
		return ag
	}
	root := start("root", pppv1.Node_ROOT)
	waitFor(t, 10*time.Second, "root registered", func() bool { return root.Addr() != "" })
	member := start("member", pppv1.Node_MEMBER)

	// --- CreateJob: the root pulls from the source; the member pulls P2P ---
	if _, err := ctlClient.CreateJob(context.Background(), &pppv1.CreateJobRequest{
		TreeId: "t1", Filename: "file.bin", Size: int64(len(content)),
	}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	waitFor(t, 30*time.Second, "root downloaded the file", func() bool {
		_, err := os.Stat(filepath.Join(root.cfg.DownloadPath, "file.bin"))
		return err == nil
	})

	// The member's GetPiece (over mTLS) triggers its peer fetch from the root.
	dataConn, err := grpc.NewClient(member.Addr(),
		grpc.WithTransportCredentials(orchestratorCreds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
	if err != nil {
		t.Fatalf("dial data: %v", err)
	}
	defer dataConn.Close()
	dataClient := pppv1.NewDataClient(dataConn)
	waitFor(t, 30*time.Second, "member fetched the file from the root over mTLS", func() bool {
		info, err := dataClient.GetFileInfo(context.Background(), &pppv1.GetFileInfoRequest{
			Key: &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"},
		})
		if err != nil || info.GetInfo() == nil {
			return false
		}
		resp, err := dataClient.GetPiece(context.Background(), &pppv1.GetPieceRequest{
			Key: &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"}, Index: 0, Size: int64(len(content)), JobId: "tls:job", MetadataId: info.GetInfo().GetMetadataId(),
		})
		if err != nil {
			return false
		}
		return resp.GetError().GetCode() == pppv1.Error_ERROR_CODE_UNSPECIFIED
	})
	if _, err := os.Stat(filepath.Join(member.cfg.DownloadPath, "file.bin")); err != nil {
		t.Fatalf("member did not persist the file: %v", err)
	}

	// --- a certless client is rejected by the TLS Data server ---
	certlessConn, err := grpc.NewClient(member.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial certless: %v", err)
	}
	defer certlessConn.Close()
	certlessClient := pppv1.NewDataClient(certlessConn)
	_, err = certlessClient.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"}, Index: 0, Size: int64(len(content)), JobId: "tls:job", MetadataId: testMetaID(),
	})
	if err == nil {
		t.Fatal("certless client was not rejected by the TLS Data server")
	}
}
