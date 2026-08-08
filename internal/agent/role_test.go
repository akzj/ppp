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

	"github.com/akzj/ppp/gen/ppp/v1"
	"github.com/akzj/ppp/internal/ctl"
	"github.com/akzj/ppp/internal/tlsutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// writeRoleFiles writes a certificate carrying the given OU role (Phase 10).
func writeRoleFiles(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn, ou string, sans []string, ips []net.IP, usages []x509.ExtKeyUsage) (certFile, keyFile string) {
	t.Helper()
	certPEM, keyPEM, err := tlsutil.GenerateTestCert(ca, caKey, cn, ou, sans, ips, usages)
	if err != nil {
		t.Fatalf("GenerateTestCert(%s): %v", cn, err)
	}
	return writeFilePEM(t, cn+".pem", certPEM), writeFilePEM(t, cn+".key", keyPEM)
}

func roleCreds(t *testing.T, caFile, certFile, keyFile string) credentials.TransportCredentials {
	t.Helper()
	return tlsClientCreds(t, caFile, certFile, keyFile)
}

// TestRoleAuthorization verifies the certificate role gate end to end (Phase
// 10): the ctl Control server and the agent Data server both reject callers
// whose certificate OU role is not allowed when -tls-require-role is set.
func TestRoleAuthorization(t *testing.T) {
	truncateCtlPG(t)
	ca, caKey, _, err := tlsutil.GenerateTestCA()
	if err != nil {
		t.Fatalf("GenerateTestCA: %v", err)
	}
	caFile := writeFilePEM(t, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}))
	sans := []string{"localhost"}
	ips := []net.IP{net.ParseIP("127.0.0.1")}

	ctlCert, ctlKey := writeRoleFiles(t, ca, caKey, "ppp-ctl", "", sans, ips, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	svcCert, svcKey := writeRoleFiles(t, ca, caKey, "ppp-node", tlsutil.RoleService, sans, ips, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})
	cliCert, cliKey := writeRoleFiles(t, ca, caKey, "ppp-orch", tlsutil.RoleClient, nil, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	noOUCert, noOUKey := writeRoleFiles(t, ca, caKey, "ppp-noou", "", sans, ips, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	adminCert, adminKey := writeRoleFiles(t, ca, caKey, "ppp-admin", "admin", sans, ips, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	// Deterministic 2-piece content served by an in-test HTTP source.
	content := make([]byte, mtlsTestContentLen)
	for i := range content {
		content[i] = byte(i * 47)
	}
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(content)
	}))
	defer httpSrv.Close()

	// --- the ctl with the role gate (allowed: service, client) ---
	ctlCfg := ctl.DefaultConfig()
	ctlCfg.PGDSN = ctlTestPGDSN
	ctlCfg.TLSCA = caFile
	ctlCfg.TLSCert = ctlCert
	ctlCfg.TLSKey = ctlKey
	ctlCfg.TLSRequireRole = "service,client"
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
	ctlAddr := ctlLis.Addr().String()

	svcCreds := roleCreds(t, caFile, svcCert, svcKey)
	cliCreds := roleCreds(t, caFile, cliCert, cliKey)
	noOUCreds := roleCreds(t, caFile, noOUCert, noOUKey)
	adminCreds := roleCreds(t, caFile, adminCert, adminKey)

	dialCtl := func(creds credentials.TransportCredentials) pppv1.ControlClient {
		conn, err := grpc.NewClient(ctlAddr, grpc.WithTransportCredentials(creds))
		if err != nil {
			t.Fatalf("dial ctl: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return pppv1.NewControlClient(conn)
	}

	// --- client role: CreateTree + CreateJob succeed ---
	cliCtl := dialCtl(cliCreds)
	if _, err := cliCtl.CreateTree(context.Background(), &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{
			Id: "t1", RootCount: 1, GroupMembers: 2, GroupChildren: 2,
			Source: &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{httpSrv.URL}},
		},
	}); err != nil {
		t.Fatalf("client CreateTree: %v", err)
	}

	// --- service role: RegisterNode succeeds ---
	svcCtl := dialCtl(svcCreds)
	if _, err := svcCtl.RegisterNode(context.Background(), &pppv1.RegisterNodeRequest{
		Node: &pppv1.Node{Id: "probe", Addr: "127.0.0.1:1", TreeId: "t1", Role: pppv1.Node_MEMBER},
	}); err != nil {
		t.Fatalf("service RegisterNode: %v", err)
	}

	// --- no-OU and wrong-role callers are PermissionDenied ---
	if _, err := dialCtl(noOUCreds).CreateTree(context.Background(), &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{Id: "t2", RootCount: 1, GroupMembers: 2, GroupChildren: 2},
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("no-OU CreateTree code = %v, want PermissionDenied", status.Code(err))
	}
	if _, err := dialCtl(adminCreds).CreateTree(context.Background(), &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{Id: "t3", RootCount: 1, GroupMembers: 2, GroupChildren: 2},
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("admin CreateTree code = %v, want PermissionDenied", status.Code(err))
	}

	// --- the root agent with the role gate (allowed: service, client) ---
	cfg := DefaultConfig()
	cfg.ID = "root"
	cfg.Addr = "127.0.0.1:0"
	cfg.CtlAddr = ctlAddr
	cfg.Tree = "t1"
	cfg.Role = pppv1.Node_ROOT
	cfg.DownloadPath = filepath.Join(t.TempDir(), "data-root")
	cfg.HeartbeatInterval = 200 * time.Millisecond
	cfg.TLSCA = caFile
	cfg.TLSCert = svcCert
	cfg.TLSKey = svcKey
	cfg.TLSServerName = "localhost"
	cfg.TLSRequireRole = "service,client"
	root, err := NewAgent(cfg)
	if err != nil {
		t.Fatalf("NewAgent(root): %v", err)
	}
	rootCtx, rootCancel := context.WithCancel(context.Background())
	if err := root.Start(rootCtx); err != nil {
		t.Fatalf("Start(root): %v", err)
	}
	t.Cleanup(func() { rootCancel(); root.Stop() })
	waitFor(t, 10*time.Second, "root registered", func() bool { return root.Addr() != "" })

	// --- client role: CreateJob succeeds and drives the root build ---
	if _, err := cliCtl.CreateJob(context.Background(), &pppv1.CreateJobRequest{
		TreeId: "t1", Filename: "file.bin", Size: int64(len(content)),
	}); err != nil {
		t.Fatalf("client CreateJob: %v", err)
	}
	waitFor(t, 30*time.Second, "root downloaded the file", func() bool {
		_, err := os.Stat(filepath.Join(root.cfg.DownloadPath, "file.bin"))
		return err == nil
	})

	dialData := func(creds credentials.TransportCredentials) pppv1.DataClient {
		conn, err := grpc.NewClient(root.Addr(),
			grpc.WithTransportCredentials(creds),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
		if err != nil {
			t.Fatalf("dial data: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return pppv1.NewDataClient(conn)
	}
	getMeta := func(d pppv1.DataClient) []byte {
		t.Helper()
		info, err := d.GetFileInfo(context.Background(), &pppv1.GetFileInfoRequest{
			Key: &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"},
		})
		if err != nil || info.GetInfo() == nil {
			t.Fatalf("GetFileInfo: %v %v", err, info.GetError())
		}
		return info.GetInfo().GetMetadataId()
	}

	// --- service-role peer: GetPiece succeeds ---
	svcData := dialData(svcCreds)
	meta := getMeta(svcData)
	resp, err := svcData.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"}, Index: 0, Size: int64(len(content)), MetadataId: meta,
	})
	if err != nil || resp.GetError() != nil {
		t.Fatalf("service GetPiece: %v %v", err, resp.GetError())
	}

	// --- client-role leaf: GetPiece succeeds ---
	cliData := dialData(cliCreds)
	resp, err = cliData.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"}, Index: 0, Size: int64(len(content)), MetadataId: meta,
	})
	if err != nil || resp.GetError() != nil {
		t.Fatalf("client GetPiece: %v %v", err, resp.GetError())
	}

	// --- wrong role: GetPiece is PermissionDenied ---
	if _, err := dialData(adminCreds).GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"}, Index: 0, Size: int64(len(content)), MetadataId: meta,
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("admin GetPiece code = %v, want PermissionDenied", status.Code(err))
	}
}

// TestRoleAuthPlaintextSkips locks the plaintext path: even with
// -tls-require-role set, a server without TLS has no peer identity to check,
// so calls pass through unchanged (the development mode).
func TestRoleAuthPlaintextSkips(t *testing.T) {
	truncateCtlPG(t)
	cfg := ctl.DefaultConfig()
	cfg.PGDSN = ctlTestPGDSN
	cfg.TLSRequireRole = "service,client" // set, but NO TLS flags -> plaintext
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, done, err := ctl.ServeControl(ctx, cfg, lis)
	if err != nil {
		t.Fatalf("ServeControl: %v", err)
	}
	defer func() { cancel(); <-done }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := pppv1.NewControlClient(conn).CreateTree(context.Background(), &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{Id: "t-plain", RootCount: 1, GroupMembers: 2, GroupChildren: 2},
	}); err != nil {
		t.Fatalf("plaintext CreateTree with the role flag set: %v", err)
	}
}
