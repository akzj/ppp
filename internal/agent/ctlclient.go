package agent

import (
	"context"
	"fmt"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ctlClient wraps the Control gRPC client used by the agent.
type ctlClient struct {
	conn   *grpc.ClientConn
	client pppv1.ControlClient
}

// dialCtl connects to the control plane.
func dialCtl(addr string) (*ctlClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("agent: dial ctl %s: %w", addr, err)
	}
	return &ctlClient{conn: conn, client: pppv1.NewControlClient(conn)}, nil
}

// Close releases the connection.
func (c *ctlClient) Close() error { return c.conn.Close() }

func (c *ctlClient) RegisterNode(ctx context.Context, node *pppv1.Node) (*pppv1.RegisterNodeResponse, error) {
	return c.client.RegisterNode(ctx, &pppv1.RegisterNodeRequest{Node: node})
}

func (c *ctlClient) Heartbeat(ctx context.Context, node *pppv1.Node) (*pppv1.HeartbeatResponse, error) {
	return c.client.Heartbeat(ctx, &pppv1.HeartbeatRequest{Node: node})
}

func (c *ctlClient) WatchTopology(ctx context.Context, treeID string) (pppv1.Control_WatchTopologyClient, error) {
	return c.client.WatchTopology(ctx, &pppv1.WatchTopologyRequest{TreeId: treeID})
}

func (c *ctlClient) WatchBannedList(ctx context.Context, treeID string) (pppv1.Control_WatchBannedListClient, error) {
	return c.client.WatchBannedList(ctx, &pppv1.WatchBannedListRequest{TreeId: treeID})
}

func (c *ctlClient) WatchJobs(ctx context.Context, treeID string) (pppv1.Control_WatchJobsClient, error) {
	return c.client.WatchJobs(ctx, &pppv1.WatchJobsRequest{TreeId: treeID})
}

func (c *ctlClient) SyncBannedList(ctx context.Context, treeID string) (*pppv1.SyncBannedListResponse, error) {
	return c.client.SyncBannedList(ctx, &pppv1.SyncBannedListRequest{TreeId: treeID})
}

func (c *ctlClient) SyncProgress(ctx context.Context) (pppv1.Control_SyncProgressClient, error) {
	return c.client.SyncProgress(ctx)
}
