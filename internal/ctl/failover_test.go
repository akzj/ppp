package ctl

import (
	"context"
	"net"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestMultiInstanceFailover verifies the PG lease leader election end to end
// over real gRPC: two control-plane instances share one PG, exactly one
// accepts mutations while the follower rejects with Unavailable, and after the
// leader stops renewing (crash) the follower takes over and serves mutations.
func TestMultiInstanceFailover(t *testing.T) {
	truncatePG(t)

	start := func(id string) (pppv1.ControlClient, func()) {
		cfg := DefaultConfig()
		cfg.PGDSN = testPGDSN
		cfg.InstanceID = id
		cfg.HTTPAddr = "127.0.0.1:0" // per-instance /leader endpoint (no bind conflict)
		cfg.LeaderLease = 1 * time.Second
		cfg.LeaderRenew = 100 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		gs, done, err := ServeControl(ctx, cfg, lis)
		if err != nil {
			cancel()
			t.Fatalf("ServeControl(%s): %v", id, err)
		}
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cancel()
			t.Fatalf("dial(%s): %v", id, err)
		}
		stop := func() {
			cancel()
			gs.Stop()
			<-done
			_ = conn.Close()
		}
		return pppv1.NewControlClient(conn), stop
	}

	ca, stopA := start("a")
	defer stopA()
	cb, stopB := start("b")
	defer stopB()

	create := func(c pppv1.ControlClient, id string) error {
		_, err := c.CreateTree(context.Background(), &pppv1.CreateTreeRequest{
			Tree: &pppv1.Tree{Id: id, RootCount: 1, GroupMembers: 1, GroupChildren: 1},
		})
		return err
	}

	// Exactly one instance accepts mutations (the leader); the follower
	// rejects with Unavailable.
	errA := create(ca, "t-a")
	errB := create(cb, "t-b")
	leaderIsA := errA == nil
	leaderIsB := errB == nil
	if leaderIsA == leaderIsB {
		t.Fatalf("exactly one leader expected: a=%v b=%v (errA=%v errB=%v)", leaderIsA, leaderIsB, errA, errB)
	}
	followerErr := errB
	if leaderIsA {
		followerErr = errB
	} else {
		followerErr = errA
	}
	if status.Code(followerErr) != codes.Unavailable {
		t.Fatalf("follower mutation code = %v, want Unavailable", status.Code(followerErr))
	}

	// Kill the leader (stop renewing; the lease then expires).
	if leaderIsA {
		stopA()
	} else {
		stopB()
	}

	// The surviving instance takes over and accepts a new mutation.
	survivor := cb
	if leaderIsA {
		survivor = cb
	} else {
		survivor = ca
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := create(survivor, "t-after-failover"); err == nil {
			return // follower took over successfully
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("follower did not take over after the leader stopped")
}
