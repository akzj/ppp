package ctl

import (
	"context"
	"testing"
	"time"
)

// waitLeader polls until the elector reports leadership.
func waitLeader(t *testing.T, e *LeaderElector, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("elector never became leader")
}

// TestLeaderElectorExactlyOneLeader verifies two electors sharing one PG elect
// exactly one leader (the lease UPDATE is atomic).
func TestLeaderElectorExactlyOneLeader(t *testing.T) {
	truncatePG(t)
	pool := openTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewLeaderElector(pool, "a", 2*time.Second, 50*time.Millisecond)
	b := NewLeaderElector(pool, "b", 2*time.Second, 50*time.Millisecond)
	go a.Run(ctx)
	go b.Run(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a.IsLeader() || b.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if a.IsLeader() == b.IsLeader() {
		t.Fatalf("exactly one leader expected, got a=%v b=%v", a.IsLeader(), b.IsLeader())
	}
}

// TestLeaderElectorRenewalMaintains verifies a leader renewing its lease keeps
// leadership across several lease periods.
func TestLeaderElectorRenewalMaintains(t *testing.T) {
	truncatePG(t)
	pool := openTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := NewLeaderElector(pool, "a", 300*time.Millisecond, 50*time.Millisecond)
	go e.Run(ctx)
	waitLeader(t, e, 5*time.Second)
	time.Sleep(500 * time.Millisecond) // several lease periods
	if !e.IsLeader() {
		t.Fatal("leader lost leadership despite renewal")
	}
}

// TestLeaderElectorLeaseExpiryTakeover verifies that when the leader stops
// renewing (crash), its lease expires and another elector takes over.
func TestLeaderElectorLeaseExpiryTakeover(t *testing.T) {
	truncatePG(t)
	pool := openTestPool(t)

	ctxA, cancelA := context.WithCancel(context.Background())
	a := NewLeaderElector(pool, "a", 300*time.Millisecond, 50*time.Millisecond)
	go a.Run(ctxA)
	waitLeader(t, a, 5*time.Second)

	// Simulate the leader crashing: stop renewing.
	cancelA()
	a.Stop()

	// A new elector takes over once a's lease expires.
	b := NewLeaderElector(pool, "b", 300*time.Millisecond, 50*time.Millisecond)
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	go b.Run(ctxB)
	waitLeader(t, b, 5*time.Second)
	if a.IsLeader() {
		t.Fatal("old leader still reported leader after stopping")
	}
}
