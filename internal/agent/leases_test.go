package agent

import (
	"testing"
	"time"
)

// TestLeaseManagerRenewExpire verifies renewal, counting and expiry pruning.
func TestLeaseManagerRenewExpire(t *testing.T) {
	now := time.Unix(1000, 0)
	l := NewLeaseManager(30 * time.Second)

	l.Renew(30*time.Second, "t1", "a.bin", "job:1", "child1", now)
	l.Renew(30*time.Second, "t1", "a.bin", "job:1", "child2", now)
	if l.Count() != 2 {
		t.Fatalf("count = %d, want 2", l.Count())
	}
	// Renewing the same key is idempotent (still one lease).
	l.Renew(30*time.Second, "t1", "a.bin", "job:1", "child1", now)
	if l.Count() != 2 {
		t.Fatalf("count after renew = %d, want 2", l.Count())
	}

	if !l.Remove("t1", "a.bin", "job:1", "child2") {
		t.Fatal("Remove returned false for an existing lease")
	}
	if l.Remove("t1", "a.bin", "job:1", "child2") {
		t.Fatal("Remove returned true for a missing lease")
	}
	if l.Count() != 1 {
		t.Fatalf("count after remove = %d, want 1", l.Count())
	}

	// Nothing expires before the grant; everything does afterwards, and the
	// expired keys are returned.
	if got := l.Expire(now.Add(29 * time.Second)); len(got) != 0 {
		t.Fatalf("expired before grant = %v, want none", got)
	}
	if got := l.Expire(now.Add(31 * time.Second)); len(got) != 1 {
		t.Fatalf("expired after grant = %v, want 1 key", got)
	}
	if l.Count() != 0 {
		t.Fatalf("count after TTL = %d, want 0", l.Count())
	}
}
