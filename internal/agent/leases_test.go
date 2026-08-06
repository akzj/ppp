package agent

import (
	"testing"
	"time"
)

// TestLeaseManagerRenewExpire verifies renewal, counting and expiry pruning.
func TestLeaseManagerRenewExpire(t *testing.T) {
	now := time.Unix(1000, 0)
	l := NewLeaseManager(30 * time.Second)

	l.Renew("t1", "a.bin", "job:1", "child1", now)
	l.Renew("t1", "a.bin", "job:1", "child2", now)
	if l.Count() != 2 {
		t.Fatalf("count = %d, want 2", l.Count())
	}
	// Renewing the same key is idempotent (still one lease).
	l.Renew("t1", "a.bin", "job:1", "child1", now)
	if l.Count() != 2 {
		t.Fatalf("count after renew = %d, want 2", l.Count())
	}

	l.Remove("t1", "a.bin", "job:1", "child2")
	if l.Count() != 1 {
		t.Fatalf("count after remove = %d, want 1", l.Count())
	}

	// Nothing expires before the TTL; everything does afterwards.
	l.Expire(now.Add(29 * time.Second))
	if l.Count() != 1 {
		t.Fatalf("count before TTL = %d, want 1", l.Count())
	}
	l.Expire(now.Add(31 * time.Second))
	if l.Count() != 0 {
		t.Fatalf("count after TTL = %d, want 0", l.Count())
	}
}
