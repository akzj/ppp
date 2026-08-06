package agent

import (
	"sync"
	"time"
)

// leaseKey identifies one child's subscription to one (tree, file, job).
type leaseKey struct {
	treeID, filename, jobID, childNodeID string
}

// LeaseManager tracks per-child per-job session leases (Subscribe/Unsubscribe
// on the Data service). Subscribes are idempotent renewals; expired leases are
// pruned by a background scan. The granted duration is explicit so the
// downloader's need accounting stays aligned with the real expiry.
//
// Orphan-lease window / renewal cadence: the agent's lease scan runs every
// LeaseTTL/2, so after a child stops renewing (e.g. its downloader stopped),
// its lease is pruned within about one TTL — the orphan window is bounded by
// [TTL/2, TTL]. Downloaders renew their upstream leases at leaseTTL/2 while
// fetching, so the upstream sees a fresh lease during active fetches and the
// lease lapses within one TTL after the fetch stops, propagating the stop
// upstream (design §3.3).
type LeaseManager struct {
	mu     sync.Mutex
	ttl    time.Duration
	leases map[leaseKey]time.Time
}

// NewLeaseManager creates a lease manager with the given default TTL.
func NewLeaseManager(ttl time.Duration) *LeaseManager {
	return &LeaseManager{ttl: ttl, leases: make(map[leaseKey]time.Time)}
}

// Renew (re)establishes a lease for the key, extending it by the explicit
// duration. The expiry stored is exactly now+duration, so a child renewing by
// the granted duration never falls out of alignment. It reports whether the
// lease was newly created (false for an idempotent renewal), which callers use
// to avoid double-counting a downloader need.
func (l *LeaseManager) Renew(d time.Duration, treeID, filename, jobID, childNodeID string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := leaseKey{treeID, filename, jobID, childNodeID}
	_, existed := l.leases[key]
	l.leases[key] = now.Add(d)
	return !existed
}

// Remove deletes a lease and reports whether one existed.
func (l *LeaseManager) Remove(treeID, filename, jobID, childNodeID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.leases[leaseKey{treeID, filename, jobID, childNodeID}]
	delete(l.leases, leaseKey{treeID, filename, jobID, childNodeID})
	return ok
}

// Count returns the number of active leases.
func (l *LeaseManager) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.leases)
}

// Expire removes every lease whose expiry is not after now and returns the
// expired keys so the caller can release the corresponding downloader needs.
func (l *LeaseManager) Expire(now time.Time) []leaseKey {
	l.mu.Lock()
	defer l.mu.Unlock()
	var expired []leaseKey
	for k, expiry := range l.leases {
		if !expiry.After(now) {
			delete(l.leases, k)
			expired = append(expired, k)
		}
	}
	return expired
}
