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
// pruned by a background scan. In phase 2 it is bookkeeping only: the
// downloader already manages fetching by need.
type LeaseManager struct {
	mu     sync.Mutex
	ttl    time.Duration
	leases map[leaseKey]time.Time
}

// NewLeaseManager creates a lease manager with the given TTL.
func NewLeaseManager(ttl time.Duration) *LeaseManager {
	return &LeaseManager{ttl: ttl, leases: make(map[leaseKey]time.Time)}
}

// Renew (re)establishes a lease for the key, extending it by the TTL.
func (l *LeaseManager) Renew(treeID, filename, jobID, childNodeID string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.leases[leaseKey{treeID, filename, jobID, childNodeID}] = now.Add(l.ttl)
}

// Remove deletes a lease.
func (l *LeaseManager) Remove(treeID, filename, jobID, childNodeID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.leases, leaseKey{treeID, filename, jobID, childNodeID})
}

// Count returns the number of active leases.
func (l *LeaseManager) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.leases)
}

// Expire removes every lease whose expiry is not after now. Run periodically.
func (l *LeaseManager) Expire(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, expiry := range l.leases {
		if !expiry.After(now) {
			delete(l.leases, k)
		}
	}
}
