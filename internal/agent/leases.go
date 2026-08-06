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
// the granted duration never falls out of alignment.
func (l *LeaseManager) Renew(d time.Duration, treeID, filename, jobID, childNodeID string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.leases[leaseKey{treeID, filename, jobID, childNodeID}] = now.Add(d)
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
