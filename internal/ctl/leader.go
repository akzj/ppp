package ctl

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LeaderElector implements PG lease-based leader election for the control
// plane. Exactly one instance holds the ctl_leader row lease at a time: the
// election UPDATE atomically takes the lease when it is free (expired) or
// already held by this instance; the holder renews periodically, and on
// exit/crash the lease expires (leader-lease) so another instance takes over.
// There is no raft — PostgreSQL is the single source of truth.
type LeaderElector struct {
	pool       *pgxpool.Pool
	instanceID string
	lease      time.Duration
	renew      time.Duration

	mu     sync.Mutex
	last   bool // last leader state, for transition logging
	stop   chan struct{}
	done   chan struct{}
	leader atomic.Bool
}

// NewLeaderElector creates the elector bound to the given PG pool.
func NewLeaderElector(pool *pgxpool.Pool, instanceID string, lease, renew time.Duration) *LeaderElector {
	return &LeaderElector{pool: pool, instanceID: instanceID, lease: lease, renew: renew}
}

// tryAcquire attempts one election round; returns true when this instance is
// the leader.
func (l *LeaderElector) tryAcquire(ctx context.Context) (bool, error) {
	leaseSecs := l.lease.Seconds()
	err := l.pool.QueryRow(ctx,
		`UPDATE ctl_leader
		 SET instance_id = $1, lease_until = now() + ($2::double precision * interval '1 second')
		 WHERE singleton_id = 1 AND (instance_id = $1 OR lease_until < now())
		 RETURNING instance_id`,
		l.instanceID, leaseSecs).Scan(&l.instanceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // another instance holds a fresh lease
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// bootstrap ensures the singleton row exists (idempotent; on a brand-new
// database one instance creates it and immediately owns the lease).
func (l *LeaderElector) bootstrap(ctx context.Context) error {
	leaseSecs := l.lease.Seconds()
	_, err := l.pool.Exec(ctx,
		`INSERT INTO ctl_leader(singleton_id, instance_id, lease_until)
		 VALUES (1, $1, now() + ($2::double precision * interval '1 second'))
		 ON CONFLICT (singleton_id) DO NOTHING`,
		l.instanceID, leaseSecs)
	return err
}

// Run acquires leadership (renewing the lease periodically) until the context
// is cancelled or Stop is called.
func (l *LeaderElector) Run(ctx context.Context) {
	l.mu.Lock()
	l.stop = make(chan struct{})
	l.done = make(chan struct{})
	l.mu.Unlock()
	defer close(l.done)

	if err := l.bootstrap(ctx); err != nil {
		log.Printf("ctl leader %s: bootstrap: %v", l.instanceID, err)
	}
	ok, err := l.tryAcquire(ctx)
	l.setLeader(ok, err, "initial acquire")

	ticker := time.NewTicker(l.renew)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.stop:
			return
		case <-ticker.C:
			ok, err := l.tryAcquire(ctx)
			l.setLeader(ok, err, "renew")
		}
	}
}

// setLeader stores the election outcome and logs state transitions only.
func (l *LeaderElector) setLeader(ok bool, err error, stage string) {
	if err != nil {
		log.Printf("ctl leader %s: %s: %v", l.instanceID, stage, err)
		l.leader.Store(false)
		return
	}
	l.leader.Store(ok)
	l.mu.Lock()
	prev := l.last
	l.last = ok
	l.mu.Unlock()
	if ok != prev {
		if ok {
			log.Printf("ctl leader %s: became leader", l.instanceID)
		} else {
			log.Printf("ctl leader %s: lost leadership", l.instanceID)
		}
	}
}

// Stop terminates the renewal loop, waits for it to exit and clears the
// leader flag (a stopped instance no longer holds the lease).
func (l *LeaderElector) Stop() {
	l.mu.Lock()
	if l.stop != nil {
		close(l.stop)
	}
	l.mu.Unlock()
	if l.done != nil {
		<-l.done
	}
	l.leader.Store(false)
}

// IsLeader reports whether this instance currently holds the lease.
func (l *LeaderElector) IsLeader() bool { return l.leader.Load() }
