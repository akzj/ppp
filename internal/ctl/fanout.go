package ctl

import (
	"sync"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// fanoutBuffer is the per-subscriber channel capacity. If a subscriber is too
// slow to drain its channel, updates are dropped; a dropped topology or banned
// update heals on the next full push or on reconnect (the banned watch starts
// with an authoritative full snapshot), which is acceptable for a control
// plane that pushes full topology snapshots.
const fanoutBuffer = 128

// Fanout delivers per-tree updates to the WatchTopology, WatchBannedList and
// WatchJobs streams. A subscriber registers with a channel and must
// unsubscribe when its stream ends (context cancellation). Publishes never
// block: a full channel is dropped rather than stalling the control plane.
type Fanout struct {
	mu       sync.Mutex
	topology map[string]map[chan *pppv1.TopologyUpdate]struct{}
	banned   map[string]map[chan *pppv1.BannedListUpdate]struct{}
	jobs     map[string]map[chan *pppv1.JobUpdate]struct{}
}

func newFanout() *Fanout {
	return &Fanout{
		topology: make(map[string]map[chan *pppv1.TopologyUpdate]struct{}),
		banned:   make(map[string]map[chan *pppv1.BannedListUpdate]struct{}),
		jobs:     make(map[string]map[chan *pppv1.JobUpdate]struct{}),
	}
}

// clearTree drops every subscriber of a tree (used when the tree is deleted).
func (f *Fanout) clearTree(treeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.topology, treeID)
	delete(f.banned, treeID)
	delete(f.jobs, treeID)
}

// ============ Topology ============

func (f *Fanout) subscribeTopology(treeID string) chan *pppv1.TopologyUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan *pppv1.TopologyUpdate, fanoutBuffer)
	if f.topology[treeID] == nil {
		f.topology[treeID] = make(map[chan *pppv1.TopologyUpdate]struct{})
	}
	f.topology[treeID][ch] = struct{}{}
	return ch
}

func (f *Fanout) unsubscribeTopology(treeID string, ch chan *pppv1.TopologyUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if subs := f.topology[treeID]; subs != nil {
		delete(subs, ch)
	}
}

func (f *Fanout) publishTopology(treeID string, up *pppv1.TopologyUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for ch := range f.topology[treeID] {
		select {
		case ch <- up:
		default: // subscriber too slow; next full push or reconnect heals.
		}
	}
}

// ============ Banned list ============

func (f *Fanout) subscribeBanned(treeID string) chan *pppv1.BannedListUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan *pppv1.BannedListUpdate, fanoutBuffer)
	if f.banned[treeID] == nil {
		f.banned[treeID] = make(map[chan *pppv1.BannedListUpdate]struct{})
	}
	f.banned[treeID][ch] = struct{}{}
	return ch
}

func (f *Fanout) unsubscribeBanned(treeID string, ch chan *pppv1.BannedListUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if subs := f.banned[treeID]; subs != nil {
		delete(subs, ch)
	}
}

func (f *Fanout) publishBanned(treeID string, up *pppv1.BannedListUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for ch := range f.banned[treeID] {
		select {
		case ch <- up:
		default: // subscriber too slow; reconnect full snapshot heals.
		}
	}
}

// ============ Jobs ============

func (f *Fanout) subscribeJobs(treeID string) chan *pppv1.JobUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan *pppv1.JobUpdate, fanoutBuffer)
	if f.jobs[treeID] == nil {
		f.jobs[treeID] = make(map[chan *pppv1.JobUpdate]struct{})
	}
	f.jobs[treeID][ch] = struct{}{}
	return ch
}

func (f *Fanout) unsubscribeJobs(treeID string, ch chan *pppv1.JobUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if subs := f.jobs[treeID]; subs != nil {
		delete(subs, ch)
	}
}

func (f *Fanout) publishJobs(treeID string, up *pppv1.JobUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for ch := range f.jobs[treeID] {
		select {
		case ch <- up:
		default: // subscriber too slow; active jobs are replayed on reconnect.
		}
	}
}
