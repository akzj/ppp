package ctl

import (
	"context"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// heartbeatScanInterval is how often expired nodes are pruned.
const heartbeatScanInterval = 3 * time.Second

// Registry accessors. They operate on s.nodes and must only be called while
// s.mu is held (RPC handlers and the heartbeat loop guarantee this).

// regGet returns the registered node with the given id, or nil.
func (s *Server) regGet(nodeID string) *pppv1.Node {
	return s.nodes[nodeID]
}

// regPut registers (or updates) a node.
func (s *Server) regPut(n *pppv1.Node) {
	s.nodes[n.GetId()] = n
}

// regDelete removes a node from the registry.
func (s *Server) regDelete(nodeID string) {
	delete(s.nodes, nodeID)
}

// regNodesByTree returns the registered nodes of a tree (unstable order).
func (s *Server) regNodesByTree(treeID string) []*pppv1.Node {
	var out []*pppv1.Node
	for _, n := range s.nodes {
		if n.GetTreeId() == treeID {
			out = append(out, n)
		}
	}
	return out
}

// regCountRoots counts registered ROOT nodes of a tree, optionally excluding
// one node id (so a re-registering root does not count against itself).
func (s *Server) regCountRoots(treeID, excludeID string) int {
	n := 0
	for _, node := range s.nodes {
		if node.GetTreeId() == treeID && node.GetRole() == pppv1.Node_ROOT && node.GetId() != excludeID {
			n++
		}
	}
	return n
}

// heartbeatLoop prunes nodes whose last heartbeat is older than the timeout
// every heartbeatScanInterval until ctx is done.
func (s *Server) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(heartbeatScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pruneExpired()
		}
	}
}

// pruneExpired removes every node silent for longer than the heartbeat
// timeout and pushes the recomputed topology for each affected tree.
func (s *Server) pruneExpired() {
	s.mu.Lock()
	now := s.now()

	// Collect expired nodes grouped by tree, remove them from the registry and
	// the store.
	expiredByTree := make(map[string][]*pppv1.Node)
	for id, n := range s.nodes {
		if now.Sub(time.Unix(n.GetLastHeartbeat(), 0)) > s.cfg.HeartbeatTimeout {
			expiredByTree[n.GetTreeId()] = append(expiredByTree[n.GetTreeId()], n)
			s.regDelete(id)
			_ = s.store.DeleteNode(n.GetTreeId(), id) // best effort
		}
	}

	// Recompute the topology once per affected tree and collect the pushes so
	// they can be delivered after releasing the lock.
	type push struct {
		treeID string
		topo   *pppv1.Topology
		gen    int64
	}
	var pushes []push
	for treeID := range expiredByTree {
		tree, err := s.store.GetTree(treeID)
		if err != nil {
			continue // tree already gone; nothing to push
		}
		topo, gen, err := s.refreshTopologyLocked(tree)
		if err != nil {
			continue // topology not computable (e.g. no roots left)
		}
		pushes = append(pushes, push{treeID: treeID, topo: topo, gen: gen})
	}
	s.mu.Unlock()

	for _, p := range pushes {
		s.fanout.publishTopology(p.treeID, &pppv1.TopologyUpdate{Generation: p.gen, Topology: p.topo})
	}
}
