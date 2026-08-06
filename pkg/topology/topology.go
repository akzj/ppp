// Package topology builds the multi-root distribution topology for a tree.
//
// The algorithm is ported from tengen's UpstreamGraph
// (pkg/sirius/services/upstream/service.go) and adapted so a tree may have
// several roots:
//   - member nodes are chunked into groups of Tree.GroupMembers,
//   - groups are layered under a root group (all roots) with a BFS orchestration,
//   - every member's upstream is the address list of its parent group,
//   - non-primary roots peer with each other, while the primary root pulls from
//     the source directly.
package topology

import (
	"errors"
	"fmt"
	"sort"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// Defaults applied when the tree does not configure the group sizes.
const (
	// DefaultGroupMembers is the default number of members per group (16).
	// Used when Tree.GroupMembers <= 0.
	DefaultGroupMembers = 16
	// DefaultGroupChildren is the default maximum number of child groups a
	// parent group may own (8). Used when Tree.GroupChildren <= 0.
	DefaultGroupChildren = 8
)

// Options carries the tree and its full node set (roots + members).
type Options struct {
	Tree  *pppv1.Tree
	Nodes []*pppv1.Node
}

// upstreamGroup is an internal node of the layering graph.
type upstreamGroup struct {
	parent   *upstreamGroup
	members  []*pppv1.Node
	children []*upstreamGroup
}

// Build computes the upstream address table for every node in the tree.
//
// Every node receives exactly one entry:
//   - the primary root (lowest node ID) pulls directly from the source
//     (PullFromSource=true with an empty upstream list);
//   - every other root lists the addresses of all remaining roots, so sibling
//     roots can fetch and mutually sync (PullFromSource=false);
//   - each member lists the addresses of every node in its parent group
//     (PullFromSource=false).
//
// The input is validated before any computation: node IDs must be non-empty
// and unique, node addresses must be non-empty, and the number of roots must
// not exceed Tree.RootCount when it is configured (RootCount<=0 means no
// limit). The returned topology mirrors Tree.Generation and Tree.Id.
func Build(opts Options) (*pppv1.Topology, error) {
	if opts.Tree == nil {
		return nil, errors.New("topology: nil tree")
	}
	if err := validateNodes(opts.Nodes); err != nil {
		return nil, err
	}

	roots, members := splitNodes(opts.Nodes)
	if len(roots) == 0 {
		return nil, errors.New("topology: tree has no root node")
	}
	if rootCount := int(opts.Tree.GetRootCount()); rootCount > 0 && len(roots) > rootCount {
		return nil, fmt.Errorf("topology: %d roots exceed tree root_count %d", len(roots), rootCount)
	}

	groupSize := int(opts.Tree.GetGroupMembers())
	if groupSize <= 0 {
		groupSize = DefaultGroupMembers
	}
	groupChildren := int(opts.Tree.GetGroupChildren())
	if groupChildren <= 0 {
		groupChildren = DefaultGroupChildren
	}

	// Deterministic order: grouping and root roles both depend on node ID.
	sort.Slice(roots, func(i, j int) bool { return roots[i].GetId() < roots[j].GetId() })
	sort.Slice(members, func(i, j int) bool { return members[i].GetId() < members[j].GetId() })

	rootGroup := &upstreamGroup{members: roots}
	orchestrate(rootGroup, chunkGroups(members, groupSize), groupChildren)

	return &pppv1.Topology{
		TreeId:        opts.Tree.GetId(),
		Generation:    opts.Tree.GetGeneration(),
		NodeUpstreams: upstreams(rootGroup, roots),
	}, nil
}

// validateNodes enforces per-node invariants before the topology is built:
// every ID must be non-empty and unique (a duplicate would silently overwrite
// an entry or duplicate an address), and every address must be non-empty
// (upstreams are address lists). Nil entries are ignored.
func validateNodes(nodes []*pppv1.Node) error {
	seen := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if n.GetId() == "" {
			return errors.New("topology: node with empty id")
		}
		if _, dup := seen[n.GetId()]; dup {
			return fmt.Errorf("topology: duplicate node id %q", n.GetId())
		}
		seen[n.GetId()] = struct{}{}
		if n.GetAddr() == "" {
			return fmt.Errorf("topology: node %q has empty addr", n.GetId())
		}
	}
	return nil
}

// splitNodes classifies nodes by role. Nodes with an unspecified role are
// treated as members: MEMBER is the common case and ROLE_UNSPECIFIED is the
// proto zero value, so callers can omit it for member nodes.
func splitNodes(nodes []*pppv1.Node) (roots, members []*pppv1.Node) {
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if n.GetRole() == pppv1.Node_ROOT {
			roots = append(roots, n)
			continue
		}
		members = append(members, n)
	}
	return roots, members
}

// chunkGroups slices the sorted members into consecutive groups of at most
// groupSize. The last group may hold fewer members.
func chunkGroups(members []*pppv1.Node, groupSize int) []*upstreamGroup {
	var groups []*upstreamGroup
	for i := 0; i < len(members); i += groupSize {
		end := i + groupSize
		if end > len(members) {
			end = len(members)
		}
		groups = append(groups, &upstreamGroup{members: members[i:end]})
	}
	return groups
}

// orchestrate assigns a parent to every member group using a breadth-first
// walk over layers, ported from tengen's makeGraph. A parent is released once
// it has accumulated groupChildren children and the next parent on the current
// layer is used; when a layer is exhausted the walk moves to the next one.
//
// Unlike tengen, the root group is NOT forced to own a single child: it may
// host up to groupChildren child groups, which is the natural layout for a
// root group with multiple roots.
func orchestrate(root *upstreamGroup, groups []*upstreamGroup, groupChildren int) {
	var current *upstreamGroup
	var nextLayer []*upstreamGroup
	currentLayer := []*upstreamGroup{root}

	for _, g := range groups {
		if current == nil {
			if len(currentLayer) == 0 {
				currentLayer = nextLayer
				nextLayer = nil
			}
			current = currentLayer[0]
			currentLayer = currentLayer[1:]
		}

		g.parent = current
		current.children = append(current.children, g)
		nextLayer = append(nextLayer, g)

		if len(current.children) >= groupChildren {
			current = nil
		}
	}
}

// upstreams builds the node_id -> NodeUpstream table. Roots are handled first
// (the primary root pulls from the source with an empty list), then a
// breadth-first walk over the group tree maps every member group to its
// parent group's addresses.
func upstreams(root *upstreamGroup, roots []*pppv1.Node) map[string]*pppv1.NodeUpstream {
	result := make(map[string]*pppv1.NodeUpstream)

	// Roots: the first (lowest ID) is primary and pulls from the source
	// directly (PullFromSource=true with empty addrs); the others peer with
	// every remaining root so they can fetch and mutually sync.
	if len(roots) > 0 {
		result[roots[0].GetId()] = &pppv1.NodeUpstream{PullFromSource: true}
		for _, r := range roots[1:] {
			addrs := make([]string, 0, len(roots)-1)
			for _, other := range roots {
				if other.GetId() != r.GetId() {
					addrs = append(addrs, other.GetAddr())
				}
			}
			// PullFromSource stays false for non-primary roots.
			result[r.GetId()] = &pppv1.NodeUpstream{Addrs: addrs}
		}
	}

	// Member groups: the addresses of a parent group become the upstream of
	// every node in its child groups (PullFromSource stays false). Each member
	// gets its own copy so callers cannot mutate one entry and affect its
	// siblings.
	queue := []*upstreamGroup{root}
	for len(queue) > 0 {
		g := queue[0]
		queue = queue[1:]

		parentAddrs := make([]string, 0, len(g.members))
		for _, p := range g.members {
			parentAddrs = append(parentAddrs, p.GetAddr())
		}
		for _, child := range g.children {
			for _, m := range child.members {
				result[m.GetId()] = &pppv1.NodeUpstream{
					Addrs: append([]string(nil), parentAddrs...),
				}
			}
			queue = append(queue, child)
		}
	}
	return result
}
