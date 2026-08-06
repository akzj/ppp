package topology

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

func member(id, addr string) *pppv1.Node {
	return &pppv1.Node{Id: id, Addr: addr, Role: pppv1.Node_MEMBER}
}

func root(id, addr string) *pppv1.Node {
	return &pppv1.Node{Id: id, Addr: addr, Role: pppv1.Node_ROOT}
}

func tree(id string, groupMembers, groupChildren int32) *pppv1.Tree {
	return &pppv1.Tree{
		Id:            id,
		Generation:    7,
		GroupMembers:  groupMembers,
		GroupChildren: groupChildren,
	}
}

// addrs builds a []string literal.
func addrs(a ...string) []string { return a }

// pullFromSource is a shorthand for the primary-root entry.
func pullFromSource() *pppv1.NodeUpstream { return &pppv1.NodeUpstream{PullFromSource: true} }

// memberUpstream is a shorthand for a member/peer entry (PullFromSource=false).
func memberUpstream(a ...string) *pppv1.NodeUpstream { return &pppv1.NodeUpstream{Addrs: addrs(a...)} }

// TestSingleRootMultiLayer covers a single root with enough member groups to
// span multiple layers. Expected values are hand-computed from the BFS
// orchestration (groupSize=2, groupChildren=2, groups G1..G5):
//
//	root -> G1, G2
//	G1   -> G3, G4
//	G2   -> G5
//
// Every member's upstream must be the addresses of its parent group, and no
// parent group may own more than groupChildren child groups.
func TestSingleRootMultiLayer(t *testing.T) {
	r0 := root("r0", "10.0.0.1")
	var nodes []*pppv1.Node
	nodes = append(nodes, r0)
	// Zero-padded IDs keep lexicographic sort equal to numeric order
	// (without padding "m10" would sort before "m2").
	for i := 1; i <= 10; i++ {
		nodes = append(nodes, member(fmt.Sprintf("m%02d", i), "10.0.0."+strconv.Itoa(10+i)))
	}

	topo, err := Build(Options{Tree: tree("t1", 2, 2), Nodes: nodes})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	if topo.GetTreeId() != "t1" || topo.GetGeneration() != 7 {
		t.Fatalf("tree metadata not passed through: id=%q generation=%d", topo.GetTreeId(), topo.GetGeneration())
	}

	want := map[string]*pppv1.NodeUpstream{
		"r0":  pullFromSource(),
		"m01": memberUpstream("10.0.0.1"),
		"m02": memberUpstream("10.0.0.1"),
		"m03": memberUpstream("10.0.0.1"),
		"m04": memberUpstream("10.0.0.1"),
		"m05": memberUpstream("10.0.0.11", "10.0.0.12"),
		"m06": memberUpstream("10.0.0.11", "10.0.0.12"),
		"m07": memberUpstream("10.0.0.11", "10.0.0.12"),
		"m08": memberUpstream("10.0.0.11", "10.0.0.12"),
		"m09": memberUpstream("10.0.0.13", "10.0.0.14"),
		"m10": memberUpstream("10.0.0.13", "10.0.0.14"),
	}
	if !reflect.DeepEqual(topo.GetNodeUpstreams(), want) {
		t.Fatalf("unexpected upstreams:\n got %v\nwant %v", topo.GetNodeUpstreams(), want)
	}
}

// TestMultipleRoots covers 3 roots. The primary root (lowest ID) must pull
// directly from the source (empty upstream, PullFromSource=true); the other
// roots must peer with every remaining root; and first-layer members must see
// all root addresses.
func TestMultipleRoots(t *testing.T) {
	r1 := root("r1", "10.0.0.1")
	r2 := root("r2", "10.0.0.2")
	r3 := root("r3", "10.0.0.3")
	nodes := []*pppv1.Node{
		r1, r2, r3,
		member("m1", "10.0.0.11"),
		member("m2", "10.0.0.12"),
		member("m3", "10.0.0.13"),
		member("m4", "10.0.0.14"),
	}

	topo, err := Build(Options{Tree: tree("t2", 2, 2), Nodes: nodes})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	want := map[string]*pppv1.NodeUpstream{
		"r1": pullFromSource(),
		"r2": memberUpstream("10.0.0.1", "10.0.0.3"),
		"r3": memberUpstream("10.0.0.1", "10.0.0.2"),
		// Both member groups hang directly under the root group.
		"m1": memberUpstream("10.0.0.1", "10.0.0.2", "10.0.0.3"),
		"m2": memberUpstream("10.0.0.1", "10.0.0.2", "10.0.0.3"),
		"m3": memberUpstream("10.0.0.1", "10.0.0.2", "10.0.0.3"),
		"m4": memberUpstream("10.0.0.1", "10.0.0.2", "10.0.0.3"),
	}
	if !reflect.DeepEqual(topo.GetNodeUpstreams(), want) {
		t.Fatalf("unexpected upstreams:\n got %v\nwant %v", topo.GetNodeUpstreams(), want)
	}
}

// TestMultiRootDeepLayering covers 3 roots with enough member groups that the
// root group reaches its child limit (groupChildren=2) and the remaining group
// spills into the second layer. The root group must own at most groupChildren
// child groups, and second-layer members must see first-layer group addresses.
func TestMultiRootDeepLayering(t *testing.T) {
	nodes := []*pppv1.Node{
		root("r3", "10.0.0.3"),
		root("r1", "10.0.0.1"),
		root("r2", "10.0.0.2"),
		member("m05", "10.0.0.15"),
		member("m01", "10.0.0.11"),
		member("m02", "10.0.0.12"),
		member("m06", "10.0.0.16"),
		member("m03", "10.0.0.13"),
		member("m04", "10.0.0.14"),
	}
	tr := tree("t3", 2, 2)
	tr.RootCount = 3

	topo, err := Build(Options{Tree: tr, Nodes: nodes})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	// groupSize=2, groupChildren=2, 6 members => G1,G2,G3.
	// BFS: G1,G2 under the root group (2 children = limit), G3 under G1.
	want := map[string]*pppv1.NodeUpstream{
		"r1":  pullFromSource(),
		"r2":  memberUpstream("10.0.0.1", "10.0.0.3"),
		"r3":  memberUpstream("10.0.0.1", "10.0.0.2"),
		"m01": memberUpstream("10.0.0.1", "10.0.0.2", "10.0.0.3"),
		"m02": memberUpstream("10.0.0.1", "10.0.0.2", "10.0.0.3"),
		"m03": memberUpstream("10.0.0.1", "10.0.0.2", "10.0.0.3"),
		"m04": memberUpstream("10.0.0.1", "10.0.0.2", "10.0.0.3"),
		// Second layer: G3 hangs under G1, so its members see G1's addresses.
		"m05": memberUpstream("10.0.0.11", "10.0.0.12"),
		"m06": memberUpstream("10.0.0.11", "10.0.0.12"),
	}
	if !reflect.DeepEqual(topo.GetNodeUpstreams(), want) {
		t.Fatalf("unexpected upstreams:\n got %v\nwant %v", topo.GetNodeUpstreams(), want)
	}
}

// TestDeterministic verifies that two builds with identical input produce
// identical output.
func TestDeterministic(t *testing.T) {
	var nodes []*pppv1.Node
	nodes = append(nodes, root("r0", "10.0.0.1"))
	for i := 1; i <= 10; i++ {
		nodes = append(nodes, member(fmt.Sprintf("m%02d", i), "10.0.0."+strconv.Itoa(10+i)))
	}
	opts := Options{Tree: tree("t4", 2, 2), Nodes: nodes}

	first, err := Build(opts)
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	second, err := Build(opts)
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("build is not deterministic:\nfirst  %v\nsecond %v", first, second)
	}
}

// shuffledCopy returns a copy of nodes in a deterministic pseudo-random order
// derived from seed.
func shuffledCopy(nodes []*pppv1.Node, seed int64) []*pppv1.Node {
	out := append([]*pppv1.Node(nil), nodes...)
	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// TestInputOrderIndependent verifies that the same node set produces the same
// topology regardless of the order the caller passes the nodes in.
func TestInputOrderIndependent(t *testing.T) {
	base := []*pppv1.Node{
		root("r3", "10.0.0.3"),
		root("r1", "10.0.0.1"),
		root("r2", "10.0.0.2"),
	}
	for i := 1; i <= 10; i++ {
		base = append(base, member(fmt.Sprintf("m%02d", i), "10.0.0."+strconv.Itoa(10+i)))
	}
	opts1 := Options{Tree: tree("t5", 2, 2), Nodes: shuffledCopy(base, 1)}
	opts2 := Options{Tree: tree("t5", 2, 2), Nodes: shuffledCopy(base, 2)}

	first, err := Build(opts1)
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	second, err := Build(opts2)
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("input order changed the topology:\nfirst  %v\nsecond %v", first, second)
	}
}

// TestEntryIsolation verifies that mutating one member's upstream entry does
// not leak into sibling members sharing the same parent group.
func TestEntryIsolation(t *testing.T) {
	var nodes []*pppv1.Node
	nodes = append(nodes, root("r0", "10.0.0.1"))
	for i := 1; i <= 4; i++ {
		nodes = append(nodes, member(fmt.Sprintf("m%02d", i), "10.0.0."+strconv.Itoa(10+i)))
	}

	topo, err := Build(Options{Tree: tree("t6", 2, 2), Nodes: nodes})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	wantSibling := append([]string(nil), topo.GetNodeUpstreams()["m02"].GetAddrs()...)

	topo.GetNodeUpstreams()["m01"].Addrs[0] = "10.255.255.255"
	if got := topo.GetNodeUpstreams()["m02"].GetAddrs(); !reflect.DeepEqual(got, wantSibling) {
		t.Fatalf("mutating m01's upstream leaked into sibling m02: got %v want %v", got, wantSibling)
	}
}

// TestRootsOnly verifies that a tree without members still builds: every root
// gets an entry and no error is returned.
func TestRootsOnly(t *testing.T) {
	nodes := []*pppv1.Node{
		root("r2", "10.0.0.2"),
		root("r1", "10.0.0.1"),
	}

	topo, err := Build(Options{Tree: tree("t7", 2, 2), Nodes: nodes})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	want := map[string]*pppv1.NodeUpstream{
		"r1": pullFromSource(),
		"r2": memberUpstream("10.0.0.1"),
	}
	if !reflect.DeepEqual(topo.GetNodeUpstreams(), want) {
		t.Fatalf("unexpected upstreams:\n got %v\nwant %v", topo.GetNodeUpstreams(), want)
	}
}

// TestNoRootErrors verifies that a tree without any root node is rejected.
func TestNoRootErrors(t *testing.T) {
	nodes := []*pppv1.Node{
		member("m1", "10.0.0.11"),
		member("m2", "10.0.0.12"),
	}
	if _, err := Build(Options{Tree: tree("t8", 2, 2), Nodes: nodes}); err == nil {
		t.Fatal("expected error for tree without roots, got nil")
	}
}

// TestNilTreeErrors verifies that a nil tree is rejected instead of producing
// a topology with an empty tree id.
func TestNilTreeErrors(t *testing.T) {
	if _, err := Build(Options{Nodes: []*pppv1.Node{root("r1", "10.0.0.1")}}); err == nil {
		t.Fatal("expected error for nil tree, got nil")
	}
}

// TestDuplicateNodeIDErrors verifies that two nodes sharing an ID are rejected
// instead of silently overwriting an entry or duplicating an address.
func TestDuplicateNodeIDErrors(t *testing.T) {
	nodes := []*pppv1.Node{
		root("r0", "10.0.0.1"),
		member("m1", "10.0.0.11"),
		member("m1", "10.0.0.12"), // duplicate ID, different addr
	}
	if _, err := Build(Options{Tree: tree("t9", 2, 2), Nodes: nodes}); err == nil {
		t.Fatal("expected error for duplicate node id, got nil")
	}
}

// TestEmptyAddrErrors verifies that a node with an empty address is rejected.
func TestEmptyAddrErrors(t *testing.T) {
	cases := []struct {
		name  string
		nodes []*pppv1.Node
	}{
		{"root", []*pppv1.Node{root("r0", "")}},
		{"member", []*pppv1.Node{root("r0", "10.0.0.1"), member("m1", "")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Build(Options{Tree: tree("t10", 2, 2), Nodes: tc.nodes}); err == nil {
				t.Fatal("expected error for empty node addr, got nil")
			}
		})
	}
}

// TestEmptyIDErrors verifies that a node without an ID is rejected.
func TestEmptyIDErrors(t *testing.T) {
	nodes := []*pppv1.Node{
		root("r0", "10.0.0.1"),
		{Addr: "10.0.0.11"}, // empty id
	}
	if _, err := Build(Options{Tree: tree("t11", 2, 2), Nodes: nodes}); err == nil {
		t.Fatal("expected error for empty node id, got nil")
	}
}

// TestRootCountExceededErrors verifies that more roots than Tree.RootCount are
// rejected.
func TestRootCountExceededErrors(t *testing.T) {
	tr := tree("t12", 2, 2)
	tr.RootCount = 3
	nodes := []*pppv1.Node{
		root("r1", "10.0.0.1"),
		root("r2", "10.0.0.2"),
		root("r3", "10.0.0.3"),
		root("r4", "10.0.0.4"),
	}
	if _, err := Build(Options{Tree: tr, Nodes: nodes}); err == nil {
		t.Fatal("expected error when root count exceeds root_count, got nil")
	}
}

// TestRootCountBoundary verifies that exactly RootCount roots is allowed and
// RootCount<=0 imposes no limit.
func TestRootCountBoundary(t *testing.T) {
	threeRoots := []*pppv1.Node{
		root("r1", "10.0.0.1"),
		root("r2", "10.0.0.2"),
		root("r3", "10.0.0.3"),
	}

	tr := tree("t13", 2, 2)
	tr.RootCount = 3
	if _, err := Build(Options{Tree: tr, Nodes: threeRoots}); err != nil {
		t.Fatalf("expected success with exactly root_count roots, got %v", err)
	}

	trNoLimit := tree("t14", 2, 2)
	trNoLimit.RootCount = 0
	if _, err := Build(Options{Tree: trNoLimit, Nodes: threeRoots}); err != nil {
		t.Fatalf("expected success with root_count=0 (no limit), got %v", err)
	}
}

// TestDefaultsApplied verifies that zero group sizes fall back to the package
// defaults and the build completes (a broken default could hang on a <=0
// group size). 40 members with groupSize 16 yield 3 groups, all under the
// root group with default groupChildren 8.
func TestDefaultsApplied(t *testing.T) {
	var nodes []*pppv1.Node
	nodes = append(nodes, root("r0", "10.0.0.1"))
	for i := 1; i <= 40; i++ {
		nodes = append(nodes, member("m"+strconv.Itoa(i), "10.0.0."+strconv.Itoa(10+i)))
	}

	topo, err := Build(Options{Tree: tree("t15", 0, 0), Nodes: nodes})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	if len(topo.GetNodeUpstreams()) != 41 {
		t.Fatalf("expected 41 entries, got %d", len(topo.GetNodeUpstreams()))
	}
	for id, up := range topo.GetNodeUpstreams() {
		if id == "r0" {
			continue
		}
		if len(up.GetAddrs()) != 1 {
			t.Fatalf("member %s upstream = %v, want exactly [root addr]", id, up.GetAddrs())
		}
		if up.GetPullFromSource() {
			t.Fatalf("member %s PullFromSource = true, want false", id)
		}
	}
	if up := topo.GetNodeUpstreams()["r0"]; len(up.GetAddrs()) != 0 || !up.GetPullFromSource() {
		t.Fatalf("primary root = addrs %v PullFromSource %v, want empty + true", up.GetAddrs(), up.GetPullFromSource())
	}
}

// TestDegenerateGroupSizes verifies that groupSize=1 and groupChildren=1 build
// without panicking; the result is a chain root->G1->G2->...->G5.
func TestDegenerateGroupSizes(t *testing.T) {
	var nodes []*pppv1.Node
	nodes = append(nodes, root("r0", "10.0.0.1"))
	for i := 1; i <= 5; i++ {
		nodes = append(nodes, member(fmt.Sprintf("m%02d", i), "10.0.0."+strconv.Itoa(10+i)))
	}

	topo, err := Build(Options{Tree: tree("t16", 1, 1), Nodes: nodes})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	want := map[string]*pppv1.NodeUpstream{
		"r0":  pullFromSource(),
		"m01": memberUpstream("10.0.0.1"),
		"m02": memberUpstream("10.0.0.11"),
		"m03": memberUpstream("10.0.0.12"),
		"m04": memberUpstream("10.0.0.13"),
		"m05": memberUpstream("10.0.0.14"),
	}
	if !reflect.DeepEqual(topo.GetNodeUpstreams(), want) {
		t.Fatalf("unexpected upstreams:\n got %v\nwant %v", topo.GetNodeUpstreams(), want)
	}
}

// TestUnspecifiedRoleIsMember documents that a node without an explicit role
// is treated as a member (the proto zero value defaults to member).
func TestUnspecifiedRoleIsMember(t *testing.T) {
	nodes := []*pppv1.Node{
		root("r0", "10.0.0.1"),
		{Id: "m1", Addr: "10.0.0.11"}, // Role left as ROLE_UNSPECIFIED.
	}

	topo, err := Build(Options{Tree: tree("t17", 2, 2), Nodes: nodes})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	up, ok := topo.GetNodeUpstreams()["m1"]
	if !ok {
		t.Fatal("unspecified-role node missing from topology; want it treated as member")
	}
	if !reflect.DeepEqual(up, memberUpstream("10.0.0.1")) {
		t.Fatalf("unspecified-role node upstream = %v, want [root addr]", up)
	}
}
