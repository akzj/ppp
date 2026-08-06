package topology

import (
	"fmt"
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
		"r0":  {Addrs: addrs()},
		"m01": {Addrs: addrs("10.0.0.1")},
		"m02": {Addrs: addrs("10.0.0.1")},
		"m03": {Addrs: addrs("10.0.0.1")},
		"m04": {Addrs: addrs("10.0.0.1")},
		"m05": {Addrs: addrs("10.0.0.11", "10.0.0.12")},
		"m06": {Addrs: addrs("10.0.0.11", "10.0.0.12")},
		"m07": {Addrs: addrs("10.0.0.11", "10.0.0.12")},
		"m08": {Addrs: addrs("10.0.0.11", "10.0.0.12")},
		"m09": {Addrs: addrs("10.0.0.13", "10.0.0.14")},
		"m10": {Addrs: addrs("10.0.0.13", "10.0.0.14")},
	}
	if !reflect.DeepEqual(topo.GetNodeUpstreams(), want) {
		t.Fatalf("unexpected upstreams:\n got %v\nwant %v", topo.GetNodeUpstreams(), want)
	}
}

// TestMultipleRoots covers 3 roots. The primary root (lowest ID) must pull
// directly from the source (empty upstream); the other roots must peer with
// every remaining root; and first-layer members must see all root addresses.
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
		"r1": {Addrs: addrs()},
		"r2": {Addrs: addrs("10.0.0.1", "10.0.0.3")},
		"r3": {Addrs: addrs("10.0.0.1", "10.0.0.2")},
		// Both member groups hang directly under the root group.
		"m1": {Addrs: addrs("10.0.0.1", "10.0.0.2", "10.0.0.3")},
		"m2": {Addrs: addrs("10.0.0.1", "10.0.0.2", "10.0.0.3")},
		"m3": {Addrs: addrs("10.0.0.1", "10.0.0.2", "10.0.0.3")},
		"m4": {Addrs: addrs("10.0.0.1", "10.0.0.2", "10.0.0.3")},
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
		nodes = append(nodes, member("m"+strconv.Itoa(i), "10.0.0."+strconv.Itoa(10+i)))
	}
	opts := Options{Tree: tree("t3", 2, 2), Nodes: nodes}

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

// TestRootsOnly verifies that a tree without members still builds: every root
// gets an entry and no error is returned.
func TestRootsOnly(t *testing.T) {
	nodes := []*pppv1.Node{
		root("r2", "10.0.0.2"),
		root("r1", "10.0.0.1"),
	}

	topo, err := Build(Options{Tree: tree("t4", 2, 2), Nodes: nodes})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	want := map[string]*pppv1.NodeUpstream{
		"r1": {Addrs: addrs()},
		"r2": {Addrs: addrs("10.0.0.1")},
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
	if _, err := Build(Options{Tree: tree("t5", 2, 2), Nodes: nodes}); err == nil {
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

	topo, err := Build(Options{Tree: tree("t6", 0, 0), Nodes: nodes})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	if len(topo.GetNodeUpstreams()) != 41 {
		t.Fatalf("expected 41 entries, got %d", len(topo.GetNodeUpstreams()))
	}
	for id, up := range topo.GetNodeUpstreams() {
		if id != "r0" && len(up.GetAddrs()) != 1 {
			t.Fatalf("member %s upstream = %v, want exactly [root addr]", id, up.GetAddrs())
		}
	}
	if up := topo.GetNodeUpstreams()["r0"]; len(up.GetAddrs()) != 0 {
		t.Fatalf("primary root upstream = %v, want empty", up.GetAddrs())
	}
}

// TestUnspecifiedRoleIsMember documents that a node without an explicit role
// is treated as a member (the proto zero value defaults to member).
func TestUnspecifiedRoleIsMember(t *testing.T) {
	nodes := []*pppv1.Node{
		root("r0", "10.0.0.1"),
		{Id: "m1", Addr: "10.0.0.11"}, // Role left as ROLE_UNSPECIFIED.
	}

	topo, err := Build(Options{Tree: tree("t7", 2, 2), Nodes: nodes})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	up, ok := topo.GetNodeUpstreams()["m1"]
	if !ok {
		t.Fatal("unspecified-role node missing from topology; want it treated as member")
	}
	if !reflect.DeepEqual(up.GetAddrs(), []string{"10.0.0.1"}) {
		t.Fatalf("unspecified-role node upstream = %v, want [root addr]", up.GetAddrs())
	}
}
