package ctl

import (
	"context"
	"errors"
	"sync"
	"testing"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testPGDSN is the PostgreSQL DSN used by the ctl tests (Docker PG, see
// docs/deployment.md). Tests skip when the database is unreachable so the
// suite is not red on machines without PG.
const testPGDSN = "postgres://ppp:ppp@127.0.0.1:25433/ppp_test"

// openTestPool opens a raw pgx pool on the test database (skips when
// unreachable); used by leader-election and failover tests.
func openTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("PostgreSQL not reachable at %s (%v); skipping", testPGDSN, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestPGStoreGenerationAtomic verifies concurrent generation bumps never lose
// increments (the atomic INSERT ... ON CONFLICT DO UPDATE ... RETURNING).
func TestPGStoreGenerationAtomic(t *testing.T) {
	st := newTestStore(t)
	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := st.BumpTopologyGeneration("t1"); err != nil {
				t.Errorf("bump: %v", err)
			}
		}()
	}
	wg.Wait()
	gen, err := st.TopologyGeneration("t1")
	if err != nil {
		t.Fatalf("generation: %v", err)
	}
	if gen != n {
		t.Fatalf("generation = %d, want %d (no lost increments)", gen, n)
	}
}

// newTestStore opens a clean PG-backed store, truncating the shared tables so
// every test starts isolated. It skips the test when PostgreSQL is
// unreachable.
func newTestStore(t *testing.T) Store {
	t.Helper()
	st, err := OpenPGStore(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("PostgreSQL not reachable at %s (%v); skipping (see docs/deployment.md)", testPGDSN, err)
	}
	t.Cleanup(func() { st.Close() })
	if _, err := st.Pool().Exec(context.Background(),
		`TRUNCATE trees, nodes, jobs, banned, progress, meta, ctl_leader RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return st
}

func testTree(id string) *pppv1.Tree {
	return &pppv1.Tree{Id: id, App: "app", Environment: "prod", Idc: "idc1", RootCount: 3}
}

func testNode(id, treeID, addr string, role pppv1.Node_Role) *pppv1.Node {
	return &pppv1.Node{Id: id, Addr: addr, TreeId: treeID, Role: role, LastHeartbeat: 100}
}

func testJob(id, treeID, filename string, state pppv1.Job_JobState) *pppv1.Job {
	return &pppv1.Job{Id: id, TreeId: treeID, Filename: filename, State: state, CreatedAt: 100, UpdatedAt: 100}
}

// TestStoreTreeCRUD covers create/get/list/delete of trees.
func TestStoreTreeCRUD(t *testing.T) {
	st := newTestStore(t)

	if err := st.CreateTree(testTree("t1")); err != nil {
		t.Fatalf("CreateTree: %v", err)
	}
	// Duplicate create is rejected.
	if err := st.CreateTree(testTree("t1")); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate CreateTree = %v, want ErrExists", err)
	}
	// Empty id is rejected.
	if err := st.CreateTree(&pppv1.Tree{}); err == nil {
		t.Fatal("CreateTree with empty id: want error, got nil")
	}

	got, err := st.GetTree("t1")
	if err != nil {
		t.Fatalf("GetTree: %v", err)
	}
	if got.GetId() != "t1" || got.GetRootCount() != 3 {
		t.Fatalf("GetTree = %v, want id=t1 root_count=3", got)
	}

	if _, err := st.GetTree("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetTree(missing) = %v, want ErrNotFound", err)
	}

	_ = st.CreateTree(testTree("t2"))
	trees, err := st.ListTrees()
	if err != nil {
		t.Fatalf("ListTrees: %v", err)
	}
	if len(trees) != 2 {
		t.Fatalf("ListTrees len = %d, want 2", len(trees))
	}

	if err := st.DeleteTree("t1"); err != nil {
		t.Fatalf("DeleteTree: %v", err)
	}
	if _, err := st.GetTree("t1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetTree after delete = %v, want ErrNotFound", err)
	}
	if err := st.DeleteTree("t1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteTree(missing) = %v, want ErrNotFound", err)
	}
}

// TestStoreNodeCRUD covers node persistence and per-tree listing.
func TestStoreNodeCRUD(t *testing.T) {
	st := newTestStore(t)

	if err := st.PutNode(testNode("n1", "t1", "10.0.0.1", pppv1.Node_ROOT)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := st.PutNode(testNode("n2", "t1", "10.0.0.2", pppv1.Node_MEMBER)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := st.PutNode(testNode("n3", "t2", "10.0.0.3", pppv1.Node_ROOT)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	// Missing id/tree is rejected.
	if err := st.PutNode(&pppv1.Node{}); err == nil {
		t.Fatal("PutNode with empty fields: want error, got nil")
	}

	nodes, err := st.ListNodes("t1")
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("ListNodes(t1) len = %d, want 2", len(nodes))
	}
	nodes, err = st.ListNodes("t2")
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("ListNodes(t2) len = %d, want 1", len(nodes))
	}
	// Empty tree id lists every node (startup registry reload).
	all, err := st.ListNodes("")
	if err != nil {
		t.Fatalf("ListNodes(\"\"): %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("ListNodes(\"\") len = %d, want 3", len(all))
	}

	if err := st.DeleteNode("t1", "n1"); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	nodes, err = st.ListNodes("t1")
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("ListNodes(t1) after delete len = %d, want 1", len(nodes))
	}
}

// TestStoreGeneration verifies topology and banned generations increment.
func TestStoreGeneration(t *testing.T) {
	st := newTestStore(t)

	if gen, _ := st.TopologyGeneration("t1"); gen != 0 {
		t.Fatalf("initial topology gen = %d, want 0", gen)
	}
	g1, err := st.BumpTopologyGeneration("t1")
	if err != nil {
		t.Fatalf("BumpTopologyGeneration: %v", err)
	}
	g2, err := st.BumpTopologyGeneration("t1")
	if err != nil {
		t.Fatalf("BumpTopologyGeneration: %v", err)
	}
	if g1 != 1 || g2 != 2 {
		t.Fatalf("topology gens = %d,%d, want 1,2", g1, g2)
	}
	// Per-tree counters are independent.
	if gen, _ := st.TopologyGeneration("t2"); gen != 0 {
		t.Fatalf("t2 topology gen = %d, want 0", gen)
	}

	if gen, _ := st.BannedGeneration("t1"); gen != 0 {
		t.Fatalf("initial banned gen = %d, want 0", gen)
	}
	gen, already, err := st.AddBanned(&pppv1.BannedFile{TreeId: "t1", Filename: "a.bin", Reason: "test"})
	if err != nil {
		t.Fatalf("AddBanned: %v", err)
	}
	if already || gen != 1 {
		t.Fatalf("AddBanned = (gen %d, already %v), want (1, false)", gen, already)
	}
	// Re-adding the same file does not bump the generation.
	gen, already, err = st.AddBanned(&pppv1.BannedFile{TreeId: "t1", Filename: "a.bin"})
	if err != nil {
		t.Fatalf("AddBanned: %v", err)
	}
	if !already || gen != 1 {
		t.Fatalf("re-AddBanned = (gen %d, already %v), want (1, true)", gen, already)
	}

	gen, removed, err := st.RemoveBanned("t1", "a.bin")
	if err != nil {
		t.Fatalf("RemoveBanned: %v", err)
	}
	if !removed || gen != 2 {
		t.Fatalf("RemoveBanned = (gen %d, removed %v), want (2, true)", gen, removed)
	}
	// Removing a missing file does not bump the generation.
	gen, removed, err = st.RemoveBanned("t1", "a.bin")
	if err != nil {
		t.Fatalf("RemoveBanned: %v", err)
	}
	if removed || gen != 2 {
		t.Fatalf("re-RemoveBanned = (gen %d, removed %v), want (2, false)", gen, removed)
	}
}

// TestStoreBannedList verifies banned CRUD and per-tree isolation.
func TestStoreBannedList(t *testing.T) {
	st := newTestStore(t)

	if _, err := st.GetBanned("t1", "a.bin"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetBanned(missing) = %v, want ErrNotFound", err)
	}

	b := &pppv1.BannedFile{TreeId: "t1", Filename: "a.bin", Reason: "mistake", JobId: "job:1"}
	if _, _, err := st.AddBanned(b); err != nil {
		t.Fatalf("AddBanned: %v", err)
	}
	got, err := st.GetBanned("t1", "a.bin")
	if err != nil {
		t.Fatalf("GetBanned: %v", err)
	}
	if got.GetReason() != "mistake" || got.GetJobId() != "job:1" {
		t.Fatalf("GetBanned = %v, want reason=mistake job=job:1", got)
	}
	if _, err := st.GetBanned("t2", "a.bin"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetBanned(t2) = %v, want ErrNotFound (tree isolation)", err)
	}

	_, _, _ = st.AddBanned(&pppv1.BannedFile{TreeId: "t1", Filename: "b.bin"})
	_, _, _ = st.AddBanned(&pppv1.BannedFile{TreeId: "t2", Filename: "a.bin"})
	list, err := st.ListBanned("t1")
	if err != nil {
		t.Fatalf("ListBanned: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("ListBanned(t1) len = %d, want 2", len(list))
	}
}

// TestStoreJobs verifies job CRUD and per-file lookup.
func TestStoreJobs(t *testing.T) {
	st := newTestStore(t)

	if err := st.CreateJob(testJob("job:1", "t1", "a.bin", pppv1.Job_CREATED)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := st.CreateJob(testJob("job:1", "t1", "a.bin", pppv1.Job_CREATED)); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate CreateJob = %v, want ErrExists", err)
	}
	if err := st.CreateJob(testJob("job:2", "t1", "b.bin", pppv1.Job_DISTRIBUTING)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := st.CreateJob(testJob("job:3", "t2", "a.bin", pppv1.Job_CREATED)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	got, err := st.GetJob("job:1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.GetFilename() != "a.bin" {
		t.Fatalf("GetJob filename = %q, want a.bin", got.GetFilename())
	}
	if _, err := st.GetJob("job:missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetJob(missing) = %v, want ErrNotFound", err)
	}

	got.State = pppv1.Job_CANCELED
	if err := st.UpdateJob(got); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	got2, _ := st.GetJob("job:1")
	if got2.GetState() != pppv1.Job_CANCELED {
		t.Fatalf("job state after update = %v, want CANCELED", got2.GetState())
	}

	byFile, err := st.JobsByFile("t1", "a.bin")
	if err != nil {
		t.Fatalf("JobsByFile: %v", err)
	}
	if len(byFile) != 1 || byFile[0].GetId() != "job:1" {
		t.Fatalf("JobsByFile(t1,a.bin) = %v, want [job:1]", ids(byFile))
	}

	all, err := st.ListJobs()
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("ListJobs len = %d, want 3", len(all))
	}
}

// TestStoreProgressUpsert verifies progress records are accepted and the
// latest per (tree, job, filename, node) is kept.
func TestStoreProgressUpsert(t *testing.T) {
	st := newTestStore(t)

	// Invalid inputs are rejected.
	if err := st.UpsertProgress(&pppv1.ProgressState{}, "n1"); err == nil {
		t.Fatal("UpsertProgress with empty record: want error, got nil")
	}

	p1 := &pppv1.ProgressState{TreeId: "t1", JobId: "job:1", Filename: "a.bin", Progress: 50}
	if err := st.UpsertProgress(p1, "n1"); err != nil {
		t.Fatalf("UpsertProgress: %v", err)
	}
	p2 := &pppv1.ProgressState{TreeId: "t1", JobId: "job:1", Filename: "a.bin", Progress: 80}
	if err := st.UpsertProgress(p2, "n1"); err != nil {
		t.Fatalf("UpsertProgress: %v", err)
	}
	if err := st.UpsertProgress(&pppv1.ProgressState{TreeId: "t1", JobId: "job:1", Filename: "a.bin", Progress: 10}, "n2"); err != nil {
		t.Fatalf("UpsertProgress: %v", err)
	}
	// A different job is a distinct key.
	if err := st.UpsertProgress(&pppv1.ProgressState{TreeId: "t1", JobId: "job:2", Filename: "a.bin", Progress: 99}, "n1"); err != nil {
		t.Fatalf("UpsertProgress: %v", err)
	}

	// The upsert keeps the latest per (tree, job, file, node): n1/job:1 is 80.
	all, err := st.ListProgress("t1")
	if err != nil {
		t.Fatalf("ListProgress: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("ListProgress(t1) len = %d, want 3", len(all))
	}
	found := 0
	for _, p := range all {
		if p.GetJobId() == "job:1" && p.GetProgress() == 80 {
			found++
		}
		if p.GetJobId() == "job:2" && p.GetProgress() == 99 {
			found++
		}
		if p.GetProgress() == 10 {
			found++
		}
	}
	if found != 3 {
		t.Fatalf("ListProgress(t1) missing records (found %d), got %v", found, all)
	}
	none, err := st.ListProgress("t2")
	if err != nil {
		t.Fatalf("ListProgress(t2): %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("ListProgress(t2) len = %d, want 0", len(none))
	}
	allAll, err := st.ListProgress("")
	if err != nil {
		t.Fatalf("ListProgress(\"\"): %v", err)
	}
	if len(allAll) != 3 {
		t.Fatalf("ListProgress(\"\") len = %d, want 3", len(allAll))
	}
}

// TestStoreProgressPersistsAcrossRestart verifies progress is durable in
// PostgreSQL: records survive a close/reopen (a new store on the same DB,
// without the per-test truncation).
func TestStoreProgressPersistsAcrossRestart(t *testing.T) {
	// Start from a clean schema so records from other tests do not leak in;
	// the second open must NOT truncate (the data must survive).
	truncatePG(t)
	open := func() Store {
		st, err := OpenPGStore(context.Background(), testPGDSN)
		if err != nil {
			t.Skipf("PostgreSQL not reachable (%v); skipping", err)
		}
		t.Cleanup(func() { st.Close() })
		return st
	}
	st := open()
	if err := st.UpsertProgress(&pppv1.ProgressState{TreeId: "t1", JobId: "job:1", Filename: "a.bin", Progress: 55}, "n1"); err != nil {
		t.Fatalf("UpsertProgress: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st2 := open()
	all, err := st2.ListProgress("t1")
	if err != nil {
		t.Fatalf("ListProgress after restart: %v", err)
	}
	if len(all) != 1 || all[0].GetProgress() != 55 {
		// Note: ListProgress returns the ProgressState; the node id lives in
		// the stored ProgressRecord key.
		t.Fatalf("progress after restart = %v, want [progress 55]", all)
	}
}

func ids(jobs []*pppv1.Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.GetId())
	}
	return out
}
