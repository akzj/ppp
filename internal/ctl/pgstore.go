package ctl

import (
	"context"
	"errors"
	"fmt"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// pgStore implements Store on PostgreSQL (the ctl's primary store, phase 7).
// Proto messages are stored as JSONB; generation counters live in the meta
// table and are incremented atomically (INSERT ... ON CONFLICT DO UPDATE ...
// RETURNING), which is safe across concurrent instances sharing one PG.
type pgStore struct {
	pool *pgxpool.Pool
}

// meta keys for generation counters.
const (
	metaTopologyGenKey = "topology_gen"
	metaBannedGenKey   = "banned_gen"
)

// migrateSchema creates the tables if they do not exist (a simple startup
// migration; the ctl is greenfield so there is no data migration).
func MigrateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS trees (
			tree_id text PRIMARY KEY,
			data jsonb NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS nodes (
			tree_id text NOT NULL,
			node_id text NOT NULL,
			data jsonb NOT NULL,
			PRIMARY KEY (tree_id, node_id)
		)`,
		`CREATE TABLE IF NOT EXISTS jobs (
			job_id text PRIMARY KEY,
			tree_id text NOT NULL,
			data jsonb NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS banned (
			tree_id text NOT NULL,
			filename text NOT NULL,
			data jsonb NOT NULL,
			PRIMARY KEY (tree_id, filename)
		)`,
		`CREATE TABLE IF NOT EXISTS progress (
			tree_id text NOT NULL,
			job_id text NOT NULL,
			filename text NOT NULL,
			node_id text NOT NULL,
			data jsonb NOT NULL,
			PRIMARY KEY (tree_id, job_id, filename, node_id)
		)`,
		`CREATE TABLE IF NOT EXISTS meta (
			tree_id text NOT NULL,
			key text NOT NULL,
			value bigint NOT NULL,
			PRIMARY KEY (tree_id, key)
		)`,
		`CREATE TABLE IF NOT EXISTS ctl_leader (
			singleton_id int PRIMARY KEY CHECK (singleton_id = 1),
			instance_id text NOT NULL,
			lease_until timestamptz NOT NULL
		)`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			return fmt.Errorf("ctl: migrate schema: %w", err)
		}
	}
	return nil
}

// pgStoreOpTimeout bounds every store operation (pool acquire + query
// round-trip) so a hung PostgreSQL cannot block a control-plane RPC forever.
// statement_timeout bounds the query on the server side; this bounds the
// client side too.
const pgStoreOpTimeout = 15 * time.Second

// opCtx returns a bounded context for a single store operation.
func (s *pgStore) opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), pgStoreOpTimeout)
}

// OpenPGStore connects to PostgreSQL and ensures the schema exists. The pool
// is configured so a hung database cannot pin a connection forever:
// statement_timeout on the server, bounded acquire/query timeouts on the
// client, and connection recycling (max lifetime / idle) to drop stale
// connections.
func OpenPGStore(ctx context.Context, dsn string) (*pgStore, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("ctl: parse pg dsn: %w", err)
	}
	// Server-side: abort a statement that runs longer than 10s.
	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	// Client-side: bound acquire/query timeouts and recycle connections.
	poolCfg.MaxConns = 16
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnLifetimeJitter = 5 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	poolCfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("ctl: connect pg: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ctl: ping pg: %w", err)
	}
	if err := MigrateSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &pgStore{pool: pool}, nil
}

// Close releases the connection pool.
func (s *pgStore) Close() error {
	s.pool.Close()
	return nil
}

// Pool returns the underlying connection pool (used by leader election).
func (s *pgStore) Pool() *pgxpool.Pool { return s.pool }

func jsonMarshal(m proto.Message) ([]byte, error) {
	return protojson.Marshal(m)
}

func jsonUnmarshal(data []byte, m proto.Message) error {
	return protojson.Unmarshal(data, m)
}

// ============ Trees ============

func (s *pgStore) CreateTree(t *pppv1.Tree) error {
	ctx, cancel := s.opCtx()
	defer cancel()

	if t == nil || t.GetId() == "" {
		return errors.New("ctl: tree id is required")
	}
	data, err := jsonMarshal(t)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO trees(tree_id, data) VALUES ($1, $2::jsonb) ON CONFLICT (tree_id) DO NOTHING`,
		t.GetId(), string(data))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrExists
	}
	return nil
}

func (s *pgStore) GetTree(id string) (*pppv1.Tree, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	if id == "" {
		return nil, errors.New("ctl: tree id is empty")
	}
	var raw string
	err := s.pool.QueryRow(ctx, `SELECT data FROM trees WHERE tree_id = $1`, id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	t := &pppv1.Tree{}
	if err := jsonUnmarshal([]byte(raw), t); err != nil {
		return nil, err
	}
	return t, nil
}

func (s *pgStore) ListTrees() ([]*pppv1.Tree, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	rows, err := s.pool.Query(ctx, `SELECT data FROM trees ORDER BY tree_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pppv1.Tree
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		t := &pppv1.Tree{}
		if err := jsonUnmarshal([]byte(raw), t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *pgStore) DeleteTree(id string) error {
	ctx, cancel := s.opCtx()
	defer cancel()

	if id == "" {
		return errors.New("ctl: tree id is empty")
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM trees WHERE tree_id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteTreeData cascade-removes every record of a tree without touching the
// tree record itself.
func (s *pgStore) DeleteTreeData(treeID string) error {
	ctx, cancel := s.opCtx()
	defer cancel()

	if treeID == "" {
		return errors.New("ctl: tree id is required")
	}
	// Atomic cleanup: all four deletes commit or roll back together so a
	// failure cannot leave a partially-cleared tree.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, q := range []string{
		`DELETE FROM nodes WHERE tree_id = $1`,
		`DELETE FROM jobs WHERE tree_id = $1`,
		`DELETE FROM banned WHERE tree_id = $1`,
		`DELETE FROM progress WHERE tree_id = $1`,
	} {
		if _, err := tx.Exec(ctx, q, treeID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// ============ Nodes ============

func (s *pgStore) PutNode(n *pppv1.Node) error {
	ctx, cancel := s.opCtx()
	defer cancel()

	if n.GetId() == "" || n.GetTreeId() == "" {
		return errors.New("ctl: node id and tree_id are required")
	}
	data, err := jsonMarshal(n)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO nodes(tree_id, node_id, data) VALUES ($1, $2, $3::jsonb)
		 ON CONFLICT (tree_id, node_id) DO UPDATE SET data = EXCLUDED.data`,
		n.GetTreeId(), n.GetId(), string(data))
	return err
}

func (s *pgStore) DeleteNode(treeID, nodeID string) error {
	ctx, cancel := s.opCtx()
	defer cancel()

	if treeID == "" || nodeID == "" {
		return errors.New("ctl: node tree_id and id are required")
	}
	_, err := s.pool.Exec(ctx,
		`DELETE FROM nodes WHERE tree_id = $1 AND node_id = $2`, treeID, nodeID)
	return err
}

func (s *pgStore) ListNodes(treeID string) ([]*pppv1.Node, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT data FROM nodes WHERE ($1 = '' OR tree_id = $1) ORDER BY node_id`, treeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pppv1.Node
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		n := &pppv1.Node{}
		if err := jsonUnmarshal([]byte(raw), n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// ============ Generations ============

func (s *pgStore) TopologyGeneration(treeID string) (int64, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	return s.getGen(ctx, treeID, metaTopologyGenKey)
}

func (s *pgStore) BannedGeneration(treeID string) (int64, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	return s.getGen(ctx, treeID, metaBannedGenKey)
}

func (s *pgStore) getGen(ctx context.Context, treeID, key string) (int64, error) {
	var v int64
	err := s.pool.QueryRow(ctx,
		`SELECT value FROM meta WHERE tree_id = $1 AND key = $2`, treeID, key).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return v, nil
}

// bumpGen atomically increments a generation counter (INSERT 1 when missing).
func (s *pgStore) bumpGen(ctx context.Context, treeID, key string) (int64, error) {
	var v int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO meta(tree_id, key, value) VALUES ($1, $2, 1)
		 ON CONFLICT (tree_id, key) DO UPDATE SET value = meta.value + 1
		 RETURNING value`,
		treeID, key).Scan(&v)
	return v, err
}

func (s *pgStore) BumpTopologyGeneration(treeID string) (int64, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	return s.bumpGen(ctx, treeID, metaTopologyGenKey)
}

// ============ Banned list ============

func (s *pgStore) AddBanned(b *pppv1.BannedFile) (int64, bool, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	if b.GetTreeId() == "" || b.GetFilename() == "" {
		return 0, false, errors.New("ctl: banned tree_id and filename are required")
	}
	data, err := jsonMarshal(b)
	if err != nil {
		return 0, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Atomic insert guard: exactly one concurrent caller inserts the row; the
	// losers get no row back and report already=true (no SELECT-then-INSERT
	// TOCTOU race, so concurrent AddBanned on the same (tree, file) never
	// hits a unique violation).
	var inserted string
	err = tx.QueryRow(ctx,
		`INSERT INTO banned(tree_id, filename, data) VALUES ($1, $2, $3::jsonb)
		 ON CONFLICT (tree_id, filename) DO NOTHING
		 RETURNING filename`,
		b.GetTreeId(), b.GetFilename(), string(data)).Scan(&inserted)
	switch {
	case err == nil:
		gen, err := s.bumpGenTx(ctx, tx, b.GetTreeId(), metaBannedGenKey)
		if err != nil {
			return 0, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, false, err
		}
		return gen, false, nil
	case errors.Is(err, pgx.ErrNoRows):
		// Already present: return the current generation, consistent within
		// this transaction with the "already exists" state.
		gen, err := s.getGenTx(ctx, tx, b.GetTreeId(), metaBannedGenKey)
		if err != nil {
			return 0, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, false, err
		}
		return gen, true, nil
	default:
		return 0, false, err
	}
}

func (s *pgStore) RemoveBanned(treeID, filename string) (int64, bool, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	if treeID == "" || filename == "" {
		return 0, false, errors.New("ctl: banned tree_id and filename are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Idempotent remove: DELETE ... RETURNING removes the row when present and
	// bumps the generation only when a removal actually happened; an absent
	// row is not an error.
	var deleted string
	err = tx.QueryRow(ctx,
		`DELETE FROM banned WHERE tree_id = $1 AND filename = $2 RETURNING filename`,
		treeID, filename).Scan(&deleted)
	switch {
	case err == nil:
		gen, err := s.bumpGenTx(ctx, tx, treeID, metaBannedGenKey)
		if err != nil {
			return 0, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, false, err
		}
		return gen, true, nil
	case errors.Is(err, pgx.ErrNoRows):
		gen, err := s.getGenTx(ctx, tx, treeID, metaBannedGenKey)
		if err != nil {
			return 0, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, false, err
		}
		return gen, false, nil
	default:
		return 0, false, err
	}
}

// getGenTx reads a generation counter inside a transaction (0 when missing).
func (s *pgStore) getGenTx(ctx context.Context, tx pgx.Tx, treeID, key string) (int64, error) {
	var v int64
	err := tx.QueryRow(ctx, `SELECT value FROM meta WHERE tree_id = $1 AND key = $2`, treeID, key).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return v, nil
}

// bumpGenTx atomically increments a generation counter inside a transaction.
func (s *pgStore) bumpGenTx(ctx context.Context, tx pgx.Tx, treeID, key string) (int64, error) {
	var v int64
	err := tx.QueryRow(ctx,
		`INSERT INTO meta(tree_id, key, value) VALUES ($1, $2, 1)
		 ON CONFLICT (tree_id, key) DO UPDATE SET value = meta.value + 1
		 RETURNING value`,
		treeID, key).Scan(&v)
	return v, err
}

func (s *pgStore) GetBanned(treeID, filename string) (*pppv1.BannedFile, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	var raw string
	err := s.pool.QueryRow(ctx,
		`SELECT data FROM banned WHERE tree_id = $1 AND filename = $2`, treeID, filename).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	b := &pppv1.BannedFile{}
	if err := jsonUnmarshal([]byte(raw), b); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *pgStore) ListBanned(treeID string) ([]*pppv1.BannedFile, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT data FROM banned WHERE tree_id = $1 ORDER BY filename`, treeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pppv1.BannedFile
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		b := &pppv1.BannedFile{}
		if err := jsonUnmarshal([]byte(raw), b); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ============ Jobs ============

func (s *pgStore) CreateJob(j *pppv1.Job) error {
	ctx, cancel := s.opCtx()
	defer cancel()

	if j.GetId() == "" {
		return errors.New("ctl: job id is empty")
	}
	data, err := jsonMarshal(j)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO jobs(job_id, tree_id, data) VALUES ($1, $2, $3::jsonb) ON CONFLICT (job_id) DO NOTHING`,
		j.GetId(), j.GetTreeId(), string(data))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrExists
	}
	return nil
}

func (s *pgStore) GetJob(id string) (*pppv1.Job, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	if id == "" {
		return nil, errors.New("ctl: job id is empty")
	}
	var raw string
	err := s.pool.QueryRow(ctx, `SELECT data FROM jobs WHERE job_id = $1`, id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	j := &pppv1.Job{}
	if err := jsonUnmarshal([]byte(raw), j); err != nil {
		return nil, err
	}
	return j, nil
}

func (s *pgStore) UpdateJob(j *pppv1.Job) error {
	ctx, cancel := s.opCtx()
	defer cancel()

	if j.GetId() == "" {
		return errors.New("ctl: job id is empty")
	}
	data, err := jsonMarshal(j)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE jobs SET data = $1::jsonb, tree_id = $2 WHERE job_id = $3`,
		string(data), j.GetTreeId(), j.GetId())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *pgStore) ListJobs() ([]*pppv1.Job, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	rows, err := s.pool.Query(ctx, `SELECT data FROM jobs ORDER BY job_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pppv1.Job
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		j := &pppv1.Job{}
		if err := jsonUnmarshal([]byte(raw), j); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *pgStore) JobsByFile(treeID, filename string) ([]*pppv1.Job, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT data FROM jobs WHERE tree_id = $1 AND data->>'filename' = $2 ORDER BY job_id`,
		treeID, filename)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pppv1.Job
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		j := &pppv1.Job{}
		if err := jsonUnmarshal([]byte(raw), j); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// ============ Progress ============

func (s *pgStore) UpsertProgress(p *pppv1.ProgressState, nodeID string) error {
	ctx, cancel := s.opCtx()
	defer cancel()

	if p == nil || p.GetTreeId() == "" || p.GetFilename() == "" || nodeID == "" {
		return errors.New("ctl: progress tree_id, filename and node_id are required")
	}
	rec := &pppv1.ProgressRecord{State: p, NodeId: nodeID}
	data, err := jsonMarshal(rec)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO progress(tree_id, job_id, filename, node_id, data) VALUES ($1, $2, $3, $4, $5::jsonb)
		 ON CONFLICT (tree_id, job_id, filename, node_id) DO UPDATE SET data = EXCLUDED.data`,
		p.GetTreeId(), p.GetJobId(), p.GetFilename(), nodeID, string(data))
	return err
}

func (s *pgStore) ListProgress(treeID string) ([]*pppv1.ProgressState, error) {
	ctx, cancel := s.opCtx()
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT data FROM progress WHERE ($1 = '' OR tree_id = $1) ORDER BY tree_id, node_id`, treeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pppv1.ProgressState
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		r := &pppv1.ProgressRecord{}
		if err := jsonUnmarshal([]byte(raw), r); err != nil {
			return nil, err
		}
		out = append(out, r.GetState())
	}
	return out, rows.Err()
}
