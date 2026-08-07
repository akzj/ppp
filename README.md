# PPP — P2P Distribution Service

PPP is a self-organizing P2P distribution system for machine-room / multi-datacenter
file delivery. A small control plane (**ppp-ctl-server**) maintains per-tree topology,
jobs and ban state; edge nodes (**ppp-service**) form a tree per logical tree and fetch
files from an HTTP/HTTPS/S3/OSS source or from upstream peers, piece by piece.

```
                     ┌──────────────────────┐
                     │    ppp-ctl-server    │   topology, jobs, banned list
                     │  (one per deployment)│
                     └──────────┬───────────┘
                     register / │ watch
                     heartbeat   │ topology / jobs / bans
      ┌─────────────────────────┼──────────────────────────┐
      ▼                         ▼                          ▼
 ┌─────────┐              ┌─────────┐                ┌─────────┐
 │ root #1 │◄────────────►│ root #2 │  (primary root │  root #n │
 │ (primary│               │ (peers  │   pulls from  │          │
 │  pulls  │               │  with   │   the source) │          │
 │  from   │               │  roots) │                │          │
 │ source) │               └────┬────┘                └────┬─────┘
 └────┬────┘                    │  GetPiece / Subscribe    │
      │                         ▼                          ▼
      │                  ┌─────────────┐             ┌─────────────┐
      └─────────────────►│ member group│────────────►│ child groups│
                          │  (layer 1)  │             │  (layer 2+) │
                          └─────────────┘             └─────────────┘
```

- **Tree identity**: `tree = app/env/idc`; every tree has an independent topology and
  namespace, so different applications or environments never share state.
- **Distribution**: the primary root (lowest node ID) pulls a file from the source; every
  other node fetches from its upstreams via `GetPiece` (on-demand, full-file subtask
  download, loop-prevented by a hop chain) or is triggered by a center job.
- **Control**: `CreateJob` pushes a file to the whole tree (the root downloads from the
  source); `CancelJob` bans the file everywhere (neither served nor fetched); `Unban`
  restores it. Bans are persisted locally on every node, so a restarted node keeps
  rejecting during the restart window before the ctl re-syncs.

## Quick start

Build the two binaries:

```sh
go build -o bin/ppp-ctl-server ./cmd/ppp-ctl-server
go build -o bin/ppp-service ./cmd/ppp-service
```

Start the control plane (a free port, a fresh bbolt DB):

```sh
bin/ppp-ctl-server -addr 127.0.0.1:9001 -db /tmp/ppp/ctl.db
```

Create a tree with one root, two members per group and two child groups per group,
whose source is an HTTP server:

```json
CreateTreeRequest {
  tree: {
    id: "myapp/dev/idc1",
    root_count: 1, group_members: 2, group_children: 2,
    source: { type: HTTP, urls: ["http://127.0.0.1:8080"] }
  }
}
```

Start the services (role `root` or `member`; `-role` is required to form the topology):

```sh
bin/ppp-service -id root-1 -addr 127.0.0.1:9101 -ctl-addr 127.0.0.1:9001 \
    -tree myapp/dev/idc1 -role root -download-path /tmp/ppp/root-1
bin/ppp-service -id m-1 -addr 127.0.0.1:9102 -ctl-addr 127.0.0.1:9001 \
    -tree myapp/dev/idc1 -role member -download-path /tmp/ppp/m-1
```

Push a file to the tree and cancel/unban it:

```json
CreateJobRequest  { tree_id: "myapp/dev/idc1", filename: "app.tar", size: 1048576100 }
CancelJobRequest  { tree_id: "myapp/dev/idc1", filename: "app.tar" }   // bans it
UnbanRequest      { tree_id: "myapp/dev/idc1", filename: "app.tar" }   // restores it
```

The complete request/response wiring (including `GetPiece`, `WatchTopology`,
`WatchJobs`) is exercised end-to-end against real binaries in
[`internal/e2e`](internal/e2e/e2e_test.go).

## Directory layout

```
cmd/ppp-ctl-server/   control-plane binary (gRPC + bbolt)
cmd/ppp-service/      edge data-node binary (Data gRPC + piece store)
internal/agent/       node implementation: downloader, lease manager, piece store,
                      banned list, HTTP/S3 sources
internal/ctl/         control plane: registry, fan-out, jobs, banned list, bbolt store
internal/e2e/         real-multi-process end-to-end tests
pkg/topology/         deterministic tree-topology builder (ported idea from tengen)
proto/ + gen/         protobuf definitions and generated Go code
docs/design-v2.md     design document (v2)
docs/deployment.md    deployment reference
```

## Capabilities

- Self-organizing tree topology (roots, member groups, child groups), deterministic
  and generation-versioned; nodes reconnect and refresh on any change.
- Piece-level distribution over gRPC with session leases (`Subscribe`/`Unsubscribe`)
  so upstreams stop fetching when downstreams stop needing.
- HTTP/HTTPS and S3/OSS sources with mirror failover (see `docs/deployment.md`).
- Unified sparse-file piece store (one hole file per file + bbolt index,
  `pwrite`/`pread`, no mmap) with crash recovery: a mid-download resumes from
  the persisted index after a restart.
- Cancel/ban/unban with per-node local persistence (`banned.db`), effective even in
  the restart window before the ctl sync.

## Known limitations

- **No TLS**: all gRPC is plaintext (`insecure.NewCredentials`). Do not expose the
  ports beyond a trusted network.
- **OSS/S3 credentials** come from the environment (`AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`); there is no per-source credential store.
- **Job-driven downloads are root-only**: members pull on demand via `GetPiece`
  (the `watchJobsLoop` is implemented for roots).
- **In-progress files are not evicted from the store's open-cache** on silent stop: a
  downloader that stops without completing leaves its `.cds.pieces` handle open until
  the agent stops (completed files are idle-evicted after 60 s).
- **Single-instance ctl**: no HA/replication of the control plane yet.
- **Progress reporting is best-effort** (the ctl persists the latest per-node report;
  no delivery guarantees).
- **`-count=N` race runs need memory headroom**: the workspace CI limit is a hard
  4 GB `ulimit -v`; the race detector alone reserves ~2.2 GB, so repeated in-process
  iterations (`go test -race -count=N`) can exhaust it. See `docs/deployment.md`.
- **Transparent pass-through back-to-source is not implemented** (a node always
  downloads into its local piece store before serving).

## License

MIT — see [LICENSE](LICENSE).
