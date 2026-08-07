# PPP Deployment Reference

This document is the operational reference for `ppp-ctl-server` and `ppp-service`.
For the design, see [design-v2.md](design-v2.md); for a quick start, see the
[README](../README.md).

## Binaries

| Binary | Purpose |
|--------|---------|
| `cmd/ppp-ctl-server` | Control plane: gRPC Control service, durable bbolt state, topology / jobs / banned-list fan-out. Single instance. |
| `cmd/ppp-service` | Edge data node: registers with the ctl, serves the Data gRPC service, downloads files from the source or upstream peers into a local piece store. |

## `ppp-ctl-server` flags

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `:9090` | gRPC listen address. Use `127.0.0.1:PORT` or `:PORT`. |
| `-db` | `ppp-ctl.db` | bbolt database file path (durable state: trees, nodes, jobs, banned list, progress). |
| `-heartbeat-timeout` | `30s` | How long a node may stay silent before it is pruned and its topology entry removed. |
| `-group-members` | `16` | Default members per group for trees created without explicit `GroupMembers`. |
| `-group-children` | `8` | Default child groups per group for trees created without explicit `GroupChildren`. |

## `ppp-service` flags

| Flag | Default | Description |
|------|---------|-------------|
| `-id` | hostname | Node id registered with the control plane. |
| `-addr` | `:0` | Data gRPC listen address; **also the address advertised to peers**. In production use a reachable address. |
| `-ctl-addr` | (required) | Control plane gRPC address, e.g. `127.0.0.1:9001`. |
| `-tree` | (required) | Tree id this node belongs to, e.g. `app/env/idc`. |
| `-role` | `member` | `root` or `member`. **Required for the topology**: a root registers with role ROOT; members are leaves/middle layers. |
| `-download-path` | `./ppp-data` | Directory for the local piece store. |
| `-store` | `mmap` | Deprecated: `file` and `mmap` both select the single unified sparse-file store (kept for compatibility). |
| `-heartbeat-interval` | `5s` | Heartbeat cadence to the ctl. |
| `-download-concurrency` | `4` | Max concurrent piece fetches per file. |
| `-lease-ttl` | `30s` | Session-lease duration for downstream `Subscribe` calls. |

Both binaries accept `-h` for the full flag list.

## Topology parameters

A tree is created with:

- `root_count` (1–3, enforced by the ctl): number of root nodes.
- `group_members`: nodes per non-root group.
- `group_children`: child groups per group.

The topology is deterministic and rebuilt on every node registration/heartbeat/prune.
Rules:

- The **primary root** is the root with the **lowest node ID**; it pulls from the
  source (`PullFromSource=true`, empty upstream list).
- Non-primary roots peer with each other (their upstream list = the other roots'
  addresses), so roots back each other up.
- Every member group's upstream = its parent group's node addresses; members fetch
  on demand via `GetPiece` (full-file subtask download).
- A missing root is a legitimate transient state: it yields an empty topology
  (every node gets an empty upstream) so subscribers can detect the broken link.

## Piece source conventions

The tree (or job) `Source` message:

- `type`: `HTTP` / `HTTPS` / `OSS` / `S3`.
- `urls[]`: mirror list; the first reachable mirror is used (failover in order).
  - HTTP/HTTPS: each url is a direct piece URL (fetched with a `Range` header).
  - S3/OSS: **`urls[0]` is the custom BaseEndpoint** for OSS/MinIO/S3-compatible
    stores. When empty, the region's default AWS endpoint is used.
- `bucket` + `key`: the S3/OSS object.
- `region`: optional; falls back to the environment.

S3/OSS credentials come from the environment: `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. Requests use **path-style** addressing (`/bucket/key`) with
`GetObject` + a `Range` header, mirroring the HTTP source's piece-window model.

## Storage layout

The store is **tree-agnostic**: download paths contain only the file basename, never
the tree/app/env/idc (tree identity is a control-plane concept; the data plane
rejects requests for a different tree). Filenames are enforced to be safe
basenames (no path separators, not `.`/`..`).

The store is a single unified **sparse-file** implementation (no mmap, no
per-piece files): one hole file per file written with `pwrite` and read with
`pread`, plus a bbolt index.

```
<download-path>/<basename>            final file after MarkComplete
<download-path>/<basename>.cds.pieces sparse piece data (one file; missing pieces = holes)
<download-path>/<basename>.cds.index  bbolt index: piece index -> PieceInfo
```

- On open: if `<basename>` exists the file is complete and opened read-only
  (every piece present); otherwise the `.cds.pieces` + `.cds.index` pair is
  opened and the existence map is rebuilt from the bbolt index, so a
  crash mid-download resumes.
- `MarkComplete` flushes, unmaps, drops the index and **renames `.cds.pieces` to
  `<basename>`** — the final file is the real, readable artifact.
- Completed files idle longer than 60 s are evicted from the in-memory cache
  (unmapped, reopened on demand). In-progress files stay open while the downloader
  is active.
- `banned.db` (bbolt) in the same download path persists the local banned list;
  it is loaded on startup so a restarted node rejects banned files during the
  restart window, before the ctl re-syncs.
- `ResolvePath` (Data RPC) returns the final `local_path` and whether the file is
  currently present.

The `file` store (`-store file`) keeps one readable file per piece:
`<download-path>/<basename>_<index>.piece` plus `<basename>.meta` holding the
total size.

## gRPC message size

A 4 MiB piece plus protobuf framing exceeds gRPC's 4 MiB default limit. The Data
server and peer/test clients set a 16 MiB max message size. Custom clients must do
the same.

## Memory and CI notes

- The test suite is green with `go test ./...` and
  `go test -race -count=1 ./internal/agent/ ./internal/ctl/ ./internal/e2e/`.
- **`-count=N` race runs need memory headroom.** The workspace CI limit is a hard
  `ulimit -v` of 4 GB (cannot be raised in-process). The race detector alone
  reserves ~2.2 GB of virtual address space, and Go accumulates arena reservations
  across in-process `-count=N` iterations, so repeated race iterations of the agent
  package can exceed the cap with `mmap: cannot allocate memory`. Recommendation:
  - Use `-count=1` for race runs (the reliable gate), or
  - raise `ulimit -v` (e.g. `ulimit -v unlimited`) on machines that permit it
    before `go test -race -count=N`, or
  - run `-count=N` without `-race`.
  - Note: the race detector's shadow allocation can still flake (~1 in 10
    single-pass runs) under this tight cap because TSan's shadow and the Go
    arena collide in the constrained address space; the code is race-clean
    (0 data races, suite green).
  - The residual flake is also bounded by the thread limit (`ulimit -u`, 4096
    on this machine): TSan maps one OS thread per goroutine, so a shared,
    near-limit thread budget can push allocation over the edge. Raise it if
    permitted, or keep repeated race runs at `-count ≤ 3`.

## Known limitations (deployment-relevant)

- No TLS — all gRPC is plaintext; keep the ports on a trusted network.
- OSS/S3 credentials are environment-only.
- Job-driven downloads are root-only; members pull on demand via `GetPiece`.
- The control plane is single-instance (no HA yet).
- Progress reporting is best-effort.
- Transparent pass-through back-to-source is not implemented.
