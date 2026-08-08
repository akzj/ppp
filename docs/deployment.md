# PPP Deployment Reference

This document is the operational reference for `ppp-ctl-server` and `ppp-service`.
For the design, see [design-v2.md](design-v2.md); for a quick start, see the
[README](../README.md).

## Binaries

| Binary | Purpose |
|--------|---------|
| `cmd/ppp-ctl-server` | Control plane: gRPC Control service, durable PostgreSQL state, topology / jobs / banned-list fan-out. Multiple instances share one PG; a PG lease elects exactly one leader (no raft). |
| `cmd/ppp-service` | Edge data node: registers with the ctl, serves the Data gRPC service, downloads files from the source or upstream peers into a local piece store. |

## `ppp-ctl-server` flags

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `:9090` | gRPC listen address. Use `127.0.0.1:PORT` or `:PORT`. |
| `-http-addr` | `:9091` | `/leader` health-check listen address (200 leader / 503 follower). |
| `-pg-dsn` | `postgres://ppp:ppp@127.0.0.1:25433/ppp` | PostgreSQL DSN (the ctl's primary store). |
| `-instance-id` | `ctl-1` | Control-plane instance id (used in leader election). |
| `-leader-lease` | `10s` | Leader lease duration; after expiry another instance may take over. |
| `-leader-renew` | `2s` | Leader lease renewal interval. |
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
| `-heartbeat-interval` | `5s` | Heartbeat cadence to the ctl. |
| `-download-concurrency` | `4` | Max concurrent piece fetches per file. |
| `-lease-ttl` | `30s` | Session-lease duration for downstream `Subscribe` calls. |

Both binaries accept `-h` for the full flag list.

## Control plane storage & leader election (PostgreSQL)

The ctl's primary store is PostgreSQL (phase 7); the previous bbolt backend was
removed. Proto records are stored as JSONB. Tables are created automatically at
startup (`CREATE TABLE IF NOT EXISTS`):

```
trees(tree_id text PK, data jsonb)
nodes(tree_id text, node_id text, data jsonb, PK(tree_id, node_id))
jobs(job_id text PK, tree_id text, data jsonb)
banned(tree_id text, filename text, data jsonb, PK(tree_id, filename))
progress(tree_id, job_id, filename, node_id, data jsonb, PK(tree_id, job_id, filename, node_id))
meta(tree_id text, key text, value bigint, PK(tree_id, key))   -- topology_gen / banned_gen
ctl_leader(singleton_id int PK CHECK (singleton_id=1), instance_id text, lease_until timestamptz)
```

Generation counters are incremented atomically
(`INSERT ... ON CONFLICT DO UPDATE SET value = meta.value + 1 RETURNING value`),
safe across concurrent instances sharing one PG.

**Leader election (PG lease, no raft):** each instance renews the singleton
`ctl_leader` row:
`UPDATE ctl_leader SET instance_id=$1, lease_until=now()+lease WHERE singleton_id=1 AND (instance_id=$1 OR lease_until < now()) RETURNING instance_id`.
The instance whose UPDATE returns a row is the leader; the first row is
bootstrapped with `INSERT ... ON CONFLICT DO NOTHING`. The leader renews every
`-leader-renew`; on exit/crash the lease expires within `-leader-lease` and
another instance takes over.

**Leader duties** (follower mutations/watch calls return `Unavailable` — an LB
should route only to the leader): mutation RPCs, the watch streams (topology /
banned / jobs fan-out), heartbeat liveness pruning, and topology computation +
generation bumps. Read-only single-shot queries (GetTree/ListTrees/
ListBanned/QueryJob/ListJobs/SyncProgress) may be served by any instance. Expose
the `/leader` HTTP endpoint (via `-http-addr`) to the LB/VIP so it routes to the
current leader (200) and away from followers (503).

**Docker PG development** (the test suite and the e2e use it):

```sh
docker run -d --name ppp-pg -p 127.0.0.1:25433:5432 -e POSTGRES_USER=ppp -e POSTGRES_PASSWORD=ppp -e POSTGRES_DB=ppp postgres:16
psql -h 127.0.0.1 -p 25433 -U ppp -d ppp <<'SQL'
CREATE DATABASE ppp_test;        -- ctl unit + server tests
CREATE DATABASE ppp_test_agent;  -- agent integration tests (in-process ctl)
CREATE DATABASE ppp_test_e2e;    -- real multi-process e2e
SQL
```

Each test package uses its **own** database (they run in parallel under
`go test ./...`), truncates its tables before each test, and **skips** when
PostgreSQL is unreachable (see the test comments), so the suite is not red on
machines without PG.

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

## gRPC message size

A 4 MiB piece plus protobuf framing exceeds gRPC's 4 MiB default limit. The Data
server and peer/test clients set a 16 MiB max message size. Custom clients must do
the same.

## TLS / mTLS

All gRPC links can be protected with mutual TLS (phase 8): ctl ↔ service (control plane),
service ↔ service (piece fetch / subscribe) and the orchestrator/leaf client. This phase does
**CA mutual verification** — every endpoint verifies the peer's certificate against the configured
CA. Identity-based authorization (which node may do what) is a later concern.

**Flags** (both binaries accept the same four):

| Flag | Meaning |
|------|---------|
| `-tls-ca` | CA certificate file (PEM). Trust anchor for peers and client-CA for servers. |
| `-tls-cert` | This node's certificate (PEM). |
| `-tls-key` | This node's private key (PEM). |
| `-tls-server-name` | The name a **client** verifies on the server certificate (agent only; the ctl only serves). |

When all TLS flags are empty the link stays **plaintext** (development compatibility; nothing
changes for existing deployments). Setting `-tls-ca` alone with `-tls-cert/-tls-key` enables mTLS.

**Topology:** the ctl server requires and verifies a client certificate; every `ppp-service`
presents its certificate to the ctl and to its peers, and verifies the ctl/peer server name
(`-tls-server-name`, e.g. the certificate CN/SAN). Orchestrator/leaf clients present their own
certificate and verify the service they dial. The `/leader` HTTP health endpoint stays plaintext
(it only returns 200/503 for an LB/VIP).

**Generating certificates** (example with openssl; the tests generate an in-memory PKI with
crypto/x509 + ECDSA):

```sh
# CA
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -keyout ca.key -out ca.pem \
  -days 365 -nodes -subj "/CN=ppp-ca" -addext "basicConstraints=critical,CA:TRUE"

# ctl server cert (ServerAuth, SAN localhost/127.0.0.1)
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -keyout ctl.key -out ctl.csr -nodes \
  -subj "/CN=ppp-ctl"
openssl x509 -req -in ctl.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out ctl.pem -days 365 \
  -extfile <(printf "extendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1")

# node cert (ServerAuth + ClientAuth, SAN localhost/127.0.0.1)
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -keyout node.key -out node.csr -nodes \
  -subj "/CN=ppp-node"
openssl x509 -req -in node.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out node.pem -days 365 \
  -extfile <(printf "extendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1")

# orchestrator client cert (ClientAuth)
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -keyout orch.key -out orch.csr -nodes \
  -subj "/CN=ppp-orch"
openssl x509 -req -in orch.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out orch.pem -days 365 \
  -extfile <(printf "extendedKeyUsage=clientAuth")
```

Run the ctl with `-tls-ca ca.pem -tls-cert ctl.pem -tls-key ctl.key`; run each service with
`-tls-ca ca.pem -tls-cert node.pem -tls-key node.key -tls-server-name localhost`; orchestrator
clients connect with the CA + their client cert and verify `localhost`.

**`-tls-server-name` ops note (P2):** the single flag constrains **both** the ctl client and the
peer client's server-certificate SANs. An **empty** value does **not** disable verification — gRPC
then verifies the **dialed address's hostname**. Since this system normally dials by IP
(`addr` is `IP:port`), the two sensible configurations are:

- **Leave `-tls-server-name` empty** and put the dialed IPs in the server certificates as IP SANs
  (e.g. `subjectAltName=IP:127.0.0.1`), or
- **Give every server the same DNS name** (e.g. `subjectAltName=DNS:ppp.internal`) and set
  `-tls-server-name ppp.internal` on every client.

A cert that matches neither the configured name nor the dialed IP is rejected.

## Identity authorization (certificate roles)

mTLS proves *who* the peer is (the CA signed its certificate) but not *what it may do*: any
certificate signed by the CA could claim any role. Phase 10 closes that gap by binding an
**identity role** to the certificate's **Subject OU** field and having each server reject callers
whose role is not allowed.

**Role convention** (one OU value per certificate):

| Role | Carried by |
|------|-----------|
| `ctl` | the control-plane operator/leader certificate |
| `service` | ppp agent/peer certificates |
| `client` | orchestrator/leaf SDK certificates |

**Scope — a coarse per-connection gate.** The interceptor is a **per-connection** gate, not a
per-RPC one: once a caller's role is in the allowed list it may call **any** RPC of that service.
The intended separation (service for RegisterNode/Subscribe + peer Data, client for
CreateTree/CreateJob + leaf Data) is not enforced method-by-method — e.g. with the gate on, a
`service` certificate can also call `CreateTree`/`CreateJob`. **Per-RPC method-level authorization
is left for a later phase.** Do not rely on the role to restrict which specific RPCs a caller may
invoke.

**Flag** (both binaries): `-tls-require-role <comma-separated roles>`

- Set, e.g. `-tls-require-role service,client`: the server installs unary + streaming role
  interceptors and rejects any caller whose certificate OU is not in the list with
  **PermissionDenied** (gRPC code 7; the message includes the caller's role).
- Unset (default): mTLS keeps working exactly as before — **no role check**. Certificates without
  an OU (older deployments) are fully compatible.
- Plaintext (no TLS flags): nothing to check — calls pass through even if the flag is set. The
  server logs a **WARNING** at startup in that case ("role authorization is NOT enforced"), so a
  misconfigured deployment cannot silently believe the gate is active.

Example with the openssl commands above, adding the OU to each `-subj`:

```sh
# ctl server cert (no role needed — it only serves)
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -keyout ctl.key -out ctl.csr -nodes \
  -subj "/CN=ppp-ctl"
# node cert with the service role
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -keyout node.key -out node.csr -nodes \
  -subj "/CN=ppp-node/OU=service"
# orchestrator cert with the client role
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -keyout orch.key -out orch.csr -nodes \
  -subj "/CN=ppp-orch/OU=client"
```

Run the ctl with `-tls-require-role service,client` and each service with
`-tls-require-role service,client`; orchestrator/leaf clients connect with a `client`-OU
certificate. A certificate with any other OU (or none) is rejected at the RPC boundary.

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
