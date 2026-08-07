# PPP — P2P 分发服务设计（v2）

> 状态：DRAFT v2（已按用户 7 项确认更新）
> 目标仓库：github.com/akzj/ppp（**greenfield，完全脱离 sirius，仅借鉴现有设计**）

## 0. 用户确认的关键决策
| 项 | 决策 |
|----|------|
| Tree 规模 | 单 tree < 5000 节点 |
| Tree 维度 | `{app, env, idc}`（无 zone） |
| 节点归属 | 一个 node 只属于一个 tree（简化状态空间） |
| 控制面 | `ppp-ctl-server` 中心化部署，**支持高可用** |
| 边缘服务 | `ppp-service`（每节点独立下载服务进程） |
| 协议 | **gRPC** |
| 拓扑 | 每 idc 存在 **root 1-3 个（可配置）**：root 先从源（OSS/S3/HTTP/HTTPS）下载，再在 root 节点间同步 |
| 与 sirius | 完全不依赖，仅借鉴设计概念 |
| 取消 | **持久化"禁止分发"列表**：ctl 维护取消文件列表 → 树级分发到 ppp-service → 请求时门控拒绝（最终一致性） |
| 数据面回源 | 支持**透传**与**创建子任务下载完整文件**两种模式；机房分发默认子任务模式 |
| 取消粒度 | 不按 piece 计数；会话级订阅租约（per-child per-job）辅助 |

## 1. 总体架构

```
┌───────────────────────────────────────────────────────┐
│              ppp-ctl-server (中心控制面, HA)            │
│  - TreeManager : Tree{app,env,idc} 注册/生命周期       │
│  - Membership  : 节点注册/心跳 (node → 唯一 tree)      │
│  - Topology    : 每 tree 计算多root分发拓扑+父地址表    │
│  - JobManager  : 分发任务 创建/进度/取消(cancelJob)    │
│  - Push        : 拓扑/任务/取消 下发到节点 (gRPC)       │
└───────────────────────────────────────────────────────┘
        ▲ 注册/心跳/任务/取消 (gRPC)
        │
┌───────┴───────────────────────────────────────┐
│            ppp-service (边缘数据节点)            │
│  - 属于唯一 tree                                 │
│  - 从父节点拉 piece (gRPC GetPiece)              │
│  - 为子节点提供 piece (gRPC)                     │
│  - 本地缓存: mmap(.cds.pieces) + bbolt index     │
│  - 每 (tree,file) downloader + 会话订阅租约     │
└────────────────────────────────────────────────┘
        ▲ 叶子应用/服务 经 SDK (gRPC) 拉取
```

### 每 tree 的拓扑形态（多 root）
```
                      源 (OSS/S3/HTTP/HTTPS)
                          │
              ┌───────────┼───────────┐
           [root1]     [root2]     [root3]      ← root 组(1-3, 可配置)
              │  └───────┼───────┘  │           ← root 间互相同步
              │                    │
           [层2组]  [层2组] ... [层2组]          ← 中间层（组内互为备份）
              │                    │
           [叶子] ...           [叶子]           ← <5000 节点/tree
```
- **root 组**：每 tree 1-3 个 root（可配置）。root 从源拉取；root 间互为上游（同步/容灾）。
- **非 root 层**：沿用现有 `UpstreamGraph`（GroupMembers/GroupChildren）算法思想，从 sirius 抽出为共享算法包，按 tree 独立计算。

## 2. 核心抽象

### 2.1 Tree
```go
type Tree struct {
    ID           string    // 全局唯一: app + "/" + env + "/" + idc
    App          string
    Environment  string    // dev|beta|pre|prod
    Idc          string
    RootCount    int       // root 节点数, 默认 1, 1-3
    GroupMembers int       // 每组节点数 (非root层)
    GroupChildren int      // 每组子组数
    Source       Source    // 源: OSS/S3/HTTP/HTTPS
}
```
- 内容命名空间：`(TreeID, Filename)` 唯一确定一份内容。

### 2.2 Node（ppp-service 注册信息）
```go
type Node struct {
    ID    string
    Addr  string        // gRPC 地址
    Tree  TreeID        // 节点唯一归属
    Role  Role          // root | member
    Labels map[string]string
}
```

### 2.3 Job（分发任务）
```go
type Job struct {
    ID       string
    TreeID   TreeID
    Filename string
    Size     int64
    MD5      string
    Source   Source       // 根节点拉源用
    State    JobState     // created|distributing|success|failed|canceled
    Targets  []NodeID     // 需要下载的节点集合(可选, 默认全树)
}
```
- 创建：`CreateJob` → 控制面通知 root 组拉源 → 沿拓扑分发。
- 取消：`CancelJob(JobID)` → ctl 写入持久化 banned list → 树级分发到 ppp-service → 请求时门控拒绝（详见 §3）。

**jobID 的两类来源（用户关键澄清：扩容场景本地生成）**：
- **中心生成**：编排式分发任务（CreateJob）由 ctl 分配 JobID。
- **本地生成**：节点自发起下载（**扩容机器注册 → 更新 upstreams → 回源**）由节点本地生成 JobID（UUID），非中心分配。
- **取消键 = (TreeID, Filename)，不依赖 JobID**：banned list 按文件级生效，无论 jobID 来自中心还是本地，命中即拒绝 → 统一覆盖编排任务与扩容回源。

## 3. 取消机制（持久化禁止分发列表 + 会话订阅租约）

### 3.1 为什么需要订阅模型（用户问题解答）
订阅模型解决的是"**一个上游节点的下载器到底该不该停**"。

场景：Job 把文件 F 分发给 B1 及其子节点 C1、C2。
```
[root] ── [B1] ──┬── [C1]
                 └── [C2]
```
- B1 为 F 建一个 Downloader（从 root 拉 piece）。它同时服务 C1、C2。
- C1、C2 各自在开始下载 F 时**订阅一次**（按子节点，不按 piece）：C1 订阅 → B1 订阅数 =1；C2 订阅 → =2。
- 如果 C1 取消/完成并退订，但 C2 还在下载 → B1 **不能停**（否则 C2 断了），订阅数 =1。
- 只有当 **所有订阅者都取消/完成**（订阅数=0）且无 local need → B1 的 downloader 才停，并告诉 root"我不再需要 F" → root 对应订阅数也 -1。

**这正是现有代码缺失的**：旧代码 C1 请求 piece 触发 B1 现建 downloader，C1 取消后没人通知 B1 → B1 永远下载 F（"取消的任务持续存在"）。



### 3.2 主取消机制：持久化"禁止分发"列表（用户定案：只能持久化实现）
用户定案：取消必须**持久化**，不依赖瞬时广播。机制如下：

1. **ppp-ctl-server 维护取消文件列表（banned list）**：持久化记录 `(TreeID, Filename, bannedAt, reason, jobID)`。取消即把该文件加入列表（`CancelJob(JobID)` → 标记 job canceled + 写入 banned list）。
2. **树级分发**：banned list 像拓扑一样**按树分发**到所有 ppp-service（ctl 推送增量 + 节点重连全量拉取 + **本地持久化**，带版本号/generation 检测变更）。节点重启后重新同步，仍保持拒绝。
3. **请求时门控**：ppp-service 收到下游 piece/下载请求时，**先查本地 banned list**：命中 → 直接**返回错误（禁止分发）**，不服务、不触发回源、不激活任何下载任务。
4. **下载器门控**：已运行的 downloader 收到列表更新 → 立即停止对应 (TreeKey) 的下载与清理。
5. **最终一致性**：列表异步收敛；即使个别节点尚未同步，漏网的请求也会在**上游已同步节点处被拦截**（逐级拒绝）→ 链路收敛，任务停止。

**为什么这修复"取消的任务持续存在"**：叶子请求 → 上游节点先查 banned list → 命中即拒绝 → 不会因 child need 回源激活任务；已激活的下载器也因列表更新而停止。**请求被"禁分发"门控拦在源头**，不需要逐跳的取消传播，更不需要 per-request 活性查询。

### 3.3 会话订阅租约（辅助机制，不按 piece 计数）
**按 piece 引用计数不可行**：100GB / 4MB ≈ 25,000 piece/节点；一个中间节点若服务 20 个子节点，经过它的 piece 请求达 50 万级，逐 piece 计数是百万级原子操作，不可接受。

**改为"子节点会话租约"（per-child per-job，不是 per-piece）**：
- 子节点 C 开始下载 (tree,file,jobID) 时，向父节点 B **订阅一次**：`Subscribe(tree,file,jobID,childID)`。
- B 的 downloader 订阅数 = **正在下载该文件的子节点数**（由拓扑扇出决定，通常 <20），而不是 piece 数。
- C 通过**心跳/租约续期**（TTL）维持订阅；C 完成或取消 → 退订（或租约过期自动摘除）。
- downloader 存活条件 = `订阅数 > 0` 或 `local need`；banned/job canceled 时全部清空。
- 单文件单节点订阅操作量 ≈ 子节点数（几十次），不是百万级。

订阅租约只用于辅助场景：**纯中转节点**（本节点无 local need、仅为子节点下载）在最后一个订阅者离开后提前停止下载，不浪费带宽。

### 3.4 崩溃容错
- banned list 本地持久化 + 重连全量同步 → 节点重启后仍拒绝已取消文件（**持久化取消的可靠保证**）。
- 订阅租约 TTL/心跳：子节点失联 X 秒自动摘除 → 防僵尸下载（节点崩溃时无法退订）。

## 3.5 数据面回源模式（用户澄清）

ppp-service 为下游提供 piece；**本地没有数据时向上回源，支持两种模式**：
- **透传（pass-through）**：下游 piece 请求直接中继到上游，数据流回下游，**不本地落盘**。适用于纯中转/本节点不需要该文件的场景。
- **创建子任务下载完整文件（full-download subtask）**：本节点为该 (tree,file) 建 downloader 从上游拉完整文件，本地缓存后再服务下游。**机房分发场景默认此模式**——因为 idc 内所有节点大概率都需要同一份文件（不然就不需要 P2P），子任务既服务下游又满足本节点自身需求。

**need 模型（下载器存活条件）**：一个 (tree,file) downloader 的"需要方"有两类——
- **local need**：本节点是该 Job 的目标（自己需要这文件）
- **child need**：为服务下游子节点而启动（子任务）

任一 need 存在则 downloader 存活；cancelJob 到达时**清空该 JobID 的全部 need → 下载器停止**（含 child need 触发的子任务，这正是"取消的任务持续存在"的修复）；仅某个子节点取消时只摘除对应 child need，不影响 local need 与其它子节点。

## 4. 数据节点（ppp-service）重设计

### 4.1 状态与存储
```go
type TreeKey struct {
    TreeID   TreeID
    Filename string
}

type Service struct {
    tree        TreeCtx            // 节点唯一归属 tree: 上游addrs、root组、配置
    downloaders map[TreeKey]*Downloader
    pieceFiles  map[TreeKey]*PieceFile
    // 存储: DownloadPath/<TreeID>/<filename> (.cds.pieces/.cds.index 沿用)
}
```
- 因节点只属于一个 tree，`tree` 可单例；但状态键仍用 TreeKey（统一、防未来多 tree）。

### 4.2 per-task ctx（关键修复）
- downloader 的 ctx 从 per-(tree, job/task) 派生，不再用服务级 ctx。
- 取消 = cancel downloader ctx + 订阅摘除 + 清理 PieceFile 状态。

### 4.3 取消清理
- Stop 时：取消在途协程、移除 downloader、明确 PieceFile 保留/删除策略（保留则重开，不复用已关闭对象）。
- 修复旧代码：closePieceFileRoutine 只回收 exist=true → 增加对已关闭未完成 entry 的回收（防 use-after-unmap）。
- 进度：ppp-service 持 ground truth，按 (tree_id, job_id, node_id, filename) upsert 上报 ctl。

## 5. 控制面（ppp-ctl-server）设计

- **HA**：多实例 + leader 选举（raft/etcd 语义），单实例也可运行。
- **Topology**：`UpstreamGraph` 算法从 sirius 抽出为共享包；按 tree 计算：root 组（1-3 root 互为上游）+ 非 root 分组；结果存内存 + 定期重算（节点上下线触发）。
- **Push**：拓扑变更/任务下发/**banned list 增量** → gRPC 推送节点（节点重连全量拉取）。
- **JobManager**：状态机 created→distributing→success/failed/canceled；进度聚合（节点 upsert 上报）；**CancelJob → 标记 canceled + 写 banned list + 推送列表**。
- **对外 API**（gRPC）：
```
rpc CreateJob(Job) -> Job
rpc QueryJob(JobID) -> Job
rpc CancelJob(JobID) -> Job
rpc ListJobs(...)
rpc RegisterNode(Node)
rpc Heartbeat(Node)
rpc WatchTopology(TreeID) -> stream TopologyUpdate
rpc WatchBannedList(TreeID) -> stream BannedListUpdate   // 增量
rpc SyncBannedList(TreeID, gen) -> BannedList             // 重连全量
rpc SyncProgress(stream ProgressRecord)
```

## 6. 数据面协议（gRPC，节点间 + 叶子）
```
rpc GetPiece(GetPieceRequest) -> Piece
  GetPieceRequest { tree, filename, index, size, from[]hop, job_id }
rpc GetFile / DownloadFile(DownloadFileRequest) -> stream ProgressState
// 取消不靠逐跳 RPC：由 banned list 驱动（见 §3.2）
// 命中 banned 的 GetPiece/DownloadFile 返回 ErrBannedDistribution
```
- `from[]hop` 防环（沿用 From 思想，携带 nodeID + jobID）：**仅用于环检测**——任一节点若发现自己的 nodeID 已在请求的 from 链中，直接拒绝（LOOP_DETECTED）。订阅/退订不经过 from 链，走显式 `Subscribe`/`Unsubscribe` 会话租约（见 §3.3）。
- piece 传输：unary 每 4MB piece；可后续优化为 streaming。

## 7. 与现有代码的关系
- **完全 greenfield**：新仓库 ppp 独立实现。
- **借鉴**：piece 分片/crc64 校验/mmap+bbolt 缓存/From 防环/UpstreamGraph 分组算法。
- 旧 tengen 的 ppp 与 sirius 不再演进。

## 8. 实现阶段规划（待确认后启动）
- 阶段 0：仓库骨架 + proto（ctl + data）+ 共享算法包（UpstreamGraph 移植）
- 阶段 1：ppp-ctl-server（Tree/Membership/Topology/JobManager + HA）
- 阶段 2：ppp-service 数据面（TreeKey 状态、per-task ctx、GetPiece、下载器+会话订阅租约）
- 阶段 3：banned list（ctl 持久化 + 树级分发 + 请求门控）+ 订阅租约 + 崩溃容错
- 阶段 4：存储（mmap+bbolt 移植）+ 进度上报 + 清理修复
- 阶段 5：端到端集成测试（多 root、取消、崩溃恢复）

## 9. 遗留小问题（已提给用户）
1. root 组同步模型：primary-only（1 个 root 拉源，其余 root 从 root 组内拉，省源带宽，推荐） vs all（每个 root 都拉源，并行快但源带宽×N）？
2. 存储层是否完全移植旧 mmap+bbolt 方案（推荐）？
3. 叶子拉取也走 gRPC GetPiece？是否需要 HTTP 兼容 shim？
