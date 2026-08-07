# PPP 文件分发核心流程

> 状态：核心设计方案（后续数据面实现与协议演进的依据）  
> 范围：文件从源站进入 PPP、形成不可变 metadata，并沿 Tree 逐层复制到所有需要该文件的节点。  
> 非目标：本文不把文件目录、文件 metadata 或 piece metadata 放入 ctl。

## 1. 核心结论

PPP 使用 `filename` 作为业务侧文件标识。在同一个 Tree 内，业务必须满足以下约束：

```text
(tree_id, filename) 在有效生命周期内只对应一份内容
```

业务方上传文件到 S3/OSS/HTTP 源时不要求提供 MD5 或其他 digest。文件的第一份 metadata 由 primary root 在首次回源下载时生成；后续节点不重新生成或修改 metadata，只从 upstream 原样复制。

```text
Source
  │ primary root 下载数据并生成第一份 metadata
  ▼
primary root（唯一 metadata 源头）
  │ 复制 data + immutable metadata
  ▼
secondary roots / 第一层节点
  │ 复制 data + immutable metadata
  ▼
后续各层节点 / 扩容节点
```

因此，文件一致性来自以下组合：

1. 第一份 metadata 只由 primary root 根据源文件生成；
2. metadata 完成后封存为不可变对象；
3. 每一层只复制 metadata，不修改它；
4. 节点使用 metadata 中的预期 digest 验证收到的 piece；
5. data 和 metadata 作为同一个 artifact 原子发布；
6. 同 filename 出现不同 metadata identity 时立即报告内容冲突，禁止混写或覆盖。

## 2. 组件职责边界

### 2.1 ppp-ctl-server

ctl 只关心控制面状态：

- Tree 生命周期；
- Node 注册、心跳和存活性；
- 计算并下发 upstream 拓扑；
- 创建、取消和查询分发任务；
- 分发 banned list；
- 接收 best-effort 进度。

ctl **不关心**：

- 文件实际内容；
- 文件或 piece digest；
- metadata 文件；
- 哪个节点缓存了哪些 piece；
- 扩容节点的本地文件期望列表；
- metadata 在节点之间如何复制。

Job 是一次任务编排信号，不是文件 catalog，也不是内容身份。取消仍按 `(tree_id, filename)` 生效，但这不表示 ctl 管理该文件的 metadata。

### 2.2 ppp-service

数据节点负责：

- primary root 从源下载文件并生成第一份 metadata；
- 向 downstream 提供 sealed metadata 和 pieces；
- 从 upstream 原样复制 metadata；
- 根据 metadata 校验每个 piece；
- 管理 staging、断点续传和最终原子发布；
- 检测同 filename 的内容冲突；
- 按订阅租约维护 downloader 生命周期。

### 2.3 业务方或部署系统

业务方仍以 filename 使用文件，并负责声明某个节点需要哪些 filename。

扩容节点注册 ctl 后只能得到 upstream 地址，不会从 ctl 得到文件目录。扩容下载的触发来源可以是：

- 节点本地的期望文件列表；
- 部署系统下发的 filename；
- 节点重启后恢复的未完成下载记录；
- 叶子应用调用 `DownloadFile(filename)`。

得到 filename 后，节点直接向 upstream 获取 metadata 和 pieces，不需要 ctl 参与数据面协商。

## 3. Artifact 与不可变 Metadata

### 3.1 Artifact 定义

一个可对外分发的文件制品定义为：

```text
Artifact = (tree_id, filename, data, sealed metadata)
```

其中 filename 是业务身份，metadata 描述该 filename 当前唯一合法的内容。JobID 只标识下载原因，不属于内容身份；同一 artifact 可以被中心 Job、本地扩容任务或叶子请求复用。

### 3.2 Metadata 内容

建议采用固定版本、确定性编码的二进制格式：

```text
FileMetadataV1 {
    magic             // 格式标识
    version           // metadata 格式版本
    filename
    file_size
    piece_size
    piece_count
    digest_algorithm  // 建议 SHA-256
    file_digest       // 完整文件 digest
    piece_digests[]   // 按 piece index 连续排列的 digest
}
```

`metadata_id` 不写入自身，而是在 metadata 完成编码后计算：

```text
metadata_id = SHA-256(canonical_metadata_bytes)
```

`metadata_id` 用来快速判断两台 upstream 是否提供同一份 metadata；`piece_digests[index]` 是接收 piece 时的预期值；`file_digest` 是最终发布前的整文件校验值。

metadata 必须采用确定性编码：相同字段和 digest 数组必须产生完全相同的字节序列。禁止使用包含不稳定 map 顺序、生成时间、node ID 或 JobID 的编码内容，否则相同文件会产生不同 metadata_id。

### 3.3 Metadata 大小与存储形式

metadata 不按 piece 写成 ctl/数据库记录，而是一个与文件并存的紧凑二进制 sidecar。

当前 piece size 为 4 MiB：

```text
100 GiB / 4 MiB = 25,600 pieces
25,600 * 32-byte SHA-256 = 800 KiB
```

即使未来达到 100 万个 pieces，digest 数组也约为 32 MiB。它相对大文件仍然很小，但不适合塞进单个控制面 RPC 或数据库行。

metadata 通过数据面流式复制并在节点本地落盘。协议应支持 metadata 长度和分块传输，使实现未来可以在不改变语义的情况下加入分页、按范围读取或本地 mmap；无论底层如何优化，对外仍是一份完整、不可变、由 `metadata_id` 标识的字节序列。

### 3.4 BUILDING 与 SEALED

`BUILDING` 是本地 staging 状态，不是可传播 metadata 的一个可变字段：

- `BUILDING`：root 正在下载源文件和生成 digest；禁止对外提供该 artifact；
- `SEALED`：数据、完整 metadata 和 commit marker 已原子发布；只读，可向 downstream 提供。

sealed metadata 不能原地追加、修复或替换。任何变化都必须在新的 staging 中重新构建，验证后再决定是相同内容还是冲突。

## 4. Primary Root 首次回源

### 4.1 准备阶段

ctl 创建 Job 后只通知 root 组。由拓扑确定的 primary root 负责回源；其他 root 不独立从源生成 metadata，而是像普通节点一样从 primary root 复制 artifact。

primary root 对 `(tree_id, filename)` 获取本地互斥锁，并创建与 Job 隔离的 staging：

```text
<download-path>/.staging/<escaped-filename>/<job-id>/data
<download-path>/.staging/<escaped-filename>/<job-id>/metadata.tmp
```

不同 Job 即使 filename 相同，也不能写入同一个 piece 文件或 index。

### 4.2 单遍下载并生成 Metadata

primary root 从 S3/OSS/HTTP 顺序或并发读取 pieces。对每个 piece：

1. 检查实际长度及 piece 边界；
2. 写入 staging data；
3. 计算该 piece 的 SHA-256；
4. 将 digest 按 index 写入 metadata.tmp 的固定位置；
5. 更新完整文件 digest；若采用并发拉取，则最终按文件顺序计算或使用一次顺序读取，不能按 goroutine 完成顺序拼接 digest；
6. 持久化断点恢复所需的 piece index。

源站返回的 ETag、Content-MD5 或 checksum 可以作为附加校验，但不是必需输入，也不能替代 PPP 自己生成的 metadata。

### 4.3 封存与发布

全部 pieces 完成后，primary root 必须依次完成：

1. 验证实际文件大小；
2. 确认所有 piece digest 均已生成；
3. 完成 `file_digest`；
4. 生成确定性 metadata bytes；
5. 计算 `metadata_id`；
6. fsync staging data 与 metadata；
7. 若已有同 filename 的 sealed artifact，执行冲突判断；
8. 原子发布 data、metadata 和 commit marker；
9. 只有发布全部成功后，才允许 `GetFileInfo/GetMetadata/GetPiece` 对外返回该 artifact。

实现不能先设置 complete 再忽略 rename/fsync/metadata 写入错误。任一步失败都保持未发布状态，并向任务报告失败。

### 4.4 Primary Root 故障切换

“唯一 metadata 源头”约束作用于一份 artifact 的生成链，不要求某个固定 node 永远担任 primary root。

- primary root 在 SEALED 前宕机：未完成 staging 只属于该节点，不能被其他节点当作权威 metadata；拓扑选出的新 primary 应先向其 root upstreams 查询同 filename 的 sealed artifact，确认不存在后才重新从源构建；
- primary root 在 SEALED 后宕机：其他已经复制完成的 root 继续提供同一 metadata_id；新 primary 必须优先从它们复制，不能直接回源生成替代 metadata；
- 如果树内同时发现多个 metadata_id：立即进入 `CONTENT_CONFLICT`，不得因 root 排序、Job 时间或多数票选择其中一个；
- 如果源对象在首次构建期间发生变化：当前构建应因 size、piece 或最终 digest 不一致而失败；重试产生的候选 artifact 仍须遵守同 filename 冲突规则。

ctl 只负责通过新拓扑决定谁是 primary 以及 upstream 地址，不参与查找或裁决 metadata。

## 5. 逐层复制流程

### 5.1 协议语义

数据面需要补充三类能力；具体 RPC 名称可以调整，但语义必须保留：

```protobuf
rpc GetFileInfo(GetFileInfoRequest) returns (FileInfo);
rpc GetMetadata(GetMetadataRequest) returns (stream MetadataChunk);
rpc GetPiece(GetPieceRequest) returns (GetPieceResponse);

message FileInfo {
  TreeKey key = 1;
  int64 file_size = 2;
  int64 piece_size = 3;
  int64 piece_count = 4;
  bytes metadata_id = 5;
  int64 metadata_size = 6;
  string digest_algorithm = 7;
}

message MetadataChunk {
  bytes metadata_id = 1;
  int64 offset = 2;
  bytes data = 3;
}
```

`GetPieceRequest` 应携带本次 downloader 已绑定的 `metadata_id`。upstream 只有在本地 sealed artifact 的 metadata_id 相同时才返回 piece；否则返回 `CONTENT_CONFLICT`。Piece 响应可以继续携带快速传输 checksum，但 downstream 是否接受该 piece，最终由本地已验证 metadata 中的预期 digest 决定。

metadata 较大时采用 streaming/chunk，而不是单条 gRPC message。节点先完整写入临时 metadata，计算并确认 metadata_id 后，才将其绑定为本次下载的权威 metadata。后续可优化成可验证分页，但不能改变“复制同一份不可变 metadata”的语义。

### 5.2 Downstream 下载步骤

一个节点需要 filename 时执行：

1. 从当前 upstream 获取 `FileInfo`；
2. 若本地已有 sealed artifact：
   - metadata_id 相同，直接复用；
   - metadata_id 不同，返回 `CONTENT_CONFLICT`；
3. 若本地存在未完成 staging：
   - metadata_id 相同，从已有 pieces 续传；
   - metadata_id 不同，隔离旧 staging，禁止混用；
4. 流式下载 metadata 到临时文件；
5. 校验长度、格式、filename、file size、piece 参数和 metadata_id；
6. 将该 metadata 原样保存为本次 downloader 的只读权威 metadata；
7. 并发请求缺失 pieces；
8. 每个 piece 写盘前计算 digest，并与 `piece_digests[index]` 比较；
9. 不匹配时丢弃该 piece、记录 upstream 故障并尝试其他 upstream；
10. 所有 pieces 完成后计算并校验 `file_digest`；
11. 原子发布本地 artifact，随后才允许继续服务下一层。

节点绝不能根据收到的 piece 重新生成一份新的“权威 metadata”。它可以重新计算 digest 做验证，但落盘和下发的 metadata bytes 必须是 upstream metadata 的原样副本。

### 5.3 切换 Upstream

拓扑更新、超时或 piece 校验失败时，downloader 可以切换 upstream。切换前必须先比较 `FileInfo.metadata_id`：

- 相同：继续复用现有 metadata 和已下载 pieces；
- 不同：拒绝该 upstream 并报告 `CONTENT_CONFLICT`；
- upstream 没有 sealed artifact：尝试其他 upstream，不能要求它临时生成 metadata。

同一组 upstream 中出现不同 metadata_id 是严重一致性告警，不能通过多数投票自动选择一个版本。系统没有业务版本号，自动选择可能把错误内容合法化。

### 5.4 何时可以向下一层提供数据

默认采用完整 artifact 后再分发：节点只有在 data 与 metadata 都 sealed 后才向 downstream 提供它们。这样每一层只有一种简单状态：不存在、构建中、已发布。

未来如需边下载边分发，必须另行设计可验证的增量 metadata、页级封存和失败撤销；不能直接暴露 BUILDING metadata。该优化不属于当前核心流程。

## 6. 初次 Job 与扩容下载

### 6.1 中心 Job

```text
orchestrator -> ctl.CreateJob(tree, filename, source)
ctl          -> root WatchJobs
primary root -> source 下载并生成、seal 第一份 artifact
other roots  -> primary root 复制 artifact
members      -> 各自 upstream 逐层复制 artifact
```

Job 只负责“现在需要分发 filename”的编排。metadata 不写入 Job，也不经 ctl fanout。

如果当前实现仅由 root 消费 WatchJobs，则成员的下载由下游请求、部署系统本地触发或后续独立的任务触发机制启动；无论触发方式如何，文件内容始终通过数据面从 upstream 获取。

### 6.2 扩容节点

```text
new node -> ctl.RegisterNode
ctl      -> new node: topology/upstreams
业务期望状态 -> new node: 需要 filename
new node -> upstream: FileInfo + metadata + pieces
```

扩容不需要重新创建中心 Job，也不需要 ctl 保存历史文件目录。新节点生成本地 JobID，仅用于 downloader、进度和订阅租约；它复制 upstream 已有的 sealed metadata，不产生新的 metadata 源头。

## 7. 同 Filename 内容冲突

filename 是不可变业务身份，而不是允许覆盖的路径。以下情况必须返回明确的 `CONTENT_CONFLICT`：

- 本地已有同 filename，但 metadata_id 不同；
- 两个 upstream 对同 filename 返回不同 metadata_id；
- 新回源候选 artifact 与本地已发布 artifact 的 metadata_id 不同；
- 请求携带的 metadata_id 与被请求节点的 sealed metadata_id 不同；
- metadata 声明的 size/piece 参数与请求或本地 staging 不一致。

冲突发生后：

- 禁止覆盖现有正式文件；
- 禁止把候选 pieces 写入现有 staging；
- 保留隔离的 staging 供诊断或按保留策略清理；
- 当前任务失败并上报清晰原因；
- 不通过修改 metadata、重新编号 pieces 或选择某个 upstream 自动恢复。

业务若确实需要发布不同内容，必须使用新的 filename；这是系统的核心业务约束。

如果已有 artifact，而新 Job 要求重新读取同一源对象以确认内容，primary root 必须在独立 staging 中重新构建候选 metadata，再比较 metadata_id。仅凭 filename、S3 URL 或可变 ETag 不能证明内容相同。若源使用可信、不可变的 object version ID，可在后续优化中复用已验证 artifact，但不能弱化冲突规则。

## 8. 取消、禁止分发与租约

banned gate 的优先级高于所有 metadata 和 piece 操作：

- banned filename 不返回 FileInfo、metadata 或 piece；
- 正在构建或复制的 downloader 立即取消；
- staging 按清理策略删除或隔离；
- 已 sealed 的本地 artifact 是否物理删除由缓存策略决定，但 banned 期间绝不能对外服务；
- Unban 只恢复分发权限，不改变 metadata，也不创建新版本。

订阅租约只决定 downloader 是否仍有 `child need`，不参与内容身份判断。JobID、lease 和 metadata_id 是三个独立维度。

## 9. 崩溃恢复与原子性

节点重启后根据本地状态恢复：

- 有 commit marker 且 data/metadata 完整：作为 sealed artifact 打开；
- 有 staging metadata 且 metadata_id 已验证：按 piece index 断点续传；
- metadata 未完整下载或 metadata_id 未验证：删除临时 metadata并重新获取；
- data pieces 存在但没有已验证 metadata：不得服务或复用，防止把未知内容绑定到 filename；
- metadata 已发布但 data 未原子发布：视为未完成，不能对外服务。

建议的最终布局：

```text
<download-path>/<filename>                 # sealed data
<download-path>/<filename>.cds.metadata    # immutable metadata bytes
<download-path>/<filename>.cds.commit      # metadata_id、格式版本、原子发布标记
```

staging 使用 JobID 或随机实例 ID 隔离。最终发布应优先使用同一文件系统内 rename，并明确崩溃时的提交顺序和恢复规则。commit marker 最后写入；只有它存在且内容匹配时 artifact 才可见。

## 10. Digest 与信任模型

建议 metadata 的 piece digest 和 file digest 使用 SHA-256。当前 CRC64 可以保留为快速传输错误检测，但不能单独承担内容身份和冲突判断。

需要区分：

- `digest(data) == piece response hash` 只能证明响应数据和响应自带 hash 一致；
- `digest(data) == immutable metadata.expected_digest[index]` 才能证明数据符合本次 artifact；
- `SHA-256(metadata bytes) == metadata_id` 证明 metadata 在复制过程中没有改变。

该模型假设已通过 mTLS 接入的 PPP 节点遵循协议，目标是防止源内容变化、节点版本分叉、传输损坏和误混写。若威胁模型包含恶意节点伪造 data 与 metadata，需要由 primary root 对 metadata_id 做数字签名，并让所有节点持有独立信任根；这属于后续安全增强，不应把签名职责放到 ctl 的文件存储中。

## 11. 必须长期成立的不变量

实现和测试必须共同守住以下不变量：

1. ctl 不存储、不转发文件 metadata；
2. 一个 artifact 只有 primary root 生成第一份 metadata；
3. 非 primary 节点只复制 sealed metadata，不能生成替代版本；
4. BUILDING artifact 永不对外可见；
5. 同一 downloader 从开始到完成只绑定一个 metadata_id；
6. piece 写盘前必须匹配 metadata 中的预期 digest；
7. data、metadata、commit marker 全部成功后才算 complete；
8. 同 filename、不同 metadata_id 永不覆盖、永不混写；
9. JobID、filename 和 metadata_id 不可互相替代；
10. 切换 upstream 只能在 metadata_id 相同的节点之间继续；
11. banned 状态下不提供 metadata 和 data；
12. 扩容下载只依赖 ctl 提供的 upstreams，文件内容协商完全走数据面。

## 12. 错误模型与可观测性

数据面至少需要区分：

- `NOT_FOUND`：upstream 没有 sealed artifact；
- `NOT_READY`：artifact 正在 BUILDING，尚不可服务；
- `CONTENT_CONFLICT`：metadata_id 或文件参数不一致；
- `METADATA_CORRUPT`：metadata 长度、格式或 metadata_id 校验失败；
- `PIECE_DIGEST_MISMATCH`：piece 与权威 metadata 不一致；
- `FILE_DIGEST_MISMATCH`：完整文件最终校验失败；
- `BANNED`：文件被禁止分发；
- `LOOP_DETECTED`：拓扑请求形成环；
- `INTERNAL`：本地存储、fsync、rename 等系统错误。

关键日志和指标应包含 tree_id、filename、metadata_id、job_id、node_id、upstream、piece index 和错误类别。metadata_id 可以只打印固定长度前缀，但冲突事件应能关联两端完整值。

## 13. 实现顺序

当前代码已经有 piece store、GetPiece CRC64、root 回源、拓扑、租约和 banned gate，但尚未完整实现本文的 artifact metadata 流程。建议按以下顺序落地：

1. 定义确定性 `FileMetadataV1` 编码和本地 sidecar；
2. 修正完成语义：data、metadata 和 commit 全部成功才 complete；
3. primary root 使用隔离 staging 单遍生成 metadata；
4. 增加 `GetFileInfo` 与 streaming `GetMetadata`；
5. Downloader 绑定 metadata_id，并用预期 piece digest 校验；
6. 增加 `CONTENT_CONFLICT` 等明确错误码；
7. 实现 downstream 原样复制、断点续传和 upstream 切换检查；
8. 增加同 filename 并发 Job、源对象变化、metadata 损坏、piece 损坏、崩溃恢复和扩容下载测试；
9. 根据实际规模再引入 metadata 分页或按范围读取优化。

任何性能优化都不能绕过第 11 节的不变量。
