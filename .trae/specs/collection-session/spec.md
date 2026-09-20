# CollectionSession：带父上下文与单调时钟的采集一致性 - 产品需求文档

## Overview
- **Summary**: 为 pgmetrics 引入 `CollectionSession`，以父 context、单调时钟和统一时间锚点贯穿一次采集；为每个采集域记录请求区间、观测窗口、数据源时间与新鲜度；单库内对目录/统计域使用显式只读 REPEATABLE READ 事务（同时冻结 MVCC 目录快照与 PG 统计快照），动态视图标注为观测窗口；多库与云源按共同锚点计算偏差并保留提供方时间戳；模型输出整体窗口、最大偏差、超界域与降级/未完成状态。
- **Purpose**: 当前采集把数十条 SQL 当作"同一时刻"的结果，但多库顺序采集、AWS/Azure 取"最新一个样本"并丢弃时间戳，用户无法判断数据是否代表同一瞬间、云指标是否陈旧、采集是否被取消。需要机器可读的完整性契约和有界的一致性开销。
- **Target Users**: pgmetrics CLI 使用者（human/json/csv 输出消费者）；以库方式调用 collector 的下游程序；排查跨库/跨云指标时间偏差的运维人员。

## Goals
- 一次采集（单库、多库、含 AWS/Azure）拥有统一锚点、单调时钟计时、逐域时间元数据与整体完整性结论。
- 单库内被声明为"同一快照"的目录/统计域真正共享一个 MVCC 快照与一个 PostgreSQL 统计快照；动态视图诚实地标注观测窗口，不伪称事务一致。
- 云采集器以锚点为基准选择最接近的样本、保留提供方时间戳；样本不新鲜时标记 stale，无样本时标记 unavailable，绝不把旧值伪装成当前值。
- 父 context 到期/取消后不再启动新的数据库或云请求；已完成域保留准确窗口；完整性契约标记快照未完成。
- 一致性机制的开销有明确、可测试的上限：不新增连接；单库默认运行的事务数量为常量；事务持有时间不超过配置期限。

## Non-Goals
- 不实现跨数据库实例之间的事务一致性（不同连接/服务器，物理上不可能）；跨库只做锚点对齐与偏差报告。
- 不把云 API 改为可配置时间源之外的能力（如重试、缓存、回填）。
- 不重构现有查询的指标内容/字段语义，不改动 pgbouncer/pgpool 模式的采集项。
- 不消除现存的 `log.Fatalf` 错误处理风格（取消语义仅保证"新请求不启动"的边界门控，见 AC-5 范围说明）。
- 不引入新的第三方依赖。

## Background & Context
- 代码库为 Go（go 1.26/1.27），单连接模型：`collector/getConn` 设 `MaxOpenConns(1)`；`collector.collectCluster` 与 `collector.collectDatabase` 顺序调用数十个独立查询函数（[collect.go](file:///Users/nancy/swe-project/pgmetrics_fork2/collector/collect.go)）。
- 每条语句天然一个快照，但 `getTables`/`getIndexes`/`getDatabases` 等跨语句组合在采集中途 DDL/计数变化时会互相不一致。
- PostgreSQL 机制：在一个事务内，首次访问统计视图时建立的**统计快照**在事务内复用（`pg_stat_clear_snapshot()` 文档）；REPEATABLE READ 事务持有同一个 **MVCC 快照**。因此单个只读 REPEATABLE READ 事务可同时冻结目录与统计计数。
- 云采集现状：AWS `GetMetricData` 取 `TimestampDescending` 后的 `Values[0]`，仅存 float 值（[aws.go](file:///Users/nancy/swe-project/pgmetrics_fork2/collector/aws.go)）；Azure 从 timeseries 末尾找第一个有值点，同样丢弃 `TimeStamp`（[azure.go](file:///Users/nancy/swe-project/pgmetrics_fork2/collector/azure.go)）。
- 模型版本当前 `1.21`（[model.go](file:///Users/nancy/swe-project/pgmetrics_fork2/model.go)），所有变更必须以 additive 方式升级到 `1.22`，保持旧字段与 JSON 兼容。
- 本机有 Docker 且 `postgres:17-alpine` 镜像已拉取，用于集成验收；常规 `go test` 不依赖 Docker（伪驱动/伪云 API 的密闭测试）。

## 设计决策（规定）
1. **采集域分类**
   - `snapshot`（快照域，单库内成组放入只读 REPEATABLE READ 事务）：
     - 集群组 `cluster_catalog`：databases（含 size）、tablespaces（含 size）、roles、replication_slots。
     - 每库组 `db_<dbname>`：tables（含 partition/parent 信息）、indexes（含 indexdef）、sequences、user_functions、extensions、disabled_triggers、publications、subscriptions。
   - `observed`（观测域，事务外自动提交，记录各自请求窗口）：settings、system_info、activity、backend_type_counts、wal_archiver、bgwriter/checkpointer、replication_out/in、admin_funcs（LSN）、last_xact、vacuum_progress、各 progress 视图、notification、locks、wal_activity、stat_io/stat_locks/stat_recovery、checkpoint 控制信息、statements、bloat、citus、system（主机指标）、logs，以及 RDS/Azure 云源。
   - 选择理由：statements/bloat/citus 可能因扩展缺失或较重查询出错/降级，放在事务外避免错误污染快照事务；它们自身单条语句自洽。
2. **时钟**：`Clock` 可注入（默认 `time.Now`，携带单调读数）；进程内持续时间用单调时钟；跨源时间（含云提供方时间戳）只用墙上时间比较。
3. **锚点**：session 开始时取一次墙上时间 `anchor`，所有库与云请求共用；云查询时间窗为 `[anchor-5m, anchor]`。
4. **服务端时钟偏移**：每个数据库连接建立后仅做 1 次偏移测量（并入 `SELECT current_user, clock_timestamp()`，零额外往返）；观测域的数据源窗口 = 客户端单调窗口 + 偏移。
5. **快照组开销上限（单库 postgres 默认）**：恰好 2 个事务（集群组 + 当前库组）；每事务 3 条额外协议语句（BEGIN / 快照查询 / COMMIT），快照查询为 `SELECT clock_timestamp(), COALESCE(pg_current_snapshot()::text,'')`（待机/旧版查询失败不致命，快照 ID 留空）；**新增连接数 = 0**；多库 N 个库时事务总数 = N+1。
6. **事务持有上限**：`MaxSnapshotHold`（配置，默认 10000ms）。组在语句之间检查截止时间：到限则立即 COMMIT 已完成部分、组内剩余域标记 `skipped`、完整性置 incomplete；若截止发生在语句执行中导致查询失败，则 ROLLBACK 并把该组已追加到 model 的切片回滚到组前检查点。
7. **取消门控**：session 在每个来源（每库连接、RDS、Azure）启动前与每个域启动前检查 ctx；ctx 结束后不启动新请求，已完成域窗口保留，最终 `complete=false` 并记 `cancel_reason`。进行中单条语句被杀的行为不在本次承诺范围（沿用现有错误处理）。
8. **云选样**：对返回的所有样本点，选择提供方时间戳距 anchor 最近的点（AWS 逐 metric 从全部 Timestamps/Values 中选；Azure 在候选点中按 Average→Total→Maximum 取值优先级选最近点），并保留每个指标的时间戳。
9. **新鲜度与状态**：`MaxStale`（默认 300s）控制云样本新鲜度；最近样本年龄 ≤ MaxStale 为 `ok`，有样本但过旧为 `stale`，无样本为 `unavailable`。任一源 stale/unavailable 或最大偏差超 `MaxSkew`（默认 300s）→ 整体 `degraded`；取消/域缺失/组未完成 → `incomplete`（优先级高于 degraded）。
10. **API 兼容**：新增 `CollectWithContext(ctx, o, dbnames)`；`Collect` 保持签名并以 `context.Background()` 委托。AWS SDK v1 调用全部改为 `*WithContext` 变体；Azure 已有 ctx。

## Functional Requirements
- **FR-1（Session 核心）**：`CollectionSession` 持有父 ctx 派生 ctx、可注入单调时钟、anchor；提供域观察 API（记录 name/source/class、请求起止、单调耗时、观测窗口、数据源时间、新鲜度、snapshot id、状态）与来源登记 API；结束时汇总整体窗口、最大偏差、超界域与整体状态，写入 `model.Collection`。
- **FR-2（快照组）**：按设计决策在单库内以只读 REPEATABLE READ 事务执行 snapshot 域，组内取统一 snapshot id 与数据时间；持有受 MaxSnapshotHold 约束；observed 域在事务外执行并带窗口。
- **FR-3（多源锚点）**：每个数据库与云源登记实际数据时间（最早/最晚）、请求区间与时钟偏移；整体输出 anchor、整体最早/最晚、max_skew、max_allowed_skew、超限域列表。
- **FR-4（AWS）**：保留每指标提供方时间戳（`BasicTimes`）、enhanced 事件时间戳；按 anchor 最近选样；状态 ok/stale/unavailable；使用 ctx 感知的 SDK 调用。
- **FR-5（Azure）**：保留每指标 `TimeStamp`（`MetricTimes`）；按 anchor 最近选样；状态 ok/stale/unavailable。
- **FR-6（取消）**：父 ctx 结束后不启动新库/云请求与新域；部分结果保留；完整性契约标记 incomplete 与原因。
- **FR-7（输出）**：JSON 含完整完整性对象；human 报告新增 "Collection Integrity" 小节（anchor、整体窗口、最大偏差/限值、状态、超界/陈旧域）；CSV 输出 `pgmetrics.collection.*` 记录（含逐域/逐源状态）。
- **FR-8（配置）**：`CollectConfig` 新增 `MaxSkewSec`、`MaxStaleSec`、`MaxSnapshotHoldMillisec`（含默认值）；CLI 提供 `--max-skew`、`--max-stale`、`--snapshot-hold`。

## Non-Functional Requirements
- **NFR-1**：`go build ./...`、`go vet ./...`、`go test ./...` 全部通过；不引入第三方依赖。
- **NFR-2**：默认配置下，无云选项的单库采集相较改造前不增加任何连接；增加的协议语句有常量上限（设计决策 5）。
- **NFR-3**：JSON 输出对旧消费者向后兼容：仅新增 schema 1.22 字段（omitempty），既有字段不删不改类型。
- **NFR-4**：pgbouncer/pgpool 模式不使用快照事务，完整性对象仍正确（全部 observed）。
- **NFR-5**：密闭单测不依赖 Docker/云凭证；真实 Postgres 与云 API 模拟通过集成测试与可注入接口验证。

## Constraints
- **Technical**: Go 1.26+；database/sql + pgx/v5 stdlib；单连接模型；AWS SDK v1（仅 WithContext 变体）；Azure armmonitor 具体 struct 需通过包内工厂变量/接口解耦以便测试。
- **Business**: additive schema 变更，版本号升至 `1.22`；保持 `Collect` 与 CLI 既有行为默认不变（除新增信息外）。
- **Dependencies**: 仅现有 go.mod 依赖；测试用 Docker postgres:17-alpine（本地已具备）。

## Assumptions
- 目标服务器为 PostgreSQL 9.4+（与现有支持矩阵一致）；`pg_current_snapshot()` 不可用（待机/极旧版）时快照 ID 留空，不影响事务一致性本身。
- REPEATABLE READ READ ONLY 事务在 hot standby 可用；size 函数（`pg_database_size` 等）在只读事务中可执行。
- 云主机时钟与采集机时钟以提供方返回时间戳为准；偏差按墙上时间绝对值计算，不做 NTP 校正。
- `MaxStale`/`MaxSkew` 默认 300s 覆盖现有 5 分钟云查询窗；用户可按环境（Azure 单服务器 PT15M 粒度）调宽。

## Acceptance Criteria

### AC-1: 单库快照域在采集中途变更下保持同一快照
- **Type**: `rule`
- **Given**: 运行中的测试 Postgres，采集期间有并发会话持续执行 DDL（建/删表）与计数变更（INSERT/DELETE）；采集流程在组内首条查询后通过测试钩子阻塞，待变更发生后再继续。
- **When**: 对该库执行一次带 CollectionSession 的采集。
- **Then**: cluster_catalog 与 db_<db> 组内所有域记录相同的 `snapshot_id`；组内跨查询的对象成员关系一致（中途新建对象在 tables 与 indexes 两侧同时缺席，既存对象两侧同时在场），统计计数来自同一统计快照。
- **Pass Condition**: 集成测试断言组内 snapshot_id 全相等、成员集合一致；密闭伪驱动测试断言组内查询全部经由同一个 `sql.Tx` 且事务选项为 READ ONLY + REPEATABLE READ。
- **Evidence**: `go test ./collector/ -run 'Snapshot'`（密闭）与 `PGMETRICS_INTEGRATION=1 go test ./collector/ -run IntegrationSnapshot`（Docker PG17）输出。

### AC-2: 实时域给出各自观测窗口而不伪称事务一致
- **Type**: `rule`
- **Given**: 同 AC-1 的采集中途变更场景。
- **When**: 采集完成并检查 activity、locks、bgwriter、statements 等 observed 域元数据。
- **Then**: 每个 observed 域有独立 `window_start_us/window_end_us` 与请求区间（`class="observed"`，无 snapshot_id）；snapshot 域 `class="snapshot"` 且带 snapshot_id；两类标注在 JSON 中可区分。
- **Pass Condition**: 集成测试断言 observed 域窗口非空、互不相同且 class 正确，snapshot 域无观测窗伪装；窗口端值与注入时钟/实测时序单调一致。
- **Evidence**: 集成测试与 `session_test.go` 断言。

### AC-3: 多库与模拟云源输出实际时间、整体窗口、最大偏差与降级
- **Type**: `rule`
- **Given**: 一个 PG 实例上 2 个数据库顺序采集；模拟 CloudWatch/Azure API 对不同指标返回距 anchor 不同偏移（含显著超前/滞后）的时间戳样本；配置较小 `MaxSkew`。
- **When**: 执行采集（云走接口注入的伪实现，真实选样代码路径）。
- **Then**: 输出包含每个来源的实际数据起止时间、整体最早/最晚时间、`max_skew_us`；存在超阈值源时 `status="degraded"` 且 `out_of_bounds` 列出该域/源；阈值放宽后 status="ok"。
- **Pass Condition**: 测试以两组阈值各跑一次，分别断言 degraded+out_of_bounds 与 ok；窗口端值等于所注入样本时间戳的最小/最大值。
- **Evidence**: `go test ./collector/ -run 'Skew|MultiSource'`。

### AC-4: 云指标保留提供方时间戳，不新鲜即 stale、无样本即 unavailable
- **Type**: `rule`
- **Given**: 伪 AWS CloudWatch 对部分指标只返回远超 MaxStale 的旧点、对某指标返回空 Timestamps；伪 Azure 同理返回旧点/空 timeseries；enhanced monitoring 返回带时间戳的事件。
- **When**: 执行云采集。
- **Then**: RDS.`basic_times`/Azure.`metric_times` 逐指标保存提供方时间戳（等于伪服务返回值，非采集机当前时间）；RDS 保留 enhanced 事件时间；旧指标对应源/域状态 `stale`，空指标/空响应为 `unavailable`；`basic`/`metrics` 值映射中陈旧值仍带其旧时间戳（不伪装成当前值）；样本充足且新时为 `ok`。
- **Pass Condition**: AWS/Azure 密闭测试分别覆盖 fresh/stale/empty 三态并逐字段断言时间戳与状态。
- **Evidence**: `go test ./collector/ -run 'AWS|Azure'`。

### AC-5: 父上下文取消后不启动新请求且完整性标记未完成
- **Type**: `rule`
- **Given**: 多库 + 云配置的采集；测试钩子在首个数据库来源完成后阻塞，此时取消父 ctx 后放行。
- **When**: 采集继续推进。
- **Then**: 后续数据库连接与 AWS/Azure 请求均不启动（伪驱动/伪云接口记录零调用）；已完成域保留准确请求区间与窗口；`model.Collection.complete=false`、`status="incomplete"`、`cancel_reason` 非空；整体窗口仅由已完成域计算。
- **Pass Condition**: 测试断言取消后零新请求、已完成域数量与窗口正确、incomplete 契约字段齐备；另测父 ctx 预置超时场景同样不启动首个云请求。
- **Evidence**: `go test ./collector/ -run 'Cancel'`。

### AC-6: 一致性开销与长事务持有有明确上限
- **Type**: `rule`
- **Given**: 默认配置对单库执行 postgres 模式采集；伪驱动记录连接、事务与语句。
- **When**: 采集完成。
- **Then**: 新增数据库连接数为 0（仍为单连接）；显式只读事务恰为 2 个（集群组 + 当前库组），每事务额外语句仅 BEGIN/快照查询/COMMIT；任何快照事务持有时长不超过 `MaxSnapshotHold`（注入慢钩子：到限后组内停止新域、事务立即结束、完整性 incomplete）。
- **Pass Condition**: 伪驱动统计连接数=1、事务数=2、组外无显式事务；慢钩子测试断言事务在 ≤MaxSnapshotHold+容差 内结束且没有域在到限后启动。
- **Evidence**: `go test ./collector/ -run Overhead`。

### AC-7: 三种输出展示整体窗口与超界域
- **Type**: `rule`
- **Given**: 含 degraded/incomplete 与 out_of_bounds 域的模型夹具。
- **When**: 分别以 json/human/csv 渲染。
- **Then**: JSON 含完整 collection 对象；human 含 Collection Integrity 小节并列出状态、整体窗口、偏差与超界/陈旧域；CSV 含 `pgmetrics.collection.status`、`window_start_us`、`window_end_us`、`max_skew_us` 及逐域记录。
- **Pass Condition**: 输出夹具测试对三种格式做字符串/结构化断言。
- **Evidence**: `go test ./... -run Output`（cmd 包）与人工 smoke（Docker PG 实跑三种格式）。

### AC-8: 输出的可读性与信息组织质量
- **Type**: `rubric`
- **Dimension**: 完整性信息在不干扰既有报告结构前提下的可用性（措辞、单位、状态可辨识、JSON 字段命名一致性）。
- **Scale**: 1-5
- **Anchors**: 1 = 信息缺失或淹没在既有输出中；3 = 字段齐全但单位/措辞含混；5 = human 小节一目了然、状态词统一（ok/degraded/incomplete/stale/unavailable/skipped）、微秒字段命名统一为 `*_us`、旧消费者无感知。
- **Pass Threshold**: >= 4
- **Evidence**: review 阶段对照三种格式实跑输出评分。

## Open Questions
- 无（关键取舍已在"设计决策"中规定；默认阈值 300s/300s/10000ms 可在审批时调整）。
