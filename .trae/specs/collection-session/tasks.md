# CollectionSession：带父上下文与单调时钟的采集一致性 - 实施计划

## Task 1: 模型层 schema 1.22 — 完整性契约与云时间/状态字段
- **Status**: `completed`
- **Completion Evidence**: model.go 升至 1.22；新增 CollectionIntegrity/SourceTiming/DomainTiming 与 7 个状态常量、2 个 class 常量；RDS/Azure additive 字段；model_test.go 覆盖 TR-1.1/1.2（omitempty + round-trip），`go test .` PASS。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 [model.go](file:///Users/nancy/swe-project/pgmetrics_fork2/model.go) 将 `ModelSchemaVersion` 升至 `"1.22"`（注释加 1.22 条目）。
  - 新增类型：`CollectionIntegrity`、`SourceTiming`、`DomainTiming`；状态常量（`ok/degraded/incomplete/stale/unavailable/error/skipped`）。
  - `Model` 新增 `Collection *CollectionIntegrity`（`json:"collection,omitempty"`）。
  - `RDS` 新增 `SampleTime`、`WindowStart`、`WindowEnd`（unix micros）、`Status`、`BasicTimes map[string]int64`、`EnhancedAt`；`Azure` 新增同名时间窗/`Status`/`MetricTimes map[string]int64`。保留原 `Basic`/`Enhanced`/`Metrics` 字段与类型不变。
  - 时间字段：完整性/窗口/偏差/新鲜度一律 int64 微秒、JSON 名统一 `*_us`；云 per-metric 时间戳亦为微秒。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-7, AC-8
- **Test Requirements**:
  - `rule` TR-1.1: `go build ./...` 通过；`json.Marshal` 一个含 Collection 的 Model 后新字段存在、旧字段不变（向后兼容夹具测试）。
  - `rule` TR-1.2: 空 Model（Collection=nil）JSON 中不出现 `collection` 键（omitempty）。
- **Notes**: 纯类型变更，无逻辑。

## Task 2: CollectionSession 核心 — 时钟、锚点、域观测、偏差汇总、取消
- **Status**: `completed`
- **Completion Evidence**: collector/session.go（Clock/SessionConfig/Hooks/CollectionSession：Begin/EndSource、Observe/ObserveSnapshot/RecordDomain、Finalize、selectClosest）；CollectConfig 加 MaxSkewSec/MaxStaleSec/MaxSnapshotHoldMillisec（默认 300/300/10000）；session_test.go 6 例覆盖 TR-2.1~2.4 + 取消门控/error/selectClosest，`go test ./collector -run TestSession` PASS，go vet 干净。
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 新文件 `collector/session.go`：
    - `type Clock func() time.Time`；`SessionConfig{MaxSkew, MaxStale, MaxSnapshotHold time.Duration}`。
    - `CollectionSession`：父 ctx 派生 ctx/cancel、clock、anchor（墙上）、start（单调）、cfg、来源与域记录（加锁）、`Hooks`（`AfterDomain func(*Domain)` 等测试钩子）。
    - `newSession(parent context.Context, cfg, clock)`；`Observe(source, name, class, snapshotID, dataTime, fn) DomainStatus`：ctx 已结束则不执行 fn、直接记 skipped；记录请求起止（单调耗时）、窗口、新鲜度（anchor−dataTime）。
    - `BeginSource/EndSource`：来源请求区间、数据起止、时钟偏移、状态。
    - `Finalize()`：计算 window_start/end、max_skew（各源数据中点相对 anchor 的最大绝对偏差；云域按其样本时间参与）、out_of_bounds（偏差超 MaxSkew 或新鲜度超 MaxStale）、status（incomplete > degraded > ok）、complete、cancel_reason，产出 `pgmetrics.CollectionIntegrity`。
    - 纯函数：`selectClosestTime(anchor, candidates)`、偏差/窗口聚合，供云选样与单测复用。
  - `CollectConfig` 新增 `MaxSkewSec`、`MaxStaleSec`、`MaxSnapshotHoldMillisec` 及默认值（300/300/10000），在 `DefaultCollectConfig` 填充。
  - 新文件 `collector/session_test.go`（密闭，注入虚拟时钟）：窗口单调、skew 聚合、阈值 degraded、stale/unavailable、取消不执行 fn、Finalize 优先级 incomplete>degraded>ok。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-5
- **Test Requirements**:
  - `rule` TR-2.1: 注入时钟下两域窗口端点严格单调且等于时钟推进值；`go test` 证据。
  - `rule` TR-2.2: 构造 3 源偏移（0、+5s、−90s），MaxSkew=60s → degraded 且 out_of_bounds 含 −90s 源；放宽到 120s → ok。
  - `rule` TR-2.3: 父 ctx 取消后新 Observe 的 fn 零执行、域状态 skipped；Finalize 输出 complete=false 与非空 cancel_reason。
  - `rule` TR-2.4: stale 域（dataTime 早于 anchor−MaxStale）触发 degraded；unavailable 源（无数据时间）状态保留且不污染窗口。
- **Notes**: session 不接触 sql；只做时序与状态账本。

## Task 3: 采集器 executor 重构与父上下文贯通
- **Status**: `completed`
- **Completion Evidence**: querier 接口 + c.q；4 文件 92 个调用点全部 c.q.* 且 ctx 经 c.effCtx()（session 派生）；CollectWithContext 导出、Collect 委托；getConn/getDBNames 吃 ctx；多库/logs/RDS/Azure 启动门控；getCurrentUser 合并 clock_timestamp 每连接一次测偏移（pgbouncer best-effort）；go vet/build 干净，Grep 残留 c.db.Query*=0；TR-3.3 由 TestCancelGatesNewConnection 覆盖（connects=0）；TR-3.2 完整端到端由 Task 8 真实 PG 集成覆盖（伪驱动覆盖组机制）。
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - [collect.go](file:///Users/nancy/swe-project/pgmetrics_fork2/collector/collect.go)：
    - 新增 `type querier` 接口（QueryContext/QueryRowContext/ExecContext），`*sql.DB` 与 `*sql.Tx` 天然满足；collector 增加 `q querier` 字段，随连接切换。
    - 将 collect.go/citus.go/pgbouncer.go/pgpool.go 中全部 `c.db.QueryContext/QueryRowContext/ExecContext` 机械替换为 `c.q.*`（同包 sed 式编辑后人工核对）；每查询 ctx 改为 `session.Ctx()` 派生（保留 per-query timeout 为 `context.WithTimeout(sessionCtx, c.timeout)`，可用 helper `c.queryCtx()`）。
    - 新增导出 API `CollectWithContext(ctx context.Context, o CollectConfig, dbnames []string) *pgmetrics.Model`；`Collect` 以 Background 委托；session 在该函数创建并在 defer 中 Finalize 写入 `result.Collection`。
    - 多库循环、RDS、Azure 启动前做 `select { case <-ctx.Done(): … ; default }` 门控；`getConn` 的 SET ROLE 改用 session ctx；`getDBNames` 同理。
    - getCurrentUser 查询并入 `clock_timestamp()`（postgres/pgpool 路径），连接建立时登记来源与时钟偏移；pgbouncer 路径做单独的偏移测量（容错失败=偏移 0）。
  - 保持全部现有采集逻辑与错误处理不变。
- **Acceptance Criteria Addressed**: AC-5, AC-6
- **Test Requirements**:
  - `rule` TR-3.1: `go vet ./...` 与 `go build ./...` 通过；替换后不存在残留 `c.db.Query`（Grep 证据，除字段赋值外为 0）。
  - `rule` TR-3.2: 密闭伪驱动（`driver.Driver` 实现，Task 4 建）下跑通一次完整 postgres 采集（伪行满足主要 Scan），无 panic、完整性对象生成。
  - `rule` TR-3.3: ctx 在多库循环前取消时，第二个库的 driver.Connect 零调用（伪驱动计数）。
- **Notes**: 替换面广但纯机械；伪驱动在 Task 4 实现，本任务先保证编译，伪驱动冒烟可与 Task 4 合并提交但 TR 分别记录。

## Task 4: 单库快照组（显式只读事务 + 统计快照）与观测域包装
- **Status**: `completed`
- **Completion Evidence**: snapshot.go（snapshotGroup/groupRunner：REPEATABLE READ READ ONLY、clock_timestamp+pg_current_snapshot 一次往返、时钟门控 hold、COMMIT 已完成/ROLLBACK+检查点、SkipDomain）；collectCluster/collectDatabase 重构为 cluster_catalog 与 db_<db> 两组+observed 域；pgbouncer/pgpool 全 observed；fakedriver_test.go+snapshot_test.go 覆盖 TR-4.1（BEGIN 子句/组内查询全在 Tx）、TR-4.2（connect=1/tx=2/每事务仅 BEGIN+快照 SELECT+COMMIT/无并发事务）、TR-4.3（hold 80ms：1 个域 ok 2 skipped、事务持有 ≤hold+100ms、incomplete、无超界查询）、TR-4.4（检查点回滚+ROLLBACK+后续域不执行）；全部 PASS。TR-4.5 域分类对照表在 review.md。
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - `collector/snapshot.go`：
    - `snapshotGroup(name string, fn func() error)`：`BeginTx(READ ONLY, REPEATABLE READ)`；组首执行 `SELECT clock_timestamp(), COALESCE(pg_current_snapshot()::text,'')`（错误不致命）得 dataTime/snapshotID；设置组 ctx 截止 `min(session ctx, start+MaxSnapshotHold)`；域经 session.Observe(snapshot) 执行；正常 COMMIT；语句间到限→COMMIT 已完成部分、剩余域 skipped、session 标记 incomplete；语句错误→ROLLBACK + model 检查点回滚。
    - model 检查点：组前记录相关切片长度与 citus map 键，回滚时截断/删除。
    - 观测域 helper `observed(name, fn)`：自动 commit 模式下经 session.Observe 包装，窗口经时钟偏移换算服务端时间。
  - 重构 `collectCluster`：snapshot 组包裹 databases/tablespaces/roles/replication_slots；其余调用逐一包 observed 域名（settings、system_info、control_*、activity、be_type_counts、wal_archiver、bgwriter、replication、wal_receiver、admin_funcs、last_xact、notification、locks、wal_counts、progress_*、checkpointer、stat_io、stat_locks、stat_recovery、log_info）。
  - 重构 `collectDatabase`：snapshot 组包裹 tables(+partition/parent)、indexes(+indexdef)、sequences、functions、extensions、triggers、publications、subscriptions；statements、bloat、citus 包 observed。
  - pgbouncer/pgpool：仅 observed 包装，无事务。
  - `collector/fakedriver_test.go`：最小 database/sql/driver（Conn/Stmt/Tx/Rows，按脚本/SQL 关键字返回伪列），统计 Connect/Begin/Commit/语句；`snapshot_test.go` 断言：连接=1、事务=2、组内查询走同一 Tx、隔离级别/只读子句出现在 BEGIN 语句、组外无显式事务；慢钩子下事务在 MaxSnapshotHold 容差内结束、到限后无新域启动、incomplete。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-6
- **Test Requirements**:
  - `rule` TR-4.1: 伪驱动记录两次 BEGIN 均含 `ISOLATION LEVEL REPEATABLE READ` 与 `READ ONLY`；集群组与库组内的查询语句全部在对应 Tx 上执行（无组内语句落到 db 直连）。
  - `rule` TR-4.2: 伪驱动统计 Connect 次数=1，显式事务数=2，每事务附加语句仅 BEGIN/快照 SELECT/COMMIT。
  - `rule` TR-4.3: 注入组内钩子使到限触发：事务结束时刻 − 开始 ≤ MaxSnapshotHold+100ms；到限后域 fn 零执行；Finalize complete=false。
  - `rule` TR-4.4: 回滚检查点：组内某语句伪报错时，组前已存在的 model 切片内容不被污染（长度回到检查点）。
  - `rubric` TR-4.5: 域划分与 spec 设计决策一致性；1-5；1=遗漏/错分类超过 3 处，3=错漏 1 处，5=完全一致且命名稳定；阈值 >=4；证据为评审对照表。
- **Notes**: 真实 PG 端到端一致性由 Task 8 验证。

## Task 5: AWS — ctx 贯通、锚点最近选样、时间戳保留、陈旧/不可用状态
- **Status**: `completed`
- **Completion Evidence**: aws.go 重写（cwAPI/cwlogsAPI/rdsAPI 三接口+newAwsCollectorFn 工厂注入；collect(ctx,anchor,maxStale,dbid,out) 查询窗锚定，逐 metric selectClosest 选样并写 BasicTimes，EnhancedAt=事件 ms*1000，SampleTime/WindowStart/End 仅基于被选点，无点 unavailable/过旧 stale；collectLogs 全 WithContext）；collectFromRDS 接 session 账本；aws_test.go 4 例覆盖 TR-5.1（−10s/−200s 选 −10s）、TR-5.2（stale+时间戳保留 / unavailable 不暴露旧值）、TR-5.3、TR-5.4（取消零调用），`go test ./collector -run TestAWS` PASS。
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - [aws.go](file:///Users/nancy/swe-project/pgmetrics_fork2/collector/aws.go)：
    - 定义包内接口 `cwAPI/cwlogsAPI/rdsAPI`（仅覆盖使用到的 WithContext 方法），awsCollector 持有可注入字段；`newAwsCollector` 注入真实 SDK client。
    - `collect(ctx, anchor, maxStale, dbid, out)`：查询窗 `[anchor−5m, anchor]`；用 `*WithContext` 方法；对每 metric 全部点以 `selectClosestTime` 选最近点，写 `out.Basic[name]` 与 `out.BasicTimes[name]=ts.UnixMicro()`；enhanced 事件取时间戳写 `out.EnhancedAt`（事件 Timestamp 毫秒→微秒），保留 GetLogEventsWithContext。
    - 汇总：`out.SampleTime`=被选点中最近/代表时间，`WindowStart/End`=全部被选点最小/最大；状态：无任何点→unavailable；最旧/最近样本判定——最近样本年龄 > maxStale → stale，否则 ok；逐指标陈旧不删除值但时间戳在。
    - `collectFromRDS` 改为传 session ctx/anchor/MaxStale，并向 session 登记 aws 来源（数据起止、状态、新鲜度）；失败域状态 error，不 fatal 退出采集。
  - `aws_test.go`：伪三个 SDK 接口，覆盖 fresh 多点选最近、全部过旧 stale、空点 unavailable、enhancedAt 保留、ctx 取消时零调用。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-5
- **Test Requirements**:
  - `rule` TR-5.1: 伪 CloudWatch 对 metric A 返回距 anchor −10s 与 −200s 两点 → 选 −10s 点值与时间戳；断言 BasicTimes[A] 等于 −10s 的 UnixMicro。
  - `rule` TR-5.2: 全部点年龄 > MaxStale → out.Status=stale；空 Timestamps/空结果 → unavailable；新点 → ok。
  - `rule` TR-5.3: Enhanced 事件 message 内/外时间戳写入 EnhancedAt，等于事件 Timestamp。
  - `rule` TR-5.4: 预取消 ctx 调用 collect 返回 context.Canceled 且三个 SDK 接口零调用。
- **Notes**: 不改日志下载的业务语义，仅换 WithContext 与门控。

## Task 6: Azure — ctx 锚点选样、时间戳保留、陈旧/不可用状态
- **Status**: `completed`
- **Completion Evidence**: azure.go（azureMetricsLister 接口+newAzureMetricsClient/newAzureCredential 包级工厂；timespan 以 anchor 为 to；全 timeseries data point 中选 TimeStamp 最近点，同点取值 Average→Total→Maximum；MetricTimes/SampleTime/Window*；空→unavailable、过旧→stale；ResourceRegion nil 防护）；collectFromAzure 接 session 账本；azure_test.go 3 例覆盖 TR-6.1（−5s Total 胜过 −180s Average）、TR-6.2、TR-6.3（region/name 回归）+取消零调用，`go test ./collector -run TestAzure` PASS。
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - [azure.go](file:///Users/nancy/swe-project/pgmetrics_fork2/collector/azure.go)：
    - 引入接口 `azureMetricsLister`（签名对齐 `armmonitor.MetricsClient.List`）与包级工厂变量 `newAzureMetricsClient(subID, cred) (azureMetricsLister, error)`，测试替换。
    - timespan 以 anchor 为 `to`；逐 metric 在全部 data point 中找 TimeStamp 距 anchor 最近者，取值优先级 Average→Total→Maximum；写 `Metrics[name]` 与 `MetricTimes[name]`；SampleTime/WindowStart/End；空 timeseries 指标跳过；整体无有效点 → unavailable；最近点过旧 → stale。
    - `collectFromAzure` 传 session ctx/anchor/MaxStale 并登记来源。
  - `azure_test.go`：伪 lister 返回构造的 armmonitor.Response，覆盖最近选样（含多个 Average 点与仅有 Total 的旧点）、stale、空响应 unavailable、时间戳逐指标保留。
- **Acceptance Criteria Addressed**: AC-3, AC-4
- **Test Requirements**:
  - `rule` TR-6.1: metric X 有 −5s(Total) 与 −180s(Average) 两点 → 选 −5s Total 值，MetricTimes[X]=−5s 微秒。
  - `rule` TR-6.2: 全部点过旧→stale；timeseries 为空/全 nil 时间戳→unavailable；新点→ok。
  - `rule` TR-6.3: ResourceRegion/资源名解析行为不变（回归断言）。

## Task 7: 输出层 — human 小节、CSV 记录、CLI 配置
- **Status**: `completed`
- **Completion Evidence**: report.go 新增 writeCollectionIntegrity（Anchor/Overall Window+span/Max Skew+limit+EXCEEDED/Max Stale/大写 Status/Cancel Reason/Out-of-bounds，Collection=nil 时整段省略）并接入 postgres/pgbouncer/pgpool 三入口；csv.go 输出 pgmetrics.collection.* 标量 + source/domain 索引化记录 + out_of_bounds；main.go 新增 --max-skew/--max-stale/--snapshot-hold（默认 300/300/10000ms，0 关闭）及 help 文案；output_test.go 3 例覆盖 TR-7.1（degraded+OOB、incomplete+cancel、nil 省略）、TR-7.2（CSV status/domain/source/oob）、JSON round-trip+omitempty，全部 PASS；go vet/build/`go test ./...` 全绿。
- **Priority**: medium
- **Depends On**: Task 1, Task 2
- **Description**:
  - [report.go](file:///Users/nancy/swe-project/pgmetrics_fork2/cmd/pgmetrics/report.go)：postgres/pgbouncer/pgpool 三个 human 入口在 "pgmetrics run at" 之后统一输出 Collection Integrity 小节：Anchor、Overall Window（起..止 + span）、Max Skew（值 + 限值，超限标注）、Status（DEGRADED/INCOMPLETE 大写提示）、Out-of-bounds/Stale 域列表（无则省略）。
  - [csv.go](file:///Users/nancy/swe-project/pgmetrics_fork2/cmd/pgmetrics/csv.go)：输出 `pgmetrics.collection.*`：status、complete、anchor_time_us、window_start_us、window_end_us、max_skew_us、max_allowed_skew_us、cancel_reason；逐源 `collection.source.<name>.*`；逐域 `collection.domain.<i>.{source,name,class,status,data_time_us,window_start_us,window_end_us}`。
  - [main.go](file:///Users/nancy/swe-project/pgmetrics_fork2/cmd/pgmetrics/main.go)：新增 `--max-skew=SECS`、`--max-stale=SECS`、`--snapshot-hold=MILLIS` 解析、help 文案与基本校验（>0）。
  - `cmd/pgmetrics/output_test.go`：夹具模型（degraded+out_of_bounds、incomplete 两种）断言三种格式关键串。
- **Acceptance Criteria Addressed**: AC-7, AC-8
- **Test Requirements**:
  - `rule` TR-7.1: human 输出含 "Collection Integrity"、整体窗口两端、"DEGRADED" 与超界域名；incomplete 夹具含 "INCOMPLETE" 与 cancel_reason。
  - `rule` TR-7.2: CSV 含 `pgmetrics.collection.status,degraded` 与至少一条逐域记录；JSON 解码后字段齐全。
  - `rubric` TR-7.3: 输出可用性（对齐 AC-8）；1-5；阈值 >=4；证据为三种格式实跑样例评审。
- **Notes**: 复用现有 tableWriter/时间格式化风格。

## Task 8: Docker Postgres 集成验收（5 项验收端到端）
- **Status**: `completed`
- **Completion Evidence**: integration_test.go（PGMETRICS_INTEGRATION=1 门控，docker postgres:17-alpine + trust + 空闲端口 + pg_isready + t.Cleanup）4 例全部 PASS：TestIntegrationSnapshot（组内 tables 域后阻塞，旁路建表/索引/INSERT/ANALYZE，pg_stat_activity 见 idle-in-transaction 事务；两组按域集断言 snapshot id/window 一致、中途对象 tables/indexes 双双缺席且服务端确已提交、observed 域窗口齐全）；TestIntegrationMultiSource（2 库 + 伪 AWS −20s/Azure −80s：MaxSkew=30s → degraded+complete、OOB 含 azure/metrics、max_skew≥70s、整体窗口覆盖两端、per-metric 时间戳保留；放宽 600s → ok）；TestIntegrationCancel（roles 域后取消：intdb2/aws-rds/azure 零启动、云 SDK 零调用、已完成域保留窗口、incomplete+cancel_reason）；TestIntegrationOverhead（两组均命中、snapshot 域≥10、xact_commit 增量弱上界 3*observed+12、运行后无残留 idle-in-transaction）。关键修复：session 拆分为 gateCtx（父 ctx，门控+云 SDK）与 workCtx（不继承父取消，在途 SQL 由 query/statement timeout 兜底），取消不再触发 log.Fatalf，在途快照组可提交已完成部分。
- **Priority**: high
- **Depends On**: Task 4, Task 5, Task 6, Task 7
- **Description**:
  - `collector/integration_test.go`（构建不受限，运行时无 `PGMETRICS_INTEGRATION=1` 则 t.Skip）：用 `docker run -d -e POSTGRES_HOST_AUTH_METHOD=trust` 启动 postgres:17-alpine（固定容器名、空闲端口映射、健康等待 pg_isready），建 2 个库与若干表/索引。
  - 用真实连接串走 `CollectWithContext`（NoSizes 视场景），通过 session.Hooks 注入阻塞点：
    1. `IntegrationSnapshot`：组内首域后阻塞；旁路连接循环 DDL+INSERT/DELETE 200ms 后放行；断言组内 snapshot_id 全相同；中途新表在 tables/indexes 两侧同时缺席；observed 域 class/窗口正确（AC-1、AC-2）。
    2. `IntegrationMultiSource`：2 库 + 注入伪 AWS/Azure lister（不同时间偏移），小阈值→degraded/out_of_bounds，大阈值→ok；断言 window_start/end 与 max_skew（AC-3、AC-4）。
    3. `IntegrationCancel`：钩子在首库完成后阻塞，取消 ctx，断言后续库与云零启动、已完成域窗口保留、complete=false/incomplete/reason（AC-5）。
    4. `IntegrationOverhead`：对运行中的 PG 开 log_statement 日志或利用 pg_stat_statements 难以精确统计；改为以 session 账本断言：域 class 分布正确、组事务数（借助服务器侧 `pg_stat_database.xact_commit` 前后差值给出上界断言：采集造成的提交数增量 ≤ 域数中 observed 自动提交 + 2N 组事务 的模型预期；弱断言）+ 密闭 TR-4.2/4.3 的强断言共同覆盖 AC-6。
  - 测试结束清理容器（t.Cleanup → docker rm -f）。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-3, AC-4, AC-5, AC-6
- **Test Requirements**:
  - `rule` TR-8.1: 四个集成测试在本机 Docker 上全部 PASS（附命令输出）；环境缺失时 SKIP 不算失败。
  - `rule` TR-8.2: Snapshot 用例中直接用旁路连接在组事务期间查询 `pg_stat_activity` 证明存在一个状态为 `idle in transaction`/active 的 REPEATABLE READ 会话（辅助证据），且应用层 snapshot_id 一致断言为主要证据。
- **Notes**: 钩子字段设计须在 Task 2/4 预留（AfterDomain、组内阻塞点、云 client 注入点）。

## Task 9: 全量验证、三种格式 smoke 与独立 Review
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 7, Task 8
- **Description**:
  - `gofmt -l .` 为空、`go vet ./...`、`go build ./...`、`go test ./...` 全绿；对 Docker PG 实跑 CLI 的 human/json/csv（默认参数与 --no-sizes），人工核对完整性小节/JSON/CSV。
  - 按 Spec Mode 发起一次只读独立 Review（新上下文 reviewer），据 review.md 结果修复后复审直至 pass。
- **Acceptance Criteria Addressed**: AC-1..AC-8（全部）
- **Test Requirements**:
  - `rule` TR-9.1: 上述命令输出全绿并记录证据；三种格式 smoke 输出存档（路径/摘要写入 Completion Evidence）。
  - `rule` TR-9.2: Review 结果为 pass；每个 AC 均有独立证据覆盖。
