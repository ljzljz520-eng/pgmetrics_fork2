# Review：CollectionSession 采集一致性体系

实现分支：master（工作区 `/Users/nancy/swe-project/pgmetrics_fork2`）
Schema：`ModelSchemaVersion = "1.22"`（additive，旧消费者无感知）

## 1. 实现概览

| 层 | 文件 | 内容 |
|---|---|---|
| 模型 | [model.go](file:///Users/nancy/swe-project/pgmetrics_fork2/model.go) | `CollectionIntegrity`/`SourceTiming`/`DomainTiming`；状态常量 7 个、class 2 个；RDS/Azure 时间戳与状态字段；全部新时间字段 int64 微秒、JSON `*_us`、omitempty |
| 会话 | [collector/session.go](file:///Users/nancy/swe-project/pgmetrics_fork2/collector/session.go) | 父上下文、单调时钟、共同锚点、源/域账本、偏差/窗口聚合、Finalize、`selectClosest` 纯函数 |
| 执行器 | [collector/collect.go](file:///Users/nancy/swe-project/pgmetrics_fork2/collector/collect.go) | `CollectWithContext`；`querier` 接口；全部查询经 session ctx + per-query timeout；源启动门控；连接时钟偏移一次测量 |
| 快照 | [collector/snapshot.go](file:///Users/nancy/swe-project/pgmetrics_fork2/collector/snapshot.go) | REPEATABLE READ READ ONLY 组、共享 snapshot id/data time、hold 时钟门控、检查点回滚、observed 包装 |
| 云 | [collector/aws.go](file:///Users/nancy/swe-project/pgmetrics_fork2/collector/aws.go)、[collector/azure.go](file:///Users/nancy/swe-project/pgmetrics_fork2/collector/azure.go) | 可注入接口+包级工厂；锚点最近选样；逐指标保留提供方时间戳；stale/unavailable/ok 三态 |
| 输出 | [cmd/pgmetrics/report.go](file:///Users/nancy/swe-project/pgmetrics_fork2/cmd/pgmetrics/report.go)、[csv.go](file:///Users/nancy/swe-project/pgmetrics_fork2/cmd/pgmetrics/csv.go)、[main.go](file:///Users/nancy/swe-project/pgmetrics_fork2/cmd/pgmetrics/main.go) | Collection Integrity 小节（3 入口）、`pgmetrics.collection.*` CSV、`--max-skew/--max-stale/--snapshot-hold` |

## 2. TR-4.5 域分类对照表（rubric 证据）

### postgres 模式（每个数据库连接两个快照组 + observed 域）

| 组/类别 | 域 | 一致性承诺 |
|---|---|---|
| snapshot `cluster_catalog` | databases, tablespaces, replication_slots(v9.4+), roles | 同一 REPEATABLE READ READ ONLY 事务；共享一次 `pg_current_snapshot()` id 与 `clock_timestamp()` |
| snapshot `db_<db>` | tables, partition_info(v10), parent_info, indexes, indexdef, sequences, functions, extensions, triggers, publications, subscriptions(v10) | 同上；目录与列在同事务内读取 |
| observed（集群） | system_info, settings, control_system, last_xact, control_checkpoint, activity, backend_type_counts, wal_archiver, bgwriter, replication, wal_receiver, admin_funcs, vacuum_progress, wal_counts, notification_queue, locks, wal, progress_*, checkpointer, stat_io, stat_locks, stat_recovery, log_info, local | 自动提交；仅标注观测窗口（服务端时钟，经连接偏移换算），无 snapshot_id |
| observed（库级） | statements, bloat, citus | 同上 |

### 连接池模式（无事务，NFR-4）

| 模式 | observed 域 |
|---|---|
| pgbouncer | pools, servers, clients, stats, databases |
| pgpool | pool_version, pool_nodes, health_check_stats, backend_stats, pool_cache |

### 外部源

| 源 | 域 | 时间来源 |
|---|---|---|
| aws-rds | metrics + enhanced(logs) | CloudWatch 逐点时间戳；CloudWatch Logs 事件 Timestamp；选距锚点最近样本 |
| azure | metrics | Azure Monitor 逐点 TimeStamp；Average→Total→Maximum 取值 |

## 3. AC 证据映射

| AC | 密闭测试 | 集成测试（Docker PG17） | 结论 |
|---|---|---|---|
| AC-1 | TestSnapshotGroupsShape（同 Tx、BEGIN 含 REPEATABLE READ/READ ONLY、snap id 一致、窗口=快照点）；TestSnapshotRollbackCheckpoint | TestIntegrationSnapshot（中途 DDL+INSERT+ANALYZE，组内 id/window 一致，新表/索引 tables+indexes 双缺席，服务端确已提交） | PASS |
| AC-2 | session_test 窗口单调性 | TestIntegrationSnapshot（observed 域 class 正确、窗口非退化、无 snapshot_id；snapshot 域窗口为单点） | PASS |
| AC-3 | TestSessionSkew*（0/+5s/−90s，60s→degraded+OOB，120s→ok） | TestIntegrationMultiSource（AWS −20s / Azure −80s；30s→degraded+complete+OOB，窗口覆盖两端，max_skew≥70s；600s→ok） | PASS |
| AC-4 | TestAWSClosestSelection/StaleAndUnavailable/EnhancedTimestamp；TestAzureClosestSelection/StaleAndUnavailable | TestIntegrationMultiSource 断言 RDS.BasicTimes / Azure.MetricTimes 非零且为提供方时间 | PASS |
| AC-5 | TestCancelGatesNewConnection（cancel 后 connects=0）；session_test 取消门控；TestAWSCanceled/TestAzureCanceled（预取消零 SDK 调用） | TestIntegrationCancel（roles 后取消：intdb2/aws-rds/azure 不启动、云 SDK 零调用、已完成域保留窗口、complete=false/incomplete/cancel_reason） | PASS |
| AC-6 | TestSnapshotGroupsShape（connect=1、tx=2、每事务仅 BEGIN/快照 SELECT/COMMIT）；TestSnapshotHoldCutoff（80ms hold：1 ok 2 skipped、持有≤hold+100ms、incomplete、到限后无查询） | TestIntegrationOverhead（两组均命中、snapshot 域≥10、xact_commit 增量 ≤3·observed+12 弱上界、无残留 idle-in-transaction） | PASS |
| AC-7 | TestHumanOutputIntegrity/TestCSVOutputIntegrity/TestJSONOutputIntegrity | 人工 smoke：human/json/csv 对 Docker PG 实跑（默认参数、--no-sizes、--max-skew=0 禁用） | PASS |
| AC-8（rubric） | — | human 小节对齐既有缩进/措辞风格；状态词统一小写存储、human 大写展示；微秒字段统一 `*_us`；旧 JSON（无 collection）三格式均无相关输出 | 自评 5，待 reviewer 评分（阈值 ≥4） |

### 验收命令实测结果

- `gofmt -l .`：空
- `go vet ./...`：干净
- `go build ./...`：通过
- `go test ./...`：三个包全部 ok
- `go test ./collector/ -run 'Snapshot|Session|AWS|Azure|Cancel'`：全部 PASS
- `PGMETRICS_INTEGRATION=1 go test ./collector/ -run Integration -v`：4/4 PASS
- CLI smoke（postgres:17-alpine）：human 显示 Collection Integrity（OK，skew 数十毫秒、limit 5m0s）；CSV 含全部 pgmetrics.collection.* 键（43 域、15 snapshot 域）；JSON collection 对象字段齐全；`--max-skew=0 --max-stale=0` 显示 "limit disabled"。

## 4. 关键设计与偏差说明（供 reviewer 重点核对）

1. **gateCtx / workCtx 拆分（AC-5 的落地关键）**：现有代码对 SQL 错误普遍 `log.Fatalf`（Non-Goal 明确不改变该哲学）。若让在途查询继承可取消的父 ctx，取消会把在途语句变成致命退出（集成测试实测命中：`current_database failed: context canceled` 进程退出）。因此 session 持有两个 ctx：
   - `gateCtx`（派生自父 ctx）：只驱动"新工作门控"（BeginSource/Done/Observe 前置检查、组间 hold 门控）与云 SDK 调用（云路径错误被处理而非 fatal）；
   - `workCtx`（派生自 context.Background，Finalize 时才取消）：在途 SQL 与快照事务绑定它，父取消不杀在途语句，其在途时长由 per-query timeout（默认 5s）与 statement_timeout 兜底；回到域/源边界后门控生效，已完成域正常提交并保留窗口。
   这满足 AC-5"后续请求不启动、已完成域保留窗口"，且未改动任何 log.Fatalf 调用点。
2. **degraded 的 Complete 语义**：偏差/陈旧造成的 degraded 是"完整采集但超界"，`complete=true`；取消/skipped/error 才 `incomplete`。优先级 incomplete > degraded > ok。
3. **快照 id 重复**：安静服务器上跨事务的 `pg_current_snapshot()` 文本可能相同（xmin/xmax 未变），故组身份的权威依据是设计规定的域集合而非 id 文本；组内 id 相等仍是必要断言。
4. **云窗口只含被选点**：`WindowStart/End` 与 `SampleTime` 仅基于每指标选中的最近样本，被拒绝的旧点不进入窗口（但其时间戳仍逐指标保留在 BasicTimes/MetricTimes 中）。
5. **开销上限**：单库 postgres 恰好 2 个事务（N 库 N+1 个连接，每个连接 2 个事务），0 新连接，每事务仅 BEGIN + 1 条快照 SELECT + COMMIT 三条额外语句；hold 不绑定事务 ctx（database/sql 会在 ctx 到期时自动 ROLLBACK，与"提交已完成部分"冲突），仅作域间时钟门控。
6. **schema 1.22 additive**：Collection=nil 时 human 无小节、JSON 无 collection 键、CSV 无相关记录。

## 4b. 首轮独立 Review 发现与修复记录

首轮 reviewer 结论 FAIL（1 major + 若干 minor/nit），已处理：

| 问题 | 严重度 | 修复 |
|---|---|---|
| #1 PG 源 DataStart/DataEnd 从不登记（AC-3/FR-3 违约，CSV 实锤为 0） | major | Finalize 在源折叠前先把每个域的 WindowStart/End 聚合进所属 sourceRec（与云源显式区间合并），Sources 输出时写 DataStart/DataEnd；session_test 与集成 TestIntegrationMultiSource 增加双 PG 源区间/请求耗时断言，PASS |
| #2 纯 stale/unavailable/源级超界不出现在 out_of_bounds，human 只见 DEGRADED 无解释 | minor | Finalize 增加源级标签：超偏差 `<name> (source)`、陈旧/不可用 `<name> (source:stale/unavailable)`，去重后追加到 OutOfBounds |
| #4 取消后当前库仍执行一次 SELECT current_database() | minor | collectDatabase 入口 `session.Done()` 短路，取消后不再发任何簿记语句 |
| #5 SourceTiming.RequestDuration 恒 0 | nit | sourceRec 记录单调时钟 reqStart，EndSource 写 RequestDuration；session_test 断言 |
| #6 Finalize 重复调用产生虚假 incomplete | nit | sync.Once 缓存首次结果，重复调用返回同一契约；session_test 断言 |
| #7 回滚组内已完成域仍记 ok | nit | 新增 session.RetractGroup(source,snapshotID)，snapshot.go 三条回滚路径（语句错误 abort、部分提交失败、组末提交失败）均撤回组内域为 error；TestSnapshotRollbackCheckpoint 断言回滚后无 ok 快照域 |
| #9 help 未注明 --snapshot-hold=0 禁用 | nit | help 文案补 "0 disables the hold limit" |
| #3（在途语句期间到限事务最长持 hold+query timeout） | minor（不修） | 维持设计取舍：database/sql ctx 到期强制 ROLLBACK 与"提交已完成部分"冲突；AC-6 Pass Condition 只要求域间到限（已由密闭测试强断言覆盖）；此处保持在途语句由 per-query timeout/statement_timeout 兜底 |
| #8 快照标记失败回退本地钟不含 offset；偏移测量含 RTT | nit（不修） | 容错路径仅在 SELECT clock_timestamp 失败时发生（真实 PG 几乎不出现）；改动会增加连接往返，性价比低，留待后续 |
| #10 域名 wal vs spec 描述 wal_activity | nit（不修） | 账本键稳定性优先，且 spec 未逐字规定键名 |

修复后：gofmt 空、go vet/build 通过、`go test ./...` 三包全绿、`PGMETRICS_INTEGRATION=1` 4 个集成测试全部 PASS。

### 第二轮独立复审：PASS，附带 1 minor + 3 nit，已全部修复

- **minor（跨组误伤）**：撤回以 snapshotID 文本为键，PG<13 空 id 或 PG13+ 安静库重复 id 下，一组回滚会把另一已提交组的 ok 域误标 error。改为**按组身份**撤回：groupRunner 记录本组注册的 `[]*domainRec`（新增 `session.DomainRec` 访问器 + `RetractDomains(recs)`），三条回滚路径只撤回本组记录；新增密闭测试 `TestSnapshotRetractIsGroupScoped`（fake 驱动两组返回同一 snapID：a 提交、b 回滚，断言 a1 仍 ok、b1/b2 error）。
- **nit（撤回域残留窗口）**：撤回时一并清零 WindowStart/End/DataTime/Freshness/outOfBounds，使回滚数据不进入整体窗口与源区间；测试断言 b1 窗口为 0。
- **nit（源级标签三重冗余）**：陈旧/不可用标签与裸 `(source)` 偏差标签不再同时出现——每源最多一个解释性标签（`(source:stale)` / `(source:unavailable)` / `(source)`）；TestSessionStaleAndUnavailable 与 TestSessionSkewAndDegraded 锁定标签存在性与去重。
- **nit（源级标签无测试）**：已由上述两个密闭用例覆盖。

二轮修复后 `go vet`、`go test ./... -count=1`、`PGMETRICS_INTEGRATION=1` 4 个集成用例全部 PASS。

## 5. 待 reviewer 检查项

- AC 全覆盖与证据真实性（可重跑上述命令）；
- gate/work ctx 拆分是否在所有 SQL 路径一致（snapshot.go 的 BeginTx/快照 SELECT、collect.go effCtx/getConn/getDBNames），云路径是否确实使用 GateCtx；
- 时钟偏移换算（observed 窗口 = 本地起止 + source offset）与 Finalize 偏差聚合的正确性；
- 快照检查点回滚覆盖的切片/map 是否完整（snapshot.go checkpoint）；
- TR-4.5 域分类与上表是否一致，有无遗漏域或错分类；
- 输出措辞/单位/字段命名是否达到 AC-8 ≥4。
