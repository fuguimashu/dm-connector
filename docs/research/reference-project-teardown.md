---
title: 参考项目拆解 — devlive-community/dameng-connector
date: 2026-07-27
issue: https://github.com/fuguimashu/dm-connector/issues/3
source_repo: https://github.com/devlive-community/dameng-connector
source_branch: dev (默认分支)
source_commit: 1f4209b6497593a37c3d1e6d650f92fae68eb574
source_commit_date: 2026-03-23 10:34:24 +0800 ("feat: support flink cdc pipeline close #5")
clone_time_utc: 2026-07-27T01:20Z
clone_method: git clone --depth 50 (仅 18 个 commit，实际是全量历史)
repo_metadata_snapshot: stars 82 / forks 33 / open issues 11 / language Java / license MIT / pushed_at 2026-03-23T02:35:40Z (gh api repos/devlive-community/dameng-connector)
---

# 参考项目拆解与坑位清单：devlive-community/dameng-connector

> 本文所有源码路径均相对于 **参考项目仓库根**（`devlive-community/dameng-connector`），行号对应 commit `1f4209b`。
> 本文不是对参考项目的贬低——它是目前公开可得、最完整的 DM8 CDC 实现，值得认真拆解。但它的多数"实现"是 Debezium Oracle connector 的直接移植，很多 Oracle 语义并未被真正替换，这正是我们要避开的坑。

---

## TL;DR

1. **它是真的 CDC connector，不是 JDBC dialect。** 捕获层走 DM 的 **`DBMS_LOGMNR` + `V$LOGMNR_CONTENTS`**（DM 在 Oracle 兼容模式下提供的 LogMiner 兼容接口），拉取模式为**轮询式批量拉取**，不是流式推送。
2. **它本质上是 Debezium 1.9.8.Final 的 Oracle LogMiner connector 的 fork + 改名。** 包名 `org.devlive.connector.dameng`，但类名、注释、作者署名（Chris Cranford / Gunnar Morling / Andrey Pustovetov）、甚至类名 `OracleDdlParser` / `OracleSnapshotContext` / `OracleDmlParser` 都原样保留。每个源文件头部仍写着 `Copyright Debezium Authors. Licensed under the Apache Software License version 2.0`（例：`debezium-connector/src/main/java/org/devlive/connector/dameng/Scn.java:1-5`），而仓库根 `LICENSE` 是 MIT — **许可证声明存在冲突**。
3. **`START_LOGMNR` 的 SCN 范围参数被整个注释掉了。** `SqlUtils.startLogMinerStatement()` 无论传入什么 startScn/endScn，都返回硬编码的 `BEGIN DBMS_LOGMNR.START_LOGMNR(OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;`（`debezium-connector/.../logminer/SqlUtils.java:210-230`）。范围过滤退化成 `V$LOGMNR_CONTENTS` 上的 `WHERE SCN > ? AND SCN <= ?`（同文件 `:259`）。这是最大的性能与正确性隐患。
4. **初始快照依赖 DM 的闪回查询**：`SELECT * FROM "S"."T" AS OF SCN <scn>`（`DamengSnapshotChangeEventSource.java:322`），且快照前会对每张表 `LOCK TABLE ... IN EXCLUSIVE MODE`（`:120`）。所以 README 要求开启 `ENABLE_FLASHBACK`，而 issue #16 正是用户在质疑"达梦官方不建议使用闪回，性能影响大"。**无 chunk splitting、无无锁算法、无 watermark。**
5. **DDL 在增量阶段被显式丢弃。** `LogMinerQueryResultProcessor.java:171-177`：`// todo: DDL operations are not yet supported during streaming while using LogMiner.` — 只打日志然后 `continue`。对应 issue #12「经测试不支持 ddl」。
6. **位点只有一个标量 SCN + commit_scn，无 gtid/无 per-table 位点。** 事务缓冲区在内存里（`TransactionalBuffer`），进程崩溃则未提交事务全丢；且 offset 只在 buffer 空时才前进（`LogMinerStreamingChangeEventSource.java:214-217`）——长事务会把位点钉住。
7. **Flink 侧是 Flink CDC 1.x 架构**：`DebeziumSourceFunction` 单并行度；新加的 pipeline source 是自己手搓的 FLIP-27 Source，但 **split 是空壳、checkpoint 不含任何位点**（`flink-cdc-connector/.../pipeline/DamengPipelineSplit.java` 全类只有一个常量 SPLIT_ID），且队列满时**静默丢事件**。
8. **依赖树很轻**：Debezium 只依赖 `debezium-api` / `debezium-embedded` / `debezium-ddl-parser` 1.9.8.Final + jsqlparser + protobuf；DM 驱动 `com.dameng:Dm8JdbcDriver16:8.1.1.49` 在 Maven Central 上有、且其 POM 元数据自称 Apache-2.0。

---

## 1. 捕获层：怎么读 DM8 的变更

### 用的接口：DBMS_LOGMNR / V$LOGMNR_CONTENTS（Oracle 兼容模式）

不是触发器、不是时间戳轮询、不是达梦自研逻辑日志 API。走的是 DM 在 **Oracle 兼容模式** 下提供的 LogMiner 兼容包。

前置条件（README.md「达梦数据库 CDC 配置指南」第 2–5 节，一手来源）：

| 配置 | 值 | 说明 |
|---|---|---|
| `COMPATIBLE_MODE` | `2` | `SP_SET_PARA_VALUE(2,'compatible_mode',2)`，需重启 |
| 归档日志 | `ALTER DATABASE ARCHIVELOG` | |
| `ENABLE_FLASHBACK` | `1` | 快照阶段的 `AS OF SCN` 需要 |
| `RLOG_APPEND_LOGIC` | `1` | README 原文：「一定要处理，设置为 1 否则 `V$LOGMNR_CONTENTS` 一直是 0」= DM 版的 supplemental logging |

### 轮询而非流式

`LogMinerStreamingChangeEventSource.execute()`（`debezium-connector/.../logminer/LogMinerStreamingChangeEventSource.java:126-237`）是一个 `while (context.isRunning())` 循环，每轮：

1. `getEndScn(...)` 算出本轮 endScn（`LogMinerHelper.java:148-176`）；
2. `flushLogWriter(...)` — 往自建表 `LOG_MINING_FLUSH` 写当前 SCN 并 commit，强制 DM 刷 redo（`LogMinerHelper.java:187-200`，建表语句 `SqlUtils.java:66`）；
3. 检测 log switch → `endMining` + 重新 `setRedoLogFilesForMining`（`:170-190`）；
4. `startLogMining(...)` → **注意：这里传的 startScn/endScn 被 `SqlUtils.startLogMinerStatement()` 完全忽略**（`SqlUtils.java:210-230`，Oracle 版的带 `startScn =>` / `endScn =>` 的语句被整段注释掉，只留一句硬编码的 `DBMS_LOGMNR.START_LOGMNR(OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG)`）；
5. 先跑一条 `SELECT COUNT(*) FROM V$LOGMNR_CONTENTS`（`:193-196`）——**每轮全表 count，纯浪费**；
6. 再跑真正的 `logMinerContentsQuery`，`WHERE SCN > ? AND SCN <= ?`（`SqlUtils.java:253-297`）；
7. `pauseBetweenMiningSessions()` 睡一觉（`:340-345`）。

### 批大小 / 睡眠时间配置项（`DamengConnectorConfig.java`）

| 配置项 key | 默认值 | 行号 |
|---|---|---|
| `log.mining.batch.size.default` | 20 000（SCN 跨度，非行数） | `:186-192` |
| `log.mining.batch.size.min` | 1 000 | `:194-200` |
| `log.mining.batch.size.max` | 100 000 | `:203-209` |
| `log.mining.view.fetch.size` | 10 000 | `:178-184` |
| `log.mining.sleep.time.default.ms` | 1 000 | `:228-234` |
| `log.mining.sleep.time.min.ms` | 0 | `:237-243` |
| `log.mining.sleep.time.max.ms` | 3 000 | `:211-217` |
| `log.mining.sleep.time.increment.ms` | 200 | `:246-252` |
| `log.mining.strategy` | `CATALOG_IN_REDO` / `ONLINE_CATALOG` | `:98-104`（**实际无效**，见上文第 4 步；`LogMinerStreamingChangeEventSource.java:257-259` 里 `buildDataDictionary(connection)` 也被注释掉了） |
| `log.mining.continuous.mine` | `false` | `:105-110` |
| `log.mining.archive.log.hours` | 0 | `:165-170` |
| `log.mining.transaction.retention.hours` | 0 | `:133-138` |
| `debezium.source.transaction.auto.commit.timeout.ms` | 1000 | `:255-260` — **DM 特有的补丁**，见 §5 |
| `database.port` | **1528**（Oracle 遗留，DM 实际是 5236） | `:175-177` |

**批大小是"SCN 跨度"而不是"行数"**，`getEndScn` 用 `startScn + batchSize` 试探，并根据与 currentScn 的距离动态放大/缩小（`LogMinerHelper.java:153-175`）。SCN 在 DM 里的分布密度和 Oracle 未必一致，这个自适应逻辑是否合适 **未证实**。

### 位点表示与推进

- 位点类型：`Scn`，内部就是一个 `BigInteger`（`debezium-connector/.../Scn.java:16-34`）。`Scn.MAX = BigInteger.valueOf(-2)`（`:22`），`Scn.NULL = new Scn(null)`（`:27`）——**用 -2 表示"最大"是个很容易踩的语义陷阱**：`compareTo` 走的是 BigInteger 数值比较（`:132-144`），所以 `Scn.MAX` 实际比任何正常 SCN 都**小**。
- offset map 只有三个 key：`scn` / `commit_scn` / `snapshot`+`snapshot_completed`（`SourceInfo.java:18-22`，`DamengOffsetContext.java:102-120`）。
- 当前 SCN 的取法（**DM 特有，非 Oracle 的 `V$DATABASE.CURRENT_SCN`**）：`SELECT CLSN FROM V$ARCH_FILE WHERE STATUS = 'ACTIVE'`（`SqlUtils.java:130-133`）。
- 最老可用 SCN：`SELECT ARCH_LSN FROM SYS.V$ARCH_FILE ORDER BY CREATE_TIME LIMIT 1`（`SqlUtils.java:149`）。**这里有个明确 bug**：如果 `log.mining.archive.log.hours != 0`，会在 `LIMIT 1` 后面再拼 `AND FIRST_TIME >= ...`（`:150-152`），生成的 SQL 语法必然报错。
- 归档日志清单：`SELECT PATH,ARCH_LSN,CLSN,STATUS FROM SYS.V$ARCH_FILE`（`LogMinerHelper.java:621`），把 `ARCH_LSN` 当 firstScn、`CLSN` 当 nextScn、`STATUS='ACTIVE'` 当 current。
- **位点推进策略**：主循环里 `startScn = endScn` 后，**只有 `transactionalBuffer.isEmpty()` 时**才 `offsetContext.setScn(startScn)`（`LogMinerStreamingChangeEventSource.java:212-217`）；否则位点由 commit 路径推进到 buffer 中最小 SCN（`TransactionalBuffer.java:196-201`）。一个长事务挂着 → 位点永久不前进 → 归档日志被清掉后重启必须重新快照（`LogMinerHelper.java:511-515` 抛 `"None of log files contains offset SCN: ..., re-snapshot is required."`）。

---

## 2. 在 Debezium 框架里的落位

**大量复用 Debezium 1.9.8 通用组件，不是自造框架。** 继承关系（一手，来自各文件的 `extends`/`implements` 行）：

| 本项目类 | 继承 / 实现的 Debezium 组件 | 位置 |
|---|---|---|
| `DamengConnector` | （Kafka Connect `SourceConnector`，经 Debezium `RelationalBaseSourceConnector`；**未逐行确认**） | `DamengConnector.java` |
| `DamengConnectorTask` | `io.debezium.connector.common.BaseSourceTask<MapBackedPartition, DamengOffsetContext>` | `DamengConnectorTask.java:35-36` |
| `DamengConnectorConfig` | `RelationalDatabaseConnectorConfig`（复用 `SERVER_NAME`/`HOSTNAME`/`PORT` 等 Field，见 `:80,141,176`） | `DamengConnectorConfig.java` |
| `DamengDatabaseSchema` | `io.debezium.relational.HistorizedRelationalDatabaseSchema` | `DamengDatabaseSchema.java:29-30` |
| `DamengSnapshotChangeEventSource<P>` | `io.debezium.relational.RelationalSnapshotChangeEventSource<P, DamengOffsetContext>` | `DamengSnapshotChangeEventSource.java:41-42` |
| `LogMinerStreamingChangeEventSource` | `io.debezium.pipeline.source.spi.StreamingChangeEventSource<MapBackedPartition, DamengOffsetContext>` | `LogMinerStreamingChangeEventSource.java:63-64` |
| `MapBackedPartition` | `io.debezium.pipeline.spi.Partition` | `MapBackedPartition.java` |
| `DamengOffsetContext` | `io.debezium.pipeline.spi.OffsetContext`（含内部 `Loader`） | `DamengOffsetContext.java:291-296` |
| `DamengChangeEventSourceFactory` | `io.debezium.pipeline.source.spi.ChangeEventSourceFactory` | `DamengChangeEventSourceFactory.java` |

`DamengConnectorTask.start()`（`:53-131`）把这些串起来，用的全是 Debezium 原生管线：`ChangeEventQueue` → `EventDispatcher` → `ChangeEventSourceCoordinator`。

**Schema history 用的是 Debezium 的 `HistorizedRelationalDatabaseSchema`**，`initializeStorage()` + `recover(previousOffsets)`（`DamengConnectorTask.java:62,75`）。Flink 侧默认落到本地文件 `io.debezium.relational.history.FileDatabaseHistory`（`flink-cdc-connector/.../DamengSource.java:145-146`，默认文件名 `history.txt`）——**这在 Flink 集群里是个坑：本地文件不随 checkpoint 迁移。**

**留了 Oracle 遗留但已死的分支**：`DamengConnectorConfig` 里保留了 `XSTREAM_SERVER_NAME`（`:91`）、`RAC_SYSTEM`/`RAC_NODES`（`:144,151`）、`PDB_NAME`（`:52`）。RAC 相关代码（`instantiateFlushConnections`、`flushRacLogWriters`）对 DM 没有意义。

**`StreamingChangeEventSource` 的三个新接口方法是空壳**（`LogMinerStreamingChangeEventSource.java:347-366`）：`init()` 空、`executeIteration()` 直接 `return false`、`commitOffset(Map)` 空。所有活都在 `execute()` 的大循环里。

---

## 3. 初始快照

`DamengSnapshotChangeEventSource` 走 Debezium 的 `RelationalSnapshotChangeEventSource` 模板方法：

- **锁**：`lockTablesForSchemaSnapshot()` 对每张捕获表执行 `LOCK TABLE "S"."T" IN EXCLUSIVE MODE`（`:108-123`），先打 savepoint `dbz_schema_snapshot`（`:111`），读完 schema 后 `rollback(savepoint)` 释放（`:126-130`）。**读表结构期间是排他锁。**
- **先记位点再快照**：`determineSnapshotOffset()`（`:132-153`）在读数据前拿 currentScn，写进 offset。
- **一致性读靠闪回**：`getSnapshotSelect()` 返回 `SELECT * FROM "S"."T" AS OF SCN <snapshotScn>`（`:314-323`）。所以**数据读取阶段不需要持锁**，一致性由 DM 闪回保证。
- **快照/增量衔接**：快照结束后 streaming 从同一个 SCN 起步（`LogMinerStreamingChangeEventSource.java:131` `startScn = offsetContext.getScn()`）。**没有 watermark、没有 low/high watermark 去重**，靠"闪回 SCN == 起始挖掘 SCN"这一个点对齐。
- **无分片**：`getSnapshotSelect` 就是整表一条 `SELECT *`，没有主键 chunk splitting，没有并行快照。Flink 侧同样没有（见 §6 与 §Flink 部分）。
- **DDL 事件来源**：`getCreateTableEvent()` 调 `SELECT DBMS_METADATA.GET_DDL('TABLE', ..., ...) FROM DUAL`（`:277-278`），把 DDL 文本作为 CREATE 事件写进 schema history。
- **有个 Oracle 遗留的自旋**：`determineSnapshotOffset` 里 `do { currentScn = getCurrentScn(ctx); } while (areSameTimestamp(latestTableDdlScn, currentScn));`（`:143-146`），`areSameTimestamp` 用的是 `SELECT 1 FROM DUAL WHERE SCN_TO_TIMESTAMP(a) = SCN_TO_TIMESTAMP(b)`（`:183`），`getLatestTableDdlScn` 用 `TIMESTAMP_TO_SCN(MAX(last_ddl_time)) FROM all_objects`（`:199-207`）。这三个函数/视图在 DM 上的行为 **未证实**——若 DM 不支持 `SCN_TO_TIMESTAMP`，这里会抛异常或死循环。
- `getSnapshottingTask()`（`:69-85`）：有已完成的历史 offset 就跳过快照；否则按 `snapshot.mode` 决定是否带数据。

---

## 4. DDL / schema 变更

**增量阶段：完全不处理。** 一手证据，`debezium-connector/.../logminer/LogMinerQueryResultProcessor.java:170-177`：

```java
// DDL
if (operationCode == RowMapper.DDL) {
    // todo: DDL operations are not yet supported during streaming while using LogMiner.
    historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, 0, redoSql);
    LOGGER.info("DDL: {}, REDO_SQL: {}", logMessage, redoSql);
    continue;
}
```

只记日志、只喂给可选的 `HistoryRecorder`（默认实现是 `NeverHistoryRecorder`，什么都不做），然后 `continue`。**不 dispatch schema change event，不更新 in-memory schema。**

后果（issue #12「经测试不支持 ddl」，一手用户报告）：新建表后插数据 → 报"表结构找不到"；加列/删列/改类型后，内存里的 `Table` 仍是旧结构，`LogMinerDmlParser` 按旧列数解析 redo SQL → 列错位或全 null。

**DDL 解析器是存在的，但只在快照/history recovery 路径用**：
- `DamengDatabaseSchema.getDdlParser()` 返回 `new OracleDdlParser()`（`DamengDatabaseSchema.java:53-57`）。
- `OracleDdlParser` + `antlr/listener/` 下 12 个 listener（`AlterTableParserListener`、`CreateTableParserListener`、`ColumnDefinitionParserListener` 等）**全部是 Debezium `debezium-ddl-parser` 里 Oracle grammar 的 listener 移植，语法本身来自依赖 jar，本仓库没有自己的 `.g4` 文件**（`find . -name '*.g4'` 无结果）。
- `OracleDdlParser.java:143` 有一处 `throw new UnsupportedOperationException("Not implemented yet")`。
- `ColumnDefinitionParserListener.java:198,228` 对不认识的类型直接 `throw new IllegalArgumentException("Unsupported column type: ...")` —— **DM 特有类型（`BIT`、`TEXT`、`TIME WITH TIME ZONE`、`INTERVAL`）走 Oracle grammar 大概率在这里炸**。未实测证实。

**schema history topic 是有的**（`HistorizedRelationalDatabaseSchema`），但因为增量阶段不产生 schema change event，history 里只会有快照期的 CREATE。

---

## 5. 已知问题清单（一手：issues + TODO 注释 + commit）

### 5.1 丢事件 / 重复事件 / 顺序

| 问题 | 来源 | 说明 |
|---|---|---|
| **HashMap 导致事务乱序丢数据** | issue #14（closed）+ PR #15 + commit `fc7b5fe`（"fix:新版本批量提交事务丢数据的问题"） | 用户实测：DM 8.4 安全版批量提交时返回的事务 ID（如 `0000000000009FDF`..`E3`）经 `HashMap`/`HashSet` 后**顺序被打乱**，导致 commit 时 SCN 乱序，触发 `"Transaction {} was already processed, ignore"` 分支从而丢数据。修复方式是改用 `LinkedHashMap`/`LinkedHashSet` —— 当前代码 `TransactionalBuffer.java:59`（`this.transactions = new LinkedHashMap<>();`）与 `:268`（`Set<String> transactionsToCommit = new LinkedHashSet<>();`）已是修复后状态。**教训：redo 中的事务顺序必须显式保序，不能依赖任何哈希容器的迭代顺序。** |
| 重复提交的判定用 `>` 而非 `>=`，作者自己承认可能重复 | `TransactionalBuffer.java:168-169` 注释：`// Currently we cannot use ">=", because we may lose normal commit which may happen at the same time. TODO use audit table to prevent duplications` | 同 SCN 的两个 commit 无法区分 → 要么丢要么重。**这是 at-least-once 都保不住的地方。** |
| **超时自动提交**（DM 特有 hack） | `TransactionalBuffer.java:254-288` `checkAndAutoCommitTransactions()`，配置 `debezium.source.transaction.auto.commit.timeout.ms` 默认 **1000ms** | 注释原文：「即使 LogMiner 没有捕获到 COMMIT 事件，操作也能被正确处理和提交」。**即 DM 的 `V$LOGMNR_CONTENTS` 有时不返回 COMMIT 记录**，作者用"1 秒没更新就当它提交了"来兜底。这直接破坏事务原子性：超过 1 秒的事务会被拆开发出去，且**回滚的事务已经发出去了收不回来**。 |
| 未提交事务全在内存 | `TransactionalBuffer` 无持久化 | 进程崩溃 → buffer 中所有事务丢失；重启后从 offset 重新挖，但 offset 因为 buffer 非空一直没前进（`LogMinerStreamingChangeEventSource.java:214-217`），理论上能重放——**但代价是长事务期间位点完全不动**。 |
| 长事务被"遗弃" | `TransactionalBuffer.abandonLongTransactions()`（`:322-359`）+ `LogMinerStreamingChangeEventSource.abandonOldTransactionsIfExist()`（`:239-251`） | 超过 `log.mining.transaction.retention.hours` 的事务被**静默丢弃**（只 WARN），且 offset 被强推到 thresholdScn → **确定性丢数据**。默认值 0 = 关闭，但一旦开启就是这个行为。 |
| Flink pipeline 队列满静默丢事件 | `flink-cdc-connector/.../pipeline/DamengPipelineSourceReader.java:176-181`（队列容量 10000，`:43`） | `LOG.warn("Event queue full, dropping event")` 然后丢。**背压下静默数据丢失。** |
| Flink pipeline checkpoint 不含位点 | `pipeline/DamengPipelineSplit.java`（整类只有 `SPLIT_ID` 常量）、`DamengPipelineSourceReader.java:91-97`、`DamengPipelineSplitEnumerator.java:28-33`（restore 构造器丢弃 checkpoint 参数） | Flink checkpoint 里没有 SCN，恢复只能靠 Debezium 自己的 `offset.storage` 文件；而 `DamengPipelineSourceFactory` 根本没设 `offset.storage`。 |

### 5.2 类型映射 / 精度

| 问题 | 来源 |
|---|---|
| **`io.debezium.time.Timestamp` 收到 String 报 `Invalid Java object ... with type INT64: class java.lang.String`**，用户指出触发条件是 TIMESTAMP 列的 default value 是 `SYSDATE` | issue #18（open） |
| **整数被解析成 String 导致每列 value 变 null** | issue #4（open）+ PR #11 / commit `118c6d4`「修复整数转换逻辑以处理字符串输入」。issue #4 原文质疑「为何默认使用 `LogMinerDmlParser`，其 `SimpleDmlParser` 存在问题吗？」默认值见 `DamengConnectorConfig.java:157-164`（`log.mining.dml.parser`，默认 `FAST` = `LogMinerDmlParser`） |
| **DM TIMESTAMP 格式转换** | commit `491bf14`「fix: 修复达梦 TIMESTAMP 格式转换问题」，代码在 `DamengValueConverters.java:496-527`，特判 `TIMESTAMP'...'` / `TO_TIMESTAMP(...)` / `TO_DATE(...)` 三种字面量 |
| `Types.FLOAT`/`NUMERIC`/`STRUCT` 一律映射成 `SchemaBuilder.string()` | `DamengValueConverters.java:104-110` |
| variable-scale decimal 特殊值未处理 | `DamengValueConverters.java:425` `// TODO Need to handle special values, it is not supported in variable scale decimal` |
| FLOAT 精度语义错（bit vs decimal digits） | `antlr/listener/ColumnDefinitionParserListener.java:165,175` `// TODO float's precision is about bits not decimal digits` |
| **空间/未知类型直接置 null** | `logminer/parser/SimpleDmlParser.java:97` `dmlContent.replaceAll("= Unsupported Type", "= null"); // todo address spatial data types`；`LogMinerDmlParser.java:66-67` 定义 `UNSUPPORTED` / `UNSUPPORTED_TYPE` 常量 |
| **before 镜像解析不全** | issue #10（open）「数据变更后，before 里的数据解析不全」——LogMiner 只在 supplemental logging 足够时才有完整 before |
| 更新事件不工作 | issue #13（open）「经测试不支持更新数据」（无正文） |
| Flink 侧「改一个字段其他字段变 null」 | issue #17（open），用 `dameng-cdc` SQL connector 同步到另一张 DM 表 |
| **Flink pipeline 的类型映射几乎全丢** | `flink-cdc-connector/.../pipeline/DamengSchemaConverter.java:51-92` 只按 Connect 原生类型名映射（int8/16/32/64、float32/64、boolean、string、bytes，其余一律 `STRING()`），**完全忽略 Debezium logical type name**。后果：DECIMAL→`BYTES`（精度 scale 全丢）、DATE/TIME/TIMESTAMP→裸 `INT`/`BIGINT` epoch、`Bits`→`BYTES`、INTERVAL 无映射。且 `DamengRecordData.java` 里 `getZonedTimestamp`/`getLocalZonedTimestampData`/`getArray`/`getMap`/`getRow` **全部 `return null;`**（`:162-202`），`getTimestamp` 忽略 precision 参数只用 `fromMillis`（`:149-159`），`getBinary` 对 base64 字符串不解码直接 `String.valueOf(val).getBytes()`（`:174-184`）→ **BLOB 内容损坏**。 |
| Flink metadata 路径与 data 路径类型不一致 | `pipeline/DamengMetadataAccessor.java:127-181` 保留了 `DECIMAL(p,s)`/`CHAR(n)`/`VARCHAR(n)`/`TIMESTAMP(6)`，与上面的 `DamengSchemaConverter` 结论不同 → 同一张表两条路径给出两套 schema。且它把 `Types.BIT` 和 `Types.BOOLEAN` 都映射成 `BOOLEAN()`（`:167-169`）→ DM `BIT(n)`, n>1 被错误折叠 |

### 5.3 LOB / 大字段

- `RowMapper.getSqlRedo()`（`logminer/RowMapper.java:139-166`）处理 `CSF=1` 续行拼接（LogMiner 单行 `SQL_REDO` 上限 4000 字节）。有硬编码上限：超过 `lobLimitCounter` 就 `LOGGER.warn("LOB value was truncated due to the connector limitation of {} MB", 40)` 然后 `break`（`:154-157`）—— **大于 ~40MB 的 LOB 被静默截断，只有一条 WARN**。

### 5.4 运行/环境类

| 问题 | 来源 |
|---|---|
| `DBMS_LOGMNR.add_logfile` 报「无法添加重复的日志文件」 | issue #6（open）。根因指向 `LogMinerHelper.setRedoLogFilesForMining()` 的去重逻辑（`:495-540`）在 DM 上不奏效；注意还有一个平行的、几乎全被注释掉的 `setDamengRedoLogFilesForMining()`（`:548-580`）用不同的 `DBMS_LOGMNR.ADD_LOGFILE('<file>')` 语法，**并未被主流程调用** |
| `start_logmgr` 报「没有配置日志文件」 | issue #8（open），DM 8.1.1 企业版 |
| 启动 `StackOverflowError` | issue #9（open），debezium-server 场景 |
| 每轮 `SELECT COUNT(*) FROM V$LOGMNR_CONTENTS` | `LogMinerStreamingChangeEventSource.java:193-196`，纯 debug 用途但无条件执行 → **性能瓶颈** |
| 错误识别仍在匹配 `ORA-xxxxx` 错误码 | `SqlUtils.java:445-451`（`ORA-03135`/`ORA-12543`/`ORA-00604`/`ORA-01089`/`ORA-00600`）；DM 抛的是 `dm.jdbc.driver.DMException` 与中文消息（见 issue #6 堆栈）→ **连接故障重连判定在 DM 上基本失效** |
| `EXCLUDED_SCHEMAS` 是 Oracle 的系统 schema 列表 | `DamengConnectorConfig.java:172-174`（`appqossys`/`ctxsys`/`dvsys`/`xdb`...），对 DM 无意义，且漏了 DM 的系统 schema |
| 默认端口 1528（Oracle）而非 DM 的 5236 | `DamengConnectorConfig.java:175`（Flink 侧 `DamengTableSourceFactory.java:38` 又用 5236 → 两处不一致） |
| DM 版本校验只查 major version == 8 | `flink-cdc-connector/.../DamengValidator.java:21,29-47`；错误信息还写着 "connecting to **Oracle**"（`:45`） |
| 每次调用都 `DriverManager.registerDriver(new DmDriver())` | `DamengValidator.java:52`、`DamengMetadataAccessor.java:185` |

---

## 6. 依赖树重量

未执行 `mvn dependency:tree`（沙箱内无网络下载 Maven 仓库的保证）；以下是**基于 `pom.xml` 的直接依赖手工分析**，传递依赖标注为推断。

### 根 `pom.xml`
纯聚合 POM，`packaging=pom`，两个 module：`debezium-connector`、`flink-cdc-connector`。version `2025.1.1`，Java 8 目标（`java.version=1.8`）。构建期插件：checkstyle 3.3.1（`checks.xml`，`failOnViolation=true`）、spotbugs 4.8.5.0（`findbugs.xml`）、gpg、central-publishing。**注意 `<licenses>` 块里写着 `The MIT License` 但 URL 指向 `https://projectlombok.org/LICENSE`（复制粘贴错误）**。

### `debezium-connector/pom.xml`（直接依赖，共 8 个）

| 依赖 | 版本 | 归属 |
|---|---|---|
| `io.debezium:debezium-api` | 1.9.8.Final | **Debezium 核心必需** |
| `io.debezium:debezium-embedded` | 1.9.8.Final | **Debezium 核心必需**（传递引入 `debezium-core`、`kafka-connect-api`、`kafka-clients`——本项目代码直接 import `org.apache.kafka.connect.*`，如 `TransactionalBuffer.java:14`） |
| `io.debezium:debezium-ddl-parser` | 1.9.8.Final | **Debezium 必需**（传递引入 **ANTLR 4 runtime** + Oracle/MySQL grammar；`antlr/` 下的 listener 全依赖它） |
| `com.github.jsqlparser:jsqlparser` | 2.1 | `SimpleDmlParser`（legacy DML parser）用；**版本 2.1 非常老**（当前主线 4.x/5.x） |
| `com.google.protobuf:protobuf-java` | 3.8.0 | 用途不明——源码中未见 protobuf import。**疑似 Oracle XStream 遗留，可删** |
| `com.dameng:Dm8JdbcDriver16` | 8.1.1.49 | **DM JDBC 驱动**（见 §7） |
| `com.google.code.findbugs:findbugs-annotations` | 3.0.1 | 仅注解（`@SuppressFBWarnings`） |
| `org.slf4j:slf4j-api` + `slf4j-simple` | 2.0.13 | **`slf4j-simple` 是实现绑定，被打进库里 = 反模式**，会和宿主的日志实现冲突 |

另外代码里直接用了 **Guava**（`DamengConnectorTask.java:8-9` import `com.google.common.collect.Maps/Sets`），但 pom 里**没有显式声明 guava** → 靠传递依赖，随时会 `NoClassDefFoundError`。

**结论：Debezium 侧依赖确实很轻** —— 除 Debezium 三件套 + ANTLR + Kafka Connect API 外，几乎没有额外负担。`jsqlparser` 和 `protobuf-java` 是可以砍掉的。

### `flink-cdc-connector/pom.xml`（直接依赖，共 15 个）

**Flink 带进来的（全部 `provided`，不进 shade jar）**：`flink-streaming-java`、`flink-core`、`flink-runtime`、`flink-table-common`、`flink-table-api-java-bridge`、`flink-table-runtime`、`flink-table-planner-loader`，均 1.18.0。

**Flink CDC 带进来的（非 provided，会进 jar）**：`org.apache.flink:flink-connector-debezium` 3.1.0、`org.apache.flink:flink-cdc-common` 3.1.0。

**可疑/多余**：
- `org.apache.flink:flink-connector-jdbc:3.2.0-1.19` —— **注意版本后缀 `-1.19`，与 flink 1.18.0 不匹配**；
- `mysql:mysql-connector-java:8.0.33` —— **一个达梦 connector 依赖 MySQL 驱动**，且是已弃用的 `mysql` groupId（应为 `com.mysql:mysql-connector-j`）。**纯冗余，还带 GPL-2.0-with-FOSS-exception 的许可负担**；
- `io.debezium:debezium-core` 1.9.8.Final 被声明为 **`test` scope**，但主代码路径经 `debezium-connector` 传递已有；
- `slf4j-simple` 又来一遍。

打包用 `maven-shade-plugin` 3.5.2（`:141`）。

---

## 7. 许可证与"可借鉴边界"

### 参考项目本身

- **仓库 `LICENSE` = MIT License, Copyright (c) 2024 The Devlive Software Foundation**（`LICENSE:1-3`；`gh api` 返回 `"license": {"key":"mit","spdx_id":"MIT"}` 一致）。
- **但每个 Java 源文件头部仍写着**：
  ```
  /*
   * Copyright Debezium Authors.
   * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
   */
  ```
  （例：`Scn.java:1-5`、`TransactionalBuffer.java:1-5`、`SqlUtils.java:1-5`、`DamengSnapshotChangeEventSource.java:1-5`——几乎全部文件）。多个类还保留了原作者 `@author Chris Cranford` / `@author Gunnar Morling` / `@author Andrey Pustovetov`（Debezium 核心 committer）。
- **实务判断**：这些文件的**上游真实许可是 Apache-2.0（Debezium）**，devlive 只是在 fork 上加了 MIT 的仓库级 LICENSE。这不是"MIT 授权你随便用"，而是"Apache-2.0 代码被重新分发，仓库级 LICENSE 声明与文件级声明冲突"。
  - 好消息：**Apache-2.0 和 MIT 都是宽松许可，都允许我们借鉴/复制代码。**
  - 必须做的：**保留原始版权声明与许可声明**（Apache-2.0 §4(a)(b)(c) 要求保留 NOTICE、版权、许可文本，并标注修改）。**不能把文件头的 Debezium 版权声明删掉换成我们自己的。**
  - 建议：**若要复制代码，直接从 Debezium 上游（Apache-2.0，来源清晰）复制，而不是从这个 fork 复制** —— 避免继承它的许可声明混乱，也避免继承它的 bug。
- **本项目的"发明"部分**（DM 特有的 SQL：`V$ARCH_FILE`/`CLSN`/`ARCH_LSN`、`RLOG_APPEND_LOGIC`、auto-commit-timeout hack、Flink pipeline 模块）是 devlive 原创，受 MIT 覆盖，借鉴需保留 MIT 版权行。

### DM JDBC 驱动

- 坐标 `com.dameng:Dm8JdbcDriver16:8.1.1.49`，**确实在 Maven Central 上**（`https://repo1.maven.org/maven2/com/dameng/Dm8JdbcDriver16/8.1.1.49/` 返回 200）。
- 其 **POM 元数据自称 `The Apache License, Version 2.0`**，developer `dmtech@dameng.com`，SCM 指向 `https://gitee.com/dmedu/dm-jdbc-jars`（一手：上述 POM 内容）。
- **⚠️ 需自行核实**：POM 里的 license 字段是发布者填的元数据，**不等于 jar 内实际 EULA**。达梦 JDBC 驱动的实际分发条款、以及"随我们的 connector 一起分发"是否被允许，**本次调研未证实**。建议：
  - 默认按 **`provided`/`optional` scope** 处理，让用户自己提供驱动，不打进我们的 fat jar；
  - 若要打包分发，需先取得达梦书面/明确的分发许可。
- 参考项目把它声明成**默认 compile scope**（`debezium-connector/pom.xml:49-53`、`flink-cdc-connector/pom.xml:124-128`），并用 shade 打进 jar —— **这正是我们要避开的做法**。
- 另注：`flink-cdc-connector` 引入的 `mysql:mysql-connector-java:8.0.33` 是 **GPL-2.0 with FOSS Exception**，把它 shade 进发行包会引入不必要的许可复杂度。

---

## 8. 可借鉴的 / 要避开的（核心交付物）

### ✅ 可借鉴

| # | 内容 | 出处 | 为什么 |
|---|---|---|---|
| A1 | **DM 版 LogMiner 的前置条件清单**：`COMPATIBLE_MODE=2` + `ARCHIVELOG` + `ENABLE_FLASHBACK=1` + **`RLOG_APPEND_LOGIC=1`** | `README.md` 第 2–5 节 | 这是全项目最有价值的一手情报。尤其 `RLOG_APPEND_LOGIC=1`——不开则 `V$LOGMNR_CONTENTS` 恒为空。我们应把这四项做成**启动时的强制前置校验**（参考项目只在 README 里写，代码里不校验——见 A7）。 |
| A2 | **DM 系统视图到 Oracle 概念的映射表** | `SqlUtils.java:130-133`（current SCN = `SELECT CLSN FROM V$ARCH_FILE WHERE STATUS='ACTIVE'`）、`:149`（oldest = `SELECT ARCH_LSN FROM SYS.V$ARCH_FILE ORDER BY CREATE_TIME LIMIT 1`）、`LogMinerHelper.java:621`（日志清单 = `SELECT PATH,ARCH_LSN,CLSN,STATUS FROM SYS.V$ARCH_FILE`）、`LogMinerStreamingChangeEventSource.java:290,298,330`（`V$ARCH_FILE`/`V$ARCHIVED_LOG`） | DM 没有 `V$DATABASE.CURRENT_SCN`、没有 `V$LOG`/`V$LOGFILE`（在线 redo 视图），一切都落在 `V$ARCH_FILE` 上，字段是 `ARCH_LSN`(first) / `CLSN`(next) / `STATUS`('ACTIVE' 表示当前)。**这套映射直接可用，省我们几天摸索。** |
| A3 | **位点编码方式：单一 `BigInteger` SCN + `commit_scn` 双字段** | `Scn.java:16-34`、`SourceInfo.java:18-22`、`DamengOffsetContext.java:102-120` | 结构极简、易序列化、易跨 Flink checkpoint。**但要改掉 `Scn.MAX = -2` 的坑（见 B1）。** |
| A4 | **LogMiner 结果集 `CSF=1` 续行拼接** | `RowMapper.java:139-166` | `V$LOGMNR_CONTENTS.SQL_REDO` 单行上限 4000 字节，长 SQL/LOB 会分多行返回，必须按 `CSF` 标志顺序拼接。这是必踩的坑，直接抄逻辑（但把 40MB 硬上限改成可配 + 显式报错，见 B6）。 |
| A5 | **`LOG_MINING_FLUSH` 表强制刷 redo 的技巧** | `SqlUtils.java:66-68`、`LogMinerHelper.java:187-200` | 建一张 `LOG_MINING_FLUSH(LAST_SCN NUMBER)`，每轮 UPDATE+COMMIT 一次，逼数据库把 log writer buffer 落盘，避免"最新变更还在内存里挖不到"。**思路可借鉴**，但要评估 DM 上是否真有必要（未证实）。 |
| A6 | **事务顺序必须用保序容器** | issue #14 + commit `fc7b5fe` + `TransactionalBuffer.java:59,268` | DM 8.4 批量提交返回的事务 ID 经哈希容器迭代会乱序 → SCN 乱序 → 丢数据。**我们的事务缓冲区从第一天起就用 `LinkedHashMap` 或显式按 SCN 排序的结构。这是一条用血换来的教训。** |
| A7 | **在 Debezium 框架里的落位方案整体正确** | `DamengConnectorTask.java:53-131` 全文 | 复用 `BaseSourceTask` + `ChangeEventQueue` + `EventDispatcher` + `ChangeEventSourceCoordinator` + `RelationalSnapshotChangeEventSource` + `HistorizedRelationalDatabaseSchema` 的整套骨架是对的，**不要自己造框架**。这份 wiring 代码可以作为我们的起点模板。 |
| A8 | **DM TIMESTAMP 字面量的三种形态解析** | `DamengValueConverters.java:496-527`（`TIMESTAMP'...'` / `TO_TIMESTAMP(...)` / `TO_DATE(...)`）+ commit `491bf14` | DM 的 redo SQL 里日期时间以字面量文本出现，形态不止一种，必须都覆盖。 |
| A9 | **Flink table connector 的选项命名** | `DamengTableSourceFactory.java:30-68`（identifier `dameng-cdc`；`hostname`/`port`(5236)/`username`/`password`/`database`/`table`/`server-id`/`server-name`） | 与 Flink CDC 社区惯例一致，用户迁移成本低，直接沿用（但要**补上 `scan.startup.mode` / `scan.incremental.snapshot.*` 等它缺失的选项**）。 |

### ❌ 要避开

| # | 做法 | 出处 | 会导致什么 |
|---|---|---|---|
| B1 | **`Scn.MAX = new Scn(BigInteger.valueOf(-2))`** | `Scn.java:22` + `compareTo` `:132-144` | `compareTo` 走纯数值比较，所以这个"MAX"比任何真实 SCN 都**小**。`LogMinerHelper.getScnFromString()`（`:640-646`）在字段为空时返回 `Scn.MAX` 当作"当前日志的 nextScn = 无穷大"，但 `getOnlineLogFilesForOffsetScn` 里 `logFile.getNextScn().compareTo(offsetScn) >= 0` 就会判假 → **日志文件被错误排除**。→ 我们用 `Optional<Scn>` 或独立的 `isUnbounded` 标志，不要用魔数负值。 |
| B2 | **把 `START_LOGMNR` 的 SCN 范围参数注释掉，改在 `V$LOGMNR_CONTENTS` 上做 `WHERE SCN > ? AND SCN <= ?`** | `SqlUtils.java:210-230`（返回硬编码语句）vs `:259`（WHERE 子句） | LogMiner 会挖掘**整个已 add 的日志文件集合**，然后由数据库端做全量过滤。批大小配置形同虚设，`V$LOGMNR_CONTENTS` 物化开销随日志量线性增长。叠加 `:193-196` 每轮无条件 `SELECT COUNT(*) FROM V$LOGMNR_CONTENTS` → **两次全量扫描**。→ 我们必须查清 DM 的 `DBMS_LOGMNR.START_LOGMNR` 到底支持哪些参数（`StartScn`/`EndScn`/`StartTime`/`EndTime`），**把范围下推到 START_LOGMNR**。这是 §未证实 里的头号待查项。 |
| B3 | **"超时 1 秒就当事务已提交"** | `TransactionalBuffer.checkAndAutoCommitTransactions()` `:254-288`，默认 `debezium.source.transaction.auto.commit.timeout.ms=1000` | 彻底破坏事务原子性：>1s 的事务被拆成多批发出；**回滚的事务已经发出去了无法撤回**（`rollback()` `:296-317` 只能移除仍在 buffer 里的）。→ 我们要么查清 DM 为何不返回 COMMIT（可能是 `RLOG_APPEND_LOGIC` 或 `OPERATION_CODE` 映射问题），要么显式做成 opt-in 的"降级模式"并在文档里标注**破坏一致性**。**绝不能作为默认行为。** |
| B4 | **增量阶段直接 `continue` 丢弃所有 DDL** | `LogMinerQueryResultProcessor.java:170-177` | issue #12 实证：新建表插数据报"表结构找不到"；加列后按旧 schema 解析 redo → 列错位/全 null。→ 我们必须在 streaming 阶段 dispatch schema change event 并更新 in-memory `Table`。**DDL 处理不是 v2 功能，是正确性前提。** |
| B5 | **直接复用 Debezium 的 Oracle ANTLR grammar 解析 DM DDL** | `DamengDatabaseSchema.java:53-57` → `OracleDdlParser`；`ColumnDefinitionParserListener.java:198,228` 遇到未知类型 `throw new IllegalArgumentException("Unsupported column type: ...")` | DM 特有类型（`BIT`、`TEXT`、`TIME WITH TIME ZONE`、`INTERVAL`、`CLASS`）不在 Oracle grammar 里 → **抛异常直接把 connector 打挂**，而不是降级。→ 我们要么写 DM 自己的 grammar，要么在 listener 层做"未知类型降级为 STRING + WARN"的兜底，**永远不要因为一个未知列类型让整个任务失败**。 |
| B6 | **LOB 超过 40MB 静默截断** | `RowMapper.java:154-157`（只有一条 `LOGGER.warn`） | 数据静默损坏，下游无从察觉。→ 我们应该**抛异常或发出显式的 truncation marker 字段**，让下游能判定。 |
| B7 | **重复 commit 判定用 `>` 且作者承认无解** | `TransactionalBuffer.java:168-169` 的 TODO | 同 SCN 的并发 commit 无法区分 → 丢或重二选一。→ 我们的位点必须比"单一 SCN"更细：至少 `(SCN, XID)` 或 `(SCN, ROW_ID/RS_ID+SSN)` 复合位点，才能做到幂等续传。**这是位点设计的核心约束。** |
| B8 | **长事务遗弃机制静默丢数据并强推 offset** | `TransactionalBuffer.abandonLongTransactions()` `:322-359` + `LogMinerStreamingChangeEventSource.java:239-251`（`offsetContext.setScn(thresholdScn)`） | 只 WARN 不报错，位点被推过未处理的数据。→ 要么报错停机让人介入，要么把被遗弃事务写进 dead-letter，**不能悄悄推位点**。 |
| B9 | **`slf4j-simple` 作为 compile 依赖打进库** | `debezium-connector/pom.xml:64-68`、`flink-cdc-connector/pom.xml:119-123` | 库不应绑定日志实现，会与宿主（Kafka Connect / Flink）的日志系统冲突。→ 只依赖 `slf4j-api`。 |
| B10 | **DM JDBC 驱动 compile scope + shade 进发行 jar** | `debezium-connector/pom.xml:49-53`、`flink-cdc-connector/pom.xml:124-128` + shade plugin `:141` | 分发第三方商业驱动的许可风险（POM 自称 Apache-2.0，但实际 EULA 未证实）。→ 用 `provided`，文档指导用户自备驱动。 |
| B11 | **依赖冗余与版本错配** | `flink-cdc-connector/pom.xml`：`mysql-connector-java:8.0.33`（达梦 connector 依赖 MySQL 驱动，且 GPL）、`flink-connector-jdbc:3.2.0-1.19`（Flink 1.18 项目引 1.19 构建）；`debezium-connector/pom.xml`：`protobuf-java:3.8.0`（源码中无引用）、`jsqlparser:2.1`（版本极老）；Guava 被 import 但未声明 | 许可污染 + ClassNotFound/版本冲突。→ 依赖清单从零开始列，每一条都要能说清为什么。 |
| B12 | **Flink pipeline source 的 split 是空壳，checkpoint 不含位点** | `pipeline/DamengPipelineSplit.java`（全类只有 `SPLIT_ID` 常量）；`DamengPipelineSourceReader.java:91-97`（snapshotState 返回空 split）、`:107-113`（addSplits 忽略 split 内容）；`DamengPipelineSplitEnumerator.java:28-33`（restore 构造器把 checkpoint 参数直接丢掉）；`DamengEventFlinkSource.java:75-84`（序列化返回 `new byte[0]`） | **Flink 的 exactly-once/at-least-once 语义完全失效**，恢复只能靠 Debezium 的本地 offset 文件；而 `DamengPipelineSourceFactory` 根本没配 `offset.storage`。→ 我们的 split **必须携带 SCN 位点**，`snapshotState` 必须序列化它，restore 必须读回来。 |
| B13 | **Flink pipeline 队列满静默丢事件** | `DamengPipelineSourceReader.java:176-181`、`:223-225`（`ArrayBlockingQueue` 容量 10000，`:43`） | 背压 = 数据丢失。→ 用阻塞 `put()` 让背压传导，不要 `offer()` + drop。 |
| B14 | **Flink 类型映射忽略 Debezium logical type name** | `pipeline/DamengSchemaConverter.java:51-92`（`fieldNode` 参数传进来但从未使用），配合 `DamengRecordData.java:162-202` 的一堆 `return null;` | DECIMAL 精度全丢、DATE/TIME/TIMESTAMP 变裸整数、ZonedTimestamp/Array/Map/Row 直接 null、BLOB base64 不解码。→ 类型映射必须**以 Debezium logical schema name 为准**（`io.debezium.time.*`、`org.apache.kafka.connect.data.Decimal`、`io.debezium.data.Bits`…），而不是 Connect 原生类型。 |
| B15 | **保留大量 Oracle 死代码/死配置而不清理** | `DamengConnectorConfig.java`：`XSTREAM_SERVER_NAME:91`、`RAC_SYSTEM:144`、`RAC_NODES:151`、`PDB_NAME:52`、`EXCLUDED_SCHEMAS:172-174`（Oracle 系统 schema）、`DEFAULT_PORT=1528:175`；`SqlUtils.java:445-451` 匹配 `ORA-xxxxx` 错误码；`DamengValidator.java:45` 错误信息说 "connecting to Oracle"；`OracleDdlParser`/`OracleSnapshotContext`/`OracleDmlParser` 类名 | 配置面板全是用户看不懂的无效选项；**`ORA-` 错误码匹配在 DM 上完全失效 → 连接故障重连逻辑等于没有**（DM 抛 `dm.jdbc.driver.DMException` + 中文消息，见 issue #6 堆栈）。→ 从 Debezium Oracle 移植时，**每一处 Oracle 特定字符串都要显式过一遍**，宁可先删掉再按需加回。 |
| B16 | **`oldestFirstChangeQuery` 拼串 bug** | `SqlUtils.java:149-154`：`... ORDER BY CREATE_TIME LIMIT 1` 之后再 `append("AND FIRST_TIME >= ...")` | 只要 `log.mining.archive.log.hours != 0` 就生成语法错误的 SQL。→ 说明该配置项从未被测试过。**我们的每个配置项都要有对应测试。** |
| B17 | **快照阶段对每张表 `LOCK TABLE ... IN EXCLUSIVE MODE` + 依赖闪回 `AS OF SCN`** | `DamengSnapshotChangeEventSource.java:108-123`、`:314-323` | issue #16 一手用户反馈：「达梦官方不建议使用闪回，说是对性能影响较大」。且排他锁在大表/高并发库上不可接受。→ 我们应优先设计**无锁增量快照**（Flink CDC chunk splitting + low/high watermark），把闪回作为可选路径。 |

---

## 9. 未证实 / 未查明

1. **DM 的 `DBMS_LOGMNR.START_LOGMNR` 到底支持哪些命名参数**（`StartScn` / `EndScn` / `StartTime` / `EndTime` / `Options` 的可用取值）。参考项目把它们全注释掉了（`SqlUtils.java:212-229`），**无法判断是"DM 不支持"还是"作者没调通"**。→ **这是我们最该先验证的一件事**，直接决定捕获层的性能上限。
2. **DM 是否支持 `DBMS_LOGMNR.CONTINUOUS_MINE`**。`log.mining.continuous.mine` 配置项存在（`DamengConnectorConfig.java:105-110`），代码里也有 `isContinuousMining` 分支（`LogMinerStreamingChangeEventSource.java:134,182-190,256-271`），但从未被拼进实际的 START_LOGMNR 语句。
3. **`DBMS_LOGMNR_D.BUILD`（数据字典构建）在 DM 上是否可用**。`SqlUtils.BUILD_DICTIONARY`（`:48`）定义了但 `buildDataDictionary(connection)` 的两处调用都被注释掉（`LogMinerStreamingChangeEventSource.java:257-259, 266-268`）。因此 `CATALOG_IN_REDO` 策略实际等价于 `ONLINE_CATALOG` → **这可能正是 DDL 追踪做不了的根因之一**。
4. **DM 是否支持 `SCN_TO_TIMESTAMP` / `TIMESTAMP_TO_SCN`**。快照路径（`DamengSnapshotChangeEventSource.java:183,199`）和 `SqlUtils.diffInDaysQuery()`（`:420-426`，用于长事务遗弃判定）都依赖它们。若不支持，`determineSnapshotOffset` 的 `do-while` 自旋（`:143-146`）行为不明。
5. **`V$LOGMNR_CONTENTS` 的 `OPERATION_CODE` 在 DM 上的取值语义**。代码沿用 Oracle 的 1=INSERT / 2=DELETE / 3=UPDATE / 5=DDL / 7=COMMIT / 34=? / 36=ROLLBACK（`SqlUtils.java:262-267`、`LogMinerQueryResultProcessor.java:132`）。注意 §5 里的 auto-commit hack 暗示 **DM 可能不稳定地返回 code 7**——但没有一手证据说明原因。
6. **`ROW_ID` / `ROLL_BACK` 列在 DM 上的语义**（`LogMinerQueryResultProcessor.java:129-130` 读取，用于 `undoDmlOperation`）。
7. **DM JDBC 驱动 `Dm8JdbcDriver16` jar 内的实际 EULA**。Maven Central 上的 POM 元数据写 Apache-2.0，但未下载 jar 核实 LICENSE/NOTICE 文件内容，也未查证达梦官方对再分发的表述。
8. **`log.mining.batch.size.*` 的 SCN 跨度语义在 DM 上是否合理**。DM 的 SCN(LSN) 递增速率与 Oracle 不同，20000 的默认跨度对应多少变更量未知。
9. **issue #13「经测试不支持更新数据」的具体表现**（issue 正文为空，无堆栈无复现步骤）。
10. **参考项目是否有任何 CI 通过的集成测试**。`.github/workflows/publish-maven.yml` 只有发布流程；`debezium-connector/src/test/` 只有一个 `DamengDebeziumConnectorTest.java`（96 行），需要真实 DM 实例，**不是可自动化运行的测试**。→ 无法从测试用例反推预期行为。
11. **`DamengConnector`（`SourceConnector` 层）的确切基类**。文件存在（95 行）但本次未逐行读取，仅从 `DamengConnectorTask` 的引用推断。
12. **fork 关系**：无法确认该项目是从 Debezium 的哪个 commit / 哪个第三方 fork 派生（git 历史只有 18 个 commit，首个 commit `2d93813 feat: 支持 Debezium 连接器` 就是一次性导入全部代码，无上游 merge 记录）。
