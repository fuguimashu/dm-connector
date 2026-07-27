# 达梦数据库（DM8+）变更捕获接口调研

- **日期**：2026-07-27
- **问题**：达梦数据库（DM8+）提供哪些变更捕获接口，各自的能力与代价？
- **关联 Issue**：[#2](https://github.com/fuguimashu/dm-connector/issues/2)
- **口径**：准确性优先于完整性。没有找到权威依据的点一律标注「未证实」，并说明检索过程。

---

## 结论摘要 / TL;DR

达梦对外可用的变更捕获接口实际上只有**一类**：**基于物理逻辑日志（physical-logical log）的日志挖掘 LOGMNR**。它有两种调用形态，底层是同一套机制：

| 形态 | 调用方式 | 说明 |
|---|---|---|
| A. `DBMS_LOGMNR` 系统包 + `V$LOGMNR_CONTENTS` | 纯 SQL / JDBC | 门槛最低，Debezium 风格连接器普遍用这条路 |
| B. Logmnr JNI / C 客户端接口（`logmnr.jar` / `dmlogmnr_client.dll`） | 本地库调用 | 官方为应用程序直连提供，返回结构化 `LogmnrRecord`，可设并行度/缓存 |

除此之外，达梦官方的 **DMHS / DMDRS** 是**独立收费的商业复制产品**，走自研的日志直读通道，不通过 `DBMS_LOGMNR`，不是我们能直接建连接器的开放 API。

**代价（都必须付）**：

1. 必须开归档（`ARCH_INI=1` / `ALTER DATABASE ARCHIVELOG`）。
2. 必须开物理逻辑日志 `RLOG_APPEND_LOGIC ∈ {1,2,3,4}`（相当于 Oracle 的 supplemental logging）+ `RLOG_IGNORE_TABLE_SET=1`（或逐表 `ADD LOGIC LOG`）。
3. **官方文档明确：`DBMS_LOGMNR` 目前只支持分析归档日志**，不能挖在线 redo。这意味着 SQL 形态下的**延迟下界 = 归档切换周期**，这是本次调研对架构影响最大的一条。
4. 要拿到 UPDATE/DELETE 的完整前像，需要 `RLOG_APPEND_LOGIC=2`，代价是**归档量约翻倍、写负载明显变慢**（有实测，但仅为单一博客来源）。
5. 闪回（`ENABLE_FLASHBACK`）**不是日志挖掘的前提**，它是**初始全量快照**（`SELECT ... AS OF SCN`）的前提。这一点与社区连接器 README 的表述不同，见第 3 节。

**建议**：捕获层建在 **A（`DBMS_LOGMNR` + `V$LOGMNR_CONTENTS`）** 上作为 MVP，位点用 LSN（DM 的 `SCN` 列实为 LSN），并把「B（JNI 接口）」作为降低延迟/提高吞吐的第二阶段备选。详见「决策建议」。

---

## 1. 日志挖掘（LogMiner-class 接口）

### 1.1 包与过程名

官方 DM8 系统包手册《DBMS_LOGMNR 包》给出的完整签名（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package)）：

```sql
PROCEDURE ADD_LOGFILE (
    LOGFILENAME IN VARCHAR,
    OPTIONS     IN INT DEFAULT DBMS_LOGMNR.ADDFILE,
    RAFTID      IN INT
);

PROCEDURE START_LOGMNR (
    STARTSCN     IN BIGINT   DEFAULT 0,
    ENDSCN       IN BIGINT   DEFAULT 0,
    STARTTIME    IN DATETIME DEFAULT '1988/1/1',
    ENDTIME      IN DATETIME DEFAULT '2110/12/31',
    DICTFILENAME IN VARCHAR  DEFAULT '',
    OPTIONS      IN INT      DEFAULT 0
);

PROCEDURE REMOVE_LOGFILE (LOGFILENAME IN VARCHAR);
PROCEDURE END_LOGMNR();

FUNCTION COLUMN_PRESENT (SQL_REDO_UNDO IN BIGINT, COLUMN_NAME IN VARCHAR DEFAULT '') RETURN INT;
FUNCTION MINE_VALUE     (SQL_REDO_UNDO IN BIGINT, COLUMN_NAME IN VARCHAR DEFAULT '') RETURN VARCHAR;
```

`ADD_LOGFILE` 的 `OPTIONS` 常量：`DBMS_LOGMNR.NEW`（结束当前会话并新建）、`DBMS_LOGMNR.ADDFILE`、`DBMS_LOGMNR.REMOVEFILE`；`RAFTID` 仅 DMDPC 环境有意义（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package)）。

`START_LOGMNR` 的 `OPTIONS` 官方列出的取值（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package)）：

| 常量 | 值 | 含义 |
|---|---|---|
| `COMMITTED_DATA_ONLY` | 2 | 只输出已提交事务 |
| `DICT_FROM_ONLINE_CATALOG` | 16 | 用在线数据字典重构 SQL |
| `NO_SQL_DELIMITER` | 64 | 重构 SQL 不带结尾分隔符 |
| `NO_ROWID_IN_STMT` | 2048 | 重构 SQL 中不含 ROWID |

> ⚠️ 社区连接器 `dameng-connector` 代码中出现的 `DBMS_LOGMNR.CONTINUOUS_MINE`、`DBMS_LOGMNR.DDL_DICT_TRACKING`、`DBMS_LOGMNR_D.BUILD`、`SKIP_CORRUPTION` 等常量，**在达梦官方 `DBMS_LOGMNR` 文档中没有对应条目**，是从 Debezium Oracle 连接器 fork 时残留的 Oracle 符号（[二手，源码](https://github.com/devlive-community/dameng-connector/blob/dev/debezium-connector/src/main/java/org/devlive/connector/dameng/logminer/SqlUtils.java)）。**不要假设达梦支持 CONTINUOUS_MINE**——未证实。

包本身需要先创建：`SP_CREATE_SYSTEM_PACKAGES(1,'DBMS_LOGMNR');`（[一手-社区](https://eco.dameng.com/community/article/6bca9620fed06013fe6120e3d12e0377)、[一手-社区](https://eco.dameng.com/community/training/715c7abbbc3794def0d9a442bfec7d60)）。`SP_CREATE_SYSTEM_PACKAGES` 官方要求调用者具备 **DBA 角色**（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/package-overview.html)）。

### 1.2 调用流程（SQL 形态）

标准五步（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package)，实操验证见[一手-社区](https://eco.dameng.com/community/training/715c7abbbc3794def0d9a442bfec7d60)）：

```sql
-- 1. 找归档
SELECT NAME, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE# FROM V$ARCHIVED_LOG;
-- 2. 加文件
DBMS_LOGMNR.ADD_LOGFILE('/arch/ARCHIVE_LOCAL1_xxx.log');
-- 3. 开始分析
DBMS_LOGMNR.START_LOGMNR(OPTIONS => 2130);   -- 2+16+64+2048
-- 4. 读结果
SELECT OPERATION_CODE, SCN, SQL_REDO, TIMESTAMP, SEG_OWNER, TABLE_NAME FROM V$LOGMNR_CONTENTS;
-- 5. 结束
DBMS_LOGMNR.END_LOGMNR();
```

**关键限制（官方原文）**：「目前 DBMS_LOGMNR 只支持对归档日志进行分析」（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package)）。另：不支持 DM MPP 环境；DMDPC 下 `ADD_LOGFILE` 只能添加同一节点的日志。

`V$LOGMNR_CONTENTS`、`V$LOGMNR_LOGS`、`V$LOGMNR_PARAMETERS` 三个视图是**会话级**的，其他会话查不到本会话的分析结果（[二手/单一来源](https://www.modb.pro/db/26307)，与官方「START_LOGMNR 后查询 V$LOGMNR_CONTENTS」的会话内语义一致，但会话隔离这一点官方文档未直接给出——**部分未证实**）。

### 1.3 返回记录的结构

**SQL 形态**（`V$LOGMNR_CONTENTS`）：官方 `DBMS_LOGMNR` 页面点名的列有 `OPERATION_CODE`、`SCN`、`SQL_REDO`、`TIMESTAMP`、`SEG_OWNER`、`TABLE_NAME`（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package)）。社区实操另外用到 `SQL_UNDO`、`REDO_VALUE`、`UNDO_VALUE`、`SESSION_INFO`（[一手-社区](https://eco.dameng.com/community/article/7d43e5e008104dccb303cc8c5ffe5353)），社区连接器实际 SELECT 的是 `SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME, ROW_ID, ROLL_BACK`（[二手，源码](https://github.com/devlive-community/dameng-connector/blob/dev/debezium-connector/src/main/java/org/devlive/connector/dameng/logminer/SqlUtils.java)）。

> **未证实**：`V$LOGMNR_CONTENTS` 的**完整、逐列的官方列定义**。我检索了《DM8 系统管理员手册》附录 1（数据字典）——只有 SYSOBJECTS 等静态数据字典，无动态性能视图；官方「动态管理和性能视图」页把动态视图清单指向「附录 2」，但我未能定位到可直接抓取的附录 2 页面 URL。现场可用 `SELECT * FROM V$DYNAMIC_TABLES` / 直接 `SELECT * FROM V$LOGMNR_CONTENTS WHERE 1=0` 取列名来确认。

**操作码映射**（[一手-社区](https://eco.dameng.com/community/article/7d43e5e008104dccb303cc8c5ffe5353)）：

```
INTERNAL 0、INSERT 1、DELETE 2、UPDATE 3、BATCH_UPDATE 4、DDL 5、START 6、COMMIT 7、
SEL_LOB_LOCATOR 9、LOB_WRITE 10、LOB_TRIM 11、SELECT_FOR_UPDATE 25、LOB_ERASE 28、
MISSING_SCN 34、ROLLBACK 36、SEQ MODIFY 37、XA_COMMIT 38、UNSUPPORTED 255
```

**JNI/C 形态**（`LogmnrRecord`）——这是**结构化程度最高**的一条路，官方《Logmnr 接口使用说明》逐字段列出（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/logmnr-interface-instructions.html)）：

| 字段 | 官方释义 |
|---|---|
| `scn` | 当前记录的 **LSN** |
| `startScn` / `commitScn` | 当前事务的起始 / 截止 LSN |
| `timestamp` / `startTimestamp` / `commitTimestamp` | 记录 / 事务起始 / 事务截止时间 |
| `xid` | 当前记录的事务 ID 号 |
| `operation` / `operationCode` | 操作类型 / 操作码 |
| `rollBack` | 当前记录是否被回滚（1/0） |
| `segOwner` / `tableName` | 用户名 / 表名 |
| `rowId` | 对应记录的行号 |
| `rbasqn` / `rbablk` / `rbabyte` | 归档文件号 / 块号 / 块内偏移 |
| `dataObj` / `dataObjv` | 对象 ID / 对象版本号 |
| `sqlRedo` | 当前记录对应的 SQL 语句 |
| `rsId` / `ssn` / `csf` | 记录集 ID / 连续 SQL 标志 / 与 SSN 配合 |
| `subtabId` / `srcRaftId` / `status` | 子表 ID / 事务来源节点号 / 日志状态 |

JNI 侧函数流：`initLogmnr → createConnect → addLogFile → startLogmnr → getData → endLogmnr → closeConnect → deinitLogmnr`，另有 `setAttr` 可调 `LOGMNR_ATTR_PARALLEL_NUM`（2~16，缺省 2）、`LOGMNR_ATTR_BUFFER_NUM`（8~1024，缺省 8）、`LOGMNR_ATTR_CONTENT_NUM`（256~2048，缺省 256）、`LOGMNR_ATTR_TRX_END`（是否查找事务结束记录，缺省 1）、`LOGMNR_ATTR_TRX_WAIT_TIME`（缺省 60s）（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/logmnr-interface-instructions.html)）。

**前像 / 后像**：`sqlRedo` / `SQL_REDO` 是**重构后的 SQL 文本**，不是结构化的 before/after 列值对。要拿前像必须靠 `RLOG_APPEND_LOGIC` 配置（见第 2 节），而且社区明确指出：「对于有主键的表进行更新和删除操作，无法通过 `V$LOGMNR_CONTENTS` 视图得到必须的前映像」，除非把 `RLOG_APPEND_LOGIC` 设为 2（[一手-社区](https://eco.dameng.com/community/article/6bca9620fed06013fe6120e3d12e0377)）。同一来源还指出 `V$LOGMNR_CONTENTS` **没有 UNDO_SQL**、只有 REDO_SQL（与另一篇社区文列出 `sql_undo` 列冲突——见「置信度」节）。

**DDL**：有，`OPERATION_CODE = 5`。见第 7 节。

---

## 2. `RLOG_APPEND_LOGIC` 参数

### 2.1 它到底改了什么

它开启的是达梦所谓的**物理逻辑日志（physical-logical log）**：「物理逻辑日志是按照特定的格式存储的服务器的逻辑操作，专门用于 DBMS_LOGMNR 包挖掘获取数据库系统的历史执行语句」（[一手-社区](https://eco.dameng.com/community/article/6bca9620fed06013fe6120e3d12e0377)）。

达梦原生的 REDO 日志是**纯物理页级日志**——「记录了所有物理页的修改，基本信息包括操作类型、表空间号、文件号、页号、页内偏移、实际数据等」（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/backup-restore-introduction.html)）。物理页日志无法反解出行级 SQL，所以必须**额外**往 redo 里追加一份逻辑操作记录。

👉 **是的，它在效果上等同于 Oracle 的 supplemental / additional logging**：默认关闭，开了才有行级前像可用。这是**推测性的类比**（术语对应关系官方未做类比），但机制与后果一致，可作为团队内的心智模型。

官方 `DBMS_LOGMNR` 页面把它写成硬前置条件：「配置归档后，还需要将 dm.ini 中的 `RLOG_APPEND_LOGIC` 选项置为 1、2、3 或 4」（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package)）。

### 2.2 取值语义

（[一手-社区，达梦技术社区专文](https://eco.dameng.com/community/post/202411151126417HJ5M4VTSMA7VA30QF)；与官方 `DBMS_LOGMNR` 页取值范围一致）

| 值 | 含义 | 对 CDC 的意义 |
|---|---|---|
| 0 | 不启用（**新装库默认值**） | 挖不到任何行级变更，`V$LOGMNR_CONTENTS` 为空 |
| 1 | 有主键列时，UPDATE/DELETE 只记录主键列信息；无主键列则记录所有列 | 只能拿到主键作为定位符，**前像不全** |
| 2 | 无论是否有主键，UPDATE/DELETE 都记录**所有列**信息 | **完整前像**，Debezium 风格 `before` 需要它 |
| 3 | UPDATE 记录更新列信息 + ROWID；DELETE 只有 ROWID | 依赖 ROWID 定位，前像最省但最难用 |
| 4 | 只生成**事务及 DDL** 相关的逻辑日志 | 纯 DDL 订阅场景 |

配套参数：
- `RLOG_IGNORE_TABLE_SET`：1 = 记录**所有表**的物理逻辑日志；0 = 只记录指定表，需在建表/改表时 `ADD LOGIC LOG`。新装库默认 0（[一手-社区](https://eco.dameng.com/community/article/6bca9620fed06013fe6120e3d12e0377)；[一手-社区，flink-cdc 用法](https://eco.dameng.com/community/article/bb49aab895c5971f540faf822ffad9ef)）。**这条极易踩坑：只设 `RLOG_APPEND_LOGIC` 不设 `RLOG_IGNORE_TABLE_SET` 且未 `ADD LOGIC LOG`，挖出来还是空的。**
- `RLOG_APPEND_SYSTAB_LOGIC`：控制系统表是否记逻辑日志，社区建议生产设 0（[一手-社区](https://eco.dameng.com/community/article/6bca9620fed06013fe6120e3d12e0377)）。
- `LOGMNR_DELAY_ANALYZE`：官方提到当它 = 1 时 `COLUMN_PRESENT` / `MINE_VALUE` 不可用（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package)）。**该参数本身的完整定义未证实**（下文）。
- `LOGMNR_PARSE_LOB=1`：社区连接器维护者称配置后可在日志中拿到完整 LOB 文本（[二手，issue #16 评论](https://github.com/devlive-community/dameng-connector/issues/16)）——**低可信度/单一来源**，官方文档未检索到该参数。

### 2.3 动态还是静态？—— **结论有冲突，需实测**

| 来源 | 说法 |
|---|---|
| 达梦技术社区专文 | 「该参数为动态参数，可以在线修改」，`alter system set 'RLOG_APPEND_LOGIC'=1 both;`（[一手-社区](https://eco.dameng.com/community/post/202411151126417HJ5M4VTSMA7VA30QF)） |
| 社区「DM 日志挖掘」 | 用 `alter system set 'RLOG_APPEND_LOGIC'=1;`（在线）（[一手-社区](https://eco.dameng.com/community/article/6bca9620fed06013fe6120e3d12e0377)） |
| 达梦社区 flink-cdc 教程 | `sp_set_para_value(2,'RLOG_APPEND_LOGIC',2);` — scope=2 意味着**只改 ini、需重启**（[一手-社区](https://eco.dameng.com/community/article/bb49aab895c5971f540faf822ffad9ef)） |
| 归档挖掘实操 | 直接改 dm.ini 后**重启**（[一手-社区](https://eco.dameng.com/community/training/715c7abbbc3794def0d9a442bfec7d60)） |
| `dameng-connector` README | 「修改 dm.ini 后需要重启数据库服务才能生效」（[二手](https://github.com/devlive-community/dameng-connector)） |

**判断（推测）**：参数本身很可能是 SESSION/SYS 级动态参数，`alter system set ... both` 可即时生效；社区教程用 scope=2 + 重启只是保守做法。但**在小版本间可能有差异**，且「动态生效后已在飞的事务是否补记逻辑日志」无来源说明。→ **列为必须 spike 验证项**。可用 `SELECT PARA_NAME, PARA_TYPE, PARA_VALUE FROM V$DM_INI WHERE PARA_NAME='RLOG_APPEND_LOGIC'` 直接看 `PARA_TYPE`。

### 2.4 对源库写性能与日志量的影响

唯一找到的**带数字**的实测来自一篇 CSDN 博客（**低可信度 / 单一来源 / 非一手**，[链接](https://blog.csdn.net/weixin_56866387/article/details/139558813)）：100 万行表、10 轮 update+commit：

- 归档量：10.84 GB → 19.85 GB（**约 +83%，接近翻倍**）
- 耗时：5 分 12 秒 → 8 分 00 秒（**约 +54%**）

达梦社区文亦定性描述 `RLOG_APPEND_LOGIC=2` 会导致「日志量成倍增长」（[一手-社区](https://eco.dameng.com/community/article/6bca9620fed06013fe6120e3d12e0377)）。

**未证实**：达梦官方从未给出量化的性能开销数字。上述数字只能作为「量级参考」，不能写进对客户的 SLA。真实开销强依赖于取值（1 vs 2 vs 3）、表宽度、写放大比例。

---

## 3. 前置条件：归档日志 与 闪回

### 3.1 归档日志（必须，且是硬前提）

原因很直接：**`DBMS_LOGMNR` 只能分析归档日志文件**，联机 REDO 挖不了（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package)）。JNI 接口的前提也写着 `ARCH_INI = 1`（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/logmnr-interface-instructions.html)）。

开启方式（[一手-社区](https://eco.dameng.com/community/training/4559ddbeb3a7c6c4674db47e33d3bdd9)，与社区 flink-cdc 教程一致）：

```sql
ALTER DATABASE MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE ADD ARCHIVELOG 'DEST=/dm/arch, TYPE=LOCAL, FILE_SIZE=1024, SPACE_LIMIT=2048';
ALTER DATABASE OPEN;
```

对应 `dm.ini` 的 `ARCH_INI=1` + `dmarch.ini`（`ARCH_TYPE`/`ARCH_DEST`/`ARCH_FILE_SIZE`/`ARCH_SPACE_LIMIT`）。

**代价 / 运维负担**：

- **磁盘**：归档持续增长，`SPACE_LIMIT` 打满会阻塞库。开了 `RLOG_APPEND_LOGIC` 后归档量还要再翻一倍左右（见 2.4）。
- **归档清理与 CDC 的竞争**：DBA 的归档删除策略必须晚于 CDC 消费位点，否则连接器会丢日志。这是生产上最常见的断链原因（**推测**，基于同类 Oracle LogMiner 连接器的经验，无达梦专属来源）。
- **延迟**：⭐ 最关键的一条。因为只能挖归档，**变更必须等到联机 redo 归档落盘之后才可见**。`FILE_SIZE` 越大延迟越高。这基本决定了纯 SQL 形态的 `DBMS_LOGMNR` 做不了「亚秒级」CDC。
  - **未证实**：达梦是否有「强制切换归档」的联机命令可用来主动压低延迟（Oracle 的 `ALTER SYSTEM SWITCH LOGFILE` 等价物）。我检索了归档配置与 `DBMS_LOGMNR` 官方页，未找到明确语句。需 spike 验证。
- 若数据库已是实时主备（数据守护），归档配置会与 REALTIME 归档并存，配置更复杂（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/data-guard.html)）。

### 3.2 闪回（`ENABLE_FLASHBACK`）—— **不是日志挖掘的前提**

这是本次调研纠正的一个流行误解。

- 达梦官方 `DBMS_LOGMNR` 页面**只列了归档 + `RLOG_APPEND_LOGIC`** 两个前提，**没有提闪回**（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package)）。JNI 接口页同样只列 `ARCH_INI` + `RLOG_APPEND_LOGIC`（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/logmnr-interface-instructions.html)）。
- 达梦社区「DM 日志挖掘」把 `ENABLE_FLASHBACK=1` / `UNDO_RETENTION=3600` 单独归在「**数据恢复配合闪回**」小节下，而非日志挖掘前提（[一手-社区](https://eco.dameng.com/community/article/6bca9620fed06013fe6120e3d12e0377)）。
- `dameng-connector` README 把「开启闪回」列为 CDC 配置第 4 步（[二手](https://github.com/devlive-community/dameng-connector)）。**真实原因在其源码里**：初始快照阶段用的是闪回查询——`SELECT * FROM <table> AS OF SCN <snapshotOffset>`（[二手，源码 `DamengSnapshotChangeEventSource.java`](https://github.com/devlive-community/dameng-connector/blob/dev/debezium-connector/src/main/java/org/devlive/connector/dameng/DamengSnapshotChangeEventSource.java)）。
- 该项目 issue #16「是否可以不启用闪回？」中用户实测确认：**`snapshot.mode=schema_only` 时不开闪回也能正常触发变更同步**（[二手](https://github.com/devlive-community/dameng-connector/issues/16)）。

**结论**：`ENABLE_FLASHBACK` 是**「一致性初始快照」的前提**，不是「增量捕获」的前提。达梦官方闪回查询语法确实支持 `AS OF <SCN|LSN lsn>` 与 `AS OF TIMESTAMP`（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/flashback-query.html)）。

**闪回的代价**（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/flashback-query.html)）：

- 依赖回滚段 UNDO 记录；`UNDO_RETENTION` 缺省仅 **90 秒**。快照跑得比 `UNDO_RETENTION` 长，UNDO 被覆盖，快照直接失败 →「如果因保留时间超过了初始化参数 UNDO_RETENTION 所指定的值……那么就不能将表中数据恢复到指定的时间了」。大表全量快照必须把 `UNDO_RETENTION` 显著调大（社区示例 3600s），代价是 UNDO 表空间膨胀。
- 「闪回不能跨越修改了表结构的 DDL」——快照期间发生 DDL 会破坏快照。
- **DM MPP 环境不支持闪回查询**；**DMDPC 环境只支持基于时间的闪回，不支持基于 LSN 的闪回**。
- 不支持临时表、列存表、外部表、视图。

---

## 4. 权限模型

**官方明确的只有两条**：

1. 创建/删除系统包 `SP_CREATE_SYSTEM_PACKAGES` 需要 **DBA 角色**（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/package-overview.html)）。
2. 官方 JNI 示例全程「在 SYSDBA 用户下」操作（[一手-社区](https://eco.dameng.com/community/article/75a5eda7e2f592f1f1af0aeb7d3c75f7)）。所有社区实操教程也都用 SYSDBA。

**社区/第三方给出的一份较细的授权清单**（[二手，DataPipeline 用户手册](https://docs.datapipeline.com/wg7gzp4olbtub3vv/ygyy6k0s0rtsyc48/dgoa5i7ighva165c/nndhzv1crbqaig5a/)；我 WebFetch 该页只拿到导航框架，具体清单来自搜索摘要，**低可信度**）：

```
GRANT SELECT ON V$ARCHIVED_LOG;      GRANT SELECT ON V$LOGMNR_CONTENTS;
GRANT SELECT ON V$RLOG;              GRANT SELECT ON ALL_OBJECTS;
GRANT SELECT ON ALL_TAB_COLUMNS;     GRANT SELECT ON DBA_OBJECTS;
GRANT SELECT ON V$LOCK;              GRANT SELECT ON V$DM_INI;
GRANT SELECT ON V$SESSIONS;          GRANT SELECT ON ALL_CONS_COLUMNS;
GRANT SELECT ON DBA_CONS_COLUMNS;
```

再加上实际必需的（从 `dameng-connector` 实际访问对象反推，[二手，源码](https://github.com/devlive-community/dameng-connector)）：`V$ARCH_FILE`、`V$LOGMNR_LOGS`、`V$LOG`、`V$LOGFILE`、`V$VERSION`、`V$DATABASE`，以及 `EXECUTE ON SYS.DBMS_LOGMNR`、被捕获表的 `SELECT`（快照阶段）。

> **未证实（重要）**：**达梦是否存在一个「不含 DBA 角色」的最小 CDC 权限集**。我检索了达梦官方系统包手册、`DBMS_LOGMNR` 页、日志挖掘社区文，**均未给出 `DBMS_LOGMNR` 的 EXECUTE 授权说明或最小权限指引**；所有一手材料都直接用 SYSDBA。
>
> 保守结论：**按目前证据，生产上需要给 CDC 账号 DBA 级权限**（至少首次建包时需要）。这是一个真实的安全阻力点，值得在 spike 里逐条试探能否降权。

---

## 5. 位点语义（Offset / Position）

### 5.1 达梦用的是 LSN，不是 Oracle 意义上的 SCN

达梦官方《备份还原》给出的定义（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/backup-restore-introduction.html)）：

> 「LSN（Log Sequence Number）是由系统自动维护的 Bigint 类型数值，具有**自动递增、全局唯一**特性，每一个 LSN 值代表着 DM 系统内部产生的一个**物理事务**。」

四个关键 LSN：

| 名称 | 官方释义 |
|---|---|
| `CUR_LSN` | 系统已经分配的最大 LSN 值 |
| `FLUSH_LSN` | 已发起刷盘请求、但还没真正写入联机 REDO 文件的最大 LSN |
| `FILE_LSN` | 已经写入联机 REDO 日志文件的最大 LSN |
| `CKPT_LSN` | 检查点 LSN，所有 LSN ≤ CKPT_LSN 的物理事务修改的数据页均已落盘 |

关系：**`CKPT_LSN ≤ FILE_LSN ≤ FLUSH_LSN ≤ CUR_LSN`**（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/backup-restore-introduction.html)）。

**「SCN」在达梦里是 Oracle 兼容的别名**。证据：
- 官方 `LogmnrRecord.scn` 字段的释义直接写「当前记录的 **LSN**」（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/logmnr-interface-instructions.html)）。
- 官方闪回查询语法写作 `AS OF <SCN|LSN lsn>`，两个关键字并列等价（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/flashback-query.html)）。

→ **我们的位点类型就是 `bigint` LSN。**

### 5.2 单调性

- **单个实例内**：官方定义就是「自动递增、全局唯一」，物理事务提交时分配 `CUR_LSN+1`。可以作为严格单调的水位线。
- ⚠️ **并行日志下有序性有洞**：「如果开启并行日志，一个 RLOG_PKG 包内包含多路并行产生的日志，每一路并行日志的 LSN 是递增的，但是**各路之间并不能保证 LSN 有序**」（[一手-社区](https://eco.dameng.com/community/training/11314861ebb4d357da3a21e2952957e8)）。同一来源也提到 DSC（共享集群）多节点环境下相邻日志包 LSN 「总体递增但不一定连续」。
  → 单机 + 未开并行日志时可安全用 LSN 做严格 offset；**DSC / 并行日志场景下不能假设全局严格有序**。
- 连接器实际取当前位点的方式：`SELECT CLSN FROM V$ARCH_FILE WHERE STATUS = 'ACTIVE'`（[二手，源码 `SqlUtils.currentScnQuery()`](https://github.com/devlive-community/dameng-connector/blob/dev/debezium-connector/src/main/java/org/devlive/connector/dameng/logminer/SqlUtils.java)）——注意它取的是**当前活动归档文件的 LSN**，不是 `V$RLOG.CUR_LSN`，这本身也印证了「只能挖归档」的约束。

### 5.3 重启与主备切换后的连续性

- **进程重启**：LSN 由数据库维护并持久化在 redo/归档中，重启不重置。连接器只要把 LSN 存成 offset，重启后从该 LSN 之后继续 `START_LOGMNR(STARTSCN => ...)` 即可（**推测**——官方未针对 CDC 场景背书，但由 LSN「全局唯一、自动递增」的定义可直接推出）。
- **主备切换（switchover / failover）**：⚠️ **未证实，且有风险信号**。
  - 官方数据守护文档说的是：「Switchover 完成后，主备库之间数据是不完全同步的，要由新主库 B 的守护进程通过 Recovery 流程，重新同步数据到新备库 A」（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/data-guard.html)）。
  - 实时主备是「主库发送 redo 给备库→备库重演」（[一手-社区](https://eco.dameng.com/community/training/11314861ebb4d357da3a21e2952957e8)），从机制上 LSN 空间是**共享**的，切换后大概率连续。
  - 但**官方没有任何一句明确说「切换后 LSN 保持连续、不回退」**。Failover（非计划切换）时新主库可能丢失尾部日志，此时 LSN 是否会**重新分配**同一区间 → **完全未证实，必须实测**。这是位点语义上最大的未知。
- 另一个未解冲突：**能否在备库上做日志挖掘**（用于卸载主库压力）？
  - 达梦社区一篇说「在新的版本中，支持在达梦备库执行 LOGMNR，以减少对主库操作的影响」（[一手-社区](https://eco.dameng.com/community/article/2b327cc00893e4f86a3c400afd60bd76)）。
  - 达梦社区 flink-cdc 教程却说「主备模式的备库不支持」「DCS 不支持」，建议改用逻辑备库或快照备份（[一手-社区](https://eco.dameng.com/community/article/bb49aab895c5971f540faf822ffad9ef)）。
  - → **版本相关，未证实**。需按目标 DM 小版本实测。

---

## 6. 官方 CDC 通道（DMHS / DMDRS / DMETL）

达梦官方数据同步/复制产品线（[一手-达梦官网](https://www.dameng.com/DMHS.html)、[一手-达梦官网](https://www.dameng.com/product/DMDRS.html)、[一手-达梦](https://eco.dameng.com/info/products/dts)）：

| 产品 | 定位 | 与日志挖掘的关系 |
|---|---|---|
| **DMHS**（达梦数据实时同步软件） | 异构环境实时同步复制 | 源端 CPT 模块**自研日志直读**，不走 `DBMS_LOGMNR` |
| **DMDRS**（达梦数据复制软件） | 新一代复制产品，DMHS 的演进 | 同上，模块化：Manager / CPT / DSS / EXEC / CVT / SCHED |
| **DMETL** | 数据交换平台，非实时 | 与 CDC 无关 |
| **DTS** | 纯 Java 的 JDBC/ODBC 迁移工具 | 全量迁移，非 CDC |

**DMHS/DMDRS 的捕获机制**（[一手-社区](https://eco.dameng.com/community/post/20260403102811YQR7OBL9S1PZE1SPMP)）：

- CPT「持续监控源数据库的**在线重做日志与归档日志**」，「持续扫描源库的在线重做日志，当在线日志归档后，自动读取归档日志文件，确保无任何增量操作遗漏」。
  → ⭐ **这正是 `DBMS_LOGMNR` 做不到的**：DMHS/DMDRS 能读在线 redo，所以能做到秒级；`DBMS_LOGMNR` 只能读归档。
- 「基于数据库重做日志的 CDC 技术，直接读取数据库原生的在线重做日志与归档日志」，「无需依赖 Oracle 的 LogMiner 或类似工具」。
- 启动时「通过 NET 模块向目标端查询已完成同步的最大 LSN 位点作为起始位置」——位点语义同样是 LSN。
- DDL：提供**基于日志**（直接从重做/归档日志解析 DDL，源库无需建任何对象）和**事件触发器**两种方式。

**可获得性 / 授权**：

- DMDRS 官方文档中 Manager 模块会「启动授权校验线程，校验 License 信息」（[一手](https://eco.dameng.com/document/dm/zh-cn/start/DMDRS_Product_Introduction.html)）→ **是需要 License 的独立商业产品**，不随 DM 数据库授权附送。
- DMDRS 支持源库：DM7、DM8（单机、DSC）、Oracle 10g+（单机/RAC）、DB2 9.7/10.5/11.5、MySQL 5.6+、SQL Server 2008R2/2016/2019（[一手](https://eco.dameng.com/document/dm/zh-cn/start/DMDRS_Product_Introduction.html)）。
- DDL 双向同步仅支持 DM 与 Oracle 系列（[一手](https://eco.dameng.com/document/dm/zh-cn/start/DMDRS_Product_Introduction.html)）。

> **未证实**：DMHS/DMDRS 的具体价格、是否有开发者/评估版、是否对外暴露可编程的输出接口（能否把 DMDRS 的 CPT 输出接到我们自己的下游，而不是它自带的 EXEC 入库模块）。达梦官网与文档均未公开。**这需要商务渠道询价，不是技术调研能解决的。**
>
> **未证实**：达梦是否有对外开放的、可编程的「日志直读 SDK」（即 DMHS/DMDRS 的 CPT 能力的独立形态）。检索 `eco.dameng.com` 产品手册目录未发现此类接口文档；`logmnr-interface-instructions.html` 的 JNI/C 接口是唯一公开的程序化日志读取入口，而它同样受「只读归档」约束（其前提写着 `ARCH_INI=1`，示例全为归档文件路径）。

---

## 7. DDL 变更

**能拿到，形式是重构后的 DDL SQL 文本。**

- `V$LOGMNR_CONTENTS` 的 `OPERATION_CODE = 5` 即 DDL（[一手-社区](https://eco.dameng.com/community/article/7d43e5e008104dccb303cc8c5ffe5353)、[一手-社区](https://eco.dameng.com/community/article/6bca9620fed06013fe6120e3d12e0377)）。
- 官方对 `DBMS_LOGMNR` 的定位就是「对归档日志进行挖掘，**重构出 DDL 和 DML 等操作**」（[一手-社区](https://eco.dameng.com/community/article/2b327cc00893e4f86a3c400afd60bd76)，与官方包手册口径一致）。社区实操中 `CREATE TABLE BOOKS` / `CREATE TABLE COURSE` 确实出现在分析结果里（同上）。
- `RLOG_APPEND_LOGIC=4` 是「只生成事务以及 DDL 相关的逻辑日志」——说明 DDL 日志与行级前像是**两条独立的开关维度**（[一手-社区](https://eco.dameng.com/community/post/202411151126417HJ5M4VTSMA7VA30QF)）。
- JNI 侧 `LogmnrRecord.operation` 官方列出的类型包含 DDL（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/logmnr-interface-instructions.html)）。

**现实告警（二手，但很重要）**：`dameng-connector` 有一个 open issue **#12「经测试不支持 ddl」**，用户报告预期的 `CREATE TABLE` / `INSERT` 语句都没有正常产生（[二手](https://github.com/devlive-community/dameng-connector/issues/12)）。同项目还有 #10「数据变更后，before 里的数据解析不全」、#13「经测试不支持更新数据」、#4「LogMinerDmlParser 可能导致 Insert 事件中每列的值为 Null」（[二手](https://github.com/devlive-community/dameng-connector/issues)）。

→ 这些说明：**「日志里有 DDL」和「连接器能可靠地把 DDL 解析成 schema 变更事件」是两回事**。`SQL_REDO` 是文本，需要一个达梦方言的 SQL parser。这是我们连接器最大的实现工作量所在。

**未证实**：DDL 记录中是否携带足够的元信息（如变更前后的完整列定义、对象 ID `dataObj`/版本 `dataObjv` 是否足以做 schema 版本管理）。`LogmnrRecord` 有 `dataObj`/`dataObjv` 字段（[一手](https://eco.dameng.com/document/dm/zh-cn/pm/logmnr-interface-instructions.html)），但官方未说明其与 schema 演进的对应关系。

---

## 置信度与未证实项

### 高置信（达梦官方文档直接支撑）

- `DBMS_LOGMNR` 的过程签名、OPTIONS 常量、「只支持归档日志」限制、不支持 MPP。
- `RLOG_APPEND_LOGIC` 是 `START_LOGMNR` 的硬前提；取值 0~4 的语义。
- `LogmnrRecord` 的完整字段表及 `scn` 即 LSN。
- LSN / CUR_LSN / FLUSH_LSN / FILE_LSN / CKPT_LSN 的定义与偏序关系。
- `ENABLE_FLASHBACK` / `UNDO_RETENTION`（缺省 90 秒）的语义与限制；`AS OF SCN|LSN` 语法。
- `SP_CREATE_SYSTEM_PACKAGES` 需要 DBA 角色。
- DMHS/DMDRS 读在线 redo + 归档、走 License。

### 明确的「未证实」清单

| # | 未证实项 | 检索了什么 / 为何不确定 |
|---|---|---|
| U1 | **`V$LOGMNR_CONTENTS` 的完整官方列定义** | 查了《DM8 系统管理员手册》附录 1（只含静态数据字典）、「动态管理和性能视图」页（把清单指向"附录 2"，未定位到可抓取 URL）。现有列名来自多篇社区文与连接器源码，可能不全或含 Oracle 遗留列。**现场用 `V$DYNAMIC_TABLES` 或空查询取列名即可闭环。** |
| U2 | **`SQL_UNDO` 列是否真实存在且可用** | 一篇达梦社区文在示例里 SELECT 了 `sql_undo`（[链接](https://eco.dameng.com/community/article/7d43e5e008104dccb303cc8c5ffe5353)），另一篇达梦社区文明确说「没有 UNDO_SQL，只能找到对应的 REDO_SQL」（[链接](https://eco.dameng.com/community/article/6bca9620fed06013fe6120e3d12e0377)）。**两篇都是达梦社区，直接冲突。** 可能是列存在但恒为 NULL，也可能版本差异。 |
| U3 | **`RLOG_APPEND_LOGIC` 到底是动态还是静态** | 4 个来源分成两派（见 2.3）。可能是版本差异或作者保守。`V$DM_INI.PARA_TYPE` 一查便知。 |
| U4 | **性能开销的官方数字** | 达梦官方从未发布量化数据。目前唯一数字来自 CSDN 单篇博客（归档 +83%、耗时 +54%）——**低可信度**。 |
| U5 | **最小权限集（能否不给 DBA）** | 达梦官方系统包手册、`DBMS_LOGMNR` 页、所有社区实操均使用 SYSDBA，**没有任何一手材料给出降权指引**。第三方（DataPipeline）的 GRANT 清单我未能从原页正文验证（WebFetch 只拿到导航框架）。 |
| U6 | **主备 failover 后 LSN 是否连续/不回退** | 官方数据守护文档只讲 Recovery 流程，未对 LSN 连续性作任何承诺。**位点语义上最大的未知。** |
| U7 | **备库能否执行 LOGMNR** | 达梦社区两篇官方社区文互相矛盾（一篇说「新版本支持备库执行 LOGMNR」，一篇说「主备模式的备库不支持」）。版本相关。 |
| U8 | **是否有主动切换归档的联机命令** | 未在归档配置与 `DBMS_LOGMNR` 官方页找到。若没有，纯 SQL 形态的延迟无法主动压低。 |
| U9 | **`DBMS_LOGMNR.CONTINUOUS_MINE` / `DDL_DICT_TRACKING` / `DBMS_LOGMNR_D.BUILD` 在 DM 上是否存在** | 官方 OPTIONS 清单里**没有**这些常量。社区连接器代码里的引用高度疑似 Oracle fork 残留。**不要基于它们做设计。** |
| U10 | **`LOGMNR_DELAY_ANALYZE` / `LOGMNR_PARSE_LOB` 的完整定义** | 前者仅在官方 `DBMS_LOGMNR` 页被顺带提及（=1 时 `COLUMN_PRESENT`/`MINE_VALUE` 不可用），无独立参数说明；后者仅见于连接器 issue 评论，官方零命中。 |
| U11 | **DMHS/DMDRS 的价格、评估版、可编程输出接口** | 官网与文档均不公开。需商务渠道。 |
| U12 | **DDL 记录的元信息丰富度**（能否支撑 schema 演进） | 官方只说 `operation` 含 DDL、`sqlRedo` 是重构 SQL，未说明 DDL 场景下各字段的填充规则。 |
| U13 | **归档保留策略与 CDC 消费位点的交互**（日志被清理时 `START_LOGMNR` 的行为/报错） | 无达梦专属来源。第 3.1 节相关表述属**推测**。 |
| U14 | **`RLOG_APPEND_LOGIC` 动态生效时，已在飞事务是否补记逻辑日志** | 无任何来源。若不补记，动态开启会产生一个「部分列缺失」的窗口。 |

### 明确标注为「推测」的判断

- `RLOG_APPEND_LOGIC` ≈ Oracle supplemental logging（术语类比，机制与后果一致，官方未作此类比）。
- 进程重启后按 LSN 续读可行（由 LSN 定义推出，官方未针对 CDC 背书）。
- 归档清理策略与 CDC 位点的竞争是主要断链原因（源自同类 Oracle 连接器经验）。

---

## 决策建议

### 我们的捕获层应该建在哪个接口上

**MVP：`DBMS_LOGMNR` + `V$LOGMNR_CONTENTS`（纯 JDBC）。**

理由：

1. 它是达梦**唯一**开放、免费、纯 SQL 可达的变更捕获接口。JNI 接口能力更强，但引入 native 依赖（`logmnr.jar` + `dmlogmnr_client.dll`/`.so`）、跨平台打包、版本绑定，对 MVP 不划算。
2. 生态已有先例（`dameng-connector` 82 star、Debezium fork），证明路是通的，也暴露了坑在哪。
3. 位点语义清晰：`bigint` LSN，官方定义「自动递增、全局唯一」。

### 代价（写进 ADR 的话就是这些）

| 代价 | 严重度 | 说明 |
|---|---|---|
| **延迟 = 归档切换周期** | 🔴 高 | 官方明确 `DBMS_LOGMNR` 只能挖归档。做不到亚秒级。这是**产品能力的天花板**，必须提前对齐预期。 |
| **源库写性能下降 + 归档量翻倍** | 🔴 高 | 要完整前像必须 `RLOG_APPEND_LOGIC=2`。参考量级：归档 +83%、写耗时 +54%（低可信度单一来源）。需 DBA 签字。 |
| **需要 DBA 级权限** | 🟠 中高 | 至少建包时需要。目前无一手材料支持降权方案。安全评审会卡。 |
| **`SQL_REDO` 是文本，要写达梦方言 parser** | 🟠 中高 | 前像/后像不是结构化的。社区连接器在这里 bug 最集中（#4/#10/#12/#13）。这是我们的主要工作量与差异化点。 |
| **需要改源库静态配置（可能要重启）** | 🟠 中 | 归档 + `RLOG_APPEND_LOGIC` + `RLOG_IGNORE_TABLE_SET`。若确认是静态参数，接入门槛就是一次停机窗口。 |
| **DSC / 并行日志下 LSN 全序不保证** | 🟡 中 | 单机场景可忽略；集群场景需要额外的排序/去重策略。 |
| **failover 后位点行为未知** | 🟡 中 | 高可用场景的正确性缺口。 |

### 权衡：为什么不选 DMHS/DMDRS

- 它们能读在线 redo，能做秒级——**技术上更优**。
- 但它们是独立 License 的闭源商业产品，且自带落库执行端（EXEC），不是给我们做连接器的 SDK。选它等于我们不做连接器、改做集成商。
- 若客户已经买了 DMDRS，我们的连接器就没有存在必要；若没买，我们的价值恰恰在于提供一个不用买 License 的方案。→ **定位上互补，不是选型对象。**

### 二阶段备选：JNI Logmnr 接口

如果 MVP 落地后延迟成为核心痛点，升级到 `logmnr.jar`：
- 返回结构化 `LogmnrRecord`（含 `startScn`/`commitScn`/`xid`/`rollBack`/`rbasqn/rbablk/rbabyte` 精确物理位点），比解析 `SQL_REDO` 文本可靠得多。
- 可调并行度（`LOGMNR_ATTR_PARALLEL_NUM` 2~16）与缓存，吞吐上限更高。
- `LOGMNR_ATTR_TRX_END` / `LOGMNR_ATTR_TRX_WAIT_TIME` 直接处理「等事务结束」，省掉我们自己实现事务缓冲。
- 但**仍受「只读归档」约束**（官方前提写着 `ARCH_INI=1`，示例全是归档路径），所以它**可能解决不了延迟问题，只能解决吞吐与解析可靠性问题**。→ 见 Spike 项 S3。

### Spike / 原型必须验证的项（按优先级）

| # | 验证什么 | 怎么验 | 阻断什么决策 |
|---|---|---|---|
| **S1** | **归档切换延迟到底多大，能否主动压低** | 设小 `FILE_SIZE`（64MB），测「写入 → 可挖出」的端到端延迟；试找强制归档切换命令（U8） | 产品的延迟 SLA。**最高优先级。** |
| **S2** | **`V$LOGMNR_CONTENTS` 真实列清单 + `SQL_UNDO` 是否可用** | `SELECT * FROM V$LOGMNR_CONTENTS WHERE 1=0` 取列名；跑一轮 UPDATE 看 `SQL_UNDO` 是否有值 | 前像获取方案（U1/U2） |
| **S3** | **JNI 接口能否读在线 redo** | 用 `logmnr.jar` 尝试 `addLogFile` 联机 redo 文件路径 | 是否有低延迟路径存在（若能，直接跳过 SQL 形态） |
| **S4** | **`RLOG_APPEND_LOGIC` 的 `PARA_TYPE`（动静态）+ 动态改后是否立即生效** | `SELECT PARA_NAME,PARA_TYPE,PARA_VALUE FROM V$DM_INI WHERE PARA_NAME LIKE 'RLOG%'`；在线改后立刻写数据看能否挖出 | 接入是否需要停机窗口（U3/U14） |
| **S5** | **最小权限集** | 建普通用户，逐条 GRANT 直到能完成 add_logfile → start → select → end；确认 `EXECUTE ON SYS.DBMS_LOGMNR` 是否可单独授予 | 能否通过安全评审（U5） |
| **S6** | **failover 后 LSN 连续性** | 搭两节点实时主备，记录切换前后 `CUR_LSN`，切换后从旧位点续挖 | 高可用场景是否可用（U6） |
| **S7** | **DDL 在 `V$LOGMNR_CONTENTS` 里的真实形态** | 跑 CREATE / ALTER ADD COLUMN / DROP，看 `OPERATION_CODE=5` 记录的 `SQL_REDO`、`TABLE_NAME`、`dataObj/dataObjv` 填充情况 | schema 演进设计（U12、issue #12） |
| **S8** | **写性能与归档量的真实开销** | `RLOG_APPEND_LOGIC` 0 / 1 / 2 三档，同一 workload 对比 TPS 与归档增长 | 给客户 DBA 的开销承诺（U4） |
| **S9** | **归档被清理后 `START_LOGMNR` 的报错行为** | 消费落后后手动删归档，观察报错 | 断链检测与告警设计（U13） |

---

## 参考来源

### 一手 —— 达梦官方产品手册（`eco.dameng.com/document`）

1. [DBMS_LOGMNR 包](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package) — 过程签名、OPTIONS、前提、限制
2. [Logmnr 接口使用说明](https://eco.dameng.com/document/dm/zh-cn/pm/logmnr-interface-instructions.html) — JNI/C 接口、`LogmnrRecord` 全字段、`setAttr` 常量
3. [系统包概述 / package-overview](https://eco.dameng.com/document/dm/zh-cn/pm/package-overview.html) — 系统包清单、`SP_CREATE_SYSTEM_PACKAGES` 与 DBA 角色要求
4. [备份还原简介](https://eco.dameng.com/document/dm/zh-cn/pm/backup-restore-introduction.html) — LSN / CUR_LSN / FLUSH_LSN / FILE_LSN / CKPT_LSN 定义与偏序
5. [闪回](https://eco.dameng.com/document/dm/zh-cn/pm/flashback-query.html) — `ENABLE_FLASHBACK`、`UNDO_RETENTION`、`AS OF SCN|LSN` 语法与限制
6. [数据守护使用说明](https://eco.dameng.com/document/dm/zh-cn/pm/data-guard.html) — 实时归档、switchover 后的 Recovery 流程
7. [产品手册总目录](https://eco.dameng.com/document/dm/zh-cn/pm/) — 手册结构
8. [DM8 系统管理员手册 附录 1](https://eco.dameng.com/document/dm/zh-cn/pm/dm8-admin-manual-appendix1.html) — （检索 `V$LOGMNR_CONTENTS` 未命中，仅静态数据字典）
9. [DMDRS 产品介绍](https://eco.dameng.com/document/dm/zh-cn/start/DMDRS_Product_Introduction.html) — CPT 模块、支持源库、DDL 双向同步范围、License 校验

### 一手 —— 达梦官方社区 / 官网（`eco.dameng.com/community`、`www.dameng.com`）

10. [达梦数据库 RLOG_APPEND_LOGIC 参数详细说明](https://eco.dameng.com/community/post/202411151126417HJ5M4VTSMA7VA30QF) — 取值 0~4 语义、动态参数说法
11. [DM 日志挖掘](https://eco.dameng.com/community/article/6bca9620fed06013fe6120e3d12e0377) — 物理逻辑日志定义、`RLOG_IGNORE_TABLE_SET`、`RLOG_APPEND_SYSTAB_LOGIC`、前像限制、「无 UNDO_SQL」、闪回归在恢复场景
12. [达梦日志挖掘 LOGMNR 功能介绍](https://eco.dameng.com/community/article/2b327cc00893e4f86a3c400afd60bd76) — 前提条件、DDL 重构、「新版本支持备库执行 LOGMNR」
13. [达梦归档日志挖掘](https://eco.dameng.com/community/article/7d43e5e008104dccb303cc8c5ffe5353) — 完整操作码映射表、列示例（含 `sql_undo`）
14. [归档日志挖掘测试](https://eco.dameng.com/community/training/715c7abbbc3794def0d9a442bfec7d60) — 完整实操 SQL、SYSDBA、重启要求
15. [JAVA 调用 DM Logmnr 进行归档日志分析方法](https://eco.dameng.com/community/article/75a5eda7e2f592f1f1af0aeb7d3c75f7) — JNI 实操、`ARCH_INI=1` + `RLOG_APPEND_LOGIC` 前提、SYSDBA
16. [Dm8_flink_cdc 连接器使用](https://eco.dameng.com/community/article/bb49aab895c5971f540faf822ffad9ef) — `sp_set_para_value(2,...)`、`ADD LOGIC LOG`、版本要求 8.1.3.77+、「备库不支持」
17. [达梦实时主备守护集群详解](https://eco.dameng.com/community/training/11314861ebb4d357da3a21e2952957e8) — 并行日志下 LSN 各路间无序
18. [达梦数据库归档日志详解](https://eco.dameng.com/community/training/4559ddbeb3a7c6c4674db47e33d3bdd9) — 归档开启 SQL、`dmarch.ini` 参数
19. [浅谈 DMHS 架构与原理](https://eco.dameng.com/community/post/20260403102811YQR7OBL9S1PZE1SPMP) — CPT 读在线 redo + 归档、不依赖 LogMiner、DDL 两种捕获方式、LSN 起始位点
20. [达梦数据实时同步软件 DMHS（官网）](https://www.dameng.com/DMHS.html)
21. [达梦数据复制软件 DMDRS（官网）](https://www.dameng.com/product/DMDRS.html)
22. [数据迁移工具 DTS](https://eco.dameng.com/info/products/dts)

### 二手 —— 真实连接器实现（作为「它们调用了什么 API」的证据）

23. [devlive-community/dameng-connector](https://github.com/devlive-community/dameng-connector) — README 的 CDC 配置清单（含闪回步骤）
24. [`logminer/SqlUtils.java`](https://github.com/devlive-community/dameng-connector/blob/dev/debezium-connector/src/main/java/org/devlive/connector/dameng/logminer/SqlUtils.java) — 实际 SELECT 的列、`SELECT CLSN FROM V$ARCH_FILE WHERE STATUS='ACTIVE'`、Oracle 遗留常量
25. [`logminer/LogMinerHelper.java`](https://github.com/devlive-community/dameng-connector/blob/dev/debezium-connector/src/main/java/org/devlive/connector/dameng/logminer/LogMinerHelper.java) — `DBMS_LOGMNR.ADD_LOGFILE` 调用形式
26. [`DamengSnapshotChangeEventSource.java`](https://github.com/devlive-community/dameng-connector/blob/dev/debezium-connector/src/main/java/org/devlive/connector/dameng/DamengSnapshotChangeEventSource.java) — 快照使用 `SELECT * FROM t AS OF SCN <n>`（闪回的真实用途）
27. [issue #16「是否可以不启用闪回？」](https://github.com/devlive-community/dameng-connector/issues/16) — 用户实测 `snapshot.mode=schema_only` 不开闪回可用；`LOGMNR_PARSE_LOB=1`
28. [issue #12「经测试不支持 ddl」](https://github.com/devlive-community/dameng-connector/issues/12)
29. [issue 列表](https://github.com/devlive-community/dameng-connector/issues) — #4/#10/#13 解析可靠性问题

### 二手 / 低可信度（单一来源，仅作量级参考）

30. [达梦 8 开启物理逻辑日志对系统的影响（CSDN）](https://blog.csdn.net/weixin_56866387/article/details/139558813) — **唯一**的性能实测数字：归档 10.84GB→19.85GB、耗时 5m12s→8m00s。**低可信度。**
31. [DataPipeline 达梦连接文档](https://docs.datapipeline.com/wg7gzp4olbtub3vv/ygyy6k0s0rtsyc48/dgoa5i7ighva165c/nndhzv1crbqaig5a/) — GRANT 清单（正文未能直接验证，来自搜索摘要）。**低可信度。**
32. [DM 数据库 DBMS_LOGMNR 使用方法（墨天轮）](https://www.modb.pro/db/26307) — `V$LOGMNR_*` 视图会话级隔离的说法。**单一来源。**
