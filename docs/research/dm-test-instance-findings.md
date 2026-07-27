# 达梦测试实例：搭建过程与实测结论

对应票 [#5 准备一个可用的达梦测试实例](https://github.com/fuguimashu/dm-connector/issues/5)。

这份文档是**实测**结果，优先级高于 [`dm-capture-mechanism.md`](dm-capture-mechanism.md) 里基于文档与社区文章的推断。凡两者冲突处，以本文为准，并在下面显式标注。

复现脚本在 [`spike/dm-instance/`](../../spike/dm-instance/)。

---

## 1. 实例

| 项 | 值 |
| --- | --- |
| 镜像 | `cnxc/dm8:20251114-kylin`（Docker Hub，社区维护，amd64/arm64，1.3 GB） |
| 版本 | `DM Database Server 64 V8`，`DB Version: 0x7000d`，build `03134284458-20251113-301923-20178` |
| 授权 | `V$LICENSE`: `AUTHORIZED_CUSTOMER = DEVELOP USER`，`EXPIRED_DATE = 2026-11-13` —— **开发试用授权，2026-11-13 到期**，届时换更新的 tag 即可 |
| 容器名 | `dm-cdc-spike` |
| 端口 | `localhost:5236` |
| SYSDBA | `SYSDBA / SYSDBA_abc123`（镜像默认，见镜像 ENV `SYSDBA_PWD`） |
| CDC 账号 | `DM_CDC / DmCdc_2026#` |
| 归档目录 | 容器内 `/opt/dmdbms/arch` |
| 数据目录 | 容器内 `/opt/dmdbms/data/DAMENG` |
| 建库参数 | `PAGE_SIZE=32`，`CASE_SENSITIVE=1`，`UNICODE_FLAG=1`，`LENGTH_IN_CHAR=1`，`LOG_SIZE=512` |

```bash
docker run -d --name dm-cdc-spike -p 5236:5236 cnxc/dm8:20251114-kylin
# 就绪判据：docker logs dm-cdc-spike | grep "SYSTEM IS READY"
```

**凭据管理**：目前是本地一次性 spike，凭据直接写在脚本与本文里，不涉及任何真实数据。若后续要接 CI，再改成从环境变量注入。

### 生效参数

| 参数 | 值 | PARA_TYPE |
| --- | --- | --- |
| `ARCH_INI` | 1 | SYS |
| `RLOG_APPEND_LOGIC` | 2 | SYS |
| `RLOG_IGNORE_TABLE_SET` | 1（镜像默认已是 1） | SYS |
| `LOGMNR_PARSE_LOB` | 0（默认） | SYS |
| `LOGMNR_GEN_UNDO` | 0（默认） | SYS |

归档配置：`ARCHIVE_LOCAL1`，`TYPE=LOCAL`，`DEST=/opt/dmdbms/arch`，`FILE_SIZE=64`(MB)，`SPACE_LIMIT=4096`(MB)。

---

## 2. 推翻或修正调研结论的三条

### 2.1 ⭐ 延迟下界**不是**归档切换周期 —— 正在写的归档文件就能挖

调研文档的最强结论是「`DBMS_LOGMNR` 只能挖归档 ⇒ 延迟下界 = 归档切换周期 ⇒ 做不了亚秒级」。**实测不成立。**

`V$ARCH_FILE` 里状态为 `ACTIVE`（当前正在写入、尚未切换）的归档文件，可以直接 `ADD_LOGFILE` + `START_LOGMNR` 挖出**刚刚提交的**事务。测试中提交后立刻挖，数据即可见。

官方「只支持分析归档日志」这句话是真的，但它约束的是**日志的形态**（必须落到归档文件），不是**文件的完成度**。达梦是持续追加写归档文件的，不是攒满一个文件才落盘。

⇒ 延迟下界实际是**归档刷盘延迟**，量级在毫秒到秒，而不是 `FILE_SIZE / 写入速率`。这直接改变 [#13 延迟目标](https://github.com/fuguimashu/dm-connector/issues/13) 的前提，也让整个方案的产品定位从「准实时批」回到「实时 CDC」。

> 仍需验证：高写入压力下 ACTIVE 文件的可见性滞后有多大；这需要压测，不是本票范围。

顺带确认（原 U8）：**`ALTER SYSTEM ARCHIVE LOG CURRENT` 在达梦上可用**，会立刻切出一个新归档文件。所以主动切换这条路也有，只是按上面的结论我们多半用不着。

### 2.2 ⭐ 不需要 DBA 权限

调研的保守结论是「生产上需要给 CDC 账号 DBA 级权限」，并把它列为「安全评审会卡」的风险。**实测不需要。**

一个普通用户只要有：

```sql
GRANT CREATE SESSION TO DM_CDC;
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO DM_CDC;
GRANT SELECT ON SYS.V$LOGMNR_CONTENTS TO DM_CDC;
GRANT SELECT ON SYS.V$ARCH_FILE TO DM_CDC;
GRANT SELECT ON SYS.V$RLOG TO DM_CDC;
GRANT SELECT ON SYS.V$DM_INI TO DM_CDC;
```

就能完整跑通 `ADD_LOGFILE` → `START_LOGMNR` → 查 `V$LOGMNR_CONTENTS`。达梦允许对 `V$` 视图逐个 `GRANT SELECT`。

唯一仍需 DBA 的是一次性的 `SP_CREATE_SYSTEM_PACKAGES(1,'DBMS_LOGMNR')`（建包），可由 DBA 在部署时代做一次。

**但有个必须写进规格的安全反面**：日志挖掘**不受表级权限约束**。测试中 `DM_CDC` 只被授予 `T_BASIC` 的 `SELECT`，却能从 `V$LOGMNR_CONTENTS` 里读到 `T_LOB`、`T_NO_PK` 的全部变更内容。即 `EXECUTE ON DBMS_LOGMNR` 事实上等价于**全库变更读权限**。

### 2.3 参数全是动态的，接入不需要停机窗口

原 U3 有四个来源分成两派。实测：`ARCH_INI`、`RLOG_APPEND_LOGIC`、`RLOG_IGNORE_TABLE_SET`、`LOGMNR_PARSE_LOB`、`LOGMNR_GEN_UNDO` 的 `PARA_TYPE` **全部是 `SYS`**，`SP_SET_PARA_VALUE(1, ...)`（1 = 同时改 dm.ini 与内存）改完立即生效，全程没有重启实例。

开归档本身要 `ALTER DATABASE MOUNT` / `OPEN`，这是一次几百毫秒的状态切换，不是进程重启。

> 仍未验证（原 U14）：动态开启 `RLOG_APPEND_LOGIC` 的瞬间，已在飞的事务是否补记逻辑日志。若不补记，会有一个「部分列缺失」的窗口。

**踩到的坑**：`ALTER DATABASE ARCHIVELOG` 必须在 `ALTER DATABASE ADD ARCHIVELOG '...'` **之前**执行（前者负责把 `ARCH_INI` 置 1），顺序反了会报 `[-810] 系统未配置归档`。绝大多数教程写的是反的。

---

## 3. `V$LOGMNR_CONTENTS` 的真实形态（原 U1，现已闭环）

共 **63 列**。与设计相关的是这些：

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `SCN` | BIGINT | 位点。实为 LSN，单调递增 |
| `XID` | BINARY(8) | **事务 id，真实填充**，如 `0x000000000000468B` |
| `OPERATION_CODE` / `OPERATION` | INT / VARCHAR | 见下表 |
| `SEG_OWNER` / `TABLE_NAME` | VARCHAR | schema / 表名 |
| `TIMESTAMP` | DATETIME(6) | 变更时间 |
| `ROW_ID` | VARCHAR(20) | DML 行有值（如 `AAAAAAAAAAAAAAAAAB`） |
| `SQL_REDO` | **VARCHAR(4000)** | SQL 文本，**超长会截断分行**，见 3.3 |
| `SQL_UNDO` | VARCHAR(4000) | 默认恒为 NULL，见 3.4 |
| `SSN` | INT | 同一 SCN 内的分片序号 |
| `CSF` | INT | **续行标志**：1 = 后面还有分片 |

`START_SCN` / `COMMIT_SCN` / `CSCN` / `SAFE_RESUME_SCN` / `RS_ID` 等列存在，本轮未逐一验证其填充情况。

### 3.1 OPERATION_CODE 实测取值

| code | OPERATION | 备注 |
| --- | --- | --- |
| 1 | INSERT | |
| 2 | DELETE | |
| 3 | UPDATE | |
| 5 | DDL | |
| 6 | START | **事务开始，独立一行** |
| 7 | COMMIT | **事务提交，独立一行** |

⇒ **事务边界是显式的**。参考项目 `dameng-connector` 用「1 秒静默超时」冒充提交的做法是彻头彻尾的错误实现，不是达梦的限制。我们按 `START` / `COMMIT` 记录做事务缓冲即可。

⇒ `OPTIONS => 2130`（含 `COMMITTED_DATA_ONLY`）下，**回滚的事务完全不出现**在结果里。测试中一笔 `INSERT ... ROLLBACK` 的事务确实查不到。

### 3.2 ⭐ SQL_REDO 是按**当前**字典渲染的，不是历史字典

这是本轮最危险的发现，[#9 DDL 与 schema 变更](https://github.com/fuguimashu/dm-connector/issues/9) 和 [#12 解析器策略](https://github.com/fuguimashu/dm-connector/issues/12) 必须正面处理。

时间线：

1. SCN 47772：`INSERT INTO T_BASIC (11 列)`
2. SCN 47871：`ALTER TABLE T_BASIC ADD COLUMN C_NEW VARCHAR(30)`

事后挖掘 SCN 47772 那条 INSERT，`SQL_REDO` 是：

```sql
INSERT INTO "CDC_TEST"."T_BASIC"("ID", ..., "C_TSTZ", "C_NEW") VALUES(1, ..., NULL)
```

**`C_NEW` 出现在了一条比它早诞生的 INSERT 里。** 达梦按挖掘时刻的表结构重新渲染 SQL 文本，而不是按事件发生时的结构。

后果：

- 断点续传重放一段跨过 DDL 的历史日志时，拿到的列集合是**现在**的，不是**当时**的。
- 「维护一份 schema 历史用于正确解码历史事件」这条 Debezium 式的设计在达梦上**前提不成立** —— 我们无法从日志里还原出历史 schema 下的事件，达梦已经替我们改写了。
- 反过来说，这也简化了一件事：解析器只需要对着**当前** schema 解析，不需要历史 schema 表。但代价是「事件的 schema 版本」这个概念在达梦上是模糊的。

> 仍需验证：删列、改类型、改列名之后重放会发生什么（会不会直接报错、或渲染出对不上的值）。这比加列危险得多。

### 3.3 SQL_REDO 超过 4000 字符会分片

宽行实测：一条 7000+ 字符的 INSERT 被切成两行，**SCN 相同**，`SSN` 依次为 0、1，前一片 `CSF=1` 表示后续还有。

⇒ 解析器必须先按 `(SCN, SSN)` 拼接 `CSF=1` 的分片，再解析。漏掉这一步在窄表上永远不会暴露，一上宽表就炸。

### 3.4 前像与 SQL_UNDO

`RLOG_APPEND_LOGIC=2` 下，**完整前像在 `SQL_REDO` 的 `WHERE` 子句里**：

```sql
UPDATE "CDC_TEST"."T_BASIC" SET "C_VARCHAR" = '改过了', "C_DEC" = 9999.0001
 WHERE "ID" = 1 AND "C_TINY" = 1 AND "C_BIGINT" = 9223372036854775807
   AND "C_DEC" = 1234.5678 AND "C_DOUBLE" = 3.14159E0 AND "C_CHAR" = 'abc'
   AND "C_VARCHAR" = '一段中文' AND "C_DATE" = DATE'2026-01-02' AND ...
```

`SET` 子句给后像的变化部分，`WHERE` 子句给**全列**前像。DELETE 同理（全列在 WHERE）。无主键表也一样用全列定位（`WHERE "A" = 1 AND "B" = 'x'`），不依赖 ROWID。

`SQL_UNDO` **默认恒为 NULL**。原因是一个官方文档里没有的参数 **`LOGMNR_GEN_UNDO`（默认 0）**；置 1 后 `SQL_UNDO` 就有值了。

但我们**不需要开它**：前像已经在 redo 的 WHERE 子句里，开 `LOGMNR_GEN_UNDO` 只是让待解析文本量翻倍。

### 3.5 ⭐ LOB 内容不在日志里

| 情况 | `SQL_REDO` 里的形态 |
| --- | --- |
| 短 CLOB / TEXT（10 字符） | 正常内联字符串字面量 `'short clob'` |
| 短 BLOB（4 字节） | 正常内联 `0x00010203` |
| 长 CLOB（3000 字符），`LOGMNR_PARSE_LOB=0` | 裸占位符 `OUT_CLOB` |
| 长 CLOB（3000 字符），`LOGMNR_PARSE_LOB=1` | `OUT_CLOB(LOB_ID: 525301)` |

即：**超过某个阈值的 LOB，内容根本不进逻辑日志**，`LOGMNR_PARSE_LOB=1` 也只是多给一个 LOB 句柄，不给内容。

这是一条硬能力边界，必须成为规格里的显式决策：要么回源库按 LOB_ID 取（引入一致性窗口与额外负载），要么第一版明确不支持大 LOB 并在遇到时报错。**具体阈值本轮未测定。**

### 3.6 DDL 以原始 SQL 文本完整给出

`OPERATION_CODE=5` 的行，`SQL_REDO` 就是原样的 DDL 语句，`SEG_OWNER`/`TABLE_NAME` 正确填充：

```
CREATE SCHEMA CDC_TEST AUTHORIZATION SYSDBA         (SEG_OWNER/TABLE_NAME 空)
CREATE TABLE CDC_TEST.T_BASIC (...)                 T_BASIC
ALTER TABLE CDC_TEST.T_BASIC ADD COLUMN C_NEW ...   T_BASIC
CREATE TABLE CDC_TEST.T_DDL_PROBE (...)             T_DDL_PROBE
DROP TABLE CDC_TEST.T_DDL_PROBE                     T_DDL_PROBE
```

且每条 DDL 都被包在自己的 `START` / `COMMIT` 里。DDL 捕获这条路是通的，问题只在于要不要自建达梦方言的 DDL 解析器（[#12](https://github.com/fuguimashu/dm-connector/issues/12)）。

### 3.7 值的字面量渲染

| 类型 | 渲染 |
| --- | --- |
| DOUBLE | `3.14159E0`（科学计数法） |
| DECIMAL | `1234.5678` |
| BIGINT | `9223372036854775807` |
| CHAR/VARCHAR | `'abc'`、`'一段中文'`（UTF-8 直出） |
| DATE | `DATE'2026-01-02'` |
| TIME | `TIME'10:20:30'` |
| TIMESTAMP | `TIMESTAMP'2026-01-02 10:20:30.123456'` |
| TIMESTAMP WITH TIME ZONE | `TIMESTAMP'2026-01-02 10:20:30.123456 +08:00'` |
| BLOB（短） | `0x00010203` |
| NULL | `NULL`（WHERE 里是 `IS NULL`） |

标识符一律双引号包裹（DML）；DDL 保留原样不加引号。

---

## 4. `V$ARCH_FILE`：归档文件到位点的映射

`V$ARCH_FILE` 提供了位点→文件的完整映射，位点管理直接用它即可：

| 列 | 说明 |
| --- | --- |
| `PATH` | 归档文件绝对路径，`ADD_LOGFILE` 直接用 |
| `STATUS` | `ACTIVE`（正在写）/ `INACTIVE`（已切换） |
| `ARCH_LSN` | 该文件覆盖的起始 LSN |
| `CLSN` | 该文件覆盖的结束 LSN（ACTIVE 的会持续增长） |
| `ARCH_SEQ` / `NEXT_SEQ` | 文件序号，用于检测断链 |
| `CREATE_TIME` / `CLOSE_TIME` | |
| `LLOG_FIRST_TIME` / `LLOG_LAST_TIME` | 逻辑日志的时间区间 |

⇒ 「给定一个位点 LSN，找出该从哪个归档文件开始挖」是一个 `WHERE ARCH_LSN <= :lsn AND :lsn <= CLSN` 的查询。
⇒ 「归档被清理导致断链」可以通过 `ARCH_SEQ` 不连续、或位点落在最小 `ARCH_LSN` 之前来检测（[#7 位点抽象](https://github.com/fuguimashu/dm-connector/issues/7) 的第 5 条）。

`V$RLOG.CUR_LSN` 给出当前最新 LSN，是位点的上界与延迟计算的基准。

---

## 5. 操作达梦时踩到的坑（写给后面的会话）

1. **`CREATE SCHEMA` 开启一个块**，必须用**单独一行的 `/`** 收尾。不加的话后面所有语句都会被当成这个块的一部分而永不执行，`disql` 只会静静地打印续行号。
2. **脚本必须是 LF 换行**。CRLF 会让 `;` 失效，症状同上。Windows 上用 `tr -d '\r'` 过一道。
3. **Git Bash 下 `docker exec` 要加 `MSYS_NO_PATHCONV=1`**，否则 `/opt/dmdbms/bin/disql` 会被改写成 `C:/Program Files/Git/opt/...`。
4. `V$` 视图在 heredoc 里要注意 `$` 的转义；用带引号的 heredoc（`<<'EOF'`）最省事。
5. `SET PAGESIZE 0` 会连表头一起吞掉，取列名用 `DESCRIBE` 更可靠。

---

## 6. 本轮仍未验证的

| # | 问题 | 归属 |
| --- | --- | --- |
| V1 | 删列 / 改类型 / 改列名 后重放历史日志会发生什么（3.2 只测了加列） | [#9](https://github.com/fuguimashu/dm-connector/issues/9) / [#12](https://github.com/fuguimashu/dm-connector/issues/12) |
| V2 | LOB 内联与外置的具体阈值 | 新票 |
| V3 | 高写入压力下 ACTIVE 归档文件的可见性滞后 | [#13](https://github.com/fuguimashu/dm-connector/issues/13) |
| V4 | `RLOG_APPEND_LOGIC` 0/1/2 三档的写性能与归档量开销 | 未成票（属吞吐目标，仍在迷雾里） |
| V5 | 归档被清理后 `START_LOGMNR` 的具体报错行为 | [#7](https://github.com/fuguimashu/dm-connector/issues/7) / [#10](https://github.com/fuguimashu/dm-connector/issues/10) |
| V6 | 动态开 `RLOG_APPEND_LOGIC` 时在飞事务是否补记 | [#10](https://github.com/fuguimashu/dm-connector/issues/10) |
| V7 | 国产 JDK 21 发行版 + 达梦 JDBC 驱动的冒烟验证 | [#15](https://github.com/fuguimashu/dm-connector/issues/15) |
| V8 | `START_SCN` / `COMMIT_SCN` / `CSCN` / `RS_ID` 的填充情况 | [#7](https://github.com/fuguimashu/dm-connector/issues/7) |
