-- 达梦测试实例：开归档 + 开物理逻辑日志。以 SYSDBA 执行。
-- 已在 cnxc/dm8:20251114-kylin 上实测通过，见 docs/research/dm-test-instance-findings.md。
-- 注意：闪回（FLASHBACK）不是日志挖掘的前提，这里不开——只有初始快照才可能需要。

-- === 0. 先看清参数的动静态属性 ===
-- 实测：ARCH_INI / RLOG_APPEND_LOGIC / RLOG_IGNORE_TABLE_SET / LOGMNR_PARSE_LOB /
-- LOGMNR_GEN_UNDO 的 PARA_TYPE 全是 SYS —— 都是动态参数，改完无需重启。
SELECT PARA_NAME, PARA_TYPE, PARA_VALUE FROM V$DM_INI
 WHERE PARA_NAME IN ('ARCH_INI','RLOG_APPEND_LOGIC','RLOG_IGNORE_TABLE_SET')
    OR PARA_NAME LIKE '%LOGMNR%';

-- === 1. 开归档 ===
-- ⚠️ 顺序很关键：必须先 ALTER DATABASE ARCHIVELOG（它把 ARCH_INI 置 1），
-- 再 ADD ARCHIVELOG。反过来会报 [-810] 系统未配置归档。
ALTER DATABASE MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE ADD ARCHIVELOG 'DEST=/opt/dmdbms/arch, TYPE=LOCAL, FILE_SIZE=64, SPACE_LIMIT=4096';
ALTER DATABASE OPEN;

SELECT ARCH_MODE FROM V$DATABASE;
SELECT ARCH_NAME, ARCH_TYPE, ARCH_DEST, ARCH_FILE_SIZE, ARCH_SPACE_LIMIT, ARCH_IS_VALID FROM V$DM_ARCH_INI;

-- === 2. 开物理逻辑日志（SP_SET_PARA_VALUE 的第一个参数 1 = 同时改 dm.ini 与内存）===
-- 2 = 记录完整前像（UPDATE/DELETE 的 sqlRedo WHERE 子句会带全列旧值），代价是归档量约翻倍。
SP_SET_PARA_VALUE(1, 'RLOG_APPEND_LOGIC', 2);
-- 1 = 所有表都记逻辑日志；漏配这一条 V$LOGMNR_CONTENTS 会直接返回空。
-- （cnxc 镜像里默认已是 1，新装库不一定。）
SP_SET_PARA_VALUE(1, 'RLOG_IGNORE_TABLE_SET', 1);

-- === 3. 两个官方文档没有的 LOGMNR 参数（实测存在，默认都是 0）===
-- LOGMNR_GEN_UNDO=1 → SQL_UNDO 列才有值；默认 0 时永远是 NULL。
-- 我们不需要它：前像已在 SQL_REDO 的 WHERE 子句里，开了只是让待解析文本翻倍。
-- SP_SET_PARA_VALUE(1, 'LOGMNR_GEN_UNDO', 1);
-- LOGMNR_PARSE_LOB=1 → 大 LOB 从裸 `OUT_CLOB` 变成 `OUT_CLOB(LOB_ID: <id>)`，
-- 仍然拿不到内容，只多一个句柄。见 findings 文档的 LOB 一节。
-- SP_SET_PARA_VALUE(1, 'LOGMNR_PARSE_LOB', 1);

-- === 4. 建 DBMS_LOGMNR 系统包（需 DBA 角色，一次性）===
SP_CREATE_SYSTEM_PACKAGES(1, 'DBMS_LOGMNR');
