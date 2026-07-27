-- 专用 CDC 账号 + 最小权限。以 SYSDBA 执行。
-- 实测结论：**不需要 DBA 角色**就能完成 add_logfile → start_logmnr → 查 V$LOGMNR_CONTENTS。
-- 这推翻了调研阶段「生产上需给 CDC 账号 DBA 级权限」的保守结论（U5）。
-- 唯一仍需 DBA 的是一次性的 SP_CREATE_SYSTEM_PACKAGES（见 01 脚本），可由 DBA 代做。

CREATE USER DM_CDC IDENTIFIED BY "DmCdc_2026#";

GRANT CREATE SESSION TO DM_CDC;
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO DM_CDC;

-- 挖掘与位点所需的动态视图。注意达梦允许对 V$ 视图逐个 GRANT SELECT。
GRANT SELECT ON SYS.V$LOGMNR_CONTENTS TO DM_CDC;   -- 挖掘结果
GRANT SELECT ON SYS.V$ARCH_FILE TO DM_CDC;         -- 归档文件清单 + 每个文件的 LSN 区间
GRANT SELECT ON SYS.V$RLOG TO DM_CDC;              -- CUR_LSN，位点上界
GRANT SELECT ON SYS.V$DM_INI TO DM_CDC;            -- 启动时校验前置参数

-- 快照阶段要读被捕获表
GRANT SELECT ON CDC_TEST.T_BASIC TO DM_CDC;
GRANT SELECT ON CDC_TEST.T_LOB TO DM_CDC;
GRANT SELECT ON CDC_TEST.T_NO_PK TO DM_CDC;
GRANT SELECT ON CDC_TEST.T_COMPOSITE_PK TO DM_CDC;

-- ⚠️ 安全提示：日志挖掘**不受表级权限约束**。实测 DM_CDC 只被授予 T_BASIC 的 SELECT，
-- 却能从 V$LOGMNR_CONTENTS 里读到 T_LOB、T_NO_PK 的全部变更内容。
-- 也就是说 EXECUTE ON DBMS_LOGMNR 事实上等价于「全库变更读权限」，
-- 规格里必须把这一点作为安全须知写明。
