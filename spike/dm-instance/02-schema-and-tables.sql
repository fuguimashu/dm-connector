-- 测试 schema 与覆盖典型类型的表。以 SYSDBA 执行。

-- 注意：CREATE SCHEMA 在达梦里开启一个块，必须用单独一行的 / 结束，
-- 否则后续所有语句都会被当成这个块的一部分而永不执行。
CREATE SCHEMA CDC_TEST AUTHORIZATION SYSDBA;
/

-- 数值 / 字符 / 时间 的常规组合，带主键。
CREATE TABLE CDC_TEST.T_BASIC (
    ID          INT PRIMARY KEY,
    C_TINY      TINYINT,
    C_BIGINT    BIGINT,
    C_DEC       DECIMAL(18, 4),
    C_DOUBLE    DOUBLE,
    C_CHAR      CHAR(10),
    C_VARCHAR   VARCHAR(200),
    C_DATE      DATE,
    C_TIME      TIME,
    C_TS        TIMESTAMP,
    C_TSTZ      TIMESTAMP WITH TIME ZONE
);

-- 大对象单独一张表：LOB 在日志挖掘里是已知的高风险区。
CREATE TABLE CDC_TEST.T_LOB (
    ID      INT PRIMARY KEY,
    C_CLOB  CLOB,
    C_BLOB  BLOB,
    C_TEXT  TEXT
);

-- 无主键表：用于观察 sqlRedo 的 WHERE 子句如何定位行（ROWID？全列？）。
CREATE TABLE CDC_TEST.T_NO_PK (
    A   INT,
    B   VARCHAR(50)
);

-- 复合主键 + NULL 值，用于观察前像的列填充情况。
CREATE TABLE CDC_TEST.T_COMPOSITE_PK (
    K1  INT,
    K2  VARCHAR(20),
    V   VARCHAR(100),
    PRIMARY KEY (K1, K2)
);
