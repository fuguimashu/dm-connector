# 达梦测试实例（spike）

一次性可重建的本地达梦 8 实例，用于验证日志挖掘的真实行为。实测结论见
[`docs/research/dm-test-instance-findings.md`](../../docs/research/dm-test-instance-findings.md)。

## 起实例

```bash
docker run -d --name dm-cdc-spike -p 5236:5236 cnxc/dm8:20251114-kylin
# 等就绪
until docker logs dm-cdc-spike 2>&1 | grep -q "SYSTEM IS READY"; do sleep 5; done
```

镜像是社区维护的，内置达梦官方**开发试用授权**（`DEVELOP USER`，2026-11-13 到期）。
仅用于本地开发测试，不用于任何生产或分发场景。授权过期后换更新的 tag。

- 端口 `5236`，`SYSDBA / SYSDBA_abc123`
- 版本 `DM8 V8 / DB Version 0x7000d / build 20251113`

## 灌脚本

`disql` 从 stdin 读脚本时有两个坑：文件必须是 **LF** 换行；`CREATE SCHEMA` 必须用单独一行的 `/` 收尾。
Git Bash 下还要加 `MSYS_NO_PATHCONV=1`，否则容器内路径会被改写。

```bash
run() {
  tr -d '\r' < "$1" | MSYS_NO_PATHCONV=1 docker exec -i dm-cdc-spike \
    /opt/dmdbms/bin/disql -L SYSDBA/SYSDBA_abc123@localhost:5236
}

run 01-archive-and-logic-log.sql   # 开归档 + 物理逻辑日志 + 建 DBMS_LOGMNR 包
run 02-schema-and-tables.sql       # CDC_TEST schema 与覆盖典型类型的表
run 03-cdc-user.sql                # DM_CDC 账号与最小权限
run 04-verify-logmnr.sql           # 产生变更并挖出来（第 4 段的归档路径需手工填）
```

## 拆掉

```bash
docker rm -f dm-cdc-spike
```

数据没有挂 volume，容器删了就干净了 —— 这是故意的，每次都从零重建才能保证脚本是可复现的。
