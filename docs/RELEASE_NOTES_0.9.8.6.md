# Release Notes — V0.9.8.6

发布时间：2026-02-27

## 重点更新
- 授权执行路径增强：新增“延后授权”分流机制。目标对象当前不存在且本轮不创建时，不再进入 `grants_miss`，统一标记为 `DEFERRED_TARGET_MISSING_NOT_PLANNED`。
- 新增延后授权明细报告：`deferred_grants_detail_<ts>.txt`，并写入 report_db，支持数据库侧直接查询。
- 新增 `fixup_scripts/grants_deferred/README.txt` 兜底提醒：即使 deferred SQL 未生成，也能明确后续授权动作，防止遗漏。
- `run_fixup.py` 默认安全跳过 `grants_deferred`，对象补齐后再显式执行。

## 约束/索引重名处理
- `name_collision` 执行层级前置到 `constraint/index` 之前。
- 同名约束在低版本 OB 自动回退 `DROP + ADD` 策略，规避 `RENAME CONSTRAINT` 兼容问题。
- 配置默认值明确为：
  - `name_collision_mode = fixup`
  - `name_collision_rename_existing = true`

## 口径与噪声治理
- `MLOG$_*`（Oracle 物化视图日志表）默认按 EXCLUDED 处理，不参与缺失/不一致校验与 fixup 生成。
- `LONG/LOB_OVERSIZE` 作为风险项（RISK_ONLY），不再默认阻断依赖对象检查；仅在父表目标缺失时阻断依赖修补。

## 文档与版本同步
- 主程序、`run_fixup.py`、`init_users_roles.py`、`prod_diagnose.py` 版本统一升级到 `0.9.8.6`。
- README / readme_config / ARCHITECTURE / TECHNICAL_SPECIFICATION / DEPLOYMENT / ADVANCED_USAGE 已同步更新。
