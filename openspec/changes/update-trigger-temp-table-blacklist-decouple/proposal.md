# Change: Decouple temporary-table trigger incompatibility from blacklist mode

## Why
当前程序已经能把“挂在临时表上的 DML 触发器”归类为：

- `reason_code=TRIGGER_ON_TEMP_TABLE_UNSUPPORTED`
- `reason=OB 不支持在临时表上创建 DML 触发器（实测 ORA-00600/-4007）`

并输出专门明细：

- `triggers_temp_table_unsupported_detail_<ts>.txt`

但当前这条专门分类仍主要依赖“父表先被黑名单识别为 `TEMP_TABLE/TEMPORARY_TABLE`”。

这带来两个问题：

1. 当 `blacklist_mode=disabled` 时，临时表本身可能仍需进入正常校验范围，但“临时表触发器不支持”的专门分类会变得不稳定；
2. 用户期望是：
   - 临时表在未启用黑名单时仍然参与校验；
   - 依赖临时表的其他对象继续按正常规则进入校验/生成；
   - 只有“挂在临时表上的 simple DML trigger”作为特殊兼容边界，被明确提示需要改造。

同时，当前这类触发器虽然会进入报告，但默认不会形成清晰的 unsupported trigger DDL 参考路径，用户难以把“为什么不能直接执行”与“原始 DDL 长什么样”联动起来。

## What Changes

1. 将“临时表触发器不支持”判定从 blacklist 结果中解耦：
   - 直接基于源/目标临时表元数据（如 `DBA_TABLES.TEMPORARY` / 已加载 temporary table 元数据）识别；
   - 不再要求父表必须先命中 blacklist reason 才能触发 `TRIGGER_ON_TEMP_TABLE_UNSUPPORTED`。

2. 明确临时表与其依赖对象的范围语义：
   - 当 `blacklist_mode=disabled` 时，临时表仍参与正常校验；
   - 依赖该临时表的非 trigger 对象继续按现有逻辑参与校验与 fixup 生成；
   - 仅“挂在临时表上的 simple DML trigger”走特殊 unsupported 分类。

3. 为这类触发器增加 unsupported DDL 输出：
   - 不进入可执行目录 `fixup_scripts/trigger/`
   - 输出到 `fixup_scripts/unsupported/trigger/`
   - 文件头和注释中明确：
     - `reason_code=TRIGGER_ON_TEMP_TABLE_UNSUPPORTED`
     - `action=改造/不迁移`
     - 这是 OceanBase 的已知兼容边界，不应直接执行

4. 保持报告口径清晰一致：
   - 继续出现在 `triggers_temp_table_unsupported_detail_<ts>.txt`
   - 继续进入 `unsupported/blocked` 统计与 `manual_actions_required_<ts>.txt`
   - 不计入 `缺失(可修补)`，也不计入普通 trigger fixup 产物

## Non-Goals

- 不改变默认 `blacklist_mode=auto` 的整体行为
- 不把所有依赖临时表的对象一律判为 unsupported
- 不新增配置开关
- 不改变 `run_fixup.py` 的执行策略

## Impact

- Affected specs:
  - `compare-objects`
  - `generate-fixup`
  - `export-reports`
- Affected code:
  - `schema_diff_reconciler.py`
  - `test_schema_diff_reconciler.py`
  - README / `readme_config.txt`（最小必要说明）

## Validation Plan

1. 单元测试
   - `blacklist_mode=disabled` 时，临时表触发器仍被判为 `TRIGGER_ON_TEMP_TABLE_UNSUPPORTED`
   - 同场景下非 trigger 依赖对象不被该规则误伤
   - 临时表 trigger DDL 输出到 `unsupported/trigger/`，不进入 `trigger/`

2. 实库验证（Oracle + OceanBase）
   - Oracle 创建 GTT + trigger 源对象
   - OceanBase 复现 `ORA-00600/-4007 simple dml trigger isn't used on user table not supported`
   - 对比程序产物，确认报告/unsupported DDL/普通 trigger 目录口径一致

3. 统计复核
   - 主报告汇总
   - `triggers_temp_table_unsupported_detail_<ts>.txt`
   - `unsupported_objects_detail_<ts>.txt`
   - `manual_actions_required_<ts>.txt`
   - report_db（如启用）

