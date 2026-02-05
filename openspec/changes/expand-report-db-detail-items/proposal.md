# Change: 报告明细 JSON 行化入库（detail_item）

## Why
目前 DIFF_REPORT_DETAIL 的 `detail_json` 承载了大量差异细节（列缺失/类型差异/索引列清单/约束表达式/序列属性等），客户查询不方便，导致定位问题仍需依赖 TXT。用户诉求是“所有细节一行一个”，例如缺失 13 个序列应在数据库中可直接逐行查询。

## What Changes
- 新增细粒度明细表 `DIFF_REPORT_DETAIL_ITEM`，将 detail_json 中的关键细节展开为**行级记录**。
- 覆盖范围包括：
  - TABLE 列差异（缺失列/多余列/长度差异/类型差异）。
  - INDEX 差异（缺失/多余/表达式差异）。
  - CONSTRAINT 差异（缺失/多余/表达式差异/降级 PK）。
  - SEQUENCE 差异（缺失/多余/属性差异）。
  - TRIGGER 差异（缺失/多余/表达式差异）。
  - 不支持/阻断对象的原因细节（reason_code、dependency、root_cause 等）。
- 新增配置控制：
  - `report_db_detail_item_enable`（默认 true when report_db_store_scope=full）
  - `report_db_detail_item_max_rows`（默认与 report_db_detail_max_rows 相同，0 不限制）
- 保留原 `detail_json`（兼容现有逻辑和 TXT），但新增行化表用于查询。

## Impact
- Affected specs: `export-reports`, `configuration-control`
- Affected code: `schema_diff_reconciler.py`（写库扩展 + 行化生成）
- Affected docs: `readme_config.txt`, `docs/ADVANCED_USAGE.md`, `HOW_TO_READ_REPORTS_IN_OB.txt`
- Non-breaking: 旧表结构不变；新增表和开关可控
