# Release Notes — V0.9.8.3

发布时间：2026-02-05

## 重点更新
- report_to_db 扩展覆盖范围，支持 full 模式写入更多报告表。
- 新增 DIFF_REPORT_DETAIL_ITEM（明细行化），便于按列级别排查。
- 新增 report_db_store_scope / report_db_detail_item_enable 等配置。
- HOW_TO_READ_REPORTS_IN_OB SQL 全量校验并修正。

## 兼容性
- 保持与 0.9.8.x 系列兼容；新增表仅在 report_to_db=true 时创建。

## 说明
- 建议使用 report_db_store_scope=full 以获取更完整的排查视角。
