# 版本对比清单 — 0.9.8.2 → 0.9.8.3

对比范围：V0.9.8.2 与 V0.9.8.3

## 版本号
- 版本号更新至 0.9.8.3

## 主要变化
- report_to_db 扩展写库范围（full 模式更多表）。
- 新增 DIFF_REPORT_DETAIL_ITEM 细粒度差异入库。
- 新增 DIFF_REPORT_ARTIFACT_LINE 报告文本逐行入库（full 模式可 DB 侧完整复盘 txt）。
- 新增 report_db_store_scope / report_db_detail_item_enable。
- 新增报告分析视图（ACTIONS / OBJECT_PROFILE / TRENDS / PENDING_ACTIONS / GRANT_CLASS / USABILITY_CLASS）。
- 新增写库失败追踪与整改闭环表（WRITE_ERRORS / RESOLUTION）。
- HOW_TO_READ_REPORTS_IN_OB_77_sqls.txt SQL 全量校验修正，并补充新视图/追踪查询模板。
