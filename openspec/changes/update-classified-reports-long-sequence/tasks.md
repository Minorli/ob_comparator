## 1. Implementation
- [ ] 1.1 LONG 黑名单表从阻断源集合移除；缺失表生成 fixup（LONG/LONG RAW→CLOB/BLOB）
- [ ] 1.2 阻断根因追溯字段 root_cause（ObjectSupportReportRow 扩展）
- [ ] 1.3 导出 per-type missing/unsupported 明细（管道符格式）并写入 report_index
- [ ] 1.4 更新主报告指引与 REPORTS_CATALOG
- [ ] 1.5 Sequence 比较改为 existence-only，移除 mismatch 统计与明细

## 2. Tests
- [ ] 2.1 BLACKLIST_DEPENDENCY_TEST_CASE 全覆盖（SPE/DIY 阻断、LONG 不阻断）
- [ ] 2.2 per-type 明细输出与字段校验（含 ROOT_CAUSE）
- [ ] 2.3 Sequence existence-only：missing/extra/ok 无 mismatch
- [ ] 2.4 LONG 表缺失生成 fixup，依赖对象仍可生成
