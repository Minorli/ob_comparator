## 1. Implementation
- [x] 1.1 定义新增报告表 DDL（USABILITY / PACKAGE_COMPARE / TRIGGER_STATUS）
- [x] 1.2 扩展 report_to_db 写入流程（插入三类新数据）
- [x] 1.3 复用 report_retention_days 清理新表历史记录
- [x] 1.4 保持缺失/不支持明细不新增表，提供查询模板
- [x] 1.5 report_to_db 默认值调整为 true（含模板与向导）

## 2. Tests
- [x] 2.1 单元测试：写入数据行构造（hash/路径/摘要字段）
- [x] 2.2 单元测试：保留清理覆盖新增表

## 3. Docs
- [x] 3.1 更新 readme_config.txt 说明 DB 报告范围与新表
- [x] 3.2 更新 docs/ADVANCED_USAGE.md：新增查询模板
