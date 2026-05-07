# 文档索引

`docs/` 目录现在只保留当前仍有交付价值的主文档，不再堆历史版本快照、版本 diff 和规划草稿。

## 当前保留
- `ADVANCED_USAGE.md`：高级用法、report_to_db、run_fixup、DDL 清洗与授权策略
- `ARCHITECTURE.md`：架构与主流程分层
- `CHANGELOG.md`：版本变更总表（统一替代旧的 release note / version diff）
- `DEPLOYMENT.md`：离线部署、打包与交付建议
- `RELEASE_CHECKLIST.md`：发版前后检查清单，覆盖证据、打包、GitHub release 与回滚
- `RELEASE_GOVERNANCE.md`：发布门禁与 evidence JSON 口径
- `TECHNICAL_SPECIFICATION.md`：规格口径与关键实现约束

## 阅读顺序
1. 日常使用先看 `README.md` 与 `readme_config.txt`
2. 需要配置细节时看 `readme_config.txt`
3. 需要运行/排障技巧时看 `ADVANCED_USAGE.md`
4. 需要理解程序边界与实现口径时看 `TECHNICAL_SPECIFICATION.md`
5. 需要部署交付时看 `DEPLOYMENT.md`
6. 需要回溯最近版本变化时看 `CHANGELOG.md`
7. 准备公开发版时看 `RELEASE_CHECKLIST.md` 与 `RELEASE_GOVERNANCE.md`

## 说明
- `HOW_TO_READ_REPORTS_IN_OB_latest.txt` 与时间快照文件不放在 `docs/`，因为它们是交付给客户/DBA 的数据库侧排障手册。
- `report_sql_<timestamp>.txt` 只提供 `report_id` 与 HOW TO 入口，不再内嵌 HOW TO 正文。
