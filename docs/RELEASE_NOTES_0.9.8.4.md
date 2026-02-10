# Release Notes — V0.9.8.4

发布时间：2026-02-10

## 重点更新
- TRIGGER 扩展校验修复：按 `OWNER.TRIGGER_NAME` 比较，修复同表跨 schema 同名触发器误报 `EXTRA_TRIGGER`。
- CONSTRAINT 扩展校验降噪：在签名层与对比层双重忽略 OceanBase 自动 `*_OBNOTNULL_*` 约束，减少 `NOT NULL` 命名差异噪声。
- report_to_db `full` 模式增强：新增 `DIFF_REPORT_ARTIFACT_LINE`，支持 run 目录 txt 报告逐行入库，数据库侧可完整复盘文本报告。
- 文档与版本统一升级到 `0.9.8.4`（README、配置说明、核心设计/部署/高级使用文档同步）。

## 兼容性
- 与 `0.9.8.x` 系列保持兼容；本次修复为降噪与正确性增强，不改变既有修补脚本执行入口。

## 说明
- 建议继续使用 `report_to_db=true` + `report_db_store_scope=full`，优先通过数据库查询定位差异根因，再回看 txt 细节文件。
