## 1. Implementation
- [x] 1.1 将 `constraint_status_sync_mode` 默认值从 `enabled_only` 改为 `full`
- [x] 1.2 同步更新配置模板、配置说明、README 与技术文档

## 2. Verification
- [x] 2.1 `openspec validate update-constraint-status-sync-default-full --strict`
- [x] 2.2 `python3 -m py_compile $(git ls-files '*.py')`
- [x] 2.3 `.venv/bin/python -m unittest test_schema_diff_reconciler`
- [x] 2.4 Oracle + OceanBase 实库验证已存在 FK 的 `VALIDATED -> NOT VALIDATED` 状态修复
