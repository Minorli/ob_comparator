# 版本对比清单 — 0.9.8.5 → 0.9.8.6

对比范围：V0.9.8.5 与 V0.9.8.6

## 版本号
- 版本号更新至 `0.9.8.6`。

## 主要变化
- 新增“延后授权”机制：不可执行授权从 `grants_miss` 分流，避免 run_fixup 当前轮无意义失败。
- 新增 `deferred_grants_detail_<ts>.txt`，并支持 report_db 查询。
- 新增 `fixup_scripts/grants_deferred/README.txt`，确保后续授权不遗漏。
- `run_fixup.py` 默认跳过 `grants_deferred`，需对象补齐后显式执行。
- `name_collision` 修复层级前置，低版本 OB 的约束重命名自动回退到 `DROP + ADD`。
- 默认排除 `MLOG$_*`，并优化 `LONG/LOB_OVERSIZE` 依赖阻断口径，减少噪声与误阻断。

## 文档同步
- README / readme_config / ARCHITECTURE / TECHNICAL_SPECIFICATION / DEPLOYMENT / ADVANCED_USAGE 已同步升级到 `0.9.8.6`。
