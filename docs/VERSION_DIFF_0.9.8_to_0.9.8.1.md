# 版本对比清单 — 0.9.8 → 0.9.8.1

对比范围：V0.9.8 与 V0.9.8.1

## 核心变化概览
- VIEW remap 别名误替换修复 + 回归测试。
- SYS_C* 列识别增强，支持复杂后缀，修补改为 `ALTER TABLE ... FORCE`。
- run_fixup 迭代失败统计改为累计汇总。
- config.ini.template 去重，统计脚本模板统一。
- 文档更新与版本号推进到 0.9.8.1。

## 关键提交（按时间倒序）
- `2a79a3b` Bump version to 0.9.8.1 and update docs
- `f8994d4` Fix fixup summary counts and tooling hygiene
- `50ac3f2` Handle SYS_C columns with suffixes
- `0c56e77` Add view remap regression tests
- `c7562b6` Fix view remap alias replacement

## 主要文件变更清单
- `schema_diff_reconciler.py`
  - 版本号更新至 0.9.8.1
  - SYS_C 列识别增强（后缀兼容）
- `run_fixup.py`
  - 迭代模式累计失败数修复
  - 统一错误预览与异常日志
- `collect_source_object_stats.py`
  - INDEX/CONSTRAINT/TRIGGER SQL 模板统一
  - 清理未使用 import
- `config.ini.template`
  - 去重 `ddl_*` 配置项
- `readme_config.txt`
  - 版本号与注意事项更新
- `README.md` / `docs/*`
  - 版本号与新增说明更新
- `test_run_fixup.py` / `test_collect_source_object_stats.py` / `test_config_template.py`
  - 新增回归测试与模板重复检测

## 兼容性与行为差异
- 现有配置兼容，新增/强化项为可选开关或输出增强。
- 建议查看 `readme_config.txt` 获取完整配置说明与注意事项。
