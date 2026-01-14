# Change: Add synonym fixup scope toggle

## Why
当前同义词修补脚本默认包含 PUBLIC 与私有同义词。需要一个可配置开关来控制只生成 PUBLIC 同义词，避免输出过量或重复。

## What Changes
- 新增配置开关控制同义词修补脚本范围（全部非系统同义词 vs 仅 PUBLIC 同义词）。
- 修补脚本生成阶段按开关过滤 SYNONYM 输出，并避免重复/冲突的筛选逻辑。

## Impact
- Affected specs: generate-fixup, configuration-control
- Affected code: schema_diff_reconciler.py (fixup generation + config load)
- Docs/config: config.ini.template, readme_config.txt, README.md
