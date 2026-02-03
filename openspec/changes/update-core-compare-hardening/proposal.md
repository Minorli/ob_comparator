# Change: Core Compare Hardening (PUBLIC owner normalization + FK update_rule + view alias safety)

## Why
近期审查发现 PUBLIC/__public 语义不一致、FK update_rule 比对缺失、视图别名替换风险以及 obclient 传参稳定性问题。这些问题会引发误报、漏报或修复脚本错误，需要在不改变整体逻辑的前提下修复与验证。

## What Changes
- 统一 PUBLIC 与 OB __public 的逻辑 owner 语义，报告/映射一律使用 PUBLIC 表达。
- FK 约束比对补齐 update_rule 规则。
- 明确视图依赖重写仅替换对象名，不替换别名（并加入回归验证）。
- obclient SQL 传参从 `-e` 改为 stdin 方式，提升含特殊字符 SQL 的稳定性。
- sys.exit 改为异常链路，在并发环境中保证错误可追踪并安全收敛。

## Impact
- Affected specs: compare-objects, resolve-remap
- Affected code: schema_diff_reconciler.py (metadata load, constraint compare, view remap, obclient, error handling)
- Reports: 不应改变总体数量，仅修正 PUBLIC/ __public 相关误报

## Non-Goals
- 不修改触发器比对/清洗/生成逻辑。
- 不引入新的用户开关或改变现有配置语义。
- 不调整 fixup 选择策略（除 PUBLIC/ __public 误判修正）。
