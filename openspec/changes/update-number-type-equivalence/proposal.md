# Change: Normalize NUMBER-equivalent forms across Oracle and OceanBase

## Why
当前 NUMBER 的比较逻辑只覆盖了 `NUMBER(*,0)` 与 `NUMBER(38,0)` 的等价场景，导致 `NUMBER(*)`、`NUMBER(*,s)`、`NUMBER(p)` 与 `NUMBER(p,0)`、`DECIMAL/NUMERIC` 等等价写法在 Oracle 与 OceanBase 之间被误判为不一致，并生成错误的修补 SQL。需要扩展 NUMBER 等价规则，避免“等价写法误报”。

## What Changes
- 扩展 NUMBER 类型的规范化与比较规则：将 `NUMBER/DECIMAL/NUMERIC` 统一归一，补齐缺省 scale，处理 `*` 精度的等价映射（`*` 视为 38）。
- 在比较阶段识别等价写法并视为一致，不生成 number_precision 类型的 fixup。
- 修正 NUMBER 类型的展示/生成文字（例如 precision 为 NULL 且 scale 有值时，避免输出 `NUMBER(scale)`）。
- 增加单元测试与 compatibility_suite 实测用例，覆盖 Oracle 19c 与 OB 4.2.5.7 的 NUMBER 元数据与 DDL 兼容性矩阵。

## Impact
- Affected specs: `compare-objects`, `generate-fixup`.
- Affected code: `schema_diff_reconciler.py` (NUMBER 比较与格式化), tests (`test_schema_diff_reconciler.py`), compatibility suite (number matrix).
- No new switches by default; equivalence logic is deterministic.
