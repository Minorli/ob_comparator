# Change: View constraint cleanup modes (auto/force/off) + reports

## Why
Oracle 支持在 VIEW 列清单中定义约束（如 `CONSTRAINT ... RELY DISABLE`），但 OceanBase 4.2.5.7 实测不支持该语法，创建会报 ORA-00900。当前程序不会识别此类语法：
- 可安全清洗的（RELY DISABLE / DISABLE / NOVALIDATE）未自动处理
- 不可清洗的（ENABLE 或不明确状态）缺少显式报表与提示

客户希望：
1) 能清洗的自动清洗；
2) 无法清洗的明确列出；
3) 需要开关控制，默认安全清洗。

## What Changes
- 新增配置开关：`view_constraint_cleanup = auto|force|off`（默认 auto）
  - **auto**：仅清除 `RELY DISABLE / DISABLE / NOVALIDATE` 的 VIEW 约束条目
  - **force**：无条件清除 VIEW 约束条目（包括 ENABLE）
  - **off**：完全不清洗
- 新增两类报告（写入 run 目录并纳入 report_index）：
  - `view_constraint_cleaned_detail_<ts>.txt`：已清洗（含规则与片段）
  - `view_constraint_uncleanable_detail_<ts>.txt`：无法清洗（含原因与片段）
- 当清洗导致支持性变化时：
  - 可清洗 VIEW 归类为 **SUPPORTED missing**（进入 fixup）
  - 不可清洗 VIEW 归类为 **UNSUPPORTED/BLOCKED**（进入 unsupported）
  - 统计口径随分类变化：不支持/阻断下降、可修补上升（总缺失不变）

## Impact
- Affected specs: `compare-objects`, `generate-fixup`, `export-reports`, `configuration-control`
- Affected code: `schema_diff_reconciler.py`（view DDL 解析/清洗、compat 判定、报告导出、主报告提示）
- Behavior: 默认更智能（auto），但通过 off 保留旧行为

## Evidence (OB test)
在 OceanBase 4.2.5.7 中执行：
```
CREATE OR REPLACE FORCE VIEW VW_CONS_TEST (
  NOTI_DATE,
  REGSNO,
  CONSTRAINT "CLAIM_INFO_PK" PRIMARY KEY (NOTI_DATE, REGSNO) RELY DISABLE
) AS SELECT NOTI_DATE, REGSNO FROM T_CONS_TEST;
```
返回 ORA-00900 语法错误；移除 CONSTRAINT 子句后创建成功。
