# Change: Usability detail root-cause enrichment

## Why
当前 `usability_check_detail` 报告中的原因/建议过于粗糙，仅能输出“视图编译错误/重新编译”等泛化描述，无法指导用户定位具体依赖对象、权限或语法问题。用户需要在报告中直接看到**准确根因**和**可执行的排查建议**，并明确与依赖链/权限链的关联。

## What Changes
- 将可用性错误分类文案“视图编译错误”改为“视图查询报错”。
- 将 `root_cause` 与 `recommendation` 从“单一句式”升级为**细化原因 + 具体对象/权限提示**。
- 结合 `dependency_chains` / `VIEWs_chain` / 依赖汇总数据，在可用性明细中补充：
  - 失败视图的**第一层依赖对象**
  - 若依赖对象缺失/不支持/被黑名单阻断，明确标出
  - 若权限不足，指示缺失的对象/权限
- 维持当前“缺失对象/不支持对象”的可用性检查行为（SKIPPED）不变，但在明细中给出更精确原因。

## Impact
- Affected specs: `export-reports`
- Affected code: `schema_diff_reconciler.py`（可用性分类与明细导出）
- Reports: `usability_check_detail_<ts>.txt`

