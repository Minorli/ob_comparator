# Change: Add object usability checks for VIEW and SYNONYM

## Why
现有对比仅判断对象“是否存在”，无法识别“存在但不可用”的 VIEW/SYNONYM（依赖断裂、权限不足、FORCE 创建等）。迁移后期客户最关心的是“对象能否真正可用”，需要明确报告与定位原因。

## What Changes
- 新增可选的对象可用性校验（仅 VIEW/SYNONYM），通过 `SELECT * FROM <obj> WHERE 1=2` 验证可用性。
- 支持源端可用性对照、超时保护与并发策略；采样为可选能力，默认关闭（不采样）。
- 新增可用性明细报告与汇总统计，并写入主报告摘要区。
- 新增配置开关/参数与说明文档。

## Impact
- Affected specs: compare-objects, export-reports, configuration-control
- Affected code: schema_diff_reconciler.py (metadata流程、对比流程、报告输出)
- Risks: 额外 SQL 校验带来的耗时与网络波动；通过超时/并发/采样控制
