# Change: 扩展触发器状态校验与不支持对象分流报告

## Why
- 触发器目前仅校验“是否存在”，忽略 VALID/INVALID 与 ENABLED/DISABLED 差异，迁移后状态不一致不可控。
- 黑名单表、临时表及视图不兼容点会导致 fixup 直接失败，缺失统计也被混在一起，客户难以判断哪些可直接修补、哪些必须改造。
- 主报告在几十万对象规模下过大，缺失清单与明细堆叠，缺乏“可修补 vs 需改造”的高层视角。

## What Changes
- 触发器增强：拉取 VALID/INVALID + ENABLED/DISABLED 双状态，比较并输出差异；触发器清单报告改名并纳入状态差异详情。
- 对象可用性分级：引入“支持/不支持/被阻断”分类，黑名单表、临时表、视图不兼容规则都会标记为不支持；依赖不支持对象的视图/同义词/触发器/PLSQL 标记为被阻断。
- DDL 分流：可支持对象仍进入 fixup 目录；不支持/被阻断对象的 DDL 输出到独立目录（如 tables_unsupported / views_unsupported），避免 fixup 执行时反复报错。
- 视图清洗规则扩展：识别 SYS.OBJ$、DBLINK、缺失的 DBA/ALL 视图等不兼容点；对 DBA_USERS.USER_ID 做兼容性替换；生成不支持原因与依赖信息。
- 报告拆分：主报告仅保留摘要与行动指引，明细移入按类型拆分的子报告；缺失统计新增“不支持/被阻断”列。
- 配置与文档：新增必要的配置项与说明，最终版本更新至 v0.9.8。

## Impact
- Affected specs: compare-objects, export-reports, generate-fixup, configuration-control
- Affected code: schema_diff_reconciler.py, README.md, readme_config.txt, config.ini/template, docs
