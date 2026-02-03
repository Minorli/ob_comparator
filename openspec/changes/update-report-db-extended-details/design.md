## Context
- 已有 report_to_db 将 summary/counts/detail/grants 落库。
- 客户要求 usability_check_detail、package_compare、trigger_status 也需落库。
- 缺失/不支持明细已存在于 DIFF_REPORT_DETAIL，可通过查询实现，无需新表。

## Goals / Non-Goals
- Goals:
  - 将三类报告数据结构化入库，便于检索与趋势分析。
  - 保持 report_id 与 run 目录关联。
  - 保留策略统一为 report_retention_days（默认 90 天）。
- Non-Goals:
  - 不将 package diff 大文本入库（仅摘要+hash+路径）。
  - 不新增 per-type missing/unsupported 表。

## Decisions
### Decision 1: 新增三张表
- `DIFF_REPORT_USABILITY`
  - 记录对象可用性校验明细。
- `DIFF_REPORT_PACKAGE_COMPARE`
  - 记录 package / package body 对比摘要、hash、文件路径。
- `DIFF_REPORT_TRIGGER_STATUS`
  - 记录触发器状态差异、启用状态、有效性。

### Decision 2: 缺失/不支持明细复用 DIFF_REPORT_DETAIL
- 通过 report_type + object_type 查询获取。
- 避免重复存储与字段分裂。

### Decision 3: 保留策略复用 report_retention_days
- 新表执行相同清理逻辑。

### Decision 4: report_to_db 默认开启
- 配置缺失时默认 true，减少用户遗忘导致的“仅文本无库内数据”问题。

## Schema Draft
### DIFF_REPORT_USABILITY
- REPORT_ID VARCHAR2(64) NOT NULL
- OBJECT_TYPE VARCHAR2(32)
- SCHEMA_NAME VARCHAR2(128)
- OBJECT_NAME VARCHAR2(256)
- USABLE NUMBER(1)  (1=可用,0=不可用)
- STATUS VARCHAR2(32)
- REASON VARCHAR2(1024)
- DETAIL_JSON CLOB
- CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP

### DIFF_REPORT_PACKAGE_COMPARE
- REPORT_ID VARCHAR2(64) NOT NULL
- SCHEMA_NAME VARCHAR2(128)
- OBJECT_NAME VARCHAR2(256)
- OBJECT_TYPE VARCHAR2(32)  (PACKAGE/PACKAGE BODY)
- SRC_STATUS VARCHAR2(32)
- TGT_STATUS VARCHAR2(32)
- DIFF_STATUS VARCHAR2(32)
- DIFF_HASH VARCHAR2(64)
- DIFF_SUMMARY VARCHAR2(4000)
- DIFF_PATH VARCHAR2(512)
- CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP

### DIFF_REPORT_TRIGGER_STATUS
- REPORT_ID VARCHAR2(64) NOT NULL
- SCHEMA_NAME VARCHAR2(128)
- TRIGGER_NAME VARCHAR2(256)
- SRC_EVENT VARCHAR2(256)
- TGT_EVENT VARCHAR2(256)
- SRC_ENABLED VARCHAR2(8)
- TGT_ENABLED VARCHAR2(8)
- SRC_VALID VARCHAR2(16)
- TGT_VALID VARCHAR2(16)
- DIFF_STATUS VARCHAR2(32)
- REASON VARCHAR2(1024)
- CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP

## Indexing
- 所有表建立 (REPORT_ID, OBJECT_TYPE/SCHEMA/NAME) 组合索引，支撑 report_id + 类型查询。

## Risks / Trade-offs
- 报告写库量增加，可能影响性能；通过 batch 写入与 retention 清理缓解。

## Migration Plan
- 在 ensure_report_db_tables_exist 中新增表与索引创建。
- 写入逻辑扩展（save_report_to_db）。
- 保留策略扩展到新表。

## Open Questions
- 无
