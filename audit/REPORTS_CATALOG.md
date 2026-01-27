# ob_comparator 报告文件目录（精确版）
# 生成日期: 2026-01-27
# 重要: 本文档准确描述每个报告的数据来源和内容

==============================================================================
一、主报告 (REPORT)
==============================================================================

文件名: report_{timestamp}.txt
说明:   主校验报告，Rich 格式转纯文本
生成条件: 无条件生成
内容:   检查汇总表、缺失对象、差异对象、扩展对象校验、依赖分析等全部内容

==============================================================================
二、明细报告 (DETAIL)
==============================================================================

1. missing_objects_detail_{timestamp}.txt
   说明: 缺失对象支持性明细
   生成条件: report_detail_mode=split 且数据非空
   数据来源: support_summary.missing_detail_rows (来自 classify_missing_objects)
   字段: SRC_FULL | TYPE | TGT_FULL | STATE | REASON_CODE | REASON | DEPENDENCY | ACTION | DETAIL
   对象类型: TABLE, VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, SYNONYM, TYPE, TYPE BODY 等主对象

2. unsupported_objects_detail_{timestamp}.txt
   说明: 不支持/阻断对象明细
   生成条件: report_detail_mode=split 且数据非空
   数据来源: support_summary.unsupported_rows
   字段: 同 missing_objects_detail
   ★重要: 包含因依赖不支持表而被阻断的 INDEX/CONSTRAINT/TRIGGER
   REASON_CODE: DEPENDENCY_UNSUPPORTED(依赖不支持表), VIEW_COMPAT_*(VIEW兼容性), DBLINK_*(DBLINK问题) 等

3. indexes_unsupported_detail_{timestamp}.txt
   说明: ★仅 DESC 列索引★ 的不支持明细
   生成条件: extra_results["index_unsupported"] 非空（不受 report_detail_mode 控制）
   数据来源: classify_unsupported_indexes() 返回的 IndexUnsupportedDetail
   字段: TABLE | INDEX_NAME | COLUMNS | REASON_CODE | REASON | OB_ERROR_HINT
   REASON_CODE: 固定为 INDEX_DESC
   ★注意: 不包含因依赖不支持表而被阻断的索引（那些在 unsupported_objects_detail）

4. constraints_unsupported_detail_{timestamp}.txt
   说明: ★仅 DEFERRABLE 等语法不支持★ 的约束明细
   生成条件: extra_results["constraint_unsupported"] 非空（不受 report_detail_mode 控制）
   数据来源: classify_unsupported_check_constraints() 返回的 ConstraintUnsupportedDetail
   字段: TABLE | CONSTRAINT_NAME | SEARCH_CONDITION | REASON_CODE | REASON | OB_ERROR_HINT
   REASON_CODE: DEFERRABLE_CONSTRAINT 等
   ★注意: 不包含因依赖不支持表而被阻断的约束（那些在 unsupported_objects_detail）

5. extra_targets_detail_{timestamp}.txt
   说明: 目标端多余对象明细
   生成条件: report_detail_mode=split 且数据非空
   数据来源: tv_results["extra_targets"] (来自 check_primary_objects)
   字段: TYPE | TARGET_FULL
   对象类型: ★仅 PRIMARY_OBJECT_TYPES★ (TABLE, VIEW, PROCEDURE, FUNCTION, PACKAGE, 
            PACKAGE BODY, SYNONYM, JOB, SCHEDULE, TYPE, TYPE BODY)
   ★注意: 不包含 SEQUENCE/INDEX/CONSTRAINT/TRIGGER（它们的多余在 extra_mismatch_detail）

6. skipped_objects_detail_{timestamp}.txt
   说明: 仅打印未校验对象明细
   生成条件: report_detail_mode=split 且数据非空
   数据来源: tv_results["skipped"]
   字段: TYPE | SRC_FULL | TGT_FULL | REASON
   适用: MATERIALIZED VIEW 等配置为 print_only 的类型

7. mismatched_tables_detail_{timestamp}.txt
   说明: 表列不匹配明细
   生成条件: report_detail_mode=split 且数据非空
   数据来源: tv_results["mismatched"] 中 TYPE=TABLE 的项
   字段: TABLE | MISSING_COLS | EXTRA_COLS | LENGTH_MISMATCHES | TYPE_MISMATCHES

8. column_order_mismatch_detail_{timestamp}.txt
   说明: 列顺序差异明细
   生成条件: report_detail_mode=split 且数据非空
   数据来源: tv_results["column_order_mismatched"]
   字段: TABLE | SRC_ORDER | TGT_ORDER

9. comment_mismatch_detail_{timestamp}.txt
   说明: 注释差异明细
   生成条件: report_detail_mode=split 且数据非空
   数据来源: comment_results["mismatched"]
   字段: TABLE | TABLE_COMMENT_DIFF | MISSING_COLS | EXTRA_COLS | COLUMN_COMMENT_DIFFS

10. extra_mismatch_detail_{timestamp}.txt
    说明: 扩展对象（INDEX/CONSTRAINT/SEQUENCE/TRIGGER）差异明细
    生成条件: report_detail_mode=split 且数据非空
    数据来源: extra_results 中的 index_mismatched, constraint_mismatched, 
             sequence_mismatched, trigger_mismatched
    字段: TYPE | OBJECT | MISSING | EXTRA | DETAIL
    ★重要: SEQUENCE 的多余数据在此报告，不在 extra_targets_detail

11. noise_suppressed_detail_{timestamp}.txt
    说明: 降噪明细（被自动过滤的系统生成对象）
    生成条件: report_detail_mode=split 且数据非空
    数据来源: noise_suppressed_details
    字段: TYPE | SCOPE | REASON | IDENTIFIERS | DETAIL

12. dependency_detail_{timestamp}.txt
    说明: 依赖关系差异明细
    生成条件: report_detail_mode=split 且数据非空
    数据来源: dependency_report 中的 missing, unexpected, skipped
    字段: CATEGORY | DEPENDENT | DEPENDENT_TYPE | REFERENCED | REFERENCED_TYPE | REASON
    CATEGORY值: MISSING(缺失依赖), EXTRA(多余依赖), SKIPPED(跳过)

13. package_compare_{timestamp}.txt
    说明: PACKAGE/PACKAGE BODY 对比明细
    生成条件: PACKAGE 或 PACKAGE BODY 在校验范围且有结果
    数据来源: package_results["rows"]
    字段: SRC_FULL | TYPE | SRC_STATUS | TGT_FULL | TGT_STATUS | RESULT | ERROR_COUNT | FIRST_ERROR

==============================================================================
三、辅助报告 (AUX)
==============================================================================

1. report_index_{timestamp}.txt
   说明: 报告索引
   生成条件: 无条件生成
   字段: CATEGORY | PATH | ROWS | DESCRIPTION

2. object_mapping_{timestamp}.txt
   说明: 全量对象映射（源端->目标端）
   生成条件: fixup 生成时
   格式: SRC_FULL<TAB>OBJECT_TYPE<TAB>TGT_FULL（每行一个映射）

3. remap_conflicts_{timestamp}.txt
   说明: 无法自动推导的对象（需手动配置 remap_rules.txt）
   生成条件: 存在冲突时
   格式: SRC_FULL<TAB>OBJECT_TYPE<TAB>REASON

4. dependency_chains_{timestamp}.txt
   说明: 对象依赖链（下探到 TABLE/MVIEW）
   生成条件: 存在依赖关系时
   内容: [SOURCE - ORACLE] 源端依赖链 + [TARGET - REMAPPED] 目标端依赖链 + 依赖环检测

5. VIEWs_chain_{timestamp}.txt
   说明: VIEW 依赖链（用于 fixup 执行排序）
   生成条件: 存在 VIEW 缺失时
   内容: VIEW 的拓扑排序，确保依赖先创建

6. blacklist_tables.txt
   说明: 黑名单表清单
   生成条件: 存在黑名单配置时
   字段: TABLE_FULL | BLACK_TYPE | DATA_TYPE | STATUS | DETAIL | REASON
   特殊功能: LONG/LONG RAW 会校验目标端是否已转换为 CLOB/BLOB

7. trigger_status_report.txt
   说明: 触发器状态/清单报告
   生成条件: 使用 trigger_list 或存在触发器状态差异时
   内容:
     - [Section] trigger_list 清单筛选: ENTRY | STATUS | DETAIL
     - [Section] 触发器状态差异: TRIGGER | SRC_EVENT | TGT_EVENT | SRC_ENABLED | TGT_ENABLED | SRC_VALID | TGT_VALID | DETAIL

8. filtered_grants.txt
   说明: 过滤掉的不兼容 GRANT 权限
   生成条件: 存在过滤权限时
   字段: CATEGORY | GRANTEE | PRIVILEGE | OBJECT | REASON

9. fixup_skip_summary_{timestamp}.txt
   说明: Fixup 跳过原因汇总
   生成条件: fixup 生成时
   内容: 按对象类型分组，显示 missing_total, task_total, generated, 以及各 skip 原因的数量

10. ddl_format_report_{timestamp}.txt
    说明: DDL 格式化报告（SQLcl 格式化统计）
    生成条件: ddl_format_enable=true
    内容: 各对象类型的 formatted/skipped/failed 数量

11. ddl_punct_clean_{timestamp}.txt
    说明: 全角标点清洗报告
    生成条件: 存在全角标点替换时
    字段: TYPE | OBJECT | REPLACED | SAMPLES

12. ddl_hint_clean_{timestamp}.txt
    说明: DDL hint 清洗报告
    生成条件: 存在 hint 处理时
    字段: TYPE | OBJECT | POLICY | TOKENS | KEPT | REMOVED | UNKNOWN | KEPT_SAMPLES | REMOVED_SAMPLES | UNKNOWN_SAMPLES

==============================================================================
四、OMS 迁移辅助目录
==============================================================================

目录: missed_tables_views_for_OMS/
说明: 缺失 TABLE/VIEW 规则目录

1. {SCHEMA}_T.txt
   说明: 缺失的 TABLE 列表（供 OMS 消费）
   生成条件: 存在缺失 TABLE 且非黑名单/非阻断
   格式: 每行一个表名

2. {SCHEMA}_V.txt
   说明: 缺失的 VIEW 列表
   生成条件: 存在缺失 VIEW 且非阻断
   格式: 每行一个视图名

==============================================================================
五、run_fixup.py 执行报告
==============================================================================

文件名: fixup_scripts/errors/fixup_errors_{timestamp}.txt
说明:   Fixup 执行错误报告
生成条件: 执行 fixup 时有错误
字段:   FILE | STMT_INDEX | ERROR_CODE | OBJECT | MESSAGE

==============================================================================
六、报告目录结构示例
==============================================================================

reports/
└── run_{timestamp}/
    ├── report_{timestamp}.txt
    ├── report_index_{timestamp}.txt
    ├── missing_objects_detail_{timestamp}.txt
    ├── unsupported_objects_detail_{timestamp}.txt
    ├── constraints_unsupported_detail_{timestamp}.txt
    ├── indexes_unsupported_detail_{timestamp}.txt
    ├── mismatched_tables_detail_{timestamp}.txt
    ├── column_order_mismatch_detail_{timestamp}.txt
    ├── comment_mismatch_detail_{timestamp}.txt
    ├── extra_mismatch_detail_{timestamp}.txt
    ├── dependency_detail_{timestamp}.txt
    ├── package_compare_{timestamp}.txt
    ├── object_mapping_{timestamp}.txt
    ├── dependency_chains_{timestamp}.txt
    ├── VIEWs_chain_{timestamp}.txt
    ├── remap_conflicts_{timestamp}.txt
    ├── fixup_skip_summary_{timestamp}.txt
    ├── ddl_format_report_{timestamp}.txt
    ├── ddl_punct_clean_{timestamp}.txt
    ├── ddl_hint_clean_{timestamp}.txt
    ├── blacklist_tables.txt
    ├── trigger_status_report.txt
    ├── filtered_grants.txt
    └── missed_tables_views_for_OMS/
        ├── SCHEMA1_T.txt
        ├── SCHEMA1_V.txt
        └── ...

==============================================================================
七、汇总表与明细报告数据来源对照
==============================================================================

汇总表"不支持/阻断"列的数据来源与明细报告对应关系：

| 对象类型    | 汇总数来源                                      | 明细报告                                         |
|------------|---------------------------------------------|------------------------------------------------|
| TABLE/VIEW | missing_support_counts[unsupported+blocked] | unsupported_objects_detail                     |
| INDEX      | extra_blocked_counts["INDEX"]               | unsupported_objects_detail (依赖阻断)            |
|            | + extra_results["index_unsupported"]        | + indexes_unsupported_detail (仅DESC索引)       |
| CONSTRAINT | extra_blocked_counts["CONSTRAINT"]          | unsupported_objects_detail (依赖阻断)            |
|            | + extra_results["constraint_unsupported"]   | + constraints_unsupported_detail (仅DEFERRABLE) |
| TRIGGER    | extra_blocked_counts["TRIGGER"]             | unsupported_objects_detail                     |

汇总表"多余"列的数据来源与明细报告对应关系：

| 对象类型           | 汇总数来源                      | 明细报告              |
|------------------|--------------------------------|------------------------|
| PRIMARY_OBJECTS  | extra_targets                  | extra_targets_detail   |
| SEQUENCE         | sequence_mismatched.extra      | extra_mismatch_detail  |
| INDEX            | index_mismatched.extra         | extra_mismatch_detail  |
| CONSTRAINT       | constraint_mismatched.extra    | extra_mismatch_detail  |
| TRIGGER          | trigger_mismatched.extra       | extra_mismatch_detail  |

★重要★: 如果汇总显示有不支持索引，但 indexes_unsupported_detail 未生成，
       请检查 unsupported_objects_detail 中 TYPE=INDEX 的行（因依赖阻断）。

==============================================================================
八、统计
==============================================================================

报告类型        数量
--------        ----
主报告          1
明细报告        13
辅助报告        12
OMS 辅助        2+
执行报告        1
--------        ----
总计            25+

==============================================================================
# END OF FILE
==============================================================================
