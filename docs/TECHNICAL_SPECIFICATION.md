• 系统定位
  本程序是 Oracle → OceanBase（Oracle 模式）迁移后的“结构一致性校验 + 修补脚本生成”引擎，设计核心是“Dump-Once, Compare-Locally + 人工审计前置”。它不自动执行任何 DDL，而是将修补方案落为脚本，强制引入人工审核，保证生产安全。

  全流程概述

  1. 读取配置并进行运行前自检（路径、依赖、权限、目录可写性）。
  2. 连接 Oracle（Thick Mode）批量拉取元数据与权限、依赖。
  3. 通过 obclient 批量拉取 OceanBase 元数据与依赖。
  4. 解析 remap 规则并做规则有效性校验。
  5. 构建完整对象映射与主检查清单（master_list），对 one-to-many/冲突场景做显式回退或报冲突。
  6. 执行主对象检查（TABLE/VIEW/PLSQL/TYPE/JOB/SCHEDULE/SYNONYM 等）与扩展对象检查（INDEX/CONSTRAINT/SEQUENCE/TRIGGER）。
  7. 可选执行注释一致性校验、依赖关系校验与依赖链导出。
  8. 可选生成修补脚本、授权脚本、重编译脚本，产出多维度报告与快照。
  9. 由 run_fixup.py 执行脚本（支持依赖排序、迭代重试、view-chain 自动修复与重编译）。

  配置与输入约束（核心控制面）

  - 连接与运行参数：config.ini.template 中的 [ORACLE_SOURCE]、[OCEANBASE_TARGET]、[SETTINGS]，含 obclient_timeout/cli_timeout/fixup_cli_timeout、fixup_dir/report_dir/log_dir、report_width、fixup_workers、progress_log_interval、dbcat_* 等。
  - 范围控制：check_primary_types、check_extra_types、check_dependencies、check_comments、generate_fixup、generate_grants、fixup_schemas、fixup_types、trigger_list。
  - 映射控制：remap_rules.txt（可选），未配置时默认 1:1 映射。
  - 额外输入：OMS_USER.TMP_BLACK_TABLE（黑名单过滤）、trigger_list（触发器筛选）、dbcat_output（DDL 缓存）。

  元数据采集机制

  Oracle 侧（schema_diff_reconciler.py）

  - 通过 oracledb Thick Mode 查询 DBA_OBJECTS、DBA_TAB_COLUMNS（含 CHAR_USED、CHAR_LENGTH、隐藏列标志 HIDDEN_COLUMN）、DBA_INDEXES/DBA_IND_COLUMNS、DBA_CONSTRAINTS/DBA_CONS_COLUMNS、DBA_TRIGGERS、DBA_SEQUENCES、DBA_DEPENDENCIES、DBA_TAB_COMMENTS/
    DBA_COL_COMMENTS、DBA_ERRORS。
  - 分区键列通过 DBA_PART_KEY_COLUMNS、DBA_SUBPART_KEY_COLUMNS 加载，用于 PK 降级策略。
  - 同义词从 DBA_SYNONYMS 批量读取并缓存元数据，避免逐个 DBMS_METADATA。
  - 过滤/去噪：跳过 SYS_IOT_OVER_* IOT 表；对 MVIEW 进行 TABLE/MVIEW 去重，避免误当表校验。

  OceanBase 侧

  - 使用 obclient 批量查询 DBA_OBJECTS、DBA_TAB_COLUMNS、DBA_INDEXES/DBA_IND_COLUMNS、DBA_CONSTRAINTS/DBA_CONS_COLUMNS、DBA_TRIGGERS、DBA_SEQUENCES、DBA_DEPENDENCIES、DBA_TAB_COMMENTS/DBA_COL_COMMENTS。
  - TYPE 与 TYPE BODY 补偿：DBA_TYPES 补齐 TYPE；当启用 TYPE BODY 检查时通过 DBA_SOURCE 探测实际存在的 TYPE BODY，避免误报。
  - 过滤/去噪：忽略 OB 自动生成的 *_OBNOTNULL_* 约束；忽略 OMS 自动索引（_OMS_ROWID 且列集含 OMS_* 四列）。

  对象映射与 Remap 推导

  - remap_rules.txt 逐行 SRC=TARGET 解析；非法规则（源对象不存在）写入 *_invalid.txt 并从内存映射中移除，避免误用。
  - master_list：仅包含主对象类型，作为主检查输入。
  - full_object_mapping：覆盖全部受管类型（含 INDEX/SEQUENCE/TRIGGER），供依赖、授权、fixup 全流程复用。
  - one-to-many 冲突处理：若同一目标对象被多个源对象映射，当前对象回退为 1:1；对 PACKAGE/TYPE 与 BODY 强制同目标。
  - schema 推导策略：基于 TABLE 映射推导 schema mapping；遇到一对多时，独立对象依赖推导（基于依赖链统计目标 schema 出现频次）；依附对象（INDEX/CONSTRAINT/SEQUENCE）跟随父表 schema；TRIGGER 默认不跟随，仅显式 remap 生效。
  - NO_INFER_SCHEMA_TYPES：VIEW/MATERIALIZED VIEW/TRIGGER/PACKAGE/PACKAGE BODY 默认不走 schema 推导。

  ———

  检查阶段总体规则

  - 检查范围完全由 check_primary_types / check_extra_types / check_comments / check_dependencies 控制；未启用的类型不会被加载、对比或生成修复脚本。
  - 主对象检查输出：missing / mismatched / ok / skipped / extra_targets / extraneous。
  - 额外对象检查输出：索引、约束、序列、触发器分别记录 ok 与 mismatched 详情。

  主对象检查（逐类规则）

  TABLE
  存在性判定：目标端 DBA_OBJECTS 中存在 OWNER.TABLE 即通过，否则记为缺失。
  列集合对比：

  1. 源端列集：过滤 OMS 列（OMS_OBJECT_NUMBER/OMS_RELATIVE_FNO/OMS_BLOCK_NUMBER/OMS_ROW_NUMBER）与 Oracle Hidden 列（HIDDEN_COLUMN=YES）。
  2. 目标端列集：过滤 OMS 列（无论 VISIBLE/INVISIBLE）。
  3. 缺失列 = 源列集 - 目标列集；多余列 = 目标列集 - 源列集。
     长度校验（仅 VARCHAR/VARCHAR2）：

  - BYTE 语义：目标长度需满足 ceil(src * 1.5) 下限；超过 ceil(src * 2.5) 记为 oversize（提示人工评估）。
  - CHAR 语义：目标长度必须与源端完全一致。
    类型校验（仅 LONG/LONG RAW）：
  - Oracle LONG → OB CLOB，Oracle LONG RAW → OB BLOB，若目标类型不符合则记录类型不匹配。
    特殊处理：若源端列元数据缺失，则将该表标记为 mismatched，并注明“源端列信息获取失败”。

  VIEW
  仅检查存在性：目标端存在即 OK；否则记缺失。

  MATERIALIZED VIEW
  默认 “仅打印不校验”（print-only），产生“skipped + 原因”，不进入缺失清单。

  PROCEDURE / FUNCTION / TYPE / TYPE BODY / JOB / SCHEDULE
  仅检查存在性：目标端存在即 OK；否则记缺失。
  TYPE BODY 的目标端存在性仅在 check_primary_types 包含 TYPE BODY 时基于 DBA_SOURCE 进行准确判定，避免误报。

  SYNONYM
  仅检查存在性：目标端存在即 OK；否则记缺失。
  源端同义词仅纳入指向 source_schemas 的对象；PUBLIC 同义词也遵从相同过滤。

  PACKAGE / PACKAGE BODY
  不走主对象检查；单独走有效性与错误对比：

  - SOURCE_INVALID：源端 INVALID（不计入 mismatch，但记录详情）。
  - MISSING_TARGET：目标端不存在。
  - TARGET_INVALID：目标端 INVALID，读取 DBA_ERRORS 的首条错误信息。
  - STATUS_MISMATCH：两端状态不一致但未落入上述分类。

  扩展对象检查（逐类规则）

  INDEX

  - 对比方式：忽略索引名称，按 “列顺序序列 + 唯一性” 做指纹匹配。
  - 过滤规则：
      1. OMS 自动索引（_OMS_ROWID + OMS 列全集）从元数据阶段剔除。
      2. 若 PK/UK 约束已覆盖列集，对应索引缺失不再重复报告。
      3. 处理 SYS_NC 列名差异：同名索引若仅 SYS_NC 列名不同则视为一致。
  - 唯一性判定：源 NONUNIQUE → 目标 UNIQUE 且被约束支撑时视为迁移正常，否则记录唯一性不一致。

  CONSTRAINT（PK/UK/FK）

  - PK/UK：按列序列匹配；目标端额外约束若列集存在于源端则忽略；_OMS_ROWID 约束忽略。
  - 分区键降级：当源端 PK 未包含分区键列时，允许目标端用 PK 或 UK 任何一种匹配；未匹配则记录为降级缺失。
  - FK：除列序列外，还校验被引用表是否经 remap 后一致；若目标端同列集引用表不一致，会记录“引用对象不一致”。
  - OB 自动 *_OBNOTNULL_* 约束在 OB 元数据阶段即被移除，避免误报“多余约束”。

  SEQUENCE

  - 以 schema 映射为单位比较序列名集合；缺失/多余分别报告。
  - 若 Oracle 侧序列元数据为 None，程序会判断该 schema 是否在其它元数据中出现：
      - 若 schema 事实上不存在，则跳过比较（避免误报）。
      - 若 schema 存在但无序列，目标端存在序列则按“额外序列”报告。

  TRIGGER

  - 缺失/多余对比采用 “源→目标 remap 后的全名集合” 比对。
  - 对于同名存在：额外校验触发事件与状态（STATUS）。
  - 缺失映射会写入 missing_mappings，为 fixup 和 trigger_list 过滤提供依据。

  注释一致性检查（TABLE/列注释）

  - 仅在 check_comments=true 且 Oracle/OB 注释元数据加载成功时执行。
  - 表注释：两端文本归一化后比较。
  - 列注释：过滤 OMS 列与 Oracle Hidden 列；比较缺失、额外与内容不一致。
  - 注释比对失败不会影响结构对比结果，但会在报告中独立呈现。

  依赖关系检查

  - Oracle DBA_DEPENDENCIES → remap 后形成“期望依赖集合”；OB DBA_DEPENDENCIES 形成“实际集合”。
  - 差集分类：missing / unexpected / skipped（映射缺失、对象未纳管）。
  - 缺失依赖会生成针对不同类型的明确行动建议（如 ALTER COMPILE、重建 VIEW、重建 SYNONYM）。
  - 可选输出 dependency_chains_*.txt 和 VIEWs_chain_*.txt，包含依赖链路径、目标对象存在性与授权状态。

  黑名单表（OMS_USER.TMP_BLACK_TABLE）

  - 黑名单表输出到 main_reports/blacklist_tables.txt，记录 black_type、data_type、原因与状态。
  - LONG/LONG RAW 黑名单表会额外对目标端进行 “是否已转换为 CLOB/BLOB” 校验，标记 VERIFIED/TYPE_MISMATCH/MISSING_COLUMN 等。
  - 黑名单表不会进入 tables_views_miss/（OMS 规则输出），避免误提交到 OMS。

  ———

  修补脚本生成（总体逻辑）

  - 入口：generate_fixup=true 且存在 master_list。
  - 安全清理：若 fixup_dir 位于运行目录或子目录，自动清空旧脚本；若位于外部路径则跳过清理以避免误删。
  - DDL 来源策略：
      1. dbcat（缓存优先，未命中再跑 dbcat）；
      2. DBMS_METADATA 兜底（VIEW 永远使用 DBMS_METADATA）；
      3. 同义词优先直接用 DBA_SYNONYMS 元数据拼 DDL。
  - 生成顺序：SEQUENCE → TABLE CREATE → TABLE ALTER → VIEW/其他对象 → INDEX → CONSTRAINT → TRIGGER → COMPILE → GRANTS。
  - 并发：fixup_workers 控制并发生成；progress_log_interval 控制进度日志。
  - 过滤：fixup_schemas/fixup_types 只允许指定范围生成；trigger_list 仅生成清单内触发器（异常/空清单自动回退全量）。

  Fixup 对象级逻辑（逐类说明）

  SEQUENCE

  - DDL 抽取：dbcat 或 DBMS_METADATA 兜底；记录来源统计。
  - 清洗：移除 NOKEEP/NOSCALE/GLOBAL，去掉 hints 与 Oracle 特有语法；加入 ALTER SESSION SET CURRENT_SCHEMA。
  - 输出目录：fixup_scripts/sequence/；可附加授权语句。

  TABLE（CREATE）

  - DDL 抽取：dbcat；若 DDL 含 “UNSUPPORTED”，尝试 DBMS_METADATA 兜底。
  - 自动增宽：对 BYTE 语义的 VARCHAR/VARCHAR2 统一放大到 ceil(src * 1.5)，避免修补后仍被长度校验拦截。
  - 清洗：去 storage/tablespace/hint/Oracle 专用语法，移除 ENABLE/ENABLE NOVALIDATE，清理 dbcat wrapper；必要时 normalize。
  - 输出目录：fixup_scripts/table/；附加授权语句。

  TABLE（ALTER，列差异修补）

  - 缺失列：生成 ALTER TABLE ... ADD，类型按源元数据构造；BYTE 语义 VARCHAR 放大；保留默认值与 NOT NULL。
  - 新增列注释：自动补 COMMENT ON COLUMN（过滤 OMS 列）。
  - 长度差异：
      - BYTE 语义短：生成 MODIFY 放大到下限；
      - CHAR 语义不一致：生成 MODIFY 强制等长；
      - 过大：仅输出 WARNING 注释，提醒人工评估是否收敛。
  - LONG/LONG RAW 类型差异：生成 MODIFY 切为 CLOB/BLOB。
  - 目标端多余列：仅输出注释掉的 DROP 建议。
  - 输出目录：fixup_scripts/table_alter/。

  VIEW

  - DDL 抽取：固定 DBMS_METADATA。
  - 版本感知：OB 版本 < 4.2.5.7 时移除 WITH CHECK OPTION；同时剥离 CHECK OPTION 的 CONSTRAINT 名称。
  - 语法清洗：移除 EDITIONABLE/BEQUEATH/SHARING/DEFAULT COLLATION/CONTAINER_MAP 等 Oracle 专有语法。
  - 质量修复：修复行内注释吞行、拆分列名（依据列元数据）。
  - 依赖重写：解析 FROM/JOIN/UPDATE/INTO 等引用，按 remap 替换对象；无前缀引用在同 schema 下强制补全为目标全名。
  - 拓扑排序：对缺失 VIEW 做依赖拓扑排序（Kahn），循环依赖放末尾创建。
  - 输出目录：fixup_scripts/view/；附加授权语句。

  MATERIALIZED VIEW

  - 默认 print-only，不进入缺失修补；若显式启用检查且缺失进入修补，则按“其他对象”路径生成。
  - 清洗规则走 GENERAL 规则集（移除 editionable/hints/Oracle 专用语法等）。
  - 输出目录：fixup_scripts/materialized_view/。

  PROCEDURE / FUNCTION / PACKAGE / PACKAGE BODY / TYPE / TYPE BODY

  - DDL 抽取：dbcat；必要时 DBMS_METADATA 兜底。
  - 重映射：adjust_ddl_for_object 改写主对象名与依赖对象引用；PL/SQL 体内进一步 remap 引用（屏蔽字符串/注释）。
  - 清洗规则（PLSQL 规则集）：
      - 清理 END; 后多余分号、移除单独分号行、去 PRAGMA、去 hints、去 BFILE/XMLTYPE 语法、清理多余分号/点号/空行/行尾空白。
      - 全角标点清洗（默认开启，屏蔽字符串/注释/双引号标识符）。
  - 输出目录：fixup_scripts/procedure、function、package、package_body、type、type_body。

  SYNONYM

  - DDL 抽取：优先使用 DBA_SYNONYMS 元数据拼 DDL；未命中再用 DBMS_METADATA。
  - 目标重写：FOR 子句指向 remap 后对象；PUBLIC 同义词名称归一化为无 schema 前缀。
  - 过滤：若目标对象不在 source_schemas 且无显式 remap，跳过生成。
  - 输出目录：fixup_scripts/synonym/。

  INDEX

  - 优先从 TABLE DDL 中提取 CREATE INDEX 语句；提取失败则用元数据重建（列序列 + UNIQUE）。
  - remap 表名与 schema 后写入 fixup_scripts/index/。

  CONSTRAINT

  - 优先从 TABLE DDL 中提取 ALTER TABLE ... ADD CONSTRAINT；失败时用元数据重建 PK/UK/FK。
  - FK 生成时自动 remap REFERENCES 的表名；跨 schema 自动补 REFERENCES 授权。
  - 分区表 PK 未包含分区键时，生成 UNIQUE 并注明降级原因。
  - 输出目录：fixup_scripts/constraint/。

  TRIGGER

  - DDL 抽取：dbcat 或 DBMS_METADATA。
  - remap：强制重写触发器名与 ON 子句中的表名；PL/SQL 体内对象引用重写；避免遗漏跨 schema 表引用。
  - 清洗：PLSQL 规则集 + 全角标点清洗；必要时加入 EXECUTE/SELECT 授权。
  - 触发器清单过滤：若 trigger_list 有效，仅生成清单内缺失触发器；空或不可读自动回退全量。
  - 输出目录：fixup_scripts/trigger/。

  JOB / SCHEDULE

  - 走“其他对象”流程：dbcat → remap → GENERAL 规则清洗 → 写入对应目录。
  - 输出目录：fixup_scripts/job/、fixup_scripts/schedule/。

  COMPILE（依赖重编译）

  - 依据依赖缺失报告，对仍存在的对象生成 ALTER ... COMPILE。
  - 对 PACKAGE/PACKAGE BODY 同时生成 ALTER PACKAGE ... COMPILE 与 COMPILE BODY。
  - 输出目录：fixup_scripts/compile/。

  GRANTS

  - 由 Oracle 权限元数据 + 依赖推导生成目标端授权计划。
  - 支持权限过滤（系统/对象权限白名单），并输出 filtered_grants.txt。
  - 支持合并语句：同对象多权限、同权限多 grantee 合并以减少脚本量。
  - 对缺失用户/角色授权做跳过并报告；生成角色 DDL（可排除 ORACLE_MAINTAINED）。
  - 输出目录：fixup_scripts/grants_all/ 与 fixup_scripts/grants_miss/。

  DDL 清洗规则字典（分组说明）

  - PLSQL 组（PROCEDURE/FUNCTION/PACKAGE/TYPE/TRIGGER）：清 END; 结尾、移除 PRAGMA、删除 Oracle hints、删除 BFILE/XMLTYPE 语法、清多余分号/点号/空行，必要时清除 ;/ 组合。
  - TABLE 组：移除 STORAGE/TABLESPACE、删除 hints、删除 Oracle 专有语法。
  - SEQUENCE 组：移除 NOKEEP/NOSCALE/GLOBAL，删除 hints。
  - GENERAL 组（VIEW/MVIEW/SYNONYM）：移除 EDITIONABLE/NONEDITIONABLE、删除部分 hints、删除 Oracle 专有语法。
  - 通用：normalize_ddl_for_ob 移除 USING INDEX...ENABLE/DISABLE 与 MVIEW 的 ON DEMAND；cleanup_dbcat_wrappers 去 DELIMITER/$$；strip_enable_novalidate 移除 ENABLE NOVALIDATE。

  ———

  执行器（run_fixup.py）设计要点

  - 执行层级与依赖排序：--smart-order 按固定层级执行，GRANT 在依赖对象之前。
  - 执行单位：逐语句执行，失败不阻断同文件后续语句；成功脚本移入 fixup_scripts/done/<dir>/。
  - 授权脚本特殊处理：成功 GRANT 语句从原文件移除，失败保留；生成 fixup_scripts/errors/ 错误报告（上限 200 条）。
  - 迭代模式 --iterative：分轮重试失败脚本，尤其针对 VIEW 依赖问题；可自动识别缺失对象并优先执行依赖脚本。
  - view-chain 自动修复 --view-chain-autofix：读取 VIEWs_chain_*.txt 生成 per-view plan 与 SQL，支持 done 目录兜底。
  - 自动重编译 --recompile：多轮执行 ALTER ... COMPILE 直至 INVALID 消失或达到重试上限。

  ———

  报告与审计输出

  - 主报告：main_reports/report_<timestamp>.txt（Rich 快照）
  - PACKAGE 明细：main_reports/package_compare_<timestamp>.txt
  - 缺失 TABLE/VIEW 规则，供OMS消费：main_reports/tables_views_miss/
  - 黑名单表：main_reports/blacklist_tables.txt
  - 依赖链：main_reports/dependency_chains_<timestamp>.txt、main_reports/VIEWs_chain_<timestamp>.txt
  - 授权过滤清单，即OB不支持的部分：main_reports/filtered_grants.txt
  - DDL 清洗报告：main_reports/ddl_punct_clean_<timestamp>.txt
  - fixup 输出：fixup_scripts/<object_type>/，含 grants_all/grants_miss/compile/table_alter 等子目录。

  ———

  生产投产边界与人工审计要求

  - 本程序仅做结构级校验与修补，不做数据一致性验证、性能评估或业务逻辑验证。
  - TABLE 类型仅校验列名集合、VARCHAR 长度区间与 LONG 类型转换；不做通用数据类型/精度/默认值的全面一致性校验。
  - 依赖关系与授权依赖于 DBA_* 权限与 remap 完整性；若 remap_conflicts 存在，则对应对象被跳过，需要显式修正后重跑。
  - MATERIALIZED VIEW 默认仅打印不校验；若迁移需物化视图一致性，需显式放开并人工评估。
  - fixup 脚本必须人工审核后执行，尤其是 table_alter 中的 DROP 建议与约束降级场景。
