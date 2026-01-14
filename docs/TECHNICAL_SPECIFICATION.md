# Oracle to OceanBase 结构一致性校验与修复引擎
## 技术设计与实现原理白皮书 (Technical Design Specification)

**版本**: 1.0.0
**日期**: 2026-01-13
**适用场景**: 异构数据库迁移（Oracle -> OceanBase）后的结构一致性校验、对象补全、DDL 语法转换与依赖修复。

---

### 系统定位
本程序是 Oracle → OceanBase（Oracle 模式）迁移后的“结构一致性校验 + 修补脚本生成”引擎，设计核心是“Dump-Once, Compare-Locally + 人工审计前置”。它不自动执行任何 DDL，而是将修补方案落为脚本，强制引入人工审核，保证生产安全。

### 全流程概述
1. **配置自检**：读取配置并进行运行前自检（路径、依赖、权限、目录可写性）。
2. **元数据快照 (Oracle)**：连接 Oracle（Thick Mode）批量拉取元数据与权限、依赖。
3. **元数据快照 (OceanBase)**：通过 obclient 批量拉取 OceanBase 元数据与依赖。
4. **规则解析**：解析 remap 规则并做规则有效性校验。
5. **对象映射构建**：构建完整对象映射与主检查清单（master_list），对 one-to-many/冲突场景做显式回退或报冲突。
6. **结构校验**：执行主对象检查（TABLE/VIEW/PLSQL/TYPE/JOB/SCHEDULE/SYNONYM 等）与扩展对象检查（INDEX/CONSTRAINT/SEQUENCE/TRIGGER）。
7. **高级校验**：可选执行注释一致性校验、依赖关系校验与依赖链导出。
8. **脚本生成**：可选生成修补脚本、授权脚本、重编译脚本，产出多维度报告与快照。
9. **执行修复**：由 `run_fixup.py` 执行脚本（支持依赖排序、迭代重试、view-chain 自动修复与重编译）。

### 配置与输入约束（核心控制面）
- **连接与运行参数**：`config.ini.template` 中的 `[ORACLE_SOURCE]`、`[OCEANBASE_TARGET]`、`[SETTINGS]`，含超时设置、目录路径、并发控制等。
- **范围控制**：`check_primary_types`、`check_extra_types`、`check_dependencies`、`generate_fixup` 等开关。
- **映射控制**：`remap_rules.txt`（可选），未配置时默认 1:1 映射。
- **额外输入**：
    - `OMS_USER.TMP_BLACK_TABLE`：黑名单过滤。
    - `trigger_list`：触发器筛选。
    - `dbcat_output`：DDL 缓存。

### 元数据采集机制

#### Oracle 侧 (`schema_diff_reconciler.py`)
- 通过 `oracledb` Thick Mode 查询：
    - `DBA_OBJECTS`, `DBA_TAB_COLUMNS` (含 `CHAR_USED`, `HIDDEN_COLUMN`)
    - `DBA_INDEXES`/`DBA_IND_COLUMNS`
    - `DBA_CONSTRAINTS`/`DBA_CONS_COLUMNS`
    - `DBA_TRIGGERS`, `DBA_SEQUENCES`, `DBA_DEPENDENCIES`
    - `DBA_TAB_COMMENTS`/`DBA_COL_COMMENTS`
    - `DBA_ERRORS`
- **同义词优化**：从 `DBA_SYNONYMS` 批量读取并缓存元数据，避免逐个 `DBMS_METADATA` 调用。
- **去噪**：跳过 `SYS_IOT_OVER_*` IOT 表；对 MVIEW 进行 TABLE/MVIEW 去重。

#### OceanBase 侧
- 使用 `obclient` 批量查询对应 DBA 视图。
- **类型补偿**：`DBA_TYPES` 补齐 TYPE；启用 TYPE BODY 检查时通过 `DBA_SOURCE` 探测。
- **去噪**：忽略 OB 自动生成的 `*_OBNOTNULL_*` 约束；忽略 OMS 自动索引。

#### 对象映射与 Remap 推导
- **规则解析**：`remap_rules.txt` 逐行解析，非法规则自动剔除。
- **清单构建**：`master_list` (主对象) 与 `full_object_mapping` (全量映射)。
- **冲突处理**：one-to-many 场景下，当前对象回退为 1:1；PACKAGE/TYPE 与 BODY 强制同目标。
- **Schema 推导策略**：
    - 基于 TABLE 映射推导 schema mapping。
    - 独立对象：基于依赖链统计目标 schema 出现频次进行推导。
    - 依附对象 (INDEX/CONSTRAINT/SEQUENCE)：跟随父表 schema。
    - TRIGGER：默认不跟随，仅显式 remap 生效。

---

## 检查阶段总体规则

- **检查范围**：完全由配置开关控制；未启用的类型不会被加载、对比或生成修复脚本。
- **主对象输出**：missing / mismatched / ok / skipped / extra_targets / extraneous。
- **扩展对象输出**：索引、约束、序列、触发器分别记录 ok 与 mismatched 详情。

### 主对象检查（逐类规则）

#### TABLE
- **存在性**：目标端 `DBA_OBJECTS` 中存在即通过。
- **列集合对比**：
    - 源端过滤：OMS 列、Oracle Hidden 列。
    - 目标端过滤：OMS 列。
    - 差异：Missing Column / Extra Column。
- **长度校验 (VARCHAR/VARCHAR2)**：
    - **BYTE 语义**：目标长度需满足 `ceil(src * 1.5)` 下限；超过 `ceil(src * 2.5)` 记为 oversize。
    - **CHAR 语义**：目标长度必须完全一致。
- **类型校验**：
    - Oracle `LONG` -> OB `CLOB`
    - Oracle `LONG RAW` -> OB `BLOB`

#### VIEW / PROCEDURE / FUNCTION / TYPE / SYNONYM
- **校验规则**：仅检查存在性 (目标端存在即 OK)。
- **MVIEW**：默认仅打印不校验。
- **TYPE BODY**：基于 `DBA_SOURCE` 准确判定。

#### PACKAGE / PACKAGE BODY
- **校验规则**：不走主对象检查，单独走有效性对比。
- **状态分类**：
    - `SOURCE_INVALID`：源端无效（记录详情，不计入不匹配）。
    - `MISSING_TARGET`：目标端缺失。
    - `TARGET_INVALID`：目标端无效（读取 `DBA_ERRORS`）。
    - `STATUS_MISMATCH`：状态不一致。

### 扩展对象检查

#### INDEX
- **对比方式**：忽略名称，按 **"列顺序序列 + 唯一性"** 指纹匹配。
- **过滤规则**：
    1. 剔除 OMS 自动索引。
    2. 若 PK/UK 约束已覆盖列集，索引缺失不重复报告。
    3. 兼容 `SYS_NC` 列名差异。
- **唯一性**：源 NONUNIQUE -> 目标 UNIQUE 且被约束支撑时视为正常。

#### CONSTRAINT (PK/UK/FK)
- **对比方式**：按列序列匹配。
- **特殊处理**：
    - 忽略 `_OMS_ROWID` 约束。
    - **分区键降级**：源端 PK 未包含分区键时，允许目标端用 PK 或 UK 匹配。
    - **FK 校验**：校验被引用表是否经 remap 后一致。
    - 忽略 OB 自动 `*_OBNOTNULL_*` 约束。

#### SEQUENCE
- **对比方式**：以 schema 映射为单位比较序列名集合。
- **空 Schema 处理**：若 Oracle schema 实际上不存在，跳过比较。

#### TRIGGER
- **对比方式**：源 -> 目标 remap 后的全名集合比对。
- **属性校验**：同名存在时，额外校验 **触发事件** 与 **状态 (STATUS)**。

### 其他检查
- **注释一致性**：表注释、列注释（过滤 OMS/Hidden 列）。
- **依赖关系**：Oracle `DBA_DEPENDENCIES` remap 后与 OB 对比；缺失依赖生成行动建议。
- **黑名单表**：`OMS_USER.TMP_BLACK_TABLE` 输出清单，不进入缺失规则。

---

## 修补脚本生成（总体逻辑）

- **入口**：`generate_fixup=true`。
- **策略**：
    - 安全清理旧脚本。
    - DDL 来源：`dbcat` (缓存优先) -> `DBMS_METADATA` (兜底)。
    - 同义词：优先元数据拼接。
- **顺序**：SEQUENCE -> TABLE CREATE -> TABLE ALTER -> VIEW/其他 -> INDEX -> CONSTRAINT -> TRIGGER -> COMPILE -> GRANTS。

### Fixup 对象级逻辑

#### SEQUENCE
- **清洗**：移除 `NOKEEP`, `NOSCALE`, `GLOBAL`，去除 hints。
- **输出**：`fixup_scripts/sequence/`

#### TABLE (CREATE)
- **抽取**：`dbcat`。
- **优化**：自动增宽 BYTE 语义 VARCHAR；去除 storage/tablespace 等 Oracle 专有语法。
- **输出**：`fixup_scripts/table/`

#### TABLE (ALTER)
- **缺失列**：生成 `ALTER TABLE ... ADD`；自动补注释。
- **长度差异**：生成 `MODIFY`（放大或等长）。
- **类型差异**：生成 `MODIFY` (LONG -> CLOB/BLOB)。
- **输出**：`fixup_scripts/table_alter/`

#### VIEW
- **抽取**：固定 `DBMS_METADATA`。
- **版本感知**：OB 版本 < 4.2.5.7 时移除 `WITH CHECK OPTION`。
- **清洗**：移除 `EDITIONABLE`, `BEQUEATH`, `SHARING` 等。
- **修复**：行内注释吞行修复、列名拆分修复。
- **依赖重写**：解析 SQL 体重写对象引用（Schema Remap）。
- **拓扑排序**：基于依赖关系排序生成。
- **输出**：`fixup_scripts/view/`

#### PL/SQL 对象 (PROC/FUNC/PKG/TYPE)
- **抽取**：`dbcat` (优先) / `DBMS_METADATA`。
- **清洗**：全角标点清洗、移除 PRAGMA、清理 END 结尾符号。
- **重映射**：SQL 体内对象引用重写。
- **输出**：对应子目录。

#### SYNONYM
- **抽取**：元数据拼接。
- **重写**：`FOR` 子句指向 remap 后对象。
- **输出**：`fixup_scripts/synonym/`

#### INDEX & CONSTRAINT
- **抽取**：优先从 TABLE DDL 提取；失败时用元数据重建。
- **FK 处理**：自动 remap 引用表；跨 schema 补授权。
- **输出**：`fixup_scripts/index/`, `fixup_scripts/constraint/`

#### TRIGGER
- **重写**：强制重写名称与 `ON` 子句表名；SQL 体重写。
- **过滤**：支持 `trigger_list` 白名单。
- **输出**：`fixup_scripts/trigger/`

#### COMPILE & GRANTS
- **COMPILE**：生成 `ALTER ... COMPILE` 语句。
- **GRANTS**：基于权限元数据 + 依赖推导生成授权；支持合并语句；输出 `filtered_grants.txt`。

---

## 生产级安全与审计

- **Read-Only**：程序仅执行 `SELECT`，不执行任何修改操作。
- **脚本隔离**：产物为 SQL 文件，按类型隔离，强制人工审核。
- **报告输出**：
    - 主报告：`report_<timestamp>.txt`
    - 缺失规则：`tables_views_miss/`
    - 黑名单：`blacklist_tables.txt`
    - 依赖链：`dependency_chains_*.txt`, `VIEWs_chain_*.txt`
    - 过滤授权：`filtered_grants.txt`
    - 清洗报告：`ddl_punct_clean_*.txt`

**结论**：本程序通过内存模型解决性能问题，通过拓扑排序与语法清洗解决兼容性问题，通过权限推导解决连通性问题，并配合严格的文件生成策略，符合生产环境投产标准。