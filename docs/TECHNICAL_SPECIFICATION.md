# Oracle to OceanBase 结构一致性校验与修复引擎
## 技术设计与实现原理白皮书 (Technical Design Specification)

**版本**: 1.0.0
**日期**: 2026-01-13
**适用场景**: 异构数据库迁移（Oracle -> OceanBase）后的结构一致性校验、对象补全、DDL 语法转换与依赖修复。

---

## 1. 核心设计哲学 (Core Philosophy)

本程序的设计并未止步于简单的“名称比对”，而是致力于解决异构数据库迁移中**“最后一公里”**的复杂性问题。其核心架构基于以下三大支柱：

1.  **内存驻留型比对架构 (Dump-Once, Compare-Locally)**
    *   **设计背景**：在包含数万对象的生产库中，传统的“逐个对象查询”模式会导致严重的网络 I/O 延迟。
    *   **实现机制**：程序启动时，通过高并发 Fetch 仅对 Oracle 和 OceanBase 进行一次全量元数据快照，构建内存中的对象树（Object Tree）。
    *   **价值**：将 $O(N \times Network)$ 的复杂度降低为 $O(1 \times Network)$，后续比对逻辑均在内存中毫秒级完成。

2.  **生产安全优先 (Production Safety First)**
    *   **Read-Only 铁律**：工具**绝不自动执行**任何 DDL 语句。
    *   **脚本化产出**：所有的修复动作（Fixup）均生成为标准 SQL 脚本文件，按对象类型物理隔离，强制引入人工审计环节。

3.  **智能调和引擎 (Intelligent Reconciliation)**
    *   **深度清洗**：内置 Oracle-to-OB 语法清洗器（正则流水线）。
    *   **依赖编排**：内置视图依赖拓扑排序算法。
    *   **环境感知**：根据目标端 OB 版本动态调整 DDL 语法策略。

---

## 2. 检查阶段：深度差异分析逻辑 (Inspection Rules)

### 2.1 主对象：表 (TABLE)
*   **列集一致性 (Column Set)**：
    *   **OMS 降噪**：自动剔除 OceanBase 迁移工具生成的隐藏列（`OMS_OBJECT_NUMBER`, `OMS_ROW_NUMBER` 等），防止误报“多余列”。
    *   **Oracle 隐藏列**：自动识别并忽略 Oracle 端 `HIDDEN_COLUMN='YES'` 的列。
*   **字符类型扩容窗口 (VARCHAR Expansion)**：
    *   **背景**：Oracle (GBK) -> OB (UTF8) 需要扩容。
    *   **判定公式**：目标端长度 $L_{tgt}$ 需满足 $[ \lceil L_{src} \times 1.5 \rceil, \lceil L_{src} \times 2.5 \rceil ]$。
    *   **语义感知**：精确区分 `BYTE` 与 `CHAR` 语义。若源端为 `CHAR` 语义，则强制要求目标端长度数值完全一致。
*   **LOB 类型兼容**：
    *   Oracle `LONG` -> OceanBase `CLOB`
    *   Oracle `LONG RAW` -> OceanBase `BLOB`

### 2.2 索引 (INDEX) —— 指纹比对
*   **去名化比对 (Name-Agnostic)**：忽略索引名称差异，基于 **"唯一性 + 排好序的列名列表"** 生成指纹进行比对。
*   **冗余降噪**：自动忽略以 `_OMS_ROWID` 结尾的内部索引。
*   **约束关联降噪**：如果一个唯一索引是为了支撑主键/唯一约束而存在，且该约束已报告缺失，则在索引检查报告中**自动隐去**该索引的缺失记录，避免重复报警。

### 2.3 约束 (CONSTRAINT) —— 智能降级容忍
*   **PK/UK 降级识别 (Downgrade Tolerance)**：
    *   **场景**：源端有 PK 约束，目标端只有对应的 Unique Index 但无 PK 约束对象。
    *   **判定**：程序报告“缺失约束”，但在索引报告中视为“索引一致”。引导 DBA 补全约束定义，而非重建索引。
*   **OB 自动约束过滤**：自动忽略 `OBNOTNULL` 检查约束。
*   **FK 跨 Schema 感知**：比对 FK 时，不仅比对列，还比对**被引用表 (Referenced Table)** 是否经过了正确的 Remap 映射。

### 2.4 代码对象与视图
*   **有效性穿透**：不仅检查对象存在性，还检查 `STATUS`。若目标端为 `INVALID`，程序会自动提取 `DBA_ERRORS` 中的具体错误信息（如 Line X 报错）展示在报告中。
*   **视图依赖**：仅比对存在性。但若主检查未开启视图，而后续依赖分析发现视图缺失，会自动触发补全逻辑。

---

## 3. 修复阶段：智能脚本生成引擎 (Reconciliation Engine)

### 3.1 DDL 获取的双通道机制
*   **dbcat 通道**：用于 `TABLE`, `SEQUENCE`, `INDEX`, `CONSTRAINT`。利用其成熟的类型映射能力。
*   **DBMS_METADATA 通道**：用于 `VIEW`, `PL/SQL`, `TRIGGER`。获取原汁原味的逻辑定义，再进行清洗。

### 3.2 视图专用：版本感知与拓扑排序
1.  **版本感知降级 (Version-Aware Downgrade)**：
    *   **探测**：启动时查询 `SELECT OB_VERSION() FROM DUAL`。
    *   **逻辑**：若目标版本 **< 4.2.5.7**，强制剥离 `WITH CHECK OPTION` 子句，规避内核兼容性 Bug（保可用，弃约束）。
2.  **拓扑排序 (Topological Sort)**：
    *   **逻辑**：解析缺失视图的 `FROM/JOIN` 依赖关系，构建 DAG（有向无环图），使用 Kahn 算法排序。
    *   **产出**：生成的脚本严格遵循“先创建被依赖对象”的顺序，极大降低编译报错率。

### 3.3 触发器与同义词：特殊处理逻辑
1.  **触发器 (TRIGGER)**：
    *   **宿主重定向**：正则重写 `ON schema.table` 子句，强制指向 Remap 后的目标表。
    *   **内容清洗**：对 PL/SQL 体内的对象引用进行上下文感知的 Remap 替换（配合 `SqlMasker` 保护字符串常量）。
2.  **同义词 (SYNONYM)**：
    *   **内存合成**：不调用昂贵的 `GET_DDL`。直接读取 `DBA_SYNONYMS` 元数据，在内存中拼接 `CREATE SYNONYM` 语句，性能提升百倍。
    *   **智能过滤**：自动保留 `@DBLINK` 后缀；自动剔除指向系统对象（如 `SYS`）的 PUBLIC 同义词。

### 3.4 自动授权推导 (Grant Inference)
*   **逻辑**：如果生成的对象（如 View/FK）引用了另一个 Schema 的对象，程序自动检查 OB 权限表。
*   **补全**：若权限缺失，自动生成 `GRANT SELECT/REFERENCES ON target TO owner` 语句，放入 `grants_miss` 目录。

---

## 4. DDL 转换与清洗规则字典 (Sanitization Dictionary)

本章节列出了程序内置的所有硬编码清洗规则。

### 4.1 通用清洗规则
*   **Wrapper Stripping**：移除 `dbcat` 的 `DELIMITER` 和 `$$` 标记。
*   **Schema Injection**：头部强制插入 `ALTER SESSION SET CURRENT_SCHEMA = target;`。
*   **Storage Removal**：剔除 `STORAGE`, `TABLESPACE`, `PCTFREE`, `INITRANS` 等物理属性。
*   **Constraint State**：将 `ENABLE NOVALIDATE`（OB 不支持）降级为默认开启；将 `ENABLE VALIDATE` 标准化为 `VALIDATE`。

### 4.2 Oracle 专有语法剥离
*   剔除 `EDITIONABLE` / `NONEDITIONABLE`。
*   剔除 `BEQUEATH CURRENT_USER` / `DEFINER`。
*   剔除 `SHARING = ...`。
*   剔除 `DEFAULT COLLATION ...`。
*   剔除 `CONTAINER_MAP`。

### 4.3 PL/SQL 深度清洗
*   **全角转半角**：自动扫描非字符串区域，将全角标点（`；` `（` `）` `，`）转换为半角，防止编译失败。
*   **Pragma 清理**：移除 `PRAGMA AUTONOMOUS_TRANSACTION`（OB 暂不支持）。
*   **结束符修正**：修正 `END;` 后多余的分号，确保符合 OB 的 `/` 结束符规范。

### 4.4 特定对象规则
*   **TABLE**：VARCHAR2 长度不足时，强制修改 DDL 中的长度定义为 $\lceil src \times 1.5 \rceil$。
*   **SEQUENCE**：剔除 `NOKEEP`, `NOSCALE`, `GLOBAL` 参数。
*   **INDEX**：若 `dbcat` 导出失败，支持基于元数据（`DBA_IND_COLUMNS`）手动拼接 `CREATE INDEX` 语句作为兜底。

---

## 5. 生产级安全与健壮性设计

### 5.1 安全机制
1.  **只读操作**：全组件无写权限，仅执行 `SELECT`。
2.  **文件隔离**：Fixup 脚本按对象类型分目录存储，便于分批执行。
3.  **头部警示**：每个脚本文件包含自动生成的注释，标明源对象来源、生成时间及逻辑依据。

### 5.2 健壮性与容错
1.  **超时熔断**：`obclient` 和 `dbcat` 调用均受 `subprocess` 级超时控制。
2.  **并发控制**：内置线程池，支持 `worker` 数量配置。
3.  **优雅降级**：
    *   `dbcat` 失败 -> 自动尝试 `DBMS_METADATA` 兜底。
    *   触发器清单损坏 -> 自动降级为全量处理并告警。

---

## 6. 总结

该程序是一个**高内聚、低耦合、具备编译器前端特性的数据库迁移辅助引擎**。

它通过**内存模型**解决了性能问题，通过**拓扑排序**解决了依赖问题，通过**语法清洗**解决了兼容性问题，通过**权限推导**解决了连通性问题。其对 PK/UK 降级、LOB 兼容、字符集扩容及视图版本兼容性的处理，均体现了对 Oracle 与 OceanBase 内核差异的深刻理解。

**建议结论**：架构设计严谨，逻辑闭环，风险可控，**符合生产投产标准**。
