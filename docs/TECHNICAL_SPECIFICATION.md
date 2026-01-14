# Oracle → OceanBase 结构一致性校验与修复引擎
## 技术规格说明 (Technical Specification)

**版本**：0.9.7  
**日期**：2026-01-13  
**适用场景**：Oracle → OceanBase（Oracle 模式）迁移后的结构一致性校验、对象补全、DDL 兼容性修复。

---

## 1. 系统定位
本工具是迁移后的结构一致性审计与修复脚本生成引擎。核心理念是 **“Dump-Once, Compare-Locally + Script-First”**：
- 所有元数据一次性拉取到本地内存进行比对
- 修补方案输出为可审计 SQL，不自动执行
- 依赖、授权、DDL 清洗形成闭环

## 2. 输入与控制面

### 2.1 配置文件
- `config.ini`：连接信息、超时、输出目录、功能开关
- `remap_rules.txt`：显式 remap 规则
- `trigger_list`：触发器过滤清单（可选）

### 2.2 外部依赖
- Oracle Instant Client + `oracledb` Thick Mode
- OceanBase `obclient`
- JDK + `dbcat`

### 2.3 可配置开关（核心）
- `check_primary_types` / `check_extra_types`
- `generate_fixup` / `generate_grants`
- `check_dependencies` / `check_comments`
- `infer_schema_mapping` / `ddl_punct_sanitize` / `ddl_hint_policy`

---

## 3. 元数据采集

### 3.1 Oracle 侧
- 通过 Thick Mode 批量读取 `DBA_*`：对象、列、索引、约束、触发器、序列、依赖、权限、注释。
- IN 列表分批执行，规避 1000 项限制。

### 3.2 OceanBase 侧
- 通过 `obclient` 一次性读取 `DBA_*` 视图。
- 结果缓存至 `ObMetadata`，避免循环访问。

---

## 4. 映射与推导

### 4.1 Remap 规则优先级
1) 显式 remap 规则
2) 默认保持原 schema 的类型
3) 依附对象跟随父表
4) 依赖推导
5) schema 映射回退

### 4.2 默认保持原 schema 的类型
- VIEW / MATERIALIZED VIEW / TRIGGER / PACKAGE / PACKAGE BODY

### 4.3 依附对象
- INDEX / CONSTRAINT / SEQUENCE / SYNONYM
- 跟随父表 schema（只改 schema，不改对象名）

### 4.4 依赖推导
- PROCEDURE / FUNCTION / TYPE / SYNONYM
- 基于依赖目标 schema 频次统计推导

---

## 5. 对比规则

### 5.1 TABLE
- 列集合对比（忽略 OMS_* 和隐藏列）
- VARCHAR/VARCHAR2 长度窗口校验
- LONG/LONG RAW 自动映射为 CLOB/BLOB

### 5.2 VIEW / PLSQL / TYPE / SYNONYM / JOB / SCHEDULE
- 存在性校验

### 5.3 PACKAGE / PACKAGE BODY
- 有效性校验（`DBA_ERRORS` 摘要）

### 5.4 INDEX
- 按列序列 + 唯一性匹配
- 兼容 SYS_NC 列名差异

### 5.5 CONSTRAINT
- PK/UK/FK 按列序列匹配
- 忽略 `_OBNOTNULL_` 约束

### 5.6 SEQUENCE
- 按 schema 映射比较集合

### 5.7 TRIGGER
- 目标存在性 + 触发事件与状态

---

## 6. 依赖与授权

### 6.1 依赖校验
- `DBA_DEPENDENCIES` 构建期望依赖集合
- 与 OB 实际依赖比对，输出缺失/多余依赖

### 6.2 授权生成
- 基于 `DBA_TAB_PRIVS`、`DBA_SYS_PRIVS`、`DBA_ROLE_PRIVS`
- 支持权限合并与白名单过滤
- 输出 `grants_miss/` 与 `grants_all/`

---

## 7. 修补脚本生成

### 7.1 DDL 获取
- dbcat（批量导出）
- DBMS_METADATA（VIEW 兜底）

### 7.2 DDL 清洗与兼容
- Hint 策略过滤
- PL/SQL 结尾修正
- Oracle 特有语法清理
- VIEW 行内注释修复

### 7.3 输出目录
- `fixup_scripts/table/` / `table_alter/`
- `fixup_scripts/view/`
- `fixup_scripts/compile/`
- `fixup_scripts/grants_miss/`

---

## 8. run_fixup 执行语义

### 8.1 Smart Order
按依赖层级执行（sequence → table → grants → view → code → index/constraint → trigger）。

### 8.2 Iterative 模式
失败脚本自动重试，直至收敛或达到最大轮次。

### 8.3 VIEW 链路自动修复
依据 `VIEWs_chain_*.txt` 生成计划并执行，每个 VIEW 独立输出 plan/sql。

### 8.4 错误报告
失败语句汇总到 `fixup_scripts/errors/`，便于集中排查。

---

## 9. 报告体系
- `report_*.txt`：主报告
- `package_compare_*.txt`：包对比明细
- `remap_conflicts_*.txt`：推导冲突
- `VIEWs_chain_*.txt`：VIEW 链路
- `filtered_grants.txt`：过滤权限

---

## 10. 性能与可靠性
- 大部分逻辑在内存执行，避免高频 DB 访问。
- 可配置超时与并发线程数。
- dbcat 输出缓存复用，减少重复扫描。

---

## 11. 安全与审计
- 主程序只执行 SELECT。
- 修补脚本需人工审核执行。
- 输出包含完整变更线索与摘要。

---

## 12. 已知限制
- 对极端复杂 DDL（如 `q'[...]'` 字符串）需人工复核清洗结果。
- init_users_roles 默认密码需后续改密。
