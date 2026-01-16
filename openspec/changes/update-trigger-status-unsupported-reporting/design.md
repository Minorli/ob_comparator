# Design: 触发器状态对比与不支持对象分流

## Goals
- 输出触发器在 Oracle/OB 的 VALID/INVALID 与 ENABLED/DISABLED 差异。
- 将缺失对象按“可修补 / 不支持 / 被阻断”分层，避免误导。
- fixup 目录只保留可修补对象，不支持对象单独落盘供人工改造。
- 主报告保持简洁，高层视角明确下一步动作。

## Non-Goals
- 不自动修复触发器状态（仅识别并报告）。
- 不重写复杂 SQL 语义，仅做规则化清洗与标记。

## 1) 触发器状态对比
- **Oracle 元数据**
  - DBA_TRIGGERS: STATUS (ENABLED/DISABLED)。
  - DBA_OBJECTS: STATUS (VALID/INVALID) 作为有效性来源。
- **OceanBase 元数据**
  - DBA_TRIGGERS: STATUS (如存在)；
  - DBA_OBJECTS: STATUS (VALID/INVALID)。
- **状态模型**
  - TriggerState = {enabled: ENABLED/DISABLED/UNKNOWN, valid: VALID/INVALID/UNKNOWN}
  - 通过 OWNER/TRIGGER_NAME 关联，缺失字段降级为 UNKNOWN。
- **差异判定**
  - 事件(EVENT)差异、enabled 差异、valid 差异分别记录到 detail。
- **报告文件改名**
  - `trigger_miss.txt` → `trigger_status_report.txt`
  - 内容包含：清单校验、缺失触发器、状态不一致触发器、与清单不匹配统计。

## 2) 不支持对象分类与传播
### 2.1 分类定义
- **SUPPORTED**：可自动 fixup。
- **UNSUPPORTED**：OB 不支持或策略禁止迁移（需改造）。
- **BLOCKED**：对象本身可支持，但依赖 UNSUPPORTED 对象导致无法创建。

### 2.2 不支持来源
- **表级（UNSUPPORTED）**
  - 黑名单规则引擎（DIY/LONG/LOB_OVERSIZE/TEMP_TABLE/DBLINK 等）。
  - 临时表统一标记为 UNSUPPORTED（根据现有规则与 DBA_TABLES.TEMPORARY）。
- **视图级（UNSUPPORTED）**
  - 规则识别（详见 §3）。
- **依赖传播（BLOCKED）**
  - 使用 DBA_DEPENDENCIES/依赖图递归标记：
    - 视图/同义词/触发器/过程/函数/包依赖 UNSUPPORTED 表或视图 → BLOCKED。

### 2.3 分类输出
- 生成 `unsupported_objects_detail_<ts>.txt`，字段以 `|` 分隔：
  - OBJ_TYPE | SRC_FULL | TGT_FULL | SUPPORT_STATE | REASON_CODE | REASON_TEXT | DEPENDENCY | ACTION | DETAIL

## 3) VIEW 兼容性规则
- **不可支持（UNSUPPORTED）**
  - DDL 中包含 `SYS.OBJ$`（OB 不支持）。
  - DDL 中包含 DBLINK (`@`)（策略禁止）。
  - 引用 OB 不存在的系统视图（如 `DBA_DATA_FILES`, `ALL_RULES` 等）。
- **可清洗（SUPPORTED with rewrite）**
  - `DBA_USERS.USER_ID` → `DBA_USERS.USERID`（仅限非字符串/注释区域）。
- **需要权限（BLOCKED or NEEDS_PRIVILEGE）**
  - 引用存在但可能缺权限的 DBA 视图（如 `DBA_OBJECTS`/`DBA_SOURCE`/`DBA_JOBS`）。
  - 记录为“需要授权/改造”而不是直接 fixup。
- **实现要点**
  - 使用 SqlMasker 避免在字符串/注释内误替换。
  - 可扩展规则表（可选 JSON 规则文件）。

## 4) fixup 目录分流
- **可支持对象（可修补）**
  - 继续写入现有 fixup 目录（如 `fixup_scripts/table`, `fixup_scripts/view`）。
- **不支持对象**
  - 新目录 `fixup_scripts/tables_unsupported/` 存放不支持表 DDL。
  - 视图/同义词/触发器/PLSQL 进入 `fixup_scripts/unsupported/<type>/`。
- **临时表**
  - 独立子目录，例如 `fixup_scripts/tables_unsupported/temporary/`。
- **fixup 执行策略**
  - 仅对 SUPPORTED 对象生成 fixup 脚本；UNSUPPORTED/BLOCKED 仅落盘，不参与执行。

## 5) 报告拆分与摘要强化
- **主报告**：保留总览、汇总统计、下一步建议（简洁）。
- **明细报告**：按类型拆分，例如：
  - missing_objects_<type>.txt
  - unsupported_objects_detail_<ts>.txt
  - trigger_status_report.txt
- **汇总表**：新增列/分组，展示：missing_total, missing_unsupported, missing_blocked, missing_supported。

## 6) 配置建议（新增）
- `report_detail_mode = split|full|summary`（默认 split）。
- `view_compat_rules_path`（可选，JSON 规则扩展）。
- `view_dblink_policy = block|allow`（默认 block）。

## 7) 测试策略（Oracle 19c / OB 4.2.5.7）
- **触发器状态**：构造 4 种组合 VALID/INVALID × ENABLED/DISABLED，分别在 Oracle/OB 验证元数据列。
- **视图规则**：
  - SYS.OBJ$ 依赖视图 → UNSUPPORTED
  - DBA_USERS.USER_ID → rewrite
  - DBA_DATA_FILES / ALL_RULES → UNSUPPORTED
  - DBLINK 视图 → UNSUPPORTED
  - 依赖黑名单表的视图/触发器/包 → BLOCKED
- **报告输出**：
  - 主报告分割验证
  - unsupported_objects_detail_<ts>.txt 与 trigger_status_report.txt 格式验证
