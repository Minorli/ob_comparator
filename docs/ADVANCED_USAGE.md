# Advanced Usage Guide

本手册聚焦两部分：
1) Remap 推导与对象归属规则  
2) `run_fixup.py` 的高级执行方式

---

## 一、Remap 推导与对象归属

### 1. 规则优先级（从高到低）

1) **显式 remap 规则**（`remap_rules.txt`）  
2) **不参与推导的类型**：保持原 schema  
3) **依附对象**：跟随父表的 remap 目标 schema  
4) **依赖推导**：根据引用对象的 remap 结果推导  
5) **schema 映射回退**：多对一/一对一场景下使用主流 schema

一旦出现推导冲突或无法推导，对象会被记录到 `remap_conflicts_*.txt` 中，并从本轮对比/修复中跳过。

### 2. 哪些对象会“保持原 schema”？

默认不参与推导（除非显式 remap）：
- VIEW
- MATERIALIZED VIEW
- TRIGGER
- PACKAGE / PACKAGE BODY

这意味着：
- **触发器不会自动改 schema**。如果触发器依赖的表被 remap 到其他 schema，会自动在 fixup 阶段补充授权。
- **视图不会自动改 schema**。但视图内部对表的引用仍会按 remap 规则替换。
- **MATERIALIZED VIEW / PACKAGE / PACKAGE BODY** 默认仅打印不校验（OB 不支持或默认跳过）。

### 3. 依附对象如何跟随父表？

以下对象会跟随父表的目标 schema：
- INDEX
- CONSTRAINT
- SEQUENCE（优先根据依赖对象推导）
- SYNONYM（优先跟随指向对象或依赖对象）

原则：**只变更 schema，不改对象名**。  
示例：`SRC_A.IDX_ORDERS` 的父表 remap 到 `OB_A.ORDERS` → 结果是 `OB_A.IDX_ORDERS`。

### 4. 依赖推导适用于哪些对象？

以下类型会通过 `DBA_DEPENDENCIES` 推导目标 schema：
- PROCEDURE / FUNCTION
- TYPE / TYPE BODY
- SYNONYM（非 PUBLIC）

依赖推导规则：
- 统计对象依赖的表/视图/序列等目标 schema  
- 若目标 schema 唯一 → 采用  
- 若多个 schema 且同等出现 → 认为冲突，写入 `remap_conflicts_*.txt`

### 5. 一对一 / 多对一 / 一对多场景示例

#### 多对一（Many-to-One）
```
SRC_A.T1 -> OB_A.T1
SRC_B.T2 -> OB_A.T2
```
- TABLE：显式 remap  
- VIEW / TRIGGER / PACKAGE：保持原 schema（除非显式 remap）  
- INDEX/CONSTRAINT/SEQUENCE：跟随父表 → 归入 OB_A  
- 依赖对象（PROC/TYPE 等）：依赖推导 → 多数会归入 OB_A

#### 一对一（One-to-One）
```
SRC_A.* -> OB_A.*
```
规则同上，只是推导更稳定。

#### 一对多（One-to-Many）
```
SRC_A.T1 -> OB_A.T1
SRC_A.T2 -> OB_B.T2
```
此时：
- VIEW / TRIGGER 等**仍保持 SRC_A schema**  
- 依赖推导可能冲突（引用多个 schema）  
→ 冲突对象会出现在 `remap_conflicts_*.txt`，需显式 remap

### 6. Remap 冲突如何处理？

输出位置：
- `main_reports/remap_conflicts_*.txt`
- 报告中 “无法自动推导” 章节

处理方式：
1) 在 `remap_rules.txt` 显式补齐  
2) 重新运行对比

**注意**：冲突对象不会自动回退到源 schema，避免误判。

### 7. 检查范围与类型控制

`check_primary_types` 和 `check_extra_types` 会影响：
- 元数据加载  
- Remap 推导  
- 对比与报告  
- fixup 生成范围

示例：仅检查表
```ini
check_primary_types = TABLE
check_extra_types =
```
此时 PACKAGE/VIEW 等不会参与推导或校验。

完整可选值见 `readme_config.txt` 与 `config.ini.template`。

### 8. 禁用推导（可选）

如需完全依赖显式 remap，可关闭推导：
```ini
infer_schema_mapping = false
```

---

## 二、run_fixup.py 高级用法

`run_fixup.py` 负责执行 `fixup_scripts/` 下的 SQL，并支持：
- 依赖感知排序（`--smart-order`）
- 自动重编译 INVALID 对象（`--recompile`）
- 目录/类型/文件名过滤

### 1. 推荐执行方式
```bash
python3 run_fixup.py --smart-order --recompile
```

### 2. 过滤执行

仅执行部分目录：
```bash
python3 run_fixup.py --only-dirs table,table_alter,grants
```

按对象类型过滤（自动映射到目录）：
```bash
python3 run_fixup.py --only-types TABLE,VIEW,PROCEDURE
```

按文件名过滤：
```bash
python3 run_fixup.py --glob "*SCHEMA_A*.sql"
```

排除目录：
```bash
python3 run_fixup.py --exclude-dirs trigger,job
```

### 3. 执行顺序

默认顺序（标准优先级）：
```
sequence -> table -> table_alter -> constraint -> index -> view -> ...
```

`--smart-order` 启用依赖感知排序：
```
Layer 0: sequence
Layer 1: table
Layer 2: table_alter
Layer 3: grants
Layer 4: view, synonym
Layer 5: materialized_view
Layer 6: procedure, function
Layer 7: package, type
Layer 8: package_body, type_body
Layer 9: constraint, index
Layer 10: trigger
Layer 11: job, schedule
```

### 4. 自动重编译

`--recompile` 会在执行完成后：
- 查询 `DBA_OBJECTS` 中 `INVALID` 对象  
- 尝试 `ALTER ... COMPILE`  
- 最多重试 `--max-retries` 次（默认 5）

示例：
```bash
python3 run_fixup.py --smart-order --recompile --max-retries 10
```

### 5. 幂等执行

`run_fixup.py` 具有幂等性：
- 成功的脚本会移动到 `fixup_scripts/done/`
- 再次执行时只处理失败项

---

## 三、常见问题速查

**Q1: VIEW/触发器为什么没跟随表的 remap？**  
A: 这类对象默认保持原 schema。若需要迁移，请显式 remap。

**Q2: 为什么提示“无法自动推导”？**  
A: 依赖指向多个 schema 或依赖缺失。请补充显式 remap。

**Q3: 我设置了 `check_primary_types=TABLE`，为什么没有推导 PACKAGE？**  
A: 检查范围被限制为 TABLE，其他类型不会加载也不会推导。

---

更新时间：2026-01-05
