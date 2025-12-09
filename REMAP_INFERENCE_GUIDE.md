# Remap 推导能力说明

## 概述

本工具支持基于 TABLE 的 remap 规则自动推导其他对象类型的目标 schema，减少用户配置工作量。但在某些场景下仍需显式指定 remap 规则。

## 支持的映射场景

### 1. 多对一映射（Many-to-One）✅ 完全支持

**场景示例**：
```
HERO_A.HEROES     -> OLYMPIAN_A.HEROES
HERO_A.TREASURES  -> OLYMPIAN_A.HERO_TREASURES
HERO_B.LEGENDS    -> OLYMPIAN_A.LEGENDS
HERO_B.TREASURES  -> OLYMPIAN_A.LEGEND_TREASURES
```

**推导能力**：
- ✅ TABLE：需要显式 remap
- ✅ VIEW/PROCEDURE/FUNCTION/PACKAGE：**自动推导**到 OLYMPIAN_A
- ✅ TRIGGER/INDEX/CONSTRAINT/SEQUENCE：**自动推导**（跟随父表）

**原理**：
- 程序识别出 HERO_A → OLYMPIAN_A 和 HERO_B → OLYMPIAN_A
- 为非 TABLE 对象自动应用 schema 映射
- 例如：`HERO_A.VW_HERO_STATUS` 自动推导为 `OLYMPIAN_A.VW_HERO_STATUS`

### 2. 一对一映射（One-to-One）✅ 完全支持

**场景示例**：
```
GOD_A.DOMAINS   -> PRIMORDIAL.REALMS
GOD_A.PANTHEON  -> PRIMORDIAL.PANTHEON
GOD_A.PORTALS   -> PRIMORDIAL.GATES
```

**推导能力**：
- ✅ TABLE：需要显式 remap
- ✅ VIEW/PROCEDURE/FUNCTION/PACKAGE：**自动推导**到 PRIMORDIAL
- ✅ TRIGGER/INDEX/CONSTRAINT/SEQUENCE：**自动推导**（跟随父表）

**原理**：
- 程序识别出 GOD_A → PRIMORDIAL 的唯一映射
- 所有 GOD_A 的非 TABLE 对象自动推导到 PRIMORDIAL

### 3. 一对多映射（One-to-Many）⚠️ 部分支持

**场景示例**：
```
MONSTER_A.LAIR         -> TITAN_A.LAIR_INFO
MONSTER_A.MINIONS      -> TITAN_A.MINIONS
MONSTER_A.TRAPS        -> TITAN_B.TRAP_STATUS
MONSTER_A.CURSES       -> TITAN_B.CURSES
```

**推导能力**：
- ✅ TABLE：需要显式 remap
- ❌ VIEW/PROCEDURE/FUNCTION/PACKAGE：**无法自动推导**，必须显式指定
- ✅ TRIGGER/INDEX/CONSTRAINT/SEQUENCE：**自动推导**（跟随父表）

**原因**：
- MONSTER_A 的表分散到 TITAN_A 和 TITAN_B 两个 schema
- 程序无法判断 `MONSTER_A.VW_LAIR_RICHNESS` 应该放在 TITAN_A 还是 TITAN_B
- 但 `MONSTER_A.TRG_LAIR_BI` 可以自动跟随父表 `LAIR` 到 TITAN_A

**解决方案**：
在 `remap_rules.txt` 中显式指定独立对象的 remap：
```
MONSTER_A.VW_LAIR_RICHNESS    = TITAN_B.VW_LAIR_RICHNESS
MONSTER_A.VW_DANGER_MATRIX    = TITAN_B.VW_DANGER_MATRIX
MONSTER_A.SP_SUMMON_MINION    = TITAN_B.SP_SUMMON_MINION
MONSTER_A.PKG_MONSTER_OPS     = TITAN_B.PKG_MONSTER_OPS
```

## 对象类型分类

### 依附对象（Dependent Objects）
这些对象依附于表，会自动跟随父表的 schema：
- TRIGGER（触发器）
- INDEX（索引）
- CONSTRAINT（约束）
- SEQUENCE（序列，如果与表关联）

### 独立对象（Independent Objects）
这些对象不依附于特定表，需要 schema 映射推导：
- VIEW（视图）
- MATERIALIZED VIEW（物化视图）
- PROCEDURE（存储过程）
- FUNCTION（函数）
- PACKAGE / PACKAGE BODY（包/包体）
- SYNONYM（同义词）
- TYPE / TYPE BODY（类型/类型体）
- JOB / SCHEDULE（作业/调度）

## 最佳实践

### 生产环境配置建议

1. **仅提供 TABLE 的 remap 规则**（推荐）：
   ```
   # 只配置表的映射
   HERO_A.HEROES     = OLYMPIAN_A.HEROES
   HERO_A.TREASURES  = OLYMPIAN_A.HERO_TREASURES
   HERO_B.LEGENDS    = OLYMPIAN_A.LEGENDS
   ```
   - 适用场景：多对一、一对一映射
   - 其他对象会自动推导

2. **一对多场景需要补充独立对象的 remap**：
   ```
   # 表的映射
   MONSTER_A.LAIR    = TITAN_A.LAIR_INFO
   MONSTER_A.TRAPS   = TITAN_B.TRAP_STATUS
   
   # 必须显式指定独立对象
   MONSTER_A.VW_LAIR_RICHNESS = TITAN_B.VW_LAIR_RICHNESS
   MONSTER_A.PKG_MONSTER_OPS  = TITAN_B.PKG_MONSTER_OPS
   ```

3. **检查程序警告**：
   - 程序会自动检测一对多场景并输出警告
   - 警告信息会提示哪些 schema 需要显式配置独立对象

### 验证推导结果

运行程序后，检查以下内容：

1. **查看警告日志**：
   ```
   检测到一对多 schema 映射场景（源schema的表分散到多个目标schema）：
     MONSTER_A -> ['TITAN_A', 'TITAN_B']
   
   注意：在一对多场景下，独立对象（VIEW/PROCEDURE/FUNCTION/PACKAGE等）
   无法自动推导目标schema，必须在 remap_rules.txt 中显式指定。
   ```

2. **检查报告中的缺失对象**：
   - 如果 VIEW/PROCEDURE 等对象被报告为"缺失"
   - 可能是因为一对多场景下未显式指定 remap

3. **检查生成的 fixup 脚本**：
   - 查看 DDL 中的 schema 和表引用是否正确
   - 特别注意跨 schema 引用的表名

## DDL 引用替换

无论对象如何推导，程序都会自动替换 DDL 中的表引用：

**原始 VIEW DDL**：
```sql
CREATE VIEW HERO_A.VW_ALL_TREASURES AS
SELECT * FROM HERO_A.TREASURES
UNION ALL
SELECT * FROM HERO_B.TREASURES;
```

**自动替换后**：
```sql
CREATE VIEW OLYMPIAN_A.VW_ALL_TREASURES AS
SELECT * FROM OLYMPIAN_A.HERO_TREASURES
UNION ALL
SELECT * FROM OLYMPIAN_A.LEGEND_TREASURES;
```

替换规则：
- 带 schema 前缀的引用：`HERO_A.TREASURES` → `OLYMPIAN_A.HERO_TREASURES`
- 不带 schema 前缀的引用：`TREASURES` → `OLYMPIAN_A.HERO_TREASURES`（如果发生跨 schema remap）
- 主对象名称：`END PKG_DIVINITY` → `END PKG_COSMOS`

## 配置选项

### infer_schema_mapping

控制是否启用 schema 映射推导：

```ini
[SETTINGS]
infer_schema_mapping = true   # 默认值，推荐保持开启
```

- `true`：启用自动推导（推荐）
- `false`：禁用自动推导，所有对象都需要显式 remap

### 查看推导结果

程序会在日志中输出推导的 schema 映射：

```
[INFO] Schema 映射推导结果（基于 TABLE）：
  HERO_A -> OLYMPIAN_A
  HERO_B -> OLYMPIAN_A
  GOD_A -> PRIMORDIAL
  MONSTER_A -> MONSTER_A (一对多，无法推导)
```

## 故障排查

### 问题：VIEW/PROCEDURE 被报告为缺失

**可能原因**：
1. 一对多场景下未显式指定 remap
2. `infer_schema_mapping` 被设置为 `false`

**解决方案**：
1. 检查是否有一对多警告
2. 在 `remap_rules.txt` 中添加显式 remap
3. 确认 `infer_schema_mapping = true`

### 问题：生成的 DDL 中表引用不正确

**可能原因**：
1. `remap_rules.txt` 中缺少某些表的 remap
2. 表名在 DDL 中使用了别名或特殊格式

**解决方案**：
1. 确保所有被引用的表都在 `remap_rules.txt` 中
2. 检查 `all_replacements` 是否包含所有表的映射
3. 人工审核并修正生成的 fixup 脚本

## 总结

| 映射场景 | TABLE | 独立对象 | 依附对象 | 配置要求 |
|---------|-------|---------|---------|---------|
| 多对一 | 显式 | 自动推导 | 自动推导 | 仅配置 TABLE |
| 一对一 | 显式 | 自动推导 | 自动推导 | 仅配置 TABLE |
| 一对多 | 显式 | **需显式** | 自动推导 | TABLE + 独立对象 |

**推荐做法**：
1. 始终显式配置所有 TABLE 的 remap
2. 多对一和一对一场景：其他对象会自动推导
3. 一对多场景：根据警告提示，补充独立对象的 remap
4. 运行后检查报告和 fixup 脚本，确认推导结果正确
