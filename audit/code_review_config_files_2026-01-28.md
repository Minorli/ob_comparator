# 配置文件审查报告

**审查日期**: 2026-01-28
**审查范围**: config.ini.template, blacklist_rules.json
**文件**: 配置模板和规则文件

---

## 1. config.ini.template 问题

### 1.1 重复配置项 (中优先级)

**位置**: 行 191-203 和 236-249

**重复的配置项**:
```ini
# 第一次出现 (行 191-203)
ddl_punct_sanitize      = true
ddl_hint_policy         = keep_supported
ddl_hint_allowlist      =
ddl_hint_denylist       =
ddl_hint_allowlist_file =

# 第二次出现 (行 236-249) - 完全重复
ddl_punct_sanitize      = true
ddl_hint_policy         = keep_supported
ddl_hint_allowlist      =
ddl_hint_denylist       =
ddl_hint_allowlist_file =
```

**问题**: 5 个配置项重复定义，可能导致混淆

**建议**: 删除行 236-249 的重复配置

---

## 2. blacklist_rules.json 审查

### 2.1 结构良好

文件结构清晰，包含 7 条规则:
- `DIY_TYPES` - 自定义类型
- `UNSUPPORTED_TYPES` - 不支持的数据类型
- `LONG_TYPES` - LONG/LONG RAW 类型
- `LOB_OVERSIZE` - 超大 LOB
- `TEMPORARY_TABLES` - 临时表
- `EXTERNAL_TABLES` - 外部表
- `IOT_TABLES` - IOT 表

### 2.2 潜在问题

**SQL 注入风险 (低)**

**位置**: 所有规则的 `{{owners_clause}}` 占位符

**代码**:
```sql
WHERE owner IN ({{owners_clause}})
```

**评估**: 占位符由程序内部生成，非用户直接输入，风险较低。但应确保 `owners_clause` 生成时正确转义。

---

## 3. 问题汇总

| 文件 | 问题 | 严重程度 |
|------|------|----------|
| config.ini.template | 5 个配置项重复 (行 236-249) | 中 |
| blacklist_rules.json | 无明显问题 | - |

---

## 4. 修复建议

1. **删除 config.ini.template 中的重复配置** (行 236-249)
2. **同步更新 readme_config.txt** 确保文档与模板一致

---

*审查工具: Claude Code (claude-opus-4-5-20251101)*
