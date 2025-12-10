# Run Fixup 高级使用指南

## 概述

`run_fixup.py` 和 `run_fixup_v2.py` 是用于批量执行修补脚本的工具，支持依赖感知排序、自动重编译和灵活的过滤选项。

---

## 版本对比

| 特性 | run_fixup.py (v1) | run_fixup_v2.py (v2) |
|------|-------------------|----------------------|
| 基础执行 | ✅ | ✅ |
| 优先级排序 | ✅ | ✅ |
| 依赖感知排序 | ❌ | ✅ (--smart-order) |
| 授权前置 | ❌ | ✅ (--smart-order) |
| 自动重编译 | ❌ | ✅ (--recompile) |
| 目录过滤 | ✅ | ✅ |
| 类型过滤 | ✅ | ✅ |
| 文件名过滤 | ✅ | ✅ |
| 执行报告 | ✅ | ✅ (增强) |

**推荐：** 使用 `run_fixup_v2.py --smart-order --recompile` 获得最佳体验

---

## 快速开始

### 基础用法

```bash
# 使用默认 config.ini，执行所有脚本
python3 run_fixup.py

# 使用自定义配置文件
python3 run_fixup.py /path/to/config.ini
```

### 增强用法 (v2)

```bash
# 启用依赖感知排序 + 自动重编译
python3 run_fixup_v2.py --smart-order --recompile

# 自定义重编译次数
python3 run_fixup_v2.py --smart-order --recompile --max-retries 10
```

---

## 执行顺序

### 标准优先级排序 (v1 和 v2 默认)

```
1. sequence      - 序列
2. table         - 表
3. table_alter   - 表修改
4. constraint    - 约束
5. index         - 索引
6. view          - 视图
7. materialized_view - 物化视图
8. synonym       - 同义词
9. procedure     - 存储过程
10. function     - 函数
11. package      - 包规范
12. package_body - 包体
13. type         - 类型
14. type_body    - 类型体
15. trigger      - 触发器
16. job          - 作业
17. schedule     - 调度
18. grants       - 授权
```

### 依赖感知排序 (v2 --smart-order)

```
Layer 0: sequence           - 无依赖对象
Layer 1: table              - 基础表
Layer 2: table_alter        - 表修改
Layer 3: grants             - 授权（在依赖对象前执行）← 关键
Layer 4: view, synonym      - 简单依赖对象
Layer 5: materialized_view  - 物化视图
Layer 6: procedure, function - 独立例程
Layer 7: package, type      - 包规范和类型
Layer 8: package_body, type_body - 包体和类型体
Layer 9: constraint, index  - 约束和索引
Layer 10: trigger           - 触发器（最后）
Layer 11: job, schedule     - 作业和调度
```

**关键优化：**
- ✅ 授权在第3层执行，早于所有依赖对象
- ✅ 视图在包之前，避免编译失败
- ✅ 触发器最后执行，确保表结构完整

---

## 过滤选项

### 按子目录过滤

```bash
# 只执行表和视图脚本
python3 run_fixup.py --only-dirs table,view

# 排除触发器和作业
python3 run_fixup.py --exclude-dirs trigger,job
```

### 按对象类型过滤

```bash
# 只执行表和索引
python3 run_fixup.py --only-types TABLE,INDEX

# 支持的类型（自动映射到目录）
python3 run_fixup.py --only-types "TABLE,VIEW,PROCEDURE,FUNCTION,PACKAGE"
```

**类型映射表：**
```
TABLE            -> table/
TABLE_ALTER      -> table_alter/
SEQUENCE         -> sequence/
INDEX            -> index/
CONSTRAINT       -> constraint/
VIEW             -> view/
MATERIALIZED_VIEW -> materialized_view/
SYNONYM          -> synonym/
PROCEDURE        -> procedure/
FUNCTION         -> function/
PACKAGE          -> package/
PACKAGE_BODY     -> package_body/
TYPE             -> type/
TYPE_BODY        -> type_body/
TRIGGER          -> trigger/
JOB              -> job/
SCHEDULE         -> schedule/
GRANTS           -> grants/
```

### 按文件名过滤

```bash
# 只执行包含日期的脚本
python3 run_fixup.py --glob "*20250110*.sql"

# 只执行特定 schema 的脚本
python3 run_fixup.py --glob "SCHEMA_A.*.sql"

# 组合多个模式
python3 run_fixup.py --glob "*VIEW*.sql" --glob "*PROC*.sql"
```

### 组合过滤

```bash
# 只执行 SCHEMA_A 的表和视图脚本
python3 run_fixup.py --only-types TABLE,VIEW --glob "SCHEMA_A.*.sql"

# 执行所有脚本，但排除触发器
python3 run_fixup.py --exclude-dirs trigger
```

---

## 重编译功能 (v2)

### 为什么需要重编译？

在对象创建过程中，可能出现以下情况导致对象状态为 INVALID：

1. **循环依赖**
   ```
   VIEW_A 引用 VIEW_B
   VIEW_B 引用 VIEW_A
   → 创建时至少有一个会 INVALID
   ```

2. **依赖对象尚未创建**
   ```
   PROCEDURE_A 调用 FUNCTION_B
   如果 FUNCTION_B 在 PROCEDURE_A 之后创建
   → PROCEDURE_A 会 INVALID
   ```

3. **权限问题**
   ```
   VIEW_A (SCHEMA_B) 引用 TABLE_A (SCHEMA_A)
   如果 GRANT 在 VIEW_A 之后执行
   → VIEW_A 会 INVALID
   ```

### 重编译策略

```bash
# 启用自动重编译（默认最多5次）
python3 run_fixup_v2.py --recompile

# 自定义重试次数
python3 run_fixup_v2.py --recompile --max-retries 10

# 结合依赖感知排序（推荐）
python3 run_fixup_v2.py --smart-order --recompile
```

### 重编译过程

```
[重编译] 第 1/5 轮，发现 12 个 INVALID 对象
  ✓ SCHEMA_A.VIEW_A (VIEW)
  ✓ SCHEMA_A.VIEW_B (VIEW)
  ✓ SCHEMA_B.PROC_A (PROCEDURE)
  ✗ SCHEMA_B.PKG_A (PACKAGE BODY): ORA-04063: package body has errors
  ...

[重编译] 第 2/5 轮，发现 3 个 INVALID 对象
  ✓ SCHEMA_B.PKG_A (PACKAGE BODY)
  ✓ SCHEMA_C.FUNC_A (FUNCTION)
  ...

[重编译] 第 3/5 轮，发现 0 个 INVALID 对象

重编译统计:
  重编译成功 : 12
  仍为INVALID: 0
```

### 手动检查 INVALID 对象

```sql
-- 查询所有 INVALID 对象
SELECT OWNER, OBJECT_NAME, OBJECT_TYPE, STATUS
FROM DBA_OBJECTS
WHERE STATUS = 'INVALID'
ORDER BY OWNER, OBJECT_TYPE, OBJECT_NAME;

-- 手动重编译
ALTER VIEW SCHEMA_A.VIEW_A COMPILE;
ALTER PROCEDURE SCHEMA_B.PROC_A COMPILE;
ALTER PACKAGE SCHEMA_C.PKG_A COMPILE;
ALTER PACKAGE BODY SCHEMA_C.PKG_A COMPILE;
```

---

## 执行报告

### 标准报告 (v1)

```
================== 执行结果汇总 ==================
扫描脚本数 : 150
实际执行数 : 148
成功       : 145
失败       : 3
跳过       : 2

明细表：
+------------------------------------------+---------------------------+
| 脚本                                     | 信息                      |
+------------------------------------------+---------------------------+
| fixup_scripts/table/SCHEMA_A.TABLE1.sql  | 成功 (已移至 done/table/) |
| fixup_scripts/view/SCHEMA_A.VIEW1.sql    | 成功 (已移至 done/view/)  |
| fixup_scripts/trigger/SCHEMA_A.TRG1.sql  | 失败                      |
+------------------------------------------+---------------------------+
```

### 增强报告 (v2)

```
======================================================================
开始执行修补脚本
目录: /path/to/fixup_scripts
模式: 依赖感知排序 (SMART ORDER)
重编译: 启用 (最多 5 次重试)
共发现 SQL 文件: 150
======================================================================

======================================================================
第 0 层
======================================================================
[001/150] fixup_scripts/sequence/SCHEMA_A.SEQ1.sql -> ✓ 成功
[002/150] fixup_scripts/sequence/SCHEMA_A.SEQ2.sql -> ✓ 成功

======================================================================
第 1 层
======================================================================
[003/150] fixup_scripts/table/SCHEMA_A.TABLE1.sql -> ✓ 成功
[004/150] fixup_scripts/table/SCHEMA_A.TABLE2.sql -> ✓ 成功

======================================================================
第 3 层
======================================================================
[010/150] fixup_scripts/grants/SCHEMA_A.grants.sql -> ✓ 成功
[011/150] fixup_scripts/grants/SCHEMA_B.grants.sql -> ✓ 成功

======================================================================
第 4 层
======================================================================
[012/150] fixup_scripts/view/SCHEMA_A.VIEW1.sql -> ✓ 成功
[013/150] fixup_scripts/view/SCHEMA_A.VIEW2.sql -> ✗ 失败
    ORA-00942: table or view does not exist

...

======================================================================
重编译阶段
======================================================================
[重编译] 第 1/5 轮，发现 5 个 INVALID 对象
  ✓ SCHEMA_A.VIEW2 (VIEW)
  ✓ SCHEMA_B.PROC1 (PROCEDURE)
  ...

======================================================================
执行结果汇总
======================================================================
扫描脚本数 : 150
实际执行数 : 148
成功       : 145
失败       : 3
跳过       : 2

重编译统计:
  重编译成功 : 5
  仍为INVALID: 0

======================================================================
详细结果
======================================================================

✓ 成功 (145):
  fixup_scripts/sequence/SCHEMA_A.SEQ1.sql
  fixup_scripts/table/SCHEMA_A.TABLE1.sql
  ...

✗ 失败 (3):
  fixup_scripts/view/SCHEMA_A.VIEW3.sql
    ORA-00942: table or view does not exist
  ...
```

---

## 常见场景

### 场景1：首次全量执行

```bash
# 推荐：使用依赖感知排序 + 自动重编译
python3 run_fixup_v2.py --smart-order --recompile

# 或者分步执行
python3 run_fixup.py --only-dirs sequence,table,table_alter
python3 run_fixup.py --only-dirs grants
python3 run_fixup.py --only-dirs view,procedure,function,package
python3 run_fixup.py --only-dirs trigger
python3 run_fixup_v2.py --recompile
```

### 场景2：只修复失败的脚本

```bash
# 第一次执行后，失败的脚本仍在原目录
# 成功的脚本已移到 done/ 目录

# 再次执行，只会执行失败的脚本
python3 run_fixup_v2.py --smart-order --recompile
```

### 场景3：增量执行（只执行新增脚本）

```bash
# 假设新增了一些视图和存储过程
python3 run_fixup.py --only-types VIEW,PROCEDURE

# 或者按日期过滤
python3 run_fixup.py --glob "*20250110*.sql"
```

### 场景4：跨 Schema 依赖

```bash
# 场景：SCHEMA_B 的对象依赖 SCHEMA_A 的表
# 需要先授权，再创建对象

# 方案1：使用依赖感知排序（自动处理）
python3 run_fixup_v2.py --smart-order

# 方案2：手动分步执行
python3 run_fixup.py --only-dirs table          # 先创建表
python3 run_fixup.py --only-dirs grants         # 再授权
python3 run_fixup.py --only-dirs view,procedure # 最后创建依赖对象
```

### 场景5：循环依赖

```bash
# 场景：VIEW_A 和 VIEW_B 互相引用

# 方案1：使用重编译（推荐）
python3 run_fixup_v2.py --recompile --max-retries 10

# 方案2：手动处理
python3 run_fixup.py --only-dirs view  # 创建所有视图（可能INVALID）
# 然后手动重编译
obclient -h ... -u ... -p... <<EOF
ALTER VIEW SCHEMA_A.VIEW_A COMPILE;
ALTER VIEW SCHEMA_A.VIEW_B COMPILE;
EOF
```

### 场景6：大批量执行（生产环境）

```bash
# 使用 nohup 后台执行，记录日志
nohup python3 run_fixup_v2.py --smart-order --recompile > fixup.log 2>&1 &

# 实时查看进度
tail -f fixup.log

# 执行完成后检查结果
grep "执行结果汇总" fixup.log -A 10
grep "✗ 失败" fixup.log
```

---

## 故障排查

### 问题1：权限不足

**症状：**
```
ORA-01031: insufficient privileges
```

**解决：**
```bash
# 确认 config.ini 中的用户有足够权限
# 建议使用 SYS 或具有 DBA 角色的用户

# 检查当前用户权限
obclient -h ... -u ... -p... -e "SELECT * FROM USER_ROLE_PRIVS;"
```

### 问题2：对象已存在

**症状：**
```
ORA-00955: name is already used by an existing object
```

**解决：**
```bash
# 方案1：删除已存在的对象
obclient -h ... -u ... -p... -e "DROP VIEW SCHEMA_A.VIEW1;"

# 方案2：修改脚本使用 CREATE OR REPLACE
# 在 fixup_scripts/ 中编辑 SQL 文件
```

### 问题3：依赖对象不存在

**症状：**
```
ORA-00942: table or view does not exist
```

**解决：**
```bash
# 方案1：使用依赖感知排序
python3 run_fixup_v2.py --smart-order

# 方案2：检查是否缺少授权
python3 run_fixup.py --only-dirs grants

# 方案3：检查依赖对象是否在 remap_rules.txt 中
# 确保所有依赖对象都有对应的 remap 规则
```

### 问题4：大量 INVALID 对象

**症状：**
```
重编译统计:
  重编译成功 : 50
  仍为INVALID: 20
```

**解决：**
```bash
# 增加重编译次数
python3 run_fixup_v2.py --recompile --max-retries 20

# 查看具体错误
obclient -h ... -u ... -p... <<EOF
SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
FROM DBA_OBJECTS
WHERE STATUS = 'INVALID';

-- 查看编译错误
SELECT * FROM DBA_ERRORS
WHERE OWNER = 'SCHEMA_A' AND NAME = 'VIEW1';
EOF
```

### 问题5：执行超时

**症状：**
```
执行超时 (> 60 秒)
```

**解决：**
```bash
# 在 config.ini 中增加超时时间
[SETTINGS]
obclient_timeout = 300  # 5分钟

# 或者拆分大脚本为多个小脚本
```

---

## 最佳实践

### 1. 执行前准备

```bash
# 备份目标数据库
# 确认 config.ini 配置正确
# 检查 fixup_scripts/ 目录结构
ls -la fixup_scripts/

# 预估执行时间（每个脚本约1-5秒）
# 150个脚本 ≈ 5-10分钟
```

### 2. 推荐执行流程

```bash
# Step 1: 首次执行（使用增强版）
python3 run_fixup_v2.py --smart-order --recompile > fixup_$(date +%Y%m%d_%H%M%S).log 2>&1

# Step 2: 检查结果
grep "执行结果汇总" fixup_*.log -A 20

# Step 3: 如有失败，查看详情
grep "✗ 失败" fixup_*.log -A 2

# Step 4: 修复失败脚本后重新执行
python3 run_fixup_v2.py --smart-order --recompile

# Step 5: 最终验证
obclient -h ... -u ... -p... <<EOF
-- 检查 INVALID 对象
SELECT COUNT(*) FROM DBA_OBJECTS WHERE STATUS = 'INVALID';

-- 检查对象数量
SELECT OBJECT_TYPE, COUNT(*) 
FROM DBA_OBJECTS 
WHERE OWNER IN ('SCHEMA_A', 'SCHEMA_B')
GROUP BY OBJECT_TYPE;
EOF
```

### 3. 生产环境建议

- ✅ 使用 `--smart-order` 确保正确的执行顺序
- ✅ 使用 `--recompile` 自动处理 INVALID 对象
- ✅ 使用 `nohup` 后台执行，避免网络中断
- ✅ 保存执行日志，便于审计和排查
- ✅ 分批执行（先表，再视图，最后触发器）
- ✅ 每批执行后验证结果
- ❌ 不要在业务高峰期执行
- ❌ 不要跳过 grants 目录

### 4. 幂等性保证

```bash
# run_fixup 支持幂等执行：
# - 成功的脚本会移到 done/ 目录
# - 再次执行只会处理失败的脚本
# - 可以安全地多次执行

# 示例：
python3 run_fixup_v2.py --smart-order --recompile  # 第1次
# 修复失败的脚本
python3 run_fixup_v2.py --smart-order --recompile  # 第2次（只执行失败的）
python3 run_fixup_v2.py --smart-order --recompile  # 第3次（直到全部成功）
```

---

## 性能优化

### 并发执行（实验性）

```bash
# 注意：当前版本不支持并发
# 未来版本可能支持：
# python3 run_fixup_v2.py --smart-order --workers 4

# 当前可以手动并发执行不同类型：
python3 run_fixup.py --only-types TABLE &
python3 run_fixup.py --only-types SEQUENCE &
wait
python3 run_fixup.py --only-types VIEW &
python3 run_fixup.py --only-types PROCEDURE &
wait
```

### 批量执行

```bash
# 对于大量脚本（1000+），建议分批执行
python3 run_fixup.py --only-dirs sequence,table
python3 run_fixup.py --only-dirs grants
python3 run_fixup.py --only-dirs view,synonym
python3 run_fixup.py --only-dirs procedure,function,package
python3 run_fixup.py --only-dirs trigger
python3 run_fixup_v2.py --recompile
```

---

## 附录

### A. 目录结构示例

```
fixup_scripts/
├── sequence/
│   ├── SCHEMA_A.SEQ1.sql
│   └── SCHEMA_A.SEQ2.sql
├── table/
│   ├── SCHEMA_A.TABLE1.sql
│   └── SCHEMA_B.TABLE1.sql
├── table_alter/
│   └── SCHEMA_A.TABLE1.alter_columns.sql
├── grants/
│   ├── SCHEMA_A.grants.sql
│   └── SCHEMA_B.grants.sql
├── view/
│   ├── SCHEMA_A.VIEW1.sql
│   └── SCHEMA_A.VIEW2.sql
├── procedure/
│   └── SCHEMA_A.PROC1.sql
├── package/
│   └── SCHEMA_A.PKG1.sql
├── package_body/
│   └── SCHEMA_A.PKG1.sql
├── trigger/
│   └── SCHEMA_A.TRG1.sql
└── done/              # 成功执行的脚本移到这里
    ├── sequence/
    ├── table/
    └── ...
```

### B. 配置文件示例

```ini
[OCEANBASE_TARGET]
executable  = /usr/bin/obclient
host        = 172.16.0.147
port        = 2883
user_string = SYS@ob4ora#observer147
password    = YourPassword

[SETTINGS]
fixup_dir           = fixup_scripts
obclient_timeout    = 120
```

### C. 常用命令速查

```bash
# 全量执行（推荐）
python3 run_fixup_v2.py --smart-order --recompile

# 只执行表和视图
python3 run_fixup.py --only-types TABLE,VIEW

# 排除触发器
python3 run_fixup.py --exclude-dirs trigger

# 按文件名过滤
python3 run_fixup.py --glob "*SCHEMA_A*.sql"

# 查看帮助
python3 run_fixup.py --help
python3 run_fixup_v2.py --help
```

---

## 版本历史

- **v2.0** (2025-12-10): 增加依赖感知排序和自动重编译
- **v1.0** (2025-12-09): 初始版本，支持基础执行和过滤

---

## 支持

如有问题，请查看：
- `docs/CHANGELOG.md` - 版本变更记录
- `docs/FIXES_v0.8.1.md` - 已知问题和修复
- `docs/TABLE_CENTRIC_REMAP_AUDIT.md` - 对象推导逻辑说明

---

**作者：** OceanBase Migration Team  
**更新日期：** 2025-12-10
