# 业务逻辑深度审查报告

**审查重点**: 功能正确性、边界情况、实际使用场景  
**优先级**: 🔴 关键

---

## 一、Remap推导逻辑审查

### 1.1 核心业务场景

**场景**: Oracle → OceanBase 迁移时，需要将对象从源schema映射到目标schema

**支持的映射模式**:
1. **一对一**: `SRC_A → TGT_A`
2. **一对多**: `SRC_A → TGT_A, TGT_B` (按表拆分)
3. **多对一**: `SRC_A, SRC_B → TGT_A` (合并schema)

### 1.2 发现的逻辑问题 🔴

#### 问题1: SEQUENCE推导策略不一致

**位置**: `resolve_remap_target()` 第4435-4536行

```python
# 第4435行: 如果策略是source_only，直接返回
if obj_type_u == 'SEQUENCE' and sequence_policy == "source_only":
    return src_name_u

# 第4535行: 又检查一次相同条件
if obj_type_u == 'SEQUENCE' and sequence_policy == "source_only":
    return src_name_u
```

**问题**: 
- 重复检查导致逻辑混乱
- 第二次检查永远不会执行（第一次已返回）
- 中间的推导逻辑（4510-4529行）在source_only模式下被跳过

**实际影响**:
```python
# 场景: SEQUENCE策略为source_only，但有父表映射
remap_rules = {"A.T1": "B.T1"}
object_parent_map = {"A.SEQ1": "A.T1"}  # SEQ1属于T1
sequence_remap_policy = "source_only"

# 期望: A.SEQ1 → A.SEQ1 (保持原schema)
# 实际: A.SEQ1 → A.SEQ1 ✅ 正确

# 但如果策略是infer:
sequence_remap_policy = "infer"
# 期望: A.SEQ1 → B.SEQ1 (跟随表)
# 实际: 需要测试验证
```

**修复建议**:
```python
def resolve_remap_target(...):
    # 1. 显式规则优先（保持不变）
    if key in remap_rules:
        return remap_rules[key]
    
    # 2. SEQUENCE特殊处理
    if obj_type_u == 'SEQUENCE':
        if sequence_policy == "source_only":
            return src_name_u  # 早期返回
        elif sequence_policy == "infer":
            # 执行推导逻辑
            pass
        # 不要重复检查
```

---

#### 问题2: TRIGGER schema推导逻辑缺陷 ⚠️

**位置**: `NO_INFER_SCHEMA_TYPES` 第625-631行

```python
NO_INFER_SCHEMA_TYPES: Set[str] = {
    'VIEW',
    'MATERIALIZED VIEW',
    'TRIGGER',  # ❌ TRIGGER不参与schema推导
    'PACKAGE',
    'PACKAGE BODY'
}
```

**问题**: TRIGGER被标记为不推导schema，但实际场景中：

**场景1**: 表remap后，触发器应该跟随
```sql
-- 源端
CREATE TRIGGER SRC_A.TRG1 ON SRC_A.T1 ...

-- 如果表remap: SRC_A.T1 → TGT_B.T1
-- 期望: SRC_A.TRG1 → SRC_A.TRG1 (保持原schema)
-- 实际: SRC_A.TRG1 → SRC_A.TRG1 ✅ 符合设计

-- 但触发器DDL中需要修改表引用:
CREATE TRIGGER SRC_A.TRG1 ON TGT_B.T1 ...  -- 表名需要调整
```

**潜在问题**: 
- 触发器保持原schema，但所属表已迁移到其他schema
- 跨schema触发器可能有权限问题
- DDL调整逻辑是否正确处理了这种情况？

**需要验证的测试用例**:
```python
def test_trigger_cross_schema_after_table_remap():
    """测试表remap后触发器的跨schema引用"""
    remap_rules = {"A.T1": "B.T1"}
    object_parent_map = {"A.TRG1": "A.T1"}
    
    # 触发器保持A schema
    target = resolve_remap_target("A.TRG1", "TRIGGER", remap_rules, 
                                   object_parent_map=object_parent_map)
    assert target == "A.TRG1"
    
    # 但DDL中的表引用应该是B.T1
    # 需要检查DDL调整逻辑
```

---

#### 问题3: 循环依赖检测不完整 ⚠️

**位置**: `resolve_remap_target()` 第4414-4416行

```python
node = (src_name_u, obj_type_u)
if node in path:
    return None  # ❌ 检测到循环，返回None
path.add(node)
```

**问题**: 循环依赖时返回None，但调用方可能将None当作"使用源名称"

**危险场景**:
```python
# A.V1 依赖 A.V2，A.V2 依赖 A.V1 (循环)
deps = {
    ("A", "V1", "VIEW", "A", "V2", "VIEW"),
    ("A", "V2", "VIEW", "A", "V1", "VIEW"),
}

# 推导A.V1时:
# 1. 进入A.V1，添加到path
# 2. 推导依赖A.V2
# 3. 进入A.V2，添加到path
# 4. 推导依赖A.V1
# 5. 检测到A.V1在path中，返回None
# 6. A.V2推导失败，返回None
# 7. A.V1推导失败，返回None

# 调用方处理:
tgt_name = resolve_remap_target(...) or src_name_u
# 结果: A.V1 → A.V1, A.V2 → A.V2

# 问题: 没有警告用户存在循环依赖！
```

**修复建议**:
```python
def resolve_remap_target(...):
    node = (src_name_u, obj_type_u)
    if node in path:
        # 记录循环依赖
        if remap_conflicts:
            remap_conflicts[node] = f"循环依赖: {' -> '.join(str(n) for n in path)} -> {node}"
        log.warning("检测到循环依赖: %s", node)
        return None
```

---

#### 问题4: 多对一映射冲突处理不一致 🔴

**位置**: `generate_master_list()` 第4654-4663行

```python
if key in target_tracker:
    existing_src = target_tracker[key]
    if existing_src != src_name_u:
        log.warning("检测到多对一映射: ...")
        tgt_name_u = src_name_u  # ❌ 回退为1:1
        tgt_name = src_name_u
        key = (tgt_name_u, obj_type_u)
```

**问题**: 多对一冲突时，**后来的对象**被回退，但这可能不是最优选择

**场景**:
```python
# 两个源表映射到同一目标
remap_rules = {
    "A.T1": "C.T1",
    "B.T1": "C.T1",  # 冲突！
}

# 处理顺序: A.T1 先处理，B.T1 后处理
# 结果:
# A.T1 → C.T1  ✅ 第一个映射成功
# B.T1 → B.T1  ❌ 被回退为1:1

# 问题: 用户明确配置了B.T1 → C.T1，为什么要回退？
# 应该报错让用户修正配置，而不是静默回退
```

**修复建议**:
```python
if key in target_tracker:
    existing_src = target_tracker[key]
    if existing_src != src_name_u:
        # 严重错误: 用户配置了冲突的映射
        error_msg = (
            f"配置错误: 多个源对象映射到同一目标\n"
            f"  目标: {tgt_name_u} ({obj_type_u})\n"
            f"  源1: {existing_src}\n"
            f"  源2: {src_name_u}\n"
            f"请修正 remap_rules 配置"
        )
        log.error(error_msg)
        # 选项1: 抛出异常
        raise ValueError(error_msg)
        # 选项2: 记录冲突，继续处理
        if remap_conflicts:
            remap_conflicts[(src_name_u, obj_type_u)] = "多对一映射冲突"
```

---

## 二、表结构对比逻辑审查

### 2.1 VARCHAR长度校验逻辑 🔴

**位置**: `schema_diff_reconciler.py:8776-8783`

```python
if src_dtype in ('VARCHAR2', 'VARCHAR'):
    src_len = src_info.get("char_length") or src_info.get("data_length")
    tgt_len = tgt_info.get("char_length") or tgt_info.get("data_length")
    
    try:
        src_len_int = int(src_len)
        tgt_len_int = int(tgt_len)
        
        # 校验逻辑
        min_required = math.ceil(src_len_int * VARCHAR_LEN_MIN_MULTIPLIER)  # 1.5倍
        max_reasonable = math.ceil(src_len_int * VARCHAR_LEN_OVERSIZE_MULTIPLIER)  # 2.5倍
```

**问题1: char_length可能是浮点数** (已在Bug报告中提到)

**问题2: CHAR vs BYTE语义混淆** 🔴

```python
# Oracle支持两种长度语义:
VARCHAR2(100 CHAR)  -- 字符语义，100个字符
VARCHAR2(100 BYTE)  -- 字节语义，100个字节

# 当前代码:
src_len = src_info.get("char_length") or src_info.get("data_length")
```

**危险场景**:
```sql
-- 源端 (UTF8, 3字节/字符)
CREATE TABLE A.T1 (
    NAME VARCHAR2(100 CHAR)  -- 100个字符，最多300字节
);

-- 目标端 (如果按字节创建)
CREATE TABLE B.T1 (
    NAME VARCHAR2(150 BYTE)  -- 150字节，只能存50个中文字符！
);

-- 数据迁移时会截断！
```

**实际代码检查**:
```python
# 第6393行: 读取char_length
"char_length": row[10],

# 第14575行: DDL生成时
if dt in ("VARCHAR", "VARCHAR2"):
    ln = _pick_length(char_length if char_used == "C" else (char_length or data_length))
    # 如果char_used="C"，使用char_length ✅
    # 如果char_used="B"，使用data_length ✅
    # 但对比时没有考虑char_used！❌
```

**修复建议**:
```python
def compare_varchar_length(src_info, tgt_info):
    """正确对比VARCHAR长度，考虑CHAR/BYTE语义"""
    src_char_used = src_info.get("char_used", "B").upper()
    tgt_char_used = tgt_info.get("char_used", "B").upper()
    
    # 如果语义不同，需要转换
    if src_char_used == "C" and tgt_char_used == "B":
        # 源是字符，目标是字节
        # 需要考虑字符集：UTF8最多3字节/字符，AL32UTF8最多4字节
        src_len_char = src_info.get("char_length")
        tgt_len_byte = tgt_info.get("data_length")
        
        # 保守估计: 1字符=4字节
        min_required_bytes = src_len_char * 4
        if tgt_len_byte < min_required_bytes:
            return "长度不足(字节语义)"
    
    # 同语义对比
    # ...
```

---

### 2.2 列对比逻辑缺陷 ⚠️

**位置**: 列集合对比逻辑

**问题**: 只对比列名集合，不对比列顺序

```python
# 当前逻辑 (简化)
src_cols = set(src_columns.keys())
tgt_cols = set(tgt_columns.keys())

missing = src_cols - tgt_cols
extra = tgt_cols - src_cols
```

**场景**:
```sql
-- 源端
CREATE TABLE A.T1 (
    ID NUMBER,
    NAME VARCHAR2(100),
    AGE NUMBER
);

-- 目标端 (列顺序不同)
CREATE TABLE B.T1 (
    NAME VARCHAR2(100),  -- 顺序变了
    ID NUMBER,
    AGE NUMBER
);

-- 当前检查: ✅ 通过 (列集合相同)
-- 实际问题: 列顺序不同可能影响:
--   1. SELECT * 结果顺序
--   2. INSERT 不指定列名时的行为
--   3. 某些应用程序的假设
```

**建议**: 添加列顺序检查（可选，通过配置控制）

---

## 三、依赖分析逻辑审查

### 3.1 传递依赖缓存逻辑 ✅

**位置**: `precompute_transitive_table_cache()` 第4200-4265行

**优点**: 
- 使用反向图+队列，避免重复DFS
- 正确处理循环依赖
- 性能优化到位

**测试覆盖**: 
```python
# test_schema_diff_reconciler.py:1819
def test_precompute_transitive_table_cache_handles_cycle(self):
    deps = {
        ("A", "P1", "PROCEDURE", "A", "P2", "PROCEDURE"),
        ("A", "P2", "PROCEDURE", "A", "P1", "PROCEDURE"),  # 循环
        ("A", "P1", "PROCEDURE", "A", "T1", "TABLE"),
    }
    # ✅ 有测试覆盖
```

---

### 3.2 依赖推导边界情况 ⚠️

**问题**: 深度依赖链可能导致性能问题

```python
# 场景: V1 → V2 → V3 → ... → V100 → T1
# 每个VIEW依赖下一个VIEW，最终依赖T1

# 推导V1时需要递归100层
# 虽然有缓存，但首次计算仍然很慢
```

**建议**: 添加递归深度限制

```python
MAX_DEPENDENCY_DEPTH = 50

def collect_transitive_tables(..., depth=0):
    if depth > MAX_DEPENDENCY_DEPTH:
        log.warning("依赖深度超过限制: %d", depth)
        return set()
    # ...
```

---

## 四、授权管理逻辑审查

### 4.1 权限推导逻辑 ✅

**位置**: `resolve_privilege_target()` 第8064-8099行

**优点**: 考虑了对象类型推导和remap

### 4.2 系统权限隐含逻辑 ⚠️

**位置**: 第7873-7877行

```python
def _sys_satisfies(identity: str, required_priv: str) -> bool:
    implied = SYS_PRIV_IMPLICATIONS.get(required_priv, set())
    if not implied:
        return False
    return any(pv in sys_privs.get(identity, set()) for pv in implied)
```

**问题**: `SYS_PRIV_IMPLICATIONS` 定义不完整

```python
# run_fixup.py:342
SYS_PRIV_IMPLICATIONS = {
    "SELECT": {
        "SELECT ANY TABLE",
        "SELECT ANY SEQUENCE",
        "SELECT ANY DICTIONARY",
    },
    "EXECUTE": {
        "EXECUTE ANY PROCEDURE",
        "EXECUTE ANY TYPE",
    },
    # ❌ 缺少其他权限的隐含关系
}
```

**缺失的隐含关系**:
- `INSERT` → `INSERT ANY TABLE`
- `UPDATE` → `UPDATE ANY TABLE`
- `DELETE` → `DELETE ANY TABLE`
- `CREATE` → `CREATE ANY TABLE`, `CREATE ANY VIEW`, etc.

**修复建议**: 补全所有权限隐含关系

---

## 五、黑名单过滤逻辑审查

### 5.1 LOB大小检测逻辑 ⚠️

**位置**: `blacklist_rules.json`

```json
{
  "id": "LOB_OVERSIZE",
  "sql": "...HAVING SUM(a.bytes) / 1024 / 1024 > {{lob_max_mb}}"
}
```

**问题**: 
1. 只检查segment大小，不检查实际数据大小
2. 多个LOB列时，是否应该累加？

**场景**:
```sql
CREATE TABLE T1 (
    ID NUMBER,
    DOC1 CLOB,  -- segment 300MB
    DOC2 CLOB,  -- segment 300MB
);

-- 当前逻辑: 每个LOB单独检查
-- DOC1: 300MB < 512MB ✅
-- DOC2: 300MB < 512MB ✅

-- 实际: 表总LOB大小 600MB > 512MB
-- 应该标记为LOB_OVERSIZE？
```

---

## 六、关键测试用例设计

### 6.1 Remap推导测试

```python
class TestRemapBusinessLogic(unittest.TestCase):
    """业务逻辑测试: Remap推导"""
    
    def test_sequence_source_only_with_parent_table(self):
        """SEQUENCE source_only策略下，即使有父表映射也保持原schema"""
        remap_rules = {"A.T1": "B.T1"}
        object_parent_map = {"A.SEQ1": "A.T1"}
        
        target = resolve_remap_target(
            "A.SEQ1", "SEQUENCE", remap_rules,
            object_parent_map=object_parent_map,
            sequence_remap_policy="source_only"
        )
        
        assert target == "A.SEQ1", "source_only应保持原schema"
    
    def test_sequence_infer_follows_parent_table(self):
        """SEQUENCE infer策略下，跟随父表remap"""
        remap_rules = {"A.T1": "B.T1"}
        object_parent_map = {"A.SEQ1": "A.T1"}
        
        target = resolve_remap_target(
            "A.SEQ1", "SEQUENCE", remap_rules,
            object_parent_map=object_parent_map,
            sequence_remap_policy="infer"
        )
        
        assert target == "B.SEQ1", "infer应跟随父表"
    
    def test_trigger_cross_schema_reference(self):
        """触发器跨schema引用: 表remap后触发器保持原schema"""
        remap_rules = {"A.T1": "B.T1"}
        object_parent_map = {"A.TRG1": "A.T1"}
        
        # 触发器保持A schema
        target = resolve_remap_target(
            "A.TRG1", "TRIGGER", remap_rules,
            object_parent_map=object_parent_map
        )
        assert target == "A.TRG1"
        
        # TODO: 验证DDL中表引用被正确调整为B.T1
    
    def test_circular_dependency_detection(self):
        """循环依赖检测"""
        deps = {
            ("A", "V1", "VIEW", "A", "V2", "VIEW"),
            ("A", "V2", "VIEW", "A", "V1", "VIEW"),
        }
        graph = build_dependency_graph(deps)
        remap_conflicts = {}
        
        target = resolve_remap_target(
            "A.V1", "VIEW", {},
            dependency_graph=graph,
            source_dependencies=deps,
            remap_conflicts=remap_conflicts
        )
        
        # 应该检测到循环并记录
        assert ("A.V1", "VIEW") in remap_conflicts
        assert "循环" in remap_conflicts[("A.V1", "VIEW")]
    
    def test_many_to_one_conflict_error(self):
        """多对一映射冲突应该报错"""
        remap_rules = {
            "A.T1": "C.T1",
            "B.T1": "C.T1",  # 冲突
        }
        source_objects = {
            "A.T1": {"TABLE"},
            "B.T1": {"TABLE"},
        }
        
        # 应该抛出异常或记录冲突
        with self.assertRaises(ValueError):
            generate_master_list(source_objects, remap_rules)
```

### 6.2 VARCHAR长度测试

```python
class TestVarcharLengthComparison(unittest.TestCase):
    """VARCHAR长度对比逻辑测试"""
    
    def test_char_vs_byte_semantics(self):
        """CHAR vs BYTE语义对比"""
        src_info = {
            "data_type": "VARCHAR2",
            "char_length": 100,
            "char_used": "C",  # 字符语义
        }
        tgt_info = {
            "data_type": "VARCHAR2",
            "data_length": 300,  # 字节语义
            "char_used": "B",
        }
        
        # 100 CHAR需要至少300 BYTE (UTF8)
        # 应该通过检查
        result = compare_varchar_columns(src_info, tgt_info)
        assert result["status"] == "OK"
    
    def test_char_length_float_parsing(self):
        """char_length浮点数解析"""
        src_info = {
            "data_type": "VARCHAR2",
            "char_length": "100.5",  # 浮点数字符串
        }
        
        # 应该正确解析为100
        length = safe_parse_int(src_info["char_length"])
        assert length == 100
```

### 6.3 依赖分析测试

```python
class TestDependencyAnalysis(unittest.TestCase):
    """依赖分析逻辑测试"""
    
    def test_deep_dependency_chain(self):
        """深度依赖链性能测试"""
        # 构造100层依赖链
        deps = set()
        for i in range(99):
            deps.add(("A", f"V{i}", "VIEW", "A", f"V{i+1}", "VIEW"))
        deps.add(("A", "V99", "VIEW", "A", "T1", "TABLE"))
        
        graph = build_dependency_graph(deps)
        cache = precompute_transitive_table_cache(graph)
        
        # 应该能正确计算
        assert cache[("A.V0", "VIEW")] == {"A.T1"}
        
        # 性能: 应该在合理时间内完成
        import time
        start = time.time()
        cache = precompute_transitive_table_cache(graph)
        duration = time.time() - start
        assert duration < 1.0, f"性能问题: {duration}秒"
```

---

## 七、修复优先级

### P0 - 立即修复
1. **VARCHAR CHAR/BYTE语义混淆** - 可能导致数据截断
2. **多对一映射静默回退** - 应该报错而不是回退
3. **char_length浮点数解析** - 数据丢失

### P1 - 高优先级
4. **SEQUENCE推导逻辑重复检查** - 代码混乱
5. **循环依赖无警告** - 用户无感知
6. **系统权限隐含关系不完整** - 授权遗漏

### P2 - 中优先级
7. **列顺序检查缺失** - 可选功能
8. **深度依赖链限制** - 性能优化
9. **LOB大小检测逻辑** - 边界情况

---

## 八、总结

这次审查发现了**6个业务逻辑问题**，其中：
- 🔴 **3个高危**: 可能导致数据问题或配置错误
- ⚠️ **3个中危**: 影响功能正确性

**关键发现**:
1. VARCHAR长度对比未考虑CHAR/BYTE语义差异
2. 多对一映射冲突处理不当
3. SEQUENCE推导逻辑存在冗余代码

**下一步**: 
1. 补充完整的业务逻辑测试用例
2. 修复高危问题
3. 添加更多边界情况测试
