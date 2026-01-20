# 业务逻辑测试用例设计

基于业务逻辑审查，设计以下测试用例补充现有测试。

## 一、Remap推导测试用例

### 1. SEQUENCE策略测试
```python
def test_sequence_source_only_ignores_parent():
    """source_only策略下SEQUENCE保持原schema"""
    pass

def test_sequence_infer_follows_parent():
    """infer策略下SEQUENCE跟随父表"""
    pass

def test_sequence_dominant_table_strategy():
    """dominant_table策略测试"""
    pass
```

### 2. 循环依赖测试
```python
def test_circular_view_dependency():
    """VIEW循环依赖检测"""
    pass

def test_circular_procedure_dependency():
    """PROCEDURE循环依赖检测"""
    pass
```

### 3. 多对一冲突测试
```python
def test_many_to_one_explicit_conflict():
    """显式配置的多对一冲突应报错"""
    pass

def test_many_to_one_inferred_conflict():
    """推导产生的多对一冲突处理"""
    pass
```

## 二、VARCHAR长度测试用例

### 1. 语义对比测试
```python
def test_varchar_char_vs_byte():
    """CHAR vs BYTE语义对比"""
    pass

def test_varchar_char_length_float():
    """char_length浮点数解析"""
    pass
```

### 2. 长度校验测试
```python
def test_varchar_length_insufficient():
    """长度不足检测"""
    pass

def test_varchar_length_oversize():
    """长度过大检测"""
    pass
```

## 三、依赖分析测试用例

### 1. 深度依赖测试
```python
def test_deep_dependency_chain_100_levels():
    """100层依赖链性能测试"""
    pass

def test_dependency_with_missing_objects():
    """依赖对象缺失处理"""
    pass
```

### 2. 复杂依赖测试
```python
def test_diamond_dependency():
    """菱形依赖结构"""
    pass

def test_cross_schema_dependency():
    """跨schema依赖"""
    pass
```

## 四、授权管理测试用例

### 1. 权限隐含测试
```python
def test_select_any_table_implies_select():
    """SELECT ANY TABLE隐含SELECT权限"""
    pass

def test_all_system_privilege_implications():
    """所有系统权限隐含关系"""
    pass
```

### 2. 角色权限测试
```python
def test_role_privilege_inheritance():
    """角色权限继承"""
    pass

def test_nested_role_privileges():
    """嵌套角色权限"""
    pass
```

---

详细测试实现见项目test目录。
