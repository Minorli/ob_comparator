## ADDED Requirements

### Requirement: Python 3.7 Runtime Compatibility

主程序 MUST 保持 Python 3.7 运行时可启动，不得在模块导入或类定义阶段使用
Python 3.9+ 才支持的内建泛型下标写法。

#### Scenario: ObMetadata annotations remain Python 3.7 compatible

- **WHEN** `schema_diff_reconciler.py` 在 Python 3.7 运行
- **THEN** `ObMetadata` 类定义 MUST NOT 因 `frozenset[...]` 之类写法触发
  `TypeError: 'type' object is not subscriptable`

#### Scenario: Typing aliases are used for generic annotations

- **WHEN** 项目需要声明集合、映射、序列等泛型类型
- **THEN** 应使用 `typing` 中的兼容类型别名
- **AND** 项目代码 MUST NOT 引入 `list[...]`、`dict[...]`、`set[...]`、
  `frozenset[...]`、`tuple[...]` 这类 Python 3.9+ 运行时写法
