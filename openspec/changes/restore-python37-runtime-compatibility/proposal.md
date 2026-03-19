## Why

生产环境仍存在 Python 3.7 运行器。当前 `schema_diff_reconciler.py` 中引入了
`frozenset[Tuple[str, str]]` 这种 PEP 585 写法，Python 3.7 在类定义阶段会直接报：

`TypeError: 'type' object is not subscriptable`

这会导致主程序在启动阶段崩溃，属于运行时兼容性回归。

## What Changes

- 恢复主程序对 Python 3.7 的运行时兼容。
- 禁止在项目代码中使用 Python 3.9+ 才支持的内建泛型下标写法。
- 继续使用 `typing` 中的兼容类型别名，例如 `FrozenSet[...]`、`List[...]`、`Dict[...]`。

## Impact

- 只影响 Python 运行时兼容性，不改变 compare、fixup、report 逻辑。
- 不引入新的配置项。
- 不改变数据库语义与输出口径。
