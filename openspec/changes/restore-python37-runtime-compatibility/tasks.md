## 1. Implementation

- [x] 1.1 审计项目代码中的 Python 3.9+ 泛型注解写法
- [x] 1.2 将主程序中的不兼容写法改为 `typing` 兼容写法
- [x] 1.3 同步更新开发文档中的兼容约束

## 2. Verification

- [x] 2.1 运行 `openspec validate restore-python37-runtime-compatibility --strict`
- [x] 2.2 运行 `python3 -m py_compile $(git ls-files '*.py')`
- [x] 2.3 运行相关单元测试
- [x] 2.4 记录无法直接在本机执行 Python 3.7 解释器验证的限制
