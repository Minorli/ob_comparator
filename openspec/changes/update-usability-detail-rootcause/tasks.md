## 1. Implementation
- [x] 1.1 调整可用性错误分类文案："视图编译错误" -> "视图查询报错"
- [x] 1.2 在可用性检查结果中补充 root_cause/recommendation 生成逻辑（支持对象/权限/依赖提示）
- [x] 1.3 使用依赖图/依赖数据提取第一层依赖对象
- [x] 1.4 将依赖对象的缺失/不支持/黑名单状态写入 usability_check_detail
- [x] 1.5 对 synonym 可用性错误同样输出精确原因（对象不存在/权限不足/链路指向不支持对象）

## 2. Tests
- [ ] 2.1 构造视图依赖缺失表的用例，验证 root_cause 包含缺失对象
- [ ] 2.2 构造权限不足的视图用例，验证 recommendation 指向 GRANT
- [ ] 2.3 构造不支持视图的用例，验证 SKIPPED 且理由更精确

## 3. Validation
- [x] 3.1 完成一次全量运行，确保 usability_check_detail 输出正确
- [x] 3.2 文档更新（readme_config / 报告说明）
