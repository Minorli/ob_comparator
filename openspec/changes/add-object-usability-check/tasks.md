## 1. Implementation
- [x] 1.1 新增可用性校验开关、超时、并发、采样等配置解析与校验
- [x] 1.2 在对比流程中接入 VIEW/SYNONYM 可用性校验（目标端为主，可选源端对照）
- [x] 1.3 设计并输出可用性明细报告（| 分隔）及汇总统计
- [x] 1.4 在主报告中新增“可用性统计”区块
- [x] 1.5 根因归类与推荐建议（常见 ORA 错误码映射）

## 2. Tests
- [x] 2.1 单元测试：可用性状态判定、超时处理、采样逻辑
- [ ] 2.2 集成测试：VIEW/SYNONYM 可用性结果与报告输出格式

## 3. Docs
- [x] 3.1 更新 readme_config.txt（新增开关与参数说明）
- [x] 3.2 更新 docs/TECHNICAL_SPECIFICATION.md（对比规则）
- [x] 3.3 更新 docs/CHANGELOG.md
