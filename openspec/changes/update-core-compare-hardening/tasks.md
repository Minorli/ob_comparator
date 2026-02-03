## 1. Implementation
- [ ] 1.1 引入 PUBLIC/__public 归一化工具函数，并在 OB 元数据入口统一归一
- [ ] 1.2 比对与报告层统一展示 PUBLIC（不暴露 __public）
- [ ] 1.3 同义词解析/依赖推导与 remap 使用 PUBLIC 逻辑 owner
- [ ] 1.4 FK 约束比对补齐 update_rule 字段与 mismatch 说明
- [ ] 1.5 obclient SQL 执行改为 stdin 方式（保留 timeout 与 stderr 诊断）
- [ ] 1.6 sys.exit 替换为异常链路，并在顶层统一捕获与退出
- [ ] 1.7 增加视图别名替换回归测试（单测 + 真实库验证）

## 2. Verification (DB)
- [ ] 2.1 Oracle PUBLIC 同义词 + OB __public 对照验证
- [ ] 2.2 PUBLIC + 私有同义词链路依赖验证
- [ ] 2.3 remap + PUBLIC 同义词映射验证
- [ ] 2.4 FK update_rule 差异场景验证
- [ ] 2.5 obclient 特殊字符 SQL 执行稳定性验证
- [ ] 2.6 视图别名冲突场景真实库验证

## 3. Tests & Docs
- [ ] 3.1 单元测试：PUBLIC owner 归一化
- [ ] 3.2 单元测试：PUBLIC 同义词匹配与映射
- [ ] 3.3 单元测试：FK update_rule 比对
- [ ] 3.4 单元测试：视图别名替换不误伤
- [ ] 3.5 更新相关说明文档（仅限 PUBLIC 语义变化与 FK update_rule 说明）
