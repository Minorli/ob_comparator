# Release Notes — V0.9.8.2

发布日期：2026-02-03

## 亮点
- PUBLIC/`__public` 语义统一：报告与比对一律按 Oracle 语义展示 PUBLIC。
- FK 约束比对补齐 `UPDATE_RULE`。
- obclient SQL 改为 stdin 传入，特殊字符执行更稳定。
- 致命错误收敛为异常链路，避免并发任务直接 `sys.exit` 造成日志丢失。

## 新增
- VIEW/SYNONYM 可用性校验（可选）：目标端 `SELECT * FROM <obj> WHERE 1=2` 验证可用性，支持源端对照/超时/并发/抽样控制，并输出明细报告。
- VIEW 兼容规则增强：识别 X$ 系统对象引用并判定不支持（用户自建 X$ 对象除外）。
- VIEW 修补授权拆分：新增 `view_prereq_grants/`（依赖对象前置授权）与 `view_post_grants/`（创建后授权）。

## 变更
- VIEW DDL 清洗移除 `FORCE` 关键字，避免创建不可用视图。
- run_fixup 顺序加入 `view_prereq_grants/` 与 `view_post_grants/`。
- PUBLIC 同义词在 OB 侧 `__public` 归一化为 PUBLIC 展示。

## 修复
- FK 约束缺失 UPDATE_RULE 的比对遗漏。
- obclient `-e` 参数在特殊字符场景下的失败风险。
- 并发环境中 `sys.exit` 导致的非预期终止。

## 验证摘要
- Oracle/OB 实库验证：PUBLIC 同义词、链式同义词、VIEW 别名冲突、obclient 特殊字符、FK update_rule。
- 单元测试已覆盖 PUBLIC 归一化与 UPDATE_RULE 差异。

## 注意事项
- OB 侧实际 owner 为 `__public`，工具逻辑与报告统一展示 PUBLIC。
- OB 不支持 `ON UPDATE CASCADE`，若尝试创建将报错并回退为默认行为。
