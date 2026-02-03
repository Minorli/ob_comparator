## Context
当前比较与修复体系中，PUBLIC 同义词在 Oracle 与 OceanBase 上 owner 表现不一致（Oracle=PUBLIC，OB=__public），且 FK update_rule 未纳入比对；另外视图依赖重写存在别名误替换风险，obclient -e 传参在特殊字符场景稳定性不足。需要在不改变整体逻辑的前提下进行硬化修复与验证。

## Goals
- 统一 PUBLIC / __public 语义，修复由 owner 表达差异引发的误报/漏报。
- FK 比对覆盖 update_rule，防止行为差异未被发现。
- 视图依赖替换逻辑明确不替换别名，并有回归用例。
- obclient 执行稳定性提升，避免特殊字符 SQL 失败。
- 并发错误收敛：sys.exit 统一为异常链路。

## Non-Goals
- 不修改触发器相关逻辑。
- 不新增或改变用户配置开关。
- 不调整 fixup 选择策略（除 PUBLIC/ __public 误判修正）。

## Decisions
### 1) PUBLIC / __public 归一化
- **Decision**: 内存逻辑 owner 统一为 PUBLIC；执行层保留真实 owner 值。
- **Rationale**: 用户报告与 remap 规则均基于 Oracle 语义，报告与比较应一致。

### 2) obclient SQL 传参改 stdin
- **Decision**: 将 SQL 通过 stdin 传给 obclient，避免 -e 参数对特殊字符敏感。
- **Rationale**: 降低报错概率，且不改变功能逻辑。

### 3) FK update_rule 比对
- **Decision**: 采集/比对 update_rule，与 delete_rule 同级处理。
- **Rationale**: 影响外键行为一致性。

### 4) 视图别名替换
- **Decision**: 替换仅限 FROM/JOIN 段首 token 的对象名，避免别名误替换。
- **Rationale**: 已出现“别名被替换为 schema.alias”问题。

### 5) sys.exit 改异常链路
- **Decision**: 所有致命错误抛出异常，由顶层统一捕获退出。
- **Rationale**: 并发任务中 sys.exit 会导致非预期退出与日志丢失。

## Risks / Trade-offs
- PUBLIC 归一化涉及大量 key 归一化，需确保不影响非 PUBLIC schema。
- obclient stdin 方式可能影响 stdout/stderr 解析，需回归确认。

## Validation Plan (Oracle + OB)
### A. PUBLIC / __public
1. Oracle: 创建 A.T1 与 PUBLIC SYNONYM T1 FOR A.T1
2. OB: 确认同义词 owner 为 __public
3. 运行对比：报告中不出现 __public，差异为 0

### B. PUBLIC + 私有链路
1. Oracle: B.PSYN -> A.T1, PUBLIC SYNONYM T1 -> B.PSYN
2. 运行对比：依赖解析最终定位 A.T1，且不生成重复 fixup

### C. PUBLIC + remap
1. remap A.* -> A_NEW.*
2. PUBLIC SYNONYM T1 -> A.T1
3. 运行对比：输出 DDL 指向 A_NEW.T1

### D. FK update_rule
1. Oracle: ON UPDATE CASCADE / NO ACTION 对比
2. OB: 若不支持，归入不支持报告；若支持，差异应被捕捉

### E. obclient 特殊字符 SQL
1. 组织含 " $ \\ ; 中文 的 SQL
2. -e 与 stdin 对比（最终用 stdin）

### F. 视图别名冲突
1. Oracle: 创建 LIFEDATA.T 与视图 FROM UWSDATA.POL_INFO T
2. fixup 视图 DDL 中不得出现 LIFEDATA.T 替换

## Test Plan (Unit/Regression)
- unit: normalize_owner_public
- unit: public_synonym_key_match
- unit: compare_constraints_update_rule
- unit: remap_view_dependencies_alias_safety
- regression: run full compare on mixed PUBLIC/remap schemas

## Migration Plan
- 仅代码更新，无配置变更。
- 回滚：恢复旧版本的 owner 处理与 obclient 执行方式。

## Open Questions
- 是否需要在报告中显式提示 OB 内部 owner 为 __public（默认不展示）
