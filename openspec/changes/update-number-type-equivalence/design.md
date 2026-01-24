## Context
Oracle 与 OceanBase 在 NUMBER 元数据表达上存在差异：同样的语义可能被存储为不同的 precision/scale 组合，尤其是 `NUMBER(*)`、`NUMBER(*,s)`、`NUMBER(p)` 等等价写法会被误判为不一致。现有逻辑只覆盖了 `NUMBER(*,0)` → `NUMBER(38,0)`。

## Goals / Non-Goals
- Goals:
  - 将 NUMBER/DECIMAL/NUMERIC 的等价写法统一规范化，避免误报。
  - 比较与 fixup 生成不再对等价写法产生差异或修补语句。
  - 通过 Oracle 19c 与 OB 4.2.5.7 的实测矩阵验证规则正确。
- Non-Goals:
  - 不引入新的配置开关（保持行为可预测）。
  - 不改变非 NUMBER 类列的比较策略。

## Decisions
- Decision: 引入 NUMBER 归一化签名（NumberSignature），统一类型别名与缺省精度/标度。
  - NUMBER/DECIMAL/NUMERIC → NUMBER
  - precision 缺省且 scale 缺省 → 视为“unbounded”
  - precision 缺省但 scale 存在 → 视为 `precision=38, scale=scale`（对应 `NUMBER(*,s)`）
  - precision 存在且 scale 缺省 → 视为 `scale=0`（对应 `NUMBER(p)`）
- Decision: 以归一化签名进行比较，等价写法视为一致，不生成 number_precision fixup。
- Decision: 在 DDL/展示时避免输出无效的 `NUMBER(scale)`，对 `precision=None, scale!=None` 统一输出 `NUMBER(38,scale)`（或保留 `NUMBER(*,scale)` 的等价形式，具体实现以兼容性测试为准）。

## Risks / Trade-offs
- 风险: “unbounded” 与 `NUMBER(38,0)` 语义并非完全相同，过度等价可能掩盖真实缩窄。
  - 缓解: 在 compatibility_suite 中验证 OB 对 `NUMBER` 的元数据与插入行为；如果 OB 将 `NUMBER` 表达为 `38,0` 但仍允许小数，则可安全等价。
- 风险: DDL 输出规范化可能影响用户预期的字面格式。
  - 缓解: 规范化仅用于比较与 fixup 生成，不改变源端真实 DDL。

## Migration Plan
- 实测 Oracle/OB NUMBER 元数据矩阵，确认等价规则。
- 逐步替换 NUMBER 比较逻辑并补充单元测试。
- 更新兼容性测试脚本并记录结果。

## Open Questions
- OB 对 `NUMBER`（无 precision/scale）是否返回 `NULL/NULL` 还是 `38/0`？
- 若 OB 返回 `38/0`，是否应将其视为 unbounded 等价（推荐根据实测结论决定）。
