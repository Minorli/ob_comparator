## 1. Implementation
- [x] 1.1 添加统一标识符引号工具（quote_identifier/quote_qualified/normalize_qualified）
- [x] 1.2 修复触发器 CREATE/ON 及触发器体内 remap 引用的引号输出
- [x] 1.3 修复 VIEW/SYNONYM/FK REFERENCES/ALTER/DROP 等 DDL 输出的引号格式
- [x] 1.4 修正 CHECK 约束比对：命中名称即视为存在，仅记录表达式差异
- [x] 1.5 覆盖单元测试：触发器/视图/同义词/外键/约束比对
- [x] 1.6 在 Oracle+OB 实测 DDL 可执行性并更新测试报告
