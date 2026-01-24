## 1. Implementation
- [x] 1.1 实测 Oracle/OB NUMBER 元数据矩阵并记录结果（NUMBER, NUMBER(*), NUMBER(*,s), NUMBER(p), NUMBER(p,0), DECIMAL, NUMERIC）。
- [x] 1.2 实现 NUMBER 归一化签名与等价比较逻辑（替换现有 is_number_star_zero_equivalent 分支）。
- [x] 1.3 修正 NUMBER 类型的展示/输出逻辑（precision None + scale not None 的格式化）。
- [x] 1.4 更新 number_precision 的 fixup 生成，确保等价写法不生成修补语句。
- [x] 1.5 单元测试：覆盖等价写法与非等价写法。
- [x] 1.6 compatibility_suite：新增 NUMBER 等价矩阵脚本与 Oracle/OB 实测验证。
