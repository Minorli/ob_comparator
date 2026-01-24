## 1. Implementation
- [x] 1.1 CHECK 表达式 normalization：折叠冗余括号（保留字符串字面量）。
- [x] 1.2 过滤 OB 派生 UNIQUE 约束（需证明源端存在等价 UNIQUE INDEX 或表达式索引）。
- [x] 1.3 DEFERRABLE/DEFERRED 的 PK/UK/FK/CHECK 标记为不支持并排除缺失。
- [x] 1.4 识别 DESC 索引为不支持（读取 DBA_IND_COLUMNS.DESCEND）。
- [x] 1.5 DDL 清洗移除 `USING INDEX <index_name>` 子句。
- [x] 1.6 单元测试覆盖 CHECK 括号、UNIQUE 约束派生、DESC 索引、DEFERRABLE 约束。
- [x] 1.7 Oracle/OB 实测并记录结果。
