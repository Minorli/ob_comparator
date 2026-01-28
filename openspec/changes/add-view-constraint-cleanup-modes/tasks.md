## 1. Implementation
- [ ] 1.1 新增配置 `view_constraint_cleanup`（auto/force/off），默认 auto
- [ ] 1.2 解析 VIEW 列清单中的 CONSTRAINT 子句并分类：可清洗/不可清洗
- [ ] 1.3 auto 模式：仅清除 RELY DISABLE / DISABLE / NOVALIDATE 子句
- [ ] 1.4 force 模式：无条件清除 CONSTRAINT 子句
- [ ] 1.5 off 模式：不清洗
- [ ] 1.6 生成清洗记录与无法清洗记录，并加入 report_index
- [ ] 1.7 主报告提示与 run summary/next_steps 更新

## 2. Tests
- [ ] 2.1 单测：auto 模式清洗 RELY DISABLE/ DISABLE / NOVALIDATE
- [ ] 2.2 单测：auto 模式遇 ENABLE => 标记不可清洗
- [ ] 2.3 单测：force 模式清洗 ENABLE
- [ ] 2.4 单测：off 模式不清洗且不生成清洗记录
- [ ] 2.5 报告输出：cleaned/uncleanable 文件与 report_index 条目
- [ ] 2.6 真实库验证（OB 4.2.5.7）：清洗后 DDL 可创建

## 3. Docs
- [ ] 3.1 config.ini.template / readme_config.txt 添加开关说明
- [ ] 3.2 主报告提示说明新增清洗/无法清洗明细
