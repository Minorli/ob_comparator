## 1. Implementation
- [ ] 1.1 读取现有 missing/unsupported 明细行，按 TYPE 划分为 missing/unsupported 两类
- [ ] 1.2 生成 per-type 报告文件并写入 report_index
- [ ] 1.3 主报告新增说明段落，指出 per-type 文件路径与用途
- [ ] 1.4 文档更新：readme_config.txt + docs 报告说明

## 2. Tests
- [ ] 2.1 单测：输入混合类型缺失/不支持行，输出对应文件且字段头正确
- [ ] 2.2 单测：report_detail_mode!=split 时不输出 per-type 文件
- [ ] 2.3 单测：index/constraint/triggers 的 DETAIL 字段包含 key=value 结构
