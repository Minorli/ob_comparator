## 1. Analysis
- [x] 复核触发器 DDL 生成与 remap 逻辑，定义 schema 补全的覆盖范围与风险点
- [x] 盘点序列 remap 推导链路与依赖来源，确认可切换策略
- [x] 梳理索引缺失统计与 fixup 输出差异来源，拟定跳过原因分类
- [x] 识别修补阶段的配置冲突组合与现有 hardcode 行为清单

## 2. Implementation
- [x] 实现触发器 DDL 的 schema 前缀补全与 remap 重写（含 ON 子句与触发器体）
- [x] 新增序列 remap 策略开关并接入推导/校验/修补逻辑
- [x] 增加索引修补“跳过原因”统计与报告输出
- [x] tables_views_miss 更名为 missed_tables_views_for_OMS，并同步提示文本
- [x] 为每次执行生成 report_dir 子目录并重定向所有报告输出
- [x] 增加修补阶段的配置冲突/重复逻辑告警

## 3. Tests & Docs
- [x] 补充触发器引用补全、序列策略、索引跳过统计的单元测试
- [x] 更新 config.ini.template、readme_config.txt、README.md 与 changelog
