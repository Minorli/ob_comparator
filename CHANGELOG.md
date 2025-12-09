# Changelog

All notable changes to OceanBase Comparator Toolkit will be documented in this file.

## [0.8.1] - 2025-12-09

### Fixed
- **性能问题**：修复fixup脚本生成阶段显示错误的耗时
  - 问题：每个对象显示20-30秒，但实际已从缓存加载
  - 原因：dbcat批次总耗时被记录到每个对象
  - 修复：将批次总耗时平均分配给批次中的每个对象
  
### Changed
- **日志优化**：
  - 缓存加载时使用实际读取耗时（通常<0.01秒）
  - 只在非缓存或耗时较长（>0.1秒）时输出详细日志
  - 减少日志噪音，提升可读性

### Added
- **性能调优文档**：新增 `PERFORMANCE_TUNING.md`
  - 详细说明性能问题的根本原因
  - 提供完整的性能调优建议
  - 包含故障排查和最佳实践

### Technical Details
- 修改 `_run_dbcat_chunk()` 函数，计算平均耗时
- 修改 `fetch_ddl_with_timing()` 函数，区分缓存和运行耗时
- 优化日志输出条件

---

## [0.8] - 2025-12-09

### Added
- **智能Schema推导**：基于依赖分析的一对多映射自动推导
  - 分析对象引用的表，选择出现次数最多的目标schema
  - 支持VIEW/PROCEDURE/FUNCTION/PACKAGE等独立对象的智能推导
  - 输出详细的推导日志 `[推导]`
  
- **多对一映射序列比对修复**：
  - 修复了多对一场景下错误报告"多余序列"的bug
  - 正确处理多个源schema映射到同一目标schema的情况

- **DDL对象名替换增强**：
  - 修复了PACKAGE/PROCEDURE等对象的END语句名称替换
  - 正确处理主对象的裸名引用（不带schema前缀）

- **一对多场景警告**：
  - 自动检测一对多映射场景并输出警告
  - 提示用户哪些对象需要显式配置

### Changed
- **Remap推导逻辑优化**：
  - 优先级：显式规则 > 依赖分析 > schema映射
  - 支持所有映射场景（多对一、一对一、一对多）

- **文档完善**：
  - 新增 `REMAP_INFERENCE_GUIDE.md` 详细说明推导能力
  - 更新 README.md 反映最新功能
  - 新增 `AUDIT_REPORT.md` 程序一致性审核报告

### Fixed
- 修复序列比对在多对一映射场景下的误报
- 修复DDL中主对象名的裸名替换问题
- 修复依赖关系字段名错误（`type` → `object_type`）

### Technical Details
- 新增 `infer_target_schema_from_dependencies()` 函数
- 更新 `resolve_remap_target()` 支持依赖分析参数
- 更新 `build_schema_mapping()` 输出一对多警告
- 更新 `adjust_ddl_for_object()` 处理主对象裸名

---

## [0.7] - 2025-12-08

### Added
- 表/列注释一致性检查
- 依赖关系校验和重编译脚本生成
- 跨schema授权建议（GRANT脚本）
- TABLE列长度校验和ALTER修补

### Changed
- 采用"一次转储，本地对比"架构
- 批量查询DBA_*视图，避免循环调用
- dbcat缓存复用机制

### Fixed
- 性能优化：减少数据库调用次数
- 内存优化：本地数据结构对比

---

## [0.6] - 2025-12-07

### Added
- 基础对象类型检测（TABLE/VIEW/PROCEDURE/FUNCTION等）
- Remap规则验证
- 基础修补脚本生成

### Changed
- 初始版本架构设计

---

## Future Roadmap

### Planned for 0.9
- [ ] 性能监控和统计
- [ ] 更详细的差异报告（列类型、默认值等）
- [ ] 支持更多数据库对象类型（DIRECTORY、DB_LINK等）
- [ ] 交互式修补脚本审核工具

### Planned for 1.0
- [ ] GUI界面
- [ ] 配置模板管理
- [ ] 批量场景支持
- [ ] 完整的单元测试覆盖

---

## Version Numbering

版本号格式：`MAJOR.MINOR`

- **MAJOR**: 重大架构变更或不兼容更新
- **MINOR**: 新功能添加或重要bug修复
