# OceanBase Comparator Toolkit 文档目录

## 核心文档

### [CHANGELOG.md](CHANGELOG.md)
版本变更记录，包含所有版本的新增功能、修复和改进。

### [DESIGN.md](DESIGN.md)
设计和架构说明，解释程序的核心设计思路和实现原理。

## 功能指南

### [REMAP_INFERENCE_GUIDE.md](REMAP_INFERENCE_GUIDE.md)
Remap推导能力详细说明，涵盖：
- 多对一映射场景（HERO_A + HERO_B → OLYMPIAN_A）
- 一对一映射场景（GOD_A → PRIMORDIAL）
- 一对多映射场景（MONSTER_A → TITAN_A + TITAN_B）
- 智能schema推导逻辑

### [NAMING_CONFLICT_GUIDE.md](NAMING_CONFLICT_GUIDE.md)
索引和约束命名冲突检测与自动解决指南，包括：
- 冲突场景说明
- 自动重命名策略
- 使用示例和故障排查

### [RENAME_PATTERN_DETECTION.md](RENAME_PATTERN_DETECTION.md)
表重命名模式检测增强说明，支持多种重命名模式识别。

### [PERFORMANCE_TUNING.md](PERFORMANCE_TUNING.md)
性能调优指南，包含：
- 缓存加载优化
- 并行处理配置
- 磁盘I/O问题诊断

## 发布相关

### [RELEASE_SUMMARY.md](RELEASE_SUMMARY.md)
版本发布摘要，面向管理层的执行总结。

### [RELEASE_CHECKLIST.md](RELEASE_CHECKLIST.md)
发布前检查清单，确保版本质量。

### [AUDIT_REPORT.md](AUDIT_REPORT.md)
程序一致性审核报告，验证所有对象类型的完整性。

## 技术说明

### [V0.8.5_REFACTORING.md](V0.8.5_REFACTORING.md)
v0.8.5重构说明，完全依赖DBA_DEPENDENCIES的架构改进。

### [DEPENDENT_OBJECT_FIX.md](DEPENDENT_OBJECT_FIX.md)
依附对象父表映射修复说明（v0.8.4）。

### [CONSTRAINT_COUNT_FIX.md](CONSTRAINT_COUNT_FIX.md)
约束统计错误修复说明（v0.8.3）。

## 部署相关

### [README_CROSS_PLATFORM.md](README_CROSS_PLATFORM.md)
离线/跨平台wheelhouse打包与交付指南。

## 版本历史

- **v0.8.5** (2025-12-09): 重构对象推导逻辑，完全依赖DBA_DEPENDENCIES
- **v0.8.4** (2025-12-09): 扩展依附对象父表映射（已废弃）
- **v0.8.3** (2025-12-09): 修复约束和索引统计错误
- **v0.8.2** (2025-12-09): 索引和约束命名冲突检测与自动解决
- **v0.8.1** (2025-12-09): 性能问题修复和并行缓存加载
- **v0.8** (2025-12-09): 智能Schema推导和多对一映射支持

## 快速导航

**新用户**: 从主 [README.md](../README.md) 开始  
**了解Remap**: 阅读 [REMAP_INFERENCE_GUIDE.md](REMAP_INFERENCE_GUIDE.md)  
**性能优化**: 参考 [PERFORMANCE_TUNING.md](PERFORMANCE_TUNING.md)  
**版本变更**: 查看 [CHANGELOG.md](CHANGELOG.md)  
**架构设计**: 深入 [DESIGN.md](DESIGN.md)
