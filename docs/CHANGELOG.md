# Changelog

All notable changes to OceanBase Comparator Toolkit will be documented in this file.

## [Unreleased]

### Documentation
- 全面更新 README 与文档细节（Remap 规则、冲突处理、run_fixup 高级用法、部署与配置说明）。

## [0.9.2] - 2026-01-05

### Changed
- 触发器/视图默认保持源 schema（仅显式 remap 才改变），触发器脚本附带跨 schema 授权。
- `check_primary_types`/`check_extra_types` 贯穿 remap 推导、依赖校验与元数据加载范围。
- MATERIALIZED VIEW 与 PACKAGE/PACKAGE BODY 默认仅打印不校验，避免 OB 不支持/默认跳过。
- 无法自动推导的对象会单独汇总并在报告中提示，避免误回退。

## [0.9.0] - 2025-12-23

### Security & Reliability
- **DDL 重写引擎重构**: 引入 `SqlMasker`，彻底解决正则替换时误伤字符串/注释的风险。
- **视图依赖解析升级**: 新增 Token 级解析器，完美支持 `FROM A, B` 等复杂 SQL 语法的表依赖提取。
- **PL/SQL 智能推导**: 增强 `remap_plsql_object_references`，支持本地未限定引用的自动 Schema 补全。

### Documentation
- **文档重构**: 整合分散的 markdown 文档为 `ADVANCED_USAGE.md`、`DEPLOYMENT.md`、`ARCHITECTURE.md`。

## [0.8.8] - 2025-12-11

### Changed
- 全局版本号更新为 `0.8.8`，README/DESIGN/发布文档同步。
- PUBLIC 同义词默认用元数据批量获取，过滤仅保留指向 `source_schemas` 的目标，FOR 子句按 remap 重写并清理 NONEDITIONABLE。
- 触发器 DDL 的 ON 子句使用 remap 后的表名，修补脚本文件名与内容保持一致。
- PL/SQL fixup 清理补充：移除紧邻 `/` 之前的单独分号行，避免多余 `;/` 组合。

### Fixed
- OceanBase 索引列获取不再虚构 `UNKNOWN` 索引：只有 DBA_INDEXES 中存在的索引才追加列，防止误报唯一性/缺失。
- PUBLIC 同义词 DDL 生成不会给对象名重复加 `PUBLIC.` 前缀。

## [0.8.1] - 2025-12-10

### Fixed
- **修复 "too many values to unpack (expected 2)" 错误**：
  - 问题：生产环境中对象名包含多个点号（如 `SCHEMA.PACKAGE.PROCEDURE`）导致 `split('.')` 返回超过2个元素
  - 影响：VIEW 处理和 OTHER_OBJECTS 任务在大量对象时崩溃
  - 修复方案：
    - ✅ 所有 `split('.')` 改为 `split('.', 1)` 确保只分割成2部分
    - ✅ 添加 `try-except ValueError` 防御性代码捕获异常
    - ✅ 添加长度检查 `if len(parts) != 2` 作为额外保护
  - 涉及位置：
    - `get_relevant_replacements()` 函数（第5938-5939行）
    - 缺失对象处理（第6135-6136行）
    - Schema映射处理（第1490-1491行、第1547行）
    - 依赖对象处理（第6904-6907行）
    - 表对比处理（第2860-2866行、第3402-3408行、第3535-3541行）

- **修复 OceanBase 版本检测失败**：
  - 问题：使用 `SELECT VERSION()` 在 OceanBase Oracle 模式下报错
  - 影响：无法获取版本号，导致 VIEW DDL 清理策略失效
  - 修复：改用 `SELECT OB_VERSION() FROM DUAL` 并修正版本号解析逻辑
  - 涉及函数：`get_oceanbase_version()`, `get_oceanbase_info()`

- **移除不必要的元数据缺失警告**：
  - 问题：当源端 Oracle 表无索引/约束元数据时，打印大量警告信息
  - 影响：日志噪音过多，影响问题定位
  - 修复：移除 "源端 Oracle 该表无索引元数据" 和 "源端 Oracle 该表无约束元数据" 警告
  - 原因：这些情况是正常的（表可能确实没有索引/约束），不应作为警告

- **修复同名索引但 SYS_NC 列名不同的误报**：
  - 问题：Oracle 和 OceanBase 对隐藏列命名不同（如 `SYS_NC00023$` vs `SYS_NC38$`）
  - 影响：同一个索引被同时报告为"缺失"和"多余"
  - 修复：添加 SYS_NC 列名标准化逻辑，识别同名索引并从告警中剔除
  - 实现：
    - `normalize_sys_nc_columns()`: 将 SYS_NC 列标准化为通用形式
    - `has_same_named_index()`: 检查是否存在同名索引
    - `is_sys_nc_only_diff()`: 检查是否仅 SYS_NC 列名不同

- **增强 OMS 索引过滤逻辑**：
  - 问题：原逻辑要求索引列精确匹配4个 OMS 列，过于严格
  - 影响：包含额外业务列的 OMS 索引无法被正确识别
  - 修复：改为检查索引名以 `_OMS_ROWID` 结尾且包含所有4个 OMS 列作为子集
  - 函数：`is_oms_index()`

- **修复 `non_view_missing_objects` 变量作用域错误**：
  - 问题：变量在使用前未定义，导致 `UnboundLocalError`
  - 影响：修补脚本生成失败
  - 修复：将 VIEW/非VIEW 对象分离逻辑移到使用前执行

### Added
- **防御性错误处理**：
  - 在所有 `fetch_ddl_with_timing()` 调用处添加返回值长度检查
  - 在所有 `split()` 操作处添加 `try-except ValueError` 保护
  - 详细记录异常信息便于问题定位

- **IOT 表过滤**：
  - 自动跳过 `SYS_IOT_OVER_*` 开头的 IOT 溢出表
  - 避免这些系统表参与对比和修补脚本生成
  - 在日志中统计跳过的 IOT 表数量

- **注释标准化增强**：
  - 去除控制字符（`\x00-\x1f\x7f`）
  - 识别并过滤 `NULL`/`<NULL>`/`NONE` 等无效注释
  - 函数：`normalize_comment_text()`

### Changed
- **并发处理优化**：
  - 添加 `fixup_workers` 配置项，默认使用 CPU 核心数（最多12）
  - 添加 `progress_log_interval` 配置项，控制进度日志输出频率
  - 使用 `ThreadPoolExecutor` 并发生成修补脚本

- **报告宽度配置**：
  - 添加 `report_width` 配置项（默认160），避免 nohup 时被截断为80列
  - 确保报告在后台运行时也能完整显示

### Technical Details
- 版本号更新为 `0.8.1`
- 添加 `__version__` 和 `__author__` 元数据
- 导入 `threading`, `json`, `time`, `concurrent.futures` 模块
- 添加 `DBCAT_DIR_TO_TYPE` 反向映射字典

---

## [0.8.5] - 2025-12-09

### Changed
- **重构对象推导逻辑，完全依赖DBA_DEPENDENCIES**：
  - 问题：v0.8.4的 `get_object_parent_tables()` 方法仍然有局限性，只处理特定对象类型
  - 根本原因：所有对象（VIEW/PROCEDURE/FUNCTION/PACKAGE/TRIGGER/SYNONYM等）的DDL中都可能引用表
  - 解决方案：废弃 `object_parent_map`，完全依赖 `DBA_DEPENDENCIES` 进行推导
  - 优势：
    - ✅ 覆盖所有对象类型（不再有遗漏）
    - ✅ 基于实际的依赖关系（更准确）
    - ✅ 代码更简洁（减少冗余逻辑）

### Deprecated
- **`get_object_parent_tables()` 函数已废弃**：
  - 保留函数签名用于向后兼容
  - 实际返回空字典，不再执行查询
  - 推导逻辑现在完全依赖 `infer_target_schema_from_dependencies()`

### Technical Details
- 简化 `resolve_remap_target()` 函数：
  - 移除对 `object_parent_map` 的依赖
  - 推导顺序：显式remap规则 → 依赖分析推导 → schema映射推导
  - 依赖分析推导现在应用于所有非TABLE对象
- `DBA_DEPENDENCIES` 已经包含了所有对象对表的引用关系
- DDL中的表名替换由 `adjust_ddl_for_object()` 统一处理

### Benefits
- **更全面的覆盖**：所有对象类型都能正确推导，包括：
  - VIEW（查询中的表）
  - PROCEDURE/FUNCTION/PACKAGE（代码中的表）
  - TRIGGER（触发的表和代码中的表）
  - SYNONYM（指向的对象）
  - TYPE/TYPE BODY（可能引用的表）
- **更准确的推导**：基于Oracle的依赖关系元数据，而不是手工查询
- **更简洁的代码**：减少了100+行冗余代码

---

## [0.8.4] - 2025-12-09

### Fixed
- **依附对象父表映射不完整**：
  - 问题：`get_object_parent_tables()` 只处理TRIGGER，导致INDEX/CONSTRAINT/SEQUENCE无法跟随父表的remap
  - 影响：在一对多场景下（如 MONSTER_A → TITAN_A + TITAN_B），这些对象无法正确推导目标schema
  - 修复：扩展函数以处理所有依附对象：
    - ✅ TRIGGER: 通过 DBA_TRIGGERS 查询父表
    - ✅ INDEX: 通过 DBA_INDEXES 查询父表
    - ✅ CONSTRAINT: 通过 DBA_CONSTRAINTS 查询父表
    - ✅ SEQUENCE: 通过分析触发器代码中的 .NEXTVAL 引用推断父表

### Enhanced
- **SEQUENCE智能推导**：
  - 分析触发器代码中的序列使用（如 SEQ_NAME.NEXTVAL）
  - 将序列关联到使用它的表
  - 支持带schema前缀和不带schema前缀的序列引用

### Technical Details
- 修改 `get_object_parent_tables()` 函数：
  - 添加 DBA_INDEXES 查询获取索引的父表
  - 添加 DBA_CONSTRAINTS 查询获取约束的父表
  - 添加触发器代码分析获取序列的父表
  - 使用正则表达式匹配 `SCHEMA.SEQ_NAME.NEXTVAL` 模式

---

## [0.8.3] - 2025-12-09

### Fixed
- **约束和索引统计错误**：
  - 问题：检查汇总中显示Oracle有63个约束，OceanBase有97个约束，数量不匹配
  - 原因：统计时Oracle只统计remap规则中涉及的表的约束，而OceanBase统计了目标schema下所有表的约束
  - 修复：修改 `compute_object_counts()` 函数，确保两端都只统计remap规则中涉及的表的约束和索引
  - 影响：INDEX统计也应用了相同的修复逻辑

### Technical Details
- 修改 `compute_object_counts()` 函数：
  - 从 `full_object_mapping` 中提取所有涉及TABLE的源表和目标表
  - 统计约束/索引时，只统计这些表的约束/索引
  - 确保Oracle和OceanBase使用相同的过滤逻辑

---

## [0.8.2] - 2025-12-09

### Added
- **索引和约束命名冲突检测与自动解决**：
  - 自动检测同一目标schema下的索引/约束名称冲突
  - **智能识别重命名表**：
    - 自动检测目标端的重命名表（如 `ORDERS_RENAME_20251118`）
    - 识别其索引/约束与即将创建的原表名冲突
    - 支持多种重命名模式：RENAME/BACKUP/BAK/OLD/HIST/HISTORY/ARCHIVE/ARC/TMP/TEMP
    - 支持有无下划线的格式（`_RENAME_20251118` 或 `_RENAME20251118`）
    - 支持多种日期格式：YYYYMMDD(8位)、YYMMDD(6位)、YYMM(4位)
  - 智能重命名策略：
    - 优先提取表名中的重命名后缀（关键词+日期）
    - 否则使用表名后缀作为区分标识
    - 确保新名称不超过30字符限制
    - 如仍冲突则添加数字后缀
  - 应用场景：
    - 多个源schema的不同表remap到同一目标schema
    - 表被重命名后原表重建（**核心场景**）
    - 目标端已存在同名索引/约束
  - 自动重命名CREATE TABLE中内联的约束
  - 自动重命名独立的CREATE INDEX和ALTER TABLE ADD CONSTRAINT语句
  - 详细日志输出冲突检测和重命名信息

### Changed
- **修补脚本生成增强**：
  - 在生成INDEX/CONSTRAINT脚本前执行冲突检测
  - 为冲突对象生成带重命名标记的脚本文件
  - 脚本头部注释说明原名和重命名原因
  - 特别标注"来自重命名表"的冲突

### Technical Details
- 新增 `extract_table_suffix_for_renaming()` 函数：提取表名中的重命名后缀，支持多种模式
- 增强 `detect_naming_conflicts()` 函数：
  - 识别重命名表并提取原始表名
  - 检测重命名表的索引/约束与即将创建的表冲突
  - 输出详细的冲突来源信息
- 增强 `generate_conflict_free_name()` 函数：使用提取的重命名后缀生成新名称
- 新增 `rename_embedded_constraints_indexes()` 函数：重命名CREATE TABLE DDL中的内联约束/索引
- 修改INDEX和CONSTRAINT生成逻辑，应用重命名映射
- 修改TABLE生成逻辑，处理内联约束/索引的重命名

---

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
