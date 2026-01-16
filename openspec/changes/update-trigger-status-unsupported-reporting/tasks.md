## 1. Investigation & Design
- [x] 确认 Oracle/OB 的 DBA_TRIGGERS/DBA_OBJECTS 字段（STATUS/VALID）可用性与含义
- [x] 构造触发器 4 种状态验证脚本（VALID/INVALID × ENABLED/DISABLED）

## 2. Trigger Status Comparison
- [x] 扩展触发器元数据加载，补充 VALID/INVALID
- [x] 扩展触发器比较逻辑：enabled/valid 差异
- [x] 触发器报告重命名并输出状态差异明细

## 3. Unsupported Classification
- [x] 定义 SupportState / ReasonCode / Action 建模与统计接口
- [x] 将黑名单/临时表纳入 UNSUPPORTED
- [x] 依赖传播：将依赖 UNSUPPORTED 的对象标记为 BLOCKED

## 4. View Compatibility Rules
- [x] 新增 SYS.OBJ$ / DBLINK / 缺失 DBA 视图识别
- [x] DBA_USERS.USER_ID → USERID 的清洗规则（避免字符串/注释误改）
- [x] 视图规则命中时输出原因与依赖对象

## 5. Fixup Output Segregation
- [x] 新增 tables_unsupported 目录与临时表子目录
- [x] 视图/同义词/触发器/PLSQL 的 UNSUPPORTED/BLOCKED 分流目录
- [x] Fixup 仅处理 SUPPORTED 目录

## 6. Report Restructure
- [x] 主报告新增“缺失=支持/不支持/被阻断”汇总
- [x] 输出 unsupported_objects_detail_<ts>.txt（| 分隔）
- [x] 报告拆分策略与文件索引

## 7. Configuration & Docs
- [x] 新增/更新配置项与中文说明
- [x] README / docs 说明输出目录与报告文件变更
- [x] 版本号更新到 v0.9.8 并更新 CHANGELOG

## 8. Tests (Oracle 19c / OB 4.2.5.7)
- [x] 触发器 4 种状态对比与报告输出
- [x] 视图规则命中与依赖阻断案例
- [x] 不支持表/临时表分流与报告统计验证
- [x] 大规模报告拆分输出验证
