# 版本对比清单 — 0.9.8.3 → 0.9.8.4

对比范围：V0.9.8.3 与 V0.9.8.4

## 版本号
- 版本号更新至 0.9.8.4

## 主要变化
- TRIGGER 扩展校验改为 `OWNER.TRIGGER_NAME` 粒度比较，修复跨 schema 同名触发器误报 `EXTRA_TRIGGER`。
- CONSTRAINT 扩展校验在签名层与对比层双重忽略 `*_OBNOTNULL_*`，降低 Oracle `SYS_C*` 与 OB 自动非空约束命名差异噪声。
- report_to_db `full` 模式支持报告逐行入库（`DIFF_REPORT_ARTIFACT_LINE`），增强数据库侧排查覆盖。
- README / 配置说明 / 架构与技术文档统一升级到 0.9.8.4。
