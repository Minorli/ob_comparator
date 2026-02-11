## 1. Implementation
- [x] 1.1 扩展配置解析：`generate_interval_partition_fixup` 支持 `auto|true|false`（兼容旧布尔）
- [x] 1.2 新增配置：`mview_check_fixup_mode=auto|on|off`，并完成校验与默认值
- [x] 1.3 增加版本门控计算：输出 `effective_interval_fixup_enabled` 与 `effective_mview_enabled`
- [x] 1.4 比对流程接入 `effective_mview_enabled`（MVIEW 自动从 print-only 切换为正常校验）
- [x] 1.5 修补流程接入 `effective_interval_fixup_enabled`（4.4.2+ auto 默认不生成 interval 补齐）
- [x] 1.6 修补流程在 `effective_mview_enabled=true` 时支持 MVIEW DDL 生成（metadata 优先）
- [x] 1.7 主报告与日志新增“版本门控生效值”说明
- [x] 1.8 更新 `config.ini.template` 与 `readme_config.txt`

## 2. Tests
- [x] 2.1 新增单测：版本解析与门控决策矩阵
- [x] 2.2 新增单测：MVIEW print-only/normal-check 切换行为
- [x] 2.3 新增单测：interval auto 在不同版本下的生成开关
- [x] 2.4 回归测试：`python3 -m py_compile $(git ls-files '*.py')`
- [x] 2.5 回归测试：`test_schema_diff_reconciler.py` / `test_config_template.py`

## 3. Validation
- [ ] 3.1 在 OB < 4.4.2 环境验证：MVIEW 仍仅打印，interval auto 默认生成
- [ ] 3.2 在 OB >= 4.4.2 环境验证：MVIEW 校验与 fixup 打开，interval auto 默认关闭
- [x] 3.3 版本未知场景验证：回退旧行为并有明确提示
