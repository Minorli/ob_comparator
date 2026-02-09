## 1. Implementation
- [x] 1.1 增加状态差异数据结构与检测逻辑（TRIGGER/CONSTRAINT）
- [x] 1.2 增加状态差异报告导出 `status_drift_detail_<ts>.txt`
- [x] 1.3 增加状态修复脚本生成逻辑（受开关控制，默认关闭）
- [x] 1.4 增加配置解析/校验/向导提示与默认值
- [x] 1.5 更新 `config.ini.template` 与 `readme_config.txt`

## 2. Validation
- [x] 2.1 语法检查：`python3 -m py_compile $(git ls-files '*.py')`
- [x] 2.2 单元测试：新增状态匹配与脚本生成测试
- [x] 2.3 Oracle + OceanBase 实机验证：状态差异识别与脚本可执行性
