# 主程序索引

本文件是主入口与核心流程的速查表，用于快速定位关键逻辑。

## 入口脚本
- `schema_diff_reconciler.py`：主对比与修补脚本生成流程。
- `run_fixup.py`：修补脚本执行器（单轮/迭代/VIEW 链路）。
- `init_users_roles.py`：用户/角色初始化与授权同步。
- `init_test.py`：测试场景初始化工具（基于 `test_scenarios/`）。

## schema_diff_reconciler.py 主流程
1. 解析 CLI 参数，必要时进入 `--wizard` 向导。
2. 读取配置、初始化日志、校验路径与依赖。
3. 加载 remap 规则，读取源端对象清单。
4. 读取依附对象父表关系、依赖关系、同义词元数据。
5. 生成对象映射与 master_list（主对象检查清单）。
6. Dump OceanBase 元数据（一次转储）。
7. Dump Oracle 元数据（批量查询）。
8. 主对象检查 + PACKAGE 有效性对比。
9. 扩展对象检查（INDEX/CONSTRAINT/SEQUENCE/TRIGGER）。
10. 注释一致性校验（可配置关闭）。
11. 依赖关系校验与缺失依赖定位。
12. 授权计划与缺失授权脚本生成（可配置关闭）。
13. DDL 提取与清洗（dbcat + DBMS_METADATA）。
14. 生成 fixup_scripts（分类型输出）。
15. 输出报告与运行总结。

## run_fixup.py 执行模式
- `run_single_fixup`：单轮执行 + 依赖排序。
- `run_iterative_fixup`：多轮迭代自动重试，适合依赖复杂对象。
- `run_view_chain_autofix`：按 VIEW 依赖链生成执行计划。

## 核心输出位置
- `main_reports/report_*.txt`：主报告
- `main_reports/VIEWs_chain_*.txt`：VIEW 依赖链
- `fixup_scripts/`：修补脚本目录
- `fixup_scripts/errors/`：run_fixup 错误报告
