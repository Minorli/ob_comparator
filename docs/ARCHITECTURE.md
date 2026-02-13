# 数据库对象对比工具设计文档

> 当前版本：V0.9.8.5（Dump-Once, Compare-Locally + 依赖分析 + 修补脚本生成）

## 1. 设计原则
- **一次转储、本地对比**：最大限度减少数据库往返，性能可控。
- **配置驱动**：所有行为由 `config.ini` 控制，可复用和审计。
- **脚本优先、人工审核**：主程序仅生成 SQL，不自动执行。
- **可回滚、可追踪**：报告与脚本输出具备完整审计线索。

## 2. 组件划分
- **`schema_diff_reconciler.py`**：主流程，负责元数据采集、差异对比、修补脚本生成、报告输出。
- **`run_fixup.py`**：修补脚本执行器（smart-order/迭代/VIEW 链路）。
- **`init_users_roles.py`**：用户/角色初始化与授权同步。

## 3. 总体流程
```
配置读取 -> Remap 规则 -> 源对象清单 -> 依赖关系/同义词
       -> 目标端元数据转储 -> 源端元数据转储
       -> 主对象对比 -> 扩展对象对比 -> 注释校验
       -> 依赖校验/授权推导 -> DDL 清洗与修补脚本生成
       -> 报告输出
```

## 4. 元数据采集

### Oracle 侧
- `oracledb` Thick Mode 批量读取：
  - `DBA_OBJECTS / DBA_TAB_COLUMNS / DBA_INDEXES / DBA_CONSTRAINTS`
  - `DBA_TRIGGERS / DBA_SEQUENCES / DBA_DEPENDENCIES`
  - `DBA_TAB_COMMENTS / DBA_COL_COMMENTS`
  - `DBA_*_PRIVS`（授权脚本来源）
- 使用分批 IN 列表规避 1000 限制。

### OceanBase 侧
- `obclient` 一次性读取 DBA 视图。
- 结果全部缓存到内存结构 `ObMetadata`。

## 5. 映射与推导
- 解析 `remap_rules.txt` 并验证合法性。
- 构建：
  - `master_list`（主对象检查清单）
  - `full_object_mapping`（全量映射，供依赖/DDL 使用）
- 支持：显式 remap、依附对象跟随、依赖推导、schema 回退。

## 6. 对比策略
- **TABLE**：列名集合、VARCHAR 长度窗口、LONG/LONG RAW 转换。
- **VIEW/PLSQL/TYPE/SYNONYM/JOB/SCHEDULE**：存在性检查。
- **PACKAGE**：有效性与错误摘要。
- **INDEX/CONSTRAINT/SEQUENCE/TRIGGER**：列组合/唯一性/触发事件/状态。

## 7. 依赖与授权
- `DBA_DEPENDENCIES` 构建期望依赖集合，与目标端对比。
- 缺失依赖输出 `ALTER ... COMPILE` 修补脚本。
- 授权脚本基于依赖推导与权限元数据生成。

## 8. DDL 生成与清洗
- **DDL 来源**：dbcat（批量）+ DBMS_METADATA（VIEW 兜底）。
- **清洗策略**：
  - Hint 过滤与白名单
  - PL/SQL 标点清洗
  - Oracle 特有语法清理
  - VIEW 注释吞行修复

## 9. 输出与执行
- 报告输出到 `main_reports/`（默认 `run_<timestamp>` 分目录），脚本输出到 `fixup_scripts/`。
- 报告索引 `report_index_<timestamp>.txt` 用于快速定位细节文件。
- `run_fixup.py` 支持：
  - 依赖感知排序（smart-order）
  - 迭代重试（iterative）
  - VIEW 链路自动修复（view-chain）
  - 错误报告（errors 目录）

## 10. 可靠性与性能
- 可配置超时：`obclient_timeout`、`cli_timeout`、`fixup_cli_timeout`。
- 生成过程支持多线程（`fixup_workers`）。
- 扩展对象校验支持并发与批量调优（`extra_check_workers`/`extra_check_chunk_size`）。
- dbcat 输出支持缓存复用（`dbcat_output/`）。

## 11. 安全与审计
- 主程序仅执行 SELECT，不直接执行 DDL。
- 修补脚本需人工审核后执行。
- 运行总结与关键清单确保可追溯。

## 12. 交付前正确性基线
- 静态语法：`python3 -m py_compile $(git ls-files '*.py')`
- 单元测试：`.venv/bin/python -m unittest discover -v`
- 可选联调：在测试环境执行 `schema_diff_reconciler.py` + `run_fixup.py --glob "__NO_MATCH__"` 验证链路可达。
- 建议每次交付都记录本次测试结果（通过/失败/跳过项），并随交付包一并存档。
