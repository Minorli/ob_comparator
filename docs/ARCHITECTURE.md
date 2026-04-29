# 数据库对象对比工具设计文档

> 当前版本：V0.9.9.6（截至 2026-04-29）
> 核心模式：Dump-Once, Compare-Locally + 依赖分析 + 修补脚本生成

## 1. 设计原则
- **一次转储、本地对比**：最大限度减少数据库往返，性能可控。
- **配置驱动**：所有行为由 `config.ini` 控制，可复用和审计。
- **脚本优先、人工审核**：主程序仅生成 SQL，不自动执行。
- **可回滚、可追踪**：报告与脚本输出具备完整审计线索。
- **运行期文档分离**：HOW TO 手册属于交付文档，主程序只输出入口，不在运行时内嵌或读取其正文。

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

- 默认 `source_db_mode=oracle`，保持既有 Oracle → OceanBase 主路径。
- `source_db_mode=oceanbase` 时，源端改走 `obclient + source adapter + capability registry`，避免误复用 Oracle-only loader / cleanup / grant 逻辑。

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
- 当 `source_db_mode=oceanbase` 时，源端 OceanBase metadata 会先适配成 `OracleMetadata` 兼容 bundle，再复用现有 compare/fixup 主链。

## 5. 映射与推导
- 解析 `remap_rules.txt` 并验证合法性。
- 构建：
  - `master_list`（主对象检查清单）
  - `full_object_mapping`（全量映射，供依赖/DDL 使用）
- 支持：显式 remap、依附对象跟随、依赖推导、schema 回退。
- `remap_root_closure` 下会区分 managed mapping 与 discovery-only mapping；后者只用于诊断与审计，不直接参与 operator-facing compare/fixup。

## 6. 对比策略
- **TABLE**：列名集合、Oracle source VARCHAR/VARCHAR2 BYTE 扩容窗口、CHAR_USED='C' 非扩容语义、OB source strict 1:1 类型对比、LONG/LONG RAW 转换。
- **VIEW/PLSQL/TYPE/SYNONYM/JOB/SCHEDULE**：存在性检查。
- **PACKAGE**：有效性与错误摘要。
- **INDEX/CONSTRAINT/SEQUENCE/TRIGGER**：列组合/唯一性/触发事件/状态。
- **VIEW rewrite**：只改写表类数据来源（TABLE/VIEW/MVIEW/SYNONYM）；callable 依赖只做诊断，不参与 DDL rewrite。

## 7. 依赖与授权
- `DBA_DEPENDENCIES` 构建期望依赖集合，与目标端对比。
- 缺失依赖输出 `ALTER ... COMPILE` 修补脚本。
- 授权脚本基于依赖推导与权限元数据生成。
- 授权生成支持两种模式：
  - `full`：按 Oracle 对象/列级/系统/角色授权为主，叠加依赖补充
  - `structural`：只生成对象创建、编译、跨 schema 依赖闭环所需的最小授权

## 8. DDL 生成与清洗
- **DDL 来源**：
  - Oracle source：dbcat（批量）+ DBMS_METADATA（VIEW 兜底）
  - OceanBase source：OB source provider（metadata synthesis / source text / OB-safe exporter）
- **清洗策略**：
  - Hint 过滤与白名单
  - PL/SQL 标点清洗
  - 证据门禁下的兼容性清洗（不再默认删除未证实不支持的 PRAGMA / STORAGE / TABLESPACE）
  - VIEW 注释吞行修复
- Oracle-only TABLE rewrite（如 GTT rewrite、VARCHAR/VARCHAR2 BYTE 长度膨胀）仅允许在 `source_db_mode=oracle` 下生效；`CHAR_USED='C'` 必须保持不扩容，`source_db_mode=oceanbase` 必须保持 1:1 strict compare。

## 9. 输出与执行
- 报告输出到 `main_reports/`（默认 `run_<timestamp>` 分目录），脚本输出到 `fixup_scripts/`。
- 报告索引 `report_index_<timestamp>.txt` 用于快速定位细节文件。
- `report_sql_<timestamp>.txt` 只提供 `report_id` 与 HOW TO 入口。
- 若运行期间命中保护性降级，会额外输出 `runtime_degraded_detail_<timestamp>.txt`；`COMPARE` 级事件表示本轮结果是 partial compare。
- `fixup_scripts/README_FIRST.txt` 作为 fixup 根目录导航，明确默认执行边界。
- `run_fixup.py` 支持：
  - 依赖感知排序（smart-order）
  - 迭代重试（iterative）
  - VIEW 链路自动修复（view-chain）
  - 错误报告（errors 目录）
  - timeout-stop 保护（单脚本超时后停止后续语句）

## 10. 可靠性与性能
- 可配置超时：`obclient_timeout`、`cli_timeout`、`fixup_cli_timeout`。
- 生成过程支持多线程（`fixup_workers`）。
- 扩展对象校验支持并发与批量调优（`extra_check_workers`/`extra_check_chunk_size`）。
- dbcat 输出支持缓存复用（`dbcat_output/`）。
- `run_fixup` 拓扑排序已改为非递归实现，深依赖链不再受 Python 递归栈限制。
- `JOB_ACTION` 与 scoped text matching 带有大文本/高扇出/递归深度保护，避免个别对象把整轮 compare 拖死。
- `dependency_chains` 导出在大图下支持提前跳过与链路截断，避免审计附件反向拖垮主流程。

## 11. 安全与审计
- 主程序仅执行 SELECT，不直接执行 DDL。
- 修补脚本需人工审核后执行。
- 运行总结与关键清单确保可追溯。

## 12. 交付前正确性基线
- 静态语法：`python3 -m py_compile $(git ls-files '*.py')`
- 单元测试：`.venv/bin/python -m unittest discover -v`
- 可选联调：在测试环境执行 `schema_diff_reconciler.py` + `run_fixup.py --glob "__NO_MATCH__"` 验证链路可达。
- 建议每次交付都记录本次测试结果（通过/失败/跳过项），并随交付包一并存档。
