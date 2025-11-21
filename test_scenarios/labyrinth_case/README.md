# Labyrinth Case（复杂映射与依赖全覆盖）

本案例专门用于“清空并重建”源/目标库后，验证 `schema_diff_reconciler.py` 在复杂 remap、跨 schema 依赖、对象类型全覆盖场景下的健壮性。

## 目录
```
test_scenarios/labyrinth_case/
├── oracle_setup.sql           -- Oracle 源端完整建模（LAB_*）
├── oceanbase_setup.sql        -- OceanBase 目标端残缺+噪声版本（OB_*）
└── remap_rules_labyrinth.txt  -- 逐对象 remap 定义（含 PACKAGE BODY、TYPE BODY）
```

## 如何使用
1. **清空并重建源端 (Oracle)**  
   ```sql
   sqlplus sys/<pwd>@<oracle_dsn> as sysdba
   @test_scenarios/labyrinth_case/oracle_setup.sql
   ```
   脚本会 DROP/CREATE `LAB_CORE/LAB_FIN/LAB_APP/LAB_UTIL` 并创建表/视图/MV/序列/触发器/类型/包/同义词等。

2. **清空并重建目标端 (OceanBase)**  
   ```sql
   obclient -h <host> -P <port> -u <tenant user> -p
   source test_scenarios/labyrinth_case/oceanbase_setup.sql
   ```
   脚本会 DROP/CREATE `OB_BASE/OB_FIN/OB_SALES/OB_APP/OB_ANALYTICS/OB_UTIL`，但故意缺失/缩减/错指部分对象，并加入干扰对象。

3. **运行对比工具**  
   在 `config.ini` 中设置：
   ```
   [SETTINGS]
   source_schemas = LAB_CORE,LAB_FIN,LAB_APP,LAB_UTIL
   remap_file = test_scenarios/labyrinth_case/remap_rules_labyrinth.txt
   generate_fixup = true
   ```
   然后执行 `python3 schema_diff_reconciler.py`，查看控制台、`main_reports/` 报告与 `fixup_scripts/`。

## 预期异常（应被主程序检测）
- **缺失对象**  
  - 序列/触发器：`SEQ_ACCOUNT`/`TRG_ACCOUNT_BI`、`SEQ_LEDGER`/`TRG_LEDGER_BI`、`SEQ_ORDER`/`SEQ_ORDER_LINE`/`TRG_ORDER_BI`、`SEQ_LOG`/`TRG_LOG_BI` 全部缺失。  
  - 表：`APP_ORDER_LINE` 缺失。  
  - 代码对象：`PKG_CORE_UTIL`（规范+体）、`PKG_FIN_RECON`（规范+体）、`PKG_APP_API` 包体、`PR_CREATE_ORDER`、`PR_REFRESH_STATUS`、`FN_ORDER_TOTAL`、`FN_LOG_AND_BALANCE`、`PR_LOG_ACTIVITY`、`FN_GET_BALANCE`、`MV_STATUS_COUNT`、`V_ORDER_SUM`、类型 `T_AUDIT_TAG` 及其 BODY 等。
- **列/长度不匹配**  
  - `OB_BASE.CORE_ACCOUNT` 缺少 `NOTE` 列且 `NAME` 长度从 60→20（应触发长度不足与缺列）。  
  - `OB_SALES.APP_ORDER` 缺少 `NOTE/CREATED_BY`，`ORDER_CODE/CHANNEL` 长度被缩短。
- **多余对象**  
  - `OB_FIN.EXTRA_SEQ_FIN_NOISE`、`OB_SALES.EXTRA_SHADOW_ORDER`、`OB_APP.FN_NOISE` 等应被标记为目标端多余。
- **依赖缺口**  
  - 跨 schema 调用：`PKG_CORE_UTIL` 引用 `LAB_FIN.FN_GET_BALANCE`，`PKG_APP_API` 联动 `LAB_FIN` 与 `LAB_CORE`，`LAB_FIN.PKG_FIN_RECON` 反调 `LAB_CORE.PKG_CORE_UTIL`；目标缺少这些对象会导致依赖缺失与重编译脚本输出。  
  - 同义词指向错误：`OB_UTIL.SYN_FN_BALANCE` 指向不存在的 `OB_FIN.FN_GET_BALANCE`，`SYN_ORDER_VIEW` 指向表而非视图，会在依赖对比中显示“额外/缺失依赖”。  
  - 跨 schema GRANT：`LAB_APP`/`LAB_FIN`/`LAB_UTIL` 对 `LAB_CORE` 对象的访问应触发 `GRANT SELECT/EXECUTE` 建议。
- **类型与包体**  
  - `T_AUDIT_TAG` BODY 缺失、包体缺失应在包/类型类别下报缺，并生成对应 fix-up。

使用本案例可一次性覆盖：表/视图/MV/序列/触发器/类型/包/过程/函数/同义词的缺失与多余检测、列和长度差异、跨 schema 依赖与授权推导，以及 fix-up 脚本生成的稳健性。***
