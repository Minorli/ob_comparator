# 回归测试记录（OMS_USER）

时间：2026-02-04  
目的：验证近期大改动（report_db、fixup、视图清洗、LONG 转换、可用性校验等）

## 1. 测试对象构造

### Oracle（OMS_USER）
- TF_LONG：含 LONG 列
- TF_BASE：VARCHAR2(10)
- TF_VIEW：含 inline `--` 注释
- TF_SEQ：序列
- TF_TRG：触发器

### OceanBase（OMS_USER）
- TF_LONG：LONG_COL 改为 CLOB
- TF_BASE：VARCHAR2(5)（故意制造差异）
- TF_VIEW / TF_SEQ / TF_TRG：不创建（制造缺失）

## 2. 主程序运行（首次）

- config: `/tmp/config_oms_test.ini`
- report_dir: `/tmp/reports_test_oms/run_20260204_112858`
- report_id: `20260204_112858_fc03f697`

### 报告库命中（DIFF_REPORT_DETAIL）
- MISMATCHED TABLE: TF_BASE
- MISMATCHED TABLE: TF_LONG
- MISMATCHED TRIGGER: TF_TRG（挂在 TF_BASE）
- MISSING VIEW: TF_VIEW

## 3. fixup 执行

- `run_fixup.py /tmp/config_oms_test.ini`
- 生成并执行目录：`/tmp/fixup_test_oms`

测试对象执行结果（全部成功）：
- `done/view/OMS_USER.TF_VIEW.sql`
- `done/sequence/OMS_USER.TF_SEQ.sql`
- `done/trigger/OMS_USER.TF_TRG.sql`
- `done/table_alter/OMS_USER.TF_BASE.alter_columns.sql`
- `done/table_alter/OMS_USER.TF_LONG.alter_columns.sql`

## 4. 修补结果检查（OB）

```
TF_SEQ  SEQUENCE  VALID
TF_TRG  TRIGGER   VALID
TF_VIEW VIEW      VALID
```

TF_BASE 列：
- VAL VARCHAR2 长度已提升（目标列改为 15）

TF_LONG 列：
- LONG_COL 为 CLOB

## 5. 主程序运行（复测）

- report_dir: `/tmp/reports_test_oms/run_20260204_113354`
- report_id: `20260204_113354_a55a973c`

### 结果
- DIFF_REPORT_DETAIL 中未再出现 TF_* 相关差异
- 证明修补成功

## 6. 报告库写入验证

```
DIFF_REPORT_SUMMARY          1
DIFF_REPORT_COUNTS          11
DIFF_REPORT_DETAIL          40
DIFF_REPORT_TRIGGER_STATUS   3
DIFF_REPORT_USABILITY       16
DIFF_REPORT_PACKAGE_COMPARE  1
DIFF_REPORT_GRANT           261
```

## 7. 额外观察

- `report_db` 外键删除尝试产生 ORA-02443 噪声
  - 已修复：增加 ALL_CONSTRAINTS 检查后再 drop，避免报错
