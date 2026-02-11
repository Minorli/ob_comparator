## Context
当前实现已经具备：
- OB 版本探测（`get_oceanbase_version` + `extract_ob_version_number` + `compare_version`）；
- VIEW 清洗中的版本分支（`WITH CHECK OPTION`）。

但 `MATERIALIZED VIEW` 与 interval 分区补齐仍为静态策略，无法随 OB 版本能力自动切换。

## Goals / Non-Goals
- Goals:
  - 基于 OB 版本（阈值 `4.4.2`）自动切换 MVIEW 与 interval 的默认行为；
  - 保留人工强制覆盖；
  - 在日志与报告中可见“最终生效值”，便于排障；
  - 不改变既有 remap / 依赖推导语义。
- Non-Goals:
  - 不在本变更中引入更多对象类型的版本门控；
  - 不修改 run_fixup 执行器策略。

## Design Decisions

### 1) 配置模型
- `generate_interval_partition_fixup`：
  - 新支持值：`auto|true|false`；
  - 兼容旧值：`1/0/yes/no/on/off`（映射为 true/false）。
- `mview_check_fixup_mode`（新配置）：
  - 值：`auto|on|off`。

### 2) 生效值计算
定义能力门控结果：
- `effective_interval_fixup_enabled`
- `effective_mview_enabled`

计算逻辑（伪代码）：
```text
is_ob_442_plus = parsed_ob_version && compare_version(ob_version, "4.4.2") >= 0

effective_interval_fixup_enabled =
  if interval_cfg in {true,false} -> interval_cfg
  else if interval_cfg == auto:
    if version_known -> (not is_ob_442_plus)
    else -> true   # 保守回退到旧行为

effective_mview_enabled =
  if mview_mode == on  -> true
  if mview_mode == off -> false
  if mview_mode == auto:
    if version_known -> is_ob_442_plus
    else -> false  # 保守回退到旧行为
```

### 3) 代码接入点
- 初始化阶段：
  - 解析配置并计算两个 `effective_*`；
  - 记录到 `settings`，供后续全流程复用。
- 比对阶段：
  - `PRINT_ONLY_PRIMARY_TYPES` 改为运行态可变集合；
  - `MATERIALIZED VIEW` 在 `effective_mview_enabled=false` 时保持仅打印；
  - 在 `effective_mview_enabled=true` 时参与正常 missing/extra/mismatch 检测。
- 修补阶段：
  - interval 分区补齐脚本生成改用 `effective_interval_fixup_enabled`；
  - MVIEW 缺失时在 `effective_mview_enabled=true` 下参与 DDL 生成；
  - dbcat 不支持 MVIEW 时，走 `DBMS_METADATA` 主路径（与 VIEW 类似的兜底思路）。

### 4) 可观测性
- 主报告与日志输出：
  - 原始配置值；
  - OB 版本解析结果；
  - `effective_interval_fixup_enabled` 与 `effective_mview_enabled`；
  - 若回退到保守模式（版本未知），输出显式告警。

## Risks and Mitigations
- 风险：版本字符串解析失败导致行为不符合用户预期。
  - 缓解：`auto` 明确保守回退 + 强日志提示 + 支持手工强制值。
- 风险：MVIEW 开启后 DDL 来源不稳定。
  - 缓解：dbcat 明确跳过 MVIEW，固定走 metadata 提取并保留失败明细。
- 风险：既有配置兼容性。
  - 缓解：保留旧布尔写法，默认值改动仅体现在 `auto` 语义。

## Validation Plan
- 单元测试：
  - 版本判定矩阵：`4.4.1` / `4.4.2` / `4.4.2.7` / unknown。
  - `auto|true|false` + `auto|on|off` 组合覆盖。
- 集成测试：
  - mock OB 版本验证报告生效值输出；
  - MVIEW 在 auto+4.4.2 下进入 fixup，auto+4.4.1 下仅打印；
  - interval 在 auto+4.4.2 下不生成，在 auto+4.4.1 下生成。
