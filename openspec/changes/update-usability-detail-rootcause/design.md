## Context
可用性检查目前只执行 `SELECT * FROM <obj> WHERE 1=2`，错误信息仅做粗粒度分类，无法直接指示依赖对象或权限根因。

## Goals / Non-Goals
- Goals:
  - 在 `usability_check_detail` 中输出可操作的根因与建议
  - 结合依赖链与权限链报告给出对象级提示
  - 保持当前 SKIPPED 逻辑（缺失/不支持）不变
- Non-Goals:
  - 不新增额外数据库查询（避免额外负载）
  - 不改变可用性检查范围与采样逻辑

## Decisions
- Decision: 使用已有的 dependency_chains/VIEWs_chain/依赖汇总作为根因补充来源
- Decision: 错误文本分类保留，但补充“对象/权限/依赖”字段
- Decision: 文案统一“视图查询报错”以符合真实行为（SELECT 失败）

## Risks / Trade-offs
- Risk: 依赖报告缺失时只能降级输出
  - Mitigation: 在 root_cause 中标注“依赖链报告缺失”

## Migration Plan
1. 解析依赖链文件 -> 生成依赖映射
2. 增强可用性结果生成逻辑
3. 更新报告说明与样例
