# Design: 触发器修补增强与报告隔离

## 1) 触发器 DDL 重写策略
目标：
- CREATE TRIGGER 明确带目标 schema 前缀。
- ON 子句与触发器体内的对象引用，统一补全为 remap 后的 schema 前缀。
- 避免对字符串字面量/注释进行误替换。

策略：
1. **主对象 schema**：使用 full_object_mapping 解析触发器目标名（无映射则回退源端）。
2. **引用对象集**：优先使用依赖图（source_dependencies）收集触发器依赖对象；无依赖时退化为 full_object_mapping 中与触发器 source_schema 相关的对象集合。
3. **安全重写**：复用 SqlMasker（现有）屏蔽字符串与注释后再做替换。
4. **schema 补全规则**：
   - 已有 schema 前缀的引用：替换为 remap 后 schema（若目标变化）。
   - 无 schema 的引用：在 ON 子句、INSERT INTO、UPDATE、DELETE FROM、MERGE INTO、FROM、JOIN 等 DML/DDL 语义上下文中补全 schema。
5. **重写顺序**：先改 CREATE TRIGGER 主对象名，再改 ON 子句，再改触发器体引用，避免互相覆盖。

## 2) 序列 remap 策略
新增配置 `sequence_remap_policy`：
- `infer`（默认）：沿用当前“依赖推导 + 主流表 schema 推导”的逻辑。
- `source_only`：始终保持源 schema，不做 remap 推导。
- `dominant_table`：仅使用 TABLE remap 主流 schema 推导，不使用依赖推导。

在 resolve_remap_target 与 sequence 检查/修补阶段统一应用该策略，并在报告中输出策略说明。

## 3) 索引修补差异说明
新增 fixup 统计：
- 缺失索引总数（compare 结果）
- 实际生成索引数
- 跳过原因分类：
  - 表缺失/未进入 master_list
  - fixup_types/fixup_schemas 过滤
  - DDL 抓取失败（dbcat/metadata）
  - 源端索引被约束合并/忽略规则过滤

输出到 report_dir 下的专用文件，并在运行总结中附摘要。

## 4) 报告目录隔离
新增 `report_dir_layout` 配置：
- `flat`（兼容旧行为）：输出到 report_dir 根目录
- `per_run`：输出到 report_dir/run_<timestamp>/

report_dir 仅负责根目录配置，子目录由程序按 layout 生成。

## 5) OMS 目录重命名
`tables_views_miss` → `missed_tables_views_for_OMS`，保持文件格式与命名（SCHEMA_T.txt / SCHEMA_V.txt）不变。

## 6) 配置冲突预检
新增 preflight 校验与警告，覆盖：
- check_primary_types / check_extra_types 与 fixup_types 的交集为空
- trigger_list 与 TRIGGER 未启用检查
- fixup_types=INDEX/CONSTRAINT 但 TABLE 未启用（无法映射）
- 其他会导致 fixup 生成“空转”的组合

输出到日志与最终报告的“配置诊断”段落。
