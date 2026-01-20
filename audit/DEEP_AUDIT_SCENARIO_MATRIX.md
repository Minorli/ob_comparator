# 场景化交叉验证审查清单

## 审查方法论

### 旧审查方法（浮于表面）
```
功能A是否实现 ✅
功能B是否实现 ✅
结论：功能完整
```

### 新审查方法（场景化交叉验证）
```
场景组合：功能A × 功能B × 边界条件
- 代码路径1：是否处理？
- 代码路径2：是否处理？
- 异常情况：是否处理？
结论：逐一验证，找出遗漏
```

---

## 审查清单

### 1. 黑名单表 × 依赖对象处理

| 场景 | 预期行为 | 验证点 | 代码位置 | 状态 |
|------|---------|--------|---------|------|
| 黑名单表 × VIEW依赖 | 不生成VIEW DDL | `view_missing_supported` 过滤逻辑 | lines 15925-15943 | ⏳ 待验证 |
| 黑名单表 × VIEW状态检查 | 不报告VIEW缺失 | `classify_missing_objects` 中VIEW的 `BLOCKED` 标记 | lines 3248-3252 | ⏳ 待验证 |
| 黑名单表 × INDEX Fixup | 不生成INDEX DDL | `index_tasks` 创建时黑名单过滤 | lines 15763-15778 | ⏳ 待验证 |
| 黑名单表 × INDEX状态检查 | 报告为BLOCKED | `extra_blocked_counts["INDEX"]` | lines 3341-3365 | ⏳ 待验证 |
| 黑名单表 × CONSTRAINT Fixup | 不生成CONSTRAINT DDL | `constraint_tasks` 创建时黑名单过滤 | lines 15779-15793 | ⏳ 待验证 |
| 黑名单表 × CONSTRAINT状态检查 | 报告为BLOCKED | `extra_blocked_counts["CONSTRAINT"]` | lines 3367-3391 | ⏳ 待验证 |
| 黑名单表 × TRIGGER Fixup | 不生成TRIGGER DDL | `trigger_tasks` 创建时黑名单过滤 | lines 15800-15841 | ⏳ 待验证 |
| 黑名单表 × TRIGGER状态检查（差异） | **不报告INVALID差异** | `collect_trigger_status_rows` | lines 3070-3120 | ❌ **已发现问题** |
| 黑名单表 × TRIGGER状态检查（缺失） | 报告为BLOCKED | `extra_blocked_counts["TRIGGER"]` | lines 3393-3417 | ⏳ 待验证 |
| 黑名单表 × FK引用 | 不生成FK DDL | FK约束过滤 | ⏳ 待查找 | ⏳ 待验证 |
| 黑名单表 × SYNONYM指向 | 标记SYNONYM为BLOCKED | `classify_missing_objects` SYNONYM逻辑 | lines 3293-3302 | ⏳ 待验证 |

### 2. INVALID对象 × DDL生成/状态检查

| 场景 | 预期行为 | 验证点 | 代码位置 | 状态 |
|------|---------|--------|---------|------|
| INVALID PACKAGE × DDL生成 | **跳过，不生成DDL** | `package_results` 中 `src_status == "INVALID"` 过滤 | lines 15723-15727 | ⏳ 待验证 |
| INVALID VIEW × DDL生成 | 是否跳过？ | VIEW DDL获取逻辑 | ⏳ 待查找 | ⏳ 待验证 |
| INVALID TRIGGER × DDL生成 | 是否跳过？ | TRIGGER DDL获取逻辑 | ⏳ 待查找 | ⏳ 待验证 |
| INVALID对象 × 依赖传播 | 依赖它的对象是否标记为BLOCKED？ | `unsupported_nodes` 是否包含INVALID对象 | ⏳ 待查找 | ⏳ 待验证 |

### 3. 循环依赖 × 拓扑排序

| 场景 | 预期行为 | 验证点 | 代码位置 | 状态 |
|------|---------|--------|---------|------|
| VIEW循环依赖 × 拓扑排序 | 检测并报告循环 | Kahn算法循环检测 | lines 16432-16436 | ⏳ 待验证 |
| VIEW循环依赖 × DDL生成顺序 | 循环VIEW最后创建 | `sorted_view_tuples.extend(circular)` | lines 16436 | ⏳ 待验证 |
| PACKAGE循环依赖 | 是否处理？ | PACKAGE拓扑排序逻辑 | ⏳ 待查找 | ⏳ 待验证 |
| TABLE循环依赖（FK） | 是否处理？ | TABLE DDL生成顺序 | ⏳ 待查找 | ⏳ 待验证 |

### 4. 同义词 × 目标对象状态

| 场景 | 预期行为 | 验证点 | 代码位置 | 状态 |
|------|---------|--------|---------|------|
| SYNONYM指向不存在的表 | 标记为BLOCKED | `classify_missing_objects` | lines 3293-3302 | ⏳ 待验证 |
| SYNONYM指向INVALID对象 | 是否标记为BLOCKED？ | ⏳ 待查找 | ⏳ 待查找 | ⏳ 待验证 |
| PUBLIC SYNONYM × schema过滤 | 跳过不在范围内的 | `allowed_synonym_targets` 过滤 | lines 15668-15681 | ⏳ 待验证 |
| SYNONYM × remap规则 | DDL是否正确remap？ | SYNONYM DDL生成逻辑 | ⏳ 待查找 | ⏳ 待验证 |

### 5. 权限缺失 × 元数据访问

| 场景 | 预期行为 | 验证点 | 代码位置 | 状态 |
|------|---------|--------|---------|------|
| VIEW引用DBA视图 × 权限 | 标记为BLOCKED，提示授权 | `VIEW_PRIVILEGE_REQUIRED` | lines 12165-12170 | ⏳ 待验证 |
| DBA_OBJECTS查询失败 | 程序退出 | `sys.exit(1)` | ⏳ 待查找 | ⏳ 待验证 |
| DBMS_METADATA.GET_DDL失败 | 降级到DBA_VIEWS兜底 | `oracle_get_view_text` fallback | lines 16033-16039 | ⏳ 待验证 |
| 缺失对象权限 × DDL获取 | 是否有异常处理？ | DDL获取的try-except | ⏳ 待查找 | ⏳ 待验证 |

### 6. 临时表 × 迁移策略

| 场景 | 预期行为 | 验证点 | 代码位置 | 状态 |
|------|---------|--------|---------|------|
| 临时表识别 | 标记为unsupported | `TEMPORARY_TABLE` reason_code | lines 15651-15653 | ⏳ 待验证 |
| 临时表 × DDL生成 | 不生成DDL | `missing_tables_unsupported` 分离 | lines 15692-15696 | ⏳ 待验证 |
| 临时表 × 依赖对象 | 依赖临时表的对象是否标记为BLOCKED？ | ⏳ 待查找 | ⏳ 待查找 | ⏳ 待验证 |

### 7. 分区表 × 兼容性

| 场景 | 预期行为 | 验证点 | 代码位置 | 状态 |
|------|---------|--------|---------|------|
| 分区表 × DDL生成 | DDL是否包含分区信息？ | DBMS_METADATA获取的DDL | ⏳ 待查找 | ⏳ 待验证 |
| INTERVAL分区 × OB兼容性 | 是否检测不兼容？ | 兼容性规则检查 | ⏳ 待查找 | ⏳ 待验证 |
| 分区表 × 索引分区 | 本地索引是否正确处理？ | INDEX DDL生成逻辑 | ⏳ 待查找 | ⏳ 待验证 |

### 8. 外键 × 跨schema引用

| 场景 | 预期行为 | 验证点 | 代码位置 | 状态 |
|------|---------|--------|---------|------|
| 跨schema FK × remap | FK引用表名是否remap？ | CONSTRAINT DDL生成 | ⏳ 待查找 | ⏳ 待验证 |
| 跨schema FK × 权限 | 是否生成GRANT？ | `pre_add_cross_schema_grants` | lines 15893 | ⏳ 待验证 |
| FK引用不存在的表 | 是否标记为BLOCKED？ | ⏳ 待查找 | ⏳ 待查找 | ⏳ 待验证 |
| FK × DELETE_RULE | CASCADE规则是否收集？ | `DBA_CONSTRAINTS.DELETE_RULE` | ⏳ 待查找 | ❌ **已知未收集** |

### 9. 事务/回滚 × DDL执行失败

| 场景 | 预期行为 | 验证点 | 代码位置 | 状态 |
|------|---------|--------|---------|------|
| DDL执行失败 × 回滚 | DDL不支持回滚（Oracle特性） | 无事务包装 | ⏳ 待查找 | ⏳ 待验证 |
| 脚本生成 × 执行风险 | 生成独立脚本，手工审核 | fixup_scripts目录 | ⏳ 待查找 | ⏳ 待验证 |
| 批量执行 × 失败处理 | run_fixup.py的错误分类 | `classify_sql_error` | ⏳ 待查找 | ⏳ 待验证 |

### 10. 大规模迁移 × 性能/资源

| 场景 | 预期行为 | 验证点 | 代码位置 | 状态 |
|------|---------|--------|---------|------|
| 10万+对象 × 内存占用 | 是否有内存优化？ | 数据结构设计 | ⏳ 待分析 | ❌ **已知风险** |
| 并发DDL获取 × 连接池 | 是否复用连接？ | ThreadPoolExecutor使用 | ⏳ 待查找 | ⏳ 待验证 |
| 超时策略 × 大对象 | 单一timeout是否合理？ | `obclient_timeout` | lines 5133-5177 | ❌ **已知单一策略** |

---

## 审查执行状态

- ⏳ 待验证：需要读取代码并验证
- ✅ 已验证通过：代码逻辑正确
- ❌ 发现问题：代码有缺陷
- 🔍 需深入：需要进一步分析

---

## 下一步行动

按优先级逐一执行场景验证，每个场景都要：
1. 找到相关代码位置
2. 逐行分析逻辑
3. 推演边界情况
4. 记录发现的问题
