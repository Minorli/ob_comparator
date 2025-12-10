# 性能优化指南

## 问题：Fixup脚本生成阶段缓慢

### 症状1：日志显示错误的耗时
```
[DDL_FETCH] LIFEDATA.COMM_XX (VIEW) 来源=DBCAT_RUN 耗时=28.64s
```
每个对象显示需要20-30秒，但实际上已经从缓存加载。

**原因**：dbcat批次总耗时被记录到每个对象  
**修复**：v0.8.1已修复，将批次耗时平均分配

### 症状2：生产环境实际很慢（真实性能问题）
```
[性能警告] 缓存加载平均 0.523s/文件 (261.5s/500文件) - 磁盘IO慢
[性能] 150个文件>0.5s
```

**原因**：
1. **网络存储延迟高**（NFS、CIFS等）
2. **磁盘IO性能差**（机械硬盘、高负载）
3. **大量小文件读取**（每个对象一个文件）
4. **文件系统元数据操作慢**

**解决方案**：

#### 方案1：启用并行缓存加载（推荐）

```ini
[SETTINGS]
# 并行读取缓存文件，适用于慢速磁盘/网络存储
cache_parallel_workers = 4  # 建议4-8，根据CPU核心数调整
```

**效果**：
- 单线程：500文件 × 0.5s = 250秒
- 4线程：250秒 ÷ 4 = 62秒
- 8线程：250秒 ÷ 8 = 31秒

**注意**：
- 仅在文件数>20时启用并行
- 不要超过CPU核心数
- SSD环境下提升不明显

#### 方案2：使用本地SSD

```bash
# 将dbcat_output移到本地SSD
mkdir /local/ssd/dbcat_cache
ln -s /local/ssd/dbcat_cache /path/to/comparator/dbcat_output
```

**效果**：
- 网络存储：0.5s/文件
- 本地SSD：0.01s/文件
- 提升50倍

#### 方案3：禁用缓存，重新导出

```bash
# 清理旧缓存
rm -rf dbcat_output/*

# 重新运行，dbcat会直接导出到内存
python3 schema_diff_reconciler.py
```

**适用场景**：
- 缓存文件损坏
- 磁盘IO极慢
- 首次运行

#### 方案4：减少对象数量

```ini
[SETTINGS]
# 只检查必要的对象类型
check_primary_types = TABLE,VIEW
check_extra_types = 
check_dependencies = false
```

---

## 性能诊断

### 识别瓶颈

运行程序后查看日志：

```
[性能] 加载缓存索引耗时 2.35s，磁盘IO可能较慢
[性能警告] 缓存加载平均 0.523s/文件 (261.5s/500文件) - 磁盘IO慢
[建议] 1) 使用本地SSD  2) 设置cache_parallel_workers=4-8  3) 或禁用缓存
[性能] 150个文件>0.5s，前5个：
  LIFEDATA.COMM_ORDER: 1.23s
  LIFEDATA.COMM_DETAIL: 0.98s
  ...
```

**判断标准**：

| 指标 | 正常 | 慢 | 极慢 | 原因 |
|------|------|-----|------|------|
| 索引加载 | <0.1s | 0.5-2s | >2s | 文件系统元数据 |
| 平均文件读取 | <0.01s | 0.1-0.5s | >0.5s | 磁盘IO/网络 |
| 单文件最大 | <0.1s | 0.5-2s | >2s | 文件大小/延迟 |

---

## 性能调优建议

### 1. 调整dbcat批次大小

**配置项**：`dbcat_chunk_size`

```ini
[SETTINGS]
# 默认150，可根据对象复杂度调整
dbcat_chunk_size = 200  # 简单对象（VIEW/SYNONYM）
dbcat_chunk_size = 100  # 复杂对象（PACKAGE/PROCEDURE）
```

**影响**：
- 更大批次：减少dbcat调用次数，但单次耗时更长
- 更小批次：增加dbcat调用次数，但单次耗时更短

**建议**：
- 对象数量<500：使用默认150
- 对象数量>1000：增加到200-300
- 网络不稳定：减少到50-100

### 2. 启用并行导出

**配置项**：`fixup_workers`

```ini
[SETTINGS]
# 默认为CPU核心数，可手动指定
fixup_workers = 4  # 4个并行线程
```

**影响**：
- 多个schema并行导出
- 修补脚本并行生成

**建议**：
- 单schema：保持默认（自动）
- 多schema：设置为CPU核心数的50-75%
- 内存受限：减少到2-4

### 3. 复用dbcat缓存

**机制**：
- 首次运行：dbcat导出到 `dbcat_output/`
- 后续运行：优先从缓存加载
- 缓存命中：耗时<0.01秒/对象

**清理缓存**：
```bash
# 清理所有缓存
rm -rf dbcat_output/*

# 清理特定schema
rm -rf dbcat_output/*SCHEMA_NAME*
```

**建议**：
- 开发测试：保留缓存
- 生产迁移：首次运行后保留缓存
- schema变更：清理对应缓存

### 4. 调整超时时间

**配置项**：`cli_timeout`

```ini
[SETTINGS]
# dbcat/obclient超时（秒）
cli_timeout = 600  # 默认10分钟
```

**建议**：
- 对象数量<1000：保持默认600
- 对象数量>5000：增加到1200-1800
- 网络较慢：增加到1800-3600

### 5. 限制检查范围

**场景**：只需检查部分对象类型

```ini
[SETTINGS]
# 只检查TABLE和VIEW
check_primary_types = TABLE,VIEW

# 跳过扩展检查
check_extra_types = 

# 跳过依赖检查
check_dependencies = false
```

**性能提升**：
- 减少元数据查询
- 减少DDL导出
- 减少修补脚本生成

---

## 性能监控

### 查看详细耗时

**日志输出**：
```
[dbcat] 导出 schema=LIFEDATA option=--view 对象数=150...
[dbcat] 导出 schema=LIFEDATA option=--view 完成，用时 30.25s。
[dbcat] 缓存总计加载 450 个对象 DDL，剩余待导出 150。
```

**关键指标**：
- 缓存命中率：`缓存加载数 / 总对象数`
- 平均导出速度：`总耗时 / 对象数`
- 并行效率：`单线程耗时 / 并行耗时`

### 性能瓶颈识别

| 阶段 | 正常耗时 | 异常耗时 | 可能原因 |
|------|---------|---------|---------|
| 元数据转储 | 5-30秒 | >60秒 | 数据库性能、网络延迟 |
| dbcat导出 | 0.1-0.5秒/对象 | >1秒/对象 | 对象复杂、Java堆内存不足 |
| 修补脚本生成 | 0.01-0.1秒/对象 | >0.5秒/对象 | 磁盘IO、DDL复杂度 |

### 优化检查清单

- [ ] 确认dbcat缓存已启用
- [ ] 检查 `dbcat_chunk_size` 是否合理
- [ ] 验证并行线程数配置
- [ ] 确认网络连接稳定
- [ ] 检查Java堆内存（JAVA_OPTS）
- [ ] 验证磁盘IO性能

---

## 故障排查

### 问题1：缓存未生效

**症状**：每次运行都重新导出

**检查**：
```bash
ls -la dbcat_output/
# 应该看到 flat_cache/ 目录和 schema 目录
```

**解决**：
```bash
# 确认输出目录权限
chmod 755 dbcat_output/

# 确认配置正确
grep dbcat_output_dir config.ini
```

### 问题2：dbcat导出超时

**症状**：`[dbcat] 转换 schema=XXX 超时 (600s)`

**解决**：
```ini
[SETTINGS]
cli_timeout = 1800  # 增加到30分钟
dbcat_chunk_size = 50  # 减小批次
```

### 问题3：内存不足

**症状**：`java.lang.OutOfMemoryError`

**解决**：
```bash
# 设置Java堆内存
export JAVA_OPTS="-Xmx4g -Xms2g"

# 或在config.ini中配置
[SETTINGS]
java_opts = -Xmx4g -Xms2g
```

### 问题4：并行导出冲突

**症状**：文件写入错误、数据不一致

**解决**：
```ini
[SETTINGS]
fixup_workers = 1  # 禁用并行
```

---

## 最佳实践

### 开发环境
```ini
[SETTINGS]
dbcat_chunk_size = 150
fixup_workers = 2
cli_timeout = 600
check_dependencies = true
```

### 生产环境（首次迁移）
```ini
[SETTINGS]
dbcat_chunk_size = 200
fixup_workers = 4
cli_timeout = 1800
check_dependencies = true
generate_fixup = true
```

### 生产环境（增量校验）
```ini
[SETTINGS]
dbcat_chunk_size = 150
fixup_workers = 4
cli_timeout = 600
check_dependencies = false
generate_fixup = false
```

### 大规模迁移（>10000对象）
```ini
[SETTINGS]
dbcat_chunk_size = 300
fixup_workers = 8
cli_timeout = 3600
# 分批执行，每次限制schema数量
source_schemas = SCHEMA1,SCHEMA2  # 不要一次性处理所有schema
```

---

## 性能基准

### 参考数据（测试环境）

| 对象数量 | 首次运行 | 缓存运行 | 修补脚本生成 |
|---------|---------|---------|-------------|
| 100 | 30秒 | 5秒 | 10秒 |
| 500 | 2分钟 | 15秒 | 45秒 |
| 1000 | 4分钟 | 30秒 | 1.5分钟 |
| 5000 | 20分钟 | 2分钟 | 7分钟 |

**注**：实际性能取决于硬件、网络、对象复杂度等因素。

---

## 更新日志

### v0.8.1 (2025-12-09)
- 修复：dbcat批次耗时平均分配到每个对象
- 优化：缓存加载时使用实际读取耗时
- 优化：减少缓存加载的日志输出
- 新增：本性能调优文档
