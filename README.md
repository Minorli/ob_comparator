# OceanBase Comparator Toolkit

🚀 **极简必看用法**  
> 当前版本：V0.9.2（Dump-Once, Compare-Locally + 安全 DDL 生成 + 依赖智能推导）

本程序致力于以最轻量的方式实现 Oracle 到 OceanBase 的异构数据库对比与修复。采用“一次转储，本地对比”架构，彻底解决大规模对象比对时的性能瓶颈与 ORA-01555 问题。

1. **环境准备**：准备 Python 3.7+、Oracle Instant Client、obclient、JDK+dbcat。  
2. **安装依赖**：
   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. **配置**：复制模板并修改配置。
   ```bash
   cp config.ini.template config.ini
   # 编辑 config.ini 填入连接信息
   ```
4. **运行对比**：
   ```bash
   python3 schema_diff_reconciler.py
   # 或使用向导模式
   python3 schema_diff_reconciler.py --wizard
   ```
5. **执行修复**：
   审核 `fixup_scripts/` 下生成的 SQL，确认无误后自动执行：
   ```bash
   python3 run_fixup.py
   ```

## ✨ 核心特性

- **🚀 高性能架构**：放弃传统的循环查库模式，采用全量元数据快照（Dump-Once）+ 本地内存比对（Compare-Locally），速度提升显著。
- **🛡️ 安全 DDL 生成 (New)**：
  - 内置 `SqlMasker` 引擎，在重写 SQL（Remap）时自动保护字符串字面量与注释，杜绝正则误伤。
  - 智能解析视图依赖，完美支持 `FROM A, B` 等复杂语法。
  - PL/SQL 本地引用自动补全（智能推导 Schema）。
- **🧠 智能 Remap 推导**：
  - 自动识别“多对一”、“一对一”映射。
  - 针对“一对多”场景，基于依赖图（Dependency Graph）智能推导视图/存储过程的归属 Schema。
- **🔧 全面对象支持**：
  - 核心对象：TABLE, VIEW, MVIEW, PROCEDURE, FUNCTION, PACKAGE, TRIGGER, SEQUENCE, SYNONYM。
  - 深度校验：INDEX（列/唯一性）、CONSTRAINT（PK/UK/FK）、列属性（长度自动放宽策略）。
  - 注释比对：支持表级与列级注释一致性检查。
- **📦 自动化修补**：
  - 自动生成 CREATE/ALTER/GRANT/COMPILE 脚本。
  - 结构化输出到 `fixup_scripts/`，按依赖顺序组织。

## 📂 项目结构

| 路径 | 说明 |
| --- | --- |
| `schema_diff_reconciler.py` | **主程序**。负责元数据提取、对比、依赖分析及修复脚本生成。 |
| `run_fixup.py` | **执行器**。批量执行修复脚本，支持断点续传与结果归档。 |
| `config.ini.template` | 配置模板。使用前复制为 `config.ini`。 |
| `remap_rules.txt` | 对象映射规则定义文件。 |
| `docs/ADVANCED_USAGE.md` | **进阶指南**。包含 Remap 推导原理与 `run_fixup` 高级用法。 |
| `docs/DEPLOYMENT.md` | **部署指南**。离线环境/跨平台打包与交付说明。 |
| `docs/ARCHITECTURE.md` | **架构文档**。设计理念与内部实现细节。 |
| `docs/CHANGELOG.md` | 版本变更记录。 |
| `fixup_scripts/` | 生成的修复脚本目录（按对象类型分类）。 |
| `main_reports/` | 对比报告归档。 |
| `dbcat_output/` | dbcat 导出的 DDL 缓存。 |

## ⚙️ 配置说明 (`config.ini`)

### 关键配置项

- **`[ORACLE_SOURCE]` / `[OCEANBASE_TARGET]`**: 定义源端与目标端的连接信息。
- **`source_schemas`**: 待比对的源端 Schema 列表。
- **`remap_file`**: 映射规则文件路径（默认为 `remap_rules.txt`）。
- **`check_primary_types`**: 指定检查的主对象类型（如 `TABLE,VIEW`），留空则全量检查。
- **`generate_fixup`**: 是否生成修复脚本 (`true`/`false`)。
- **`check_dependencies`**: 是否启用依赖分析与授权推导。
- **`check_comments`**: 是否比对注释。

### Remap 规则 (`remap_rules.txt`)

格式为 `SOURCE.OBJECT = TARGET.OBJECT`。
建议仅配置 **TABLE** 的映射，其他对象（索引、触发器、视图等）通常可由程序自动推导。详情请参阅 [进阶指南](docs/ADVANCED_USAGE.md)。

## 🛠️ 常见操作

### 1. 仅校验表结构
```ini
check_primary_types = TABLE
check_extra_types = 
check_dependencies = false
generate_fixup = false
```

### 2. 全量对比与修复
```ini
check_primary_types = 
check_extra_types = index,constraint,sequence,trigger
check_dependencies = true
generate_fixup = true
```

### 3. 应用修复脚本
```bash
# 执行所有脚本
python3 run_fixup.py

# 仅执行表结构变更
python3 run_fixup.py --only-dirs table,table_alter

# 启用智能排序与自动重编译（推荐）
python3 run_fixup.py --smart-order --recompile
```

## 📋 要求

- **OS**: Linux (推荐) / macOS
- **Python**: 3.7+
- **Client**: Oracle Instant Client 19c+, obclient
- **Java**: JDK 8+ (用于 dbcat)

---
© 2025 Minor Li.
