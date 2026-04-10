# ob-lens 设计规格

**日期**：2026-04-10  
**状态**：待实现  
**作者**：Claude Code  

---

## 一、背景与目标

### 问题陈述

OceanBase Comparator Toolkit 功能强大，但上手门槛极高。现场工程师（含非技术背景的 DBA）面临以下具体痛点：

| 痛点 | 根因 |
|------|------|
| "xxx 没生成是什么原因？" | 原因分散在多份 `unsupported_*_detail_*.txt` 中，无导航 |
| "xxx 在哪？" | 每次运行生成 ~30 个文件，无统一入口 |
| 不知道下一步做什么 | 主报告 237 行 Rich 格式文本，信息密度高 |
| config.ini 不会填 | 100+ 配置项，readme_config.txt 太长 |
| 不知道 fixup 跑了哪些、哪些失败 | 需要看命令行日志 |
| 多次运行趋势看不出来 | 每次都是独立目录，无横向对比 |

### 目标

构建 `ob-lens`：一个零改主程序、极低部署成本的可视化层，让不懂工具内部逻辑的工程师也能独立完成迁移校验工作流。

### 约束

- **运行环境**：客户内网服务器，通过两跳 SSH 访问
- **浏览器访问**：不保证能直连，但 SSH 端口转发通常可用
- **不改主程序**：`ob-lens` 完全独立，只读现有输出文件
- **零新依赖（模式①）**：报告生成只用 Python 标准库
- **单文件可传输**：生成的 HTML 报告自包含，可 scp 出来在本地打开

---

## 二、方案选型

### 为何不选 TUI

Textual/Rich TUI 的对齐依赖终端 Unicode 宽度计算。中文字符为双宽字符，在不同 SSH 客户端（XShell、MobaXterm、SecureCRT）和字体下，表格列必然错位。这是终端渲染的结构性问题，无法通过代码修复。

### 为何不选纯 Web 服务

纯 Web 服务需要浏览器能访问服务器端口，在两跳 SSH 的内网环境中不可靠。

### 选定方案：双模式设计

```
python3 ob_lens.py              # 模式①：生成自包含 HTML 报告（零依赖）
python3 ob_lens.py --serve      # 模式②：启动交互服务器（含 fixup 执行）
```

**模式① 解决信息可视化问题**（90% 场景）：
- 每次主程序运行后自动生成 `main_reports/run_<ts>/report_<ts>.html`
- HTML 文件完全自包含（CSS + JS + 数据全部内嵌为 JSON）
- 可 `scp` 出来本地浏览器打开；或 `python3 -m http.server 8080` + SSH 端口转发

**模式② 解决实时执行问题**（需要 fixup 执行的场景）：
- 用户执行：`ssh -L 8080:localhost:8080 -J jump1,jump2 user@server`
- 本地浏览器打开 `http://localhost:8080`
- 实时流式输出 fixup 执行日志

---

## 三、数据层设计

### 3.1 数据来源（只读，不改主程序）

所有数据从现有文件读取：

| 数据 | 来源文件 | 格式 |
|------|---------|------|
| 运行列表 | `main_reports/` 目录扫描 | 目录名含时间戳 |
| 本次运行概要 | `report_index_<ts>.txt` | `CATEGORY\|PATH\|ROWS\|DESCRIPTION` |
| 问题对象全量 | `migration_focus_<ts>.txt` | `\|` 分隔，两个 section |
| 各类型明细 | `missing_*_detail_<ts>.txt`、`unsupported_*_detail_<ts>.txt` | 同上格式 |
| fixup 执行状态 | `fixup_scripts/.fixup_state_ledger.json` | `{completed: {path: {fingerprint, updated_at}}}` |
| fixup 脚本列表 | `fixup_scripts/` 目录扫描 | `*.sql` 文件，`SCHEMA.OBJECT_NAME.sql` 命名 |
| 配置 | `config.ini` | INI 格式 |

### 3.2 migration_focus 解析规格

文件头注释：`# timestamp=...  # missing_supported=N  # unsupported_or_blocked=N`

**MISSING_SUPPORTED section** 字段：
`SRC_FULL | TYPE | TGT_FULL | ACTION | DETAIL`

**UNSUPPORTED_OR_BLOCKED section** 字段：
`SRC_FULL | TYPE | TGT_FULL | STATE | REASON_CODE | REASON | DEPENDENCY | ACTION | DETAIL`

STATE 枚举：`SUPPORTED`（缺失但可修补）/ `UNSUPPORTED`（不兼容）/ `BLOCKED`（依赖阻断）

REASON_CODE 常见值及展示文案映射：

| REASON_CODE | 用户展示文案 |
|-------------|-------------|
| `BLACKLIST_IOT` | 索引组织表（IOT），OceanBase 不支持 |
| `BLACKLIST_TEMPORARY_TABLE` | 全局临时表结构不兼容，需改写 |
| `VIEW_SYS_OBJ` | 视图引用了 Oracle 系统对象 |
| `DEPENDENCY_UNSUPPORTED` | 依赖对象不兼容，需先处理依赖 |
| `DEPENDENCY_TARGET_TABLE_MISSING` | 依赖的表尚未在目标端创建 |
| `BLACKLIST_DIY` | 自定义黑名单规则匹配 |
| `BLACKLIST_SPE` | 工具内置不支持规则匹配 |

### 3.3 fixup 状态账本格式

```json
{
  "version": 1,
  "completed": {
    "table/ZZ_APP.T_EMPLOYEE.sql": {
      "fingerprint": "<sha1_hex>",
      "updated_at": "2026-03-01 10:00:00",
      "note": "可选备注，最多300字符"
    }
  }
}
```

Key 为相对于 `fixup_dir` 的路径（正斜杠）。Fingerprint 是文件内容 SHA1；文件内容变化时账本记录失效，需重新执行。

---

## 四、前端设计

### 4.1 视觉风格（Apple-inspired）

```css
/* 核心设计令牌 */
--accent:        #007AFF;   /* Apple System Blue */
--success:       #34C759;   /* Apple Green */
--warning:       #FF9500;   /* Apple Orange */
--danger:        #FF3B30;   /* Apple Red */
--neutral:       #8E8E93;   /* Apple Gray */
--bg:            #F2F2F7;   /* iOS 系统背景 */
--card-bg:       #FFFFFF;
--border:        rgba(0,0,0,0.08);
--text-primary:  #1C1C1E;
--text-secondary:#6C6C70;
--radius:        12px;       /* 卡片圆角 */
--radius-sm:     8px;
--font:          -apple-system, BlinkMacSystemFont, 'PingFang SC', 'Helvetica Neue', sans-serif;
```

所有文字内容使用中文。状态徽章用颜色+图标组合（不依赖颜色单一区分）：
- `✓ 一致`（绿色）、`✗ 缺失`（红色）、`⚡ 不兼容`（橙色）、`⛔ 阻断`（红色深）、`＋ 多余`（蓝色）

### 4.2 页面结构

#### 页面 1：仪表盘（首页）

布局：顶部导航 + 四个统计卡 + 进度条 + 快捷操作按钮 + 最近运行列表

```
┌──────────────────────────────────────────────────────────────┐
│ ob-lens  [仪表盘] [问题] [执行] [历史] [设置]    v0.9.9.1    │
├──────────────────────────────────────────────────────────────┤
│ 最近运行：2026-03-01 09:33  ●需处理  用时 4m32s             │
│                                                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │
│  │  ✓  9   │ │  ✗  32  │ │  ⚡  18  │ │  ＋  2  │       │
│  │  一致   │ │  缺失   │ │ 不兼容  │ │  多余   │       │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │
│                                                              │
│  修补进度  ██████████░░░░░░░  14/52 可自动修补  (27%)        │
│                                                              │
│  [查看所有问题]  [执行修补脚本]  [运行新对比]  [配置向导]    │
└──────────────────────────────────────────────────────────────┘
```

**数据绑定**：从最新 `run_<ts>` 目录的 `report_index_<ts>.txt` + `migration_focus_<ts>.txt` 读取。

#### 页面 2：问题浏览器

布局：左侧筛选面板（固定宽度 220px）+ 右侧对象卡片列表

**筛选面板**：
- 搜索框（对象名/Schema，即时过滤）
- 状态分组：全部 / 缺失可修补 / 不兼容 / 依赖阻断 / 多余
- 类型多选：TABLE / VIEW / INDEX / CONSTRAINT / PROCEDURE / FUNCTION / TRIGGER / PACKAGE / SEQUENCE / SYNONYM / JOB / TYPE
- Schema 下拉

**对象卡片**（点击展开详情）：
```
ZZ_APP.T_PART_INTERVAL                    TABLE  ⚡ 不兼容
─────────────────────────────────────────────────────────────
  问题   INTERVAL 分区已自动转换为 RANGE 分区
  原因   OceanBase 4.x 不支持 INTERVAL 自动分区扩展语法
  影响   分区边界行为与 Oracle 不同，写入数据前请与业务方确认
  脚本   fixup_scripts/table/ZZ_APP.T_PART_INTERVAL.sql
         ⚠ DDL_REWRITE：含语义改写，建议人工复核后再执行
  依赖   —

  [预览 DDL ▾]  [执行修补]  [✓ 标记已处理]  [📝 备注]
```

详情卡包含字段：SRC_FULL、TYPE、STATE、REASON_CODE（展示友好文案）、REASON、DEPENDENCY（如有，链接到依赖对象）、ACTION、DETAIL、对应 fixup 脚本路径、脚本执行状态（来自账本）、DDL 内容预览（折叠）。

**BLOCKED 对象**需特别展示依赖链：
```
ZZ_APP.IDX_EMP_01          INDEX  ⛔ 阻断
─────────────────────────────────────────
  阻断原因  依赖的 ZZ_APP.T_EMPLOYEE 尚未创建
  处理建议  先执行 ZZ_APP.T_EMPLOYEE 的修补脚本
  [跳转到 ZZ_APP.T_EMPLOYEE →]
```

#### 页面 3：修补执行器（模式②专属，模式①显示为灰色并提示启动服务器）

布局：执行选项区 + 进度区 + 脚本列表（实时更新）

**执行选项**：
- 模式选择：`● 智能顺序（推荐）` / `○ 仅指定类型` / `○ 仅失败项重试`
- 高级选项（折叠）：`☐ 允许创建 TABLE`（对应 `--allow-table-create`）、`☐ 执行后重编译`（对应 `--recompile`）、`☐ 迭代模式`（对应 `--iterative`）
- [开始执行] 按钮（确认对话框：列出将执行的目录和文件数）

**进度区**：
```
待执行 47   已完成 12   失败 3   跳过 5
████████████░░░░░░░░░░░  12/47

正在执行：view/ZZ_APP.V_EMPLOYEE_DETAIL.sql
```

**脚本列表**（实时追加）：
```
✓  table/ZZ_APP.T_EMPLOYEE.sql                2s
✓  table/ZZ_APP.T_DEPARTMENT.sql              1s
✗  index/ZZ_APP.IDX_EMP_01.sql   [查看输出 ▾]
   ORA-00942: table or view does not exist
   → 该索引依赖的表 ZZ_APP.T_EMPLOYEE 尚未创建
   → 建议：启用「允许创建 TABLE」后重试
```

服务器通过 SSE（Server-Sent Events）推送执行日志，前端增量追加，无需轮询。

#### 页面 4：配置向导

5 步线性流程，每步只展示关键字段：

| 步骤 | 字段 |
|------|------|
| 1. Oracle 连接 | host, port, service_name, user, password + [测试连接] |
| 2. OceanBase 连接 | executable, host, port, user_string, password + [测试连接] |
| 3. Schema 选择 | source_schemas（从 Oracle 读取列表，多选），remap_file（可选） |
| 4. 运行模式 | 三个预设：「快速检查」/「完整检查（默认）」/「自定义」。自定义展开所有选项 |
| 5. 确认 | 展示完整 config.ini 预览，[保存并运行] |

密码写入 config.ini（与现有行为一致）。向导在保存时提示用户：可选地将密码改为环境变量引用（`${COMPARATOR_ORA_PWD}`），以避免明文存储；不强制。

#### 页面 5：历史运行对比

运行列表 + 趋势折线图（Canvas 绘制，无外部依赖）：
- X 轴：日期，Y 轴：缺失对象数 / 不兼容数 / 已修补数
- 点击任意运行 → 跳转到该运行的报告视图
- 支持两个运行互相 Diff：选中两行 → 显示哪些对象状态发生了变化

---

## 五、后端设计（模式②服务器）

### 5.1 路由

| 路径 | 方法 | 说明 |
|------|------|------|
| `GET /` | GET | 返回主 HTML（单页应用） |
| `GET /api/runs` | GET | 返回所有运行列表（`[{run_id, ts, status, counts}]`） |
| `GET /api/runs/{run_id}` | GET | 返回指定运行的完整数据（报告 + 问题列表） |
| `GET /api/runs/{run_id}/objects` | GET | 对象列表，支持 `?state=&type=&schema=&q=` 过滤 |
| `GET /api/fixup/status` | GET | fixup 状态账本内容 |
| `GET /api/fixup/scripts` | GET | 所有 fixup 脚本列表及执行状态 |
| `POST /api/fixup/run` | POST | 启动 fixup 执行（body: `{mode, options}`） |
| `GET /api/fixup/stream` | GET | SSE 端点，流式返回执行日志 |
| `GET /api/config` | GET | 读取当前 config.ini（密码字段脱敏） |
| `POST /api/config` | POST | 保存配置并验证连接 |
| `POST /api/compare/run` | POST | 触发主程序运行（`schema_diff_reconciler.py`） |

### 5.2 安全性

- 服务器**只绑定 localhost**（`127.0.0.1:8080`），不监听外网接口
- 不记录密码到日志
- fixup 执行前显示确认对话框，列出将执行的命令
- `--allow-table-create` 等破坏性选项默认关闭，UI 上有醒目警告

---

## 六、文件结构

```
ob_lens.py                    # 入口（~50行）：解析参数，选择模式①或模式②
ob_lens/
  __init__.py
  report_builder.py           # 模式①：解析报告文件 → 生成自包含 HTML
  server.py                   # 模式②：HTTP 服务器（标准库 http.server 扩展）
  data/
    run_reader.py             # 读取 main_reports/ 目录，解析 report_index + migration_focus
    fixup_reader.py           # 读取 fixup_scripts/ 目录 + 状态账本
    config_reader.py          # 读写 config.ini
  static/
    template.html             # HTML 模板（含完整 Apple 风格 CSS + JS）
    app.js                    # 前端逻辑（Vanilla JS，~800行）
    style.css                 # 样式（~400行）
```

**HTML 生成逻辑**：`report_builder.py` 读取 run 目录所有数据 → 序列化为 JSON → 读取 `template.html`（其中 `style.css` 和 `app.js` 的内容在此步骤内联替换占位符）→ 注入数据 JSON → 输出单一 `.html` 文件。最终产出的 HTML 文件零外部依赖，无需网络请求。

---

## 七、与主程序的集成点

`ob-lens` 不修改主程序任何代码。集成通过以下方式：

1. **自动触发报告生成（可选）**：在 `schema_diff_reconciler.py` 最后一行后，ob-lens 检测 run 目录新建后自动执行报告生成。实际方式：用户在 wrapper 脚本中调用：
   ```bash
   python3 schema_diff_reconciler.py && python3 ob_lens.py
   ```

2. **fixup 执行**：模式②调用 `run_fixup.py` 作为子进程，捕获 stdout/stderr 通过 SSE 推送。

3. **主程序触发**：模式②的「运行新对比」按钮调用 `schema_diff_reconciler.py` 子进程，同样 SSE 推送进度。

---

## 八、分阶段交付

### Phase 1（核心价值，优先）

- `run_reader.py`：解析 run 目录数据
- `report_builder.py`：生成自包含 HTML
- HTML 模板：仪表盘 + 问题浏览器
- 集成点：运行结束后自动生成 HTML

**交付物**：运行 `python3 ob_lens.py` 后在 run 目录生成可用的 HTML 报告。

### Phase 2（执行层）

- `server.py`：模式② HTTP 服务器
- fixup 执行器界面
- SSE 实时日志推送
- 状态账本读写

**交付物**：`python3 ob_lens.py --serve` 启动后可在浏览器执行 fixup 脚本。

### Phase 3（完善）

- 配置向导
- 历史运行对比 + 趋势图
- 主程序触发执行
- 对象备注功能（写入轻量 JSON 旁注文件）

---

## 九、成功指标

- 新工程师能在不阅读任何文档的情况下，通过 UI 完成「找到所有问题对象并理解原因」
- "xxx 没生成是什么原因" 类问题减少至 0（原因在 UI 里直接可见）
- "xxx 在哪" 类问题减少至 0（搜索框一步到位）
- 配置向导完成率：5 步向导不需要查阅 readme_config.txt
