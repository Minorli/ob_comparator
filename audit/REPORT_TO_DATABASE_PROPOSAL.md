# 报告存储到数据库功能提案

**提案日期**: 2024年  
**功能类型**: 新增功能  
**影响范围**: 报告生成模块（可选启用，不影响原有功能）

---

## 一、功能概述

### 1.1 需求背景

当前报告仅以文本文件形式输出到本地目录，存在以下局限：
- 难以进行历史趋势分析
- 跨运行对比需要手动解析文件
- 团队共享需要传输文件
- 无法与监控/告警系统集成

### 1.2 功能目标

新增可选功能，将校验报告存储到 OceanBase 数据库中：

| 特性 | 说明 |
|-----|------|
| **开关控制** | 默认关闭，通过配置项 `report_to_db` 启用 |
| **自动建表** | 首次启用时自动创建所需表结构 |
| **保留原功能** | 与现有文件报告并行，不互相影响 |
| **运行隔离** | 每次运行使用唯一 `report_id`，互不干扰 |
| **向后兼容** | 不修改现有函数签名和输出格式 |

---

## 二、配置设计

### 2.1 新增配置项

在 `config.ini` 的 `[SETTINGS]` 部分新增：

```ini
[SETTINGS]
# ... 现有配置 ...

# ============ 报告存储到数据库 (可选) ============
# 是否将报告存储到 OceanBase 数据库，默认 false
report_to_db = false

# 存储报告的目标 schema（需要有建表和写入权限）
# 如果不指定，使用 OCEANBASE_TARGET 配置的用户默认 schema
report_db_schema = 

# 报告保留天数，超过此天数的历史报告将在每次运行时自动清理
# 设为 0 表示不自动清理
report_retention_days = 90

# 是否在存库失败时终止程序，默认 false（仅记录警告，继续执行）
report_db_fail_abort = false
```

### 2.2 配置解析逻辑

```python
# 在 load_config() 函数中新增
settings['report_to_db'] = config.getboolean('SETTINGS', 'report_to_db', fallback=False)
settings['report_db_schema'] = config.get('SETTINGS', 'report_db_schema', fallback='').strip().upper()
settings['report_retention_days'] = config.getint('SETTINGS', 'report_retention_days', fallback=90)
settings['report_db_fail_abort'] = config.getboolean('SETTINGS', 'report_db_fail_abort', fallback=False)
```

---

## 三、数据库表结构设计

### 3.1 表设计原则

1. **运行隔离**: 使用 `report_id` 作为每次运行的唯一标识
2. **查询友好**: 关键字段建立索引，支持高效查询
3. **扩展性**: 使用 CLOB 存储 JSON 格式详细数据
4. **兼容性**: 使用 OceanBase Oracle 模式兼容的语法

### 3.2 主报告汇总表

```sql
CREATE TABLE OB_COMPARE_REPORT_SUMMARY (
    -- 主键
    REPORT_ID           VARCHAR2(64) NOT NULL,
    
    -- 运行时间信息
    RUN_TIMESTAMP       TIMESTAMP NOT NULL,
    RUN_DATE            DATE GENERATED ALWAYS AS (TRUNC(RUN_TIMESTAMP)) VIRTUAL,
    DURATION_SECONDS    NUMBER(10,2),
    
    -- 源端信息
    SOURCE_HOST         VARCHAR2(256),
    SOURCE_PORT         NUMBER,
    SOURCE_SERVICE      VARCHAR2(128),
    SOURCE_USER         VARCHAR2(128),
    SOURCE_SCHEMAS      VARCHAR2(4000),    -- 逗号分隔的 schema 列表
    
    -- 目标端信息
    TARGET_HOST         VARCHAR2(256),
    TARGET_PORT         NUMBER,
    TARGET_TENANT       VARCHAR2(128),
    TARGET_USER         VARCHAR2(128),
    TARGET_SCHEMAS      VARCHAR2(4000),    -- 逗号分隔的 schema 列表
    
    -- 校验配置
    CHECK_PRIMARY_TYPES VARCHAR2(512),     -- 如 TABLE,VIEW,SYNONYM
    CHECK_EXTRA_TYPES   VARCHAR2(512),     -- 如 INDEX,CONSTRAINT,TRIGGER,SEQUENCE
    FIXUP_ENABLED       NUMBER(1) DEFAULT 0,
    GRANT_ENABLED       NUMBER(1) DEFAULT 0,
    
    -- 校验结果统计
    TOTAL_CHECKED       NUMBER DEFAULT 0,
    MISSING_COUNT       NUMBER DEFAULT 0,
    MISMATCHED_COUNT    NUMBER DEFAULT 0,
    OK_COUNT            NUMBER DEFAULT 0,
    SKIPPED_COUNT       NUMBER DEFAULT 0,
    UNSUPPORTED_COUNT   NUMBER DEFAULT 0,
    
    -- 扩展对象统计
    INDEX_MISSING       NUMBER DEFAULT 0,
    INDEX_MISMATCHED    NUMBER DEFAULT 0,
    CONSTRAINT_MISSING  NUMBER DEFAULT 0,
    CONSTRAINT_MISMATCH NUMBER DEFAULT 0,
    TRIGGER_MISSING     NUMBER DEFAULT 0,
    SEQUENCE_MISSING    NUMBER DEFAULT 0,
    
    -- 结论
    CONCLUSION          VARCHAR2(32),      -- PASS / FAIL / WARN
    CONCLUSION_DETAIL   VARCHAR2(1000),
    
    -- 完整报告 JSON（用于详细分析）
    FULL_REPORT_JSON    CLOB,
    
    -- 元数据
    TOOL_VERSION        VARCHAR2(64),
    HOSTNAME            VARCHAR2(256),     -- 运行程序的主机名
    CREATED_AT          TIMESTAMP DEFAULT SYSTIMESTAMP,
    
    CONSTRAINT PK_REPORT_SUMMARY PRIMARY KEY (REPORT_ID)
);

-- 索引
CREATE INDEX IDX_REPORT_TIMESTAMP ON OB_COMPARE_REPORT_SUMMARY(RUN_TIMESTAMP DESC);
CREATE INDEX IDX_REPORT_DATE ON OB_COMPARE_REPORT_SUMMARY(RUN_DATE);
CREATE INDEX IDX_REPORT_CONCLUSION ON OB_COMPARE_REPORT_SUMMARY(CONCLUSION);
CREATE INDEX IDX_REPORT_SOURCE ON OB_COMPARE_REPORT_SUMMARY(SOURCE_HOST, SOURCE_SCHEMAS);

-- 表注释
COMMENT ON TABLE OB_COMPARE_REPORT_SUMMARY IS 'OB Comparator 校验报告汇总表';
COMMENT ON COLUMN OB_COMPARE_REPORT_SUMMARY.REPORT_ID IS '报告唯一标识 (格式: YYYYMMDD_HHMMSS_UUID8)';
COMMENT ON COLUMN OB_COMPARE_REPORT_SUMMARY.CONCLUSION IS '校验结论: PASS-全部通过, FAIL-存在缺失, WARN-存在警告';
```

### 3.3 子报告明细表

```sql
CREATE TABLE OB_COMPARE_REPORT_DETAIL (
    -- 主键
    DETAIL_ID           VARCHAR2(64) NOT NULL,
    REPORT_ID           VARCHAR2(64) NOT NULL,
    
    -- 报告分类
    REPORT_TYPE         VARCHAR2(64) NOT NULL,   -- MISSING / MISMATCHED / UNSUPPORTED / OK / SKIPPED
    OBJECT_TYPE         VARCHAR2(32) NOT NULL,   -- TABLE / VIEW / INDEX / CONSTRAINT / TRIGGER / SEQUENCE / SYNONYM
    SUB_TYPE            VARCHAR2(64),            -- 如 BITMAP_INDEX, CHECK_CONSTRAINT 等
    
    -- 对象信息
    SOURCE_SCHEMA       VARCHAR2(128),
    SOURCE_NAME         VARCHAR2(128),
    TARGET_SCHEMA       VARCHAR2(128),
    TARGET_NAME         VARCHAR2(128),
    
    -- 状态和原因
    STATUS              VARCHAR2(32),            -- 具体状态码
    REASON              VARCHAR2(1000),          -- 原因描述
    BLACKLIST_REASON    VARCHAR2(256),           -- 如果是黑名单导致，记录原因
    
    -- 详细信息（JSON 格式）
    DETAIL_JSON         CLOB,
    
    -- 元数据
    CREATED_AT          TIMESTAMP DEFAULT SYSTIMESTAMP,
    
    CONSTRAINT PK_REPORT_DETAIL PRIMARY KEY (DETAIL_ID),
    CONSTRAINT FK_REPORT_DETAIL_SUMMARY FOREIGN KEY (REPORT_ID) 
        REFERENCES OB_COMPARE_REPORT_SUMMARY(REPORT_ID) ON DELETE CASCADE
);

-- 索引
CREATE INDEX IDX_DETAIL_REPORT_ID ON OB_COMPARE_REPORT_DETAIL(REPORT_ID);
CREATE INDEX IDX_DETAIL_TYPE ON OB_COMPARE_REPORT_DETAIL(REPORT_TYPE, OBJECT_TYPE);
CREATE INDEX IDX_DETAIL_SOURCE ON OB_COMPARE_REPORT_DETAIL(SOURCE_SCHEMA, SOURCE_NAME);
CREATE INDEX IDX_DETAIL_TARGET ON OB_COMPARE_REPORT_DETAIL(TARGET_SCHEMA, TARGET_NAME);

-- 表注释
COMMENT ON TABLE OB_COMPARE_REPORT_DETAIL IS 'OB Comparator 校验报告明细表';
COMMENT ON COLUMN OB_COMPARE_REPORT_DETAIL.REPORT_TYPE IS '报告类型: MISSING-缺失, MISMATCHED-不匹配, UNSUPPORTED-不支持, OK-正常, SKIPPED-跳过';
```

### 3.4 授权明细表（可选）

```sql
CREATE TABLE OB_COMPARE_REPORT_GRANTS (
    GRANT_ID            VARCHAR2(64) NOT NULL,
    REPORT_ID           VARCHAR2(64) NOT NULL,
    
    -- 授权类型
    GRANT_TYPE          VARCHAR2(32) NOT NULL,   -- OBJECT / SYSTEM / ROLE
    
    -- 授权信息
    GRANTEE             VARCHAR2(128) NOT NULL,
    GRANTOR             VARCHAR2(128),
    PRIVILEGE           VARCHAR2(64),
    TARGET_SCHEMA       VARCHAR2(128),
    TARGET_NAME         VARCHAR2(128),
    TARGET_TYPE         VARCHAR2(32),
    WITH_GRANT_OPTION   NUMBER(1) DEFAULT 0,
    
    -- 状态
    STATUS              VARCHAR2(32),            -- PLANNED / FILTERED / SKIPPED
    FILTER_REASON       VARCHAR2(256),
    
    CREATED_AT          TIMESTAMP DEFAULT SYSTIMESTAMP,
    
    CONSTRAINT PK_REPORT_GRANTS PRIMARY KEY (GRANT_ID),
    CONSTRAINT FK_REPORT_GRANTS_SUMMARY FOREIGN KEY (REPORT_ID) 
        REFERENCES OB_COMPARE_REPORT_SUMMARY(REPORT_ID) ON DELETE CASCADE
);

CREATE INDEX IDX_GRANTS_REPORT_ID ON OB_COMPARE_REPORT_GRANTS(REPORT_ID);
CREATE INDEX IDX_GRANTS_GRANTEE ON OB_COMPARE_REPORT_GRANTS(GRANTEE);
```

---

## 四、核心实现

### 4.1 新增常量和类型定义

```python
# ============ 报告存库相关常量 ============
REPORT_DB_VERSION = "1.0.0"

REPORT_TABLES = {
    "summary": "OB_COMPARE_REPORT_SUMMARY",
    "detail": "OB_COMPARE_REPORT_DETAIL",
    "grants": "OB_COMPARE_REPORT_GRANTS"
}

REPORT_TYPE_MAPPING = {
    "missing": "MISSING",
    "mismatched": "MISMATCHED",
    "ok": "OK",
    "skipped": "SKIPPED",
    "unsupported": "UNSUPPORTED"
}
```

### 4.2 自动建表函数

```python
def ensure_report_tables_exist(
    ob_cfg: ObConfig,
    target_schema: str
) -> Tuple[bool, str]:
    """
    检查并自动创建报告存储表。
    
    Args:
        ob_cfg: OceanBase 连接配置
        target_schema: 目标 schema（如果为空则使用连接用户默认 schema）
    
    Returns:
        (success, error_message)
    """
    schema_prefix = f"{target_schema}." if target_schema else ""
    
    # 检查表是否存在
    check_sql = f"""
    SELECT TABLE_NAME 
    FROM ALL_TABLES 
    WHERE OWNER = NVL('{target_schema}', USER)
      AND TABLE_NAME IN ('OB_COMPARE_REPORT_SUMMARY', 'OB_COMPARE_REPORT_DETAIL', 'OB_COMPARE_REPORT_GRANTS')
    """
    
    ok, lines, err = obclient_run_sql(ob_cfg, check_sql)
    if not ok:
        return False, f"检查报告表失败: {err}"
    
    existing_tables = set(line.strip().upper() for line in lines if line.strip())
    
    tables_to_create = []
    if "OB_COMPARE_REPORT_SUMMARY" not in existing_tables:
        tables_to_create.append(("summary", DDL_CREATE_SUMMARY_TABLE))
    if "OB_COMPARE_REPORT_DETAIL" not in existing_tables:
        tables_to_create.append(("detail", DDL_CREATE_DETAIL_TABLE))
    if "OB_COMPARE_REPORT_GRANTS" not in existing_tables:
        tables_to_create.append(("grants", DDL_CREATE_GRANTS_TABLE))
    
    if not tables_to_create:
        log.debug("[REPORT_DB] 报告表已存在，无需创建。")
        return True, ""
    
    log.info("[REPORT_DB] 开始创建报告表: %s", [t[0] for t in tables_to_create])
    
    for table_name, ddl_template in tables_to_create:
        ddl = ddl_template.format(schema=schema_prefix)
        ok, _, err = obclient_run_sql(ob_cfg, ddl)
        if not ok:
            return False, f"创建表 {table_name} 失败: {err}"
        log.info("[REPORT_DB] 已创建表: %s%s", schema_prefix, REPORT_TABLES[table_name])
    
    return True, ""
```

### 4.3 报告 ID 生成

```python
import uuid
import socket

def generate_report_id(timestamp: str) -> str:
    """
    生成唯一的报告 ID。
    
    格式: YYYYMMDD_HHMMSS_<8位UUID>
    示例: 20240203_134500_a1b2c3d4
    
    Args:
        timestamp: 运行时间戳 (格式: YYYYMMDD_HHMMSS)
    
    Returns:
        唯一的报告 ID
    """
    short_uuid = uuid.uuid4().hex[:8]
    return f"{timestamp}_{short_uuid}"
```

### 4.4 主报告写入函数

```python
def save_report_summary_to_db(
    ob_cfg: ObConfig,
    report_id: str,
    run_summary: RunSummary,
    tv_results: ReportResults,
    extra_results: ExtraCheckResults,
    endpoint_info: Dict[str, str],
    settings: Dict,
    full_report_json: Optional[str] = None
) -> Tuple[bool, str]:
    """
    将主报告汇总写入数据库。
    
    Args:
        ob_cfg: OceanBase 连接配置
        report_id: 报告唯一 ID
        run_summary: 运行摘要
        tv_results: 主对象校验结果
        extra_results: 扩展对象校验结果
        endpoint_info: 端点信息
        settings: 配置
        full_report_json: 完整报告的 JSON 字符串（可选）
    
    Returns:
        (success, error_message)
    """
    schema_prefix = settings.get('report_db_schema', '')
    if schema_prefix:
        schema_prefix = f"{schema_prefix}."
    
    # 构建统计数据
    missing_count = len(tv_results.get("missing", []))
    mismatched_count = len(tv_results.get("mismatched", []))
    ok_count = len(tv_results.get("ok", []))
    skipped_count = len(tv_results.get("skipped", []))
    
    # 计算结论
    if missing_count == 0 and mismatched_count == 0:
        conclusion = "PASS"
        conclusion_detail = "所有校验对象均已通过"
    elif missing_count > 0:
        conclusion = "FAIL"
        conclusion_detail = f"存在 {missing_count} 个缺失对象"
    else:
        conclusion = "WARN"
        conclusion_detail = f"存在 {mismatched_count} 个不匹配对象"
    
    # 转义 JSON 中的单引号
    safe_json = (full_report_json or "{}").replace("'", "''")
    
    insert_sql = f"""
    INSERT INTO {schema_prefix}OB_COMPARE_REPORT_SUMMARY (
        REPORT_ID, RUN_TIMESTAMP, DURATION_SECONDS,
        SOURCE_HOST, SOURCE_PORT, SOURCE_SERVICE, SOURCE_USER, SOURCE_SCHEMAS,
        TARGET_HOST, TARGET_PORT, TARGET_TENANT, TARGET_USER, TARGET_SCHEMAS,
        CHECK_PRIMARY_TYPES, CHECK_EXTRA_TYPES, FIXUP_ENABLED, GRANT_ENABLED,
        TOTAL_CHECKED, MISSING_COUNT, MISMATCHED_COUNT, OK_COUNT, SKIPPED_COUNT,
        INDEX_MISSING, INDEX_MISMATCHED, CONSTRAINT_MISSING, CONSTRAINT_MISMATCH,
        TRIGGER_MISSING, SEQUENCE_MISSING,
        CONCLUSION, CONCLUSION_DETAIL, FULL_REPORT_JSON,
        TOOL_VERSION, HOSTNAME
    ) VALUES (
        '{report_id}',
        TO_TIMESTAMP('{run_summary.timestamp}', 'YYYYMMDD_HH24MISS'),
        {run_summary.total_duration_seconds:.2f},
        '{endpoint_info.get("source_host", "")}',
        {endpoint_info.get("source_port", 0)},
        '{endpoint_info.get("source_service", "")}',
        '{endpoint_info.get("source_user", "")}',
        '{",".join(settings.get("source_schemas_list", []))}',
        '{endpoint_info.get("target_host", "")}',
        {endpoint_info.get("target_port", 0)},
        '{endpoint_info.get("target_tenant", "")}',
        '{endpoint_info.get("target_user", "")}',
        '{",".join(sorted(settings.get("target_schemas", set())))}',
        '{",".join(settings.get("check_primary_types", []))}',
        '{",".join(settings.get("check_extra_types", []))}',
        {1 if settings.get("fixup_enabled", False) else 0},
        {1 if settings.get("generate_grants", False) else 0},
        {run_summary.total_checked},
        {missing_count},
        {mismatched_count},
        {ok_count},
        {skipped_count},
        {len(extra_results.get("index_mismatched", []))},
        0,
        {len(extra_results.get("constraint_mismatched", []))},
        0,
        {len(extra_results.get("trigger_mismatched", []))},
        {len(extra_results.get("sequence_mismatched", []))},
        '{conclusion}',
        '{conclusion_detail}',
        '{safe_json}',
        '{REPORT_DB_VERSION}',
        '{socket.gethostname()}'
    )
    """
    
    ok, _, err = obclient_run_sql(ob_cfg, insert_sql)
    if not ok:
        return False, f"写入主报告失败: {err}"
    
    return True, ""
```

### 4.5 明细报告写入函数

```python
def save_report_details_to_db(
    ob_cfg: ObConfig,
    report_id: str,
    tv_results: ReportResults,
    extra_results: ExtraCheckResults,
    settings: Dict
) -> Tuple[bool, str]:
    """
    将明细报告批量写入数据库。
    
    Args:
        ob_cfg: OceanBase 连接配置
        report_id: 报告唯一 ID
        tv_results: 主对象校验结果
        extra_results: 扩展对象校验结果
        settings: 配置
    
    Returns:
        (success, error_message)
    """
    schema_prefix = settings.get('report_db_schema', '')
    if schema_prefix:
        schema_prefix = f"{schema_prefix}."
    
    detail_rows = []
    
    # 处理主对象结果
    for report_type in ("missing", "mismatched", "ok", "skipped"):
        items = tv_results.get(report_type, [])
        for item in items:
            detail_id = f"{report_id}_{uuid.uuid4().hex[:12]}"
            # 解析 item 结构，根据实际数据格式调整
            if isinstance(item, tuple) and len(item) >= 3:
                obj_type, src_full, tgt_full = item[:3]
                src_schema, src_name = src_full.split(".", 1) if "." in src_full else ("", src_full)
                tgt_schema, tgt_name = tgt_full.split(".", 1) if "." in str(tgt_full) else ("", str(tgt_full))
            else:
                continue
            
            detail_rows.append({
                "detail_id": detail_id,
                "report_id": report_id,
                "report_type": REPORT_TYPE_MAPPING.get(report_type, report_type.upper()),
                "object_type": obj_type,
                "source_schema": src_schema,
                "source_name": src_name,
                "target_schema": tgt_schema,
                "target_name": tgt_name
            })
    
    # 处理扩展对象结果（索引、约束、触发器、序列）
    for key, report_type in [
        ("index_mismatched", "MISMATCHED"),
        ("constraint_mismatched", "MISMATCHED"),
        ("trigger_mismatched", "MISMATCHED"),
        ("sequence_mismatched", "MISMATCHED")
    ]:
        items = extra_results.get(key, [])
        obj_type = key.split("_")[0].upper()
        for item in items:
            detail_id = f"{report_id}_{uuid.uuid4().hex[:12]}"
            detail_rows.append({
                "detail_id": detail_id,
                "report_id": report_id,
                "report_type": report_type,
                "object_type": obj_type,
                "source_schema": getattr(item, 'owner', ''),
                "source_name": getattr(item, 'name', ''),
                "target_schema": "",
                "target_name": "",
                "reason": getattr(item, 'reason', '')
            })
    
    # 批量插入
    if detail_rows:
        batch_size = 100
        total_inserted = 0
        for i in range(0, len(detail_rows), batch_size):
            batch = detail_rows[i:i+batch_size]
            values_list = []
            for row in batch:
                values_list.append(f"""
                ('{row["detail_id"]}', '{row["report_id"]}', 
                 '{row["report_type"]}', '{row["object_type"]}', NULL,
                 '{row.get("source_schema", "")}', '{row.get("source_name", "")}',
                 '{row.get("target_schema", "")}', '{row.get("target_name", "")}',
                 NULL, '{row.get("reason", "")}', NULL, NULL)
                """)
            
            insert_sql = f"""
            INSERT INTO {schema_prefix}OB_COMPARE_REPORT_DETAIL 
            (DETAIL_ID, REPORT_ID, REPORT_TYPE, OBJECT_TYPE, SUB_TYPE,
             SOURCE_SCHEMA, SOURCE_NAME, TARGET_SCHEMA, TARGET_NAME,
             STATUS, REASON, BLACKLIST_REASON, DETAIL_JSON)
            VALUES {",".join(values_list)}
            """
            
            ok, _, err = obclient_run_sql(ob_cfg, insert_sql)
            if not ok:
                log.warning("[REPORT_DB] 批量插入明细失败 (batch %d): %s", i // batch_size, err)
            else:
                total_inserted += len(batch)
        
        log.info("[REPORT_DB] 已写入 %d 条明细记录", total_inserted)
    
    return True, ""
```

### 4.6 历史报告清理函数

```python
def cleanup_old_reports(
    ob_cfg: ObConfig,
    retention_days: int,
    target_schema: str
) -> int:
    """
    清理超过保留天数的历史报告。
    
    Args:
        ob_cfg: OceanBase 连接配置
        retention_days: 保留天数
        target_schema: 目标 schema
    
    Returns:
        清理的报告数量
    """
    if retention_days <= 0:
        return 0
    
    schema_prefix = f"{target_schema}." if target_schema else ""
    
    # 先统计要清理的数量
    count_sql = f"""
    SELECT COUNT(*) FROM {schema_prefix}OB_COMPARE_REPORT_SUMMARY
    WHERE RUN_TIMESTAMP < SYSTIMESTAMP - INTERVAL '{retention_days}' DAY
    """
    
    ok, lines, _ = obclient_run_sql(ob_cfg, count_sql)
    if not ok or not lines:
        return 0
    
    try:
        count = int(lines[0].strip())
    except (ValueError, IndexError):
        count = 0
    
    if count == 0:
        return 0
    
    # 执行清理（级联删除会自动清理 detail 和 grants 表）
    delete_sql = f"""
    DELETE FROM {schema_prefix}OB_COMPARE_REPORT_SUMMARY
    WHERE RUN_TIMESTAMP < SYSTIMESTAMP - INTERVAL '{retention_days}' DAY
    """
    
    ok, _, err = obclient_run_sql(ob_cfg, delete_sql)
    if ok:
        log.info("[REPORT_DB] 已清理 %d 条过期报告 (保留 %d 天)", count, retention_days)
        return count
    else:
        log.warning("[REPORT_DB] 清理过期报告失败: %s", err)
        return 0
```

### 4.7 主入口函数

```python
def save_report_to_database(
    ob_cfg: ObConfig,
    run_summary: RunSummary,
    tv_results: ReportResults,
    extra_results: ExtraCheckResults,
    endpoint_info: Dict[str, str],
    settings: Dict,
    grant_plan: Optional[GrantPlan] = None
) -> Tuple[bool, Optional[str]]:
    """
    将报告保存到 OceanBase 数据库（主入口函数）。
    
    此函数是对外暴露的主入口，内部协调建表、写入、清理等操作。
    
    Args:
        ob_cfg: OceanBase 连接配置
        run_summary: 运行摘要
        tv_results: 主对象校验结果
        extra_results: 扩展对象校验结果
        endpoint_info: 端点信息
        settings: 配置
        grant_plan: 授权计划（可选）
    
    Returns:
        (success, report_id or error_message)
    """
    if not settings.get("report_to_db", False):
        return True, None  # 功能未启用，直接返回成功
    
    log.info("[REPORT_DB] 开始将报告写入数据库...")
    
    target_schema = settings.get("report_db_schema", "")
    
    # 1. 确保表存在
    ok, err = ensure_report_tables_exist(ob_cfg, target_schema)
    if not ok:
        log.error("[REPORT_DB] 创建报告表失败: %s", err)
        if settings.get("report_db_fail_abort", False):
            return False, err
        return True, None  # 容错模式，不影响主流程
    
    # 2. 生成报告 ID
    report_id = generate_report_id(run_summary.timestamp)
    log.info("[REPORT_DB] 报告 ID: %s", report_id)
    
    # 3. 写入主报告
    full_json = json.dumps({
        "missing": tv_results.get("missing", []),
        "mismatched": tv_results.get("mismatched", []),
        "ok_count": len(tv_results.get("ok", [])),
        "skipped_count": len(tv_results.get("skipped", []))
    }, ensure_ascii=False, default=str)
    
    ok, err = save_report_summary_to_db(
        ob_cfg, report_id, run_summary, tv_results, extra_results,
        endpoint_info, settings, full_json
    )
    if not ok:
        log.error("[REPORT_DB] 写入主报告失败: %s", err)
        if settings.get("report_db_fail_abort", False):
            return False, err
        return True, None
    
    # 4. 写入明细报告
    save_report_details_to_db(ob_cfg, report_id, tv_results, extra_results, settings)
    
    # 5. 写入授权明细（如果有）
    if grant_plan:
        save_grant_details_to_db(ob_cfg, report_id, grant_plan, settings)
    
    # 6. 清理历史报告
    retention_days = settings.get("report_retention_days", 90)
    if retention_days > 0:
        cleanup_old_reports(ob_cfg, retention_days, target_schema)
    
    log.info("[REPORT_DB] 报告已成功写入数据库, report_id=%s", report_id)
    return True, report_id
```

---

## 五、调用位置

### 5.1 在 `print_final_report()` 函数末尾调用

```python
def print_final_report(
    tv_results: ReportResults,
    # ... 其他参数 ...
    settings: Dict,
    # ... 其他参数 ...
    run_summary_ctx: RunSummaryContext,
    # ... 其他参数 ...
) -> Optional[RunSummary]:
    """生成并打印最终报告"""
    
    # ... 现有报告生成逻辑 ...
    
    # ========== 新增: 报告存库 ==========
    # 在现有报告生成完成后，额外写入数据库（如果启用）
    if settings.get("report_to_db", False):
        try:
            # 需要从上下文获取 ob_cfg
            ob_cfg = settings.get("_ob_cfg_for_report")  # 需要在调用前注入
            if ob_cfg:
                db_ok, db_result = save_report_to_database(
                    ob_cfg,
                    run_summary,
                    tv_results,
                    extra_results,
                    endpoint_info,
                    settings,
                    grant_plan
                )
                if db_ok and db_result:
                    log.info("报告已同步到数据库: report_id=%s", db_result)
        except Exception as e:
            log.warning("报告写入数据库时发生异常: %s", e)
            if settings.get("report_db_fail_abort", False):
                raise
    # ========== 新增结束 ==========
    
    return run_summary
```

---

## 六、查询示例

### 6.1 查看最近运行记录

```sql
SELECT REPORT_ID, RUN_TIMESTAMP, DURATION_SECONDS,
       SOURCE_SCHEMAS, TARGET_SCHEMAS,
       TOTAL_CHECKED, MISSING_COUNT, MISMATCHED_COUNT,
       CONCLUSION
FROM OB_COMPARE_REPORT_SUMMARY
ORDER BY RUN_TIMESTAMP DESC
FETCH FIRST 10 ROWS ONLY;
```

### 6.2 查看特定运行的缺失对象

```sql
SELECT OBJECT_TYPE, SOURCE_SCHEMA, SOURCE_NAME, REASON
FROM OB_COMPARE_REPORT_DETAIL
WHERE REPORT_ID = '20240203_134500_a1b2c3d4'
  AND REPORT_TYPE = 'MISSING'
ORDER BY OBJECT_TYPE, SOURCE_SCHEMA, SOURCE_NAME;
```

### 6.3 统计每日校验趋势

```sql
SELECT RUN_DATE,
       COUNT(*) AS RUN_COUNT,
       SUM(TOTAL_CHECKED) AS TOTAL_OBJECTS,
       SUM(MISSING_COUNT) AS TOTAL_MISSING,
       ROUND(AVG(DURATION_SECONDS), 2) AS AVG_DURATION
FROM OB_COMPARE_REPORT_SUMMARY
WHERE RUN_DATE >= TRUNC(SYSDATE) - 30
GROUP BY RUN_DATE
ORDER BY RUN_DATE DESC;
```

### 6.4 查找频繁缺失的对象

```sql
SELECT SOURCE_SCHEMA, SOURCE_NAME, OBJECT_TYPE,
       COUNT(*) AS MISS_COUNT,
       MAX(d.CREATED_AT) AS LAST_SEEN
FROM OB_COMPARE_REPORT_DETAIL d
WHERE REPORT_TYPE = 'MISSING'
  AND d.CREATED_AT >= SYSTIMESTAMP - INTERVAL '7' DAY
GROUP BY SOURCE_SCHEMA, SOURCE_NAME, OBJECT_TYPE
HAVING COUNT(*) >= 3
ORDER BY MISS_COUNT DESC;
```

---

## 七、测试计划

### 7.1 单元测试

| 测试项 | 测试内容 |
|-------|---------|
| `test_generate_report_id` | 验证 ID 格式和唯一性 |
| `test_ensure_tables_exist_new` | 表不存在时创建 |
| `test_ensure_tables_exist_already` | 表已存在时跳过 |
| `test_save_summary_basic` | 基本汇总写入 |
| `test_save_details_batch` | 批量明细写入 |
| `test_cleanup_old_reports` | 历史清理逻辑 |

### 7.2 集成测试

| 测试场景 | 验证点 |
|---------|-------|
| 功能关闭 | 不执行任何数据库操作 |
| 首次启用 | 自动创建表并写入 |
| 多次运行 | report_id 互不影响 |
| 写入失败 | 容错模式下不影响主流程 |
| 大批量数据 | 1000+ 明细批量写入 |

---

## 八、实施计划

### 阶段一：基础实现 (2-3 天)

1. 添加配置项解析
2. 实现自动建表逻辑
3. 实现主报告写入
4. 实现明细报告写入

### 阶段二：完善功能 (1-2 天)

1. 实现授权明细写入
2. 实现历史清理逻辑
3. 添加调用入口

### 阶段三：测试验证 (1-2 天)

1. 单元测试
2. 集成测试
3. 性能测试（大批量数据）

### 阶段四：文档更新 (0.5 天)

1. 更新 README
2. 更新配置说明
3. 添加查询示例

---

## 九、风险与缓解

| 风险 | 影响 | 缓解措施 |
|-----|------|---------|
| 建表权限不足 | 功能无法启用 | 提供手动建表脚本 |
| 大批量写入超时 | 写入失败 | 分批写入 + 重试机制 |
| 目标库空间不足 | 写入失败 | 自动清理 + 容错模式 |
| JSON 字段过大 | 写入慢或失败 | 可配置是否保存完整 JSON |

---

## 十、附录

### A. 完整建表脚本

见 `scripts/create_report_tables.sql`（需要创建）

### B. 回滚脚本

```sql
-- 如需回滚，执行以下脚本删除报告表
DROP TABLE OB_COMPARE_REPORT_GRANTS CASCADE CONSTRAINTS;
DROP TABLE OB_COMPARE_REPORT_DETAIL CASCADE CONSTRAINTS;
DROP TABLE OB_COMPARE_REPORT_SUMMARY CASCADE CONSTRAINTS;
```

---

**提案状态**: 待评审  
**预计工作量**: 5-7 人天
