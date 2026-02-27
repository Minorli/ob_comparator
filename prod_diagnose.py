#!/usr/bin/env python3
"""Production diff triage helper (read-only).

Goals:
1) Verify whether report_db summary metrics are consistent with detail rows.
2) Probe Oracle/OB metadata for sampled MISSING/MISMATCHED/UNSUPPORTED rows.
3) Parse fixup failure artifacts and classify ORA errors into actionable buckets.

Output:
- triage_summary_<ts>.txt
- triage_detail_<ts>.txt
- triage_fixup_failures_<ts>.txt
- triage_false_positive_candidates_<ts>.txt
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import oracledb
except ImportError:
    oracledb = None

__version__ = "0.9.8.6"


REPORT_TABLES = {
    "summary": "DIFF_REPORT_SUMMARY",
    "detail": "DIFF_REPORT_DETAIL",
    "detail_item": "DIFF_REPORT_DETAIL_ITEM",
    "counts": "DIFF_REPORT_COUNTS",
}

SUPPORTED_REPORT_TYPES = {"MISSING", "MISMATCHED", "UNSUPPORTED"}

ORA_CODE_RE = re.compile(r"(ORA-\d{5})")
LINE_SEP = "\x1f"


@dataclass
class OracleCfg:
    user: str
    password: str
    dsn: str


@dataclass
class ObCfg:
    executable: str
    host: str
    port: str
    user_string: str
    password: str
    timeout: int


@dataclass
class ToolSettings:
    report_db_schema: str
    report_dir: Path
    fixup_dir: Path


@dataclass
class DetailRow:
    report_type: str
    object_type: str
    source_schema: str
    source_name: str
    target_schema: str
    target_name: str
    status: str
    reason: str
    detail_json: str

    @property
    def source_full(self) -> str:
        return f"{self.source_schema}.{self.source_name}" if self.source_schema and self.source_name else ""

    @property
    def target_full(self) -> str:
        return f"{self.target_schema}.{self.target_name}" if self.target_schema and self.target_name else ""

    @property
    def key(self) -> Tuple[str, str, str, str, str, str, str]:
        return (
            self.report_type,
            self.object_type,
            self.source_schema,
            self.source_name,
            self.target_schema,
            self.target_name,
            self.status,
        )


@dataclass
class ProbeFact:
    exists: Optional[bool]
    status: str
    detail: str


@dataclass
class TriageEntry:
    case_id: str
    severity: str
    report_type: str
    object_type: str
    source_full: str
    target_full: str
    report_status: str
    reason_code: str
    root_cause: str
    evidence: str
    action: str
    false_positive: bool = False


@dataclass
class FixupFailure:
    line_no: int
    script_hint: str
    error_code: str
    failure_class: str
    root_cause: str
    action: str
    raw_line: str


@dataclass
class FocusObject:
    schema: str
    name: str
    object_type: str
    raw: str


class TriageError(RuntimeError):
    pass


def sql_quote(value: str) -> str:
    return "'" + (value or "").replace("'", "''") + "'"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Production report triage helper (read-only)."
    )
    parser.add_argument("config", nargs="?", default="config.ini", help="配置文件路径（默认 config.ini）")
    parser.add_argument("--report-id", dest="report_id", default="", help="指定 report_id；留空自动取最新")
    parser.add_argument("--max-samples-per-group", type=int, default=120, help="每个(report_type,object_type)最多抽样条数")
    parser.add_argument("--max-probe-total", type=int, default=3000, help="总探测条数上限")
    parser.add_argument("--output-dir", default="", help="输出目录；留空自动放到 report run 目录下")
    parser.add_argument("--ob-timeout", type=int, default=120, help="obclient 查询超时秒数")
    parser.add_argument("--skip-oracle-probe", action="store_true", help="跳过 Oracle 探测（仅用 report_db + OB）")
    parser.add_argument("--focus-object", default="", help="聚焦对象，格式 TYPE:SCHEMA.OBJECT 或 SCHEMA.OBJECT")
    parser.add_argument("--focus-object-type", default="", help="聚焦对象类型（当 --focus-object 未包含 TYPE 时可指定）")
    parser.add_argument("--deep", action="store_true", help="聚焦对象时启用深挖（依赖/映射/可用性/授权）")
    parser.add_argument("--focus-max-rows", type=int, default=3000, help="深挖模式每类查询最大行数")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser.parse_args(argv)


def read_config(config_path: Path) -> Tuple[OracleCfg, ObCfg, ToolSettings]:
    parser = configparser.ConfigParser(interpolation=None)
    if not config_path.exists():
        raise TriageError(f"配置文件不存在: {config_path}")
    parser.read(config_path, encoding="utf-8")

    if "ORACLE_SOURCE" not in parser:
        raise TriageError("配置缺少 [ORACLE_SOURCE]")
    if "OCEANBASE_TARGET" not in parser:
        raise TriageError("配置缺少 [OCEANBASE_TARGET]")

    ora_sec = parser["ORACLE_SOURCE"]
    ob_sec = parser["OCEANBASE_TARGET"]
    st_sec = parser["SETTINGS"] if "SETTINGS" in parser else {}

    oracle_cfg = OracleCfg(
        user=(ora_sec.get("user") or "").strip(),
        password=(ora_sec.get("password") or "").strip(),
        dsn=(ora_sec.get("dsn") or "").strip(),
    )
    for key, val in [("user", oracle_cfg.user), ("password", oracle_cfg.password), ("dsn", oracle_cfg.dsn)]:
        if not val:
            raise TriageError(f"[ORACLE_SOURCE] 缺少 {key}")

    ob_cfg = ObCfg(
        executable=(ob_sec.get("executable") or "").strip(),
        host=(ob_sec.get("host") or "").strip(),
        port=(ob_sec.get("port") or "").strip(),
        user_string=(ob_sec.get("user_string") or "").strip(),
        password=(ob_sec.get("password") or "").strip(),
        timeout=120,
    )
    for key, val in [
        ("executable", ob_cfg.executable),
        ("host", ob_cfg.host),
        ("port", ob_cfg.port),
        ("user_string", ob_cfg.user_string),
        ("password", ob_cfg.password),
    ]:
        if not val:
            raise TriageError(f"[OCEANBASE_TARGET] 缺少 {key}")

    try:
        int(ob_cfg.port)
    except ValueError as exc:
        raise TriageError(f"[OCEANBASE_TARGET] 端口非法: {ob_cfg.port}") from exc

    base_dir = config_path.parent.resolve()
    report_db_schema = str(st_sec.get("report_db_schema", "")).strip().upper() if st_sec else ""
    report_dir = (base_dir / str(st_sec.get("report_dir", "main_reports"))).resolve() if st_sec else (base_dir / "main_reports")
    fixup_dir = (base_dir / str(st_sec.get("fixup_dir", "fixup_scripts"))).resolve() if st_sec else (base_dir / "fixup_scripts")
    settings = ToolSettings(
        report_db_schema=report_db_schema,
        report_dir=report_dir,
        fixup_dir=fixup_dir,
    )
    return oracle_cfg, ob_cfg, settings


def _ob_cmd(ob_cfg: ObCfg) -> List[str]:
    return [
        ob_cfg.executable,
        "-h", ob_cfg.host,
        "-P", str(ob_cfg.port),
        "-u", ob_cfg.user_string,
        f"-p{ob_cfg.password}",
        "-N",
        "-B",
        "--silent",
    ]


def ob_query(ob_cfg: ObCfg, sql: str, timeout: Optional[int] = None) -> List[List[str]]:
    timeout_sec = timeout if timeout and timeout > 0 else ob_cfg.timeout
    proc = subprocess.run(
        _ob_cmd(ob_cfg) + ["-e", sql],
        capture_output=True,
        text=True,
        timeout=timeout_sec,
        check=False,
    )
    stderr = (proc.stderr or "").strip()
    stdout = proc.stdout or ""
    if proc.returncode != 0:
        raise TriageError(f"obclient 查询失败({proc.returncode}): {stderr or stdout.strip()}")
    if "ORA-" in stderr.upper():
        raise TriageError(f"obclient 错误: {stderr}")

    rows: List[List[str]] = []
    for raw in stdout.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("Warning"):
            continue
        if line.startswith("+") and line.endswith("+"):
            continue
        if "\t" in line:
            rows.append([col.strip() for col in line.split("\t")])
        elif LINE_SEP in line:
            rows.append([col.strip() for col in line.split(LINE_SEP)])
        else:
            rows.append([line])
    return rows


def _schema_prefix(settings: ToolSettings) -> str:
    return f"{settings.report_db_schema}." if settings.report_db_schema else ""


def _object_type_to_oracle_type(object_type: str) -> str:
    obj = (object_type or "").strip().upper()
    if obj == "MVIEW":
        return "MATERIALIZED VIEW"
    return obj


def _normalize_cell(value: str) -> str:
    text = (value or "").strip()
    if text.upper() == "NULL":
        return ""
    return text


def parse_focus_object(raw_focus: str, raw_type: str = "") -> Optional[FocusObject]:
    text = (raw_focus or "").strip()
    if not text:
        return None
    obj_type = (raw_type or "").strip().upper()
    obj_part = text
    if ":" in text:
        left, right = text.split(":", 1)
        if right.strip():
            obj_part = right.strip()
        if left.strip():
            obj_type = left.strip().upper()
    if "." not in obj_part:
        raise TriageError(f"--focus-object 格式非法: {raw_focus}，需要 SCHEMA.OBJECT")
    schema, name = obj_part.split(".", 1)
    schema = schema.strip().upper()
    name = name.strip().upper()
    if not schema or not name:
        raise TriageError(f"--focus-object 格式非法: {raw_focus}，需要 SCHEMA.OBJECT")
    return FocusObject(
        schema=schema,
        name=name,
        object_type=obj_type,
        raw=text,
    )


def resolve_latest_report_id(ob_cfg: ObCfg, settings: ToolSettings) -> Tuple[str, Dict[str, str]]:
    tbl = f"{_schema_prefix(settings)}{REPORT_TABLES['summary']}"
    sql = (
        "SELECT REPORT_ID, TO_CHAR(RUN_TIMESTAMP,'YYYY-MM-DD HH24:MI:SS'), NVL(RUN_DIR,''), NVL(TOOL_VERSION,'') "
        f"FROM (SELECT REPORT_ID, RUN_TIMESTAMP, RUN_DIR, TOOL_VERSION FROM {tbl} ORDER BY RUN_TIMESTAMP DESC) "
        "WHERE ROWNUM = 1"
    )
    rows = ob_query(ob_cfg, sql)
    if not rows:
        raise TriageError("DIFF_REPORT_SUMMARY 为空，无法定位最新 report_id")
    row = rows[0]
    report_id = row[0].strip()
    meta = {
        "run_timestamp": row[1].strip() if len(row) > 1 else "",
        "run_dir": row[2].strip() if len(row) > 2 else "",
        "tool_version": row[3].strip() if len(row) > 3 else "",
    }
    return report_id, meta


def load_report_meta(ob_cfg: ObCfg, settings: ToolSettings, report_id: str) -> Dict[str, str]:
    tbl = f"{_schema_prefix(settings)}{REPORT_TABLES['summary']}"
    sql = (
        "SELECT TO_CHAR(RUN_TIMESTAMP,'YYYY-MM-DD HH24:MI:SS'), NVL(RUN_DIR,''), NVL(TOOL_VERSION,''), "
        "NVL(MISSING_COUNT,0), NVL(MISSING_FIXABLE_COUNT,0), NVL(MISMATCHED_COUNT,0), "
        "NVL(UNSUPPORTED_COUNT,0), NVL(EXCLUDED_COUNT,0), "
        "NVL(FIXUP_ENABLED,0), NVL(GRANT_ENABLED,0), NVL(CONCLUSION,''), NVL(CHECK_PRIMARY_TYPES,''), "
        "NVL(DETAIL_TRUNCATED,0), NVL(DETAIL_TRUNCATED_COUNT,0) "
        f"FROM {tbl} WHERE REPORT_ID = {sql_quote(report_id)}"
    )
    rows = ob_query(ob_cfg, sql)
    if not rows:
        raise TriageError(f"report_id 不存在: {report_id}")
    row = rows[0]
    return {
        "run_timestamp": row[0] if len(row) > 0 else "",
        "run_dir": row[1] if len(row) > 1 else "",
        "tool_version": row[2] if len(row) > 2 else "",
        "missing_count": row[3] if len(row) > 3 else "0",
        "missing_fixable_count": row[4] if len(row) > 4 else "0",
        "mismatched_count": row[5] if len(row) > 5 else "0",
        "unsupported_count": row[6] if len(row) > 6 else "0",
        "excluded_count": row[7] if len(row) > 7 else "0",
        "fixup_enabled": row[8] if len(row) > 8 else "0",
        "grant_enabled": row[9] if len(row) > 9 else "0",
        "conclusion": row[10] if len(row) > 10 else "",
        "check_primary_types": row[11] if len(row) > 11 else "",
        "detail_truncated": row[12] if len(row) > 12 else "0",
        "detail_truncated_count": row[13] if len(row) > 13 else "0",
    }


def load_detail_group_counts(ob_cfg: ObCfg, settings: ToolSettings, report_id: str) -> Dict[Tuple[str, str], int]:
    tbl = f"{_schema_prefix(settings)}{REPORT_TABLES['detail']}"
    sql = (
        "SELECT REPORT_TYPE, OBJECT_TYPE, COUNT(*) "
        f"FROM {tbl} "
        f"WHERE REPORT_ID = {sql_quote(report_id)} "
        "GROUP BY REPORT_TYPE, OBJECT_TYPE "
        "ORDER BY REPORT_TYPE, OBJECT_TYPE"
    )
    rows = ob_query(ob_cfg, sql)
    result: Dict[Tuple[str, str], int] = {}
    for row in rows:
        if len(row) < 3:
            continue
        key = (row[0].strip().upper(), row[1].strip().upper())
        try:
            cnt = int(row[2].strip())
        except ValueError:
            cnt = 0
        result[key] = cnt
    return result


def load_counts_by_type(ob_cfg: ObCfg, settings: ToolSettings, report_id: str) -> Dict[str, Dict[str, int]]:
    tbl = f"{_schema_prefix(settings)}{REPORT_TABLES['counts']}"
    sql = (
        "SELECT OBJECT_TYPE, "
        "NVL(ORACLE_COUNT,0), NVL(OCEANBASE_COUNT,0), NVL(MISSING_COUNT,0), "
        "NVL(MISSING_FIXABLE_COUNT,0), NVL(UNSUPPORTED_COUNT,0), NVL(EXCLUDED_COUNT,0) "
        f"FROM {tbl} "
        f"WHERE REPORT_ID = {sql_quote(report_id)}"
    )
    rows = ob_query(ob_cfg, sql)
    result: Dict[str, Dict[str, int]] = {}
    for row in rows:
        if len(row) < 7:
            continue
        obj_type = (row[0] or "").strip().upper()
        if not obj_type:
            continue
        values: List[int] = []
        for raw in row[1:7]:
            try:
                values.append(int((raw or "0").strip() or "0"))
            except ValueError:
                values.append(0)
        result[obj_type] = {
            "oracle_count": values[0],
            "oceanbase_count": values[1],
            "missing_count": values[2],
            "missing_fixable_count": values[3],
            "unsupported_count": values[4],
            "excluded_count": values[5],
        }
    return result


def load_sample_detail_rows(
    ob_cfg: ObCfg,
    settings: ToolSettings,
    report_id: str,
    group_counts: Dict[Tuple[str, str], int],
    max_samples_per_group: int,
    max_probe_total: int,
) -> List[DetailRow]:
    tbl = f"{_schema_prefix(settings)}{REPORT_TABLES['detail']}"
    rows: List[DetailRow] = []
    total_added = 0

    for (report_type, object_type), total in sorted(group_counts.items()):
        rt = report_type.upper()
        ot = object_type.upper()
        if rt not in SUPPORTED_REPORT_TYPES:
            continue
        if total <= 0:
            continue
        sample_n = min(max(1, max_samples_per_group), total)
        remain = max_probe_total - total_added
        if remain <= 0:
            break
        sample_n = min(sample_n, remain)
        sql = (
            "SELECT REPORT_TYPE, OBJECT_TYPE, NVL(SOURCE_SCHEMA,''), NVL(SOURCE_NAME,''), "
            "NVL(TARGET_SCHEMA,''), NVL(TARGET_NAME,''), NVL(STATUS,''), NVL(REASON,''), "
            "NVL(DBMS_LOB.SUBSTR(DETAIL_JSON, 3000, 1), '') "
            f"FROM (SELECT REPORT_TYPE, OBJECT_TYPE, SOURCE_SCHEMA, SOURCE_NAME, TARGET_SCHEMA, TARGET_NAME, STATUS, REASON, DETAIL_JSON "
            f"      FROM {tbl} "
            f"      WHERE REPORT_ID = {sql_quote(report_id)} "
            f"        AND REPORT_TYPE = {sql_quote(rt)} "
            f"        AND OBJECT_TYPE = {sql_quote(ot)} "
            "      ORDER BY SOURCE_SCHEMA, SOURCE_NAME, TARGET_SCHEMA, TARGET_NAME) "
            f"WHERE ROWNUM <= {sample_n}"
        )
        out = ob_query(ob_cfg, sql)
        for raw in out:
            if len(raw) < 8:
                continue
            detail_json = raw[8] if len(raw) > 8 else ""
            rows.append(
                DetailRow(
                    report_type=_normalize_cell(raw[0] if len(raw) > 0 else "").upper(),
                    object_type=_normalize_cell(raw[1] if len(raw) > 1 else "").upper(),
                    source_schema=_normalize_cell(raw[2] if len(raw) > 2 else "").upper(),
                    source_name=_normalize_cell(raw[3] if len(raw) > 3 else "").upper(),
                    target_schema=_normalize_cell(raw[4] if len(raw) > 4 else "").upper(),
                    target_name=_normalize_cell(raw[5] if len(raw) > 5 else "").upper(),
                    status=_normalize_cell(raw[6] if len(raw) > 6 else "").upper(),
                    reason=_normalize_cell(raw[7] if len(raw) > 7 else ""),
                    detail_json=_normalize_cell(detail_json),
                )
            )
        total_added = len(rows)
        if total_added >= max_probe_total:
            break
    return rows


def load_focus_detail_rows(
    ob_cfg: ObCfg,
    settings: ToolSettings,
    report_id: str,
    focus: FocusObject,
    max_rows: int,
) -> List[DetailRow]:
    tbl = f"{_schema_prefix(settings)}{REPORT_TABLES['detail']}"
    cond = (
        f"((NVL(SOURCE_SCHEMA,'')={sql_quote(focus.schema)} AND NVL(SOURCE_NAME,'')={sql_quote(focus.name)}) "
        f" OR (NVL(TARGET_SCHEMA,'')={sql_quote(focus.schema)} AND NVL(TARGET_NAME,'')={sql_quote(focus.name)}))"
    )
    if focus.object_type:
        cond += f" AND OBJECT_TYPE={sql_quote(focus.object_type)}"
    limit = max(1, int(max_rows))
    sql = (
        "SELECT REPORT_TYPE, OBJECT_TYPE, NVL(SOURCE_SCHEMA,''), NVL(SOURCE_NAME,''), "
        "NVL(TARGET_SCHEMA,''), NVL(TARGET_NAME,''), NVL(STATUS,''), NVL(REASON,''), "
        "NVL(DBMS_LOB.SUBSTR(DETAIL_JSON, 3000, 1), '') "
        f"FROM (SELECT REPORT_TYPE, OBJECT_TYPE, SOURCE_SCHEMA, SOURCE_NAME, TARGET_SCHEMA, TARGET_NAME, STATUS, REASON, DETAIL_JSON "
        f"      FROM {tbl} "
        f"      WHERE REPORT_ID = {sql_quote(report_id)} "
        f"        AND {cond} "
        "      ORDER BY REPORT_TYPE, OBJECT_TYPE, SOURCE_SCHEMA, SOURCE_NAME, TARGET_SCHEMA, TARGET_NAME) "
        f"WHERE ROWNUM <= {limit}"
    )
    out = ob_query(ob_cfg, sql)
    rows: List[DetailRow] = []
    for raw in out:
        if len(raw) < 8:
            continue
        detail_json = raw[8] if len(raw) > 8 else ""
        rows.append(
            DetailRow(
                report_type=_normalize_cell(raw[0] if len(raw) > 0 else "").upper(),
                object_type=_normalize_cell(raw[1] if len(raw) > 1 else "").upper(),
                source_schema=_normalize_cell(raw[2] if len(raw) > 2 else "").upper(),
                source_name=_normalize_cell(raw[3] if len(raw) > 3 else "").upper(),
                target_schema=_normalize_cell(raw[4] if len(raw) > 4 else "").upper(),
                target_name=_normalize_cell(raw[5] if len(raw) > 5 else "").upper(),
                status=_normalize_cell(raw[6] if len(raw) > 6 else "").upper(),
                reason=_normalize_cell(raw[7] if len(raw) > 7 else ""),
                detail_json=_normalize_cell(detail_json),
            )
        )
    return rows


def _parse_primary_types(meta: Dict[str, str]) -> List[str]:
    raw = (meta.get("check_primary_types") or "").strip()
    if not raw:
        return [
            "TABLE", "VIEW", "MVIEW", "SYNONYM", "PROCEDURE", "FUNCTION",
            "PACKAGE", "PACKAGE BODY", "TYPE", "TYPE BODY", "TRIGGER", "SEQUENCE", "JOB", "SCHEDULE"
        ]
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


def summarize_consistency(
    meta: Dict[str, str],
    group_counts: Dict[Tuple[str, str], int],
    counts_by_type: Optional[Dict[str, Dict[str, int]]] = None,
) -> List[Tuple[str, int, int, str, str]]:
    missing_detail = sum(cnt for (rt, _), cnt in group_counts.items() if rt == "MISSING")
    primary_types = set(_parse_primary_types(meta))
    primary_count_types = primary_types - {"MATERIALIZED VIEW", "PACKAGE", "PACKAGE BODY"}
    mismatch_primary = sum(
        cnt for (rt, ot), cnt in group_counts.items() if rt == "MISMATCHED" and ot in primary_count_types
    )
    if counts_by_type:
        missing_fixable_derived = sum(
            int((vals or {}).get("missing_fixable_count", 0) or 0)
            for vals in counts_by_type.values()
        )
        missing_primary_derived = sum(
            int((counts_by_type.get(obj_type, {}) or {}).get("missing_count", 0) or 0)
            for obj_type in primary_count_types
        )
        unsupported_total_derived = sum(
            int((vals or {}).get("unsupported_count", 0) or 0)
            for vals in counts_by_type.values()
        )
    else:
        unsupported_primary = sum(
            cnt for (rt, ot), cnt in group_counts.items() if rt == "UNSUPPORTED" and ot in primary_count_types
        )
        missing_fixable_derived = missing_detail
        missing_primary_derived = missing_detail + unsupported_primary
        unsupported_total_derived = unsupported_primary

    expectations = [
        ("MISSING_FIXABLE_COUNT", int(meta.get("missing_fixable_count", "0") or 0), missing_fixable_derived),
        ("MISSING_COUNT_DERIVED", int(meta.get("missing_count", "0") or 0), missing_primary_derived),
        ("MISMATCHED_COUNT_PRIMARY", int(meta.get("mismatched_count", "0") or 0), mismatch_primary),
        ("UNSUPPORTED_COUNT_PRIMARY", int(meta.get("unsupported_count", "0") or 0), unsupported_total_derived),
    ]

    detail_truncated = str(meta.get("detail_truncated", "0") or "0").strip() in {"1", "true", "TRUE"}
    result: List[Tuple[str, int, int, str, str]] = []
    for metric, expected, actual in expectations:
        status = "OK" if expected == actual else "DRIFT"
        if status == "OK":
            note = "-"
        elif detail_truncated:
            note = "同口径复算不一致；DETAIL_TRUNCATED=1，可能由明细截断导致"
        else:
            note = "同口径复算不一致，需排查统计路径"
        result.append((metric, expected, actual, status, note))
    return result


def _reason_from_detail_text(reason: str, detail_json: str, status: str) -> Tuple[str, str, str]:
    text = " ".join([(reason or ""), (detail_json or ""), (status or "")]).upper()
    if "DBLINK" in text:
        return ("UNSUPPORTED_DBLINK", "视图/对象含 DBLINK，目标端不支持", "改造视图并移除 DBLINK 依赖")
    if "OBJ$" in text or " X$" in text or '"X$' in text:
        return ("UNSUPPORTED_SYSTEM_OBJECT", "依赖 Oracle 系统对象（OBJ$/X$）", "改写 SQL，替换系统对象引用")
    if "LONG" in text and "BLACK" in text:
        return ("BLACKLIST_LONG", "黑名单 LONG/LONG RAW 相关对象", "先完成 LONG->CLOB/BLOB 转换或按策略跳过")
    if "DEPEND" in text or "BLOCKED" in text or "阻断" in text:
        return ("DEPENDENCY_BLOCKED", "依赖对象缺失/不支持导致阻断", "先修复依赖链上游对象")
    if "PRIVILEGE" in text or "GRANT" in text or "ORA-01031" in text or "权限" in text:
        return ("PRIVILEGE_MISSING", "权限不足", "补充对象权限或 WITH GRANT OPTION")
    if "ORA-009" in text or "SYNTAX" in text or "不支持" in text:
        return ("SYNTAX_UNSUPPORTED", "语法不兼容", "改写为 OB 支持语法后再 fixup")
    if "STATUS_DRIFT" in text or "ENABLED" in text or "DISABLED" in text:
        return ("STATUS_DRIFT", "对象状态漂移（ENABLE/VALID 等）", "生成并执行状态同步 DDL")
    if "OBCHECK" in text or "OBNOTNULL" in text:
        return ("NAME_NOISE_OB_AUTO", "OB 自动约束命名差异", "做语义降噪，避免按名称误判")
    return ("UNCLASSIFIED", "未命中明确规则", "结合对象明细和依赖链继续排查")


def _classify_table_mismatch_detail(detail_json: str) -> Tuple[str, str, str]:
    parsed: Dict[str, object] = {}
    raw = (detail_json or "").strip()
    if raw:
        try:
            loaded = json.loads(raw)
            if isinstance(loaded, dict):
                parsed = loaded
        except Exception:
            parsed = {}

    length_items = parsed.get("length_mismatches") if parsed else None
    type_items = parsed.get("type_mismatches") if parsed else None
    missing_cols = parsed.get("missing_columns") if parsed else None
    extra_cols = parsed.get("extra_columns") if parsed else None

    if isinstance(length_items, list) and length_items:
        return (
            "TABLE_LENGTH_MISMATCH",
            "表字段长度不一致",
            "查看 DETAIL_ITEM 的 LENGTH_MISMATCH 明细，确认是否属于语义等价噪声或需要修复",
        )
    if isinstance(type_items, list) and type_items:
        return (
            "TABLE_TYPE_MISMATCH",
            "表字段类型不一致",
            "查看 DETAIL_ITEM 的 TYPE_MISMATCH 明细并修复目标端列类型",
        )
    if isinstance(missing_cols, list) and missing_cols:
        return (
            "TABLE_MISSING_COLUMN",
            "目标端缺少字段",
            "补齐缺失列或执行对应 fixup DDL",
        )
    if isinstance(extra_cols, list) and extra_cols:
        return (
            "TABLE_EXTRA_COLUMN",
            "目标端存在额外字段",
            "确认是否业务遗留字段；必要时按策略清理",
        )

    upper = raw.upper()
    if "LENGTH_MISMATCH" in upper or "SHORT:" in upper or "LONGER_THAN_OB_LIMIT" in upper:
        return (
            "TABLE_LENGTH_MISMATCH",
            "表字段长度不一致",
            "查看 DETAIL_ITEM 的 LENGTH_MISMATCH 明细，确认是否属于语义等价噪声或需要修复",
        )
    if "TYPE_MISMATCH" in upper:
        return (
            "TABLE_TYPE_MISMATCH",
            "表字段类型不一致",
            "查看 DETAIL_ITEM 的 TYPE_MISMATCH 明细并修复目标端列类型",
        )
    if "MISSING_COLUMN" in upper:
        return ("TABLE_MISSING_COLUMN", "目标端缺少字段", "补齐缺失列或执行对应 fixup DDL")
    if "EXTRA_COLUMN" in upper:
        return ("TABLE_EXTRA_COLUMN", "目标端存在额外字段", "确认是否业务遗留字段；必要时按策略清理")

    return ("", "", "")


def classify_detail_row(
    row: DetailRow,
    src_fact: ProbeFact,
    tgt_fact: ProbeFact,
) -> TriageEntry:
    reason_code, cause, action = _reason_from_detail_text(row.reason, row.detail_json, row.status)
    false_positive = False

    if row.report_type == "MISSING":
        if tgt_fact.exists is True:
            reason_code = "FP_TARGET_EXISTS"
            cause = "报告为缺失，但目标端对象已存在"
            action = "优先检查比较口径/remap/降噪规则"
            false_positive = True
        elif src_fact.exists is False:
            reason_code = "SRC_GONE_DURING_CHECK"
            cause = "源端对象已不存在（可能运行窗口变化）"
            action = "确认快照时间点，必要时重新运行主程序"
        elif tgt_fact.exists is False:
            reason_code = "TRUE_MISSING_CONFIRMED"
            cause = "目标端对象确实缺失"
            action = "按 fixup/OMS 流程补齐"
    elif row.report_type == "UNSUPPORTED":
        if row.object_type == "TABLE" and "RENAME" in (row.source_name or ""):
            reason_code = "BLACKLIST_RENAME"
            cause = "命中 RENAME 规则"
            action = "若需纳管，请在黑名单/排除策略中调整"
        elif tgt_fact.exists is True and reason_code in {"UNCLASSIFIED", "SYNTAX_UNSUPPORTED"}:
            reason_code = "UNSUPPORTED_BUT_PRESENT"
            cause = "对象在目标端存在，但被标记不支持（需检查语义与可用性）"
            action = "人工确认可用性；必要时下调不支持规则"
    elif row.report_type == "MISMATCHED":
        if row.object_type == "TABLE":
            detail_reason, detail_cause, detail_action = _classify_table_mismatch_detail(row.detail_json)
            if detail_reason:
                reason_code = detail_reason
                cause = detail_cause
                action = detail_action
            elif (
                row.source_schema
                and row.target_schema
                and row.source_name
                and row.target_name
                and row.source_schema == row.target_schema
                and row.source_name == row.target_name
            ):
                reason_code = "TABLE_MISMATCH_AGGREGATED"
                cause = "该行为表级聚合 mismatch（具体列差异在 detail_item）"
                action = (
                    "按对象过滤 DIFF_REPORT_DETAIL_ITEM，查看 LENGTH_MISMATCH/TYPE_MISMATCH/"
                    "MISSING_COLUMN/EXTRA_COLUMN"
                )
        if row.status == "STATUS_DRIFT":
            reason_code = "STATUS_DRIFT"
            cause = "对象存在，但状态不一致"
            action = "执行状态同步 fixup（enable/compile）"
        elif row.object_type in {"INDEX", "CONSTRAINT", "TRIGGER", "SEQUENCE"} and not row.source_name and not row.target_name:
            reason_code = "AGGREGATED_MISMATCH_ROW"
            cause = "该行为聚合类 mismatch，需要查看 detail item"
            action = "结合 DIFF_REPORT_DETAIL_ITEM 逐项定位"
        elif (
            row.object_type in {"INDEX", "CONSTRAINT", "TRIGGER"}
            and row.source_schema
            and row.target_schema
            and row.source_name
            and row.target_name
            and row.source_schema == row.target_schema
            and row.source_name == row.target_name
        ):
            reason_code = "AGGREGATED_MISMATCH_ROW"
            cause = "该行为表级聚合 mismatch（非对象名级）"
            action = "结合 detail_json/detail_item 中 missing_* 与 extra_* 字段定位"
        elif tgt_fact.exists is False:
            reason_code = "MISMATCH_TARGET_MISSING"
            cause = "报 mismatch 但目标对象不存在"
            action = "先按缺失对象处理，再复跑"
        elif reason_code == "UNCLASSIFIED" and src_fact.exists is True and tgt_fact.exists is True:
            reason_code = "MISMATCH_NEEDS_DETAIL"
            cause = "对象两端均存在，需结合明细项定位差异"
            action = "查询 DIFF_REPORT_DETAIL_ITEM，按对象查看 missing_*/extra_*/mismatch_* 细项"

    evidence = f"SRC={src_fact.status}:{src_fact.detail}; TGT={tgt_fact.status}:{tgt_fact.detail}"
    return TriageEntry(
        case_id="",
        severity="HIGH" if reason_code.startswith("TRUE_") else ("MEDIUM" if reason_code.startswith("FP_") else "LOW"),
        report_type=row.report_type,
        object_type=row.object_type,
        source_full=row.source_full,
        target_full=row.target_full,
        report_status=row.status,
        reason_code=reason_code,
        root_cause=cause,
        evidence=evidence,
        action=action,
        false_positive=false_positive,
    )


def _safe_ob_query(ob_cfg: ObCfg, sql: str) -> Tuple[List[List[str]], str]:
    try:
        return ob_query(ob_cfg, sql), ""
    except Exception as exc:  # pragma: no cover - runtime fallback
        return [], str(exc)


def collect_focus_deep_rows(
    ob_cfg: ObCfg,
    settings: ToolSettings,
    report_id: str,
    focus: FocusObject,
    max_rows: int,
    fixup_failures: List[FixupFailure],
) -> List[Tuple[str, str, str, str, str]]:
    rows: List[Tuple[str, str, str, str, str]] = []
    schema_prefix = _schema_prefix(settings)
    limit = max(1, int(max_rows))
    q_schema = sql_quote(focus.schema)
    q_name = sql_quote(focus.name)
    q_rid = sql_quote(report_id)

    def add(section: str, key1: str, key2: str, value: str, note: str = "") -> None:
        rows.append((section, key1, key2, value, note))

    add("FOCUS", "OBJECT", focus.object_type or "-", f"{focus.schema}.{focus.name}", focus.raw)

    # 1) detail_item
    detail_item_sql = (
        "SELECT REPORT_TYPE, OBJECT_TYPE, NVL(ITEM_TYPE,''), NVL(ITEM_KEY,''), "
        "NVL(SUBSTR(NVL(SRC_VALUE,'') || '|' || NVL(TGT_VALUE,''), 1, 400), ''), "
        "NVL(SUBSTR(DBMS_LOB.SUBSTR(ITEM_VALUE, 300, 1), 1, 300), '') "
        f"FROM {schema_prefix}DIFF_REPORT_DETAIL_ITEM "
        f"WHERE REPORT_ID={q_rid} "
        f"  AND ((NVL(SOURCE_SCHEMA,'')={q_schema} AND NVL(SOURCE_NAME,'')={q_name}) "
        f"    OR (NVL(TARGET_SCHEMA,'')={q_schema} AND NVL(TARGET_NAME,'')={q_name})) "
        "ORDER BY REPORT_TYPE, OBJECT_TYPE, ITEM_TYPE, ITEM_KEY"
    )
    out, err = _safe_ob_query(ob_cfg, f"SELECT * FROM ({detail_item_sql}) WHERE ROWNUM <= {limit}")
    if err:
        add("DEEP_WARN", "DETAIL_ITEM", "-", "", err)
    else:
        add("DEEP_COUNT", "DETAIL_ITEM", "rows", str(len(out)), "")
        for r in out:
            if len(r) >= 6:
                add("DETAIL_ITEM", f"{r[0]}/{r[1]}", r[2], r[3], f"{r[4]} ; {r[5]}")

    # 2) object mapping
    map_sql = (
        "SELECT NVL(SRC_SCHEMA,''), NVL(SRC_NAME,''), NVL(OBJECT_TYPE,''), NVL(TGT_SCHEMA,''), NVL(TGT_NAME,''), NVL(MAP_SOURCE,'') "
        f"FROM {schema_prefix}DIFF_REPORT_OBJECT_MAPPING "
        f"WHERE REPORT_ID={q_rid} "
        f"  AND ((NVL(SRC_SCHEMA,'')={q_schema} AND NVL(SRC_NAME,'')={q_name}) "
        f"    OR (NVL(TGT_SCHEMA,'')={q_schema} AND NVL(TGT_NAME,'')={q_name})) "
        "ORDER BY OBJECT_TYPE, SRC_SCHEMA, SRC_NAME"
    )
    out, err = _safe_ob_query(ob_cfg, f"SELECT * FROM ({map_sql}) WHERE ROWNUM <= {limit}")
    if err:
        add("DEEP_WARN", "OBJECT_MAPPING", "-", "", err)
    else:
        add("DEEP_COUNT", "OBJECT_MAPPING", "rows", str(len(out)), "")
        for r in out:
            if len(r) >= 6:
                add("OBJECT_MAPPING", f"{r[2]}", f"{r[0]}.{r[1]}", f"{r[3]}.{r[4]}", r[5])

    # 3) remap conflicts
    remap_sql = (
        "SELECT NVL(SOURCE_SCHEMA,''), NVL(SOURCE_NAME,''), NVL(OBJECT_TYPE,''), NVL(REASON,''), NVL(CANDIDATES,'') "
        f"FROM {schema_prefix}DIFF_REPORT_REMAP_CONFLICT "
        f"WHERE REPORT_ID={q_rid} AND NVL(SOURCE_SCHEMA,'')={q_schema} AND NVL(SOURCE_NAME,'')={q_name} "
        "ORDER BY OBJECT_TYPE"
    )
    out, err = _safe_ob_query(ob_cfg, f"SELECT * FROM ({remap_sql}) WHERE ROWNUM <= {limit}")
    if err:
        add("DEEP_WARN", "REMAP_CONFLICT", "-", "", err)
    else:
        add("DEEP_COUNT", "REMAP_CONFLICT", "rows", str(len(out)), "")
        for r in out:
            if len(r) >= 5:
                add("REMAP_CONFLICT", r[2], f"{r[0]}.{r[1]}", r[3], r[4])

    # 4) dependency edges
    dep_sql = (
        "SELECT NVL(DEP_SCHEMA,''), NVL(DEP_NAME,''), NVL(DEP_TYPE,''), NVL(REF_SCHEMA,''), NVL(REF_NAME,''), NVL(REF_TYPE,''), "
        "NVL(EDGE_STATUS,''), NVL(REASON,'') "
        f"FROM {schema_prefix}DIFF_REPORT_DEPENDENCY "
        f"WHERE REPORT_ID={q_rid} "
        f"  AND ((NVL(DEP_SCHEMA,'')={q_schema} AND NVL(DEP_NAME,'')={q_name}) "
        f"    OR (NVL(REF_SCHEMA,'')={q_schema} AND NVL(REF_NAME,'')={q_name})) "
        "ORDER BY DEP_SCHEMA, DEP_NAME, REF_SCHEMA, REF_NAME"
    )
    out, err = _safe_ob_query(ob_cfg, f"SELECT * FROM ({dep_sql}) WHERE ROWNUM <= {limit}")
    if err:
        add("DEEP_WARN", "DEPENDENCY", "-", "", err)
    else:
        add("DEEP_COUNT", "DEPENDENCY", "rows", str(len(out)), "")
        for r in out:
            if len(r) >= 8:
                add("DEPENDENCY", f"{r[2]}:{r[0]}.{r[1]}", f"{r[5]}:{r[3]}.{r[4]}", r[6], r[7])

    # 5) usability
    usability_sql = (
        "SELECT NVL(OBJECT_TYPE,''), NVL(SCHEMA_NAME,''), NVL(OBJECT_NAME,''), NVL(TO_CHAR(USABLE),''), NVL(STATUS,''), NVL(REASON,'') "
        f"FROM {schema_prefix}DIFF_REPORT_USABILITY "
        f"WHERE REPORT_ID={q_rid} AND NVL(SCHEMA_NAME,'')={q_schema} AND NVL(OBJECT_NAME,'')={q_name} "
        "ORDER BY OBJECT_TYPE"
    )
    out, err = _safe_ob_query(ob_cfg, f"SELECT * FROM ({usability_sql}) WHERE ROWNUM <= {limit}")
    if err:
        add("DEEP_WARN", "USABILITY", "-", "", err)
    else:
        add("DEEP_COUNT", "USABILITY", "rows", str(len(out)), "")
        for r in out:
            if len(r) >= 6:
                add("USABILITY", r[0], f"{r[1]}.{r[2]}", f"usable={r[3]}, status={r[4]}", r[5])

    # 6) grants
    grants_sql = (
        "SELECT NVL(GRANT_TYPE,''), NVL(GRANTEE,''), NVL(PRIVILEGE,''), NVL(TARGET_SCHEMA,''), NVL(TARGET_NAME,''), NVL(STATUS,''), NVL(FILTER_REASON,'') "
        f"FROM {schema_prefix}DIFF_REPORT_GRANT "
        f"WHERE REPORT_ID={q_rid} AND NVL(TARGET_SCHEMA,'')={q_schema} AND NVL(TARGET_NAME,'')={q_name} "
        "ORDER BY GRANTEE, PRIVILEGE"
    )
    out, err = _safe_ob_query(ob_cfg, f"SELECT * FROM ({grants_sql}) WHERE ROWNUM <= {limit}")
    if err:
        add("DEEP_WARN", "GRANT", "-", "", err)
    else:
        add("DEEP_COUNT", "GRANT", "rows", str(len(out)), "")
        for r in out:
            if len(r) >= 7:
                add("GRANT", f"{r[0]}:{r[2]}", r[1], f"{r[3]}.{r[4]}", f"status={r[5]}, reason={r[6]}")

    # 7) fixup failure hints
    obj_token = focus.name.upper()
    schema_token = f"{focus.schema}."
    matched = [
        f for f in fixup_failures
        if obj_token in (f.raw_line or "").upper() or obj_token in (f.script_hint or "").upper() or schema_token in (f.raw_line or "").upper()
    ]
    add("DEEP_COUNT", "FIXUP_FAIL_MATCH", "rows", str(len(matched)), "")
    for f in matched[:limit]:
        add("FIXUP_FAIL", f.error_code, f.failure_class, f.script_hint, f.raw_line[:300])

    return rows


def _probe_oracle_object(
    conn,
    object_type: str,
    schema: str,
    name: str,
) -> ProbeFact:
    if not schema or not name:
        return ProbeFact(exists=None, status="UNKNOWN", detail="empty-key")
    obj = _object_type_to_oracle_type(object_type)
    sql = ""
    binds = {"owner": schema, "name": name}
    if obj == "INDEX":
        sql = "SELECT NVL(STATUS,'-') FROM DBA_INDEXES WHERE OWNER=:owner AND INDEX_NAME=:name AND ROWNUM=1"
    elif obj == "CONSTRAINT":
        sql = (
            "SELECT NVL(STATUS,'-') || ':' || NVL(VALIDATED,'-') || ':' || NVL(CONSTRAINT_TYPE,'-') "
            "FROM DBA_CONSTRAINTS WHERE OWNER=:owner AND CONSTRAINT_NAME=:name AND ROWNUM=1"
        )
    elif obj == "TRIGGER":
        sql = (
            "SELECT NVL(STATUS,'-') || ':' || NVL(TRIGGERING_EVENT,'-') "
            "FROM DBA_TRIGGERS WHERE OWNER=:owner AND TRIGGER_NAME=:name AND ROWNUM=1"
        )
    elif obj == "JOB":
        sql = "SELECT 'SCHEDULER' FROM DBA_SCHEDULER_JOBS WHERE OWNER=:owner AND JOB_NAME=:name AND ROWNUM=1"
    elif obj == "SCHEDULE":
        sql = "SELECT 'SCHEDULE' FROM DBA_SCHEDULER_SCHEDULES WHERE OWNER=:owner AND SCHEDULE_NAME=:name AND ROWNUM=1"
    elif obj == "MATERIALIZED VIEW":
        sql = "SELECT 'MVIEW' FROM DBA_MVIEWS WHERE OWNER=:owner AND MVIEW_NAME=:name AND ROWNUM=1"
    else:
        sql = (
            "SELECT NVL(STATUS,'-') FROM DBA_OBJECTS "
            "WHERE OWNER=:owner AND OBJECT_NAME=:name AND OBJECT_TYPE=:otype AND ROWNUM=1"
        )
        binds["otype"] = obj
    try:
        with conn.cursor() as cur:
            cur.execute(sql, binds)
            row = cur.fetchone()
            if row:
                return ProbeFact(exists=True, status="FOUND", detail=str(row[0] or "-"))
            return ProbeFact(exists=False, status="NOT_FOUND", detail="-")
    except Exception as exc:
        return ProbeFact(exists=None, status="ERROR", detail=str(exc).strip().splitlines()[0][:180])


def _probe_ob_object(
    ob_cfg: ObCfg,
    object_type: str,
    schema: str,
    name: str,
) -> ProbeFact:
    if not schema or not name:
        return ProbeFact(exists=None, status="UNKNOWN", detail="empty-key")
    obj = _object_type_to_oracle_type(object_type)
    owner = schema.upper()
    obj_name = name.upper()
    sql = ""
    if obj == "INDEX":
        sql = (
            "SELECT NVL(STATUS,'-') FROM DBA_INDEXES "
            f"WHERE OWNER={sql_quote(owner)} AND INDEX_NAME={sql_quote(obj_name)} AND ROWNUM=1"
        )
    elif obj == "CONSTRAINT":
        sql = (
            "SELECT NVL(STATUS,'-') || ':' || NVL(VALIDATED,'-') || ':' || NVL(CONSTRAINT_TYPE,'-') FROM DBA_CONSTRAINTS "
            f"WHERE OWNER={sql_quote(owner)} AND CONSTRAINT_NAME={sql_quote(obj_name)} AND ROWNUM=1"
        )
    elif obj == "TRIGGER":
        sql = (
            "SELECT NVL(STATUS,'-') || ':' || NVL(TRIGGERING_EVENT,'-') FROM DBA_TRIGGERS "
            f"WHERE OWNER={sql_quote(owner)} AND TRIGGER_NAME={sql_quote(obj_name)} AND ROWNUM=1"
        )
    elif obj == "JOB":
        sql = (
            "SELECT 'SCHEDULER' FROM DBA_SCHEDULER_JOBS "
            f"WHERE OWNER={sql_quote(owner)} AND JOB_NAME={sql_quote(obj_name)} AND ROWNUM=1"
        )
    elif obj == "SCHEDULE":
        sql = (
            "SELECT 'SCHEDULE' FROM DBA_SCHEDULER_SCHEDULES "
            f"WHERE OWNER={sql_quote(owner)} AND SCHEDULE_NAME={sql_quote(obj_name)} AND ROWNUM=1"
        )
    elif obj == "MATERIALIZED VIEW":
        sql = (
            "SELECT 'MVIEW' FROM DBA_MVIEWS "
            f"WHERE OWNER={sql_quote(owner)} AND MVIEW_NAME={sql_quote(obj_name)} AND ROWNUM=1"
        )
    else:
        sql = (
            "SELECT NVL(STATUS,'-') FROM DBA_OBJECTS "
            f"WHERE OWNER={sql_quote(owner)} AND OBJECT_NAME={sql_quote(obj_name)} "
            f"AND OBJECT_TYPE={sql_quote(obj)} AND ROWNUM=1"
        )
    try:
        out = ob_query(ob_cfg, sql)
        if out:
            return ProbeFact(exists=True, status="FOUND", detail=out[0][0] if out[0] else "-")
        return ProbeFact(exists=False, status="NOT_FOUND", detail="-")
    except Exception as exc:
        return ProbeFact(exists=None, status="ERROR", detail=str(exc).strip().splitlines()[0][:180])


def _find_latest_fixup_error_file(fixup_dir: Path) -> Optional[Path]:
    errors_dir = fixup_dir / "errors"
    if not errors_dir.exists():
        return None
    files = sorted(errors_dir.glob("fixup_errors_*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def classify_fixup_error_line(line: str) -> Tuple[str, str, str, str]:
    upper = (line or "").upper()
    m = ORA_CODE_RE.search(upper)
    code = m.group(1) if m else "NO_ORA_CODE"

    if "ORA-31603" in upper:
        return code, "METADATA_NOT_FOUND", "元数据对象不存在（DBMS_METADATA）", "跳过临时对象或修正对象映射后重试"
    if "ORA-00900" in upper or "ORA-00933" in upper or "ORA-00922" in upper or "ORA-00904" in upper:
        return code, "SQL_SYNTAX", "语法不兼容或对象定义不支持", "改写 DDL 语法并重试"
    if "ORA-01031" in upper or "ORA-01917" in upper or "ORA-04042" in upper:
        return code, "PRIVILEGE_OR_GRANTEE", "权限不足或授权对象不存在", "补授权/补角色后重试"
    if "ORA-02298" in upper or "ORA-02291" in upper or "ORA-02292" in upper:
        return code, "CONSTRAINT_DATA", "约束校验失败（数据不一致）", "优先 NOVALIDATE 或先清洗数据"
    if "ORA-00600" in upper and ("-5542" in upper or "-5559" in upper):
        return code, "OBJECT_NOT_FOUND_INTERNAL", "对象不存在或编译目标不存在", "先确认对象创建成功再编译"
    if "ORA-00600" in upper:
        return code, "INTERNAL_ENGINE", "数据库内核返回内部错误", "收集 SQL 与版本信息联系 DBA/厂商"
    if "ORA-00955" in upper:
        return code, "ALREADY_EXISTS", "对象已存在", "启用幂等策略或改为 CREATE OR REPLACE"
    return code, "UNKNOWN", "未归类失败", "保留上下文后人工排查"


def parse_fixup_failures(fixup_dir: Path, logs_dir: Path) -> List[FixupFailure]:
    failures: List[FixupFailure] = []
    seen: set = set()

    latest = _find_latest_fixup_error_file(fixup_dir)
    candidate_files: List[Path] = []
    if latest:
        candidate_files.append(latest)
    elif logs_dir.exists():
        # 补扫最近日志，避免 fixup_errors 不完整时漏失
        candidate_files.extend(sorted(logs_dir.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)[:2])

    for file_path in candidate_files:
        script_hint = ""
        is_log_fallback = file_path.suffix.lower() == ".log"
        try:
            lines = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for idx, raw in enumerate(lines, start=1):
            line = raw.strip()
            if not line:
                continue
            if ".sql" in line and ("fixup_scripts/" in line or "done/" in line):
                script_hint = line.split()[0]
            if "ORA-" not in line.upper():
                continue
            if "ORA-06512" in line.upper():
                # 仅栈信息，避免污染失败归类
                continue
            if is_log_fallback and not script_hint and "fixup_scripts/" not in line:
                # 回退到运行日志时仅保留 fixup 相关行，避免将主程序元数据警告误判为 fixup 失败
                continue
            code, klass, cause, action = classify_fixup_error_line(line)
            key = (file_path.name, script_hint, code, line)
            if key in seen:
                continue
            seen.add(key)
            failures.append(
                FixupFailure(
                    line_no=idx,
                    script_hint=script_hint or file_path.name,
                    error_code=code,
                    failure_class=klass,
                    root_cause=cause,
                    action=action,
                    raw_line=line[:500],
                )
            )
    return failures


def write_pipe_table(path: Path, headers: Sequence[str], rows: Iterable[Sequence[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("# " + "|".join(headers) + "\n")
        for row in rows:
            vals = []
            for val in row:
                text = "" if val is None else str(val)
                text = text.replace("\n", " ").replace("\r", " ").replace("|", "/")
                vals.append(text)
            f.write("|".join(vals) + "\n")


def build_output_dir(cli_output_dir: str, report_meta: Dict[str, str], fallback_base: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if cli_output_dir:
        return Path(cli_output_dir).resolve()
    run_dir = (report_meta.get("run_dir") or "").strip()
    if run_dir:
        p = Path(run_dir)
        if not p.is_absolute():
            p = (fallback_base / run_dir).resolve()
        return (p / f"triage_{ts}").resolve()
    return (fallback_base / f"triage_{ts}").resolve()


def run(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    config_path = Path(args.config).resolve()

    oracle_cfg, ob_cfg, settings = read_config(config_path)
    ob_cfg.timeout = max(1, int(args.ob_timeout))

    report_id = (args.report_id or "").strip()
    latest_meta: Dict[str, str] = {}
    if not report_id:
        report_id, latest_meta = resolve_latest_report_id(ob_cfg, settings)
    report_meta = load_report_meta(ob_cfg, settings, report_id)
    if latest_meta:
        report_meta.setdefault("run_timestamp", latest_meta.get("run_timestamp", ""))
        report_meta.setdefault("run_dir", latest_meta.get("run_dir", ""))

    group_counts = load_detail_group_counts(ob_cfg, settings, report_id)
    counts_by_type = load_counts_by_type(ob_cfg, settings, report_id)
    consistency_rows = summarize_consistency(report_meta, group_counts, counts_by_type)

    focus = parse_focus_object(args.focus_object, args.focus_object_type)

    sampled_rows = load_sample_detail_rows(
        ob_cfg,
        settings,
        report_id,
        group_counts,
        max_samples_per_group=max(1, int(args.max_samples_per_group)),
        max_probe_total=max(1, int(args.max_probe_total)),
    )
    if focus:
        focus_rows = load_focus_detail_rows(
            ob_cfg,
            settings,
            report_id,
            focus,
            max_rows=max(1, int(args.focus_max_rows)),
        )
        if focus_rows:
            sampled_rows = focus_rows
        else:
            print(
                f"[WARN] focus 对象在 DIFF_REPORT_DETAIL 中未命中: {focus.schema}.{focus.name}",
                file=sys.stderr
            )

    ora_conn = None
    if not args.skip_oracle_probe and oracledb is not None:
        try:
            ora_conn = oracledb.connect(user=oracle_cfg.user, password=oracle_cfg.password, dsn=oracle_cfg.dsn)
        except Exception as exc:
            print(f"[WARN] Oracle 探测连接失败，回退仅 OB 探测: {exc}", file=sys.stderr)
            ora_conn = None
    elif not args.skip_oracle_probe and oracledb is None:
        print("[WARN] 未安装 oracledb，回退仅 OB 探测", file=sys.stderr)

    src_cache: Dict[Tuple[str, str, str], ProbeFact] = {}
    tgt_cache: Dict[Tuple[str, str, str], ProbeFact] = {}
    triage_entries: List[TriageEntry] = []
    reason_counter: Counter = Counter()

    for idx, row in enumerate(sampled_rows, start=1):
        probe_type = row.object_type
        if (
            row.report_type == "MISMATCHED"
            and row.object_type in {"INDEX", "CONSTRAINT", "TRIGGER"}
            and row.source_schema
            and row.target_schema
            and row.source_name
            and row.target_name
            and row.source_schema == row.target_schema
            and row.source_name == row.target_name
        ):
            # 表级聚合 mismatch，改用 TABLE 探测，避免把表名当作约束/索引名导致误导
            probe_type = "TABLE"

        src_key = (probe_type, row.source_schema, row.source_name)
        tgt_key = (probe_type, row.target_schema, row.target_name)

        if src_key not in src_cache:
            if ora_conn is None:
                src_cache[src_key] = ProbeFact(exists=None, status="SKIP", detail="oracle-probe-disabled")
            else:
                src_cache[src_key] = _probe_oracle_object(ora_conn, probe_type, row.source_schema, row.source_name)
        if tgt_key not in tgt_cache:
            tgt_cache[tgt_key] = _probe_ob_object(ob_cfg, probe_type, row.target_schema, row.target_name)

        entry = classify_detail_row(row, src_cache[src_key], tgt_cache[tgt_key])
        entry.case_id = f"C{idx:05d}"
        triage_entries.append(entry)
        reason_counter[entry.reason_code] += 1

    if ora_conn is not None:
        try:
            ora_conn.close()
        except Exception:
            pass

    fixup_failures = parse_fixup_failures(settings.fixup_dir, config_path.parent / "logs")
    fixup_counter: Counter = Counter([f.failure_class for f in fixup_failures])
    focus_deep_rows: List[Tuple[str, str, str, str, str]] = []
    if focus and args.deep:
        focus_deep_rows = collect_focus_deep_rows(
            ob_cfg,
            settings,
            report_id,
            focus,
            max_rows=max(1, int(args.focus_max_rows)),
            fixup_failures=fixup_failures,
        )

    output_dir = build_output_dir(args.output_dir, report_meta, settings.report_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_file = output_dir / f"triage_summary_{ts}.txt"
    detail_file = output_dir / f"triage_detail_{ts}.txt"
    fixup_file = output_dir / f"triage_fixup_failures_{ts}.txt"
    fp_file = output_dir / f"triage_false_positive_candidates_{ts}.txt"
    deep_file = output_dir / f"triage_focus_deep_{ts}.txt"

    summary_rows: List[Sequence[object]] = []
    summary_rows.append(("META", "report_id", report_id, "INFO", "-"))
    summary_rows.append(("META", "run_timestamp", report_meta.get("run_timestamp", ""), "INFO", "-"))
    summary_rows.append(("META", "run_dir", report_meta.get("run_dir", ""), "INFO", "-"))
    summary_rows.append(("META", "tool_version", report_meta.get("tool_version", ""), "INFO", "-"))
    if focus:
        summary_rows.append(("META", "focus_object", f"{focus.schema}.{focus.name}", "INFO", focus.object_type or "-"))
    summary_rows.append(("META", "deep_mode", "ON" if (focus and args.deep) else "OFF", "INFO", "-"))
    for metric, expected, actual, status, note in consistency_rows:
        summary_rows.append(("CONSISTENCY", metric, expected, actual, status + ";" + note))
    summary_rows.append(("SAMPLED", "detail_rows", len(sampled_rows), "", ""))
    summary_rows.append(("SAMPLED", "false_positive_candidates", sum(1 for x in triage_entries if x.false_positive), "", ""))
    for code, cnt in reason_counter.most_common():
        summary_rows.append(("ROOT_CAUSE", code, cnt, "", ""))
    for klass, cnt in fixup_counter.most_common():
        summary_rows.append(("FIXUP_FAIL_CLASS", klass, cnt, "", ""))

    write_pipe_table(
        summary_file,
        ["SECTION", "KEY", "VALUE1", "VALUE2", "NOTE"],
        summary_rows,
    )

    detail_rows = [
        (
            e.case_id,
            e.severity,
            e.report_type,
            e.object_type,
            e.source_full,
            e.target_full,
            e.report_status,
            e.reason_code,
            e.root_cause,
            e.evidence,
            e.action,
        )
        for e in triage_entries
    ]
    write_pipe_table(
        detail_file,
        [
            "CASE_ID",
            "SEV",
            "REPORT_TYPE",
            "OBJECT_TYPE",
            "SOURCE_OBJECT",
            "TARGET_OBJECT",
            "REPORT_STATUS",
            "REASON_CODE",
            "ROOT_CAUSE",
            "EVIDENCE",
            "ACTION",
        ],
        detail_rows,
    )

    fp_rows = [
        (
            e.case_id,
            e.report_type,
            e.object_type,
            e.source_full,
            e.target_full,
            e.reason_code,
            e.root_cause,
            e.action,
        )
        for e in triage_entries
        if e.false_positive or e.reason_code.startswith("FP_")
    ]
    write_pipe_table(
        fp_file,
        ["CASE_ID", "REPORT_TYPE", "OBJECT_TYPE", "SOURCE_OBJECT", "TARGET_OBJECT", "REASON_CODE", "ROOT_CAUSE", "ACTION"],
        fp_rows,
    )

    fixup_rows = [
        (
            f.line_no,
            f.script_hint,
            f.error_code,
            f.failure_class,
            f.root_cause,
            f.action,
            f.raw_line,
        )
        for f in fixup_failures
    ]
    write_pipe_table(
        fixup_file,
        ["LINE_NO", "SCRIPT_HINT", "ERROR_CODE", "CLASS", "ROOT_CAUSE", "ACTION", "RAW_LINE"],
        fixup_rows,
    )

    if focus and args.deep:
        write_pipe_table(
            deep_file,
            ["SECTION", "KEY1", "KEY2", "VALUE", "NOTE"],
            focus_deep_rows,
        )

    print(f"[OK] report_id={report_id}")
    print(f"[OK] output_dir={output_dir}")
    print(f"[OK] summary={summary_file}")
    print(f"[OK] detail={detail_file}")
    print(f"[OK] fixup={fixup_file}")
    print(f"[OK] false_positive={fp_file}")
    if focus and args.deep:
        print(f"[OK] deep={deep_file}")
    return 0


def main() -> None:
    try:
        raise SystemExit(run())
    except TriageError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(2)
    except KeyboardInterrupt:
        print("[ERROR] interrupted", file=sys.stderr)
        raise SystemExit(130)


if __name__ == "__main__":
    main()
