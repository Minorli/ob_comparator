#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Create customer diagnostic bundles for comparator runs."""

from __future__ import annotations

import argparse
import configparser
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from comparator_reliability import redact_sensitive_text, sanitized_config_text, utcish_now_text
except ModuleNotFoundError as exc:
    if exc.name == "comparator_reliability":
        print(
            "ERROR: missing local file comparator_reliability.py.\n"
            "Comparator 0.9.9.6+ must be deployed from the full toolkit directory; "
            "do not copy only diagnostic_bundle.py.\n"
            "Download the ob_comparator toolkit zip or copy comparator_reliability.py "
            "next to this script.\n"
            "This is an internal project file, not a pip package.",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
    raise

TOOL_VERSION = "0.9.9.6-hotfix2"
SECRET_RE = re.compile(
    r"(password|passwd|pwd|token|secret|private[_-]?key|credential|wallet)", re.IGNORECASE
)
SQL_EXTENSIONS = {".sql", ".ddl"}
DEFAULT_MAX_FILE_MB = 20
DEFAULT_MAX_BUNDLE_MB = 200
OBCLIENT_SECURE_OPT = "--defaults-extra-file"
REPORT_DB_TABLES = {
    "summary": "DIFF_REPORT_SUMMARY",
    "detail": "DIFF_REPORT_DETAIL",
    "detail_item": "DIFF_REPORT_DETAIL_ITEM",
    "grants": "DIFF_REPORT_GRANT",
    "counts": "DIFF_REPORT_COUNTS",
    "usability": "DIFF_REPORT_USABILITY",
    "table_presence": "DIFF_REPORT_TABLE_PRESENCE",
    "package_compare": "DIFF_REPORT_PACKAGE_COMPARE",
    "trigger_status": "DIFF_REPORT_TRIGGER_STATUS",
    "artifact": "DIFF_REPORT_ARTIFACT",
    "artifact_line": "DIFF_REPORT_ARTIFACT_LINE",
    "dependency": "DIFF_REPORT_DEPENDENCY",
    "view_chain": "DIFF_REPORT_VIEW_CHAIN",
    "remap_conflict": "DIFF_REPORT_REMAP_CONFLICT",
    "object_mapping": "DIFF_REPORT_OBJECT_MAPPING",
    "blacklist": "DIFF_REPORT_BLACKLIST",
    "excluded_objects": "DIFF_REPORT_EXCLUDED_OBJECT",
    "fixup_skip": "DIFF_REPORT_FIXUP_SKIP",
    "oms_missing": "DIFF_REPORT_OMS_MISSING",
    "write_errors": "DIFF_REPORT_WRITE_ERRORS",
    "resolution": "DIFF_REPORT_RESOLUTION",
}
SQL_IDENTIFIER_RE = re.compile(r"^[A-Z_][A-Z0-9_$#]*$")


def sha1_bytes(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_bool(value: object, default: bool = False) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "y", "on"}


def sql_quote(value: object) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def split_obclient_fields(line: str) -> List[str]:
    if "\t" in line:
        return line.split("\t")
    if "\\t" in line:
        return line.split("\\t")
    return [line]


def normalize_report_db_schema(value: object) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if not SQL_IDENTIFIER_RE.match(text):
        raise ValueError(f"invalid report_db_schema: {value}")
    return text


def read_config(config_path: Optional[Path]) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    if config_path:
        parser.read(config_path, encoding="utf-8")
    return parser


def parse_positive_int(value: object, default: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        return default
    return parsed if parsed > 0 else default


def diagnostic_defaults_from_config(config_path: Optional[Path]) -> Dict[str, object]:
    cfg = read_config(config_path)
    settings = cfg["SETTINGS"] if cfg.has_section("SETTINGS") else {}
    return {
        "include_sql_content": parse_bool(
            settings.get("diagnostic_include_sql_content", "false"), False
        ),
        "redact_identifiers": parse_bool(
            settings.get("diagnostic_redact_identifiers", "false"), False
        ),
        "max_file_mb": parse_positive_int(
            settings.get("diagnostic_max_file_mb", DEFAULT_MAX_FILE_MB), DEFAULT_MAX_FILE_MB
        ),
        "max_bundle_mb": parse_positive_int(
            settings.get("diagnostic_max_bundle_mb", DEFAULT_MAX_BUNDLE_MB), DEFAULT_MAX_BUNDLE_MB
        ),
    }


def extract_report_id_from_run_dir(run_dir: Optional[Path]) -> str:
    if not run_dir:
        return ""
    for path in sorted(run_dir.glob("report_sql_*.txt"), reverse=True):
        try:
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                if line.startswith("# report_id="):
                    return line.split("=", 1)[1].strip()
        except Exception:
            continue
    return ""


def redact_text(text: str) -> str:
    lines: List[str] = []
    for raw_line in (text or "").splitlines():
        line = raw_line
        if "=" in line:
            key, value = line.split("=", 1)
            if SECRET_RE.search(key):
                line = f"{key}=<REDACTED>"
            else:
                line = redact_sensitive_text(line)
        line = re.sub(r"(?i)(password|token|secret)\s*[:=]\s*[^,\s;]+", r"\1=<REDACTED>", line)
        line = redact_sensitive_text(line)
        lines.append(line)
    return "\n".join(lines)


def obclient_sql(
    cfg: configparser.ConfigParser,
    sql_text: str,
    *,
    timeout: int = 30,
) -> Tuple[bool, str, str]:
    if not cfg.has_section("OCEANBASE_TARGET"):
        return False, "", "missing [OCEANBASE_TARGET] section"
    section = cfg["OCEANBASE_TARGET"]
    executable = str(section.get("executable", "")).strip()
    host = str(section.get("host", "")).strip()
    port = str(section.get("port", "")).strip()
    user_string = str(section.get("user_string", "")).strip()
    password = str(section.get("password", "") or "")
    missing = [
        name
        for name, value in {
            "executable": executable,
            "host": host,
            "port": port,
            "user_string": user_string,
        }.items()
        if not value
    ]
    if missing:
        return False, "", "missing OCEANBASE_TARGET keys: " + ", ".join(missing)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", prefix="comparator_diag_ob_", suffix=".cnf", delete=False
    ) as defaults_file:
        defaults_file.write("[client]\n")
        defaults_file.write(
            'password="' + password.replace("\\", "\\\\").replace('"', '\\"') + '"\n'
        )
        defaults_path = Path(defaults_file.name)
    try:
        try:
            defaults_path.chmod(0o600)
        except Exception:
            pass
        result = subprocess.run(
            [
                executable,
                f"{OBCLIENT_SECURE_OPT}={defaults_path}",
                "-h",
                host,
                "-P",
                port,
                "-u",
                user_string,
                "-ss",
            ],
            input=sql_text.strip().rstrip(";") + ";\n",
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=max(1, int(timeout or 30)),
        )
    except Exception as exc:
        return False, "", str(exc)
    finally:
        try:
            defaults_path.unlink()
        except Exception:
            pass
    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        return False, stdout, stderr or f"obclient return code {result.returncode}"
    for line in (stderr + "\n" + stdout).splitlines():
        if re.match(r"^(ORA-\d{5}|OB-\d+|ERROR(\s+\d+|\b))", line.strip(), flags=re.I):
            return False, stdout, line.strip()
    return True, stdout, ""


def collect_report_db_summary(
    *,
    run_dir: Optional[Path],
    config_path: Optional[Path],
    artifacts_dir: Path,
    omitted: List[Dict[str, str]],
) -> Dict[str, object]:
    cfg = read_config(config_path)
    settings = cfg["SETTINGS"] if cfg.has_section("SETTINGS") else {}
    enabled = parse_bool(settings.get("report_to_db", "false"), False)
    summary: Dict[str, object] = {
        "enabled": bool(enabled),
        "status": "disabled",
        "report_id": "",
        "schema": "",
        "row_counts": {},
        "object_counts": [],
    }
    if not enabled:
        (artifacts_dir / "report_db_diagnostic_summary.txt").write_text(
            "report_db_enabled: false\n", encoding="utf-8"
        )
        return summary
    try:
        schema = normalize_report_db_schema(settings.get("report_db_schema", ""))
    except Exception as exc:
        summary["status"] = "invalid_config"
        summary["error"] = str(exc)
        omitted.append({"path": "report_db", "reason": str(exc)})
        return summary
    schema_prefix = f"{schema}." if schema else ""
    summary["schema"] = schema or "<target user>"
    report_id = extract_report_id_from_run_dir(run_dir)
    summary["report_id"] = report_id
    if not report_id:
        summary["status"] = "missing_report_id"
        omitted.append({"path": "report_db", "reason": "no report_sql_*.txt with report_id found"})
        return summary

    summary_sql = (
        "SELECT REPORT_ID || CHR(9) || WRITE_STATUS || CHR(9) || WRITE_EXPECTED_ROWS || "
        "CHR(9) || WRITE_ACTUAL_ROWS || CHR(9) || CONCLUSION || CHR(9) || NVL(WRITE_NOTE, '-') "
        f"FROM {schema_prefix}{REPORT_DB_TABLES['summary']} "
        f"WHERE REPORT_ID = {sql_quote(report_id)}"
    )
    ok, out, err = obclient_sql(cfg, summary_sql)
    if not ok:
        summary["status"] = "query_failed"
        summary["error"] = redact_text(err or out)
        omitted.append({"path": "report_db", "reason": summary["error"]})
        return summary
    lines = [line for line in out.splitlines() if line.strip()]
    if not lines:
        summary["status"] = "not_found"
        omitted.append({"path": "report_db", "reason": f"report_id not found: {report_id}"})
        return summary
    fields = split_obclient_fields(lines[0])
    summary.update(
        {
            "status": "queried",
            "write_status": fields[1] if len(fields) > 1 else "",
            "write_expected_rows": fields[2] if len(fields) > 2 else "",
            "write_actual_rows": fields[3] if len(fields) > 3 else "",
            "conclusion": fields[4] if len(fields) > 4 else "",
            "write_note": fields[5] if len(fields) > 5 else "",
        }
    )

    row_counts: Dict[str, int] = {}
    for key, table_name in REPORT_DB_TABLES.items():
        count_sql = (
            f"SELECT COUNT(*) FROM {schema_prefix}{table_name} "
            f"WHERE REPORT_ID = {sql_quote(report_id)}"
        )
        count_ok, count_out, count_err = obclient_sql(cfg, count_sql)
        if not count_ok:
            row_counts[key] = -1
            omitted.append(
                {"path": f"report_db.{table_name}", "reason": redact_text(count_err or count_out)}
            )
            continue
        match = re.search(r"-?\d+", count_out or "")
        row_counts[key] = int(match.group(0)) if match else -1
    summary["row_counts"] = row_counts

    counts_sql = (
        "SELECT OBJECT_TYPE || CHR(9) || ORACLE_COUNT || CHR(9) || OCEANBASE_COUNT || "
        "CHR(9) || MISSING_COUNT || CHR(9) || MISSING_FIXABLE_COUNT || CHR(9) || "
        "EXCLUDED_COUNT || CHR(9) || UNSUPPORTED_COUNT || CHR(9) || EXTRA_COUNT "
        f"FROM {schema_prefix}{REPORT_DB_TABLES['counts']} "
        f"WHERE REPORT_ID = {sql_quote(report_id)} ORDER BY OBJECT_TYPE"
    )
    counts_ok, counts_out, counts_err = obclient_sql(cfg, counts_sql)
    if counts_ok:
        object_counts = []
        for raw in counts_out.splitlines():
            parts = split_obclient_fields(raw)
            if len(parts) < 8:
                continue
            object_counts.append(
                {
                    "object_type": parts[0],
                    "oracle_count": parts[1],
                    "oceanbase_count": parts[2],
                    "missing_count": parts[3],
                    "missing_fixable_count": parts[4],
                    "excluded_count": parts[5],
                    "unsupported_count": parts[6],
                    "extra_count": parts[7],
                }
            )
        summary["object_counts"] = object_counts
    else:
        omitted.append(
            {"path": "report_db.counts", "reason": redact_text(counts_err or counts_out)}
        )

    lines_out = [
        "report_db_enabled: true",
        f"status: {summary.get('status')}",
        f"schema: {summary.get('schema')}",
        f"report_id: {summary.get('report_id')}",
        f"write_status: {summary.get('write_status', '-')}",
        f"write_expected_rows: {summary.get('write_expected_rows', '-')}",
        f"write_actual_rows: {summary.get('write_actual_rows', '-')}",
        f"conclusion: {summary.get('conclusion', '-')}",
        f"write_note: {summary.get('write_note', '-')}",
        "",
        "TABLE_KEY | ROW_COUNT",
    ]
    lines_out.extend(f"{key} | {value}" for key, value in sorted(row_counts.items()))
    if summary.get("object_counts"):
        lines_out.extend(
            [
                "",
                "OBJECT_TYPE | ORACLE | OCEANBASE | MISSING | FIXABLE | EXCLUDED | UNSUPPORTED | EXTRA",
            ]
        )
        for row in summary["object_counts"]:
            lines_out.append(
                "{object_type} | {oracle_count} | {oceanbase_count} | {missing_count} | "
                "{missing_fixable_count} | {excluded_count} | {unsupported_count} | {extra_count}".format(
                    **row
                )
            )
    (artifacts_dir / "report_db_diagnostic_summary.txt").write_text(
        "\n".join(lines_out).rstrip() + "\n", encoding="utf-8"
    )
    return summary


def read_tail(path: Path, max_lines: int = 300) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as exc:
        return f"<unable to read {path}: {exc}>"
    return "\n".join(lines[-max_lines:])


def ensure_run_dir(path: Path) -> Path:
    run_dir = Path(path).expanduser().resolve()
    if not run_dir.exists() or not run_dir.is_dir():
        raise FileNotFoundError(f"run directory does not exist: {run_dir}")
    return run_dir


def resolve_output_path(run_dir: Optional[Path], raw_output: str) -> Path:
    if raw_output:
        return Path(raw_output).expanduser().resolve()
    run_id = run_dir.name if run_dir else f"pid_{os.getpid()}"
    return Path.cwd() / f"diagnostic_bundle_{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"


def collect_candidate_files(run_dir: Path) -> List[Path]:
    patterns = [
        "report_*.txt",
        "report_index_*.txt",
        "report_sql_*.txt",
        "runtime_timeout_summary_*.txt",
        "run_heartbeat_*.json",
        "fixup_plan_*.jsonl",
        "fixup_safety_summary_*.txt",
        "compatibility_matrix_*.json",
        "compatibility_summary_*.txt",
        "recovery_manifest_*.json",
        "difference_explanations_*.jsonl",
        "difference_explanations_summary_*.txt",
        "manual_actions_required_*.txt",
        "runtime_degraded_detail_*.txt",
        "fixup_skip_summary_*.txt",
        "config_reload_events_*.txt",
    ]
    files: List[Path] = []
    for pattern in patterns:
        files.extend(sorted(run_dir.glob(pattern)))
    files.extend(sorted(run_dir.glob("*_detail_*.txt")))
    unique: List[Path] = []
    seen = set()
    for item in files:
        if item.is_file() and item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def collect_sql_file_plan_metadata(
    run_dir: Optional[Path], config_path: Optional[Path]
) -> Tuple[List[Path], Dict[str, Dict[str, object]]]:
    paths: List[Path] = []
    metadata_by_path: Dict[str, Dict[str, object]] = {}
    if not run_dir:
        return paths, metadata_by_path
    cfg = read_config(config_path)
    settings = cfg["SETTINGS"] if cfg.has_section("SETTINGS") else {}
    fixup_dir_raw = str(settings.get("fixup_dir", "fixup_scripts") or "fixup_scripts").strip()
    if config_path and fixup_dir_raw and not Path(fixup_dir_raw).expanduser().is_absolute():
        fixup_dir = config_path.parent / fixup_dir_raw
    else:
        fixup_dir = Path(fixup_dir_raw or "fixup_scripts").expanduser()
    for plan_path in sorted(run_dir.glob("fixup_plan_*.jsonl")):
        try:
            plan_lines = plan_path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            continue
        for raw in plan_lines:
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
            except Exception:
                continue
            rel_file = str(record.get("file_path") or "").strip()
            if not rel_file:
                continue
            sql_path = (fixup_dir / rel_file).resolve()
            paths.append(sql_path)
            metadata_by_path[str(sql_path)] = {
                "object_type": record.get("object_type") or "",
                "object_identity": record.get("object_identity") or "",
                "operation": record.get("operation") or "",
                "statement_identity": record.get("statement_identity") or "",
                "statement_count": record.get("statement_count") or 0,
                "safety_tier": record.get("safety_tier") or "",
                "reason_code": record.get("reason_code") or "",
                "compatibility_decision": record.get("compatibility_decision") or "",
            }
    if not paths and fixup_dir.exists():
        paths.extend(sorted(fixup_dir.rglob("*.sql")))
    unique_paths: List[Path] = []
    seen = set()
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique_paths.append(path)
    return unique_paths, metadata_by_path


def summarize_sql_files(
    paths: Iterable[Path],
    include_content: bool,
    max_bytes: int,
    metadata_by_path: Optional[Dict[str, Dict[str, object]]] = None,
) -> Tuple[List[Dict[str, object]], Dict[str, str]]:
    records: List[Dict[str, object]] = []
    content_by_name: Dict[str, str] = {}
    metadata_by_path = metadata_by_path or {}
    for path in sorted(paths):
        if not path.is_file() or path.suffix.lower() not in SQL_EXTENSIONS:
            continue
        try:
            size = path.stat().st_size
            digest = sha1_file(path)
        except Exception as exc:
            records.append({"path": str(path), "error": str(exc)})
            continue
        record = {
            "path": str(path),
            "name": path.name,
            "size_bytes": size,
            "sha1": digest,
            "content_included": False,
        }
        plan_metadata = metadata_by_path.get(str(path.resolve())) or metadata_by_path.get(str(path))
        if plan_metadata:
            record.update(plan_metadata)
        if include_content and size <= max_bytes:
            try:
                content = redact_text(path.read_text(encoding="utf-8", errors="replace"))
                arcname = f"artifacts/sql_content/{digest[:12]}_{path.name}"
                content_by_name[arcname] = content
                record["content_included"] = True
                record["content_artifact"] = arcname
            except Exception as exc:
                record["content_error"] = str(exc)
        elif include_content:
            record["omitted_reason"] = f"sql file exceeds max bytes {max_bytes}"
        else:
            record["omitted_reason"] = "sql content opt-in not enabled"
        records.append(record)
    return records, content_by_name


def heartbeat_pid(heartbeat_state: Optional[Dict[str, object]]) -> Optional[int]:
    if not isinstance(heartbeat_state, dict):
        return None
    try:
        pid_value = int(heartbeat_state.get("pid") or 0)
    except Exception:
        return None
    return pid_value if pid_value > 0 else None


def read_proc_text(proc: Path, name: str) -> str:
    return (proc / name).read_text(encoding="utf-8", errors="replace")


def process_owner_uid(proc: Path) -> Optional[int]:
    try:
        for line in read_proc_text(proc, "status").splitlines():
            if line.startswith("Uid:"):
                parts = line.split()
                if len(parts) >= 2:
                    return int(parts[1])
    except Exception:
        return None
    return None


def is_allowed_process_snapshot(
    proc: Path,
    pid: int,
    heartbeat_state: Optional[Dict[str, object]],
) -> Tuple[bool, str]:
    expected_pid = heartbeat_pid(heartbeat_state)
    owner_uid = process_owner_uid(proc)
    if owner_uid is not None and owner_uid != os.geteuid():
        return False, "pid is not owned by current user"
    if expected_pid is not None and int(pid) != expected_pid:
        return False, f"pid does not match heartbeat pid {expected_pid}"

    try:
        comm = read_proc_text(proc, "comm").strip()
    except Exception:
        comm = ""
    try:
        exe_name = (proc / "exe").resolve().name
    except Exception:
        exe_name = ""
    try:
        raw_cmdline = (proc / "cmdline").read_bytes()
        cmdline = raw_cmdline.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
    except Exception:
        cmdline = ""

    process_names = {
        item
        for item in (
            comm,
            exe_name,
            Path(cmdline.split(" ", 1)[0]).name if cmdline else "",
        )
        if item
    }
    if "obclient" in process_names:
        return True, "allowed obclient process"
    if any(name.startswith("python") for name in process_names) and (
        "schema_diff_reconciler.py" in cmdline or "run_fixup.py" in cmdline
    ):
        if expected_pid is not None:
            return True, "heartbeat pid match and comparator python process"
        return True, "allowed comparator python process"
    if expected_pid is not None:
        return False, "heartbeat pid matched but process executable is not allowed"
    return False, "pid is not a comparator python process or obclient"


def process_snapshot(
    pid: Optional[int],
    heartbeat_state: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    if not pid:
        return {}
    proc = Path("/proc") / str(pid)
    snapshot: Dict[str, object] = {"pid": pid, "available": proc.exists()}
    if not proc.exists():
        snapshot["error"] = "process not found"
        return snapshot
    allowed, reason = is_allowed_process_snapshot(proc, int(pid), heartbeat_state)
    snapshot["allowed"] = bool(allowed)
    snapshot["validation"] = reason
    try:
        snapshot["comm"] = read_proc_text(proc, "comm").strip()
    except Exception as exc:
        snapshot["comm_error"] = str(exc)
    if not allowed:
        snapshot["error"] = "process snapshot rejected"
        return snapshot
    for name in ("cmdline", "status", "stat"):
        path = proc / name
        try:
            raw = path.read_bytes()
            if name == "cmdline":
                value = raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
            else:
                value = raw.decode("utf-8", errors="replace")
            snapshot[name] = redact_text(value)
        except Exception as exc:
            snapshot[f"{name}_error"] = str(exc)
    return snapshot


def latest_log_tail(run_dir: Optional[Path]) -> Tuple[str, str]:
    candidates: List[Path] = []
    if run_dir:
        repo_root = Path(__file__).resolve().parent
        candidates.extend(sorted((repo_root / "logs").glob("run_*.log")))
        candidates.extend(sorted(run_dir.glob("*.log")))
    if not candidates:
        return "", "no log candidate found"
    latest = max(candidates, key=lambda p: p.stat().st_mtime if p.exists() else 0)
    return read_tail(latest), str(latest)


def maybe_redact_identifiers(text: str, enabled: bool, mapping: Dict[str, str]) -> str:
    if not enabled:
        return text

    def repl(match: re.Match) -> str:
        token = match.group(0)
        if token.upper() in {
            "SELECT",
            "FROM",
            "WHERE",
            "TABLE",
            "VIEW",
            "INDEX",
            "CONSTRAINT",
            "TRIGGER",
            "PACKAGE",
            "TYPE",
            "ALTER",
            "CREATE",
            "DROP",
            "NULL",
            "NOT",
            "DEFAULT",
        }:
            return token
        if token not in mapping:
            mapping[token] = "ID_" + hashlib.sha1(token.encode("utf-8")).hexdigest()[:12].upper()
        return mapping[token]

    return re.sub(r"\b[A-Za-z_][A-Za-z0-9_$#]{2,}\b", repl, text)


def write_bundle(
    *,
    run_dir: Optional[Path],
    config_path: Optional[Path],
    output_path: Path,
    pid: Optional[int],
    hang: bool,
    include_sql_content: bool,
    redact_identifiers: bool,
    max_file_mb: int,
    max_bundle_mb: int = DEFAULT_MAX_BUNDLE_MB,
) -> Path:
    if run_dir:
        run_dir = ensure_run_dir(run_dir)
    max_bytes = max(1, max_file_mb) * 1024 * 1024
    max_bundle_bytes = max(1, max_bundle_mb) * 1024 * 1024
    included_bundle_bytes = 0
    omitted: List[Dict[str, str]] = []
    files: List[Dict[str, object]] = []
    identifier_map: Dict[str, str] = {}

    with tempfile.TemporaryDirectory(prefix="comparator_diag_") as tmp:
        tmp_dir = Path(tmp)
        artifacts_dir = tmp_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        config_text = ""
        if config_path:
            try:
                config_text = sanitized_config_text(config_path)
            except Exception as exc:
                config_text = f"# unable to read config: {exc}\n"
        config_text = maybe_redact_identifiers(config_text, redact_identifiers, identifier_map)
        (tmp_dir / "config_sanitized.ini").write_text(config_text, encoding="utf-8")

        artifact_sources = collect_candidate_files(run_dir) if run_dir else []
        for source in artifact_sources:
            try:
                size = source.stat().st_size
                rel = source.relative_to(run_dir) if run_dir else source.name
                arcname = artifacts_dir / str(rel)
                if source.suffix.lower() in SQL_EXTENSIONS and not include_sql_content:
                    omitted.append(
                        {"path": str(source), "reason": "sql content opt-in not enabled"}
                    )
                    continue
                if size > max_bytes:
                    omitted.append(
                        {"path": str(source), "reason": f"file exceeds {max_bytes} bytes"}
                    )
                    continue
                content = source.read_text(encoding="utf-8", errors="replace")
                content = redact_text(content)
                content = maybe_redact_identifiers(content, redact_identifiers, identifier_map)
                encoded_content = content.encode("utf-8")
                if included_bundle_bytes + len(encoded_content) > max_bundle_bytes:
                    omitted.append(
                        {
                            "path": str(source),
                            "reason": f"bundle total would exceed {max_bundle_bytes} bytes",
                        }
                    )
                    continue
                arcname.parent.mkdir(parents=True, exist_ok=True)
                arcname.write_text(content, encoding="utf-8")
                included_bundle_bytes += len(encoded_content)
                files.append(
                    {
                        "source_path": str(source),
                        "bundle_path": str(arcname.relative_to(tmp_dir)),
                        "source_size_bytes": size,
                        "bundle_size_bytes": len(encoded_content),
                        "source_sha1": sha1_file(source),
                        "sha1": sha1_bytes(encoded_content),
                    }
                )
            except Exception as exc:
                omitted.append({"path": str(source), "reason": str(exc)})

        sql_candidates, sql_metadata = collect_sql_file_plan_metadata(run_dir, config_path)
        sql_summary, sql_content = summarize_sql_files(
            sql_candidates, include_sql_content, max_bytes, sql_metadata
        )
        (artifacts_dir / "sql_file_summary.json").write_text(
            json.dumps(sql_summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for arcname, content in sql_content.items():
            final_content = maybe_redact_identifiers(content, redact_identifiers, identifier_map)
            encoded_content = final_content.encode("utf-8")
            if included_bundle_bytes + len(encoded_content) > max_bundle_bytes:
                omitted.append(
                    {
                        "path": arcname,
                        "reason": f"bundle total would exceed {max_bundle_bytes} bytes",
                    }
                )
                continue
            target = tmp_dir / arcname
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(final_content, encoding="utf-8")
            included_bundle_bytes += len(encoded_content)

        report_db_summary = collect_report_db_summary(
            run_dir=run_dir,
            config_path=config_path,
            artifacts_dir=artifacts_dir,
            omitted=omitted,
        )

        log_tail, log_source = latest_log_tail(run_dir)
        (artifacts_dir / "log_tail.txt").write_text(
            maybe_redact_identifiers(redact_text(log_tail), redact_identifiers, identifier_map),
            encoding="utf-8",
        )

        heartbeat_files = sorted(run_dir.glob("*heartbeat*.json")) if run_dir else []
        heartbeat_state: Dict[str, object] = {}
        if heartbeat_files:
            latest_heartbeat = max(heartbeat_files, key=lambda p: p.stat().st_mtime)
            try:
                heartbeat_state = json.loads(latest_heartbeat.read_text(encoding="utf-8"))
            except Exception as exc:
                heartbeat_state = {"error": str(exc), "path": str(latest_heartbeat)}
        elif hang:
            omitted.append({"path": "heartbeat", "reason": "no heartbeat state found"})

        run_state = {
            "schema_version": 1,
            "run_dir": str(run_dir or ""),
            "hang": bool(hang),
            "heartbeat": heartbeat_state,
            "process": process_snapshot(pid, heartbeat_state),
            "log_tail_source": log_source,
            "report_db": report_db_summary,
            "collected_at": utcish_now_text(),
        }
        (tmp_dir / "run_state.json").write_text(
            json.dumps(run_state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        missing_evidence: List[str] = []
        if run_dir and not list(run_dir.glob("report_*.txt")):
            missing_evidence.append("main report")
        if run_dir and not list(run_dir.glob("report_index_*.txt")):
            missing_evidence.append("report index")
        if run_dir and not heartbeat_files:
            missing_evidence.append("heartbeat")
        if run_dir and not list(run_dir.glob("fixup_plan_*.jsonl")):
            missing_evidence.append("fixup plan")

        summary_lines = [
            "Comparator diagnostic bundle",
            f"collected_at: {utcish_now_text()}",
            f"tool_version: {TOOL_VERSION}",
            f"run_dir: {run_dir or '-'}",
            f"hang_mode: {str(bool(hang)).lower()}",
            f"pid: {pid or '-'}",
            f"sql_content_included: {str(bool(include_sql_content)).lower()}",
            f"identifier_redaction: {str(bool(redact_identifiers)).lower()}",
            f"max_bundle_mb: {max_bundle_mb}",
            f"report_db_status: {report_db_summary.get('status', '-')}",
            f"report_db_report_id: {report_db_summary.get('report_id', '-') or '-'}",
            f"log_tail_source: {log_source}",
            f"missing_evidence: {', '.join(missing_evidence) if missing_evidence else '-'}",
            "",
            "next_command:",
            " ".join(
                [
                    "python3",
                    "diagnostic_bundle.py",
                    "--run-dir",
                    shlex.quote(str(run_dir or "<run_dir>")),
                    "--config",
                    shlex.quote(str(config_path or "<config.ini>")),
                ]
            ),
        ]
        (tmp_dir / "summary.txt").write_text(
            "\n".join(summary_lines).rstrip() + "\n", encoding="utf-8"
        )

        manifest = {
            "schema_version": 1,
            "tool_version": TOOL_VERSION,
            "created_at": utcish_now_text(),
            "run_dir": str(run_dir or ""),
            "config_path": str(config_path or ""),
            "pid": pid,
            "hang": bool(hang),
            "max_file_mb": max_file_mb,
            "max_bundle_mb": max_bundle_mb,
            "included_bundle_bytes": included_bundle_bytes,
            "redaction_policy": {
                "secrets": "default",  # pragma: allowlist secret
                "sql_content_included": bool(include_sql_content),
                "identifier_redaction": bool(redact_identifiers),
            },
            "files": files,
            "omitted": omitted,
            "missing_evidence": missing_evidence,
            "report_db": report_db_summary,
        }
        manifest_path = tmp_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if redact_identifiers and identifier_map:
            map_path = tmp_dir / "identifier_hash_map.local.json"
            map_path.write_text(
                json.dumps(identifier_map, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            omitted.append(
                {
                    "path": "identifier_hash_map.local.json",
                    "reason": "local-only map excluded from bundle",
                }
            )
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(tmp_dir.rglob("*")):
                if not path.is_file():
                    continue
                if path.name == "identifier_hash_map.local.json":
                    continue
                zf.write(path, path.relative_to(tmp_dir).as_posix())
    return output_path


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a redacted diagnostic bundle for a comparator run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--run-dir", default="", help="main_reports/run_<timestamp> directory")
    parser.add_argument("--config", default="", help="config.ini used for the run")
    parser.add_argument("--output", default="", help="output diagnostic_bundle_*.zip path")
    parser.add_argument(
        "--pid", type=int, default=0, help="running comparator/run_fixup process id"
    )
    parser.add_argument(
        "--hang",
        action="store_true",
        help="collect hang-triage snapshot without waiting for process exit",
    )
    parser.add_argument(
        "--include-sql-content",
        action="store_true",
        default=None,
        help="include full SQL content after redaction and size checks",
    )
    parser.add_argument(
        "--redact-identifiers",
        action="store_true",
        default=None,
        help="replace schema/object/column-like identifiers with stable hashes",
    )
    parser.add_argument(
        "--max-file-mb", type=int, default=None, help="maximum artifact size to include"
    )
    parser.add_argument(
        "--max-bundle-mb",
        type=int,
        default=None,
        help="maximum uncompressed artifact bytes to include",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    run_dir = Path(args.run_dir).expanduser().resolve() if args.run_dir else None
    config_path = Path(args.config).expanduser().resolve() if args.config else None
    try:
        if run_dir is not None:
            run_dir = ensure_run_dir(run_dir)
        if config_path is not None and not config_path.exists():
            raise FileNotFoundError(f"config file does not exist: {config_path}")
        diag_defaults = diagnostic_defaults_from_config(config_path)
        include_sql_content = (
            bool(args.include_sql_content)
            if args.include_sql_content is not None
            else bool(diag_defaults["include_sql_content"])
        )
        redact_identifiers = (
            bool(args.redact_identifiers)
            if args.redact_identifiers is not None
            else bool(diag_defaults["redact_identifiers"])
        )
        max_file_mb = (
            max(1, int(args.max_file_mb))
            if args.max_file_mb is not None
            else int(diag_defaults["max_file_mb"])
        )
        max_bundle_mb = (
            max(1, int(args.max_bundle_mb))
            if args.max_bundle_mb is not None
            else int(diag_defaults["max_bundle_mb"])
        )
        output_path = resolve_output_path(run_dir, args.output)
        bundle_path = write_bundle(
            run_dir=run_dir,
            config_path=config_path,
            output_path=output_path,
            pid=args.pid or None,
            hang=bool(args.hang),
            include_sql_content=include_sql_content,
            redact_identifiers=redact_identifiers,
            max_file_mb=max_file_mb,
            max_bundle_mb=max_bundle_mb,
        )
    except Exception as exc:
        print(f"diagnostic bundle failed: {exc}", file=sys.stderr)
        return 2
    print(f"diagnostic bundle created: {bundle_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
