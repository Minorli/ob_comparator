#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared production reliability helpers for comparator CLI tools."""

from __future__ import annotations

import configparser
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

SAFETY_TIER_SAFE = "safe"
SAFETY_TIER_REVIEW = "review"
SAFETY_TIER_DESTRUCTIVE = "destructive"
SAFETY_TIER_MANUAL = "manual"

DECISION_OK = "OK"
DECISION_MISMATCH = "MISMATCH"
DECISION_FIXUP = "FIXUP"
DECISION_REVIEW = "REVIEW"
DECISION_SUPPRESS = "SUPPRESS"
DECISION_MANUAL = "MANUAL"

ACTION_NONE = "NONE"
ACTION_REPORT_ONLY = "REPORT_ONLY"
ACTION_GENERATE_FIXUP = "GENERATE_FIXUP"
ACTION_SUPPRESS = "SUPPRESS"
ACTION_MANUAL_REVIEW = "MANUAL_REVIEW"

COMPAT_DECISION_SUPPORTED = "supported"
COMPAT_DECISION_DEGRADED = "degraded"
COMPAT_DECISION_MANUAL = "manual"
COMPAT_DECISION_UNSUPPORTED = "unsupported"

DEFAULT_COMPATIBILITY_REGISTRY = "compatibility_registry.json"
DECISION_CONFIG_KEYS = {
    "source_db_mode",
    "source_schemas",
    "remap_file",
    "check_primary_types",
    "check_extra_types",
    "check_dependencies",
    "check_comments",
    "check_column_order",
    "check_status_drift_types",
    "generate_fixup",
    "fixup_schemas",
    "fixup_types",
    "fixup_drop_sys_c_columns",
    "generate_extra_cleanup",
    "extra_constraint_cleanup_mode",
    "plain_not_null_fixup_mode",
    "fixup_idempotent_mode",
    "fixup_idempotent_types",
    "generate_grants",
    "grant_generation_mode",
    "grant_tab_privs_scope",
    "grant_supported_sys_privs",
    "grant_supported_object_privs",
    "synonym_check_scope",
    "synonym_fixup_scope",
    "source_object_scope_mode",
    "object_created_before",
    "object_created_before_missing_created_policy",
    "blacklist_mode",
    "blacklist_rules_path",
    "blacklist_rules_enable",
    "blacklist_rules_disable",
    "blacklist_name_patterns",
    "blacklist_name_patterns_file",
    "exclude_objects_file",
    "compatibility_registry_path",
}
RUNTIME_CONFIG_KEYS = {
    "report_dir",
    "report_dir_layout",
    "report_detail_mode",
    "report_width",
    "log_dir",
    "log_level",
    "progress_log_interval",
    "slow_phase_warning_sec",
    "slow_sql_warning_sec",
    "diagnostic_bundle_enable",
    "diagnostic_bundle_output_dir",
    "diagnostic_include_sql_content",
    "diagnostic_redact_identifiers",
    "diagnostic_max_file_mb",
    "diagnostic_max_bundle_mb",
}

URL_CREDENTIAL_RE = re.compile(r"//([^:/@\s]+):([^@\s]+)@", re.I)
ORACLE_CONNECT_CREDENTIAL_RE = re.compile(r"(?<![\w$#])([A-Za-z][\w$#]*)/([^@\s/]+)@")
LONG_PASSWORD_EQUALS_RE = re.compile(r"(?i)(--password=)(\"[^\"]*\"|'[^']*'|\S+)")
LONG_PASSWORD_SPACE_RE = re.compile(r"(?i)(--password\s+)(\"[^\"]*\"|'[^']*'|\S+)")
SHORT_PASSWORD_RE = re.compile(r"(?<!\S)-p\s*\S+")
DESTRUCTIVE_SQL_RE = re.compile(r"\b(DROP|TRUNCATE|DISABLE|REVOKE)\b", re.IGNORECASE)
DROP_FORCE_RE = re.compile(r"\bDROP\b[\s\S]{0,120}\bFORCE\b", re.IGNORECASE)

MANUAL_FIXUP_DIRS = {
    "unsupported",
    "tables_unsupported",
    "materialized_view",
    "job",
    "schedule",
}
DESTRUCTIVE_FIXUP_DIRS = {
    "cleanup_safe",
    "cleanup_semantic",
    "grants_revoke",
}
SAFE_COMPILE_TYPES = {
    "FUNCTION",
    "PACKAGE",
    "PACKAGE BODY",
    "PROCEDURE",
    "TRIGGER",
    "TYPE",
    "TYPE BODY",
    "VIEW",
}
DIR_OBJECT_TYPE_MAP = {
    "compile": "COMPILE",
    "constraint": "CONSTRAINT",
    "constraint_validate_later": "CONSTRAINT",
    "context": "CONTEXT",
    "function": "FUNCTION",
    "grants_all": "GRANT",
    "grants_deferred": "GRANT",
    "grants_miss": "GRANT",
    "grants_revoke": "GRANT",
    "index": "INDEX",
    "job": "JOB",
    "materialized_view": "MATERIALIZED VIEW",
    "name_collision": "CONSTRAINT",
    "package": "PACKAGE",
    "package_body": "PACKAGE BODY",
    "procedure": "PROCEDURE",
    "schedule": "SCHEDULE",
    "sequence": "SEQUENCE",
    "sequence_restart": "SEQUENCE",
    "synonym": "SYNONYM",
    "table": "TABLE",
    "table_alter": "TABLE",
    "tables_unsupported": "TABLE",
    "trigger": "TRIGGER",
    "type": "TYPE",
    "type_body": "TYPE BODY",
    "unsupported": "UNSUPPORTED",
    "view": "VIEW",
    "view_post_grants": "GRANT",
    "view_prereq_grants": "GRANT",
    "view_refresh": "VIEW",
}


def utcish_now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def file_sha1(path: Path) -> str:
    path = Path(path)
    h = hashlib.sha1()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def stable_json_hash(value: object) -> str:
    data = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha1(data).hexdigest()


def _settings_subset(settings: Dict[str, object], keys: Set[str]) -> Dict[str, str]:
    subset: Dict[str, str] = {}
    for key in sorted(keys):
        if key not in settings:
            continue
        value = settings.get(key)
        if isinstance(value, (dict, list, set, tuple)):
            subset[key] = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        else:
            subset[key] = str(value)
    return subset


def compute_decision_config_hash(settings: Dict[str, object]) -> str:
    return stable_json_hash(_settings_subset(settings or {}, DECISION_CONFIG_KEYS))


def compute_runtime_config_hash(settings: Dict[str, object]) -> str:
    return stable_json_hash(_settings_subset(settings or {}, RUNTIME_CONFIG_KEYS))


def changed_config_keys(
    previous: Dict[str, object], current: Dict[str, object], keys: Iterable[str]
) -> List[str]:
    changed: List[str] = []
    for key in sorted(set(keys)):
        if str((previous or {}).get(key, "")) != str((current or {}).get(key, "")):
            changed.append(key)
    return changed


def build_reason_record(
    *,
    reason_code: str,
    rule_id: str,
    object_type: str,
    object_identity: str,
    item_type: str = "",
    item_key: str = "",
    source_evidence: str = "",
    target_evidence: str = "",
    decision: str = DECISION_MISMATCH,
    action: str = ACTION_REPORT_ONLY,
    compatibility_decision: str = "",
    safety_tier: str = "",
    artifact_path: str = "",
    detail: str = "",
) -> Dict[str, object]:
    return {
        "schema_version": 1,
        "reason_code": str(reason_code or "").strip().upper(),
        "rule_id": str(rule_id or "").strip(),
        "object_type": str(object_type or "").strip().upper(),
        "object_identity": str(object_identity or "").strip(),
        "item_type": str(item_type or "").strip().upper(),
        "item_key": str(item_key or "").strip(),
        "source_evidence": str(source_evidence or "").strip(),
        "target_evidence": str(target_evidence or "").strip(),
        "decision": str(decision or "").strip().upper(),
        "action": str(action or "").strip().upper(),
        "compatibility_decision": str(compatibility_decision or "").strip().lower(),
        "safety_tier": str(safety_tier or "").strip().lower(),
        "artifact_path": str(artifact_path or "").strip(),
        "detail": str(detail or "").strip(),
    }


def export_reason_records(
    records: Sequence[Dict[str, object]], report_dir: Path, timestamp: str
) -> Tuple[Path, Path]:
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = report_dir / f"difference_explanations_{timestamp}.jsonl"
    summary_path = report_dir / f"difference_explanations_summary_{timestamp}.txt"
    with jsonl_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
    by_reason: Dict[str, int] = {}
    by_decision: Dict[str, int] = {}
    by_action: Dict[str, int] = {}
    for record in records:
        reason = str(record.get("reason_code") or "UNKNOWN")
        decision = str(record.get("decision") or "UNKNOWN")
        action = str(record.get("action") or "UNKNOWN")
        by_reason[reason] = by_reason.get(reason, 0) + 1
        by_decision[decision] = by_decision.get(decision, 0) + 1
        by_action[action] = by_action.get(action, 0) + 1
    lines = [
        "# difference explanation summary",
        f"records: {len(records)}",
        f"artifact: {jsonl_path.name}",
        "",
        "# by decision",
    ]
    lines.extend(f"{key} | {value}" for key, value in sorted(by_decision.items()))
    lines.extend(["", "# by action"])
    lines.extend(f"{key} | {value}" for key, value in sorted(by_action.items()))
    lines.extend(["", "# by reason"])
    lines.extend(f"{key} | {value}" for key, value in sorted(by_reason.items()))
    summary_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return jsonl_path, summary_path


def parse_int_setting(value, default: int, minimum: Optional[int] = None) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception:
        parsed = int(default)
    if minimum is not None and parsed < minimum:
        parsed = minimum
    return parsed


def parse_float_setting(value, default: float, minimum: Optional[float] = None) -> float:
    try:
        parsed = float(str(value).strip())
    except Exception:
        parsed = float(default)
    if minimum is not None and parsed < minimum:
        parsed = minimum
    return parsed


class OperationTracker:
    """Write phase heartbeat state and periodic log output for long operations."""

    def __init__(
        self,
        run_id: str,
        state_path: Path,
        logger: logging.Logger,
        *,
        interval_sec: float = 10.0,
        slow_warning_sec: float = 300.0,
        tool: str = "comparator",
    ) -> None:
        self.run_id = str(run_id or "-")
        self.state_path = Path(state_path)
        self.logger = logger
        self.interval_sec = max(1.0, float(interval_sec or 10.0))
        self.slow_warning_sec = max(1.0, float(slow_warning_sec or 300.0))
        self.tool = tool
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._state: Optional[Dict[str, object]] = None
        self._stack: List[Dict[str, object]] = []
        self._last_emit_at = 0.0
        self._slow_warned: Dict[str, bool] = {}
        self._heartbeat_write_warned = False

    def begin(
        self,
        phase: str,
        *,
        operation_id: str = "",
        object_type: str = "",
        object_identity: str = "",
        current: Optional[int] = None,
        total: Optional[int] = None,
        artifact_path: str = "",
        detail: Optional[Dict[str, object]] = None,
    ) -> str:
        token = f"{phase}:{time.time():.6f}:{len(self._stack)}"
        now = time.time()
        with self._lock:
            if self._state:
                self._stack.append(dict(self._state))
            self._state = {
                "schema_version": 1,
                "token": token,
                "tool": self.tool,
                "run_id": self.run_id,
                "pid": os.getpid(),
                "phase": phase,
                "operation_id": operation_id or phase,
                "object_type": object_type or "",
                "object_identity": object_identity or "",
                "current": current,
                "total": total,
                "artifact_path": artifact_path or "",
                "detail": dict(detail or {}),
                "status": "running",
                "started_at": utcish_now_text(),
                "started_at_epoch": now,
                "last_heartbeat_at": utcish_now_text(),
                "last_heartbeat_epoch": now,
                "elapsed_sec": 0.0,
                "stack_depth": len(self._stack),
            }
            self._last_emit_at = 0.0
            self._ensure_thread_locked()
            self._write_state_locked()
        self.logger.info("[HEARTBEAT] start phase=%s operation=%s", phase, operation_id or phase)
        return token

    def update(
        self,
        *,
        current: Optional[int] = None,
        total: Optional[int] = None,
        object_identity: Optional[str] = None,
        artifact_path: Optional[str] = None,
        detail: Optional[Dict[str, object]] = None,
        force: bool = False,
    ) -> None:
        with self._lock:
            if not self._state:
                return
            if current is not None:
                self._state["current"] = current
            if total is not None:
                self._state["total"] = total
            if object_identity is not None:
                self._state["object_identity"] = object_identity
            if artifact_path is not None:
                self._state["artifact_path"] = artifact_path
            if detail:
                merged = dict(self._state.get("detail") or {})
                merged.update(detail)
                self._state["detail"] = merged
            self._heartbeat_locked(force=force)

    def finish(self, token: Optional[str] = None, *, status: str = "success") -> None:
        phase = "-"
        elapsed = 0.0
        with self._lock:
            if not self._state:
                return
            current_token = str(self._state.get("token") or "")
            if token and current_token and token != current_token:
                self.logger.warning(
                    "[HEARTBEAT] token mismatch: expected=%s current=%s phase=%s",
                    token,
                    current_token,
                    self._state.get("phase") or "-",
                )
            self._heartbeat_locked(force=True, status=status)
            phase = str(self._state.get("phase") or "-")
            elapsed = float(self._state.get("elapsed_sec") or 0.0)
            previous = self._stack.pop() if self._stack else None
            self._state = previous
            if self._state:
                self._state["last_heartbeat_at"] = utcish_now_text()
                self._state["last_heartbeat_epoch"] = time.time()
                self._state["stack_depth"] = len(self._stack)
                self._write_state_locked()
        self.logger.info(
            "[HEARTBEAT] finish phase=%s status=%s elapsed=%.2fs", phase, status, elapsed
        )

    @contextmanager
    def track(self, phase: str, **kwargs) -> Iterator[None]:
        token = self.begin(phase, **kwargs)
        status = "success"
        try:
            yield
        except Exception:
            status = "failed"
            raise
        finally:
            self.finish(token, status=status)

    def close(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=1.0)
        with self._lock:
            if self._state:
                self._heartbeat_locked(force=True, status="closed")
            self._state = None
            self._stack = []

    def _ensure_thread_locked(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self.tool}-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def _heartbeat_loop(self) -> None:
        sleep_sec = min(max(self.interval_sec / 2.0, 1.0), 5.0)
        while not self._stop.wait(sleep_sec):
            with self._lock:
                if self._state:
                    self._heartbeat_locked(force=False)

    def _heartbeat_locked(self, *, force: bool, status: Optional[str] = None) -> None:
        if not self._state:
            return
        now = time.time()
        started = float(self._state.get("started_at_epoch") or now)
        elapsed = max(0.0, now - started)
        self._state["last_heartbeat_at"] = utcish_now_text()
        self._state["last_heartbeat_epoch"] = now
        self._state["elapsed_sec"] = round(elapsed, 3)
        if status:
            self._state["status"] = status
        self._write_state_locked()
        should_log = force or (now - self._last_emit_at) >= self.interval_sec
        phase = str(self._state.get("phase") or "-")
        if should_log:
            self._last_emit_at = now
            current = self._state.get("current")
            total = self._state.get("total")
            progress = ""
            if current is not None and total is not None:
                progress = f" progress={current}/{total}"
            self.logger.info(
                "[HEARTBEAT] phase=%s operation=%s elapsed=%.2fs%s artifact=%s",
                phase,
                self._state.get("operation_id") or "-",
                elapsed,
                progress,
                self._state.get("artifact_path") or "-",
            )
        if elapsed >= self.slow_warning_sec and not self._slow_warned.get(phase):
            self._slow_warned[phase] = True
            self.logger.warning(
                "[HEARTBEAT] slow phase=%s elapsed=%.2fs threshold=%.2fs operation=%s",
                phase,
                elapsed,
                self.slow_warning_sec,
                self._state.get("operation_id") or "-",
            )

    def _write_state_locked(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            data = json.dumps(self._state or {}, ensure_ascii=False, indent=2, sort_keys=True)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                prefix=f".{self.state_path.name}.",
                suffix=".tmp",
                dir=str(self.state_path.parent),
                delete=False,
            ) as tmp:
                tmp.write(data)
                tmp.write("\n")
                tmp_path = Path(tmp.name)
            tmp_path.replace(self.state_path)
        except Exception as exc:
            if not self._heartbeat_write_warned:
                self._heartbeat_write_warned = True
                self.logger.warning("heartbeat state write failed: %s", exc)
            else:
                self.logger.debug("heartbeat state write failed: %s", exc)


def build_timeout_rows(
    *,
    cli_timeout: Optional[int] = None,
    obclient_timeout: Optional[int] = None,
    fixup_cli_timeout: Optional[int] = None,
    session_query_timeout_us: Optional[int] = None,
    table_presence_timeout: Optional[int] = None,
    progress_log_interval: Optional[float] = None,
    slow_phase_warning_sec: Optional[float] = None,
    slow_sql_warning_sec: Optional[float] = None,
    execution_mode: str = "",
    selected_safety_tiers: str = "",
) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []

    def add(name: str, value, detail: str) -> None:
        if value is None or value == "":
            return
        rows.append((name, str(value), detail))

    add("cli_timeout", cli_timeout, "dbcat/main external process timeout in seconds")
    add("obclient_timeout", obclient_timeout, "main-program obclient process timeout in seconds")
    add("fixup_cli_timeout", fixup_cli_timeout, "run_fixup obclient process timeout in seconds")
    add(
        "ob_session_query_timeout_us",
        session_query_timeout_us,
        "OceanBase session ob_query_timeout in microseconds",
    )
    add(
        "table_data_presence_obclient_timeout",
        table_presence_timeout,
        "table data presence obclient timeout in seconds",
    )
    add("progress_log_interval", progress_log_interval, "heartbeat/progress log interval seconds")
    add("slow_phase_warning_sec", slow_phase_warning_sec, "main-program slow phase warning seconds")
    add("slow_sql_warning_sec", slow_sql_warning_sec, "run_fixup slow SQL warning seconds")
    add("fixup_exec_mode", execution_mode, "run_fixup execution granularity")
    add("selected_safety_tiers", selected_safety_tiers, "run_fixup selected safety tiers")
    return rows


def build_timeout_warnings(
    *,
    process_timeout_sec: Optional[int],
    session_query_timeout_us: Optional[int],
    slow_warning_sec: Optional[float],
    process_name: str,
) -> List[str]:
    warnings: List[str] = []
    if process_timeout_sec and session_query_timeout_us:
        try:
            if int(session_query_timeout_us) > int(process_timeout_sec) * 1000000:
                warnings.append(
                    f"{process_name}: process timeout ({process_timeout_sec}s) will fire before "
                    f"OB session ob_query_timeout ({session_query_timeout_us}us)."
                )
        except Exception:
            pass
    if process_timeout_sec and slow_warning_sec:
        try:
            if int(process_timeout_sec) > float(slow_warning_sec):
                warnings.append(
                    f"{process_name}: a single external call may run up to {process_timeout_sec}s; "
                    f"slow warning threshold is {slow_warning_sec}s."
                )
        except Exception:
            pass
    return warnings


def format_timeout_summary(rows: List[Tuple[str, str, str]], warnings: List[str]) -> str:
    lines = ["# effective timeout summary", "NAME | VALUE | DETAIL"]
    for name, value, detail in rows:
        lines.append(f"{name} | {value} | {detail}")
    if warnings:
        lines.extend(["", "# warnings"])
        lines.extend(f"- {item}" for item in warnings)
    return "\n".join(lines).rstrip() + "\n"


def write_timeout_summary(
    path: Path, rows: List[Tuple[str, str, str]], warnings: List[str]
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_timeout_summary(rows, warnings), encoding="utf-8")
    return path


def log_timeout_summary(
    logger: logging.Logger, rows: List[Tuple[str, str, str]], warnings: List[str]
) -> None:
    logger.info("[TIMEOUT] effective timeout summary")
    for name, value, detail in rows:
        logger.info("[TIMEOUT] %s=%s (%s)", name, value, detail)
    for item in warnings:
        logger.warning("[TIMEOUT] %s", item)


def load_release_evidence(path: Path) -> Dict[str, object]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("release evidence must be a JSON object")
    return data


def validate_release_evidence(data: Dict[str, object]) -> List[str]:
    errors: List[str] = []
    required = [
        "version",
        "branch",
        "commit",
        "tag_candidate",
        "commands",
        "git_diff_hygiene",
        "tracked_test_file_hygiene",
        "real_db_smoke",
        "publishable",
    ]
    for key in required:
        if key not in data:
            errors.append(f"missing required field: {key}")
    if data.get("publishable") is not True:
        errors.append("publishable must be true")
    commands = data.get("commands")
    if not isinstance(commands, list) or not commands:
        errors.append("commands must be a non-empty list")
    smoke = data.get("real_db_smoke")
    if not isinstance(smoke, dict):
        errors.append("real_db_smoke must be an object")
    else:
        if smoke.get("source_mode") != "oracle":
            errors.append("real_db_smoke.source_mode must be oracle for the required gate")
        if smoke.get("target_mode") not in ("oceanbase", "ob"):
            errors.append("real_db_smoke.target_mode must be oceanbase")
        if smoke.get("status") != "passed":
            errors.append("real_db_smoke.status must be passed")
        for key in ("command", "duration_sec", "report_path"):
            if key not in smoke or smoke.get(key) in ("", None):
                errors.append(f"real_db_smoke.{key} is required")
    return errors


def resolve_compatibility_registry_path(settings: Dict[str, object], base_dir: Path) -> Path:
    raw_path = str((settings or {}).get("compatibility_registry_path") or "").strip()
    if raw_path:
        path = Path(raw_path).expanduser()
    else:
        path = Path(__file__).resolve().parent / DEFAULT_COMPATIBILITY_REGISTRY
    if not path.is_absolute():
        path = Path(base_dir) / path
    return path.resolve()


def load_compatibility_registry(path: Path) -> Dict[str, object]:
    path = Path(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("compatibility registry must be a JSON object")
    version = data.get("version")
    entries = data.get("entries")
    if not isinstance(version, str) or not version.strip():
        raise ValueError("compatibility registry missing string field: version")
    if not isinstance(entries, list):
        raise ValueError("compatibility registry missing array field: entries")
    required = {"source_mode", "object_family", "operation", "decision", "rationale"}
    for idx, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"compatibility registry entry {idx} must be an object")
        missing = sorted(key for key in required if not str(entry.get(key) or "").strip())
        if missing:
            raise ValueError(
                f"compatibility registry entry {idx} missing required fields: {', '.join(missing)}"
            )
        decision = str(entry.get("decision") or "").strip().lower()
        if decision not in {
            COMPAT_DECISION_SUPPORTED,
            COMPAT_DECISION_DEGRADED,
            COMPAT_DECISION_MANUAL,
            COMPAT_DECISION_UNSUPPORTED,
        }:
            raise ValueError(
                f"compatibility registry entry {idx} has unsupported decision: {decision}"
            )
    data["_path"] = str(path)
    data["_sha1"] = file_sha1(path)
    return data


def _version_tokens(value: str) -> Tuple[int, ...]:
    tokens = []
    for part in re.findall(r"\d+", value or ""):
        try:
            tokens.append(int(part))
        except Exception:
            pass
    return tuple(tokens)


def _version_matches(version: str, min_version: str, max_version: str) -> bool:
    current = _version_tokens(version)
    if not current:
        return True
    if min_version and current < _version_tokens(min_version):
        return False
    if max_version and current > _version_tokens(max_version):
        return False
    return True


def select_compatibility_entries(
    registry: Dict[str, object],
    *,
    source_mode: str,
    ob_version: str,
    object_families: Optional[Iterable[str]] = None,
    operations: Optional[Iterable[str]] = None,
) -> List[Dict[str, object]]:
    source_mode_u = (source_mode or "").strip().lower()
    family_set = {
        str(item or "").strip().upper().replace(" ", "_")
        for item in (object_families or [])
        if str(item or "").strip()
    }
    operation_set = {
        str(item or "").strip().lower() for item in (operations or []) if str(item or "").strip()
    }
    selected: List[Dict[str, object]] = []
    for entry in registry.get("entries") or []:
        if not isinstance(entry, dict):
            continue
        entry_mode = str(entry.get("source_mode") or "").strip().lower()
        if entry_mode not in {"*", source_mode_u}:
            continue
        family = str(entry.get("object_family") or "").strip().upper().replace(" ", "_")
        if family_set and family not in family_set and family != "*":
            continue
        operation = str(entry.get("operation") or "").strip().lower()
        if operation_set and operation not in operation_set and operation != "*":
            continue
        if not _version_matches(
            ob_version,
            str(entry.get("min_ob_version") or ""),
            str(entry.get("max_ob_version") or ""),
        ):
            continue
        normalized = dict(entry)
        normalized["source_mode"] = entry_mode
        normalized["object_family"] = family
        normalized["operation"] = operation
        normalized["decision"] = str(entry.get("decision") or "").strip().lower()
        selected.append(normalized)
    return selected


def export_compatibility_matrix(
    registry: Dict[str, object],
    report_dir: Path,
    timestamp: str,
    *,
    source_mode: str,
    ob_version: str,
    object_families: Optional[Iterable[str]] = None,
) -> Tuple[Path, Path, List[Dict[str, object]]]:
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    entries = select_compatibility_entries(
        registry,
        source_mode=source_mode,
        ob_version=ob_version,
        object_families=object_families,
    )
    payload = {
        "schema_version": 1,
        "registry_version": registry.get("version"),
        "registry_path": registry.get("_path"),
        "registry_sha1": registry.get("_sha1"),
        "source_mode": source_mode,
        "target_ob_version": ob_version,
        "generated_at": utcish_now_text(),
        "entries": entries,
    }
    matrix_path = report_dir / f"compatibility_matrix_{timestamp}.json"
    matrix_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    counts: Dict[str, int] = {}
    for entry in entries:
        decision = str(entry.get("decision") or "unknown")
        counts[decision] = counts.get(decision, 0) + 1
    lines = [
        "# compatibility matrix summary",
        f"registry_version: {registry.get('version')}",
        f"registry_sha1: {registry.get('_sha1')}",
        f"source_mode: {source_mode}",
        f"target_ob_version: {ob_version or 'unknown'}",
        f"matrix: {matrix_path.name}",
        "",
        "DECISION | COUNT",
    ]
    lines.extend(f"{key} | {value}" for key, value in sorted(counts.items()))
    lines.extend(["", "OBJECT_FAMILY | OPERATION | DECISION | RATIONALE | MANUAL_ACTION_HINT"])
    for entry in entries:
        lines.append(
            "{family} | {operation} | {decision} | {rationale} | {hint}".format(
                family=entry.get("object_family") or "-",
                operation=entry.get("operation") or "-",
                decision=entry.get("decision") or "-",
                rationale=entry.get("rationale") or "-",
                hint=entry.get("manual_action_hint") or "-",
            )
        )
    summary_path = report_dir / f"compatibility_summary_{timestamp}.txt"
    summary_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return matrix_path, summary_path, entries


class RecoveryManager:
    """Minimal checkpoint writer for phase and replay evidence."""

    def __init__(
        self,
        *,
        run_id: str,
        report_dir: Path,
        tool_version: str,
        settings: Dict[str, object],
        input_paths: Sequence[Path],
        code_version: str = "",
    ) -> None:
        self.run_id = str(run_id or "")
        self.report_dir = Path(report_dir)
        self.tool_version = str(tool_version or "")
        self.code_version = str(code_version or "")
        self.settings = dict(settings or {})
        self.decision_config_hash = compute_decision_config_hash(self.settings)
        self.runtime_config_hash = compute_runtime_config_hash(self.settings)
        self.input_artifact_hash = self._compute_input_hash(input_paths)
        self.manifest_path = self.report_dir / f"recovery_manifest_{self.run_id}.json"
        self.phase_checkpoints: List[Dict[str, object]] = []
        self.object_checkpoints: List[Dict[str, object]] = []
        self._dirty = False
        self._last_manifest_write_at = 0.0
        self._manifest_write_interval_sec = 2.0
        self.resume_policy: Dict[str, object] = {
            "decision_config_hash_required": True,
            "runtime_config_hash_may_change": True,
            "force_resume_requires_reason": True,
        }

    def _compute_input_hash(self, input_paths: Sequence[Path]) -> str:
        items: List[Dict[str, object]] = []
        for path in input_paths or []:
            p = Path(path)
            record: Dict[str, object] = {"path": str(p)}
            if p.exists() and p.is_file():
                try:
                    record["sha1"] = file_sha1(p)
                    record["size"] = p.stat().st_size
                except Exception as exc:
                    record["error"] = str(exc)
            elif p.exists() and p.is_dir():
                record["kind"] = "dir"
                try:
                    child_hashes = []
                    for child in sorted(p.rglob("*")):
                        if child.is_file():
                            child_hashes.append(
                                {
                                    "path": str(child.relative_to(p)),
                                    "sha1": file_sha1(child),
                                    "size": child.stat().st_size,
                                }
                            )
                    record["children"] = child_hashes
                except Exception as exc:
                    record["error"] = str(exc)
            else:
                record["missing"] = True
            items.append(record)
        return stable_json_hash(items)

    def record_phase(
        self,
        phase: str,
        *,
        state: str,
        output_paths: Optional[Iterable[Path]] = None,
        object_cursor: str = "",
        detail: str = "",
    ) -> None:
        output_list = [str(Path(p)) for p in (output_paths or []) if p]
        entry = {
            "phase": str(phase or ""),
            "state": str(state or ""),
            "timestamp": utcish_now_text(),
            "object_cursor": str(object_cursor or ""),
            "output_paths": output_list,
            "detail": str(detail or ""),
        }
        self.phase_checkpoints.append(entry)
        self.write_manifest(force=True)

    def record_object(
        self,
        *,
        phase: str,
        object_type: str,
        object_identity: str,
        state: str,
        artifacts: Optional[Iterable[Path]] = None,
        source_evidence_hash: str = "",
        target_mapping_hash: str = "",
        detail: str = "",
    ) -> None:
        entry = {
            "phase": str(phase or ""),
            "object_type": str(object_type or "").upper(),
            "object_identity": str(object_identity or ""),
            "state": str(state or ""),
            "timestamp": utcish_now_text(),
            "artifacts": [str(Path(p)) for p in (artifacts or []) if p],
            "source_evidence_hash": source_evidence_hash,
            "target_mapping_hash": target_mapping_hash,
            "detail": detail,
        }
        self.object_checkpoints.append(entry)
        self.write_manifest(force=False)

    def _manifest_payload(self) -> Dict[str, object]:
        return {
            "schema_version": 1,
            "run_id": self.run_id,
            "tool_version": self.tool_version,
            "code_version": self.code_version,
            "decision_config_hash": self.decision_config_hash,
            "runtime_config_hash": self.runtime_config_hash,
            "input_artifact_hash": self.input_artifact_hash,
            "updated_at": utcish_now_text(),
            "resume_policy": self.resume_policy,
            "phase_checkpoints": self.phase_checkpoints,
            "object_checkpoints": self.object_checkpoints,
        }

    def write_manifest(self, *, force: bool = True) -> Path:
        self._dirty = True
        now = time.time()
        if (
            not force
            and self.manifest_path.exists()
            and (now - self._last_manifest_write_at) < self._manifest_write_interval_sec
        ):
            return self.manifest_path

        self.report_dir.mkdir(parents=True, exist_ok=True)
        payload = self._manifest_payload()
        data = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        tmp_path: Optional[Path] = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                prefix=f".{self.manifest_path.name}.",
                suffix=".tmp",
                dir=str(self.report_dir),
                delete=False,
            ) as tmp:
                tmp.write(data)
                tmp.flush()
                try:
                    os.fsync(tmp.fileno())
                except Exception:
                    pass
                tmp_path = Path(tmp.name)
            os.replace(tmp_path, self.manifest_path)
        finally:
            if tmp_path is not None and tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
        self._dirty = False
        self._last_manifest_write_at = now
        return self.manifest_path

    def flush(self) -> Path:
        if self._dirty or not self.manifest_path.exists():
            return self.write_manifest(force=True)
        return self.manifest_path


def load_recovery_manifest(path: Path) -> Dict[str, object]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("recovery manifest must be a JSON object")
    return data


def validate_recovery_resume(
    manifest: Dict[str, object],
    *,
    settings: Dict[str, object],
    input_artifact_hash: str,
    tool_version: str,
    force_resume: bool = False,
    override_reason: str = "",
) -> Tuple[bool, List[str], Dict[str, object]]:
    current_decision_hash = compute_decision_config_hash(settings or {})
    current_runtime_hash = compute_runtime_config_hash(settings or {})
    try:
        schema_version = int(manifest.get("schema_version") or 0)
    except Exception:
        schema_version = 0
    schema_match = schema_version == 1
    decision_match = current_decision_hash == str(manifest.get("decision_config_hash") or "")
    runtime_match = current_runtime_hash == str(manifest.get("runtime_config_hash") or "")
    input_match = input_artifact_hash == str(manifest.get("input_artifact_hash") or "")
    version_match = str(tool_version or "") == str(manifest.get("tool_version") or "")
    errors: List[str] = []
    if not schema_match:
        errors.append("schema_version mismatch")
    if not decision_match:
        errors.append("decision_config_hash mismatch")
    if not input_match:
        errors.append("input_artifact_hash mismatch")
    if not version_match:
        errors.append("tool_version mismatch")
    allowed = not errors
    if errors and force_resume:
        if str(override_reason or "").strip():
            allowed = True
        else:
            errors.append("force resume requires non-empty override reason")
    details = {
        "schema_version": schema_version,
        "schema_match": schema_match,
        "decision_match": decision_match,
        "runtime_match": runtime_match,
        "input_match": input_match,
        "version_match": version_match,
        "runtime_config_changed": not runtime_match,
        "force_resume": bool(force_resume),
        "override_reason": str(override_reason or ""),
        "errors": list(errors),
    }
    return allowed, errors, details


def redact_sensitive_text(text: str) -> str:
    redacted = URL_CREDENTIAL_RE.sub(r"//<REDACTED>:<REDACTED>@", str(text or ""))
    redacted = ORACLE_CONNECT_CREDENTIAL_RE.sub(r"<REDACTED>/<REDACTED>@", redacted)
    redacted = LONG_PASSWORD_EQUALS_RE.sub(r"\1<REDACTED>", redacted)
    redacted = LONG_PASSWORD_SPACE_RE.sub(r"\1<REDACTED>", redacted)
    redacted = SHORT_PASSWORD_RE.sub("-p <REDACTED>", redacted)
    return redacted


def sanitized_config_text(path: Path) -> str:
    parser = configparser.ConfigParser()
    parser.optionxform = str
    parser.read(path, encoding="utf-8")
    secret_re = re.compile(
        r"(password|passwd|pwd|token|secret|private[_-]?key|credential|wallet)", re.I
    )
    for section in parser.sections():
        for key, value in list(parser.items(section)):
            if secret_re.search(key):
                parser.set(section, key, "<REDACTED>")
            else:
                parser.set(section, key, redact_sensitive_text(value))
    import io

    buf = io.StringIO()
    parser.write(buf)
    return buf.getvalue()


def release_gate_main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Validate comparator release evidence.")
    parser.add_argument("evidence", help="Path to release evidence JSON")
    args = parser.parse_args(argv)
    path = Path(args.evidence)
    try:
        evidence = load_release_evidence(path)
        errors = validate_release_evidence(evidence)
    except Exception as exc:
        print(f"release evidence invalid: {exc}", file=sys.stderr)
        return 1
    if errors:
        print("release evidence is not publishable:", file=sys.stderr)
        for item in errors:
            print(f"- {item}", file=sys.stderr)
        return 1
    print(f"release evidence OK: {path}")
    return 0


def _first_sql_keyword(sql_text: str) -> str:
    for raw_line in (sql_text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--") or line.startswith("#"):
            continue
        return line.split(None, 1)[0].upper().strip(";")
    return ""


def _count_sql_statements(sql_text: str) -> int:
    count = 0
    for part in (sql_text or "").split(";"):
        meaningful_lines = [
            line.strip()
            for line in part.splitlines()
            if line.strip()
            and not line.strip().startswith("--")
            and not line.strip().startswith("#")
        ]
        if meaningful_lines:
            count += 1
    return count


def _contains_destructive_sql(sql_text: str) -> bool:
    text = sql_text or ""
    return bool(DESTRUCTIVE_SQL_RE.search(text) or DROP_FORCE_RE.search(text))


def _is_safe_compile_sql(sql_text: str) -> bool:
    upper = " ".join((sql_text or "").upper().split())
    if " COMPILE" not in upper or "ALTER " not in upper:
        return False
    return any(f"ALTER {obj_type} " in upper for obj_type in SAFE_COMPILE_TYPES)


def _extract_sql_comment_metadata(sql_text: str) -> Dict[str, str]:
    metadata: Dict[str, str] = {}
    known_keys = {
        "reason_code",
        "rule_id",
        "source_evidence",
        "target_evidence",
        "decision",
        "action",
        "compatibility_decision",
    }
    for raw_line in (sql_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if not line.startswith("--"):
            break
        body = line[2:].strip()
        if not body:
            continue
        if ":" in body:
            key, value = body.split(":", 1)
        elif "=" in body:
            key, value = body.split("=", 1)
        else:
            continue
        key_norm = key.strip().lower().replace("-", "_").replace(" ", "_")
        if key_norm in known_keys and key_norm not in metadata:
            metadata[key_norm] = value.strip()
    return metadata


def classify_fixup_safety(relative_path: Path, sql_text: str) -> Tuple[str, str]:
    parts = relative_path.parts
    top_dir = parts[0].lower() if parts else ""
    if top_dir in MANUAL_FIXUP_DIRS:
        return SAFETY_TIER_MANUAL, f"manual-only directory: {top_dir}"
    if top_dir in DESTRUCTIVE_FIXUP_DIRS:
        return SAFETY_TIER_DESTRUCTIVE, f"destructive directory: {top_dir}"
    if _contains_destructive_sql(sql_text):
        return SAFETY_TIER_DESTRUCTIVE, "destructive SQL keyword detected"
    if top_dir == "compile" and _is_safe_compile_sql(sql_text):
        return SAFETY_TIER_SAFE, "whitelisted existing-object compile operation"
    if top_dir in {"sequence_restart", "grants_deferred"}:
        return SAFETY_TIER_MANUAL, f"requires explicit operator timing: {top_dir}"
    return SAFETY_TIER_REVIEW, "review required by default"


def infer_fixup_object_identity(relative_path: Path) -> str:
    stem = relative_path.name
    if stem.endswith(".sql"):
        stem = stem[:-4]
    suffixes = [
        ".alter_columns",
        ".compile",
        ".constraints",
        ".indexes",
        ".grants",
        ".privs",
    ]
    for suffix in suffixes:
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    return stem


def build_fixup_plan_record(fixup_dir: Path, sql_path: Path) -> Optional[Dict[str, object]]:
    try:
        relative_path = sql_path.relative_to(fixup_dir)
    except ValueError:
        relative_path = sql_path
    try:
        sql_text = sql_path.read_text(encoding="utf-8", errors="replace")
        sql_bytes = sql_path.read_bytes()
    except Exception:
        return None
    top_dir = relative_path.parts[0].lower() if relative_path.parts else ""
    tier, reason = classify_fixup_safety(relative_path, sql_text)
    metadata = _extract_sql_comment_metadata(sql_text)
    statement_count = _count_sql_statements(sql_text)
    reason_code = metadata.get("reason_code", "")
    decision = metadata.get(
        "decision",
        DECISION_FIXUP if tier in {SAFETY_TIER_SAFE, SAFETY_TIER_REVIEW} else DECISION_REVIEW,
    )
    action = metadata.get(
        "action",
        ACTION_GENERATE_FIXUP
        if tier in {SAFETY_TIER_SAFE, SAFETY_TIER_REVIEW}
        else ACTION_MANUAL_REVIEW,
    )
    return {
        "schema_version": 1,
        "file_path": str(relative_path),
        "statement_identity": f"{relative_path}#statement-1"
        if statement_count <= 1
        else f"{relative_path}#statements-1..{statement_count}",
        "object_identity": infer_fixup_object_identity(relative_path),
        "object_type": DIR_OBJECT_TYPE_MAP.get(top_dir, top_dir.upper() or "-"),
        "operation": _first_sql_keyword(sql_text) or "-",
        "statement_count": statement_count,
        "safety_tier": tier,
        "safety_reason": reason,
        "execution_default": tier in {SAFETY_TIER_SAFE, SAFETY_TIER_REVIEW},
        "file_size_bytes": len(sql_bytes),
        "file_sha1": hashlib.sha1(sql_bytes).hexdigest(),
        "reason_code": reason_code,
        "reason_record": {
            "reason_code": reason_code,
            "rule_id": metadata.get("rule_id", ""),
            "object_type": DIR_OBJECT_TYPE_MAP.get(top_dir, top_dir.upper() or "-"),
            "object_identity": infer_fixup_object_identity(relative_path),
            "source_evidence": metadata.get("source_evidence", ""),
            "target_evidence": metadata.get("target_evidence", ""),
            "decision": decision,
            "action": action,
            "safety_tier": tier,
            "artifact_path": str(relative_path),
        },
        "compatibility_decision": metadata.get("compatibility_decision", ""),
        "dependencies": [],
    }


def export_fixup_plan(
    fixup_dir: Path, report_dir: Path, timestamp: str
) -> Tuple[Path, Path, Dict[str, int]]:
    records: List[Dict[str, object]] = []
    for sql_path in sorted(Path(fixup_dir).rglob("*.sql")):
        if "/done/" in str(sql_path).replace("\\", "/"):
            continue
        record = build_fixup_plan_record(Path(fixup_dir), sql_path)
        if record:
            records.append(record)
    counts: Dict[str, int] = {
        SAFETY_TIER_SAFE: 0,
        SAFETY_TIER_REVIEW: 0,
        SAFETY_TIER_DESTRUCTIVE: 0,
        SAFETY_TIER_MANUAL: 0,
    }
    for record in records:
        tier = str(record.get("safety_tier") or SAFETY_TIER_REVIEW)
        counts[tier] = counts.get(tier, 0) + 1
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    plan_path = report_dir / f"fixup_plan_{timestamp}.jsonl"
    with plan_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
    summary_path = report_dir / f"fixup_safety_summary_{timestamp}.txt"
    lines = [
        "# fixup safety tier summary",
        "TIER | COUNT | DEFAULT_EXECUTABLE",
        f"safe | {counts.get(SAFETY_TIER_SAFE, 0)} | true",
        f"review | {counts.get(SAFETY_TIER_REVIEW, 0)} | true",
        f"destructive | {counts.get(SAFETY_TIER_DESTRUCTIVE, 0)} | false",
        f"manual | {counts.get(SAFETY_TIER_MANUAL, 0)} | false",
        "",
        f"plan: {plan_path.name}",
    ]
    summary_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return plan_path, summary_path, counts
