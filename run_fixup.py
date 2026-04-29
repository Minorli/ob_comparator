#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2025 Minorli
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Enhanced fixup script executor with dependency-aware ordering and recompilation.

Features:
  - Topological sorting based on object dependencies
  - Grant execution before dependent objects
  - Multi-pass recompilation for INVALID objects
  - Maintains backward compatibility with original run_fixup.py

Usage:
    python3 run_fixup.py [config.ini] [options]

    --smart-order     : Enable dependency-aware execution (recommended)
    --recompile       : Enable automatic recompilation of INVALID objects
    --max-retries N   : Maximum recompilation retries (default: 5)
    --allow-table-create : Allow executing fixup_scripts/table/* (default: disabled)
    --only-dirs       : Filter by subdirectories
    --only-types      : Filter by object types
    --glob            : Filter by filename patterns
"""

from __future__ import annotations

import argparse
import atexit
import configparser
import fnmatch
import hashlib
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from collections import OrderedDict, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Set, Tuple, Union

from comparator_reliability import (
    SAFETY_TIER_DESTRUCTIVE,
    SAFETY_TIER_MANUAL,
    SAFETY_TIER_REVIEW,
    SAFETY_TIER_SAFE,
    OperationTracker,
    build_fixup_plan_record,
    build_timeout_rows,
    build_timeout_warnings,
    classify_fixup_safety,
    log_timeout_summary,
    parse_float_setting,
    parse_int_setting,
    write_timeout_summary,
)

try:
    import fcntl
except Exception:  # pragma: no cover - non-POSIX fallback
    fcntl = None

__version__ = "0.9.9.6-hotfix1"

CONFIG_DEFAULT_PATH = "config.ini"
DEFAULT_FIXUP_DIR = "fixup_scripts"
DONE_DIR_NAME = "done"
DEFAULT_OBCLIENT_TIMEOUT = 60
DEFAULT_FIXUP_TIMEOUT = 3600
DEFAULT_OBCLIENT_SESSION_QUERY_TIMEOUT_US = 3600000000
DEFAULT_ERROR_REPORT_LIMIT = 200
DEFAULT_FIXUP_MAX_SQL_FILE_MB = 50
DEFAULT_FIXUP_AUTO_GRANT_CACHE_LIMIT = 10000
DEFAULT_CONFIG_HOT_RELOAD_INTERVAL_SEC = 5
DEFAULT_FIXUP_EXEC_MODE = "auto"
DEFAULT_FIXUP_EXEC_FILE_FALLBACK = True
DEFAULT_FIXUP_SAFETY_TIERS = (SAFETY_TIER_SAFE, SAFETY_TIER_REVIEW)
FIXUP_SAFETY_TIER_VALUES = {
    SAFETY_TIER_SAFE,
    SAFETY_TIER_REVIEW,
    SAFETY_TIER_DESTRUCTIVE,
    SAFETY_TIER_MANUAL,
}
MAX_RECOMPILE_RETRIES = 5
STATE_LEDGER_FILENAME = os.environ.get(
    "COMPARATOR_FIXUP_STATE_LEDGER_FILENAME", ".fixup_state_ledger.json"
)
FIXUP_RUN_LOCK_FILENAME = ".run_fixup.lock"
FIXUP_HOT_RELOAD_EVENTS_DIR = "errors"
REPO_URL = "https://github.com/Minorli/ob_comparator"
REPO_ISSUES_URL = f"{REPO_URL}/issues"
OBCLIENT_SECURE_OPT = "--defaults-extra-file"
OBCLIENT_SESSION_QUERY_TIMEOUT_US = DEFAULT_OBCLIENT_SESSION_QUERY_TIMEOUT_US
NOTICE_STATE_FILENAME = ".comparator_notice_state.json"
NOTICE_STATE_SCHEMA_VERSION = 1
MANUAL_ACTION_REPORT_KIND = "MANUAL_ACTION_REQUIRED"
MANUAL_ACTION_REPORT_SCHEMA_VERSION = 1
MANUAL_REVIEW_DIRS = ("materialized_view", "job", "schedule")
FIXUP_SUPPORT_TIER_CERTIFIED = "certified"
FIXUP_SUPPORT_TIER_PREVIEW = "preview"
FIXUP_SUPPORT_TIER_MANUAL_ONLY = "manual-only"
FIXUP_SUPPORT_TIER_UNSUPPORTED = "unsupported"
FIXUP_RETRY_POLICY_ITERATIVE = "iterative-retry"
FIXUP_RETRY_POLICY_NO_ITERATIVE = "no-iterative-retry"
FIXUP_FAMILY_EXECUTION_CONTRACTS: Dict[str, Dict[str, object]] = {
    "materialized_view": {
        "family": "MATERIALIZED VIEW",
        "support_tier": FIXUP_SUPPORT_TIER_MANUAL_ONLY,
        "retry_policy": FIXUP_RETRY_POLICY_NO_ITERATIVE,
        "iterative_retry": False,
        "note": "manual-only family，需人工核对后单次执行。",
    },
    "job": {
        "family": "JOB",
        "support_tier": FIXUP_SUPPORT_TIER_MANUAL_ONLY,
        "retry_policy": FIXUP_RETRY_POLICY_NO_ITERATIVE,
        "iterative_retry": False,
        "note": "manual-only family，需人工核对后单次执行。",
    },
    "schedule": {
        "family": "SCHEDULE",
        "support_tier": FIXUP_SUPPORT_TIER_MANUAL_ONLY,
        "retry_policy": FIXUP_RETRY_POLICY_NO_ITERATIVE,
        "iterative_retry": False,
        "note": "manual-only family，需人工核对后单次执行。",
    },
    "sequence_restart": {
        "family": "SEQUENCE RESTART",
        "support_tier": FIXUP_SUPPORT_TIER_MANUAL_ONLY,
        "retry_policy": FIXUP_RETRY_POLICY_NO_ITERATIVE,
        "iterative_retry": False,
        "note": "值同步 SQL，失败后应先核对源/目标 LAST_NUMBER。",
    },
    "cleanup_safe": {
        "family": "CLEANUP SAFE",
        "support_tier": FIXUP_SUPPORT_TIER_MANUAL_ONLY,
        "retry_policy": FIXUP_RETRY_POLICY_NO_ITERATIVE,
        "iterative_retry": False,
        "note": "destructive 清理 SQL，失败后需人工确认。",
    },
    "cleanup_semantic": {
        "family": "CLEANUP SEMANTIC",
        "support_tier": FIXUP_SUPPORT_TIER_MANUAL_ONLY,
        "retry_policy": FIXUP_RETRY_POLICY_NO_ITERATIVE,
        "iterative_retry": False,
        "note": "语义级 destructive 清理 SQL，失败后需人工确认。",
    },
    "tables_unsupported": {
        "family": "TABLE",
        "support_tier": FIXUP_SUPPORT_TIER_UNSUPPORTED,
        "retry_policy": FIXUP_RETRY_POLICY_NO_ITERATIVE,
        "iterative_retry": False,
        "note": "缺少高保真 DDL，仅输出占位脚本。",
    },
    "unsupported": {
        "family": "UNSUPPORTED",
        "support_tier": FIXUP_SUPPORT_TIER_UNSUPPORTED,
        "retry_policy": FIXUP_RETRY_POLICY_NO_ITERATIVE,
        "iterative_retry": False,
        "note": "仅输出 unsupported 草案，不纳入自动重试。",
    },
}


class RuntimeNotice(NamedTuple):
    notice_id: str
    introduced_in: str
    title: str
    message: str


class ManualActionNoticeRow(NamedTuple):
    priority: str
    stage: str
    category: str
    count: int
    default_behavior: str
    primary_artifact: str
    related_fixup_dir: str
    why: str
    recommended_action: str


def normalize_contract_dir_name(path_value: Union[Path, str, None]) -> str:
    if path_value is None:
        return ""
    raw = str(path_value).strip()
    if not raw:
        return ""
    parts = Path(raw).parts
    if not parts:
        return normalize_dir_filter(raw.split("/", 1)[0])
    head = normalize_dir_filter(parts[0])
    known_heads = (
        set(FIXUP_FAMILY_EXECUTION_CONTRACTS.keys())
        | set(DIR_OBJECT_TYPE_MAP.keys())
        | set(GRANT_DIRS)
    )
    if head in known_heads:
        return head
    if head in {DEFAULT_FIXUP_DIR, DONE_DIR_NAME} and len(parts) > 1:
        return normalize_dir_filter(parts[1])
    if len(parts) > 1:
        second = normalize_dir_filter(parts[1])
        if second:
            return second
    return head


def build_default_family_label(dir_name: str) -> str:
    if not dir_name:
        return "-"
    mapped = DIR_OBJECT_TYPE_MAP.get(dir_name)
    if mapped:
        return mapped
    return dir_name.replace("_", " ").upper()


def get_fixup_execution_contract(path_value: Union[Path, str, None]) -> Dict[str, object]:
    dir_name = normalize_contract_dir_name(path_value)
    contract = FIXUP_FAMILY_EXECUTION_CONTRACTS.get(dir_name)
    if contract is not None:
        resolved = dict(contract)
        resolved.setdefault("family", build_default_family_label(dir_name))
        resolved.setdefault("support_tier", FIXUP_SUPPORT_TIER_CERTIFIED)
        resolved.setdefault("retry_policy", FIXUP_RETRY_POLICY_ITERATIVE)
        resolved.setdefault("iterative_retry", True)
        resolved["dir_name"] = dir_name
        return resolved
    return {
        "dir_name": dir_name,
        "family": build_default_family_label(dir_name),
        "support_tier": FIXUP_SUPPORT_TIER_CERTIFIED if dir_name else "-",
        "retry_policy": FIXUP_RETRY_POLICY_ITERATIVE if dir_name else "-",
        "iterative_retry": True,
        "note": "",
    }


def resolve_notice_state_path(config_dir: Optional[Union[str, Path]]) -> Path:
    base_dir = Path(config_dir).expanduser() if config_dir else Path.cwd()
    try:
        base_dir = base_dir.resolve()
    except Exception:
        base_dir = Path.cwd().resolve()
    return base_dir / NOTICE_STATE_FILENAME


def load_notice_state(config_dir: Optional[Union[str, Path]]) -> Tuple[Path, Dict[str, object]]:
    state_path = resolve_notice_state_path(config_dir)
    state: Dict[str, object] = {
        "schema_version": NOTICE_STATE_SCHEMA_VERSION,
        "last_seen_tool_version": "",
        "seen_notices": {},
    }
    if not state_path.exists():
        return state_path, state
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            seen = payload.get("seen_notices")
            if not isinstance(seen, dict):
                seen = {}
            state["seen_notices"] = {
                str(key): str(value) for key, value in seen.items() if str(key).strip()
            }
            last_seen = payload.get("last_seen_tool_version")
            if isinstance(last_seen, str):
                state["last_seen_tool_version"] = last_seen
    except Exception as exc:
        backup_path = state_path.with_name(
            f"{state_path.name}.corrupted.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        try:
            shutil.copy2(state_path, backup_path)
        except Exception:
            backup_path = None
        logging.getLogger(__name__).warning(
            "notice 状态文件损坏，已按空状态继续: %s%s%s",
            state_path,
            f" (备份: {backup_path})" if backup_path else "",
            f" ({exc})" if exc else "",
        )
        return state_path, state
    return state_path, state


def select_unseen_notices(
    state: Dict[str, object], notices: Sequence[RuntimeNotice]
) -> List[RuntimeNotice]:
    seen_notices = state.get("seen_notices")
    if not isinstance(seen_notices, dict):
        seen_notices = {}
    return [notice for notice in notices if notice.notice_id not in seen_notices]


def persist_seen_notices(
    state_path: Path,
    state: Dict[str, object],
    current_version: str,
    notices: Sequence[RuntimeNotice],
) -> None:
    seen_notices = state.get("seen_notices")
    if not isinstance(seen_notices, dict):
        seen_notices = {}
    for notice in notices:
        seen_notices[notice.notice_id] = current_version
    latest_seen: Dict[str, str] = {}
    if state_path.exists():
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and isinstance(payload.get("seen_notices"), dict):
                latest_seen = {
                    str(key): str(value)
                    for key, value in payload.get("seen_notices", {}).items()
                    if str(key).strip()
                }
        except Exception:
            latest_seen = {}
    latest_seen.update(
        {str(key): str(value) for key, value in seen_notices.items() if str(key).strip()}
    )
    payload = {
        "schema_version": NOTICE_STATE_SCHEMA_VERSION,
        "last_seen_tool_version": current_version,
        "seen_notices": latest_seen,
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = state_path.with_name(f"{state_path.name}.lock")
    with lock_path.open("a+", encoding="utf-8") as lock_fp:
        if fcntl is not None:
            fcntl.flock(lock_fp.fileno(), fcntl.LOCK_EX)
        try:
            if state_path.exists():
                try:
                    current_payload = json.loads(state_path.read_text(encoding="utf-8"))
                    if isinstance(current_payload, dict) and isinstance(
                        current_payload.get("seen_notices"), dict
                    ):
                        merged = {
                            str(key): str(value)
                            for key, value in current_payload.get("seen_notices", {}).items()
                            if str(key).strip()
                        }
                        merged.update(payload["seen_notices"])
                        payload["seen_notices"] = merged
                except Exception:
                    pass
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=str(state_path.parent),
                prefix=f"{state_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as tmp_fp:
                tmp_fp.write(
                    json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
                )
                tmp_name = tmp_fp.name
            os.replace(tmp_name, state_path)
        finally:
            if fcntl is not None:
                fcntl.flock(lock_fp.fileno(), fcntl.LOCK_UN)


def find_latest_manual_actions_report(report_dir: Optional[Union[str, Path]]) -> Optional[Path]:
    if not report_dir:
        return None
    base_dir = Path(report_dir)
    if not base_dir.exists():
        return None
    candidates = sorted(base_dir.glob("run_*/manual_actions_required_*.txt"))
    return candidates[-1] if candidates else None


def load_manual_actions_report(path: Optional[Path]) -> List[ManualActionNoticeRow]:
    if not path or not path.exists():
        return []
    rows: List[ManualActionNoticeRow] = []
    try:
        header_seen = False
        report_kind = ""
        schema_version = ""
        expected_header = "PRIORITY|STAGE|CATEGORY|COUNT|DEFAULT_BEHAVIOR|PRIMARY_ARTIFACT|RELATED_FIXUP_DIR|WHY|RECOMMENDED_ACTION"
        for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                if line.startswith("# report_kind="):
                    report_kind = line.split("=", 1)[-1].strip()
                elif line.startswith("# schema_version="):
                    schema_version = line.split("=", 1)[-1].strip()
                elif line.startswith("# 字段说明:"):
                    declared = line.split(":", 1)[-1].strip()
                    if declared and declared != expected_header:
                        log.warning("manual_actions 报告字段说明不匹配，已拒绝加载: %s", path)
                        return []
                continue
            if not header_seen:
                header_seen = True
                if report_kind and report_kind != MANUAL_ACTION_REPORT_KIND:
                    log.warning("manual_actions 报告类型不匹配，已拒绝加载: %s", path)
                    return []
                if schema_version and schema_version != str(MANUAL_ACTION_REPORT_SCHEMA_VERSION):
                    log.warning(
                        "manual_actions 报告 schema_version=%s 不兼容，已拒绝加载: %s",
                        schema_version,
                        path,
                    )
                    return []
                if line == expected_header:
                    continue
                if line.upper().startswith("PRIORITY|STAGE|CATEGORY|COUNT|"):
                    continue
                log.warning("manual_actions 报告缺少标准表头，已拒绝加载: %s", path)
                return []
            parts = [part.strip() for part in line.split("|")]
            if len(parts) < 9:
                log.warning("manual_actions 报告字段数不足，已跳过该行: %s | %s", path, line)
                continue
            try:
                count = int(parts[3] or 0)
            except Exception:
                count = 0
            rows.append(
                ManualActionNoticeRow(
                    priority=parts[0],
                    stage=parts[1],
                    category=parts[2],
                    count=count,
                    default_behavior=parts[4],
                    primary_artifact=parts[5],
                    related_fixup_dir=parts[6],
                    why=parts[7],
                    recommended_action=parts[8],
                )
            )
    except OSError:
        return []
    return rows


def _normalize_manual_action_related_dirs(value: str) -> Set[str]:
    result: Set[str] = set()
    for token in re.split(r"[|,]", str(value or "")):
        text = normalize_dir_filter(token)
        if text:
            result.add(text)
    return result


def normalize_dir_filter(value: str) -> str:
    return str(value or "").strip().replace("\\", "/").strip("/").lower()


def dir_filter_overlaps(path_a: str, path_b: str) -> bool:
    a = normalize_dir_filter(path_a)
    b = normalize_dir_filter(path_b)
    if not a or not b:
        return False
    return a == b or a.startswith(f"{b}/") or b.startswith(f"{a}/")


def dir_path_matches_filter(path_value: str, filter_value: str) -> bool:
    path_norm = normalize_dir_filter(path_value)
    filter_norm = normalize_dir_filter(filter_value)
    if not path_norm or not filter_norm:
        return False
    return path_norm == filter_norm or path_norm.startswith(f"{filter_norm}/")


def should_scan_top_dir(dir_name: str, include_dirs: Optional[Set[str]]) -> bool:
    include_set = {
        normalize_dir_filter(item) for item in (include_dirs or set()) if normalize_dir_filter(item)
    }
    if not include_set:
        return True
    dir_norm = normalize_dir_filter(dir_name)
    return any(dir_filter_overlaps(dir_norm, item) for item in include_set)


def path_selected_by_filters(path_value: str, include_dirs: Optional[Set[str]]) -> bool:
    include_set = {
        normalize_dir_filter(item) for item in (include_dirs or set()) if normalize_dir_filter(item)
    }
    if not include_set:
        return True
    path_norm = normalize_dir_filter(path_value)
    return any(dir_path_matches_filter(path_norm, item) for item in include_set)


def path_excluded_by_filters(path_value: str, exclude_dirs: Optional[Set[str]]) -> bool:
    exclude_set = {
        normalize_dir_filter(item) for item in (exclude_dirs or set()) if normalize_dir_filter(item)
    }
    if not exclude_set:
        return False
    path_norm = normalize_dir_filter(path_value)
    return any(dir_path_matches_filter(path_norm, item) for item in exclude_set)


def select_relevant_manual_actions(
    rows: Sequence[ManualActionNoticeRow], only_dirs: Sequence[str]
) -> List[ManualActionNoticeRow]:
    selected_dirs = {
        normalize_dir_filter(item) for item in (only_dirs or []) if normalize_dir_filter(item)
    }
    if not selected_dirs:
        return list(rows)

    always_show_categories = {
        "DATA_RISK",
        "UNSUPPORTED_OBJECT",
        "UNSUPPORTED_GRANT",
        "GRANT_CAPABILITY_MANUAL",
        "GRANT_CAPABILITY_PROBE_INCOMPLETE",
        "DDL_SEMANTIC_REWRITE",
        "CASE_SENSITIVE_REVIEW",
        "FIXUP_NOT_GENERATED",
    }
    always_show_priorities = {"BLOCKER"}
    result: List[ManualActionNoticeRow] = []
    for row in rows:
        if (row.priority or "").upper() in always_show_priorities:
            result.append(row)
            continue
        if (row.category or "").upper() in always_show_categories:
            result.append(row)
            continue
        related_dirs = _normalize_manual_action_related_dirs(row.related_fixup_dir)
        if related_dirs and any(
            dir_filter_overlaps(rel, sel) for rel in related_dirs for sel in selected_dirs
        ):
            result.append(row)
    return result


def log_manual_action_preflight(
    path: Optional[Path], rows: Sequence[ManualActionNoticeRow]
) -> None:
    if not path or not rows:
        return
    log_section("执行前人工处理提醒")
    log.warning("统一清单: %s", path)
    for idx, row in enumerate(rows[:6], start=1):
        target = row.primary_artifact or row.related_fixup_dir or "-"
        log.warning(
            "%d. [%s/%s] %s x%d | %s | %s",
            idx,
            row.priority or "-",
            row.stage or "-",
            row.category or "-",
            int(row.count or 0),
            target,
            row.recommended_action or row.why or "-",
        )
    if len(rows) > 6:
        log.warning("其余 %d 类人工项请继续查看统一清单。", len(rows) - 6)


CONFIG_HOT_RELOAD_MODE_VALUES = {"off", "phase", "round"}
CONFIG_HOT_RELOAD_FAIL_POLICY_VALUES = {"keep_last_good", "abort"}
FIXUP_EXEC_MODE_VALUES = {"auto", "file", "statement"}

LOG_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FILE_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
LOG_SECTION_WIDTH = 80

_SECURE_CREDENTIAL_FILES: Set[Path] = set()


def _cleanup_secure_credential_files() -> None:
    for path in list(_SECURE_CREDENTIAL_FILES):
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
        finally:
            _SECURE_CREDENTIAL_FILES.discard(path)


atexit.register(_cleanup_secure_credential_files)


def _escape_obclient_option_value(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace('"', '\\"')


def _create_obclient_defaults_file(password: str) -> Path:
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", prefix="ob_fixup_", suffix=".cnf", delete=False
    ) as tmp:
        tmp.write("[client]\n")
        tmp.write(f'password="{_escape_obclient_option_value(password)}"\n')
        tmp_path = Path(tmp.name)
    try:
        tmp_path.chmod(0o600)
    except Exception:
        pass
    _SECURE_CREDENTIAL_FILES.add(tmp_path)
    return tmp_path


CURRENT_SCHEMA_PATTERN = re.compile(
    r'^\s*ALTER\s+SESSION\s+SET\s+CURRENT_SCHEMA\s*=\s*(?P<schema>"[^"]+"|[A-Z0-9_$#]+)\s*;?\s*$',
    re.IGNORECASE,
)


def _build_console_handler(level: int) -> logging.Handler:
    try:
        from rich.logging import RichHandler

        handler = RichHandler(
            level=level,
            show_time=True,
            omit_repeated_times=False,
            show_level=True,
            show_path=False,
            rich_tracebacks=False,
            log_time_format=LOG_TIME_FORMAT,
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        return handler
    except Exception as exc:
        logging.getLogger(__name__).debug(
            "RichHandler init failed, fallback to StreamHandler: %s", exc
        )
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(LOG_FILE_FORMAT, datefmt=LOG_TIME_FORMAT))
        return handler


def init_console_logging(level: int = logging.INFO) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    for handler in list(root_logger.handlers):
        if isinstance(handler, logging.FileHandler):
            continue
        root_logger.removeHandler(handler)
    root_logger.addHandler(_build_console_handler(level))


def set_console_log_level(level: int) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            continue
        handler.setLevel(level)


def resolve_console_log_level(level_name: Optional[str], *, is_tty: Optional[bool] = None) -> int:
    if is_tty is None:
        try:
            is_tty = sys.stdout.isatty()
        except Exception as exc:
            logging.getLogger(__name__).debug(
                "TTY detection failed, defaulting to non-tty: %s", exc
            )
            is_tty = False
    name = (level_name or "AUTO").strip().upper()
    if name == "AUTO":
        return logging.INFO if is_tty else logging.WARNING
    if hasattr(logging, name):
        return getattr(logging, name)
    return logging.INFO


def log_section(title: str, fill_char: str = "=") -> None:
    clean = f" {title.strip()} "
    if len(clean) >= LOG_SECTION_WIDTH:
        log.info("%s", title.strip())
        return
    log.info("%s", clean.center(LOG_SECTION_WIDTH, fill_char))


def log_subsection(title: str, fill_char: str = "-") -> None:
    clean = f" {title.strip()} "
    if len(clean) >= LOG_SECTION_WIDTH:
        log.info("%s", title.strip())
        return
    log.info("%s", clean.center(LOG_SECTION_WIDTH, fill_char))


def format_progress_label(current: int, total: int, width: Optional[int] = None) -> str:
    if width is None:
        width = len(str(total)) or 1
    return f"[进度 {current:0{width}}/{total}]"


def safe_first_line(text: Optional[str], limit: int = 160, default: str = "") -> str:
    if not text:
        return default
    lines = text.splitlines()
    if not lines:
        return default
    return lines[0][:limit]


init_console_logging()
log = logging.getLogger(__name__)
FIXUP_OPERATION_TRACKER: Optional[OperationTracker] = None


@contextmanager
def track_fixup_operation(phase: str, **kwargs):
    tracker = FIXUP_OPERATION_TRACKER
    token = None
    status = "success"
    if tracker is not None:
        token = tracker.begin(phase, **kwargs)
    try:
        yield
    except Exception:
        status = "failed"
        raise
    finally:
        if tracker is not None:
            tracker.finish(token, status=status)


def setup_fixup_runtime_observability(
    ob_cfg: Dict[str, str],
    fixup_dir: Path,
    report_dir: Path,
    fixup_settings: FixupAutoGrantSettings,
) -> OperationTracker:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    progress_interval = parse_float_setting(
        ob_cfg.get("progress_log_interval", 10), 10.0, minimum=1.0
    )
    slow_sql_warning_sec = parse_float_setting(
        ob_cfg.get("slow_sql_warning_sec", 60), 60.0, minimum=1.0
    )
    state_path = Path(fixup_dir) / f"run_fixup_heartbeat_{timestamp}.json"
    tracker = OperationTracker(
        run_id=timestamp,
        state_path=state_path,
        logger=log,
        interval_sec=progress_interval,
        slow_warning_sec=slow_sql_warning_sec,
        tool="run_fixup",
    )
    fixup_timeout = resolve_timeout_value(ob_cfg.get("timeout"))
    session_timeout_us = parse_int_setting(
        ob_cfg.get("session_query_timeout_us", DEFAULT_OBCLIENT_SESSION_QUERY_TIMEOUT_US),
        DEFAULT_OBCLIENT_SESSION_QUERY_TIMEOUT_US,
        minimum=0,
    )
    rows = build_timeout_rows(
        fixup_cli_timeout=fixup_timeout,
        session_query_timeout_us=session_timeout_us,
        progress_log_interval=progress_interval,
        slow_sql_warning_sec=slow_sql_warning_sec,
        execution_mode=fixup_settings.exec_mode,
        selected_safety_tiers=ob_cfg.get("selected_safety_tiers", ""),
    )
    warnings = build_timeout_warnings(
        process_timeout_sec=fixup_timeout or None,
        session_query_timeout_us=session_timeout_us or None,
        slow_warning_sec=slow_sql_warning_sec,
        process_name="run_fixup obclient",
    )
    summary_dir = Path(report_dir) if report_dir else Path(fixup_dir)
    summary_path = summary_dir / f"run_fixup_timeout_summary_{timestamp}.txt"
    write_timeout_summary(summary_path, rows, warnings)
    log_timeout_summary(log, rows, warnings)
    log.info("[HEARTBEAT] state file: %s", tracker.state_path)
    log.info("[TIMEOUT] summary file: %s", summary_path)
    return tracker


# Error classification for intelligent retry
class FailureType:
    """Classification of SQL execution failures for retry logic."""

    MISSING_OBJECT = "missing_object"  # Dependency object doesn't exist -> retryable
    PERMISSION_DENIED = "permission_denied"  # Insufficient privileges -> needs grants
    SYNTAX_ERROR = "syntax_error"  # SQL syntax error -> needs DDL fix
    DATA_CONFLICT = "data_conflict"  # Unique/constraint violation -> needs data cleanup
    CONSTRAINT_VALIDATE_FAIL = "constraint_validate_fail"  # ORA-02298 validation failed
    DUPLICATE_OBJECT = "duplicate_object"  # Object already exists -> can skip
    INVALID_IDENTIFIER = "invalid_identifier"  # Column/table name error -> needs DDL fix
    NAME_IN_USE = "name_in_use"  # Name already used -> needs resolution
    TIMEOUT = "timeout"  # Execution timeout -> may retry
    LOCK_TIMEOUT = "lock_timeout"  # Resource busy/locked
    AUTH_FAILED = "auth_failed"  # Login/auth failure
    CONNECTION_TIMEOUT = "connection_timeout"  # Network timeout
    RESOURCE_EXHAUSTED = "resource_exhausted"  # Out of shared pool/memory
    SNAPSHOT_ERROR = "snapshot_error"  # Snapshot too old
    DEADLOCK = "deadlock"  # Deadlock detected
    UNKNOWN = "unknown"  # Unknown error -> investigate


def classify_sql_error(stderr: str) -> str:
    """
    Classify OceanBase/Oracle error messages for retry logic.

    Args:
        stderr: Error message from obclient

    Returns:
        FailureType classification string
    """
    if not stderr:
        return FailureType.UNKNOWN

    stderr_upper = stderr.upper()

    # Missing object errors (retryable - object may be created in later rounds)
    if any(
        code in stderr_upper
        for code in ["ORA-00942", "ORA-04043", "OB-00942", "OB-04043", "ORA-06512"]
    ):
        if (
            "TABLE OR VIEW DOES NOT EXIST" in stderr_upper
            or "OBJECT DOES NOT EXIST" in stderr_upper
        ):
            return FailureType.MISSING_OBJECT
    if (
        "TABLE OR VIEW DOES NOT EXIST" in stderr_upper
        or "OBJECT DOES NOT EXIST" in stderr_upper
        or "ERROR 1146" in stderr_upper
    ):
        return FailureType.MISSING_OBJECT

    # Permission denied (needs grant scripts)
    if (
        "ORA-01031" in stderr_upper
        or "OB-01031" in stderr_upper
        or "ORA-01720" in stderr_upper
        or "INSUFFICIENT PRIVILEGES" in stderr_upper
        or "ERROR 1142" in stderr_upper
        or "ERROR 1227" in stderr_upper
    ):
        return FailureType.PERMISSION_DENIED

    # Authentication failure
    if (
        "ORA-01017" in stderr_upper
        or "INVALID USERNAME/PASSWORD" in stderr_upper
        or "ERROR 1045" in stderr_upper
    ):
        return FailureType.AUTH_FAILED

    # Connection timeout
    if "ORA-12170" in stderr_upper or "TNS:CONNECT TIMEOUT" in stderr_upper:
        return FailureType.CONNECTION_TIMEOUT

    # Lock timeout / resource busy
    if (
        "ORA-00054" in stderr_upper
        or "RESOURCE BUSY" in stderr_upper
        or "ERROR 1205" in stderr_upper
    ):
        return FailureType.LOCK_TIMEOUT

    # Resource exhausted
    if "ORA-04031" in stderr_upper:
        return FailureType.RESOURCE_EXHAUSTED

    # Snapshot too old
    if "ORA-01555" in stderr_upper:
        return FailureType.SNAPSHOT_ERROR

    # Deadlock
    if "ORA-00060" in stderr_upper or "ERROR 1213" in stderr_upper:
        return FailureType.DEADLOCK

    # Data conflict (unique constraint violation)
    if "ORA-00001" in stderr_upper or "UNIQUE CONSTRAINT" in stderr_upper:
        return FailureType.DATA_CONFLICT

    # Constraint validate failure (target data quality issue)
    if "ORA-02298" in stderr_upper or "CANNOT VALIDATE" in stderr_upper:
        return FailureType.CONSTRAINT_VALIDATE_FAIL

    # Invalid identifier (DDL needs fix)
    if "ORA-00904" in stderr_upper or "ERROR 1054" in stderr_upper:
        return FailureType.INVALID_IDENTIFIER

    # Name already in use (object exists)
    if (
        "ORA-00955" in stderr_upper
        or "OB-00955" in stderr_upper
        or "NAME IS ALREADY USED" in stderr_upper
        or "ALREADY EXISTS" in stderr_upper
        or "ERROR 1050" in stderr_upper
    ):
        return FailureType.NAME_IN_USE

    # Syntax errors (DDL needs fix)
    if any(
        code in stderr_upper
        for code in ["ORA-00900", "ORA-00901", "ORA-00902", "ORA-00903", "ERROR 1064"]
    ):
        return FailureType.SYNTAX_ERROR

    return FailureType.UNKNOWN


def analyze_failure_patterns(results: List["ScriptResult"]) -> Dict[str, List["ScriptResult"]]:
    """
    Analyze failure patterns and group by error type.

    Args:
        results: List of ScriptResult objects

    Returns:
        Dictionary mapping error types to list of failed results
    """
    failures_by_type = defaultdict(list)

    for result in results:
        if result.status == "FAILED":
            error_type = classify_sql_error(result.message)
            failures_by_type[error_type].append(result)

    return dict(failures_by_type)


def log_failure_analysis(failures_by_type: Dict[str, List["ScriptResult"]]) -> None:
    """
    Log detailed failure analysis with actionable suggestions.

    Args:
        failures_by_type: Dictionary of failures grouped by type
    """
    if not failures_by_type:
        return

    log_subsection("失败原因分析")

    total_failures = sum(len(items) for items in failures_by_type.values())
    log.info("总失败数: %d", total_failures)
    log.info("")

    # Missing objects (most common in VIEW scenarios)
    if FailureType.MISSING_OBJECT in failures_by_type:
        items = failures_by_type[FailureType.MISSING_OBJECT]
        log.info("❌ 依赖对象不存在: %d 个 (可在后续轮次重试)", len(items))
        log.info("   建议: 这些脚本会在依赖对象创建后自动重试成功")
        if len(items) <= 5:
            for item in items[:5]:
                log.info("     - %s", item.path.name)

    # Permission denied
    if FailureType.PERMISSION_DENIED in failures_by_type:
        items = failures_by_type[FailureType.PERMISSION_DENIED]
        log.info("❌ 权限不足: %d 个", len(items))
        log.info(
            "   建议: 先执行 fixup_scripts/grants_miss/；"
            "若对象尚未补齐，再查看 fixup_scripts/grants_deferred/（全量审计见 grants_all）"
        )
        if len(items) <= 3:
            for item in items[:3]:
                log.info("     - %s", item.path.name)

    # Syntax errors
    if FailureType.SYNTAX_ERROR in failures_by_type:
        items = failures_by_type[FailureType.SYNTAX_ERROR]
        log.info("❌ SQL语法错误: %d 个", len(items))
        log.info("   建议: 检查DDL兼容性，可能需要手动修复")
        if len(items) <= 3:
            for item in items[:3]:
                log.info("     - %s", item.path.name)

    # Duplicate/existing objects
    if (
        FailureType.DUPLICATE_OBJECT in failures_by_type
        or FailureType.NAME_IN_USE in failures_by_type
    ):
        dup_count = len(failures_by_type.get(FailureType.DUPLICATE_OBJECT, []))
        name_count = len(failures_by_type.get(FailureType.NAME_IN_USE, []))
        total_dup = dup_count + name_count
        log.info("✓ 对象已存在: %d 个 (可忽略)", total_dup)
        log.info("   说明: 这些对象已在目标库存在，无需重复创建")

    # Data conflicts
    if FailureType.DATA_CONFLICT in failures_by_type:
        items = failures_by_type[FailureType.DATA_CONFLICT]
        log.info("❌ 数据冲突/唯一约束违反: %d 个", len(items))
        log.info("   建议: 清理重复数据后重试相关DDL")
        if len(items) <= 3:
            for item in items[:3]:
                log.info("     - %s", item.path.name)

    # Constraint validate failures
    if FailureType.CONSTRAINT_VALIDATE_FAIL in failures_by_type:
        items = failures_by_type[FailureType.CONSTRAINT_VALIDATE_FAIL]
        log.info("❌ 约束校验失败(ORA-02298): %d 个", len(items))
        log.info(
            "   建议: 先清理脏数据，再执行 constraint_validate_later 下的脚本完成 ENABLE VALIDATE"
        )
        if len(items) <= 3:
            for item in items[:3]:
                log.info("     - %s", item.path.name)

    # Lock timeout
    if FailureType.LOCK_TIMEOUT in failures_by_type:
        items = failures_by_type[FailureType.LOCK_TIMEOUT]
        log.info("❌ 资源锁/超时: %d 个", len(items))
        log.info("   建议: 检查锁等待或并发冲突，稍后重试")
        if len(items) <= 3:
            for item in items[:3]:
                log.info("     - %s", item.path.name)

    # Authentication failure
    if FailureType.AUTH_FAILED in failures_by_type:
        items = failures_by_type[FailureType.AUTH_FAILED]
        log.info("❌ 认证失败: %d 个", len(items))
        log.info("   建议: 检查配置中的用户/密码是否正确")
        if len(items) <= 1:
            for item in items[:1]:
                log.info("     - %s", item.path.name)

    # Connection timeout
    if FailureType.CONNECTION_TIMEOUT in failures_by_type:
        items = failures_by_type[FailureType.CONNECTION_TIMEOUT]
        log.info("❌ 连接超时: %d 个", len(items))
        log.info("   建议: 检查网络连通性或数据库负载")
        if len(items) <= 1:
            for item in items[:1]:
                log.info("     - %s", item.path.name)

    # Resource exhausted
    if FailureType.RESOURCE_EXHAUSTED in failures_by_type:
        items = failures_by_type[FailureType.RESOURCE_EXHAUSTED]
        log.info("❌ 资源不足: %d 个", len(items))
        log.info("   建议: 检查数据库内存/共享池资源")
        if len(items) <= 1:
            for item in items[:1]:
                log.info("     - %s", item.path.name)

    # Snapshot too old
    if FailureType.SNAPSHOT_ERROR in failures_by_type:
        items = failures_by_type[FailureType.SNAPSHOT_ERROR]
        log.info("❌ 快照过旧: %d 个", len(items))
        log.info("   建议: 缩短事务或提高 UNDO 保留")
        if len(items) <= 1:
            for item in items[:1]:
                log.info("     - %s", item.path.name)

    # Deadlock
    if FailureType.DEADLOCK in failures_by_type:
        items = failures_by_type[FailureType.DEADLOCK]
        log.info("❌ 死锁: %d 个", len(items))
        log.info("   建议: 降低并发或重试")
        if len(items) <= 1:
            for item in items[:1]:
                log.info("     - %s", item.path.name)

    # Unknown errors
    if FailureType.UNKNOWN in failures_by_type:
        items = failures_by_type[FailureType.UNKNOWN]
        log.info("❓ 未知错误: %d 个", len(items))
        log.info("   建议: 查看详细错误信息进行诊断")
        if len(items) <= 3:
            for item in items[:3]:
                msg_preview = safe_first_line(item.message, 80, "无错误信息")
                log.info("     - %s: %s", item.path.name, msg_preview)

    log.info("")


TYPE_DIR_MAP = {
    "SEQUENCE": "sequence",
    "TABLE": "table",
    "TABLE_ALTER": "table_alter",
    "CONSTRAINT": "constraint",
    "INDEX": "index",
    "VIEW": "view",
    "VIEW_REFRESH": "view_refresh",
    "MATERIALIZED_VIEW": "materialized_view",
    "SYNONYM": "synonym",
    "PROCEDURE": "procedure",
    "FUNCTION": "function",
    "PACKAGE": "package",
    "PACKAGE_BODY": "package_body",
    "CONTEXT": "context",
    "TYPE": "type",
    "TYPE_BODY": "type_body",
    "TRIGGER": "trigger",
    "JOB": "job",
    "SCHEDULE": "schedule",
    "GRANTS": "grants",
}

DIR_OBJECT_TYPE_MAP = {
    dir_name: obj_type.replace("_", " ")
    for obj_type, dir_name in TYPE_DIR_MAP.items()
    if dir_name != "grants"
}
DIR_OBJECT_TYPE_MAP["table_alter"] = "TABLE"

# Execution priority for dependency-aware ordering
DEPENDENCY_LAYERS = [
    ["sequence"],  # Layer 0: No dependencies
    ["sequence_restart"],  # Layer 1: Sequence value sync (default skipped)
    ["table"],  # Layer 2: Base tables
    ["table_alter"],  # Layer 3: Table modifications
    ["view_prereq_grants", "grants"],  # Layer 4: View prereq + general grants
    ["synonym"],  # Layer 5: Synonyms
    ["view_refresh"],  # Layer 6: Existing prerequisite views
    ["view"],  # Layer 7: Missing views
    ["view_post_grants"],  # Layer 8: View post grants
    ["materialized_view"],  # Layer 9: MVIEWs
    ["type"],  # Layer 10: Types (specs)
    ["package"],  # Layer 11: Package specs
    ["procedure", "function"],  # Layer 12: Standalone routines
    ["type_body", "package_body"],  # Layer 13: Type/package bodies
    ["context"],  # Layer 14: Application contexts
    ["name_collision"],  # Layer 15: Name collision remediation
    ["constraint", "index"],  # Layer 16: Constraints and indexes
    ["trigger"],  # Layer 17: Triggers (last)
    ["job", "schedule"],  # Layer 18: Jobs
]

CORE_GRANT_DIRS_ORDER = ("grants_all", "grants_miss", "grants")
VIEW_GRANT_DIRS_ORDER = ("view_prereq_grants", "view_post_grants")
GRANT_DIRS = set(CORE_GRANT_DIRS_ORDER) | set(VIEW_GRANT_DIRS_ORDER)

GRANT_PRIVILEGE_BY_TYPE = {
    "TABLE": "SELECT",
    "VIEW": "SELECT",
    "MATERIALIZED VIEW": "SELECT",
    "SYNONYM": "SELECT",
    "SEQUENCE": "SELECT",
    "INDEX": "SELECT",
    "TYPE": "EXECUTE",
    "TYPE BODY": "EXECUTE",
    "PROCEDURE": "EXECUTE",
    "FUNCTION": "EXECUTE",
    "PACKAGE": "EXECUTE",
    "PACKAGE BODY": "EXECUTE",
    "JOB": "EXECUTE",
    "SCHEDULE": "EXECUTE",
}

GRANT_OPTION_TYPES = {"VIEW", "MATERIALIZED VIEW"}

DEFAULT_FIXUP_AUTO_GRANT_TYPES_ORDERED = (
    "VIEW",
    "MATERIALIZED VIEW",
    "SYNONYM",
    "PROCEDURE",
    "FUNCTION",
    "PACKAGE",
    "PACKAGE BODY",
    "TYPE",
    "TYPE BODY",
)
DEFAULT_FIXUP_AUTO_GRANT_TYPES = set(DEFAULT_FIXUP_AUTO_GRANT_TYPES_ORDERED)
FIXUP_AUTO_GRANT_ALLOWED_TYPES = set(GRANT_PRIVILEGE_BY_TYPE.keys())
AUTO_GRANT_SYSTEM_SCHEMAS = {"SYS", "PUBLIC"}

SYS_PRIV_IMPLICATIONS = {
    "SELECT": {
        "SELECT ANY TABLE",
        "SELECT ANY SEQUENCE",
        "SELECT ANY DICTIONARY",
    },
    "EXECUTE": {
        "EXECUTE ANY PROCEDURE",
        "EXECUTE ANY TYPE",
    },
    "REFERENCES": {
        "REFERENCES ANY TABLE",
    },
    "INSERT": {
        "INSERT ANY TABLE",
    },
    "UPDATE": {
        "UPDATE ANY TABLE",
    },
    "DELETE": {
        "DELETE ANY TABLE",
    },
}

DICTIONARY_OWNER_SCHEMAS = {"SYS", "SYSTEM"}


def resolve_implied_sys_privileges(
    required_priv: str, target_full: Optional[str] = None, target_type: Optional[str] = None
) -> Set[str]:
    """
    Resolve system privilege implication set for a target object.

    SELECT ANY DICTIONARY is only valid for dictionary owners (SYS/SYSTEM).
    It must not be treated as generic SELECT on business schemas.
    """
    required_u = (required_priv or "").upper()
    target_type_u = normalize_object_type(target_type or "")
    implied: Set[str]

    # Tighten privilege implication by object type to avoid false positives.
    if required_u == "SELECT":
        if target_type_u in {"TABLE", "VIEW", "MATERIALIZED VIEW", "SYNONYM"}:
            implied = {"SELECT ANY TABLE"}
        elif target_type_u == "SEQUENCE":
            implied = {"SELECT ANY SEQUENCE"}
        else:
            implied = set()
    elif required_u == "EXECUTE":
        if target_type_u in {"PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE BODY", "JOB", "SCHEDULE"}:
            implied = {"EXECUTE ANY PROCEDURE"}
        elif target_type_u in {"TYPE", "TYPE BODY"}:
            implied = {"EXECUTE ANY TYPE"}
        else:
            implied = set()
    elif required_u == "REFERENCES":
        implied = {"REFERENCES ANY TABLE"} if target_type_u == "TABLE" else set()
    elif required_u in {"INSERT", "UPDATE", "DELETE"}:
        implied = (
            {f"{required_u} ANY TABLE"}
            if target_type_u in {"TABLE", "VIEW", "MATERIALIZED VIEW"}
            else set()
        )
    else:
        implied = set(SYS_PRIV_IMPLICATIONS.get(required_u, set()))

    if required_u != "SELECT":
        return implied
    if not target_full:
        return implied
    schema, _ = parse_object_token(target_full)
    if (
        schema
        and schema.upper() in DICTIONARY_OWNER_SCHEMAS
        and target_type_u in {"TABLE", "VIEW", "MATERIALIZED VIEW", "SYNONYM"}
    ):
        implied.add("SELECT ANY DICTIONARY")
    return implied


def is_grant_dir(dir_name: str) -> bool:
    return dir_name.lower() in GRANT_DIRS


def resolve_grant_dirs(
    subdirs: Dict[str, Path], include_dirs: Optional[Set[str]], exclude_dirs: Set[str]
) -> List[str]:
    available = set(subdirs.keys())
    include_set = {
        normalize_dir_filter(d) for d in include_dirs or set() if normalize_dir_filter(d)
    }
    exclude_set = {
        normalize_dir_filter(d) for d in exclude_dirs or set() if normalize_dir_filter(d)
    }

    def _included(name: str) -> bool:
        return not include_set or any(dir_filter_overlaps(name, item) for item in include_set)

    def _excluded(name: str) -> bool:
        return any(dir_path_matches_filter(name, item) for item in exclude_set)

    grant_dirs: List[str] = []
    if include_set:
        if _included("grants_all") and "grants_all" in available and not _excluded("grants_all"):
            grant_dirs.append("grants_all")
        if _included("grants_miss") and "grants_miss" in available and not _excluded("grants_miss"):
            grant_dirs.append("grants_miss")
        if _included("grants"):
            if "grants_miss" in available and not _excluded("grants_miss"):
                grant_dirs.append("grants_miss")
            elif "grants" in available and not _excluded("grants"):
                grant_dirs.append("grants")
        if (
            _included("view_prereq_grants")
            and "view_prereq_grants" in available
            and not _excluded("view_prereq_grants")
        ):
            grant_dirs.append("view_prereq_grants")
        if (
            _included("view_post_grants")
            and "view_post_grants" in available
            and not _excluded("view_post_grants")
        ):
            grant_dirs.append("view_post_grants")
    else:
        if "grants_miss" in available and not _excluded("grants_miss"):
            grant_dirs.append("grants_miss")
        elif "grants" in available and not _excluded("grants"):
            grant_dirs.append("grants")
        if "view_prereq_grants" in available and not _excluded("view_prereq_grants"):
            grant_dirs.append("view_prereq_grants")
        if "view_post_grants" in available and not _excluded("view_post_grants"):
            grant_dirs.append("view_post_grants")

    # preserve order, remove duplicates
    return list(dict.fromkeys(grant_dirs))


def build_run_fixup_change_notices(
    args, fixup_dir: Path, only_dirs: List[str]
) -> List[RuntimeNotice]:
    notices: List[RuntimeNotice] = []
    selected_dirs = {
        normalize_dir_filter(item) for item in (only_dirs or []) if normalize_dir_filter(item)
    }
    selected_all = not selected_dirs
    selected_table = any(dir_filter_overlaps("table", item) for item in selected_dirs)
    selected_view = any(dir_filter_overlaps("view", item) for item in selected_dirs)
    selected_grants_revoke = any(
        dir_filter_overlaps("grants_revoke", item) for item in selected_dirs
    )
    selected_cleanup_safe = any(dir_filter_overlaps("cleanup_safe", item) for item in selected_dirs)
    selected_sequence_restart = any(
        dir_filter_overlaps("sequence_restart", item) for item in selected_dirs
    )
    selected_manual_review = any(
        any(dir_filter_overlaps(dir_name, item) for item in selected_dirs)
        for dir_name in MANUAL_REVIEW_DIRS
    )
    if not getattr(args, "allow_table_create", False) and (
        selected_table or (selected_all and (fixup_dir / "table").exists())
    ):
        notices.append(
            RuntimeNotice(
                "fixup_table_safe_gate",
                "0.9.8.7",
                "建表脚本默认不执行",
                "run_fixup 默认跳过 table/；确需建表请显式加 --allow-table-create。",
            )
        )
    if (selected_all or selected_grants_revoke) and (fixup_dir / "grants_revoke").exists():
        notices.append(
            RuntimeNotice(
                "public_grants_revoke_audit",
                "0.9.8.7",
                "PUBLIC 扩权现在会单独审计",
                "若出现 grants_revoke，请先核对源端是否声明，再决定是否回收。",
            )
        )
    if (
        (selected_all or selected_view)
        and (fixup_dir / "view").exists()
        and not getattr(args, "view_chain_autofix", False)
    ):
        notices.append(
            RuntimeNotice(
                "view_chain_autofix",
                "0.9.8.6",
                "视图链可自动修复",
                "缺失 VIEW 或依赖复杂时，可尝试 python3 run_fixup.py config.ini --view-chain-autofix。",
            )
        )
    if selected_cleanup_safe and (fixup_dir / "cleanup_safe").exists():
        notices.append(
            RuntimeNotice(
                "cleanup_safe_review",
                "0.9.8.9",
                "安全清理目录需要显式确认",
                "cleanup_safe/ 下是 destructive SQL；请先审 extra_cleanup_candidates.txt，再显式按目录执行。",
            )
        )
    if (selected_all or selected_sequence_restart) and (fixup_dir / "sequence_restart").exists():
        notices.append(
            RuntimeNotice(
                "sequence_restart_review",
                "0.9.8.9",
                "sequence_restart 默认不自动执行",
                "sequence_restart/ 是值同步 SQL；请先核对 sequence_restart_detail 与源/目标 LAST_NUMBER，再显式按目录执行。",
            )
        )
    manual_dirs_present = [
        dir_name for dir_name in MANUAL_REVIEW_DIRS if (fixup_dir / dir_name).exists()
    ]
    if manual_dirs_present and (selected_all or selected_manual_review):
        iterative_hint = ""
        if getattr(args, "iterative", False):
            iterative_hint = " 如启用 --iterative，失败脚本也只保留到 errors 报告，不会自动重试。"
        notices.append(
            RuntimeNotice(
                "manual_only_family_review",
                "0.9.9.4",
                "manual-only family 默认不自动执行",
                "、".join(manual_dirs_present)
                + "/ 属于 manual-only family；请先核对 manual_actions_required 与源端定义，再显式按目录执行。"
                + iterative_hint,
            )
        )
    return notices


def log_change_notices_block(notices: List[RuntimeNotice]) -> None:
    if not notices:
        return
    log_section("本次相关变化提醒")
    for idx, notice in enumerate(notices, start=1):
        log.warning("%d. %s：%s", idx, notice.title, notice.message)


CREATE_OBJECT_DIRS = {
    "sequence",
    "table",
    "view",
    "view_refresh",
    "materialized_view",
    "synonym",
    "procedure",
    "function",
    "package",
    "package_body",
    "type",
    "type_body",
    "trigger",
    "constraint",
    "index",
    "job",
    "schedule",
}

RE_QUOTED_DOT = re.compile(r"'([A-Za-z0-9_#$]+\.[A-Za-z0-9_#$]+)'")
RE_DOUBLE_QUOTED_DOT = re.compile(r'"([A-Za-z0-9_#$]+)"\."([A-Za-z0-9_#$]+)"')
RE_PLAIN_DOT = re.compile(r"([A-Za-z0-9_#$]+)\.([A-Za-z0-9_#$]+)")
RE_SINGLE_QUOTED_NAME = re.compile(r"'([A-Za-z0-9_#$]+)'")
RE_BLOCK_START = re.compile(
    r"^\s*CREATE\s+(OR\s+REPLACE\s+)?"
    r"(PROCEDURE|FUNCTION|PACKAGE(\s+BODY)?|TYPE(\s+BODY)?|TRIGGER)\b",
    re.IGNORECASE,
)
RE_ANON_BLOCK_START = re.compile(r"^\s*(DECLARE|BEGIN)\b", re.IGNORECASE)
RE_BLOCK_HEADER_NAME = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:NON)?EDITIONABLE\s+)?"
    r"(?:PROCEDURE|FUNCTION|PACKAGE(?:\s+BODY)?|TYPE(?:\s+BODY)?|TRIGGER)\s+"
    r'(?P<name>(?:"[^"]+"|[A-Z0-9_$#]+)(?:\s*\.\s*(?:"[^"]+"|[A-Z0-9_$#]+))?)',
    re.IGNORECASE,
)
RE_BLOCK_END = re.compile(
    r'^\s*END(?:\s+(?:"(?P<quoted>[^"]+)"|(?P<name>[A-Z0-9_$#]+)))?\s*;\s*(?:--.*)?$', re.IGNORECASE
)
PLSQL_INNER_END_KEYWORDS = {"IF", "LOOP", "CASE", "WHILE", "FOR", "REPEAT"}
RE_CREATE_VIEW = re.compile(
    r"^\s*CREATE\s+(OR\s+REPLACE\s+)?(FORCE\s+)?(MATERIALIZED\s+)?VIEW\b", re.IGNORECASE
)
RE_ERROR_CODE = re.compile(r"(ORA-\d{5}|OBE?-\d+|PLS-\d{5}|SP2-\d{4})", re.IGNORECASE)
RE_SQL_ERROR = re.compile(r"(ORA-\d{5}|OBE?-\d+|PLS-\d{5}|SP2-\d{4}|ERROR\s+\d+)", re.IGNORECASE)
RE_PLS_ERROR = re.compile(r"\bPLS-\d{5}\b", re.IGNORECASE)
RE_SP2_ERROR = re.compile(r"\bSP2-\d{4}\b", re.IGNORECASE)
RE_GENERIC_ERROR_CODE = re.compile(r"\bERROR\s+\d+\b", re.IGNORECASE)
RE_GRANT_ON = re.compile(
    r"^GRANT\s+.+?\s+ON\s+(?P<object>[^\s]+)\s+TO\s+(?P<grantee>[^\s;]+)", re.IGNORECASE | re.DOTALL
)
RE_GRANT_OBJECT = re.compile(
    r"^\s*GRANT\s+(?P<privs>.+?)\s+ON\s+(?P<object>.+?)\s+TO\s+(?P<grantees>.+)$",
    re.IGNORECASE | re.DOTALL,
)
RE_GRANT_SIMPLE = re.compile(
    r"^\s*GRANT\s+(?P<privs>.+?)\s+TO\s+(?P<grantees>.+)$", re.IGNORECASE | re.DOTALL
)
RE_WITH_OPTION = re.compile(r"\s+WITH\s+GRANT\s+OPTION|\s+WITH\s+ADMIN\s+OPTION", re.IGNORECASE)
RE_CHAIN_NODE = re.compile(r"(?P<name>[^\[]+)\[(?P<meta>[^\]]+)\]")

Q_QUOTE_DELIMS = {
    "[": "]",
    "(": ")",
    "{": "}",
    "<": ">",
}


class ConfigError(Exception):
    """Custom exception for configuration issues."""


@contextmanager
def acquire_fixup_run_lock(fixup_dir: Path):
    """Best-effort process lock to avoid concurrent run_fixup execution."""
    lock_path = fixup_dir / FIXUP_RUN_LOCK_FILENAME
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fp = lock_path.open("a+", encoding="utf-8")
    locked = False
    try:
        if fcntl is None:
            yield
            return
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except BlockingIOError as exc:
            raise ConfigError(f"检测到另一个 run_fixup 正在执行: {lock_path}") from exc
        yield
    finally:
        if locked and fcntl is not None:
            try:
                fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
        try:
            fp.close()
        except Exception:
            pass


class FixupStateLedger:
    """Local ledger to avoid duplicate replay after move-to-done failures."""

    def __init__(self, fixup_dir: Path):
        self.path = fixup_dir / STATE_LEDGER_FILENAME
        self._data: Dict[str, Dict[str, str]] = {}
        self._dirty = False
        self.skipped_completed = 0
        self.fingerprint_mismatches = 0
        self._load()

    @staticmethod
    def fingerprint(sql_payload: Union[str, bytes]) -> str:
        if isinstance(sql_payload, bytes):
            return hashlib.sha1(sql_payload).hexdigest()
        return hashlib.sha1((sql_payload or "").encode("utf-8")).hexdigest()

    def _load(self) -> None:
        if not self.path.exists():
            self._data = {}
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                self._data = (
                    payload.get("completed", {})
                    if isinstance(payload.get("completed"), dict)
                    else {}
                )
            else:
                self._data = {}
        except Exception as exc:
            log.warning("[STATE] 读取状态账本失败，将忽略旧账本: %s", exc)
            self._data = {}

    def flush(self) -> None:
        if not self._dirty:
            return
        tmp_path: Optional[Path] = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"version": 1, "completed": self._data}
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=str(self.path.parent),
                prefix=f"{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as tmp_fp:
                tmp_fp.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
                tmp_path = Path(tmp_fp.name)
            os.replace(str(tmp_path), str(self.path))
            self._dirty = False
        except Exception as exc:
            log.warning("[STATE] 写入状态账本失败: %s", exc)
            if tmp_path and tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass

    def is_completed(self, relative_path: Path, fingerprint: str) -> bool:
        key = str(relative_path).replace("\\", "/")
        item = self._data.get(key)
        if not item:
            return False
        if item.get("fingerprint") == fingerprint:
            self.skipped_completed += 1
            return True
        self.fingerprint_mismatches += 1
        log.warning(
            "[STATE] 已完成记录指纹不匹配，将重新执行: %s (ledger=%s current=%s)",
            key,
            item.get("fingerprint") or "-",
            fingerprint or "-",
        )
        return False

    def summary(self) -> Dict[str, int]:
        return {
            "ledger_records": len(self._data),
            "skipped_completed": int(self.skipped_completed),
            "fingerprint_mismatches": int(self.fingerprint_mismatches),
        }

    def mark_completed(self, relative_path: Path, fingerprint: str, note: str) -> None:
        key = str(relative_path).replace("\\", "/")
        self._data[key] = {
            "fingerprint": fingerprint,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "note": note[:300],
        }
        self._dirty = True

    def clear(self, relative_path: Path) -> None:
        key = str(relative_path).replace("\\", "/")
        if key in self._data:
            self._data.pop(key, None)
            self._dirty = True


def _coerce_summary_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return default


def log_fixup_state_ledger_summary(state_ledger: FixupStateLedger) -> None:
    state_ledger.flush()
    try:
        summary = state_ledger.summary()
    except Exception as exc:
        log.warning("[STATE] 读取状态账本摘要失败: %s", exc)
        return
    if isinstance(summary, dict):
        ledger_records = _coerce_summary_int(summary.get("ledger_records"))
        skipped_completed = _coerce_summary_int(summary.get("skipped_completed"))
        fingerprint_mismatches = _coerce_summary_int(summary.get("fingerprint_mismatches"))
    else:
        ledger_data = getattr(state_ledger, "_data", {})
        ledger_records = len(ledger_data) if isinstance(ledger_data, dict) else 0
        skipped_completed = _coerce_summary_int(getattr(state_ledger, "skipped_completed", 0))
        fingerprint_mismatches = _coerce_summary_int(
            getattr(state_ledger, "fingerprint_mismatches", 0)
        )
    log.info(
        "[STATE] ledger resume summary: records=%d skipped_completed=%d fingerprint_mismatches=%d",
        ledger_records,
        skipped_completed,
        fingerprint_mismatches,
    )


def read_sql_text_with_limit(
    sql_path: Path, max_bytes: Optional[int]
) -> Tuple[Optional[str], Optional[bytes], Optional[str]]:
    """Read SQL file with optional size limit."""
    try:
        if max_bytes and max_bytes > 0:
            size = sql_path.stat().st_size
            if size > max_bytes:
                return None, None, f"文件过大 ({size} bytes) 超过限制 {max_bytes} bytes"
        raw_bytes = sql_path.read_bytes()
        try:
            return raw_bytes.decode("utf-8"), raw_bytes, None
        except UnicodeDecodeError as exc:
            return (
                None,
                raw_bytes,
                f"文件编码不是 UTF-8，已阻断执行以避免破坏 replay/ledger 语义: {exc}",
            )
    except Exception as exc:
        return None, None, f"读取文件失败: {exc}"


def extract_sql_error(output: str) -> Optional[str]:
    if not output:
        return None

    best_line: Optional[str] = None
    best_score = -1
    best_index = 10**9
    for idx, raw_line in enumerate(output.splitlines()):
        score = score_execution_error_line(raw_line)
        if score is None:
            continue
        line = raw_line.strip()
        if score > best_score or (score == best_score and idx < best_index):
            best_score = score
            best_index = idx
            best_line = line
    return best_line


def score_execution_error_line(line: str) -> Optional[int]:
    if not line:
        return None
    stripped = line.strip()
    if not stripped:
        return None
    upper = stripped.upper()
    if RE_PLS_ERROR.search(stripped):
        return 140
    if "ORA-06512" in upper:
        return None
    if "ORA-06550" in upper:
        return 110
    if RE_ERROR_CODE.search(stripped):
        return 130
    if RE_SP2_ERROR.search(stripped):
        return 100
    if RE_GENERIC_ERROR_CODE.search(stripped):
        return 90
    return None


def _scan_sql_word_tokens(sql_text: str) -> List[Tuple[str, int, int]]:
    """Scan SQL words outside literals/comments; returns (UPPER_WORD, start, end)."""
    tokens: List[Tuple[str, int, int]] = []
    i = 0
    n = len(sql_text or "")
    in_single = False
    in_double = False
    in_q_quote = False
    q_quote_end = ""
    block_comment_depth = 0
    line_comment = False
    while i < n:
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < n else ""

        if line_comment:
            if ch in ("\n", "\r"):
                line_comment = False
            i += 1
            continue

        if block_comment_depth > 0:
            if ch == "/" and nxt == "*":
                block_comment_depth += 1
                i += 2
                continue
            if ch == "*" and nxt == "/":
                block_comment_depth -= 1
                i += 2
                continue
            i += 1
            continue

        if in_q_quote:
            if ch == q_quote_end and nxt == "'":
                in_q_quote = False
                i += 2
                continue
            i += 1
            continue

        if in_single:
            if ch == "'" and nxt == "'":
                i += 2
                continue
            if ch == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            if ch == '"' and nxt == '"':
                i += 2
                continue
            if ch == '"':
                in_double = False
            i += 1
            continue

        if ch == "-" and nxt == "-":
            line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            block_comment_depth += 1
            i += 2
            continue
        if ch == "'":
            in_single = True
            i += 1
            continue
        if ch == '"':
            in_double = True
            i += 1
            continue
        if ch in ("q", "Q") and nxt == "'" and i + 2 < n:
            delimiter = sql_text[i + 2]
            if not delimiter.isspace():
                q_quote_end = Q_QUOTE_DELIMS.get(delimiter, delimiter)
                in_q_quote = True
                i += 3
                continue

        if ch.isalpha() or ch in ("_", "$", "#"):
            start = i
            i += 1
            while i < n:
                c = sql_text[i]
                if c.isalnum() or c in ("_", "$", "#"):
                    i += 1
                    continue
                break
            word = sql_text[start:i]
            tokens.append((word.upper(), start, i))
            continue
        i += 1
    return tokens


def detect_session_sensitive_reason(sql_text: str) -> Optional[str]:
    """
    Detect SQL patterns that require same-session execution semantics.
    Keep CURRENT_SCHEMA-only statements out of this rule to preserve
    existing per-statement behavior.
    """
    tokens = [token for token, _start, _end in _scan_sql_word_tokens(sql_text or "")]
    if not tokens:
        return None

    n = len(tokens)
    for i in range(n - 1):
        if tokens[i] == "ALTER" and tokens[i + 1] == "SESSION":
            # "ALTER SESSION SET CURRENT_SCHEMA" 已由 per-statement 注入补偿处理。
            if i + 3 < n and tokens[i + 2] == "SET" and tokens[i + 3] == "CURRENT_SCHEMA":
                continue
            return "ALTER SESSION"
        if tokens[i] == "SET" and tokens[i + 1] == "ROLE":
            return "SET ROLE"

    for token in tokens:
        if token in {"DBMS_SESSION", "DBMS_APPLICATION_INFO"}:
            return token

    for i in range(max(0, n - 3)):
        if (
            tokens[i] == "CREATE"
            and tokens[i + 1] == "GLOBAL"
            and tokens[i + 2] == "TEMPORARY"
            and tokens[i + 3] == "TABLE"
        ):
            return "GLOBAL TEMPORARY TABLE"
    return None


def sanitize_view_chain_view_ddl(ddl_text: str) -> str:
    if not ddl_text:
        return ddl_text
    tokens = _scan_sql_word_tokens(ddl_text)
    if not tokens or tokens[0][0] != "CREATE":
        return ddl_text
    idx = 1
    has_or_replace = False
    if idx + 1 < len(tokens) and tokens[idx][0] == "OR" and tokens[idx + 1][0] == "REPLACE":
        has_or_replace = True
        idx += 2
    view_idx: Optional[int] = None
    for pos in range(idx, len(tokens)):
        if tokens[pos][0] == "VIEW":
            view_idx = pos
            break
        if tokens[pos][0] in ("AS", "SELECT", "WITH"):
            break
    if view_idx is None:
        return ddl_text

    create_start = tokens[0][1]
    view_end = tokens[view_idx][2]
    mid_start = tokens[idx - 1][2] if has_or_replace else tokens[0][2]
    mid = ddl_text[mid_start : tokens[view_idx][1]]
    mid_clean = re.sub(
        r"(?is)\bNO\s+FORCE\b|\bFORCE\b|\bEDITIONABLE\b|\bNONEDITIONABLE\b", " ", mid
    )
    mid_clean = " ".join(mid_clean.split())
    prefix = "CREATE"
    if has_or_replace:
        prefix += " OR REPLACE"
    replacement = prefix + (" " + mid_clean if mid_clean else "") + " VIEW"
    return ddl_text[:create_start] + replacement + ddl_text[view_end:]


def move_sql_to_done(sql_path: Path, done_dir: Path) -> str:
    """Move executed SQL to done directory with backup if needed."""

    def _next_backup_path(target_dir: Path, sql_path: Path) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        candidate = target_dir / f"{sql_path.stem}.bak_{timestamp}{sql_path.suffix}"
        index = 1
        while candidate.exists():
            candidate = target_dir / f"{sql_path.stem}.bak_{timestamp}_{index}{sql_path.suffix}"
            index += 1
        return candidate

    target_dir = done_dir / sql_path.parent.name
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / sql_path.name
    staging_path = target_dir / f".{sql_path.name}.staging_{os.getpid()}_{time.time_ns()}"
    backup_names: List[str] = []

    def _backup_existing_target() -> Optional[str]:
        if not target_path.exists():
            return None
        backup_path = _next_backup_path(target_dir, sql_path)
        try:
            os.replace(target_path, backup_path)
        except Exception as exc:
            log.warning("已存在文件备份失败: %s (%s)", target_path, str(exc)[:200])
            raise
        backup_names.append(backup_path.name)
        return backup_path.name

    def _restore_source() -> None:
        if not staging_path.exists():
            return
        try:
            shutil.move(str(staging_path), str(sql_path))
        except Exception as restore_exc:
            log.warning("移动失败后恢复源文件失败: %s (%s)", sql_path, str(restore_exc)[:200])

    try:
        shutil.move(str(sql_path), str(staging_path))
        try:
            _backup_existing_target()
        except Exception as exc:
            _restore_source()
            return f"(移动失败: 目标已存在且备份失败: {exc})"

        while True:
            try:
                os.link(staging_path, target_path)
                break
            except FileExistsError:
                try:
                    _backup_existing_target()
                except Exception as exc:
                    _restore_source()
                    return f"(移动失败: 目标在发布时重现且备份失败: {exc})"
            except Exception as exc:
                _restore_source()
                return f"(移动失败: {exc})"

        try:
            staging_path.unlink(missing_ok=True)
        except Exception:
            pass
        backup_note = f" (已备份: {', '.join(backup_names)})" if backup_names else ""
        return f"(已移至 done/{sql_path.parent.name}/){backup_note}"
    except Exception as exc:
        _restore_source()
        return f"(移动失败: {exc})"


@dataclass
class ScriptResult:
    path: Path
    status: str  # SUCCESS, FAILED, ERROR, SKIPPED
    message: str = ""
    layer: int = -1


@dataclass
class StatementFailure:
    index: int
    error: str
    statement: str


@dataclass
class ExecutionSummary:
    statements: int
    failures: List[StatementFailure]

    @property
    def success(self) -> bool:
        return not self.failures


@dataclass
class ErrorReportEntry:
    file_path: Path
    statement_index: int
    error_code: str
    object_name: str
    message: str
    family: str = "-"
    support_tier: str = "-"
    retry_policy: str = "-"


@dataclass
class FixupAutoGrantSettings:
    enabled: bool
    types: Set[str]
    fallback: bool
    cache_limit: int
    exec_mode: str = DEFAULT_FIXUP_EXEC_MODE
    exec_file_fallback: bool = DEFAULT_FIXUP_EXEC_FILE_FALLBACK


@dataclass
class FixupHotReloadSettings:
    mode: str
    interval_sec: int
    fail_policy: str


@dataclass
class FixupHotReloadRuntime:
    config_path: Path
    mode: str
    interval_sec: int
    fail_policy: str
    watch_paths: List[Path]
    snapshot: Dict[str, str]
    last_check_at: float = 0.0
    events: List[Dict[str, str]] = None

    def __post_init__(self) -> None:
        if self.events is None:
            self.events = []


@dataclass
class FixupPrecheckSummary:
    current_user: str
    target_schemas: Set[str]
    existing_schemas: Set[str]
    missing_schemas: Set[str]
    required_sys_privileges: Set[str]
    effective_sys_privileges: Set[str]
    missing_sys_privileges: Set[str]
    schema_lookup_source: str = "unknown"


class LimitedCache(OrderedDict):
    """Simple size-limited cache with LRU-style eviction."""

    def __init__(self, max_size: int):
        super().__init__()
        self.max_size = int(max_size) if max_size is not None else 0

    def __setitem__(self, key, value) -> None:
        if key in self:
            try:
                self.move_to_end(key)
            except Exception:
                pass
        super().__setitem__(key, value)
        if self.max_size > 0:
            while len(self) > self.max_size:
                try:
                    self.popitem(last=False)
                except KeyError:
                    break

    def get(self, key, default=None):
        if key in self:
            try:
                self.move_to_end(key)
            except Exception:
                pass
        return super().get(key, default)


@dataclass
class AutoGrantStats:
    planned: int = 0
    executed: int = 0
    failed: int = 0
    blocked: int = 0
    skipped: int = 0


@dataclass
class AutoGrantContext:
    settings: FixupAutoGrantSettings
    deps_by_object: Dict[Tuple[str, str], Set[Tuple[str, str]]]
    grant_index_miss: "GrantIndex"
    grant_index_all: "GrantIndex"
    obclient_cmd: List[str]
    timeout: Optional[int]
    roles_cache: Dict[str, Set[str]]
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]]
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]]
    sys_privs_cache: Dict[str, Set[str]]
    planned_statements: Set[str]
    planned_object_privs: Set[Tuple[str, str, str]]
    planned_object_privs_with_option: Set[Tuple[str, str, str]]
    planned_sys_privs: Set[Tuple[str, str]]
    applied_grants: Set[str]
    blocked_objects: Set[Tuple[str, str]]
    stats: AutoGrantStats


def normalize_config_hot_reload_mode(raw_value: Optional[str]) -> str:
    value = (raw_value or "off").strip().lower()
    if value not in CONFIG_HOT_RELOAD_MODE_VALUES:
        log.warning(
            "config_hot_reload_mode=%s 非法，回退为 off（支持: off/phase/round）", raw_value
        )
        return "off"
    return value


def normalize_config_hot_reload_fail_policy(raw_value: Optional[str]) -> str:
    value = (raw_value or "keep_last_good").strip().lower()
    if value not in CONFIG_HOT_RELOAD_FAIL_POLICY_VALUES:
        log.warning(
            "config_hot_reload_fail_policy=%s 非法，回退为 keep_last_good（支持: keep_last_good/abort）",
            raw_value,
        )
        return "keep_last_good"
    return value


def parse_config_hot_reload_interval(raw_value: Optional[str]) -> int:
    if raw_value is None or not str(raw_value).strip():
        return DEFAULT_CONFIG_HOT_RELOAD_INTERVAL_SEC
    try:
        value = int(str(raw_value).strip())
    except Exception:
        log.warning(
            "config_hot_reload_interval_sec=%s 非法，回退为 %d",
            raw_value,
            DEFAULT_CONFIG_HOT_RELOAD_INTERVAL_SEC,
        )
        return DEFAULT_CONFIG_HOT_RELOAD_INTERVAL_SEC
    if value < 1:
        log.warning(
            "config_hot_reload_interval_sec=%s 小于 1，回退为 %d",
            raw_value,
            DEFAULT_CONFIG_HOT_RELOAD_INTERVAL_SEC,
        )
        return DEFAULT_CONFIG_HOT_RELOAD_INTERVAL_SEC
    return value


def resolve_config_relative_path(base_dir: Path, raw_path: str) -> Path:
    path = Path((raw_path or "").strip()).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def resolve_hot_reload_watch_paths(
    parser: configparser.ConfigParser, config_path: Path
) -> List[Path]:
    base_dir = config_path.parent.resolve()
    watch_paths: List[Path] = [config_path.resolve()]
    settings = parser["SETTINGS"] if parser.has_section("SETTINGS") else {}
    for key in ("remap_file", "blacklist_rules_path", "exclude_objects_file"):
        raw = (settings.get(key) if settings else "") or ""
        raw = str(raw).strip()
        if not raw:
            continue
        watch_paths.append(resolve_config_relative_path(base_dir, raw))
    unique: List[Path] = []
    seen: Set[Path] = set()
    for item in watch_paths:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
    return unique


def compute_file_sha1(path: Path) -> str:
    if not path.exists():
        return "MISSING"
    try:
        data = path.read_bytes()
    except Exception as exc:
        return f"ERROR:{exc.__class__.__name__}:{str(exc)[:120]}"
    return hashlib.sha1(data).hexdigest()


def build_watch_snapshot(paths: List[Path]) -> Dict[str, str]:
    snapshot: Dict[str, str] = {}
    for path in paths:
        snapshot[str(path)] = compute_file_sha1(path)
    return snapshot


def load_fixup_hot_reload_settings(parser: configparser.ConfigParser) -> FixupHotReloadSettings:
    settings = parser["SETTINGS"] if parser.has_section("SETTINGS") else {}
    mode = normalize_config_hot_reload_mode(settings.get("config_hot_reload_mode", "off"))
    interval_sec = parse_config_hot_reload_interval(
        settings.get("config_hot_reload_interval_sec", "5")
    )
    fail_policy = normalize_config_hot_reload_fail_policy(
        settings.get("config_hot_reload_fail_policy", "keep_last_good")
    )
    if mode == "phase":
        log.warning("run_fixup 不支持 phase 热加载，已回退为 off。")
        mode = "off"
    return FixupHotReloadSettings(mode=mode, interval_sec=interval_sec, fail_policy=fail_policy)


def init_fixup_hot_reload_runtime(config_path: Path) -> Optional[FixupHotReloadRuntime]:
    parser = configparser.ConfigParser(interpolation=None)
    try:
        parser.read(config_path, encoding="utf-8")
    except Exception as exc:
        log.warning("初始化配置热加载失败，将关闭热加载: %s", exc)
        return None
    hot_settings = load_fixup_hot_reload_settings(parser)
    watch_paths = resolve_hot_reload_watch_paths(parser, config_path)
    snapshot = build_watch_snapshot(watch_paths)
    return FixupHotReloadRuntime(
        config_path=config_path,
        mode=hot_settings.mode,
        interval_sec=hot_settings.interval_sec,
        fail_policy=hot_settings.fail_policy,
        watch_paths=watch_paths,
        snapshot=snapshot,
    )


def append_fixup_hot_reload_event(
    runtime: FixupHotReloadRuntime,
    status: str,
    stage: str,
    changed_files: List[str],
    changed_keys: List[str],
    note: str,
) -> None:
    event = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
        "stage": stage,
        "changed_files": ",".join(changed_files) if changed_files else "-",
        "changed_keys": ",".join(changed_keys) if changed_keys else "-",
        "note": note or "-",
    }
    runtime.events.append(event)
    message = "[HOT_RELOAD] {status} stage={stage} files={files} keys={keys} note={note}".format(
        status=event["status"],
        stage=event["stage"],
        files=event["changed_files"],
        keys=event["changed_keys"],
        note=event["note"],
    )
    if status in {"REJECTED", "REQUIRES_RESTART"}:
        log.warning(message)
    else:
        log.info(message)


def write_fixup_hot_reload_events_report(
    fixup_dir: Path, runtime: Optional[FixupHotReloadRuntime]
) -> Optional[Path]:
    if not runtime or not runtime.events:
        return None
    report_dir = fixup_dir / FIXUP_HOT_RELOAD_EVENTS_DIR
    report_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = report_dir / f"config_reload_events_{ts}.txt"
    lines = [
        "# config hot reload events",
        f"# mode={runtime.mode} interval_sec={runtime.interval_sec} fail_policy={runtime.fail_policy}",
        "TS | STATUS | STAGE | CHANGED_FILES | CHANGED_KEYS | NOTE",
    ]
    for event in runtime.events:
        lines.append(
            "{ts} | {status} | {stage} | {changed_files} | {changed_keys} | {note}".format(
                ts=event.get("ts", "-"),
                status=event.get("status", "-"),
                stage=event.get("stage", "-"),
                changed_files=event.get("changed_files", "-"),
                changed_keys=event.get("changed_keys", "-"),
                note=event.get("note", "-"),
            )
        )
    report_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return report_path


def apply_fixup_hot_reload_at_round(
    runtime: Optional[FixupHotReloadRuntime],
    round_num: int,
    current_ob_cfg: Dict[str, str],
    current_fixup_dir: Path,
    current_report_dir: Path,
    current_fixup_settings: FixupAutoGrantSettings,
    current_max_sql_file_bytes: Optional[int],
) -> Tuple[Dict[str, str], FixupAutoGrantSettings, Optional[int], bool]:
    if not runtime or runtime.mode != "round":
        return current_ob_cfg, current_fixup_settings, current_max_sql_file_bytes, False

    now = time.time()
    if runtime.last_check_at > 0 and (now - runtime.last_check_at) < runtime.interval_sec:
        return current_ob_cfg, current_fixup_settings, current_max_sql_file_bytes, False
    runtime.last_check_at = now

    latest_snapshot = build_watch_snapshot(runtime.watch_paths)
    changed_files = [
        path
        for path in sorted(latest_snapshot.keys())
        if latest_snapshot.get(path) != runtime.snapshot.get(path)
    ]
    if not changed_files:
        runtime.snapshot = latest_snapshot
        return current_ob_cfg, current_fixup_settings, current_max_sql_file_bytes, False

    try:
        (
            candidate_ob_cfg,
            candidate_fixup_dir,
            _repo_root,
            candidate_log_level,
            candidate_report_dir,
            candidate_fixup_settings,
            candidate_max_sql_file_bytes,
        ) = load_ob_config(runtime.config_path)
        parser = configparser.ConfigParser(interpolation=None)
        parser.read(runtime.config_path, encoding="utf-8")
        candidate_hot = load_fixup_hot_reload_settings(parser)
        candidate_watch_paths = resolve_hot_reload_watch_paths(parser, runtime.config_path)
    except Exception as exc:
        runtime.snapshot = latest_snapshot
        append_fixup_hot_reload_event(
            runtime,
            status="REJECTED",
            stage=f"round-{round_num}",
            changed_files=changed_files,
            changed_keys=[],
            note=f"配置解析失败: {str(exc)[:240]}",
        )
        if runtime.fail_policy == "abort":
            raise ConfigError(f"热加载配置无效（round={round_num}）: {exc}")
        return current_ob_cfg, current_fixup_settings, current_max_sql_file_bytes, False

    immutable_keys: List[str] = []
    for key in ("executable", "host", "port", "user_string", "password"):
        if str(candidate_ob_cfg.get(key, "")) != str(current_ob_cfg.get(key, "")):
            immutable_keys.append(f"OCEANBASE_TARGET.{key}")
    if candidate_fixup_dir.resolve() != current_fixup_dir.resolve():
        immutable_keys.append("SETTINGS.fixup_dir")
    if candidate_report_dir.resolve() != current_report_dir.resolve():
        immutable_keys.append("SETTINGS.report_dir")

    applied_keys: List[str] = []
    next_ob_cfg = dict(current_ob_cfg)
    next_fixup_settings = current_fixup_settings
    next_max_sql_file_bytes = current_max_sql_file_bytes

    current_level = logging.getLogger().level
    candidate_level = resolve_console_log_level(candidate_log_level)
    if candidate_level != current_level:
        set_console_log_level(candidate_level)
        applied_keys.append("SETTINGS.log_level")

    if candidate_ob_cfg.get("timeout") != current_ob_cfg.get("timeout"):
        next_ob_cfg["timeout"] = candidate_ob_cfg.get("timeout")
        applied_keys.append("SETTINGS.fixup_cli_timeout/obclient_timeout")

    if candidate_fixup_settings != current_fixup_settings:
        next_fixup_settings = candidate_fixup_settings
        applied_keys.extend(
            [
                "SETTINGS.fixup_auto_grant",
                "SETTINGS.fixup_auto_grant_types",
                "SETTINGS.fixup_auto_grant_fallback",
                "SETTINGS.fixup_auto_grant_cache_limit",
                "SETTINGS.fixup_exec_mode",
                "SETTINGS.fixup_exec_file_fallback",
            ]
        )

    if candidate_max_sql_file_bytes != current_max_sql_file_bytes:
        next_max_sql_file_bytes = candidate_max_sql_file_bytes
        applied_keys.append("SETTINGS.fixup_max_sql_file_mb")

    if candidate_hot.interval_sec != runtime.interval_sec:
        runtime.interval_sec = candidate_hot.interval_sec
        applied_keys.append("SETTINGS.config_hot_reload_interval_sec")
    if candidate_hot.fail_policy != runtime.fail_policy:
        runtime.fail_policy = candidate_hot.fail_policy
        applied_keys.append("SETTINGS.config_hot_reload_fail_policy")
    if candidate_hot.mode != runtime.mode:
        runtime.mode = candidate_hot.mode
        applied_keys.append("SETTINGS.config_hot_reload_mode")

    if immutable_keys:
        append_fixup_hot_reload_event(
            runtime,
            status="REQUIRES_RESTART",
            stage=f"round-{round_num}",
            changed_files=changed_files,
            changed_keys=immutable_keys,
            note="存在本轮不可热加载项，需重启 run_fixup 生效",
        )
    elif applied_keys:
        append_fixup_hot_reload_event(
            runtime,
            status="APPLIED",
            stage=f"round-{round_num}",
            changed_files=changed_files,
            changed_keys=sorted(set(applied_keys)),
            note="已在轮次边界应用",
        )
    else:
        append_fixup_hot_reload_event(
            runtime,
            status="REQUIRES_RESTART",
            stage=f"round-{round_num}",
            changed_files=changed_files,
            changed_keys=[],
            note="检测到文件变化，但 run_fixup 当前运行态无可热加载项",
        )

    runtime.watch_paths = candidate_watch_paths
    runtime.snapshot = build_watch_snapshot(runtime.watch_paths)
    settings_changed = next_fixup_settings != current_fixup_settings
    return next_ob_cfg, next_fixup_settings, next_max_sql_file_bytes, settings_changed


def load_ob_config(
    config_path: Path,
) -> Tuple[Dict[str, str], Path, Path, str, Path, FixupAutoGrantSettings, Optional[int]]:
    """Load OceanBase connection info and fixup directory from config.ini."""
    parser = configparser.ConfigParser(interpolation=None)
    if not config_path.exists():
        raise ConfigError(f"配置文件不存在: {config_path}")

    parser.read(config_path, encoding="utf-8")
    source_db_mode = (
        parser.get("SETTINGS", "source_db_mode", fallback="oracle").strip().lower() or "oracle"
    )

    if "OCEANBASE_TARGET" not in parser:
        raise ConfigError("配置文件缺少 [OCEANBASE_TARGET] 配置段。")

    ob_section = parser["OCEANBASE_TARGET"]
    required_keys = ["executable", "host", "port", "user_string", "password"]
    missing = [key for key in required_keys if key not in ob_section or not ob_section[key].strip()]
    if missing:
        raise ConfigError(f"[OCEANBASE_TARGET] 缺少必填项: {', '.join(missing)}")

    ob_cfg = {key: ob_section[key].strip() for key in required_keys}
    try:
        port_value = int(ob_cfg["port"])
    except ValueError as exc:
        raise ConfigError(f"端口解析失败: {ob_cfg['port']}") from exc
    if port_value <= 0 or port_value > 65535:
        raise ConfigError(f"端口超出范围: {port_value}")
    ob_cfg["port"] = str(port_value)

    try:
        fixup_raw = parser.get("SETTINGS", "fixup_cli_timeout", fallback="").strip()
        if fixup_raw:
            fixup_timeout = int(fixup_raw)
        else:
            fixup_timeout = parser.getint(
                "SETTINGS", "obclient_timeout", fallback=DEFAULT_FIXUP_TIMEOUT
            )
        if fixup_timeout is None or fixup_timeout < 0:
            fixup_timeout = DEFAULT_FIXUP_TIMEOUT
    except Exception as exc:
        log.warning("fixup 超时解析失败，回退默认值 %s: %s", DEFAULT_FIXUP_TIMEOUT, exc)
        fixup_timeout = DEFAULT_FIXUP_TIMEOUT
    ob_cfg["timeout"] = None if fixup_timeout == 0 else fixup_timeout
    global OBCLIENT_SESSION_QUERY_TIMEOUT_US
    try:
        session_timeout_us = parser.getint(
            "SETTINGS",
            "ob_session_query_timeout_us",
            fallback=DEFAULT_OBCLIENT_SESSION_QUERY_TIMEOUT_US,
        )
        if session_timeout_us < 0:
            session_timeout_us = DEFAULT_OBCLIENT_SESSION_QUERY_TIMEOUT_US
    except Exception as exc:
        log.warning(
            "ob_session_query_timeout_us 解析失败，回退默认值 %s: %s",
            DEFAULT_OBCLIENT_SESSION_QUERY_TIMEOUT_US,
            exc,
        )
        session_timeout_us = DEFAULT_OBCLIENT_SESSION_QUERY_TIMEOUT_US
    OBCLIENT_SESSION_QUERY_TIMEOUT_US = session_timeout_us
    ob_cfg["session_query_timeout_us"] = session_timeout_us
    settings_section = parser["SETTINGS"] if parser.has_section("SETTINGS") else {}
    ob_cfg["progress_log_interval"] = str(
        parse_float_setting(settings_section.get("progress_log_interval", "10"), 10.0, minimum=1.0)
    )
    ob_cfg["slow_sql_warning_sec"] = str(
        parse_float_setting(settings_section.get("slow_sql_warning_sec", "60"), 60.0, minimum=1.0)
    )

    repo_root = config_path.parent.resolve()
    fixup_dir = parser.get("SETTINGS", "fixup_dir", fallback=DEFAULT_FIXUP_DIR).strip()
    fixup_path = (repo_root / fixup_dir).resolve()
    settings_has = parser.has_section("SETTINGS")
    allow_key_missing = (not settings_has) or (
        not parser.has_option("SETTINGS", "fixup_dir_allow_outside_repo")
    )
    force_clean_key_missing = (not settings_has) or (
        not parser.has_option("SETTINGS", "fixup_force_clean")
    )
    if allow_key_missing or force_clean_key_missing:
        missing_keys: List[str] = []
        if allow_key_missing:
            missing_keys.append("fixup_dir_allow_outside_repo")
        if force_clean_key_missing:
            missing_keys.append("fixup_force_clean")
        log.warning(
            "[MIGRATION] 检测到配置未显式设置 %s。v0.9.8.7 默认值已调整，建议在 config.ini 中明确声明。",
            ",".join(missing_keys),
        )
    allow_outside = parse_bool_flag(
        parser.get("SETTINGS", "fixup_dir_allow_outside_repo", fallback="false"), False
    )
    if not allow_outside:
        if fixup_path != repo_root and repo_root not in fixup_path.parents:
            raise ConfigError(
                f"fixup_dir 不允许在项目目录之外: {fixup_path} "
                "(如确需仓外目录，请显式配置 fixup_dir_allow_outside_repo=true)"
            )

    if not fixup_path.exists():
        raise ConfigError(f"修补脚本目录不存在: {fixup_path}")

    if source_db_mode == "oceanbase":
        log.info(
            "source_db_mode=oceanbase：run_fixup 执行语义保持不变；unsupported/、grants_deferred/、cleanup_safe/cleanup_semantic、materialized_view/job/schedule 仍默认不会自动执行，manual-only family 即便显式配合 --iterative 也不会跨轮自动重试。"
        )

    report_dir = (
        parser.get("SETTINGS", "report_dir", fallback="main_reports").strip() or "main_reports"
    )
    report_path = (repo_root / report_dir).resolve()

    log_level = parser.get("SETTINGS", "log_level", fallback="AUTO").strip().upper() or "AUTO"
    auto_grant_enabled = parse_bool_flag(
        parser.get("SETTINGS", "fixup_auto_grant", fallback="true"), True
    )
    auto_grant_types = parse_fixup_auto_grant_types(
        parser.get("SETTINGS", "fixup_auto_grant_types", fallback="")
    )
    auto_grant_fallback = parse_bool_flag(
        parser.get("SETTINGS", "fixup_auto_grant_fallback", fallback="true"), True
    )
    auto_grant_cache_limit = parser.getint(
        "SETTINGS", "fixup_auto_grant_cache_limit", fallback=DEFAULT_FIXUP_AUTO_GRANT_CACHE_LIMIT
    )
    if auto_grant_cache_limit < 0:
        auto_grant_cache_limit = DEFAULT_FIXUP_AUTO_GRANT_CACHE_LIMIT
    exec_mode = normalize_fixup_exec_mode(
        parser.get("SETTINGS", "fixup_exec_mode", fallback=DEFAULT_FIXUP_EXEC_MODE)
    )
    exec_file_fallback = parse_bool_flag(
        parser.get(
            "SETTINGS",
            "fixup_exec_file_fallback",
            fallback="true" if DEFAULT_FIXUP_EXEC_FILE_FALLBACK else "false",
        ),
        DEFAULT_FIXUP_EXEC_FILE_FALLBACK,
    )
    fixup_settings = FixupAutoGrantSettings(
        enabled=auto_grant_enabled,
        types=auto_grant_types,
        fallback=auto_grant_fallback,
        cache_limit=auto_grant_cache_limit,
        exec_mode=exec_mode,
        exec_file_fallback=exec_file_fallback,
    )
    max_sql_mb = parser.getint(
        "SETTINGS", "fixup_max_sql_file_mb", fallback=DEFAULT_FIXUP_MAX_SQL_FILE_MB
    )
    max_sql_bytes = None if max_sql_mb <= 0 else max_sql_mb * 1024 * 1024
    return ob_cfg, fixup_path, repo_root, log_level, report_path, fixup_settings, max_sql_bytes


def build_obclient_command(ob_cfg: Dict[str, str]) -> List[str]:
    """Assemble the obclient command line."""
    defaults_file = (ob_cfg.get("__ob_defaults_file") or "").strip()
    defaults_path: Optional[Path] = Path(defaults_file) if defaults_file else None
    if defaults_path is None or not defaults_path.exists():
        defaults_path = _create_obclient_defaults_file(ob_cfg["password"])
        ob_cfg["__ob_defaults_file"] = str(defaults_path)
    return [
        ob_cfg["executable"],
        f"{OBCLIENT_SECURE_OPT}={defaults_path}",
        "-h",
        ob_cfg["host"],
        "-P",
        ob_cfg["port"],
        "-u",
        ob_cfg["user_string"],
        "--prompt",
        "fixup>",
        "--silent",
    ]


def iter_sql_files_recursive(base_dir: Path) -> List[Path]:
    try:
        return sorted(
            [path for path in base_dir.rglob("*.sql") if path.is_file()], key=lambda p: str(p)
        )
    except OSError:
        return []


def collect_sql_files_by_layer(
    fixup_dir: Path,
    smart_order: bool = False,
    include_dirs: Optional[Set[str]] = None,
    exclude_dirs: Optional[Set[str]] = None,
    glob_patterns: Optional[List[str]] = None,
) -> List[Tuple[int, Path]]:
    """
    Collect SQL files with layer information for dependency-aware execution.

    Returns:
        List of (layer_index, file_path) tuples
    """
    glob_patterns = glob_patterns or ["*.sql"]
    exclude_dirs = exclude_dirs or set()

    subdirs = {
        p.name.lower(): p
        for p in fixup_dir.iterdir()
        if p.is_dir()
        and p.name != DONE_DIR_NAME
        and not path_excluded_by_filters(p.name, exclude_dirs)
    }
    grant_dirs = resolve_grant_dirs(subdirs, include_dirs, exclude_dirs)
    core_grant_dirs = [d for d in grant_dirs if d in CORE_GRANT_DIRS_ORDER]

    files_with_layer: List[Tuple[int, Path]] = []

    if smart_order:
        # Use dependency layers
        for layer_idx, layer_dirs in enumerate(DEPENDENCY_LAYERS):
            for dir_name in layer_dirs:
                if dir_name == "grants":
                    for grant_dir in core_grant_dirs:
                        if grant_dir not in subdirs:
                            continue
                        for sql_file in iter_sql_files_recursive(subdirs[grant_dir]):
                            if not sql_file.is_file():
                                continue
                            rel_str = str(sql_file.relative_to(fixup_dir))
                            if not any(
                                fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p)
                                for p in glob_patterns
                            ):
                                continue
                            files_with_layer.append((layer_idx, sql_file))
                    continue
                if not should_scan_top_dir(dir_name, include_dirs):
                    continue
                if dir_name not in subdirs:
                    continue

                for sql_file in iter_sql_files_recursive(subdirs[dir_name]):
                    if not sql_file.is_file():
                        continue
                    rel_str = str(sql_file.relative_to(fixup_dir))
                    rel_parent = normalize_dir_filter(sql_file.parent.relative_to(fixup_dir))
                    if path_excluded_by_filters(rel_parent, exclude_dirs):
                        continue
                    if not path_selected_by_filters(rel_parent, include_dirs):
                        continue
                    if not any(
                        fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p)
                        for p in glob_patterns
                    ):
                        continue
                    files_with_layer.append((layer_idx, sql_file))

        # Add remaining directories not in DEPENDENCY_LAYERS
        all_layer_dirs = {d for layer in DEPENDENCY_LAYERS for d in layer}
        all_layer_dirs.update(core_grant_dirs)
        for dir_name in sorted(subdirs.keys()):
            if dir_name in all_layer_dirs:
                continue
            if is_grant_dir(dir_name) and dir_name not in grant_dirs:
                continue
            if not should_scan_top_dir(dir_name, include_dirs):
                continue

            for sql_file in iter_sql_files_recursive(subdirs[dir_name]):
                if not sql_file.is_file():
                    continue
                rel_str = str(sql_file.relative_to(fixup_dir))
                rel_parent = normalize_dir_filter(sql_file.parent.relative_to(fixup_dir))
                if path_excluded_by_filters(rel_parent, exclude_dirs):
                    continue
                if not path_selected_by_filters(rel_parent, include_dirs):
                    continue
                if not any(
                    fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p)
                    for p in glob_patterns
                ):
                    continue
                files_with_layer.append((999, sql_file))  # Unknown layer
    else:
        # Keep non-smart execution order aligned with dependency-aware layers.
        priority = [
            "sequence",
            "sequence_restart",
            "table",
            "table_alter",
            "view_prereq_grants",
            "grants",
            "synonym",
            "view_refresh",
            "view",
            "view_post_grants",
            "materialized_view",
            "type",
            "package",
            "procedure",
            "function",
            "type_body",
            "package_body",
            "context",
            "name_collision",
            "constraint",
            "index",
            "trigger",
            "job",
            "schedule",
        ]

        seen = set()
        for idx, name in enumerate(priority):
            if name == "grants":
                for grant_dir in core_grant_dirs:
                    if grant_dir not in subdirs:
                        continue
                    for sql_file in iter_sql_files_recursive(subdirs[grant_dir]):
                        if not sql_file.is_file():
                            continue
                        rel_str = str(sql_file.relative_to(fixup_dir))
                        if not any(
                            fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p)
                            for p in glob_patterns
                        ):
                            continue
                        files_with_layer.append((idx, sql_file))
                    seen.add(grant_dir)
                continue
            if not should_scan_top_dir(name, include_dirs):
                continue
            if name in subdirs:
                for sql_file in iter_sql_files_recursive(subdirs[name]):
                    if not sql_file.is_file():
                        continue
                    rel_str = str(sql_file.relative_to(fixup_dir))
                    rel_parent = normalize_dir_filter(sql_file.parent.relative_to(fixup_dir))
                    if path_excluded_by_filters(rel_parent, exclude_dirs):
                        continue
                    if not path_selected_by_filters(rel_parent, include_dirs):
                        continue
                    if not any(
                        fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p)
                        for p in glob_patterns
                    ):
                        continue
                    files_with_layer.append((idx, sql_file))
                seen.add(name)

        # Remaining directories
        for name in sorted(subdirs.keys()):
            if name in seen:
                continue
            if is_grant_dir(name) and name not in grant_dirs:
                continue
            if not should_scan_top_dir(name, include_dirs):
                continue
            for sql_file in iter_sql_files_recursive(subdirs[name]):
                if not sql_file.is_file():
                    continue
                rel_str = str(sql_file.relative_to(fixup_dir))
                rel_parent = normalize_dir_filter(sql_file.parent.relative_to(fixup_dir))
                if path_excluded_by_filters(rel_parent, exclude_dirs):
                    continue
                if not path_selected_by_filters(rel_parent, include_dirs):
                    continue
                if not any(
                    fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p)
                    for p in glob_patterns
                ):
                    continue
                files_with_layer.append((999, sql_file))

    # Sort by layer, then by path
    files_with_layer.sort(key=lambda x: (x[0], str(x[1])))
    return files_with_layer


def normalize_identifier(raw: str) -> str:
    return raw.strip().strip('"').upper()


def quote_identifier(raw: str) -> str:
    value = (raw or "").strip().strip('"')
    return '"' + value.replace('"', '""') + '"'


def quote_qualified_name(schema: str, name: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(name)}"


def parse_object_token(token: str) -> Tuple[Optional[str], str]:
    raw = token.strip().rstrip(";")
    m = RE_DOUBLE_QUOTED_DOT.search(raw)
    if m:
        return normalize_identifier(m.group(1)), normalize_identifier(m.group(2))
    if "." in raw:
        parts = raw.split(".", 1)
        return normalize_identifier(parts[0]), normalize_identifier(parts[1])
    return None, normalize_identifier(raw)


def parse_object_from_filename(path: Path) -> Tuple[Optional[str], Optional[str]]:
    stem = path.stem
    if "." not in stem:
        return None, None
    schema, name = stem.split(".", 1)
    return normalize_identifier(schema), normalize_identifier(name)


def parse_object_identity_from_path(path: Path) -> Tuple[Optional[str], Optional[str]]:
    return parse_object_from_filename(path)


def normalize_object_type(raw: str) -> str:
    return (raw or "").strip().upper().replace("_", " ")


def parse_bool_flag(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def normalize_fixup_exec_mode(raw_value: Optional[str]) -> str:
    value = (raw_value or DEFAULT_FIXUP_EXEC_MODE).strip().lower()
    if value not in FIXUP_EXEC_MODE_VALUES:
        log.warning(
            "fixup_exec_mode=%s 非法，回退为 %s（支持: auto/file/statement）",
            raw_value,
            DEFAULT_FIXUP_EXEC_MODE,
        )
        return DEFAULT_FIXUP_EXEC_MODE
    return value


def parse_fixup_auto_grant_types(raw_value: str) -> Set[str]:
    if not raw_value or not raw_value.strip():
        return set(DEFAULT_FIXUP_AUTO_GRANT_TYPES)
    if raw_value.strip().lower() in {"all", "*"}:
        return set(FIXUP_AUTO_GRANT_ALLOWED_TYPES)
    parsed = {normalize_object_type(item) for item in raw_value.split(",") if item.strip()}
    unknown = parsed - FIXUP_AUTO_GRANT_ALLOWED_TYPES
    if unknown:
        log.warning("fixup_auto_grant_types 包含未知类型 %s，将忽略。", sorted(unknown))
    return parsed & FIXUP_AUTO_GRANT_ALLOWED_TYPES


def extract_object_from_error(stderr: str) -> Tuple[Optional[str], Optional[str]]:
    if not stderr:
        return None, None
    match = RE_QUOTED_DOT.search(stderr)
    if match:
        schema, name = match.group(1).split(".", 1)
        return normalize_identifier(schema), normalize_identifier(name)
    match = RE_DOUBLE_QUOTED_DOT.search(stderr)
    if match:
        return normalize_identifier(match.group(1)), normalize_identifier(match.group(2))
    match = RE_PLAIN_DOT.search(stderr)
    if match:
        return normalize_identifier(match.group(1)), normalize_identifier(match.group(2))
    match = RE_SINGLE_QUOTED_NAME.search(stderr)
    if match:
        return None, normalize_identifier(match.group(1))
    return None, None


def is_create_view_statement(statement: str) -> bool:
    if not statement:
        return False
    return bool(RE_CREATE_VIEW.match(statement.strip()))


def is_comment_only_statement(statement: str) -> bool:
    if not statement.strip():
        return True
    in_single = False
    in_double = False
    block_comment_depth = 0
    idx = 0
    length = len(statement)
    has_code = False

    while idx < length:
        ch = statement[idx]
        nxt = statement[idx + 1] if idx + 1 < length else ""

        if block_comment_depth > 0:
            if ch == "/" and nxt == "*":
                block_comment_depth += 1
                idx += 2
                continue
            if ch == "*" and nxt == "/":
                block_comment_depth -= 1
                idx += 2
                continue
            idx += 1
            continue

        if not in_single and not in_double:
            if ch == "/" and nxt == "*":
                block_comment_depth = 1
                idx += 2
                continue
            if ch == "-" and nxt == "-":
                while idx < length and statement[idx] != "\n":
                    idx += 1
                continue

        if ch == "'" and not in_double:
            if in_single and nxt == "'":
                has_code = True
                idx += 2
                continue
            in_single = not in_single
            has_code = True
            idx += 1
            continue

        if ch == '"' and not in_single:
            if in_double and nxt == '"':
                has_code = True
                idx += 2
                continue
            in_double = not in_double
            has_code = True
            idx += 1
            continue

        if not ch.isspace():
            has_code = True
        idx += 1

    return not has_code


def strip_leading_sql_comments(statement: str) -> str:
    if not statement:
        return ""
    idx = 0
    length = len(statement)

    while idx < length:
        while idx < length and statement[idx].isspace():
            idx += 1
        if idx >= length:
            return ""
        nxt = statement[idx + 1] if idx + 1 < length else ""
        if statement[idx] == "-" and nxt == "-":
            line_end = statement.find("\n", idx)
            if line_end == -1:
                return ""
            idx = line_end + 1
            continue
        if statement[idx] == "/" and nxt == "*":
            idx += 2
            depth = 1
            while idx < length and depth > 0:
                ch = statement[idx]
                nxt = statement[idx + 1] if idx + 1 < length else ""
                if ch == "/" and nxt == "*":
                    depth += 1
                    idx += 2
                    continue
                if ch == "*" and nxt == "/":
                    depth -= 1
                    idx += 2
                    continue
                idx += 1
            continue
        break
    return statement[idx:]


def extract_execution_error(result: subprocess.CompletedProcess) -> Optional[str]:
    stderr_error = extract_sql_error(result.stderr)
    stdout_error = extract_sql_error(result.stdout)
    if stderr_error and stdout_error:
        stderr_score = score_execution_error_line(stderr_error) or 0
        stdout_score = score_execution_error_line(stdout_error) or 0
        if stdout_score > stderr_score:
            return stdout_error
        return stderr_error
    if stderr_error:
        return stderr_error
    if stdout_error:
        return stdout_error
    if result.returncode != 0:
        combined_lines = [
            line.strip()
            for line in ((result.stderr or "") + "\n" + (result.stdout or "")).splitlines()
            if line.strip()
        ]
        if combined_lines:
            return combined_lines[-1]
        stderr = (result.stderr or "").strip()
        return stderr or "执行失败"
    return None


def parse_error_code(message: str) -> str:
    if not message:
        return "-"
    match = RE_ERROR_CODE.search(message)
    if match:
        return match.group(1).upper()
    return "-"


def parse_grant_object(statement: str) -> str:
    flat = " ".join(strip_leading_sql_comments(statement).split())
    match = RE_GRANT_ON.match(flat)
    if not match:
        return "-"
    schema, name = parse_object_token(match.group("object"))
    if schema:
        return f"{schema}.{name}"
    return name


def infer_error_object(statement: str, relative_path: Path) -> str:
    object_name = parse_grant_object(statement)
    if object_name != "-":
        return object_name
    if is_grant_dir(relative_path.parent.name):
        return "-"
    schema, name = parse_object_from_filename(relative_path)
    if schema and name:
        return f"{schema}.{name}"
    if name:
        return name
    return "-"


def infer_permission_retry_target(
    statement: str,
    relative_path: Path,
) -> Optional[Tuple[str, str, Set[str]]]:
    parsed = parse_grant_statement(statement)
    if not parsed:
        return None
    grant_type, privileges, object_full, _grantees = parsed
    if grant_type != "OBJECT" or not object_full:
        return None
    dir_name = (relative_path.parent.name or "").lower()
    if dir_name == "view_post_grants":
        obj_type = "VIEW"
    elif dir_name == "view_prereq_grants":
        obj_type = "VIEW"
    else:
        return None
    privilege_set = {(item or "").upper() for item in (privileges or ()) if (item or "").strip()}
    if not privilege_set:
        return None
    return normalize_full_name(object_full), obj_type, privilege_set


def find_matching_view_refresh_script(
    fixup_dir: Path, object_full: str, object_type: str
) -> Optional[Path]:
    if normalize_object_type(object_type or "") != "VIEW":
        return None
    schema, name = split_full_name(object_full or "")
    if not schema or not name:
        return None
    candidate = fixup_dir / "view_refresh" / f"{schema.upper()}.{name.upper()}.sql"
    return candidate if candidate.exists() else None


def execute_view_refresh_before_retry(
    *,
    fixup_dir: Path,
    object_full: str,
    object_type: str,
    obclient_cmd: List[str],
    done_dir: Path,
    timeout: Optional[int],
    layer: int,
    label: str,
    max_sql_file_bytes: Optional[int],
    state_ledger: Optional["FixupStateLedger"],
    exec_mode: str,
    exec_file_fallback: bool,
    exec_stats: Dict[str, int],
) -> Optional["ScriptResult"]:
    refresh_path = find_matching_view_refresh_script(fixup_dir, object_full, object_type)
    if not refresh_path:
        return None
    log.info("%s [VIEW_REFRESH] 先执行 %s，再重试 VIEW 授权。", label, refresh_path)
    result, _summary = execute_script_with_summary(
        obclient_cmd,
        refresh_path,
        fixup_dir,
        done_dir,
        timeout,
        layer,
        f"{label} (view_refresh)",
        max_sql_file_bytes,
        state_ledger=state_ledger,
        exec_mode=resolve_script_exec_mode(exec_mode, refresh_path),
        exec_file_fallback=exec_file_fallback,
        exec_stats=exec_stats,
    )
    return result


def grant_statement_has_option(statement: str) -> bool:
    if not statement:
        return False
    return "WITH GRANT OPTION" in statement.upper()


def format_privilege_label(privilege: str, grant_option: bool) -> str:
    if not grant_option:
        return privilege
    return f"{privilege} WITH GRANT OPTION"


def requires_grant_option(
    grantee: str,
    target_full: str,
    target_type: str,
    dependent_type: Optional[str] = None,
) -> bool:
    if not grantee or not target_full or not target_type:
        return False
    target_schema, _ = split_full_name(target_full)
    if not target_schema:
        return False
    if target_schema.upper() == grantee.upper():
        return False
    dep_type_u = (dependent_type or "").upper()
    target_type_u = target_type.upper()
    if dep_type_u in {"VIEW", "MATERIALIZED VIEW"} and target_type_u in {
        "TABLE",
        "VIEW",
        "MATERIALIZED VIEW",
    }:
        return True
    return target_type_u in GRANT_OPTION_TYPES


def parse_chain_node(token: str) -> Optional[Tuple[str, str]]:
    parsed = parse_chain_node_meta(token)
    if not parsed:
        return None
    return parsed[0], parsed[1]


def parse_chain_node_meta(token: str) -> Optional[Tuple[str, str, Tuple[str, ...]]]:
    match = RE_CHAIN_NODE.search(token or "")
    if not match:
        return None
    raw_name = (match.group("name") or "").strip()
    raw_meta = (match.group("meta") or "").strip()
    if not raw_name or not raw_meta:
        return None
    meta_parts = [p.strip().upper() for p in raw_meta.split("|") if p.strip()]
    if not meta_parts:
        return None
    obj_type = meta_parts[0]
    extra_meta = tuple(meta_parts[1:])
    name = normalize_identifier(raw_name)
    if not name or not obj_type:
        return None
    return name, obj_type, extra_meta


def parse_view_chain_line_meta(line: str) -> Optional[List[Tuple[str, str, Tuple[str, ...]]]]:
    if not line:
        return None
    stripped = line.strip()
    if (
        not stripped
        or stripped.startswith("#")
        or stripped.startswith("[")
        or stripped.startswith("-")
    ):
        return None
    if ". " in stripped:
        prefix, rest = stripped.split(". ", 1)
        if prefix.isdigit():
            stripped = rest.strip()
    tokens = [t.strip() for t in stripped.split("->") if t.strip()]
    if not tokens:
        return None
    nodes: List[Tuple[str, str, Tuple[str, ...]]] = []
    for token in tokens:
        node = parse_chain_node_meta(token)
        if not node:
            return None
        nodes.append(node)
    return nodes


def parse_view_chain_file_meta(
    path: Path,
) -> Dict[str, List[List[Tuple[str, str, Tuple[str, ...]]]]]:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {}
    chains_by_view: Dict[str, List[List[Tuple[str, str, Tuple[str, ...]]]]] = defaultdict(list)
    for line in lines:
        nodes = parse_view_chain_line_meta(line)
        if not nodes:
            continue
        root = nodes[0][0]
        chains_by_view[root].append(nodes)
    return dict(chains_by_view)


def parse_view_chain_line(line: str) -> Optional[List[Tuple[str, str]]]:
    if not line:
        return None
    stripped = line.strip()
    if (
        not stripped
        or stripped.startswith("#")
        or stripped.startswith("[")
        or stripped.startswith("-")
    ):
        return None
    if ". " in stripped:
        prefix, rest = stripped.split(". ", 1)
        if prefix.isdigit():
            stripped = rest.strip()
    tokens = [t.strip() for t in stripped.split("->") if t.strip()]
    if not tokens:
        return None
    nodes: List[Tuple[str, str]] = []
    for token in tokens:
        node = parse_chain_node(token)
        if not node:
            return None
        nodes.append(node)
    return nodes


def parse_view_chain_lines(lines: List[str]) -> Dict[str, List[List[Tuple[str, str]]]]:
    chains_by_view: Dict[str, List[List[Tuple[str, str]]]] = defaultdict(list)
    for line in lines:
        nodes = parse_view_chain_line(line)
        if not nodes:
            continue
        root = nodes[0][0]
        chains_by_view[root].append(nodes)
    return dict(chains_by_view)


def parse_view_chain_file(path: Path) -> Dict[str, List[List[Tuple[str, str]]]]:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {}
    return parse_view_chain_lines(lines)


def object_type_to_dir(obj_type: str) -> Optional[str]:
    if not obj_type:
        return None
    key = obj_type.upper().replace(" ", "_")
    return TYPE_DIR_MAP.get(key)


def select_fixup_script_for_node(
    node: Tuple[str, str],
    object_index: Dict[Tuple[str, str], List[Path]],
    name_index: Dict[str, List[Path]],
) -> Optional[Path]:
    full_name, obj_type = node
    schema, name = parse_object_token(full_name)
    if schema:
        candidates = object_index.get((schema, name), [])
        if candidates:
            dir_name = object_type_to_dir(obj_type)
            if dir_name:
                typed = [p for p in candidates if p.parent.name.lower() == dir_name]
                if typed:
                    return typed[0]
            return candidates[0]
        return None
    candidates = name_index.get(name, [])
    if len(candidates) == 1:
        return candidates[0]
    return None


def select_fixup_script_for_node_with_fallback(
    node: Tuple[str, str],
    object_index: Dict[Tuple[str, str], List[Path]],
    name_index: Dict[str, List[Path]],
    fallback_object_index: Dict[Tuple[str, str], List[Path]],
    fallback_name_index: Dict[str, List[Path]],
) -> Tuple[Optional[Path], Optional[str]]:
    primary = select_fixup_script_for_node(node, object_index, name_index)
    if primary:
        return primary, "fixup"
    fallback = select_fixup_script_for_node(node, fallback_object_index, fallback_name_index)
    if fallback:
        return fallback, "done"
    return None, None


def build_view_dependency_graph(
    chains: List[List[Tuple[str, str]]],
) -> Tuple[Set[Tuple[str, str]], Dict[Tuple[str, str], Set[Tuple[str, str]]]]:
    """Build a dependency graph where edges point from dependent VIEW to referenced object.

    For a chain A.V1 -> B.V2 -> C.T1, the graph stores:
      A.V1 -> B.V2
      B.V2 -> C.T1
    This orientation matches topo_sort_nodes(), which walks references first so prerequisites appear earlier.
    """
    nodes: Set[Tuple[str, str]] = set()
    edges: Dict[Tuple[str, str], Set[Tuple[str, str]]] = defaultdict(set)
    for chain in chains:
        for idx, node in enumerate(chain):
            nodes.add(node)
            if idx + 1 < len(chain):
                edges[node].add(chain[idx + 1])
    return nodes, edges


def topo_sort_nodes(
    nodes: Set[Tuple[str, str]], edges: Dict[Tuple[str, str], Set[Tuple[str, str]]]
) -> Tuple[List[Tuple[str, str]], List[List[Tuple[str, str]]]]:
    order: List[Tuple[str, str]] = []
    cycles: List[List[Tuple[str, str]]] = []
    visited: Set[Tuple[str, str]] = set()
    for node in sorted(nodes):
        if node in visited:
            continue
        visiting: Set[Tuple[str, str]] = {node}
        stack: List[Tuple[Tuple[str, str], Any, List[Tuple[str, str]]]] = [
            (node, iter(sorted(edges.get(node, set()))), [node])
        ]
        while stack:
            current, refs_iter, path = stack[-1]
            try:
                ref = next(refs_iter)
            except StopIteration:
                stack.pop()
                visiting.discard(current)
                if current not in visited:
                    visited.add(current)
                    order.append(current)
                continue

            if ref in visited:
                continue
            if ref in visiting:
                cycle_start = path.index(ref) if ref in path else 0
                cycles.append(path[cycle_start:] + [ref])
                continue

            visiting.add(ref)
            stack.append((ref, iter(sorted(edges.get(ref, set()))), path + [ref]))
    return order, cycles


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def split_full_name(full_name: str) -> Tuple[Optional[str], Optional[str]]:
    if not full_name or "." not in full_name:
        return None, None
    parts = full_name.split(".", 1)
    return parts[0].strip().upper(), parts[1].strip().upper()


def normalize_full_name(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if "." in value:
        schema, name = value.split(".", 1)
        return f"{normalize_identifier(schema)}.{normalize_identifier(name)}"
    return normalize_identifier(value)


def find_latest_report_file(report_dir: Path, prefix: str) -> Optional[Path]:
    if not report_dir:
        return None
    candidates: List[Path] = []
    ts_re = re.compile(rf"{re.escape(prefix)}_(\d{{8}}_\d{{6}})")
    run_ts_re = re.compile(r"run_(\d{8}_\d{6})")
    try:
        run_dirs = [p for p in report_dir.glob("run_*") if p.is_dir()]
    except OSError:
        run_dirs = []
    # Prefer newest run directory if present.
    run_dirs.sort(
        key=lambda p: run_ts_re.search(p.name).group(1) if run_ts_re.search(p.name) else "",
        reverse=True,
    )
    for run_dir in run_dirs:
        try:
            run_candidates = list(run_dir.glob(f"{prefix}_*.txt"))
        except OSError:
            continue
        if run_candidates:
            candidates.extend(run_candidates)
            break
    if not candidates:
        try:
            candidates.extend(report_dir.glob(f"{prefix}_*.txt"))
            candidates.extend(report_dir.glob(f"run_*/{prefix}_*.txt"))
        except OSError:
            return None
    if not candidates:
        parent = report_dir.parent if report_dir.parent != report_dir else None
        if parent:
            try:
                candidates.extend(parent.glob(f"{prefix}_*.txt"))
                candidates.extend(parent.glob(f"run_*/{prefix}_*.txt"))
            except OSError:
                pass
    if not candidates:
        try:
            candidates.extend(report_dir.rglob(f"{prefix}_*.txt"))
        except OSError:
            return None
    if not candidates:
        return None

    def sort_key(path: Path) -> Tuple[int, str]:
        match = ts_re.search(path.name)
        if match:
            return (1, match.group(1))
        try:
            return (0, f"{path.stat().st_mtime:020.6f}")
        except OSError:
            return (0, "0")

    candidates.sort(key=sort_key)
    return candidates[-1]


def parse_dependency_chains_file(path: Path) -> Dict[Tuple[str, str], Set[Tuple[str, str]]]:
    deps: Dict[Tuple[str, str], Set[Tuple[str, str]]] = defaultdict(set)
    if not path:
        return deps
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return deps
    section = ""
    line_re = re.compile(
        r"^\s*(?:\d+\.)?\s*(?P<dep>[^()]+)\((?P<dep_type>[^)]+)\)\s*->\s*(?P<ref>[^()]+)\((?P<ref_type>[^)]+)\)"
    )
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("["):
            section = "target" if line.upper().startswith("[TARGET") else "source"
            continue
        if section != "target":
            continue
        match = line_re.match(line)
        if not match:
            continue
        dep_full = normalize_full_name(match.group("dep"))
        ref_full = normalize_full_name(match.group("ref"))
        dep_type = normalize_object_type(match.group("dep_type"))
        ref_type = normalize_object_type(match.group("ref_type"))
        if not dep_full or not ref_full or not dep_type or not ref_type:
            continue
        deps[(dep_full, dep_type)].add((ref_full, ref_type))
    return deps


def build_dependencies_from_view_chains(
    chains_by_view: Dict[str, List[List[Tuple[str, str]]]],
) -> Dict[Tuple[str, str], Set[Tuple[str, str]]]:
    deps: Dict[Tuple[str, str], Set[Tuple[str, str]]] = defaultdict(set)
    for chains in chains_by_view.values():
        for chain in chains:
            if len(chain) < 2:
                continue
            for idx in range(len(chain) - 1):
                dep_name, dep_type = chain[idx]
                ref_name, ref_type = chain[idx + 1]
                dep_full = normalize_full_name(dep_name)
                ref_full = normalize_full_name(ref_name)
                dep_type_u = normalize_object_type(dep_type)
                ref_type_u = normalize_object_type(ref_type)
                if dep_full and ref_full and dep_type_u and ref_type_u:
                    deps[(dep_full, dep_type_u)].add((ref_full, ref_type_u))
    return deps


def init_auto_grant_context(
    fixup_settings: FixupAutoGrantSettings,
    report_dir: Path,
    fixup_dir: Path,
    exclude_dirs: List[str],
    obclient_cmd: List[str],
    timeout: Optional[int],
) -> Optional[AutoGrantContext]:
    if not fixup_settings.enabled:
        return None
    dep_file = find_latest_report_file(report_dir, "dependency_chains")
    dep_map = parse_dependency_chains_file(dep_file) if dep_file else {}
    view_chain_file = find_latest_view_chain_file(report_dir)
    if view_chain_file:
        view_chains = parse_view_chain_file(view_chain_file)
        view_deps = build_dependencies_from_view_chains(view_chains)
        for key, refs in view_deps.items():
            dep_map.setdefault(key, set()).update(refs)
    if not dep_map:
        log.warning(
            "[AUTO-GRANT] 未找到 dependency_chains/VIEWs_chain，自动补权限跳过 (report_dir=%s).",
            report_dir,
        )
        return None
    grant_index_miss = build_grant_index(fixup_dir, set(exclude_dirs), include_dirs={"grants_miss"})
    grant_index_all = build_grant_index(fixup_dir, set(exclude_dirs), include_dirs={"grants_all"})
    stats = AutoGrantStats()
    ctx = AutoGrantContext(
        settings=fixup_settings,
        deps_by_object=dep_map,
        grant_index_miss=grant_index_miss,
        grant_index_all=grant_index_all,
        obclient_cmd=obclient_cmd,
        timeout=timeout,
        roles_cache=LimitedCache(fixup_settings.cache_limit),
        tab_privs_cache=LimitedCache(fixup_settings.cache_limit),
        tab_privs_grantable_cache=LimitedCache(fixup_settings.cache_limit),
        sys_privs_cache=LimitedCache(fixup_settings.cache_limit),
        planned_statements=set(),
        planned_object_privs=set(),
        planned_object_privs_with_option=set(),
        planned_sys_privs=set(),
        applied_grants=set(),
        blocked_objects=set(),
        stats=stats,
    )
    log.info(
        "[AUTO-GRANT] 启用: types=%s fallback=%s cache_limit=%d deps=%d",
        ",".join(sorted(fixup_settings.types)),
        "true" if fixup_settings.fallback else "false",
        fixup_settings.cache_limit,
        sum(len(v) for v in dep_map.values()),
    )
    return ctx


def build_auto_grant_plan_for_object(
    ctx: AutoGrantContext,
    obj_full: str,
    obj_type: str,
    required_privileges_override: Optional[Set[str]] = None,
) -> Tuple[List[str], List[str], bool]:
    obj_full_u = normalize_full_name(obj_full)
    obj_type_u = normalize_object_type(obj_type)
    deps = ctx.deps_by_object.get((obj_full_u, obj_type_u), set())
    if not deps:
        return [], [], False
    plan_lines: List[str] = []
    sql_lines: List[str] = []
    blocked = False
    grantee_schema, _ = split_full_name(obj_full_u)
    if not grantee_schema or grantee_schema.upper() in AUTO_GRANT_SYSTEM_SCHEMAS:
        return [], [], False
    for ref_full, ref_type in sorted(deps):
        ref_full_u = normalize_full_name(ref_full)
        ref_type_u = normalize_object_type(ref_type)
        required_privs: List[str] = []
        if (
            required_privileges_override
            and obj_type_u in {"VIEW", "MATERIALIZED VIEW"}
            and ref_type_u in {"TABLE", "VIEW", "MATERIALIZED VIEW"}
        ):
            required_privs = sorted(
                {
                    (item or "").upper()
                    for item in required_privileges_override
                    if (item or "").strip()
                }
            )
        else:
            required_priv = GRANT_PRIVILEGE_BY_TYPE.get(ref_type_u)
            if required_priv:
                required_privs = [required_priv]
        if not required_privs:
            continue
        ref_schema, _ = split_full_name(ref_full_u)
        if not ref_schema or ref_schema.upper() == grantee_schema.upper():
            continue
        require_option = requires_grant_option(grantee_schema, ref_full_u, ref_type_u, obj_type_u)
        for required_priv in required_privs:
            blocked = (
                plan_object_grant_for_dependency(
                    grantee_schema,
                    ref_full_u,
                    ref_type_u,
                    required_priv,
                    require_option,
                    ctx.settings.fallback,
                    ctx.obclient_cmd,
                    ctx.timeout,
                    ctx.grant_index_miss,
                    ctx.grant_index_all,
                    ctx.roles_cache,
                    ctx.tab_privs_cache,
                    ctx.tab_privs_grantable_cache,
                    ctx.sys_privs_cache,
                    ctx.planned_statements,
                    ctx.planned_object_privs,
                    ctx.planned_object_privs_with_option,
                    ctx.planned_sys_privs,
                    plan_lines,
                    sql_lines,
                )
                or blocked
            )
    return plan_lines, sql_lines, blocked


def execute_auto_grant_for_object(
    ctx: AutoGrantContext,
    obj_full: str,
    obj_type: str,
    label: str,
    required_privileges_override: Optional[Set[str]] = None,
) -> Tuple[int, bool]:
    if not ctx.settings.enabled:
        return 0, False
    obj_full_u = normalize_full_name(obj_full)
    obj_type_u = normalize_object_type(obj_type)
    if obj_type_u not in ctx.settings.types:
        ctx.stats.skipped += 1
        return 0, False
    obj_key = (obj_full_u, obj_type_u)
    if obj_key in ctx.blocked_objects:
        ctx.stats.skipped += 1
        log.info("%s [AUTO-GRANT] %s(%s) 已阻断，跳过重复规划。", label, obj_full_u, obj_type_u)
        return 0, True
    plan_lines, sql_lines, blocked = build_auto_grant_plan_for_object(
        ctx,
        obj_full_u,
        obj_type_u,
        required_privileges_override=required_privileges_override,
    )
    if blocked:
        ctx.stats.blocked += 1
        if not sql_lines:
            ctx.blocked_objects.add(obj_key)
    if not sql_lines:
        return 0, blocked
    ctx.stats.planned += len(sql_lines)
    sql_text = "\n".join(line for line in sql_lines if not line.lstrip().startswith("--")).strip()
    if not sql_text:
        return 0, blocked
    summary = execute_sql_statements(ctx.obclient_cmd, sql_text, ctx.timeout)
    if summary.failures:
        ctx.stats.failed += len(summary.failures)
        log.warning(
            "%s [AUTO-GRANT] %s(%s) 授权失败 %d/%d",
            label,
            obj_full,
            obj_type_u,
            len(summary.failures),
            summary.statements,
        )
    else:
        ctx.stats.executed += summary.statements
        log.info(
            "%s [AUTO-GRANT] %s(%s) 授权成功 %d", label, obj_full, obj_type_u, summary.statements
        )
    return summary.statements, blocked


def reset_auto_grant_round_cache(ctx: Optional[AutoGrantContext], round_num: int) -> int:
    if not ctx:
        return 0
    cleared = len(ctx.blocked_objects)
    cache_entries = (
        len(ctx.roles_cache)
        + len(ctx.tab_privs_cache)
        + len(ctx.tab_privs_grantable_cache)
        + len(ctx.sys_privs_cache)
    )
    if cleared:
        ctx.blocked_objects.clear()
        log.info("[AUTO-GRANT] 第 %d 轮开始，已清理上一轮阻断缓存 %d 项。", round_num, cleared)
    if cache_entries:
        ctx.roles_cache.clear()
        ctx.tab_privs_cache.clear()
        ctx.tab_privs_grantable_cache.clear()
        ctx.sys_privs_cache.clear()
        log.info("[AUTO-GRANT] 第 %d 轮开始，已清理权限查询缓存 %d 项。", round_num, cache_entries)
    return cleared


def find_latest_view_chain_file(report_dir: Path) -> Optional[Path]:
    return find_latest_report_file(report_dir, "VIEWs_chain")


def check_object_exists(
    obclient_cmd: List[str],
    timeout: Optional[int],
    full_name: str,
    obj_type: str,
    exists_cache: Dict[Tuple[str, str], bool],
    planned_objects: Set[Tuple[str, str]],
    use_planned: bool = True,
) -> Optional[bool]:
    key = (full_name.upper(), obj_type.upper())
    if use_planned and key in planned_objects:
        return True
    if key in exists_cache:
        return exists_cache[key]
    schema, name = split_full_name(full_name)
    if not schema or not name:
        return None
    sql = (
        "SELECT COUNT(*) FROM DBA_OBJECTS "
        f"WHERE OWNER='{escape_sql_literal(schema)}' "
        f"AND OBJECT_NAME='{escape_sql_literal(name)}' "
        f"AND OBJECT_TYPE='{escape_sql_literal(obj_type.upper())}'"
    )
    count = query_count(obclient_cmd, sql, timeout)
    if count is None:
        return None
    exists = count > 0
    exists_cache[key] = exists
    return exists


def invalidate_exists_cache(
    exists_cache: Dict[Tuple[str, str], bool], planned_objects: Set[Tuple[str, str]]
) -> int:
    removed = 0
    for full_name, obj_type in planned_objects:
        # Use the same key format as check_object_exists: (full_name.upper(), obj_type.upper())
        key = (full_name.upper(), obj_type.upper())
        if key in exists_cache:
            exists_cache.pop(key, None)
            removed += 1
    return removed


def load_roles_for_grantee(
    obclient_cmd: List[str], timeout: Optional[int], grantee: str, roles_cache: Dict[str, Set[str]]
) -> Set[str]:
    grantee_u = (grantee or "").upper()
    if not grantee_u:
        return set()
    cached = roles_cache.get(grantee_u)
    if cached is not None:
        return cached
    sql = f"SELECT GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE='{escape_sql_literal(grantee_u)}'"
    roles = query_single_column(obclient_cmd, sql, timeout, "GRANTED_ROLE")
    roles_cache[grantee_u] = roles
    return roles


def load_tab_privs_for_identity(
    obclient_cmd: List[str],
    timeout: Optional[int],
    identity: str,
    owner: str,
    name: str,
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]],
) -> Set[str]:
    key = (identity.upper(), owner.upper(), name.upper())
    if key in tab_privs_cache:
        return tab_privs_cache[key]
    sql = (
        "SELECT PRIVILEGE FROM DBA_TAB_PRIVS "
        f"WHERE GRANTEE='{escape_sql_literal(identity.upper())}' "
        f"AND OWNER='{escape_sql_literal(owner.upper())}' "
        f"AND TABLE_NAME='{escape_sql_literal(name.upper())}'"
    )
    privs = query_single_column(obclient_cmd, sql, timeout, "PRIVILEGE")
    tab_privs_cache[key] = privs
    return privs


def load_grantable_tab_privs_for_identity(
    obclient_cmd: List[str],
    timeout: Optional[int],
    identity: str,
    owner: str,
    name: str,
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]],
) -> Set[str]:
    key = (identity.upper(), owner.upper(), name.upper())
    if key in tab_privs_grantable_cache:
        return tab_privs_grantable_cache[key]
    sql = (
        "SELECT PRIVILEGE FROM DBA_TAB_PRIVS "
        f"WHERE GRANTEE='{escape_sql_literal(identity.upper())}' "
        f"AND OWNER='{escape_sql_literal(owner.upper())}' "
        f"AND TABLE_NAME='{escape_sql_literal(name.upper())}' "
        "AND GRANTABLE='YES'"
    )
    privs = query_single_column(obclient_cmd, sql, timeout, "PRIVILEGE")
    tab_privs_grantable_cache[key] = privs
    return privs


def load_sys_privs_for_identity(
    obclient_cmd: List[str],
    timeout: Optional[int],
    identity: str,
    sys_privs_cache: Dict[str, Set[str]],
) -> Set[str]:
    identity_u = (identity or "").upper()
    if not identity_u:
        return set()
    cached = sys_privs_cache.get(identity_u)
    if cached is not None:
        return cached
    sql = f"SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE='{escape_sql_literal(identity_u)}'"
    privs = query_single_column(obclient_cmd, sql, timeout, "PRIVILEGE")
    sys_privs_cache[identity_u] = privs
    return privs


def has_required_privilege(
    obclient_cmd: List[str],
    timeout: Optional[int],
    grantee: str,
    ref_full: str,
    target_type: str,
    required_priv: str,
    roles_cache: Dict[str, Set[str]],
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]],
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]],
    sys_privs_cache: Dict[str, Set[str]],
    planned_object_privs: Set[Tuple[str, str, str]],
    planned_object_privs_with_option: Set[Tuple[str, str, str]],
    planned_sys_privs: Set[Tuple[str, str]],
    require_grant_option: bool = False,
) -> bool:
    grantee_u = (grantee or "").upper()
    required_u = (required_priv or "").upper()
    ref_schema, ref_name = split_full_name(ref_full)
    if not grantee_u or not ref_schema or not ref_name or not required_u:
        return False
    if grantee_u == ref_schema:
        return True
    if require_grant_option:
        if (grantee_u, required_u, ref_full.upper()) in planned_object_privs_with_option:
            return True
        grantable_privs = load_grantable_tab_privs_for_identity(
            obclient_cmd, timeout, grantee_u, ref_schema, ref_name, tab_privs_grantable_cache
        )
        return required_u in grantable_privs

    if (grantee_u, required_u, ref_full.upper()) in planned_object_privs:
        return True
    if (grantee_u, required_u, ref_full.upper()) in planned_object_privs_with_option:
        return True
    implied = resolve_implied_sys_privileges(required_u, ref_full, target_type)
    if implied and any((grantee_u, p) in planned_sys_privs for p in implied):
        return True

    direct_privs = load_tab_privs_for_identity(
        obclient_cmd, timeout, grantee_u, ref_schema, ref_name, tab_privs_cache
    )
    if required_u in direct_privs:
        return True

    roles = load_roles_for_grantee(obclient_cmd, timeout, grantee_u, roles_cache)
    for role in roles:
        role_privs = load_tab_privs_for_identity(
            obclient_cmd, timeout, role, ref_schema, ref_name, tab_privs_cache
        )
        if required_u in role_privs:
            return True

    if implied:
        grantee_sys = load_sys_privs_for_identity(obclient_cmd, timeout, grantee_u, sys_privs_cache)
        if any(priv in grantee_sys for priv in implied):
            return True
        for role in roles:
            role_sys = load_sys_privs_for_identity(obclient_cmd, timeout, role, sys_privs_cache)
            if any(priv in role_sys for priv in implied):
                return True

    return False


def resolve_synonym_target_map(
    edges: Dict[Tuple[str, str], Set[Tuple[str, str]]],
) -> Dict[Tuple[str, str], Tuple[str, str]]:
    synonym_targets: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for dep, refs in edges.items():
        if dep[1] != "SYNONYM":
            continue
        if len(refs) == 1:
            synonym_targets[dep] = next(iter(refs))
    return synonym_targets


def plan_object_grant_for_dependency(
    grantee: str,
    target_full: str,
    target_type: str,
    required_priv: str,
    require_grant_option: bool,
    allow_fallback: bool,
    obclient_cmd: List[str],
    timeout: Optional[int],
    grant_index_miss: GrantIndex,
    grant_index_all: GrantIndex,
    roles_cache: Dict[str, Set[str]],
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]],
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]],
    sys_privs_cache: Dict[str, Set[str]],
    planned_statements: Set[str],
    planned_object_privs: Set[Tuple[str, str, str]],
    planned_object_privs_with_option: Set[Tuple[str, str, str]],
    planned_sys_privs: Set[Tuple[str, str]],
    plan_lines: List[str],
    sql_lines: List[str],
) -> bool:
    priv_label = format_privilege_label(required_priv, require_grant_option)
    if has_required_privilege(
        obclient_cmd,
        timeout,
        grantee,
        target_full,
        target_type,
        required_priv,
        roles_cache,
        tab_privs_cache,
        tab_privs_grantable_cache,
        sys_privs_cache,
        planned_object_privs,
        planned_object_privs_with_option,
        planned_sys_privs,
        require_grant_option=require_grant_option,
    ):
        plan_lines.append(f"GRANT OK: {grantee} has {priv_label} on {target_full}")
        return False

    entries, source_label = find_grant_entries_by_priority(
        grantee,
        target_full,
        target_type,
        required_priv,
        grant_index_miss,
        grant_index_all,
        require_grant_option=require_grant_option,
    )
    if not entries:
        if allow_fallback:
            auto_stmt = build_auto_grant_statement(
                grantee, target_full, required_priv, with_grant_option=require_grant_option
            )
            if auto_stmt:
                stmt_key = normalize_statement_key(auto_stmt)
                if stmt_key not in planned_statements:
                    planned_statements.add(stmt_key)
                    plan_lines.append(f"GRANT AUTO: {priv_label} on {target_full} to {grantee}")
                    sql_lines.append("-- SOURCE: auto-generated")
                    sql_lines.append(auto_stmt.rstrip().rstrip(";") + ";")
                    planned_object_privs.add(
                        (grantee.upper(), required_priv.upper(), target_full.upper())
                    )
                    if require_grant_option:
                        planned_object_privs_with_option.add(
                            (grantee.upper(), required_priv.upper(), target_full.upper())
                        )
                return False
        plan_lines.append(f"BLOCK: 缺少 GRANT {priv_label} on {target_full} to {grantee}")
        return True

    for entry in entries:
        stmt_key = normalize_statement_key(entry.statement)
        if stmt_key in planned_statements:
            continue
        planned_statements.add(stmt_key)
        plan_lines.append(
            f"GRANT ADD: {priv_label} on {target_full} to {grantee} ({source_label}/{entry.source_path.name})"
        )
        sql_lines.append(f"-- SOURCE: {entry.source_path}")
        sql_lines.append(entry.statement.rstrip().rstrip(";") + ";")
        if entry.grant_type == "OBJECT" and entry.object_name:
            has_option = require_grant_option or grant_statement_has_option(entry.statement)
            for priv in entry.privileges:
                planned_object_privs.add((entry.grantee, priv, entry.object_name.upper()))
                if has_option:
                    planned_object_privs_with_option.add(
                        (entry.grantee, priv, entry.object_name.upper())
                    )
        elif entry.grant_type == "SYSTEM":
            for priv in entry.privileges:
                planned_sys_privs.add((entry.grantee, priv))

    return False


def ensure_view_owner_grant_option(
    view_full: str,
    view_type: str,
    edges: Dict[Tuple[str, str], Set[Tuple[str, str]]],
    synonym_targets: Dict[Tuple[str, str], Tuple[str, str]],
    obclient_cmd: List[str],
    timeout: Optional[int],
    grant_index_miss: GrantIndex,
    grant_index_all: GrantIndex,
    allow_fallback: bool,
    roles_cache: Dict[str, Set[str]],
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]],
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]],
    sys_privs_cache: Dict[str, Set[str]],
    planned_statements: Set[str],
    planned_object_privs: Set[Tuple[str, str, str]],
    planned_object_privs_with_option: Set[Tuple[str, str, str]],
    planned_sys_privs: Set[Tuple[str, str]],
    plan_lines: List[str],
    sql_lines: List[str],
    visited_views: Set[Tuple[str, str]],
) -> bool:
    node = (normalize_identifier(view_full), view_type.upper())
    if node in visited_views:
        return False
    visited_views.add(node)

    view_schema, _ = split_full_name(view_full)
    if not view_schema:
        plan_lines.append(f"BLOCK: 无法解析视图 owner for {view_full}")
        return True

    blocked = False
    refs = sorted(edges.get(node, set()))
    for ref in refs:
        ref_full, ref_type = ref
        target_full, target_type = ref_full, ref_type
        if ref_type == "SYNONYM" and ref in synonym_targets:
            target_full, target_type = synonym_targets[ref]
        required_priv = GRANT_PRIVILEGE_BY_TYPE.get(target_type.upper())
        if not required_priv:
            continue
        target_schema, _ = split_full_name(target_full)
        if not target_schema or target_schema == view_schema:
            continue
        blocked = (
            plan_object_grant_for_dependency(
                view_schema,
                target_full,
                target_type,
                required_priv,
                True,
                allow_fallback,
                obclient_cmd,
                timeout,
                grant_index_miss,
                grant_index_all,
                roles_cache,
                tab_privs_cache,
                tab_privs_grantable_cache,
                sys_privs_cache,
                planned_statements,
                planned_object_privs,
                planned_object_privs_with_option,
                planned_sys_privs,
                plan_lines,
                sql_lines,
            )
            or blocked
        )

        if target_type.upper() in GRANT_OPTION_TYPES:
            blocked = (
                ensure_view_owner_grant_option(
                    target_full,
                    target_type,
                    edges,
                    synonym_targets,
                    obclient_cmd,
                    timeout,
                    grant_index_miss,
                    grant_index_all,
                    allow_fallback,
                    roles_cache,
                    tab_privs_cache,
                    tab_privs_grantable_cache,
                    sys_privs_cache,
                    planned_statements,
                    planned_object_privs,
                    planned_object_privs_with_option,
                    planned_sys_privs,
                    plan_lines,
                    sql_lines,
                    visited_views,
                )
                or blocked
            )

    return blocked


def build_view_chain_plan(
    view_full: str,
    chains: List[List[Tuple[str, str]]],
    obclient_cmd: List[str],
    timeout: Optional[int],
    object_index: Dict[Tuple[str, str], List[Path]],
    name_index: Dict[str, List[Path]],
    done_object_index: Dict[Tuple[str, str], List[Path]],
    done_name_index: Dict[str, List[Path]],
    grant_index_miss: GrantIndex,
    grant_index_all: GrantIndex,
    allow_fallback: bool,
    repo_root: Path,
    exists_cache: Dict[Tuple[str, str], bool],
    roles_cache: Dict[str, Set[str]],
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]],
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]],
    sys_privs_cache: Dict[str, Set[str]],
    planned_statements: Set[str],
    planned_object_privs: Set[Tuple[str, str, str]],
    planned_object_privs_with_option: Set[Tuple[str, str, str]],
    planned_sys_privs: Set[Tuple[str, str]],
    planned_objects: Set[Tuple[str, str]],
    max_sql_file_bytes: Optional[int],
) -> Tuple[List[str], List[str], bool]:
    plan_lines: List[str] = []
    sql_lines: List[str] = []

    nodes, edges = build_view_dependency_graph(chains)
    order, cycles = topo_sort_nodes(nodes, edges)
    blocked = bool(cycles)
    if cycles:
        plan_lines.append("BLOCK: 检测到依赖环，跳过自动执行。")
        for cycle in cycles:
            cycle_str = " -> ".join(f"{n[0]}({n[1]})" for n in cycle)
            plan_lines.append(f"  CYCLE: {cycle_str}")
        return plan_lines, sql_lines, True

    synonym_targets = resolve_synonym_target_map(edges)
    grant_option_views: Set[Tuple[str, str]] = set()

    for node in order:
        dep_full, dep_type = node
        dep_schema, _dep_name = split_full_name(dep_full)
        refs = sorted(edges.get(node, set()))
        for ref in refs:
            ref_full, ref_type = ref
            target_full, target_type = ref_full, ref_type
            if ref_type == "SYNONYM" and ref in synonym_targets:
                target_full, target_type = synonym_targets[ref]
            required_priv = GRANT_PRIVILEGE_BY_TYPE.get(target_type.upper())
            if not required_priv:
                plan_lines.append(f"SKIP GRANT: {dep_full} -> {target_full} (type={target_type})")
                continue
            if not dep_schema:
                plan_lines.append(f"BLOCK: 无法解析 grantee for {dep_full}")
                blocked = True
                continue
            require_option = requires_grant_option(dep_schema, target_full, target_type, dep_type)
            if require_option and target_type.upper() in GRANT_OPTION_TYPES:
                blocked = (
                    ensure_view_owner_grant_option(
                        target_full,
                        target_type,
                        edges,
                        synonym_targets,
                        obclient_cmd,
                        timeout,
                        grant_index_miss,
                        grant_index_all,
                        allow_fallback,
                        roles_cache,
                        tab_privs_cache,
                        tab_privs_grantable_cache,
                        sys_privs_cache,
                        planned_statements,
                        planned_object_privs,
                        planned_object_privs_with_option,
                        planned_sys_privs,
                        plan_lines,
                        sql_lines,
                        grant_option_views,
                    )
                    or blocked
                )

            blocked = (
                plan_object_grant_for_dependency(
                    dep_schema,
                    target_full,
                    target_type,
                    required_priv,
                    require_option,
                    allow_fallback,
                    obclient_cmd,
                    timeout,
                    grant_index_miss,
                    grant_index_all,
                    roles_cache,
                    tab_privs_cache,
                    tab_privs_grantable_cache,
                    sys_privs_cache,
                    planned_statements,
                    planned_object_privs,
                    planned_object_privs_with_option,
                    planned_sys_privs,
                    plan_lines,
                    sql_lines,
                )
                or blocked
            )

        exists = check_object_exists(
            obclient_cmd, timeout, dep_full, dep_type, exists_cache, planned_objects
        )
        if exists is None:
            plan_lines.append(f"BLOCK: 无法确认对象是否存在 {dep_full}({dep_type})")
            blocked = True
            continue
        if exists:
            plan_lines.append(f"EXISTS: {dep_full}({dep_type})")
            continue
        if dep_type.upper() == "SYNONYM" and node in synonym_targets:
            target_full, target_type = synonym_targets[node]
            target_exists = check_object_exists(
                obclient_cmd, timeout, target_full, target_type, exists_cache, planned_objects
            )
            if target_exists is None:
                plan_lines.append(f"BLOCK: 无法确认同义词目标是否存在 {target_full}({target_type})")
                blocked = True
                continue
            if target_exists:
                plan_lines.append(
                    f"SKIP DDL: {dep_full}(SYNONYM) 缺少脚本，使用 {target_full}({target_type})"
                )
                continue
        ddl_path, ddl_source = select_fixup_script_for_node_with_fallback(
            node, object_index, name_index, done_object_index, done_name_index
        )
        if not ddl_path:
            plan_lines.append(f"BLOCK: 缺少 DDL for {dep_full}({dep_type})")
            blocked = True
            continue
        ddl_text, _ddl_bytes, ddl_error = read_sql_text_with_limit(ddl_path, max_sql_file_bytes)
        if ddl_error:
            plan_lines.append(f"BLOCK: 读取 DDL 失败 {ddl_path} ({ddl_error})")
            blocked = True
            continue
        ddl_text = (ddl_text or "").rstrip()
        if ddl_text and dep_type.upper() == "VIEW":
            ddl_text = sanitize_view_chain_view_ddl(ddl_text)
        if ddl_text:
            rel_path = ddl_path.relative_to(repo_root)
            if ddl_source == "done":
                plan_lines.append(f"DDL ADD (DONE): {dep_full}({dep_type}) <- {rel_path}")
            else:
                plan_lines.append(f"DDL ADD: {dep_full}({dep_type}) <- {rel_path}")
            sql_lines.append(f"-- DDL SOURCE: {rel_path}")
            sql_lines.append(ddl_text)
            planned_objects.add((dep_full.upper(), dep_type.upper()))

    return plan_lines, sql_lines, blocked


def split_sql_statements(sql_text: str) -> List[str]:
    statements: List[str] = []
    buffer: List[str] = []
    in_single = False
    in_double = False
    block_comment_depth = 0
    in_q_quote = False
    q_quote_end = ""
    slash_block = False
    slash_block_end_name = ""

    def _normalize_block_name(token: str) -> str:
        text = (token or "").strip()
        if not text:
            return ""
        if "." in text:
            text = text.rsplit(".", 1)[-1].strip()
        if text.startswith('"') and text.endswith('"') and len(text) >= 2:
            text = text[1:-1]
        return text.strip().upper()

    def _extract_block_header_end_name(line: str) -> str:
        match = RE_BLOCK_HEADER_NAME.match(line or "")
        if not match:
            return ""
        return _normalize_block_name(match.group("name") or "")

    def _is_outer_block_end(line: str, expected_name: str) -> bool:
        match = RE_BLOCK_END.match(line or "")
        if not match:
            return False
        end_name = _normalize_block_name(match.group("quoted") or match.group("name") or "")
        if end_name and end_name in PLSQL_INNER_END_KEYWORDS:
            return False
        if not end_name:
            return True
        if expected_name:
            return end_name == expected_name
        return False

    def flush_buffer() -> None:
        statement = "".join(buffer).strip()
        if statement:
            statements.append(statement)
        buffer.clear()

    for line in sql_text.splitlines(keepends=True):
        if (
            not slash_block
            and not in_single
            and not in_double
            and not in_q_quote
            and block_comment_depth == 0
            and (RE_BLOCK_START.match(line) or RE_ANON_BLOCK_START.match(line))
        ):
            slash_block = True
            slash_block_end_name = _extract_block_header_end_name(line)
        stripped = line.strip()
        if (
            not in_single
            and not in_double
            and not in_q_quote
            and block_comment_depth == 0
            and stripped == "/"
        ):
            if buffer:
                buffer.append(line)
                flush_buffer()
            slash_block = False
            slash_block_end_name = ""
            continue

        idx = 0
        while idx < len(line):
            ch = line[idx]
            nxt = line[idx + 1] if idx + 1 < len(line) else ""

            if block_comment_depth > 0:
                buffer.append(ch)
                if ch == "/" and nxt == "*":
                    buffer.append(nxt)
                    idx += 2
                    block_comment_depth += 1
                    continue
                if ch == "*" and nxt == "/":
                    buffer.append(nxt)
                    idx += 2
                    block_comment_depth -= 1
                    continue
                idx += 1
                continue

            if in_q_quote:
                buffer.append(ch)
                if ch == q_quote_end and nxt == "'":
                    buffer.append(nxt)
                    idx += 2
                    in_q_quote = False
                    continue
                idx += 1
                continue

            if not in_single and not in_double:
                if ch in ("q", "Q") and nxt == "'" and idx + 2 < len(line):
                    delimiter = line[idx + 2]
                    if not delimiter.isspace():
                        q_quote_end = Q_QUOTE_DELIMS.get(delimiter, delimiter)
                        in_q_quote = True
                        buffer.append(ch)
                        buffer.append(nxt)
                        buffer.append(delimiter)
                        idx += 3
                        continue
                if ch == "/" and nxt == "*":
                    buffer.append(ch)
                    buffer.append(nxt)
                    idx += 2
                    block_comment_depth += 1
                    continue
                if ch == "-" and nxt == "-":
                    buffer.append(line[idx:])
                    break
                if ch == ";" and not slash_block:
                    buffer.append(ch)
                    flush_buffer()
                    idx += 1
                    continue

            if ch == "'" and not in_double:
                if in_single and nxt == "'":
                    buffer.append(ch)
                    buffer.append(nxt)
                    idx += 2
                    continue
                in_single = not in_single
            elif ch == '"' and not in_single:
                if in_double and nxt == '"':
                    buffer.append(ch)
                    buffer.append(nxt)
                    idx += 2
                    continue
                in_double = not in_double

            buffer.append(ch)
            idx += 1

        if (
            slash_block
            and not in_single
            and not in_double
            and not in_q_quote
            and block_comment_depth == 0
            and _is_outer_block_end(line, slash_block_end_name)
        ):
            slash_block = False
            slash_block_end_name = ""
            flush_buffer()

    if buffer:
        flush_buffer()

    return statements


def run_sql(
    obclient_cmd: List[str], sql_text: str, timeout: Optional[int]
) -> subprocess.CompletedProcess:
    """Execute SQL text by piping it to obclient."""
    sql_payload = build_obclient_sql_payload(sql_text)
    try:
        result = subprocess.run(
            obclient_cmd,
            input=sql_payload,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            check=False,
            timeout=timeout,
        )
        if result.returncode != 0:
            err_text = f"{result.stderr or ''}\n{result.stdout or ''}".lower()
            if "unknown option" in err_text and OBCLIENT_SECURE_OPT in err_text:
                raise ConfigError(
                    "当前 obclient 不支持安全凭据参数 --defaults-extra-file，"
                    "已阻断运行以避免回退到明文 -p。"
                )
        return result
    except FileNotFoundError as exc:
        raise ConfigError(f"obclient 不存在或不可执行: {exc}") from exc
    except PermissionError as exc:
        raise ConfigError(f"obclient 权限不足: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"调用 obclient 失败: {exc}") from exc


def build_obclient_sql_payload(sql_text: str, session_timeout_us: Optional[int] = None) -> str:
    effective_timeout = (
        OBCLIENT_SESSION_QUERY_TIMEOUT_US if session_timeout_us is None else session_timeout_us
    )
    try:
        effective_timeout = max(0, int(effective_timeout))
    except (TypeError, ValueError):
        effective_timeout = DEFAULT_OBCLIENT_SESSION_QUERY_TIMEOUT_US
    base_sql = sql_text or ""
    statements: List[str] = []
    if effective_timeout > 0:
        statements.append(f"ALTER SESSION SET ob_query_timeout = {effective_timeout};")
    if base_sql:
        statements.append(base_sql)
    if not statements:
        return ""
    payload = "\n".join(statements)
    if not payload.endswith("\n"):
        payload += "\n"
    return payload


def resolve_script_exec_mode(config_mode: str, sql_path: Path) -> str:
    """
    Resolve effective execution mode for one SQL file.
    Grant directories remain statement-level to preserve prune semantics.
    """
    mode = normalize_fixup_exec_mode(config_mode)
    if is_grant_dir(sql_path.parent.name):
        return "statement"
    if mode == "auto":
        return "file"
    return mode


def new_exec_mode_stats() -> Dict[str, int]:
    return {
        "file": 0,
        "statement": 0,
        "grants_statement": 0,
        "fallback_retried": 0,
        "fallback_success": 0,
    }


def bump_exec_mode_stat(exec_stats: Optional[Dict[str, int]], key: str, inc: int = 1) -> None:
    if exec_stats is None:
        return
    exec_stats[key] = int(exec_stats.get(key, 0)) + inc


def execute_sql_statements(
    obclient_cmd: List[str],
    sql_text: str,
    timeout: Optional[int],
    mode: str = "statement",
    context_label: str = "",
    artifact_path: str = "",
) -> ExecutionSummary:
    mode = normalize_fixup_exec_mode(mode)
    if mode == "auto":
        mode = "statement"
    statements = split_sql_statements(sql_text)
    effective_statements = [stmt for stmt in statements if stmt and stmt.strip()]
    failures: List[StatementFailure] = []
    if not effective_statements:
        return ExecutionSummary(statements=0, failures=failures)

    if mode == "file":
        with track_fixup_operation(
            "fixup_file_execution",
            operation_id=context_label or "file-mode",
            current=1,
            total=1,
            artifact_path=artifact_path,
            detail={
                "execution_mode": "file",
                "statement_count": len(effective_statements),
                "timeout_sec": timeout if timeout is not None else "none",
                "statement_progress_available": False,
            },
        ):
            try:
                result = run_sql(obclient_cmd, sql_text, timeout)
            except subprocess.TimeoutExpired:
                timeout_label = "no-timeout" if timeout is None else f"> {timeout} 秒"
                failures.append(StatementFailure(1, f"执行超时 ({timeout_label})", sql_text))
                return ExecutionSummary(statements=len(effective_statements), failures=failures)
        error_msg = extract_execution_error(result)
        if error_msg:
            failures.append(StatementFailure(1, error_msg, sql_text))
        return ExecutionSummary(statements=len(effective_statements), failures=failures)

    session_sensitive_reason = detect_session_sensitive_reason(sql_text)
    if session_sensitive_reason and len(effective_statements) > 1:
        with track_fixup_operation(
            "fixup_file_execution",
            operation_id=context_label or "session-sensitive-file",
            current=1,
            total=1,
            artifact_path=artifact_path,
            detail={
                "execution_mode": "file",
                "statement_count": len(effective_statements),
                "session_sensitive_reason": session_sensitive_reason,
                "statement_progress_available": False,
                "timeout_sec": timeout if timeout is not None else "none",
            },
        ):
            try:
                result = run_sql(obclient_cmd, sql_text, timeout)
            except subprocess.TimeoutExpired:
                timeout_label = "no-timeout" if timeout is None else f"> {timeout} 秒"
                failures.append(StatementFailure(1, f"执行超时 ({timeout_label})", sql_text))
                return ExecutionSummary(statements=len(effective_statements), failures=failures)
        error_msg = extract_execution_error(result)
        if error_msg:
            failures.append(
                StatementFailure(
                    1, f"{error_msg} [session-sensitive={session_sensitive_reason}]", sql_text
                )
            )
        return ExecutionSummary(statements=len(effective_statements), failures=failures)

    current_schema: Optional[str] = None

    for idx, statement in enumerate(effective_statements, start=1):
        match = CURRENT_SCHEMA_PATTERN.match(statement.strip())
        if match:
            current_schema = match.group("schema")
        statement_to_run = statement
        if current_schema and not match:
            statement_to_run = f"ALTER SESSION SET CURRENT_SCHEMA = {current_schema};\n{statement}"
        with track_fixup_operation(
            "fixup_statement_execution",
            operation_id=(context_label or "statement-mode"),
            current=idx,
            total=len(effective_statements),
            artifact_path=artifact_path,
            detail={
                "execution_mode": "statement",
                "timeout_sec": timeout if timeout is not None else "none",
                "sql_preview": safe_first_line(statement_to_run, 120, "-"),
            },
        ):
            try:
                result = run_sql(obclient_cmd, statement_to_run, timeout)
            except subprocess.TimeoutExpired:
                timeout_label = "no-timeout" if timeout is None else f"> {timeout} 秒"
                failures.append(
                    StatementFailure(idx, f"执行超时 ({timeout_label})", statement_to_run)
                )
                for remainder_idx, remainder_statement in enumerate(
                    effective_statements[idx:], start=idx + 1
                ):
                    failures.append(
                        StatementFailure(
                            remainder_idx,
                            "前置语句超时未执行",
                            remainder_statement,
                        )
                    )
                break

        error_msg = extract_execution_error(result)
        if error_msg:
            failures.append(StatementFailure(idx, error_msg, statement_to_run))

    return ExecutionSummary(statements=len(effective_statements), failures=failures)


def execute_sql_with_mode(
    obclient_cmd: List[str],
    sql_text: str,
    timeout: Optional[int],
    exec_mode: str,
    exec_file_fallback: bool,
    exec_stats: Optional[Dict[str, int]] = None,
    context_label: str = "",
) -> ExecutionSummary:
    mode = normalize_fixup_exec_mode(exec_mode)
    if mode == "auto":
        mode = "statement"
    bump_exec_mode_stat(exec_stats, mode)

    summary = execute_sql_statements_with_optional_context(
        obclient_cmd,
        sql_text,
        timeout,
        mode=mode,
        context_label=context_label,
        artifact_path=context_label,
    )
    if mode == "file" and summary.failures and exec_file_fallback:
        bump_exec_mode_stat(exec_stats, "fallback_retried")
        if context_label:
            log.warning("%s FILE 模式失败，回退 statement 重试一次。", context_label)
        else:
            log.warning("FILE 模式失败，回退 statement 重试一次。")
        fallback_summary = execute_sql_statements_with_optional_context(
            obclient_cmd,
            sql_text,
            timeout,
            mode="statement",
            context_label=f"{context_label} fallback" if context_label else "fallback",
            artifact_path=context_label,
        )
        if fallback_summary.success:
            bump_exec_mode_stat(exec_stats, "fallback_success")
        return fallback_summary
    return summary


def execute_sql_statements_with_optional_context(
    obclient_cmd: List[str],
    sql_text: str,
    timeout: Optional[int],
    *,
    mode: str,
    context_label: str = "",
    artifact_path: str = "",
) -> ExecutionSummary:
    kwargs = {
        "mode": mode,
        "context_label": context_label,
        "artifact_path": artifact_path,
    }
    try:
        return execute_sql_statements(obclient_cmd, sql_text, timeout, **kwargs)
    except TypeError as exc:
        message = str(exc)
        if "unexpected keyword argument" not in message or not any(
            name in message for name in ("context_label", "artifact_path")
        ):
            raise
        kwargs.pop("context_label", None)
        kwargs.pop("artifact_path", None)
        return execute_sql_statements(obclient_cmd, sql_text, timeout, **kwargs)


def log_exec_mode_summary(
    exec_stats: Dict[str, int], configured_mode: str, exec_file_fallback: bool
) -> None:
    log_subsection("执行模式统计")
    log.info("配置模式   : %s", configured_mode)
    log.info("file 执行  : %d", int(exec_stats.get("file", 0)))
    log.info("statement 执行 : %d", int(exec_stats.get("statement", 0)))
    log.info("grants statement : %d", int(exec_stats.get("grants_statement", 0)))
    if exec_file_fallback:
        log.info("file->statement 回退尝试 : %d", int(exec_stats.get("fallback_retried", 0)))
        log.info("file->statement 回退成功 : %d", int(exec_stats.get("fallback_success", 0)))


def check_obclient_connectivity(
    obclient_cmd: List[str], timeout: Optional[int]
) -> Tuple[bool, str]:
    """Run a lightweight connectivity check for obclient."""
    summary = execute_sql_statements(obclient_cmd, "SELECT 1 FROM DUAL;", timeout)
    if summary.failures:
        err = summary.failures[0].error if summary.failures else "执行失败"
        return False, err
    return True, ""


def run_query_lines(
    obclient_cmd: List[str], sql_text: str, timeout: Optional[int]
) -> Tuple[bool, List[str], str]:
    try:
        result = run_sql(obclient_cmd, sql_text, timeout)
    except subprocess.TimeoutExpired:
        return False, [], "TimeoutExpired"
    error_msg = extract_execution_error(result)
    if error_msg:
        return False, [], error_msg
    lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    return True, lines, ""


def query_count(obclient_cmd: List[str], sql_text: str, timeout: Optional[int]) -> Optional[int]:
    ok, lines, _err = run_query_lines(obclient_cmd, sql_text, timeout)
    if not ok:
        return None
    for line in lines:
        token = line.split("\t", 1)[0].strip()
        if token.isdigit():
            return int(token)
    return 0


def query_single_column(
    obclient_cmd: List[str], sql_text: str, timeout: Optional[int], column_name: str
) -> Set[str]:
    ok, lines, _err = run_query_lines(obclient_cmd, sql_text, timeout)
    if not ok:
        return set()
    col_upper = column_name.strip().upper()
    values: Set[str] = set()
    for line in lines:
        token = line.split("\t", 1)[0].strip()
        if not token:
            continue
        if token.upper() == col_upper:
            continue
        values.add(token.upper())
    return values


def extract_login_user(user_string: str) -> str:
    """Extract login username (without tenant suffix) from user_string."""
    raw = (user_string or "").strip()
    if not raw:
        return ""
    return raw.split("@", 1)[0].strip().upper()


def query_single_column_values(
    obclient_cmd: List[str], sql_text: str, timeout: Optional[int], column_name: str
) -> Tuple[bool, Set[str], str]:
    ok, lines, err = run_query_lines(obclient_cmd, sql_text, timeout)
    if not ok:
        return False, set(), err
    col_upper = (column_name or "").strip().upper()
    values: Set[str] = set()
    for line in lines:
        token = line.split("\t", 1)[0].strip().upper()
        if not token or token == col_upper:
            continue
        values.add(token)
    return True, values, ""


def query_existing_schemas(obclient_cmd: List[str], timeout: Optional[int]) -> Tuple[Set[str], str]:
    """
    Query existing users/schemas using best-effort fallback.
    Returns (schemas, source_view).
    """
    attempts = (
        ("DBA_USERS", "SELECT USERNAME FROM DBA_USERS;", "USERNAME"),
        ("ALL_USERS", "SELECT USERNAME FROM ALL_USERS;", "USERNAME"),
    )
    for source_name, sql_text, col_name in attempts:
        ok, values, _err = query_single_column_values(obclient_cmd, sql_text, timeout, col_name)
        if ok:
            return values, source_name
    return set(), "unavailable"


def query_effective_sys_privileges(
    obclient_cmd: List[str], timeout: Optional[int], login_user: str
) -> Set[str]:
    """Best-effort collection of direct + role-inherited system privileges."""
    user_u = (login_user or "").strip().upper()
    if not user_u:
        return set()

    def _escape(v: str) -> str:
        return v.replace("'", "''")

    effective: Set[str] = set()
    ok_direct, direct_values, _ = query_single_column_values(
        obclient_cmd,
        f"SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE = '{_escape(user_u)}';",
        timeout,
        "PRIVILEGE",
    )
    if ok_direct:
        effective.update(direct_values)

    ok_roles, role_values, _ = query_single_column_values(
        obclient_cmd,
        f"SELECT GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE = '{_escape(user_u)}';",
        timeout,
        "GRANTED_ROLE",
    )
    if ok_roles and role_values:
        role_list = sorted(r for r in role_values if r)
        literals = ",".join("'" + _escape(r) + "'" for r in role_list)
        ok_role_privs, role_priv_values, _ = query_single_column_values(
            obclient_cmd,
            f"SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE IN ({literals});",
            timeout,
            "PRIVILEGE",
        )
        if ok_role_privs:
            effective.update(role_priv_values)

    return effective


def collect_target_schemas_from_scripts(files_with_layer: List[Tuple[int, Path]]) -> Set[str]:
    """Collect target schemas inferred from script filenames."""
    schemas: Set[str] = set()
    schema_dirs = set(CREATE_OBJECT_DIRS) | {"table_alter", "compile"}
    for _, sql_path in files_with_layer:
        dir_name = sql_path.parent.name.lower()
        if dir_name not in schema_dirs:
            continue
        schema, _name = parse_object_identity_from_path(sql_path)
        if schema:
            schemas.add(schema.upper())
    return schemas


def infer_required_sys_privileges(
    files_with_layer: List[Tuple[int, Path]], current_user: str, target_schemas: Set[str]
) -> Set[str]:
    """
    Infer required system privileges for cross-schema fixup.
    This is a heuristic precheck (warn-only), not a strict blocker.
    """
    current_u = (current_user or "").upper()
    foreign_schemas = {
        s.upper()
        for s in (target_schemas or set())
        if s and s.upper() not in {current_u, "PUBLIC", "__PUBLIC"}
    }
    if not foreign_schemas:
        return set()

    dir_names = {path.parent.name.lower() for _, path in files_with_layer}
    required: Set[str] = set()

    if "table" in dir_names:
        required.add("CREATE ANY TABLE")
    if "table_alter" in dir_names or "constraint" in dir_names:
        required.add("ALTER ANY TABLE")
    if "view" in dir_names:
        required.add("CREATE ANY VIEW")
    if "materialized_view" in dir_names:
        required.add("CREATE ANY MATERIALIZED VIEW")
    if "synonym" in dir_names:
        required.add("CREATE ANY SYNONYM")
        if any(s in {"PUBLIC", "__PUBLIC"} for s in target_schemas):
            required.add("CREATE PUBLIC SYNONYM")
    if "sequence" in dir_names:
        required.add("CREATE ANY SEQUENCE")
    if "index" in dir_names:
        required.add("CREATE ANY INDEX")
    if "trigger" in dir_names:
        required.add("CREATE ANY TRIGGER")
    if any(d in dir_names for d in {"procedure", "function", "package", "package_body"}):
        required.add("CREATE ANY PROCEDURE")
    if any(d in dir_names for d in {"type", "type_body"}):
        required.add("CREATE ANY TYPE")
    if any(d in dir_names for d in {"job", "schedule"}):
        required.add("CREATE ANY JOB")
    if any(d in GRANT_DIRS for d in dir_names):
        required.add("GRANT ANY OBJECT PRIVILEGE")

    return required


def build_fixup_precheck_summary(
    ob_cfg: Dict[str, str],
    obclient_cmd: List[str],
    timeout: Optional[int],
    files_with_layer: List[Tuple[int, Path]],
) -> FixupPrecheckSummary:
    current_user = extract_login_user(ob_cfg.get("user_string", ""))
    target_schemas = collect_target_schemas_from_scripts(files_with_layer)
    existing_schemas, lookup_source = query_existing_schemas(obclient_cmd, timeout)
    missing_schemas = set()
    if existing_schemas:
        missing_schemas = {s for s in target_schemas if s not in existing_schemas}

    required_privs = infer_required_sys_privileges(files_with_layer, current_user, target_schemas)
    effective_privs = query_effective_sys_privileges(obclient_cmd, timeout, current_user)
    missing_privs = set(required_privs)
    if effective_privs:
        missing_privs -= effective_privs

    return FixupPrecheckSummary(
        current_user=current_user,
        target_schemas=target_schemas,
        existing_schemas=existing_schemas,
        missing_schemas=missing_schemas,
        required_sys_privileges=required_privs,
        effective_sys_privileges=effective_privs,
        missing_sys_privileges=missing_privs,
        schema_lookup_source=lookup_source,
    )


def write_fixup_precheck_report(fixup_dir: Path, summary: FixupPrecheckSummary) -> Optional[Path]:
    try:
        out_dir = fixup_dir / "errors"
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = out_dir / f"fixup_precheck_{ts}.txt"
        lines: List[str] = []
        lines.append("# fixup precheck report")
        lines.append(f"# generated={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"CURRENT_USER|{summary.current_user or '-'}")
        lines.append(
            f"TARGET_SCHEMAS|{len(summary.target_schemas)}|{','.join(sorted(summary.target_schemas)) if summary.target_schemas else '-'}"
        )
        lines.append(
            f"EXISTING_SCHEMAS_SOURCE|{summary.schema_lookup_source}|{len(summary.existing_schemas)}"
        )
        lines.append(
            f"MISSING_SCHEMAS|{len(summary.missing_schemas)}|{','.join(sorted(summary.missing_schemas)) if summary.missing_schemas else '-'}"
        )
        lines.append(
            f"REQUIRED_SYS_PRIVS|{len(summary.required_sys_privileges)}|{','.join(sorted(summary.required_sys_privileges)) if summary.required_sys_privileges else '-'}"
        )
        lines.append(
            f"EFFECTIVE_SYS_PRIVS|{len(summary.effective_sys_privileges)}|{','.join(sorted(summary.effective_sys_privileges)) if summary.effective_sys_privileges else '-'}"
        )
        lines.append(
            f"MISSING_SYS_PRIVS|{len(summary.missing_sys_privileges)}|{','.join(sorted(summary.missing_sys_privileges)) if summary.missing_sys_privileges else '-'}"
        )
        out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return out_path
    except Exception as exc:
        log.warning("写入 fixup precheck 报告失败: %s", exc)
        return None


def log_fixup_precheck(summary: FixupPrecheckSummary, report_path: Optional[Path]) -> None:
    log_subsection("执行前检查")
    log.info("当前用户: %s", summary.current_user or "-")
    log.info("脚本目标 schema: %d 个", len(summary.target_schemas))
    if summary.target_schemas:
        log.info("目标 schema 列表: %s", ", ".join(sorted(summary.target_schemas)))
    if summary.schema_lookup_source != "unavailable":
        log.info(
            "目标库 schema 查询: %s (%d 个)",
            summary.schema_lookup_source,
            len(summary.existing_schemas),
        )
    else:
        log.warning("目标库 schema 查询失败，无法提前判断缺失 schema。")
    if summary.missing_schemas:
        log.warning(
            "前置检查发现缺失 schema (%d): %s",
            len(summary.missing_schemas),
            ", ".join(sorted(summary.missing_schemas)),
        )
    if summary.required_sys_privileges:
        log.info(
            "跨 schema 预估需要系统权限: %s", ", ".join(sorted(summary.required_sys_privileges))
        )
    if summary.missing_sys_privileges:
        log.warning(
            "前置检查发现可能缺少系统权限 (%d): %s",
            len(summary.missing_sys_privileges),
            ", ".join(sorted(summary.missing_sys_privileges)),
        )
    if report_path:
        log.info("前置检查清单: %s", report_path)


def build_fixup_object_index(
    files_with_layer: List[Tuple[int, Path]],
) -> Tuple[Dict[Tuple[str, str], List[Path]], Dict[str, List[Path]]]:
    by_schema_obj: Dict[Tuple[str, str], List[Path]] = defaultdict(list)
    by_name: Dict[str, List[Path]] = defaultdict(list)

    for _, path in files_with_layer:
        dir_name = path.parent.name.lower()
        if dir_name not in CREATE_OBJECT_DIRS:
            continue
        schema, name = parse_object_from_filename(path)
        if not schema or not name:
            continue
        by_schema_obj[(schema, name)].append(path)
        by_name[name].append(path)

    return dict(by_schema_obj), dict(by_name)


def infer_recompile_owners(files_with_layer: List[Tuple[int, Path]]) -> Set[str]:
    owners: Set[str] = set()
    for _, path in files_with_layer:
        schema, _name = parse_object_from_filename(path)
        if schema:
            owners.add(schema)
    return owners


def collect_sql_files_from_root(
    root_dir: Path, include_dirs: Optional[Set[str]] = None, exclude_dirs: Optional[Set[str]] = None
) -> List[Path]:
    files: List[Path] = []
    if not root_dir.exists():
        return files
    include_set = {d.lower() for d in include_dirs or set()}
    exclude_set = {d.lower() for d in exclude_dirs or set()}
    for subdir in sorted(root_dir.iterdir()):
        if not subdir.is_dir():
            continue
        if path_excluded_by_filters(subdir.name, exclude_set):
            continue
        if not should_scan_top_dir(subdir.name, include_set):
            continue
        for sql_file in iter_sql_files_recursive(subdir):
            if sql_file.is_file():
                rel_parent = normalize_dir_filter(sql_file.parent.relative_to(root_dir))
                if path_excluded_by_filters(rel_parent, exclude_set):
                    continue
                if not path_selected_by_filters(rel_parent, include_set):
                    continue
                files.append(sql_file)
    return files


@dataclass(frozen=True)
class GrantEntry:
    grantee: str
    privileges: Tuple[str, ...]
    object_name: Optional[str]
    statement: str
    source_path: Path
    grant_type: str


@dataclass
class GrantIndex:
    by_grantee_object: Dict[Tuple[str, str], List[GrantEntry]]
    by_object: Dict[str, List[GrantEntry]]
    by_grantee_sys: Dict[str, List[GrantEntry]]


def build_grant_index(
    fixup_dir: Path, exclude_dirs: Set[str], include_dirs: Optional[Set[str]] = None
) -> GrantIndex:
    by_grantee_object: Dict[Tuple[str, str], List[GrantEntry]] = defaultdict(list)
    by_object: Dict[str, List[GrantEntry]] = defaultdict(list)
    by_grantee_sys: Dict[str, List[GrantEntry]] = defaultdict(list)
    if "grants" in exclude_dirs and "grants_miss" in exclude_dirs and "grants_all" in exclude_dirs:
        return GrantIndex(dict(by_grantee_object), dict(by_object), dict(by_grantee_sys))

    subdirs = {
        p.name.lower(): p
        for p in fixup_dir.iterdir()
        if p.is_dir() and p.name != DONE_DIR_NAME and p.name.lower() not in exclude_dirs
    }
    grant_dirs = resolve_grant_dirs(subdirs, include_dirs, exclude_dirs)
    if not grant_dirs:
        return GrantIndex(dict(by_grantee_object), dict(by_object), dict(by_grantee_sys))

    for grant_dir in grant_dirs:
        grants_path = subdirs.get(grant_dir)
        if not grants_path:
            continue
        for grant_file in iter_sql_files_recursive(grants_path):
            try:
                content = grant_file.read_text(encoding="utf-8", errors="replace")
            except Exception as exc:
                log.warning("读取授权文件失败: %s (%s)", grant_file, exc)
                continue
            for statement in split_sql_statements(content):
                parsed = parse_grant_statement(statement)
                if not parsed:
                    continue
                grant_type, privs, object_full, grantees = parsed
                for grantee in grantees:
                    entry = GrantEntry(
                        grantee=grantee,
                        privileges=privs,
                        object_name=object_full,
                        statement=statement.strip(),
                        source_path=grant_file,
                        grant_type=grant_type,
                    )
                    if grant_type == "OBJECT" and object_full:
                        by_grantee_object[(grantee, object_full)].append(entry)
                        by_object[object_full].append(entry)
                    elif grant_type == "SYSTEM":
                        by_grantee_sys[grantee].append(entry)

    return GrantIndex(dict(by_grantee_object), dict(by_object), dict(by_grantee_sys))


def normalize_statement_key(statement: str) -> str:
    return " ".join(statement.upper().split())


def build_auto_grant_statement(
    grantee: str, object_full: str, required_priv: str, with_grant_option: bool = False
) -> Optional[str]:
    if not grantee or not object_full or not required_priv:
        return None
    grantee_u = normalize_identifier(grantee)
    object_u = normalize_identifier(object_full)
    priv_u = required_priv.strip().upper()
    if not grantee_u or not object_u or not priv_u:
        return None
    obj_schema, obj_name = parse_object_token(object_u)
    if obj_schema:
        object_ref = quote_qualified_name(obj_schema, obj_name)
    else:
        object_ref = quote_identifier(obj_name)
    suffix = " WITH GRANT OPTION" if with_grant_option else ""
    return f"GRANT {priv_u} ON {object_ref} TO {quote_identifier(grantee_u)}{suffix};"


def split_grant_list(raw: str) -> List[str]:
    if not raw:
        return []
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def split_grantee_list(raw: str) -> List[str]:
    if not raw:
        return []
    values = []
    for item in raw.split(","):
        cleaned = item.strip().rstrip(";")
        if not cleaned:
            continue
        values.append(normalize_identifier(cleaned))
    return values


def parse_grant_statement(
    statement: str,
) -> Optional[Tuple[str, Tuple[str, ...], Optional[str], List[str]]]:
    if not statement:
        return None
    flat = " ".join(statement.split())
    match = RE_GRANT_OBJECT.match(flat)
    if match:
        privs = split_grant_list(match.group("privs"))
        grantees_raw = RE_WITH_OPTION.sub("", match.group("grantees") or "")
        grantees = split_grantee_list(grantees_raw)
        schema, name = parse_object_token(match.group("object"))
        object_full = f"{schema}.{name}" if schema else name
        if not privs or not grantees or not object_full:
            return None
        return "OBJECT", tuple(privs), object_full, grantees
    match = RE_GRANT_SIMPLE.match(flat)
    if match:
        privs = split_grant_list(match.group("privs"))
        grantees_raw = RE_WITH_OPTION.sub("", match.group("grantees") or "")
        grantees = split_grantee_list(grantees_raw)
        if not privs or not grantees:
            return None
        return "SYSTEM", tuple(privs), None, grantees
    return None


def infer_required_privileges_from_failed_statement(
    statement: str,
    object_full: str,
    object_type: str,
) -> Optional[Set[str]]:
    parsed = parse_grant_statement(statement)
    if not parsed:
        return None
    grant_type, privileges, parsed_object_full, _grantees = parsed
    if grant_type != "OBJECT":
        return None
    parsed_object_u = normalize_full_name(parsed_object_full or "")
    object_full_u = normalize_full_name(object_full or "")
    if not parsed_object_u or not object_full_u or parsed_object_u != object_full_u:
        return None
    object_type_u = normalize_object_type(object_type)
    if object_type_u not in {"VIEW", "MATERIALIZED VIEW"}:
        return None
    privilege_set = {(item or "").upper() for item in (privileges or ()) if (item or "").strip()}
    return privilege_set or None


def select_dependency_script(
    schema: Optional[str],
    name: str,
    view_schema: Optional[str],
    object_index: Dict[Tuple[str, str], List[Path]],
    name_index: Dict[str, List[Path]],
) -> Optional[Path]:
    if schema:
        candidates = object_index.get((schema, name), [])
        return candidates[0] if candidates else None
    if view_schema:
        candidates = object_index.get((view_schema, name), [])
        if candidates:
            return candidates[0]
    candidates = name_index.get(name, [])
    if len(candidates) == 1:
        return candidates[0]
    return None


def select_grant_entries(
    grant_index: GrantIndex,
    grantee: str,
    schema: Optional[str],
    name: str,
    view_schema: Optional[str],
) -> List[GrantEntry]:
    entries: List[GrantEntry] = []
    if schema:
        key = f"{schema}.{name}"
        entries = grant_index.by_grantee_object.get((grantee, key), [])
        if entries:
            return entries
    if view_schema:
        key = f"{view_schema}.{name}"
        entries = grant_index.by_grantee_object.get((grantee, key), [])
        if entries:
            return entries
    suffix = f".{name}"
    candidates: List[GrantEntry] = []
    for (entry_grantee, obj_key), obj_entries in grant_index.by_grantee_object.items():
        if entry_grantee != grantee:
            continue
        if obj_key.endswith(suffix):
            candidates.extend(obj_entries)
    return candidates


def select_object_grant_entries_for_priv(
    grant_index: GrantIndex,
    grantee: str,
    object_full: str,
    required_priv: str,
    require_grant_option: bool = False,
) -> List[GrantEntry]:
    if not object_full:
        return []
    schema, name = parse_object_token(object_full)
    entries = select_grant_entries(grant_index, grantee, schema, name, schema)
    required = (required_priv or "").upper()
    filtered = [entry for entry in entries if required and required in entry.privileges]
    if not require_grant_option:
        return filtered
    return [entry for entry in filtered if grant_statement_has_option(entry.statement)]


def select_system_grant_entries_for_priv(
    grant_index: GrantIndex, grantee: str, target_full: str, target_type: str, required_priv: str
) -> List[GrantEntry]:
    implied = resolve_implied_sys_privileges(required_priv, target_full, target_type)
    if not implied:
        return []
    entries = grant_index.by_grantee_sys.get(grantee, [])
    if not entries:
        return []
    implied_upper = {p.upper() for p in implied}
    return [
        entry
        for entry in entries
        if any(priv.upper() in implied_upper for priv in entry.privileges)
    ]


def find_grant_entries_by_priority(
    grantee: str,
    target_full: str,
    target_type: str,
    required_priv: str,
    grant_index_miss: GrantIndex,
    grant_index_all: GrantIndex,
    require_grant_option: bool = False,
) -> Tuple[List[GrantEntry], str]:
    entries = select_object_grant_entries_for_priv(
        grant_index_miss, grantee, target_full, required_priv, require_grant_option
    )
    if entries:
        return entries, "grants_miss"
    entries = select_object_grant_entries_for_priv(
        grant_index_all, grantee, target_full, required_priv, require_grant_option
    )
    if entries:
        return entries, "grants_all"
    entries = select_system_grant_entries_for_priv(
        grant_index_miss, grantee, target_full, target_type, required_priv
    )
    if entries:
        return entries, "grants_miss"
    entries = select_system_grant_entries_for_priv(
        grant_index_all, grantee, target_full, target_type, required_priv
    )
    if entries:
        return entries, "grants_all"
    return [], ""


def apply_grant_entries(
    obclient_cmd: List[str],
    entries: List[GrantEntry],
    timeout: Optional[int],
    applied_grants: Set[str],
) -> Tuple[int, int]:
    applied = 0
    failed = 0

    for entry in entries:
        key = normalize_statement_key(entry.statement)
        if key in applied_grants:
            continue
        try:
            result = run_sql(obclient_cmd, entry.statement, timeout)
        except subprocess.TimeoutExpired:
            failed += 1
            log.warning("[GRANT] 执行超时: %s", safe_first_line(entry.statement, 160, "执行超时"))
            continue
        error_msg = extract_execution_error(result)
        if not error_msg:
            applied_grants.add(key)
            applied += 1
            continue
        failed += 1
        log.warning("[GRANT] 执行失败: %s", safe_first_line(error_msg, 160, "执行失败"))

    return applied, failed


def resolve_timeout_value(raw_timeout: Optional[int]) -> Optional[int]:
    if raw_timeout is None:
        return None
    try:
        return int(raw_timeout)
    except Exception as exc:
        log.warning("超时配置非法，回退默认值 %s: %s", DEFAULT_FIXUP_TIMEOUT, exc)
        return DEFAULT_FIXUP_TIMEOUT


def classify_view_chain_status(
    blocked: bool, skipped: bool, view_exists: Optional[bool], failure_count: int
) -> str:
    if skipped:
        return "SKIPPED"
    if blocked:
        return "BLOCKED"
    if view_exists is None:
        return "FAILED"
    if failure_count == 0 and view_exists:
        return "SUCCESS"
    if failure_count > 0 and view_exists:
        return "PARTIAL"
    return "FAILED"


def record_error_entry(
    entries: List[ErrorReportEntry],
    limit: int,
    relative_path: Path,
    statement_index: int,
    statement: str,
    error_message: str,
) -> bool:
    if len(entries) >= limit:
        return False
    error_code = parse_error_code(error_message)
    object_name = infer_error_object(statement, relative_path)
    contract = get_fixup_execution_contract(relative_path)
    message = " ".join((error_message or "").split())
    if len(message) > 200:
        message = message[:200] + "..."
    entries.append(
        ErrorReportEntry(
            file_path=relative_path,
            statement_index=statement_index,
            error_code=error_code,
            object_name=object_name,
            message=message or "-",
            family=str(contract.get("family") or "-"),
            support_tier=str(contract.get("support_tier") or "-"),
            retry_policy=str(contract.get("retry_policy") or "-"),
        )
    )
    return True


def write_error_report(
    entries: List[ErrorReportEntry], fixup_dir: Path, limit: int, truncated: bool
) -> Optional[Path]:
    if not entries:
        return None
    errors_dir = fixup_dir / "errors"
    errors_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = errors_dir / f"fixup_errors_{timestamp}.txt"
    lines = [
        "# fixup error report",
        f"# count={len(entries)} limit={limit} truncated={'true' if truncated else 'false'}",
        "FILE | STMT_INDEX | ERROR_CODE | OBJECT | FAMILY | SUPPORT_TIER | RETRY_POLICY | MESSAGE",
    ]
    for entry in entries:
        lines.append(
            f"{entry.file_path} | {entry.statement_index} | {entry.error_code} | {entry.object_name} | "
            f"{entry.family} | {entry.support_tier} | {entry.retry_policy} | {entry.message}"
        )
    if truncated:
        lines.append("[... TRUNCATED ...]")
        log.warning("[ERROR_REPORT] 错误报告达到上限 %d，输出已截断。", limit)
    report_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return report_path


def normalize_failed_path(path: Path, repo_root: Path) -> Path:
    raw = Path(path)
    if raw.is_absolute():
        try:
            return raw.resolve()
        except Exception:
            return raw
    try:
        return (repo_root / raw).resolve()
    except Exception:
        return repo_root / raw


def build_non_retryable_iterative_message(relative_path: Path) -> str:
    contract = get_fixup_execution_contract(relative_path)
    return (
        f"family={contract.get('family') or '-'}, "
        f"support_tier={contract.get('support_tier') or '-'}, "
        f"retry_policy={contract.get('retry_policy') or '-'}；"
        "上一轮失败后本轮不自动重试"
    )


def execute_grant_file_with_prune(
    obclient_cmd: List[str],
    sql_path: Path,
    repo_root: Path,
    done_dir: Path,
    timeout: Optional[int],
    layer: int,
    label_prefix: str,
    error_entries: List[ErrorReportEntry],
    error_limit: int,
    max_sql_file_bytes: Optional[int],
    state_ledger: Optional[FixupStateLedger] = None,
) -> Tuple[ScriptResult, ExecutionSummary, int, int, bool]:
    relative_path = sql_path.relative_to(repo_root)
    sql_text, sql_bytes, read_error = read_sql_text_with_limit(sql_path, max_sql_file_bytes)
    if read_error:
        msg = read_error
        log.error("%s %s -> ERROR (%s)", label_prefix, relative_path, msg)
        failure = StatementFailure(0, msg, "")
        return (
            ScriptResult(relative_path, "ERROR", msg, layer),
            ExecutionSummary(0, [failure]),
            0,
            0,
            False,
        )
    fingerprint = FixupStateLedger.fingerprint(sql_bytes or b"")
    if state_ledger and state_ledger.is_completed(relative_path, fingerprint):
        msg = "状态账本命中，跳过重复执行"
        log.warning("%s %s -> SKIP (%s)", label_prefix, relative_path, msg)
        return (
            ScriptResult(relative_path, "SKIPPED", msg, layer),
            ExecutionSummary(0, []),
            0,
            0,
            False,
        )

    statements = split_sql_statements(sql_text or "")
    kept_statements: List[str] = []
    failures: List[StatementFailure] = []
    executed_count = 0
    removed_count = 0
    skipped_non_grant_count = 0
    truncated = False

    for statement in statements:
        if is_comment_only_statement(statement):
            continue
        stripped = strip_leading_sql_comments(statement).strip()
        is_grant = stripped.upper().startswith("GRANT ")
        # grants_* 文件仅保留 GRANT 的重试语义，避免非 GRANT 语句反复执行。
        if not is_grant:
            skipped_non_grant_count += 1
            continue
        executed_count += 1

        with track_fixup_operation(
            "fixup_grant_statement",
            operation_id=f"{label_prefix} {relative_path}",
            current=executed_count,
            total=len(statements),
            artifact_path=str(relative_path),
            detail={
                "execution_mode": "statement",
                "timeout_sec": timeout if timeout is not None else "none",
                "sql_preview": safe_first_line(statement, 120, "-"),
            },
        ):
            try:
                result = run_sql(obclient_cmd, statement, timeout)
            except subprocess.TimeoutExpired:
                msg = "执行超时" if timeout is None else f"执行超时 (> {timeout} 秒)"
                failures.append(StatementFailure(executed_count, msg, statement))
                kept_statements.append(statement)
                if not truncated:
                    truncated = not record_error_entry(
                        error_entries, error_limit, relative_path, executed_count, statement, msg
                    )
                continue

        error_msg = extract_execution_error(result)
        if not error_msg:
            removed_count += 1
            continue

        message = error_msg
        failures.append(StatementFailure(executed_count, message, statement))
        kept_statements.append(statement)
        if not truncated:
            truncated = not record_error_entry(
                error_entries, error_limit, relative_path, executed_count, statement, message
            )

    summary = ExecutionSummary(executed_count, failures)
    if skipped_non_grant_count:
        log.warning(
            "%s %s 包含 %d 条非 GRANT 语句，已在 grant-prune 模式下跳过（不参与重试）。",
            label_prefix,
            relative_path,
            skipped_non_grant_count,
        )

    if executed_count == 0:
        log.warning("%s %s -> SKIP (文件为空)", label_prefix, relative_path)
        return ScriptResult(relative_path, "SKIPPED", "文件为空", layer), summary, 0, 0, truncated

    if summary.success:
        move_note = move_sql_to_done(sql_path, done_dir)
        if "移动失败" in move_note:
            if state_ledger:
                state_ledger.mark_completed(relative_path, fingerprint, "EXECUTED_BUT_MOVE_FAILED")
            log.error("%s %s -> ERROR %s", label_prefix, relative_path, move_note)
            failure = StatementFailure(0, move_note.strip("()"), "")
            return (
                ScriptResult(relative_path, "ERROR", move_note.strip(), layer),
                ExecutionSummary(executed_count, [failure]),
                removed_count,
                0,
                truncated,
            )
        if state_ledger:
            state_ledger.clear(relative_path)
        log.info(
            "%s %s -> OK %s (已清理授权 %d 条)",
            label_prefix,
            relative_path,
            move_note,
            removed_count,
        )
        return (
            ScriptResult(relative_path, "SUCCESS", move_note.strip(), layer),
            summary,
            removed_count,
            0,
            truncated,
        )

    # Rewrite file with remaining statements (non-grants + failed grants)
    rewritten = "\n\n".join(stmt.strip() for stmt in kept_statements if stmt.strip()).rstrip()
    tmp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=str(sql_path.parent),
            prefix=f"{sql_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp_fp:
            tmp_fp.write(rewritten + "\n")
            tmp_path = Path(tmp_fp.name)
        os.replace(str(tmp_path), str(sql_path))
        log.info(
            "%s %s -> FAIL (%d/%d statements), 保留失败语句 %d 条, 已清理授权 %d 条",
            label_prefix,
            relative_path,
            len(failures),
            executed_count,
            len(kept_statements),
            removed_count,
        )
    except Exception as exc:
        log.warning("%s %s -> 重写失败: %s", label_prefix, relative_path, str(exc)[:200])
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass

    first_error = failures[0].error if failures else "执行失败"
    return (
        ScriptResult(relative_path, "FAILED", first_error, layer),
        summary,
        removed_count,
        len(kept_statements),
        truncated,
    )


def execute_script_with_summary(
    obclient_cmd: List[str],
    sql_path: Path,
    repo_root: Path,
    done_dir: Path,
    timeout: Optional[int],
    layer: int,
    label_prefix: str,
    max_sql_file_bytes: Optional[int],
    state_ledger: Optional[FixupStateLedger] = None,
    exec_mode: str = "statement",
    exec_file_fallback: bool = True,
    exec_stats: Optional[Dict[str, int]] = None,
) -> Tuple[ScriptResult, ExecutionSummary]:
    relative_path = sql_path.relative_to(repo_root)
    sql_text, sql_bytes, read_error = read_sql_text_with_limit(sql_path, max_sql_file_bytes)
    if read_error:
        msg = read_error
        log.error("%s %s -> ERROR (%s)", label_prefix, relative_path, msg)
        return ScriptResult(relative_path, "ERROR", msg, layer), ExecutionSummary(
            0, [StatementFailure(0, msg, "")]
        )
    fingerprint = FixupStateLedger.fingerprint(sql_bytes or b"")
    if state_ledger and state_ledger.is_completed(relative_path, fingerprint):
        msg = "状态账本命中，跳过重复执行"
        log.warning("%s %s -> SKIP (%s)", label_prefix, relative_path, msg)
        return ScriptResult(relative_path, "SKIPPED", msg, layer), ExecutionSummary(0, [])

    if not (sql_text or "").strip():
        log.warning("%s %s -> SKIP (文件为空)", label_prefix, relative_path)
        return ScriptResult(relative_path, "SKIPPED", "文件为空", layer), ExecutionSummary(0, [])

    effective_mode = resolve_script_exec_mode(exec_mode, sql_path)
    summary = execute_sql_with_mode(
        obclient_cmd,
        sql_text or "",
        timeout=timeout,
        exec_mode=effective_mode,
        exec_file_fallback=exec_file_fallback,
        exec_stats=exec_stats,
        context_label=f"{label_prefix} {relative_path}",
    )
    if summary.statements == 0:
        log.warning("%s %s -> SKIP (文件无有效语句)", label_prefix, relative_path)
        return ScriptResult(relative_path, "SKIPPED", "文件无有效语句", layer), summary

    if summary.success:
        move_note = move_sql_to_done(sql_path, done_dir)
        if "移动失败" in move_note:
            if state_ledger:
                state_ledger.mark_completed(relative_path, fingerprint, "EXECUTED_BUT_MOVE_FAILED")
            log.error("%s %s -> ERROR %s", label_prefix, relative_path, move_note)
            failure = StatementFailure(0, move_note.strip("()"), "")
            return ScriptResult(relative_path, "ERROR", move_note.strip(), layer), ExecutionSummary(
                summary.statements, [failure]
            )
        if state_ledger:
            state_ledger.clear(relative_path)
        log.info("%s %s -> OK %s [mode=%s]", label_prefix, relative_path, move_note, effective_mode)
        return ScriptResult(relative_path, "SUCCESS", move_note.strip(), layer), summary

    first_error = summary.failures[0].error if summary.failures else "执行失败"
    log.warning(
        "%s %s -> FAIL (%d/%d statements) [mode=%s]",
        label_prefix,
        relative_path,
        len(summary.failures),
        summary.statements,
        effective_mode,
    )
    for failure in summary.failures[:3]:
        log.warning("  [%d] %s", failure.index, safe_first_line(failure.error, 200, "执行失败"))
    return ScriptResult(relative_path, "FAILED", first_error, layer), summary


def query_invalid_objects(
    obclient_cmd: List[str], timeout: Optional[int], allowed_owners: Optional[Set[str]] = None
) -> List[Tuple[str, str, str]]:
    """
    Query INVALID objects from OceanBase.

    Returns:
        List of (owner, object_name, object_type) tuples
    """
    owner_filter = ""
    if allowed_owners:
        owners = sorted(
            {owner.strip().upper() for owner in allowed_owners if owner and owner.strip()}
        )
        if owners:
            owner_in = ", ".join(f"'{escape_sql_literal(owner)}'" for owner in owners)
            owner_filter = f"\n      AND OWNER IN ({owner_in})"

    sql = f"""
    SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
    FROM DBA_OBJECTS
    WHERE STATUS = 'INVALID'
      {owner_filter}
    ORDER BY OWNER, OBJECT_TYPE, OBJECT_NAME;
    """

    try:
        result = run_sql(obclient_cmd, sql, timeout)
        if result.returncode != 0:
            return []

        invalid_objects = []
        for raw_line in (result.stdout or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            upper = line.upper()
            if upper.startswith("WARNING:") or upper.startswith("SP2-"):
                continue
            parts = line.split("\t")
            if len(parts) >= 3:
                owner = parts[0].strip()
                obj_name = parts[1].strip()
                obj_type = parts[2].strip()
                # Skip header row emitted by obclient
                if (
                    owner.upper() == "OWNER"
                    and obj_name.upper() == "OBJECT_NAME"
                    and obj_type.upper() == "OBJECT_TYPE"
                ):
                    continue
                if not re.fullmatch(r"[A-Z0-9_$#]+", owner.upper()):
                    continue
                if not owner or not obj_name or not obj_type:
                    continue
                invalid_objects.append((owner, obj_name, obj_type))

        return invalid_objects
    except Exception as exc:
        log.warning("查询 INVALID 对象失败: %s", exc)
        return []


def is_object_invalid(
    obclient_cmd: List[str],
    timeout: Optional[int],
    owner: str,
    obj_name: str,
    obj_type: str,
    cache: Optional[Dict[Tuple[str, str, str], Optional[bool]]] = None,
) -> Optional[bool]:
    key = (str(owner or "").upper(), str(obj_name or "").upper(), str(obj_type or "").upper())
    if cache is not None and key in cache:
        return cache[key]
    sql = (
        "SELECT COUNT(*) FROM DBA_OBJECTS "
        f"WHERE OWNER='{escape_sql_literal(owner)}' "
        f"AND OBJECT_NAME='{escape_sql_literal(obj_name)}' "
        f"AND OBJECT_TYPE='{escape_sql_literal(obj_type)}' "
        "AND STATUS='INVALID'"
    )
    count = query_count(obclient_cmd, sql, timeout)
    result = None if count is None else count > 0
    if cache is not None:
        cache[key] = result
    return result


def build_compile_statement(owner: str, obj_name: str, obj_type: str) -> Optional[str]:
    if not owner or not obj_name or not obj_type:
        return None
    obj_type_u = obj_type.strip().upper()
    qualified = quote_qualified_name(owner, obj_name)
    if obj_type_u in {"VIEW", "MATERIALIZED VIEW", "TYPE BODY"}:
        return None
    if obj_type_u == "PACKAGE BODY":
        return f"ALTER PACKAGE {qualified} COMPILE BODY;"
    if obj_type_u in {"PACKAGE", "TYPE", "PROCEDURE", "FUNCTION", "TRIGGER"}:
        return f"ALTER {obj_type_u} {qualified} COMPILE;"
    return None


class RecompileSummary(NamedTuple):
    total_recompiled: int
    remaining_invalid: int
    recompile_failed: int
    unsupported_types: int


def recompile_invalid_objects(
    obclient_cmd: List[str],
    timeout: Optional[int],
    max_retries: int = MAX_RECOMPILE_RETRIES,
    allowed_owners: Optional[Set[str]] = None,
) -> RecompileSummary:
    """
    Recompile INVALID objects multiple times until all are VALID or max retries reached.

    Returns:
        RecompileSummary(total_recompiled, remaining_invalid, recompile_failed, unsupported_types)
    """
    total_recompiled = 0
    recompile_failed = 0
    unsupported_types = 0

    for retry in range(max_retries):
        invalid_objects = query_invalid_objects(
            obclient_cmd, timeout, allowed_owners=allowed_owners
        )
        if not invalid_objects:
            return RecompileSummary(total_recompiled, 0, recompile_failed, unsupported_types)

        log.info("重编译轮次 %d/%d, INVALID=%d", retry + 1, max_retries, len(invalid_objects))

        recompiled_this_round = 0
        failed_this_round = 0
        invalid_status_cache: Dict[Tuple[str, str, str], Optional[bool]] = {}
        for owner, obj_name, obj_type in invalid_objects:
            compile_sql = build_compile_statement(owner, obj_name, obj_type)
            if not compile_sql:
                unsupported_types += 1
                log.info("  SKIP %s.%s (%s): unsupported compile type", owner, obj_name, obj_type)
                continue
            try:
                result = run_sql(obclient_cmd, compile_sql, timeout)
                error_msg = extract_execution_error(result)
                if not error_msg:
                    still_invalid = is_object_invalid(
                        obclient_cmd, timeout, owner, obj_name, obj_type, cache=invalid_status_cache
                    )
                    if still_invalid is False:
                        recompiled_this_round += 1
                        log.info("  OK %s.%s (%s)", owner, obj_name, obj_type)
                    elif still_invalid is True:
                        recompile_failed += 1
                        failed_this_round += 1
                        log.warning(
                            "  FAIL %s.%s (%s): still INVALID after COMPILE",
                            owner,
                            obj_name,
                            obj_type,
                        )
                    else:
                        log.warning(
                            "  WARN %s.%s (%s): 无法确认编译后状态，未计入成功",
                            owner,
                            obj_name,
                            obj_type,
                        )
                else:
                    recompile_failed += 1
                    failed_this_round += 1
                    log.warning(
                        "  FAIL %s.%s (%s): %s", owner, obj_name, obj_type, str(error_msg)[:100]
                    )
            except Exception as e:
                recompile_failed += 1
                failed_this_round += 1
                log.warning("  FAIL %s.%s (%s): %s", owner, obj_name, obj_type, str(e)[:100])

        total_recompiled += recompiled_this_round

        if recompiled_this_round == 0 and failed_this_round == 0:
            # No success and no transient failure signal in this round, stop retrying
            break

    # Final check
    final_invalid = query_invalid_objects(obclient_cmd, timeout, allowed_owners=allowed_owners)
    return RecompileSummary(
        total_recompiled, len(final_invalid), recompile_failed, unsupported_types
    )


def parse_args() -> argparse.Namespace:
    desc = textwrap.dedent(
        """\
        增强版修补脚本执行器 - 支持依赖感知排序和自动重编译

        新特性：
          --smart-order  : 启用依赖感知执行顺序（推荐）
          --recompile    : 自动重编译 INVALID 对象
          --max-retries  : 重编译最大重试次数（默认 5）
          --iterative    : 启用迭代执行模式，自动重试失败脚本（推荐用于VIEW）
          --max-rounds   : 最大迭代轮次（默认 10）
          --min-progress : 最小进展阈值（默认 1）
          --view-chain-autofix : 基于 VIEW 依赖链生成/执行修复计划
          --allow-table-create : 允许执行 table/ 建表脚本（默认关闭，防止误建空表）
          --safety-tiers : 按 safe/review/destructive/manual 分层选择执行（默认 safe,review）
          --plan-only    : 只验证本次会执行哪些脚本，不连接数据库、不执行 SQL
          --no-resume-ledger : 禁用 .fixup_state_ledger.json 跳过已完成文件/语句
          心跳输出        : run_fixup_heartbeat_<ts>.json 记录当前文件/语句、耗时和 timeout；
                            file 模式只承诺文件/进程级进度，statement 模式才有语句级进度
          注意: grants_deferred/ 默认跳过，需在补齐对象后显式执行
          注意: materialized_view/job/schedule 默认跳过，需核对后显式执行；即便显式配合 --iterative 也不会跨轮自动重试

        保留原有功能：
          --only-dirs    : 按子目录过滤
          --only-types   : 按对象类型过滤
          --glob         : 按文件名模式过滤

        项目信息：
          主页: {repo_url}
          反馈: {issues_url}
          版本: {version}
        """
    ).format(repo_url=REPO_URL, issues_url=REPO_ISSUES_URL, version=__version__)

    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "config",
        nargs="?",
        default=CONFIG_DEFAULT_PATH,
        help="config.ini path (default: config.ini)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    parser.add_argument(
        "--smart-order",
        action="store_true",
        help="Enable dependency-aware execution order (grants before dependent objects)",
    )

    parser.add_argument(
        "--recompile",
        action="store_true",
        help="Automatically recompile INVALID objects after execution",
    )

    parser.add_argument(
        "--max-retries",
        type=int,
        default=MAX_RECOMPILE_RETRIES,
        help=f"Maximum recompilation retries (default: {MAX_RECOMPILE_RETRIES})",
    )

    parser.add_argument(
        "--iterative",
        action="store_true",
        help="Enable iterative execution with automatic retry of failed scripts (recommended for VIEWs)",
    )

    parser.add_argument(
        "--view-chain-autofix",
        action="store_true",
        help="Generate per-view plans from VIEWs_chain and auto-execute them",
    )

    parser.add_argument(
        "--max-rounds",
        type=int,
        default=10,
        help="Maximum iteration rounds for --iterative mode (default: 10)",
    )

    parser.add_argument(
        "--min-progress",
        type=int,
        default=1,
        help="Minimum progress per round for --iterative mode (default: 1)",
    )

    parser.add_argument(
        "--only-dirs",
        action="append",
        help="Only execute scripts under these subdirectories (comma-separated)",
    )

    parser.add_argument(
        "--exclude-dirs",
        action="append",
        help="Skip these subdirectories (comma-separated)",
    )

    parser.add_argument(
        "--only-types",
        action="append",
        help="Only execute specific object types (e.g. TABLE,VIEW,INDEX)",
    )

    parser.add_argument(
        "--glob",
        dest="glob_patterns",
        action="append",
        help="Only execute scripts matching these glob patterns",
    )

    parser.add_argument(
        "--allow-table-create",
        action="store_true",
        help="Allow executing fixup_scripts/table/* (disabled by default for safety)",
    )
    parser.add_argument(
        "--safety-tiers",
        default=",".join(DEFAULT_FIXUP_SAFETY_TIERS),
        help=(
            "Comma-separated safety tiers to execute: safe,review,destructive,manual,all "
            "(default: safe,review)"
        ),
    )
    parser.add_argument(
        "--confirm-destructive",
        action="store_true",
        help="Required when --safety-tiers includes destructive",
    )
    parser.add_argument(
        "--confirm-manual",
        action="store_true",
        help="Required when --safety-tiers includes manual",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Collect and safety-filter fixup scripts without connecting to DB or executing SQL",
    )
    parser.add_argument(
        "--no-resume-ledger",
        action="store_true",
        help="Disable .fixup_state_ledger.json resume checks and execute matching pending SQL files normally",
    )

    return parser.parse_args()


def parse_csv_args(arg_list: List[str]) -> List[str]:
    values: List[str] = []
    for item in arg_list:
        if not item:
            continue
        values.extend([normalize_dir_filter(p) for p in item.split(",") if normalize_dir_filter(p)])
    return values


def format_safety_tiers(tiers: Set[str]) -> str:
    order = [SAFETY_TIER_SAFE, SAFETY_TIER_REVIEW, SAFETY_TIER_DESTRUCTIVE, SAFETY_TIER_MANUAL]
    return ",".join([item for item in order if item in tiers])


def parse_safety_tiers(raw_value: str) -> Set[str]:
    raw = (raw_value or "").strip()
    if not raw:
        return set(DEFAULT_FIXUP_SAFETY_TIERS)
    tokens = {normalize_dir_filter(item) for item in raw.split(",") if normalize_dir_filter(item)}
    if "all" in tokens:
        return set(FIXUP_SAFETY_TIER_VALUES)
    unknown = sorted(tokens - FIXUP_SAFETY_TIER_VALUES)
    if unknown:
        raise ConfigError(
            "未知 safety tier: {items}; 支持 safe,review,destructive,manual,all".format(
                items=", ".join(unknown)
            )
        )
    if not tokens:
        return set(DEFAULT_FIXUP_SAFETY_TIERS)
    return set(tokens)


def validate_safety_tier_confirmation(args: argparse.Namespace, tiers: Set[str]) -> None:
    if SAFETY_TIER_DESTRUCTIVE in tiers and not getattr(args, "confirm_destructive", False):
        raise ConfigError(
            "已选择 destructive safety tier，但缺少 --confirm-destructive；"
            "请先人工审核 SQL 后再显式确认。"
        )
    if SAFETY_TIER_MANUAL in tiers and not getattr(args, "confirm_manual", False):
        raise ConfigError(
            "已选择 manual safety tier，但缺少 --confirm-manual；"
            "manual-only family 需要人工动作上下文确认后再执行。"
        )


def get_selected_safety_tiers(args: argparse.Namespace) -> Set[str]:
    if hasattr(args, "safety_tiers_set"):
        return set(getattr(args, "safety_tiers_set") or DEFAULT_FIXUP_SAFETY_TIERS)
    if hasattr(args, "safety_tiers"):
        return parse_safety_tiers(getattr(args, "safety_tiers") or "")
    return set(FIXUP_SAFETY_TIER_VALUES)


def resolve_sql_file_safety(
    args: argparse.Namespace,
    fixup_dir: Path,
    sql_path: Path,
) -> Tuple[str, str]:
    cache: Dict[str, Tuple[str, str]] = getattr(args, "_fixup_safety_cache", {})
    key = str(sql_path)
    if key in cache:
        return cache[key]
    try:
        relative_path = sql_path.relative_to(fixup_dir)
    except ValueError:
        relative_path = sql_path
    try:
        sql_text = sql_path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        tier_reason = (
            SAFETY_TIER_MANUAL,
            f"unable to read SQL for safety classification: {exc}",
        )
        cache[key] = tier_reason
        setattr(args, "_fixup_safety_cache", cache)
        return tier_reason
    tier_reason = classify_fixup_safety(relative_path, sql_text)
    cache[key] = tier_reason
    setattr(args, "_fixup_safety_cache", cache)
    return tier_reason


def filter_files_by_safety_tier(
    args: argparse.Namespace,
    fixup_dir: Path,
    files_with_layer: List[Tuple[int, Path]],
) -> List[Tuple[int, Path]]:
    allowed_tiers = get_selected_safety_tiers(args)
    kept: List[Tuple[int, Path]] = []
    skipped_by_tier: Dict[str, int] = defaultdict(int)
    kept_by_tier: Dict[str, int] = defaultdict(int)
    for layer, sql_path in files_with_layer:
        tier, _reason = resolve_sql_file_safety(args, fixup_dir, sql_path)
        if tier in allowed_tiers:
            kept.append((layer, sql_path))
            kept_by_tier[tier] += 1
        else:
            skipped_by_tier[tier] += 1
    log.info("安全分层选择: %s", format_safety_tiers(allowed_tiers))
    if kept_by_tier:
        log.info(
            "安全分层保留: %s",
            ", ".join(f"{tier}={kept_by_tier[tier]}" for tier in sorted(kept_by_tier)),
        )
    if skipped_by_tier:
        log.warning(
            "安全分层跳过: %s",
            ", ".join(f"{tier}={skipped_by_tier[tier]}" for tier in sorted(skipped_by_tier)),
        )
    return kept


def log_sql_file_safety_context(
    args: argparse.Namespace,
    fixup_dir: Path,
    sql_path: Path,
    label: str,
) -> None:
    tier, reason = resolve_sql_file_safety(args, fixup_dir, sql_path)
    try:
        rel = sql_path.relative_to(fixup_dir)
    except ValueError:
        rel = sql_path
    if tier == SAFETY_TIER_MANUAL:
        log.warning("%s %s -> safety_tier=%s, reason=%s", label, rel, tier, reason)
    else:
        log.info("%s %s -> safety_tier=%s, reason=%s", label, rel, tier, reason)


def write_run_fixup_plan_validation(
    fixup_dir: Path,
    report_dir: Path,
    files_with_layer: List[Tuple[int, Path]],
    selected_tiers: Set[str],
) -> Tuple[Path, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(report_dir) if report_dir else Path(fixup_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = output_dir / f"run_fixup_plan_validation_{timestamp}.jsonl"
    summary_path = output_dir / f"run_fixup_plan_validation_{timestamp}.txt"
    counts: Dict[str, int] = defaultdict(int)
    rows: List[Dict[str, object]] = []
    for layer, sql_path in files_with_layer:
        record = build_fixup_plan_record(Path(fixup_dir), Path(sql_path))
        if not record:
            continue
        tier = str(record.get("safety_tier") or SAFETY_TIER_REVIEW)
        counts[tier] += 1
        record["execution_layer"] = layer
        record["selected_for_execution"] = tier in selected_tiers
        rows.append(record)
    with jsonl_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
    summary_lines = [
        "# run_fixup plan validation",
        f"fixup_dir: {fixup_dir}",
        f"selected_safety_tiers: {format_safety_tiers(selected_tiers)}",
        f"selected_files: {len(rows)}",
        "TIER | COUNT",
    ]
    for tier in [SAFETY_TIER_SAFE, SAFETY_TIER_REVIEW, SAFETY_TIER_DESTRUCTIVE, SAFETY_TIER_MANUAL]:
        summary_lines.append(f"{tier} | {counts.get(tier, 0)}")
    summary_lines.append("")
    summary_lines.append("No database connection or SQL execution was performed.")
    summary_path.write_text("\n".join(summary_lines).rstrip() + "\n", encoding="utf-8")
    return jsonl_path, summary_path


def main() -> None:
    """
    Main entry point with optional iterative fixup support.

    New --iterative flag enables multi-round execution with:
    - Automatic retry of failed scripts
    - Convergence detection
    - Error pattern analysis
    - Progress tracking across rounds
    """
    global FIXUP_OPERATION_TRACKER
    args = parse_args()
    try:
        args.safety_tiers_set = parse_safety_tiers(getattr(args, "safety_tiers", ""))
        validate_safety_tier_confirmation(args, args.safety_tiers_set)
        args._fixup_safety_cache = {}
    except ConfigError as exc:
        log.error("参数错误: %s", exc)
        sys.exit(2)
    if getattr(args, "iterative", False) and getattr(args, "view_chain_autofix", False):
        log.error("参数冲突: --iterative 和 --view-chain-autofix 不能同时启用。")
        sys.exit(2)
    config_arg = Path(args.config)

    # Parse filters
    only_dirs = parse_csv_args(args.only_dirs or [])
    exclude_dirs = parse_csv_args(args.exclude_dirs or [])
    only_types_raw = parse_csv_args(args.only_types or [])

    # Map types to directories
    mapped_dirs: List[str] = []
    unknown_types: List[str] = []
    for t in only_types_raw:
        key = t.upper().replace(" ", "_")
        mapped = TYPE_DIR_MAP.get(key)
        if mapped:
            mapped_dirs.append(mapped)
        else:
            unknown_types.append(t)

    if unknown_types:
        log.warning("未识别的对象类型: %s", ", ".join(unknown_types))

    if mapped_dirs:
        if only_dirs:
            merged = set(normalize_dir_filter(d) for d in only_dirs) | set(
                normalize_dir_filter(d) for d in mapped_dirs
            )
            only_dirs = sorted(merged)
        else:
            only_dirs = [normalize_dir_filter(d) for d in mapped_dirs]
    else:
        only_dirs = [normalize_dir_filter(d) for d in only_dirs] if only_dirs else []

    exclude_dirs = [normalize_dir_filter(d) for d in exclude_dirs]
    allowed_safety_tiers = set(getattr(args, "safety_tiers_set", DEFAULT_FIXUP_SAFETY_TIERS))
    default_excludes = {"constraint_validate_later"}
    if SAFETY_TIER_MANUAL not in allowed_safety_tiers:
        default_excludes.update(
            {
                "tables_unsupported",
                "unsupported",
                "grants_deferred",
                "sequence_restart",
                *MANUAL_REVIEW_DIRS,
            }
        )
    if SAFETY_TIER_DESTRUCTIVE not in allowed_safety_tiers:
        default_excludes.update({"cleanup_safe", "cleanup_semantic"})
    if not getattr(args, "allow_table_create", False):
        # Safety first: table create scripts are risky in migration workflows
        # because they can create empty target tables if OMS data load is skipped.
        default_excludes.add("table")
    exclude_set = set(exclude_dirs) | default_excludes
    if only_dirs:
        exclude_set = {
            item
            for item in exclude_set
            if not any(dir_filter_overlaps(item, include_item) for include_item in only_dirs)
        }
    if not getattr(args, "allow_table_create", False):
        # Keep table excluded unless explicit opt-in, even with --only-dirs table.
        exclude_set.add("table")
    exclude_dirs = sorted(exclude_set)

    if any(dir_filter_overlaps("table", item) for item in only_dirs) and not getattr(
        args, "allow_table_create", False
    ):
        log.warning(
            "检测到 --only-dirs/--only-types 包含 table，但默认安全策略已禁用 table 执行。"
            "如需执行建表脚本，请显式添加 --allow-table-create。"
        )
    if not getattr(args, "allow_table_create", False):
        log.warning("安全模式: 默认跳过 fixup_scripts/table/（防止误建空表）。")
    if SAFETY_TIER_MANUAL not in allowed_safety_tiers and not any(
        dir_filter_overlaps("grants_deferred", item) for item in only_dirs
    ):
        log.warning("安全模式: 默认跳过 fixup_scripts/grants_deferred/（需对象补齐后再执行）。")
    if SAFETY_TIER_MANUAL not in allowed_safety_tiers and not any(
        dir_filter_overlaps("sequence_restart", item) for item in only_dirs
    ):
        log.warning(
            "安全模式: 默认跳过 fixup_scripts/sequence_restart/（值同步 SQL 需确认 LAST_NUMBER 与执行时机后再执行）。"
        )
    if SAFETY_TIER_MANUAL not in allowed_safety_tiers and not any(
        any(dir_filter_overlaps(dir_name, item) for item in only_dirs)
        for dir_name in MANUAL_REVIEW_DIRS
    ):
        log.warning(
            "安全模式: 默认跳过 fixup_scripts/materialized_view|job|schedule/（manual-only family 需核对定义与人工事项后再显式执行）。"
        )
    if SAFETY_TIER_DESTRUCTIVE not in allowed_safety_tiers and not any(
        dir_filter_overlaps("cleanup_safe", item) for item in only_dirs
    ):
        log.warning(
            "安全模式: 默认跳过 fixup_scripts/cleanup_safe/（显式审核后再执行 destructive 清理 SQL）。"
        )
    if SAFETY_TIER_DESTRUCTIVE not in allowed_safety_tiers and not any(
        dir_filter_overlaps("cleanup_semantic", item) for item in only_dirs
    ):
        log.warning(
            "安全模式: 默认跳过 fixup_scripts/cleanup_semantic/（语义级 destructive 约束清理需显式审核后执行）。"
        )

    # Load configuration
    try:
        ob_cfg, fixup_dir, repo_root, log_level, report_dir, fixup_settings, max_sql_file_bytes = (
            load_ob_config(config_arg.resolve())
        )
    except ConfigError as exc:
        log.error("配置错误: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.error("致命错误: 无法读取配置: %s", exc)
        sys.exit(1)
    ob_cfg["selected_safety_tiers"] = format_safety_tiers(args.safety_tiers_set)

    level_name = (log_level or "AUTO").strip().upper()
    level = resolve_console_log_level(level_name)
    if level_name not in ("AUTO", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        log.warning("未知 log_level: %s，使用 %s", level_name, logging.getLevelName(level))
    if level_name == "AUTO":
        log.info("日志级别: console=%s (AUTO)", logging.getLevelName(level))
    else:
        log.info("日志级别: console=%s", logging.getLevelName(level))
    set_console_log_level(level)
    log.info("run_fixup v%s", __version__)
    log.info("安全分层执行: %s", format_safety_tiers(args.safety_tiers_set))
    if SAFETY_TIER_DESTRUCTIVE in args.safety_tiers_set:
        log.warning("已显式确认 destructive safety tier: %s", bool(args.confirm_destructive))
    if SAFETY_TIER_MANUAL in args.safety_tiers_set:
        log.warning("已显式确认 manual safety tier: %s", bool(args.confirm_manual))
    if getattr(args, "plan_only", False):
        FIXUP_OPERATION_TRACKER = None
        log.info("--plan-only: 跳过运行心跳和 timeout summary 输出。")
    else:
        FIXUP_OPERATION_TRACKER = setup_fixup_runtime_observability(
            ob_cfg, fixup_dir, report_dir, fixup_settings
        )
    diagnostic_parts = [
        "python3",
        "diagnostic_bundle.py",
        "--run-dir",
        str(report_dir),
        "--config",
        str(config_arg.resolve()),
    ]
    if FIXUP_OPERATION_TRACKER is not None:
        diagnostic_parts.extend(["--pid", str(os.getpid()), "--hang"])
    log.info("诊断包命令: %s", " ".join(shlex.quote(part) for part in diagnostic_parts))
    notice_state_path: Optional[Path] = None
    notice_state: Optional[Dict[str, object]] = None
    notices_to_mark_seen: List[RuntimeNotice] = []
    manual_actions_report_path: Optional[Path] = None
    manual_actions_for_run: List[ManualActionNoticeRow] = []
    try:
        notice_state_path, notice_state = load_notice_state(config_arg.resolve().parent)
        runtime_notices = build_run_fixup_change_notices(args, fixup_dir, only_dirs)
        notices_to_mark_seen = select_unseen_notices(notice_state, runtime_notices)
        log_change_notices_block(notices_to_mark_seen)
        manual_actions_report_path = find_latest_manual_actions_report(report_dir)
        manual_actions_for_run = select_relevant_manual_actions(
            load_manual_actions_report(manual_actions_report_path),
            only_dirs,
        )
        log_manual_action_preflight(manual_actions_report_path, manual_actions_for_run)
    except Exception as exc:
        log.debug("加载运行时提醒状态失败，已跳过: %s", exc)

    # Check if iterative mode requested via config or args
    iterative_mode = getattr(args, "iterative", False)
    if getattr(args, "plan_only", False) and iterative_mode:
        log.warning("--plan-only 不执行迭代轮次；本次只验证当前 fixup_scripts 选择结果。")
        iterative_mode = False
    max_rounds = getattr(args, "max_rounds", 10)
    min_progress = getattr(args, "min_progress", 1)
    hot_reload_runtime = init_fixup_hot_reload_runtime(config_arg.resolve())
    if hot_reload_runtime and hot_reload_runtime.mode == "round" and not iterative_mode:
        log.warning("config_hot_reload_mode=round 仅在 --iterative 下生效；本次运行不会热加载。")

    try:
        with acquire_fixup_run_lock(fixup_dir):
            if getattr(args, "view_chain_autofix", False):
                run_view_chain_autofix(
                    args,
                    ob_cfg,
                    fixup_dir,
                    repo_root,
                    report_dir,
                    only_dirs,
                    exclude_dirs,
                    fixup_settings,
                    max_sql_file_bytes,
                )
            elif iterative_mode:
                run_iterative_fixup(
                    args,
                    ob_cfg,
                    fixup_dir,
                    repo_root,
                    report_dir,
                    only_dirs,
                    exclude_dirs,
                    fixup_settings,
                    max_sql_file_bytes,
                    max_rounds,
                    min_progress,
                    hot_reload_runtime=hot_reload_runtime,
                )
            else:
                run_single_fixup(
                    args,
                    ob_cfg,
                    fixup_dir,
                    repo_root,
                    report_dir,
                    only_dirs,
                    exclude_dirs,
                    fixup_settings,
                    max_sql_file_bytes,
                )
    except ConfigError as exc:
        log.error("执行失败: %s", exc)
        sys.exit(1)
    finally:
        if FIXUP_OPERATION_TRACKER is not None:
            FIXUP_OPERATION_TRACKER.close()
            FIXUP_OPERATION_TRACKER = None
        if notice_state_path and notice_state is not None and notices_to_mark_seen:
            try:
                persist_seen_notices(
                    notice_state_path, notice_state, __version__, notices_to_mark_seen
                )
            except Exception as exc:
                log.debug("持久化运行时提醒状态失败，已忽略: %s", exc)


def run_single_fixup(
    args,
    ob_cfg: Dict[str, str],
    fixup_dir: Path,
    repo_root: Path,
    report_dir: Path,
    only_dirs: List[str],
    exclude_dirs: List[str],
    fixup_settings: FixupAutoGrantSettings,
    max_sql_file_bytes: Optional[int],
) -> None:
    """Original single-round fixup execution (backward compatible)."""

    log_section("修补脚本执行器")
    log.info("配置文件: %s", Path(args.config).resolve())
    log.info("日志级别: %s", logging.getLevelName(logging.getLogger().level))
    log.info("项目主页: %s (问题反馈: %s)", REPO_URL, REPO_ISSUES_URL)

    done_dir = fixup_dir / DONE_DIR_NAME
    done_dir.mkdir(exist_ok=True)
    state_ledger = None if getattr(args, "no_resume_ledger", False) else FixupStateLedger(fixup_dir)
    if state_ledger is None:
        log.warning("[STATE] 已禁用 ledger resume: --no-resume-ledger")

    # Collect SQL files
    files_with_layer = collect_sql_files_by_layer(
        fixup_dir,
        smart_order=args.smart_order,
        include_dirs=set(only_dirs) if only_dirs else None,
        exclude_dirs=set(exclude_dirs),
        glob_patterns=args.glob_patterns or None,
    )
    files_with_layer = filter_files_by_safety_tier(args, fixup_dir, files_with_layer)

    if getattr(args, "plan_only", False):
        jsonl_path, summary_path = write_run_fixup_plan_validation(
            fixup_dir, report_dir, files_with_layer, get_selected_safety_tiers(args)
        )
        log.info("[PLAN_ONLY] 已写入执行计划验证: %s", jsonl_path)
        log.info("[PLAN_ONLY] 已写入执行计划摘要: %s", summary_path)
        return

    if not files_with_layer:
        log.warning("目录 %s 中未找到任何 *.sql 文件。", fixup_dir)
        return
    recompile_owners = infer_recompile_owners(files_with_layer)

    obclient_cmd = build_obclient_command(ob_cfg)
    ob_timeout = resolve_timeout_value(ob_cfg.get("timeout"))
    ok_conn, conn_err = check_obclient_connectivity(obclient_cmd, ob_timeout)
    if not ok_conn:
        log.error("OBClient 连接检查失败: %s", conn_err)
        log.error("请确认网络连通性/账号权限/obclient 可用性后重试。")
        sys.exit(1)
    precheck_summary = build_fixup_precheck_summary(
        ob_cfg, obclient_cmd, ob_timeout, files_with_layer
    )
    precheck_report = write_fixup_precheck_report(fixup_dir, precheck_summary)
    log_fixup_precheck(precheck_summary, precheck_report)
    auto_grant_ctx = init_auto_grant_context(
        fixup_settings, report_dir, fixup_dir, exclude_dirs, obclient_cmd, ob_timeout
    )

    total_scripts = len(files_with_layer)
    width = len(str(total_scripts)) or 1
    results: List[ScriptResult] = []
    error_entries: List[ErrorReportEntry] = []
    error_truncated = False
    exec_stats = new_exec_mode_stats()

    log_section("执行配置")
    log.info("目录: %s", fixup_dir)
    log.info("模式: %s", "依赖感知排序 (SMART ORDER)" if args.smart_order else "标准优先级排序")
    log.info(
        "执行粒度: mode=%s, file_fallback=%s",
        fixup_settings.exec_mode,
        str(bool(fixup_settings.exec_file_fallback)).lower(),
    )
    if args.recompile:
        log.info("重编译: 启用 (最多 %d 次重试)", args.max_retries)
    if only_dirs:
        log.info("子目录过滤: %s", sorted(set(only_dirs)))
    if exclude_dirs:
        log.info("跳过子目录: %s", sorted(set(exclude_dirs)))
    if args.glob_patterns:
        log.info("文件过滤: %s", args.glob_patterns)
    log.info("读取 SQL 文件: %d", total_scripts)

    log_section("开始执行")

    # Execute scripts
    current_layer = -1
    for idx, (layer, sql_path) in enumerate(files_with_layer, start=1):
        if args.smart_order and layer != current_layer:
            current_layer = layer
            layer_name = "未知层" if layer == 999 else f"第 {layer} 层"
            log_subsection(f"执行层 {layer_name}")

        relative_path = sql_path.relative_to(repo_root)
        label = format_progress_label(idx, total_scripts, width)
        log_sql_file_safety_context(args, fixup_dir, sql_path, label)

        if is_grant_dir(sql_path.parent.name):
            bump_exec_mode_stat(exec_stats, "grants_statement")
            result, summary, _removed, _kept, truncated = execute_grant_file_with_prune(
                obclient_cmd,
                sql_path,
                repo_root,
                done_dir,
                ob_timeout,
                layer,
                label,
                error_entries,
                DEFAULT_ERROR_REPORT_LIMIT,
                max_sql_file_bytes,
                state_ledger=state_ledger,
            )
            error_truncated = error_truncated or truncated
            if result.status == "FAILED":
                first_error = summary.failures[0].error if summary.failures else result.message
                error_type = classify_sql_error(first_error)
                retry_target = None
                if summary.failures:
                    retry_target = infer_permission_retry_target(
                        summary.failures[0].statement, relative_path
                    )
                if auto_grant_ctx and error_type == FailureType.PERMISSION_DENIED and retry_target:
                    retry_full, retry_type, retry_privs = retry_target
                    applied, _blocked = execute_auto_grant_for_object(
                        auto_grant_ctx,
                        retry_full,
                        retry_type,
                        f"{label} (grant)",
                        required_privileges_override=retry_privs,
                    )
                    if applied > 0:
                        refresh_result = execute_view_refresh_before_retry(
                            fixup_dir=fixup_dir,
                            object_full=retry_full,
                            object_type=retry_type,
                            obclient_cmd=obclient_cmd,
                            done_dir=done_dir,
                            timeout=ob_timeout,
                            layer=layer,
                            label=f"{label} (grant)",
                            max_sql_file_bytes=max_sql_file_bytes,
                            state_ledger=state_ledger,
                            exec_mode=fixup_settings.exec_mode,
                            exec_file_fallback=fixup_settings.exec_file_fallback,
                            exec_stats=exec_stats,
                        )
                        if refresh_result:
                            results.append(refresh_result)
                        retry_result, retry_summary, _removed2, _kept2, truncated2 = (
                            execute_grant_file_with_prune(
                                obclient_cmd,
                                sql_path,
                                repo_root,
                                done_dir,
                                ob_timeout,
                                layer,
                                f"{label} (retry)",
                                error_entries,
                                DEFAULT_ERROR_REPORT_LIMIT,
                                max_sql_file_bytes,
                                state_ledger=state_ledger,
                            )
                        )
                        error_truncated = error_truncated or truncated2
                        results.append(retry_result)
                        continue
            results.append(result)
        else:
            obj_type = DIR_OBJECT_TYPE_MAP.get(sql_path.parent.name.lower())
            obj_schema, obj_name = parse_object_identity_from_path(sql_path)
            obj_full = f"{obj_schema}.{obj_name}" if obj_schema and obj_name else None
            if auto_grant_ctx and obj_full and obj_type:
                execute_auto_grant_for_object(auto_grant_ctx, obj_full, obj_type, label)
            result, summary = execute_script_with_summary(
                obclient_cmd,
                sql_path,
                repo_root,
                done_dir,
                ob_timeout,
                layer,
                label,
                max_sql_file_bytes,
                state_ledger=state_ledger,
                exec_mode=fixup_settings.exec_mode,
                exec_file_fallback=fixup_settings.exec_file_fallback,
                exec_stats=exec_stats,
            )
            if result.status == "SUCCESS":
                results.append(result)
                continue

            if result.status in ("SKIPPED", "ERROR"):
                results.append(result)
                if result.status == "ERROR":
                    for failure in summary.failures:
                        if error_truncated:
                            break
                        error_truncated = not record_error_entry(
                            error_entries,
                            DEFAULT_ERROR_REPORT_LIMIT,
                            relative_path,
                            failure.index,
                            failure.statement,
                            failure.error,
                        )
                continue

            first_error = summary.failures[0].error if summary.failures else result.message
            error_type = classify_sql_error(first_error)
            required_priv_override = None
            if summary.failures and obj_full and obj_type:
                required_priv_override = infer_required_privileges_from_failed_statement(
                    summary.failures[0].statement,
                    obj_full,
                    obj_type,
                )
            handled = False
            if (
                auto_grant_ctx
                and obj_full
                and obj_type
                and error_type == FailureType.PERMISSION_DENIED
            ):
                applied, _blocked = execute_auto_grant_for_object(
                    auto_grant_ctx,
                    obj_full,
                    obj_type,
                    f"{label} (grant)",
                    required_privileges_override=required_priv_override,
                )
                handled = applied > 0

            if handled:
                retry_result, retry_summary = execute_script_with_summary(
                    obclient_cmd,
                    sql_path,
                    repo_root,
                    done_dir,
                    ob_timeout,
                    layer,
                    f"{label} (retry)",
                    max_sql_file_bytes,
                    state_ledger=state_ledger,
                    exec_mode=fixup_settings.exec_mode,
                    exec_file_fallback=fixup_settings.exec_file_fallback,
                    exec_stats=exec_stats,
                )
                results.append(retry_result)
                if retry_result.status == "FAILED":
                    for failure in retry_summary.failures:
                        if error_truncated:
                            break
                        error_truncated = not record_error_entry(
                            error_entries,
                            DEFAULT_ERROR_REPORT_LIMIT,
                            relative_path,
                            failure.index,
                            failure.statement,
                            failure.error,
                        )
                continue

            results.append(result)
            for failure in summary.failures:
                if error_truncated:
                    break
                error_truncated = not record_error_entry(
                    error_entries,
                    DEFAULT_ERROR_REPORT_LIMIT,
                    relative_path,
                    failure.index,
                    failure.statement,
                    failure.error,
                )

    # Recompilation phase
    total_recompiled = 0
    remaining_invalid = 0
    recompile_failed = 0
    unsupported_recompile_types = 0
    if args.recompile:
        log_subsection("重编译阶段")
        recomp_summary = recompile_invalid_objects(
            obclient_cmd, ob_timeout, args.max_retries, allowed_owners=recompile_owners
        )
        total_recompiled = recomp_summary.total_recompiled
        remaining_invalid = recomp_summary.remaining_invalid
        recompile_failed = recomp_summary.recompile_failed
        unsupported_recompile_types = recomp_summary.unsupported_types

    if auto_grant_ctx:
        log_subsection("自动补权限统计")
        log.info("计划语句 : %d", auto_grant_ctx.stats.planned)
        log.info("执行成功 : %d", auto_grant_ctx.stats.executed)
        log.info("执行失败 : %d", auto_grant_ctx.stats.failed)
        log.info("阻断提示 : %d", auto_grant_ctx.stats.blocked)
        log.info("范围跳过 : %d", auto_grant_ctx.stats.skipped)

    # Summary
    executed = sum(1 for r in results if r.status != "SKIPPED")
    success = sum(1 for r in results if r.status == "SUCCESS")
    failed = sum(1 for r in results if r.status in ("FAILED", "ERROR"))
    skipped = sum(1 for r in results if r.status == "SKIPPED")

    log_section("执行结果汇总")
    log.info("扫描脚本数 : %d", total_scripts)
    log.info("实际执行数 : %d", executed)
    log.info("成功       : %d", success)
    log.info("失败       : %d", failed)
    log.info("跳过       : %d", skipped)
    log_exec_mode_summary(exec_stats, fixup_settings.exec_mode, fixup_settings.exec_file_fallback)

    if args.recompile:
        log_subsection("重编译统计")
        log.info("重编译成功 : %d", total_recompiled)
        log.info("重编译失败 : %d", recompile_failed)
        log.info("不支持重编译类型 : %d", unsupported_recompile_types)
        log.info("仍为INVALID: %d", remaining_invalid)
        if remaining_invalid > 0:
            log.info("提示: 运行 'SELECT * FROM DBA_OBJECTS WHERE STATUS=\\'INVALID\\';' 查看详情")

    # Analyze failures
    failures_by_type = analyze_failure_patterns(results)
    if failures_by_type:
        log_failure_analysis(failures_by_type)

    # Detailed table
    if results:
        log_subsection("详细结果")

        # Group by status
        by_status = defaultdict(list)
        for r in results:
            by_status[r.status].append(r)

        for status in ["SUCCESS", "FAILED", "ERROR", "SKIPPED"]:
            items = by_status.get(status, [])
            if not items:
                continue

            status_label = {
                "SUCCESS": "✓ 成功",
                "FAILED": "✗ 失败",
                "ERROR": "✗ 错误",
                "SKIPPED": "○ 跳过",
            }[status]

            log.info("%s (%d)", status_label, len(items))
            for item in items[:20]:  # Limit to first 20
                msg = safe_first_line(item.message, 100, "")
                log.info("  %s", item.path)
                if msg:
                    log.info("    %s", msg)

            if len(items) > 20:
                log.info("  ... 还有 %d 个", len(items) - 20)

    report_path = write_error_report(
        error_entries, fixup_dir, DEFAULT_ERROR_REPORT_LIMIT, error_truncated
    )
    if report_path:
        log.info("错误报告已输出: %s", report_path)
    if state_ledger:
        log_fixup_state_ledger_summary(state_ledger)

    log_section("执行结束")

    exit_code = 0 if failed == 0 else 1
    sys.exit(exit_code)


def run_view_chain_autofix(
    args,
    ob_cfg: Dict[str, str],
    fixup_dir: Path,
    repo_root: Path,
    report_dir: Path,
    only_dirs: List[str],
    exclude_dirs: List[str],
    fixup_settings: FixupAutoGrantSettings,
    max_sql_file_bytes: Optional[int],
) -> None:
    log_section("VIEW 链路自动修复")
    log.info("配置文件: %s", Path(args.config).resolve())
    log.info("报告目录: %s", report_dir)
    log.info("项目主页: %s (问题反馈: %s)", REPO_URL, REPO_ISSUES_URL)
    log.info(
        "执行粒度: mode=%s, file_fallback=%s",
        fixup_settings.exec_mode,
        str(bool(fixup_settings.exec_file_fallback)).lower(),
    )
    if only_dirs:
        log.info("子目录过滤: %s", sorted(set(only_dirs)))
    if exclude_dirs:
        log.info("跳过子目录: %s", sorted(set(exclude_dirs)))

    chain_path = find_latest_view_chain_file(report_dir)
    if not chain_path:
        log.error("未找到 VIEWs_chain_*.txt，请先生成依赖链报告。")
        sys.exit(1)

    chains_by_view = parse_view_chain_file(chain_path)
    if not chains_by_view:
        log.error("依赖链文件解析为空: %s", chain_path)
        sys.exit(1)
    chains_meta_by_view = parse_view_chain_file_meta(chain_path)

    files_with_layer = collect_sql_files_by_layer(
        fixup_dir,
        smart_order=False,
        include_dirs=set(only_dirs) if only_dirs else None,
        exclude_dirs=set(exclude_dirs),
        glob_patterns=args.glob_patterns or None,
    )
    files_with_layer = filter_files_by_safety_tier(args, fixup_dir, files_with_layer)
    if getattr(args, "plan_only", False):
        jsonl_path, summary_path = write_run_fixup_plan_validation(
            fixup_dir, report_dir, files_with_layer, get_selected_safety_tiers(args)
        )
        log.info("[PLAN_ONLY] 已写入执行计划验证: %s", jsonl_path)
        log.info("[PLAN_ONLY] 已写入执行计划摘要: %s", summary_path)
        return
    object_index, name_index = build_fixup_object_index(files_with_layer)
    done_dir = fixup_dir / DONE_DIR_NAME
    done_object_index: Dict[Tuple[str, str], List[Path]] = {}
    done_name_index: Dict[str, List[Path]] = {}
    done_files = collect_sql_files_from_root(
        done_dir,
        include_dirs=set(only_dirs) if only_dirs else None,
        exclude_dirs=set(exclude_dirs) if exclude_dirs else None,
    )
    if done_files:
        done_object_index, done_name_index = build_fixup_object_index(
            [(0, path) for path in done_files]
        )

    # 根据用户 --only-dirs 决定使用哪些 grant 目录
    only_dirs_set = {
        normalize_dir_filter(item) for item in (only_dirs or []) if normalize_dir_filter(item)
    }
    use_grants_miss = (
        not only_dirs_set
        or any(dir_filter_overlaps("grants_miss", item) for item in only_dirs_set)
        or any(dir_filter_overlaps("grants", item) for item in only_dirs_set)
    )
    use_grants_all = not only_dirs_set or any(
        dir_filter_overlaps("grants_all", item) for item in only_dirs_set
    )

    grant_index_miss = build_grant_index(
        fixup_dir, set(exclude_dirs), include_dirs={"grants_miss"} if use_grants_miss else set()
    )
    grant_index_all = build_grant_index(
        fixup_dir, set(exclude_dirs), include_dirs={"grants_all"} if use_grants_all else set()
    )

    plan_dir = fixup_dir / "view_chain_plans"
    sql_dir = fixup_dir / "view_chain_sql"
    plan_dir.mkdir(parents=True, exist_ok=True)
    sql_dir.mkdir(parents=True, exist_ok=True)
    state_ledger = None if getattr(args, "no_resume_ledger", False) else FixupStateLedger(fixup_dir)
    if state_ledger is None:
        log.warning("[STATE] 已禁用 ledger resume: --no-resume-ledger")

    obclient_cmd = build_obclient_command(ob_cfg)
    ob_timeout = resolve_timeout_value(ob_cfg.get("timeout"))
    ok_conn, conn_err = check_obclient_connectivity(obclient_cmd, ob_timeout)
    if not ok_conn:
        log.error("OBClient 连接检查失败: %s", conn_err)
        log.error("请确认网络连通性/账号权限/obclient 可用性后重试。")
        sys.exit(1)
    precheck_summary = build_fixup_precheck_summary(
        ob_cfg, obclient_cmd, ob_timeout, files_with_layer
    )
    precheck_report = write_fixup_precheck_report(fixup_dir, precheck_summary)
    log_fixup_precheck(precheck_summary, precheck_report)

    roles_cache: Dict[str, Set[str]] = LimitedCache(fixup_settings.cache_limit)
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]] = LimitedCache(fixup_settings.cache_limit)
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]] = LimitedCache(
        fixup_settings.cache_limit
    )
    sys_privs_cache: Dict[str, Set[str]] = LimitedCache(fixup_settings.cache_limit)
    planned_statements: Set[str] = set()
    planned_object_privs: Set[Tuple[str, str, str]] = set()
    planned_object_privs_with_option: Set[Tuple[str, str, str]] = set()
    planned_sys_privs: Set[Tuple[str, str]] = set()

    total_views = len(chains_by_view)
    view_width = len(str(total_views)) or 1
    blocked_views = 0
    failed_views = 0
    partial_views = 0
    executed_views = 0
    skipped_views = 0
    view_results: List[Tuple[str, str, List[str]]] = []
    exec_stats = new_exec_mode_stats()

    log.info("读取 VIEW 依赖链: %d", total_views)

    allow_fallback = bool(fixup_settings.fallback)

    for idx, view_full in enumerate(sorted(chains_by_view.keys()), start=1):
        exists_cache: Dict[Tuple[str, str], bool] = {}
        view_key = normalize_identifier(view_full)
        planned_objects: Set[Tuple[str, str]] = set()
        label = format_progress_label(idx, total_views, view_width)
        chains = chains_by_view.get(view_full) or []
        root_type = chains[0][0][1] if chains and chains[0] else "VIEW"
        root_exists = check_object_exists(
            obclient_cmd,
            ob_timeout,
            view_key,
            root_type,
            exists_cache,
            planned_objects,
            use_planned=False,
        )
        skipped = root_exists is True
        blocked = False
        plan_lines: List[str] = []
        sql_lines: List[str] = []
        if skipped:
            plan_lines.append("SKIP: 视图已存在，跳过自动修复。")
        else:
            plan_lines, sql_lines, blocked = build_view_chain_plan(
                view_key,
                chains,
                obclient_cmd,
                ob_timeout,
                object_index,
                name_index,
                done_object_index,
                done_name_index,
                grant_index_miss,
                grant_index_all,
                allow_fallback,
                repo_root,
                exists_cache,
                roles_cache,
                tab_privs_cache,
                tab_privs_grantable_cache,
                sys_privs_cache,
                planned_statements,
                planned_object_privs,
                planned_object_privs_with_option,
                planned_sys_privs,
                planned_objects,
                max_sql_file_bytes,
            )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plan_path = plan_dir / f"{view_key}.plan.txt"
        sql_path = sql_dir / f"{view_key}.sql"
        chain_summary = [
            f"# View: {view_key}",
            f"# Chain file: {chain_path}",
            f"# Generated: {timestamp}",
            "# Chains:",
        ]
        chain_meta_list = chains_meta_by_view.get(view_full) or []
        for chain_idx, chain in enumerate(chains):
            meta_chain = chain_meta_list[chain_idx] if chain_idx < len(chain_meta_list) else []
            display_nodes: List[str] = []
            for node_idx, node in enumerate(chain):
                node_name, node_type = node
                extra_meta: Tuple[str, ...] = ()
                if node_idx < len(meta_chain):
                    extra_meta = meta_chain[node_idx][2]
                if extra_meta:
                    display_nodes.append(f"{node_name}({node_type}|{'|'.join(extra_meta)})")
                else:
                    display_nodes.append(f"{node_name}({node_type})")
            chain_summary.append("# - " + " -> ".join(display_nodes))

        plan_content = (
            "\n".join(
                ["# VIEW chain autofix plan"] + chain_summary + ["", "# Steps:"] + plan_lines
            ).rstrip()
            + "\n"
        )
        plan_path.write_text(plan_content, encoding="utf-8")

        sql_header = [
            "-- VIEW chain autofix SQL",
            f"-- View: {view_key}",
            f"-- Chain file: {chain_path}",
            f"-- Generated: {timestamp}",
        ]
        if skipped:
            sql_header.append("-- SKIPPED: view already exists")
        if blocked:
            sql_header.append("-- BLOCKED: see plan for details")
        sql_text = "\n".join(sql_header + [""] + sql_lines).rstrip() + "\n"
        sql_path.write_text(sql_text, encoding="utf-8")

        log.info("%s [VIEW_CHAIN] %s plan=%s sql=%s", label, view_key, plan_path, sql_path)

        if skipped:
            skipped_views += 1
            status = classify_view_chain_status(blocked, True, root_exists, 0)
            view_results.append((view_key, status, ["view exists"]))
            log.info("%s [VIEW_CHAIN] %s skipped (exists).", label, view_key)
            continue
        if blocked:
            blocked_views += 1
            status = classify_view_chain_status(True, False, None, 0)
            reasons = [line for line in plan_lines if line.startswith("BLOCK:")]
            view_results.append((view_key, status, reasons))
            log.warning("%s [VIEW_CHAIN] %s blocked, skip auto-exec.", label, view_key)
            continue
        if not sql_lines:
            status = classify_view_chain_status(False, False, root_exists, 0)
            view_results.append((view_key, status, ["no statements"]))
            log.info("%s [VIEW_CHAIN] %s 无需执行。", label, view_key)
            continue

        try:
            relative_path = sql_path.relative_to(repo_root)
        except ValueError:
            relative_path = sql_path.resolve()
        fingerprint = FixupStateLedger.fingerprint(sql_text or "")
        if state_ledger and state_ledger.is_completed(relative_path, fingerprint):
            skipped_views += 1
            status = classify_view_chain_status(False, True, root_exists, 0)
            view_results.append((view_key, status, ["state ledger hit"]))
            log.warning(
                "%s [VIEW_CHAIN] %s 跳过执行：状态账本命中 (%s)。", label, view_key, relative_path
            )
            continue

        summary = execute_sql_with_mode(
            obclient_cmd,
            sql_text,
            ob_timeout,
            exec_mode=resolve_script_exec_mode(fixup_settings.exec_mode, sql_path),
            exec_file_fallback=fixup_settings.exec_file_fallback,
            exec_stats=exec_stats,
            context_label=f"{label} [VIEW_CHAIN] {view_key}",
        )
        invalidate_exists_cache(exists_cache, planned_objects | {(view_key, root_type)})
        post_exists = check_object_exists(
            obclient_cmd,
            ob_timeout,
            view_key,
            root_type,
            exists_cache,
            planned_objects,
            use_planned=False,
        )
        status = classify_view_chain_status(
            blocked=False,
            skipped=False,
            view_exists=post_exists,
            failure_count=len(summary.failures),
        )
        reasons: List[str] = []
        for failure in summary.failures:
            code = parse_error_code(failure.error)
            msg = " ".join((failure.error or "").split())
            if len(msg) > 160:
                msg = msg[:160] + "..."
            reasons.append(f"STMT {failure.index} {code} {msg}")
        if post_exists is False and not summary.failures:
            reasons.append("view missing after execution")
        view_results.append((view_key, status, reasons))

        if status == "SUCCESS":
            if state_ledger:
                state_ledger.mark_completed(relative_path, fingerprint, "VIEW_CHAIN_SUCCESS")
            executed_views += 1
            log.info(
                "%s [VIEW_CHAIN] %s 执行成功 (%d statements)。", label, view_key, summary.statements
            )
        elif status == "PARTIAL":
            partial_views += 1
            log.warning(
                "%s [VIEW_CHAIN] %s 部分成功 (%d/%d statements)。",
                label,
                view_key,
                len(summary.failures),
                summary.statements,
            )
        else:
            failed_views += 1
            log.warning(
                "%s [VIEW_CHAIN] %s 执行失败 (%d/%d statements)。",
                label,
                view_key,
                len(summary.failures),
                summary.statements,
            )

    if state_ledger:
        log_fixup_state_ledger_summary(state_ledger)
    log_section("VIEW 链路修复完成")
    log.info("视图总数: %d", total_views)
    log.info("执行成功: %d", executed_views)
    log.info("部分成功: %d", partial_views)
    log.info("跳过已存在: %d", skipped_views)
    log.info("阻塞跳过: %d", blocked_views)
    log.info("执行失败: %d", failed_views)
    log_exec_mode_summary(exec_stats, fixup_settings.exec_mode, fixup_settings.exec_file_fallback)

    if view_results:
        log_subsection("VIEW 链路结果详情")
        for view_key, status, reasons in view_results:
            if status == "SUCCESS":
                continue
            log.warning("  %s [%s]", view_key, status)
            for reason in reasons[:5]:
                log.warning("    - %s", reason)
            if len(reasons) > 5:
                log.warning("    - ... 还有 %d 条", len(reasons) - 5)

    exit_code = 0 if failed_views == 0 and blocked_views == 0 and partial_views == 0 else 1
    sys.exit(exit_code)


def run_iterative_fixup(
    args,
    ob_cfg: Dict[str, str],
    fixup_dir: Path,
    repo_root: Path,
    report_dir: Path,
    only_dirs: List[str],
    exclude_dirs: List[str],
    fixup_settings: FixupAutoGrantSettings,
    max_sql_file_bytes: Optional[int],
    max_rounds: int = 10,
    min_progress: int = 1,
    hot_reload_runtime: Optional[FixupHotReloadRuntime] = None,
) -> None:
    """
    Multi-round iterative fixup execution with automatic retry.

    Solves the VIEW dependency problem where 800 DDLs fail on first run
    because dependent objects don't exist yet.

    Args:
        args: Command line arguments
        ob_cfg: OceanBase connection config
        fixup_dir: Directory containing fixup scripts
        repo_root: Repository root path
        only_dirs: Directories to include
        exclude_dirs: Directories to exclude
        max_rounds: Maximum iteration rounds (default: 10)
        min_progress: Minimum progress per round (default: 1)
    """
    log_section("修补脚本执行器 (迭代模式)")
    log.info("配置文件: %s", Path(args.config).resolve())
    log.info("日志级别: %s", logging.getLevelName(logging.getLogger().level))
    log.info("最大轮次: %d", max_rounds)
    log.info("最小进展: 每轮至少 %d 个成功", min_progress)
    log.info("项目主页: %s (问题反馈: %s)", REPO_URL, REPO_ISSUES_URL)
    log.info("")
    log.info("迭代模式说明:")
    log.info("  - 自动重试失败的脚本")
    log.info("  - 逐轮解决依赖关系")
    log.info("  - 收敛检测停止条件")
    log.info(
        "  - 执行粒度: mode=%s, file_fallback=%s",
        fixup_settings.exec_mode,
        str(bool(fixup_settings.exec_file_fallback)).lower(),
    )
    log.info("")

    done_dir = fixup_dir / DONE_DIR_NAME
    done_dir.mkdir(exist_ok=True)
    state_ledger = None if getattr(args, "no_resume_ledger", False) else FixupStateLedger(fixup_dir)
    if state_ledger is None:
        log.warning("[STATE] 已禁用 ledger resume: --no-resume-ledger")

    current_ob_cfg = dict(ob_cfg)
    current_fixup_settings = fixup_settings
    current_max_sql_file_bytes = max_sql_file_bytes

    obclient_cmd = build_obclient_command(current_ob_cfg)
    ob_timeout = resolve_timeout_value(current_ob_cfg.get("timeout"))
    ok_conn, conn_err = check_obclient_connectivity(obclient_cmd, ob_timeout)
    if not ok_conn:
        log.error("OBClient 连接检查失败: %s", conn_err)
        log.error("请确认网络连通性/账号权限/obclient 可用性后重试。")
        sys.exit(1)
    auto_grant_ctx = init_auto_grant_context(
        current_fixup_settings, report_dir, fixup_dir, exclude_dirs, obclient_cmd, ob_timeout
    )
    error_entries: List[ErrorReportEntry] = []
    error_truncated = False

    round_num = 0
    cumulative_success = 0
    cumulative_failed = 0
    active_failed_paths: Set[Path] = set()
    non_retryable_failed_paths: Set[Path] = set()
    recompile_owners: Set[str] = set()

    all_round_results = []
    last_failure_results: List[ScriptResult] = []
    exec_stats = new_exec_mode_stats()

    while round_num < max_rounds:
        round_num += 1
        (
            current_ob_cfg,
            current_fixup_settings,
            current_max_sql_file_bytes,
            fixup_settings_changed,
        ) = apply_fixup_hot_reload_at_round(
            hot_reload_runtime,
            round_num,
            current_ob_cfg,
            fixup_dir,
            report_dir,
            current_fixup_settings,
            current_max_sql_file_bytes,
        )
        ob_timeout = resolve_timeout_value(current_ob_cfg.get("timeout"))
        if fixup_settings_changed:
            if current_fixup_settings.enabled:
                if auto_grant_ctx is None:
                    auto_grant_ctx = init_auto_grant_context(
                        current_fixup_settings,
                        report_dir,
                        fixup_dir,
                        exclude_dirs,
                        obclient_cmd,
                        ob_timeout,
                    )
                else:
                    auto_grant_ctx.settings = current_fixup_settings
            else:
                auto_grant_ctx = None
        reset_auto_grant_round_cache(auto_grant_ctx, round_num)

        log_section(f"第 {round_num}/{max_rounds} 轮")

        # Collect pending SQL files (excluding done/)
        files_with_layer = collect_sql_files_by_layer(
            fixup_dir,
            smart_order=args.smart_order,
            include_dirs=set(only_dirs) if only_dirs else None,
            exclude_dirs=set(exclude_dirs),
            glob_patterns=args.glob_patterns or None,
        )
        files_with_layer = filter_files_by_safety_tier(args, fixup_dir, files_with_layer)
        if getattr(args, "plan_only", False):
            jsonl_path, summary_path = write_run_fixup_plan_validation(
                fixup_dir, report_dir, files_with_layer, get_selected_safety_tiers(args)
            )
            log.info("[PLAN_ONLY] 已写入执行计划验证: %s", jsonl_path)
            log.info("[PLAN_ONLY] 已写入执行计划摘要: %s", summary_path)
            return
        stale_failed = [path for path in sorted(active_failed_paths, key=str) if not path.exists()]
        if stale_failed:
            for path in stale_failed:
                active_failed_paths.discard(path)
            log.info("历史失败脚本已不存在，已从失败集合移除: %d", len(stale_failed))

        if not files_with_layer:
            log.info("✓ 所有脚本已成功执行！")
            break
        if round_num == 1:
            precheck_summary = build_fixup_precheck_summary(
                ob_cfg, obclient_cmd, ob_timeout, files_with_layer
            )
            precheck_report = write_fixup_precheck_report(fixup_dir, precheck_summary)
            log_fixup_precheck(precheck_summary, precheck_report)
        recompile_owners.update(infer_recompile_owners(files_with_layer))

        object_index, name_index = build_fixup_object_index(files_with_layer)
        pre_executed: Set[Path] = set()

        total_scripts = len(files_with_layer)
        log.info("本轮读取 SQL 文件: %d", total_scripts)

        round_results: List[ScriptResult] = []
        width = len(str(total_scripts)) or 1
        current_layer = -1

        # Execute scripts for this round
        for idx, (layer, sql_path) in enumerate(files_with_layer, start=1):
            if args.smart_order and layer != current_layer:
                current_layer = layer
                layer_name = "未知层" if layer == 999 else f"第 {layer} 层"
                log_subsection(f"执行层 {layer_name}")

            relative_path = sql_path.relative_to(repo_root)
            label = format_progress_label(idx, total_scripts, width)
            log_sql_file_safety_context(args, fixup_dir, sql_path, label)
            normalized_path = normalize_failed_path(relative_path, repo_root)
            contract = get_fixup_execution_contract(relative_path)

            if sql_path in pre_executed:
                log.info("%s %s -> SKIP (已由依赖解析执行)", label, relative_path)
                continue
            if normalized_path in non_retryable_failed_paths:
                msg = build_non_retryable_iterative_message(relative_path)
                log.warning("%s %s -> SKIP (%s)", label, relative_path, msg)
                round_results.append(ScriptResult(relative_path, "SKIPPED", msg, layer))
                continue

            if is_grant_dir(sql_path.parent.name):
                bump_exec_mode_stat(exec_stats, "grants_statement")
                result, summary, _removed, _kept, truncated = execute_grant_file_with_prune(
                    obclient_cmd,
                    sql_path,
                    repo_root,
                    done_dir,
                    ob_timeout,
                    layer,
                    label,
                    error_entries,
                    DEFAULT_ERROR_REPORT_LIMIT,
                    current_max_sql_file_bytes,
                    state_ledger=state_ledger,
                )
                error_truncated = error_truncated or truncated
                if result.status == "FAILED":
                    first_error = summary.failures[0].error if summary.failures else result.message
                    error_type = classify_sql_error(first_error)
                    retry_target = None
                    if summary.failures:
                        retry_target = infer_permission_retry_target(
                            summary.failures[0].statement, relative_path
                        )
                    if (
                        auto_grant_ctx
                        and error_type == FailureType.PERMISSION_DENIED
                        and retry_target
                    ):
                        retry_full, retry_type, retry_privs = retry_target
                        applied, _blocked = execute_auto_grant_for_object(
                            auto_grant_ctx,
                            retry_full,
                            retry_type,
                            f"{label} (grant)",
                            required_privileges_override=retry_privs,
                        )
                        if applied > 0:
                            refresh_result = execute_view_refresh_before_retry(
                                fixup_dir=fixup_dir,
                                object_full=retry_full,
                                object_type=retry_type,
                                obclient_cmd=obclient_cmd,
                                done_dir=done_dir,
                                timeout=ob_timeout,
                                layer=layer,
                                label=f"{label} (grant)",
                                max_sql_file_bytes=current_max_sql_file_bytes,
                                state_ledger=state_ledger,
                                exec_mode=current_fixup_settings.exec_mode,
                                exec_file_fallback=current_fixup_settings.exec_file_fallback,
                                exec_stats=exec_stats,
                            )
                            if refresh_result:
                                round_results.append(refresh_result)
                            retry_result, retry_summary, _removed2, _kept2, truncated2 = (
                                execute_grant_file_with_prune(
                                    obclient_cmd,
                                    sql_path,
                                    repo_root,
                                    done_dir,
                                    ob_timeout,
                                    layer,
                                    f"{label} (retry)",
                                    error_entries,
                                    DEFAULT_ERROR_REPORT_LIMIT,
                                    current_max_sql_file_bytes,
                                    state_ledger=state_ledger,
                                )
                            )
                            error_truncated = error_truncated or truncated2
                            round_results.append(retry_result)
                            continue
                round_results.append(result)
                if result.status in ("FAILED", "ERROR") and not bool(
                    contract.get("iterative_retry", True)
                ):
                    non_retryable_failed_paths.add(normalized_path)
                    log.warning(
                        "%s %s -> 保留失败；family=%s, support_tier=%s, retry_policy=%s。",
                        label,
                        relative_path,
                        contract.get("family") or "-",
                        contract.get("support_tier") or "-",
                        contract.get("retry_policy") or "-",
                    )
                continue

            obj_type = DIR_OBJECT_TYPE_MAP.get(sql_path.parent.name.lower())
            obj_schema, obj_name = parse_object_identity_from_path(sql_path)
            obj_full = f"{obj_schema}.{obj_name}" if obj_schema and obj_name else None
            if auto_grant_ctx and obj_full and obj_type:
                execute_auto_grant_for_object(auto_grant_ctx, obj_full, obj_type, label)

            result, summary = execute_script_with_summary(
                obclient_cmd,
                sql_path,
                repo_root,
                done_dir,
                ob_timeout,
                layer,
                label,
                current_max_sql_file_bytes,
                state_ledger=state_ledger,
                exec_mode=current_fixup_settings.exec_mode,
                exec_file_fallback=current_fixup_settings.exec_file_fallback,
                exec_stats=exec_stats,
            )

            if result.status == "SUCCESS":
                round_results.append(result)
                continue

            if result.status in ("SKIPPED", "ERROR"):
                round_results.append(result)
                if result.status == "ERROR":
                    for failure in summary.failures:
                        if error_truncated:
                            break
                        error_truncated = not record_error_entry(
                            error_entries,
                            DEFAULT_ERROR_REPORT_LIMIT,
                            relative_path,
                            failure.index,
                            failure.statement,
                            failure.error,
                        )
                    if not bool(contract.get("iterative_retry", True)):
                        non_retryable_failed_paths.add(normalized_path)
                        log.warning(
                            "%s %s -> 保留失败；family=%s, support_tier=%s, retry_policy=%s。",
                            label,
                            relative_path,
                            contract.get("family") or "-",
                            contract.get("support_tier") or "-",
                            contract.get("retry_policy") or "-",
                        )
                continue

            # Handle failure cases with optional VIEW resolution
            first_error = summary.failures[0].error if summary.failures else result.message
            error_type = classify_sql_error(first_error)
            is_view = sql_path.parent.name.lower() == "view"
            handled = False

            if is_view and error_type == FailureType.MISSING_OBJECT:
                view_schema, _view_name = parse_object_from_filename(sql_path)
                failure_stmt = summary.failures[0].statement if summary.failures else ""
                if not is_create_view_statement(failure_stmt):
                    log.warning("%s %s -> 失败语句非 CREATE VIEW，跳过解析", label, relative_path)
                else:
                    missing_schema, missing_name = extract_object_from_error(first_error)
                    if missing_name:
                        dep_path = select_dependency_script(
                            missing_schema, missing_name, view_schema, object_index, name_index
                        )
                        if dep_path == sql_path:
                            log.warning("%s %s -> 依赖对象指向自身，跳过处理", label, relative_path)
                        elif dep_path and dep_path not in pre_executed:
                            dep_result, _dep_summary = execute_script_with_summary(
                                obclient_cmd,
                                dep_path,
                                repo_root,
                                done_dir,
                                ob_timeout,
                                layer,
                                "[DEPS]",
                                current_max_sql_file_bytes,
                                state_ledger=state_ledger,
                                exec_mode=current_fixup_settings.exec_mode,
                                exec_file_fallback=current_fixup_settings.exec_file_fallback,
                                exec_stats=exec_stats,
                            )
                            pre_executed.add(dep_path)
                            round_results.append(dep_result)
                            handled = dep_result.status == "SUCCESS"
                        else:
                            log.warning(
                                "%s %s -> 无法解析依赖对象 %s", label, relative_path, missing_name
                            )
                    else:
                        log.warning("%s %s -> 缺失对象未解析", label, relative_path)

            if error_type == FailureType.PERMISSION_DENIED:
                required_priv_override = None
                if summary.failures and obj_full and obj_type:
                    required_priv_override = infer_required_privileges_from_failed_statement(
                        summary.failures[0].statement,
                        obj_full,
                        obj_type,
                    )
                if auto_grant_ctx and obj_full and obj_type:
                    applied, _blocked = execute_auto_grant_for_object(
                        auto_grant_ctx,
                        obj_full,
                        obj_type,
                        f"{label} (grant)",
                        required_privileges_override=required_priv_override,
                    )
                    handled = applied > 0
                else:
                    log.warning("%s %s -> 权限错误未解析目标对象", label, relative_path)

            if handled:
                retry_result, retry_summary = execute_script_with_summary(
                    obclient_cmd,
                    sql_path,
                    repo_root,
                    done_dir,
                    ob_timeout,
                    layer,
                    f"{label} (retry)",
                    current_max_sql_file_bytes,
                    state_ledger=state_ledger,
                    exec_mode=current_fixup_settings.exec_mode,
                    exec_file_fallback=current_fixup_settings.exec_file_fallback,
                    exec_stats=exec_stats,
                )
                round_results.append(retry_result)
                if retry_result.status == "FAILED":
                    for failure in retry_summary.failures:
                        if error_truncated:
                            break
                        error_truncated = not record_error_entry(
                            error_entries,
                            DEFAULT_ERROR_REPORT_LIMIT,
                            relative_path,
                            failure.index,
                            failure.statement,
                            failure.error,
                        )
                    if not bool(contract.get("iterative_retry", True)):
                        non_retryable_failed_paths.add(normalized_path)
                        log.warning(
                            "%s %s -> 保留失败；family=%s, support_tier=%s, retry_policy=%s。",
                            label,
                            relative_path,
                            contract.get("family") or "-",
                            contract.get("support_tier") or "-",
                            contract.get("retry_policy") or "-",
                        )
                continue

            round_results.append(result)
            if result.status == "FAILED":
                for failure in summary.failures:
                    if error_truncated:
                        break
                    error_truncated = not record_error_entry(
                        error_entries,
                        DEFAULT_ERROR_REPORT_LIMIT,
                        relative_path,
                        failure.index,
                        failure.statement,
                        failure.error,
                    )
                if not bool(contract.get("iterative_retry", True)):
                    non_retryable_failed_paths.add(normalized_path)
                    log.warning(
                        "%s %s -> 保留失败；family=%s, support_tier=%s, retry_policy=%s。",
                        label,
                        relative_path,
                        contract.get("family") or "-",
                        contract.get("support_tier") or "-",
                        contract.get("retry_policy") or "-",
                    )

        # Round summary
        round_success = sum(1 for r in round_results if r.status == "SUCCESS")
        round_failed = sum(1 for r in round_results if r.status in ("FAILED", "ERROR"))
        round_skipped = sum(1 for r in round_results if r.status == "SKIPPED")

        cumulative_success += round_success
        for item in round_results:
            failed_path = normalize_failed_path(item.path, repo_root)
            if item.status in ("FAILED", "ERROR"):
                active_failed_paths.add(failed_path)
            elif failed_path in non_retryable_failed_paths:
                active_failed_paths.add(failed_path)
            else:
                active_failed_paths.discard(failed_path)
        cumulative_failed = len(active_failed_paths)
        current_failure_results = [
            item for item in round_results if item.status in ("FAILED", "ERROR")
        ]
        if current_failure_results:
            last_failure_results = current_failure_results

        log_subsection(f"第 {round_num} 轮结果")
        log.info("本轮成功: %d", round_success)
        log.info("本轮失败: %d", round_failed)
        log.info("本轮跳过: %d", round_skipped)
        log.info("累计成功: %d", cumulative_success)
        log.info("累计失败: %d", cumulative_failed)
        log.info("")

        all_round_results.append(
            {
                "round": round_num,
                "success": round_success,
                "failed": round_failed,
                "results": round_results,
            }
        )

        # Convergence check
        effective_min_progress = max(1, min_progress)
        if round_success < effective_min_progress:
            if round_success == 0:
                log.warning("本轮无新成功脚本，停止迭代。")
                failures_by_type = analyze_failure_patterns(
                    current_failure_results or last_failure_results
                )
                if failures_by_type:
                    log_failure_analysis(failures_by_type)
            else:
                log.warning(
                    "本轮成功数 (%d) 低于最小进展阈值 (%d)，停止迭代。",
                    round_success,
                    effective_min_progress,
                )
            break

        # Recompile after each round if enabled
        if args.recompile:
            log_subsection("轮次重编译")
            recomp_summary = recompile_invalid_objects(
                obclient_cmd,
                ob_timeout,
                2,
                allowed_owners=recompile_owners,  # Fewer retries per round
            )
            if recomp_summary.total_recompiled > 0:
                log.info("重编译成功 %d 个对象", recomp_summary.total_recompiled)

    # Final recompilation
    total_recompiled = 0
    remaining_invalid = 0
    recompile_failed = 0
    unsupported_recompile_types = 0
    if args.recompile:
        log_section("最终重编译")
        recomp_summary = recompile_invalid_objects(
            obclient_cmd, ob_timeout, args.max_retries, allowed_owners=recompile_owners
        )
        total_recompiled = recomp_summary.total_recompiled
        remaining_invalid = recomp_summary.remaining_invalid
        recompile_failed = recomp_summary.recompile_failed
        unsupported_recompile_types = recomp_summary.unsupported_types

    if auto_grant_ctx:
        log_subsection("自动补权限统计")
        log.info("计划语句 : %d", auto_grant_ctx.stats.planned)
        log.info("执行成功 : %d", auto_grant_ctx.stats.executed)
        log.info("执行失败 : %d", auto_grant_ctx.stats.failed)
        log.info("阻断提示 : %d", auto_grant_ctx.stats.blocked)
        log.info("范围跳过 : %d", auto_grant_ctx.stats.skipped)

    # Final summary
    log_section("迭代执行汇总")
    log.info("执行轮次: %d", round_num)
    log.info("总计成功: %d", cumulative_success)
    log.info("总计失败: %d", cumulative_failed)
    log_exec_mode_summary(
        exec_stats, current_fixup_settings.exec_mode, current_fixup_settings.exec_file_fallback
    )

    if args.recompile:
        log_subsection("最终重编译统计")
        log.info("重编译成功: %d", total_recompiled)
        log.info("重编译失败: %d", recompile_failed)
        log.info("不支持重编译类型: %d", unsupported_recompile_types)
        log.info("仍为INVALID: %d", remaining_invalid)

    # Final failure analysis
    if round_num > 0 and all_round_results:
        final_results = all_round_results[-1]["results"]
        failure_analysis_source = [
            item for item in final_results if item.status in ("FAILED", "ERROR")
        ]
        if not failure_analysis_source:
            failure_analysis_source = last_failure_results
        failures_by_type = analyze_failure_patterns(failure_analysis_source)
        if failures_by_type:
            log_failure_analysis(failures_by_type)

    # Per-round breakdown
    if len(all_round_results) > 1:
        log_subsection("各轮执行统计")
        for round_data in all_round_results:
            log.info(
                "第 %d 轮: 成功 %d, 失败 %d",
                round_data["round"],
                round_data["success"],
                round_data["failed"],
            )

    report_path = write_error_report(
        error_entries, fixup_dir, DEFAULT_ERROR_REPORT_LIMIT, error_truncated
    )
    if report_path:
        log.info("错误报告已输出: %s", report_path)
    reload_report_path = write_fixup_hot_reload_events_report(fixup_dir, hot_reload_runtime)
    if reload_report_path:
        log.info("配置热加载事件已输出: %s", reload_report_path)
    if state_ledger:
        log_fixup_state_ledger_summary(state_ledger)

    log_section("执行结束")

    exit_code = 0 if len(active_failed_paths) == 0 else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
