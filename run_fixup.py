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
import configparser
import fnmatch
import hashlib
import json
import logging
import re
import shutil
import subprocess
import sys
import textwrap
import time
from collections import defaultdict, OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
try:
    import fcntl
except Exception:  # pragma: no cover - non-POSIX fallback
    fcntl = None

__version__ = "0.9.8.7"

CONFIG_DEFAULT_PATH = "config.ini"
DEFAULT_FIXUP_DIR = "fixup_scripts"
DONE_DIR_NAME = "done"
DEFAULT_OBCLIENT_TIMEOUT = 60
DEFAULT_FIXUP_TIMEOUT = 3600
DEFAULT_ERROR_REPORT_LIMIT = 200
DEFAULT_FIXUP_MAX_SQL_FILE_MB = 50
DEFAULT_FIXUP_AUTO_GRANT_CACHE_LIMIT = 10000
DEFAULT_CONFIG_HOT_RELOAD_INTERVAL_SEC = 5
MAX_RECOMPILE_RETRIES = 5
STATE_LEDGER_FILENAME = ".fixup_state_ledger.json"
FIXUP_RUN_LOCK_FILENAME = ".run_fixup.lock"
FIXUP_HOT_RELOAD_EVENTS_DIR = "errors"
REPO_URL = "https://github.com/Minorli/ob_comparator"
REPO_ISSUES_URL = f"{REPO_URL}/issues"

CONFIG_HOT_RELOAD_MODE_VALUES = {"off", "phase", "round"}
CONFIG_HOT_RELOAD_FAIL_POLICY_VALUES = {"keep_last_good", "abort"}

LOG_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FILE_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
LOG_SECTION_WIDTH = 80

CURRENT_SCHEMA_PATTERN = re.compile(
    r'^\s*ALTER\s+SESSION\s+SET\s+CURRENT_SCHEMA\s*=\s*(?P<schema>"[^"]+"|[A-Z0-9_$#]+)\s*;?\s*$',
    re.IGNORECASE
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
            log_time_format=LOG_TIME_FORMAT
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        return handler
    except Exception as exc:
        logging.getLogger(__name__).debug("RichHandler init failed, fallback to StreamHandler: %s", exc)
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
            logging.getLogger(__name__).debug("TTY detection failed, defaulting to non-tty: %s", exc)
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


# Error classification for intelligent retry
class FailureType:
    """Classification of SQL execution failures for retry logic."""
    MISSING_OBJECT = "missing_object"        # Dependency object doesn't exist -> retryable
    PERMISSION_DENIED = "permission_denied"  # Insufficient privileges -> needs grants
    SYNTAX_ERROR = "syntax_error"            # SQL syntax error -> needs DDL fix
    DATA_CONFLICT = "data_conflict"          # Unique/constraint violation -> needs data cleanup
    CONSTRAINT_VALIDATE_FAIL = "constraint_validate_fail"  # ORA-02298 validation failed
    DUPLICATE_OBJECT = "duplicate_object"    # Object already exists -> can skip
    INVALID_IDENTIFIER = "invalid_identifier" # Column/table name error -> needs DDL fix
    NAME_IN_USE = "name_in_use"              # Name already used -> needs resolution
    TIMEOUT = "timeout"                       # Execution timeout -> may retry
    LOCK_TIMEOUT = "lock_timeout"             # Resource busy/locked
    AUTH_FAILED = "auth_failed"               # Login/auth failure
    CONNECTION_TIMEOUT = "connection_timeout" # Network timeout
    RESOURCE_EXHAUSTED = "resource_exhausted" # Out of shared pool/memory
    SNAPSHOT_ERROR = "snapshot_error"         # Snapshot too old
    DEADLOCK = "deadlock"                     # Deadlock detected
    UNKNOWN = "unknown"                       # Unknown error -> investigate


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
    if any(code in stderr_upper for code in ['ORA-00942', 'ORA-04043', 'OB-00942', 'OB-04043', 'ORA-06512']):
        if 'TABLE OR VIEW DOES NOT EXIST' in stderr_upper or 'OBJECT DOES NOT EXIST' in stderr_upper:
            return FailureType.MISSING_OBJECT
    if (
        'TABLE OR VIEW DOES NOT EXIST' in stderr_upper
        or 'OBJECT DOES NOT EXIST' in stderr_upper
        or 'ERROR 1146' in stderr_upper
    ):
        return FailureType.MISSING_OBJECT
    
    # Permission denied (needs grant scripts)
    if (
        'ORA-01031' in stderr_upper
        or 'OB-01031' in stderr_upper
        or 'ORA-01720' in stderr_upper
        or 'INSUFFICIENT PRIVILEGES' in stderr_upper
        or 'ERROR 1142' in stderr_upper
        or 'ERROR 1227' in stderr_upper
    ):
        return FailureType.PERMISSION_DENIED

    # Authentication failure
    if (
        'ORA-01017' in stderr_upper
        or 'INVALID USERNAME/PASSWORD' in stderr_upper
        or 'ERROR 1045' in stderr_upper
    ):
        return FailureType.AUTH_FAILED

    # Connection timeout
    if 'ORA-12170' in stderr_upper or 'TNS:CONNECT TIMEOUT' in stderr_upper:
        return FailureType.CONNECTION_TIMEOUT

    # Lock timeout / resource busy
    if 'ORA-00054' in stderr_upper or 'RESOURCE BUSY' in stderr_upper or 'ERROR 1205' in stderr_upper:
        return FailureType.LOCK_TIMEOUT

    # Resource exhausted
    if 'ORA-04031' in stderr_upper:
        return FailureType.RESOURCE_EXHAUSTED

    # Snapshot too old
    if 'ORA-01555' in stderr_upper:
        return FailureType.SNAPSHOT_ERROR

    # Deadlock
    if 'ORA-00060' in stderr_upper or 'ERROR 1213' in stderr_upper:
        return FailureType.DEADLOCK
    
    # Data conflict (unique constraint violation)
    if 'ORA-00001' in stderr_upper or 'UNIQUE CONSTRAINT' in stderr_upper:
        return FailureType.DATA_CONFLICT

    # Constraint validate failure (target data quality issue)
    if (
        'ORA-02298' in stderr_upper
        or 'CANNOT VALIDATE' in stderr_upper
    ):
        return FailureType.CONSTRAINT_VALIDATE_FAIL
    
    # Invalid identifier (DDL needs fix)
    if 'ORA-00904' in stderr_upper or 'ERROR 1054' in stderr_upper:
        return FailureType.INVALID_IDENTIFIER
    
    # Name already in use (object exists)
    if (
        'ORA-00955' in stderr_upper
        or 'OB-00955' in stderr_upper
        or 'NAME IS ALREADY USED' in stderr_upper
        or 'ALREADY EXISTS' in stderr_upper
        or 'ERROR 1050' in stderr_upper
    ):
        return FailureType.NAME_IN_USE
    
    # Syntax errors (DDL needs fix)
    if any(code in stderr_upper for code in ['ORA-00900', 'ORA-00901', 'ORA-00902', 'ORA-00903', 'ERROR 1064']):
        return FailureType.SYNTAX_ERROR
    
    return FailureType.UNKNOWN


def analyze_failure_patterns(results: List['ScriptResult']) -> Dict[str, List['ScriptResult']]:
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


def log_failure_analysis(failures_by_type: Dict[str, List['ScriptResult']]) -> None:
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
    if FailureType.DUPLICATE_OBJECT in failures_by_type or FailureType.NAME_IN_USE in failures_by_type:
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
        log.info("   建议: 先清理脏数据，再执行 constraint_validate_later 下的脚本完成 ENABLE VALIDATE")
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
    "MATERIALIZED_VIEW": "materialized_view",
    "SYNONYM": "synonym",
    "PROCEDURE": "procedure",
    "FUNCTION": "function",
    "PACKAGE": "package",
    "PACKAGE_BODY": "package_body",
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
    ["sequence"],                                    # Layer 0: No dependencies
    ["table"],                                       # Layer 1: Base tables
    ["table_alter"],                                 # Layer 2: Table modifications
    ["view_prereq_grants", "grants"],                # Layer 3: View prereq + general grants
    ["view", "synonym"],                             # Layer 4: Simple dependent objects
    ["view_post_grants"],                            # Layer 5: View post grants
    ["materialized_view"],                           # Layer 6: MVIEWs
    ["type"],                                        # Layer 7: Types (specs)
    ["package"],                                     # Layer 8: Package specs
    ["procedure", "function"],                       # Layer 9: Standalone routines
    ["type_body", "package_body"],                   # Layer 10: Type/package bodies
    ["name_collision"],                              # Layer 11: Name collision remediation
    ["constraint", "index"],                         # Layer 12: Constraints and indexes
    ["trigger"],                                     # Layer 13: Triggers (last)
    ["job", "schedule"],                             # Layer 14: Jobs
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
}


def is_grant_dir(dir_name: str) -> bool:
    return dir_name.lower() in GRANT_DIRS


def resolve_grant_dirs(
    subdirs: Dict[str, Path],
    include_dirs: Optional[Set[str]],
    exclude_dirs: Set[str]
) -> List[str]:
    available = set(subdirs.keys())
    include_set = {d.lower() for d in include_dirs or set()}
    exclude_set = {d.lower() for d in exclude_dirs or set()}

    grant_dirs: List[str] = []
    if include_set:
        if "grants_all" in include_set and "grants_all" in available and "grants_all" not in exclude_set:
            grant_dirs.append("grants_all")
        if "grants_miss" in include_set and "grants_miss" in available and "grants_miss" not in exclude_set:
            grant_dirs.append("grants_miss")
        if "grants" in include_set:
            if "grants_miss" in available and "grants_miss" not in exclude_set:
                grant_dirs.append("grants_miss")
            elif "grants" in available and "grants" not in exclude_set:
                grant_dirs.append("grants")
        if "view_prereq_grants" in include_set and "view_prereq_grants" in available and "view_prereq_grants" not in exclude_set:
            grant_dirs.append("view_prereq_grants")
        if "view_post_grants" in include_set and "view_post_grants" in available and "view_post_grants" not in exclude_set:
            grant_dirs.append("view_post_grants")
    else:
        if "grants_miss" in available and "grants_miss" not in exclude_set:
            grant_dirs.append("grants_miss")
        elif "grants" in available and "grants" not in exclude_set:
            grant_dirs.append("grants")
        if "view_prereq_grants" in available and "view_prereq_grants" not in exclude_set:
            grant_dirs.append("view_prereq_grants")
        if "view_post_grants" in available and "view_post_grants" not in exclude_set:
            grant_dirs.append("view_post_grants")

    # preserve order, remove duplicates
    return list(dict.fromkeys(grant_dirs))

CREATE_OBJECT_DIRS = {
    "sequence",
    "table",
    "view",
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
    re.IGNORECASE
)
RE_ANON_BLOCK_START = re.compile(r"^\s*(DECLARE|BEGIN)\b", re.IGNORECASE)
RE_CREATE_VIEW = re.compile(
    r"^\s*CREATE\s+(OR\s+REPLACE\s+)?(FORCE\s+)?(MATERIALIZED\s+)?VIEW\b",
    re.IGNORECASE
)
RE_ERROR_CODE = re.compile(r"(ORA-\d{5}|OB-\d+)", re.IGNORECASE)
RE_SQL_ERROR = re.compile(r"(ORA-\d{5}|OB-\d+|ERROR\s+\d+)", re.IGNORECASE)
RE_GRANT_ON = re.compile(
    r"^GRANT\s+.+?\s+ON\s+(?P<object>[^\s]+)\s+TO\s+(?P<grantee>[^\s;]+)",
    re.IGNORECASE | re.DOTALL
)
RE_GRANT_OBJECT = re.compile(
    r"^\s*GRANT\s+(?P<privs>.+?)\s+ON\s+(?P<object>.+?)\s+TO\s+(?P<grantees>.+)$",
    re.IGNORECASE | re.DOTALL
)
RE_GRANT_SIMPLE = re.compile(
    r"^\s*GRANT\s+(?P<privs>.+?)\s+TO\s+(?P<grantees>.+)$",
    re.IGNORECASE | re.DOTALL
)
RE_WITH_OPTION = re.compile(
    r"\s+WITH\s+GRANT\s+OPTION|\s+WITH\s+ADMIN\s+OPTION",
    re.IGNORECASE
)
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
        self._load()

    @staticmethod
    def fingerprint(sql_text: str) -> str:
        return hashlib.sha1((sql_text or "").encode("utf-8")).hexdigest()

    def _load(self) -> None:
        if not self.path.exists():
            self._data = {}
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                self._data = payload.get("completed", {}) if isinstance(payload.get("completed"), dict) else {}
            else:
                self._data = {}
        except Exception as exc:
            log.warning("[STATE] 读取状态账本失败，将忽略旧账本: %s", exc)
            self._data = {}

    def flush(self) -> None:
        if not self._dirty:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"version": 1, "completed": self._data}
            self.path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8"
            )
            self._dirty = False
        except Exception as exc:
            log.warning("[STATE] 写入状态账本失败: %s", exc)

    def is_completed(self, relative_path: Path, fingerprint: str) -> bool:
        key = str(relative_path).replace("\\", "/")
        item = self._data.get(key)
        return bool(item and item.get("fingerprint") == fingerprint)

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


def read_sql_text_with_limit(sql_path: Path, max_bytes: Optional[int]) -> Tuple[Optional[str], Optional[str]]:
    """Read SQL file with optional size limit."""
    try:
        if max_bytes and max_bytes > 0:
            size = sql_path.stat().st_size
            if size > max_bytes:
                return None, f"文件过大 ({size} bytes) 超过限制 {max_bytes} bytes"
        return sql_path.read_text(encoding="utf-8"), None
    except Exception as exc:
        return None, f"读取文件失败: {exc}"


def extract_sql_error(output: str) -> Optional[str]:
    if not output:
        return None
    for line in output.splitlines():
        if RE_SQL_ERROR.search(line):
            return line.strip()
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
    mid = ddl_text[mid_start:tokens[view_idx][1]]
    mid_clean = re.sub(
        r"(?is)\bNO\s+FORCE\b|\bFORCE\b|\bEDITIONABLE\b|\bNONEDITIONABLE\b",
        " ",
        mid
    )
    mid_clean = " ".join(mid_clean.split())
    prefix = "CREATE"
    if has_or_replace:
        prefix += " OR REPLACE"
    replacement = prefix + (" " + mid_clean if mid_clean else "") + " VIEW"
    return ddl_text[:create_start] + replacement + ddl_text[view_end:]


def move_sql_to_done(sql_path: Path, done_dir: Path) -> str:
    """Move executed SQL to done directory with backup if needed."""
    try:
        target_dir = done_dir / sql_path.parent.name
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / sql_path.name
        backup_note = ""
        if target_path.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = target_dir / f"{sql_path.stem}.bak_{timestamp}{sql_path.suffix}"
            try:
                target_path.replace(backup_path)
                backup_note = f" (已备份: {backup_path.name})"
            except Exception as exc:
                log.warning("已存在文件备份失败: %s (%s)", target_path, str(exc)[:200])
                return f"(移动失败: 目标已存在且备份失败: {exc})"
        shutil.move(str(sql_path), target_path)
        return f"(已移至 done/{sql_path.parent.name}/){backup_note}"
    except Exception as exc:
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


@dataclass
class FixupAutoGrantSettings:
    enabled: bool
    types: Set[str]
    fallback: bool
    cache_limit: int


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
    """Simple size-limited cache with FIFO eviction."""

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
            "config_hot_reload_mode=%s 非法，回退为 off（支持: off/phase/round）",
            raw_value
        )
        return "off"
    return value


def normalize_config_hot_reload_fail_policy(raw_value: Optional[str]) -> str:
    value = (raw_value or "keep_last_good").strip().lower()
    if value not in CONFIG_HOT_RELOAD_FAIL_POLICY_VALUES:
        log.warning(
            "config_hot_reload_fail_policy=%s 非法，回退为 keep_last_good（支持: keep_last_good/abort）",
            raw_value
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
            DEFAULT_CONFIG_HOT_RELOAD_INTERVAL_SEC
        )
        return DEFAULT_CONFIG_HOT_RELOAD_INTERVAL_SEC
    if value < 1:
        log.warning(
            "config_hot_reload_interval_sec=%s 小于 1，回退为 %d",
            raw_value,
            DEFAULT_CONFIG_HOT_RELOAD_INTERVAL_SEC
        )
        return DEFAULT_CONFIG_HOT_RELOAD_INTERVAL_SEC
    return value


def resolve_config_relative_path(base_dir: Path, raw_path: str) -> Path:
    path = Path((raw_path or "").strip()).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def resolve_hot_reload_watch_paths(parser: configparser.ConfigParser, config_path: Path) -> List[Path]:
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
    interval_sec = parse_config_hot_reload_interval(settings.get("config_hot_reload_interval_sec", "5"))
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
        snapshot=snapshot
    )


def append_fixup_hot_reload_event(
    runtime: FixupHotReloadRuntime,
    status: str,
    stage: str,
    changed_files: List[str],
    changed_keys: List[str],
    note: str
) -> None:
    event = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
        "stage": stage,
        "changed_files": ",".join(changed_files) if changed_files else "-",
        "changed_keys": ",".join(changed_keys) if changed_keys else "-",
        "note": note or "-"
    }
    runtime.events.append(event)
    message = (
        "[HOT_RELOAD] {status} stage={stage} files={files} keys={keys} note={note}".format(
            status=event["status"],
            stage=event["stage"],
            files=event["changed_files"],
            keys=event["changed_keys"],
            note=event["note"]
        )
    )
    if status in {"REJECTED", "REQUIRES_RESTART"}:
        log.warning(message)
    else:
        log.info(message)


def write_fixup_hot_reload_events_report(
    fixup_dir: Path,
    runtime: Optional[FixupHotReloadRuntime]
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
        "TS | STATUS | STAGE | CHANGED_FILES | CHANGED_KEYS | NOTE"
    ]
    for event in runtime.events:
        lines.append(
            "{ts} | {status} | {stage} | {changed_files} | {changed_keys} | {note}".format(
                ts=event.get("ts", "-"),
                status=event.get("status", "-"),
                stage=event.get("stage", "-"),
                changed_files=event.get("changed_files", "-"),
                changed_keys=event.get("changed_keys", "-"),
                note=event.get("note", "-")
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
    current_max_sql_file_bytes: Optional[int]
) -> Tuple[Dict[str, str], FixupAutoGrantSettings, Optional[int], bool]:
    if not runtime or runtime.mode != "round":
        return current_ob_cfg, current_fixup_settings, current_max_sql_file_bytes, False

    now = time.time()
    if runtime.last_check_at > 0 and (now - runtime.last_check_at) < runtime.interval_sec:
        return current_ob_cfg, current_fixup_settings, current_max_sql_file_bytes, False
    runtime.last_check_at = now

    latest_snapshot = build_watch_snapshot(runtime.watch_paths)
    changed_files = [
        path for path in sorted(latest_snapshot.keys())
        if latest_snapshot.get(path) != runtime.snapshot.get(path)
    ]
    if not changed_files:
        runtime.snapshot = latest_snapshot
        return current_ob_cfg, current_fixup_settings, current_max_sql_file_bytes, False

    try:
        candidate_ob_cfg, candidate_fixup_dir, _repo_root, candidate_log_level, candidate_report_dir, candidate_fixup_settings, candidate_max_sql_file_bytes = load_ob_config(
            runtime.config_path
        )
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
            note=f"配置解析失败: {str(exc)[:240]}"
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
        applied_keys.extend([
            "SETTINGS.fixup_auto_grant",
            "SETTINGS.fixup_auto_grant_types",
            "SETTINGS.fixup_auto_grant_fallback",
            "SETTINGS.fixup_auto_grant_cache_limit",
        ])

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
            note="存在本轮不可热加载项，需重启 run_fixup 生效"
        )
    elif applied_keys:
        append_fixup_hot_reload_event(
            runtime,
            status="APPLIED",
            stage=f"round-{round_num}",
            changed_files=changed_files,
            changed_keys=sorted(set(applied_keys)),
            note="已在轮次边界应用"
        )
    else:
        append_fixup_hot_reload_event(
            runtime,
            status="REQUIRES_RESTART",
            stage=f"round-{round_num}",
            changed_files=changed_files,
            changed_keys=[],
            note="检测到文件变化，但 run_fixup 当前运行态无可热加载项"
        )

    runtime.watch_paths = candidate_watch_paths
    runtime.snapshot = build_watch_snapshot(runtime.watch_paths)
    settings_changed = next_fixup_settings != current_fixup_settings
    return next_ob_cfg, next_fixup_settings, next_max_sql_file_bytes, settings_changed


def load_ob_config(config_path: Path) -> Tuple[Dict[str, str], Path, Path, str, Path, FixupAutoGrantSettings, Optional[int]]:
    """Load OceanBase connection info and fixup directory from config.ini."""
    parser = configparser.ConfigParser(interpolation=None)
    if not config_path.exists():
        raise ConfigError(f"配置文件不存在: {config_path}")

    parser.read(config_path, encoding="utf-8")

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
            fixup_timeout = parser.getint("SETTINGS", "obclient_timeout", fallback=DEFAULT_FIXUP_TIMEOUT)
        if fixup_timeout is None or fixup_timeout < 0:
            fixup_timeout = DEFAULT_FIXUP_TIMEOUT
    except Exception as exc:
        log.warning("fixup 超时解析失败，回退默认值 %s: %s", DEFAULT_FIXUP_TIMEOUT, exc)
        fixup_timeout = DEFAULT_FIXUP_TIMEOUT
    ob_cfg["timeout"] = None if fixup_timeout == 0 else fixup_timeout

    repo_root = config_path.parent.resolve()
    fixup_dir = parser.get("SETTINGS", "fixup_dir", fallback=DEFAULT_FIXUP_DIR).strip()
    fixup_path = (repo_root / fixup_dir).resolve()
    allow_outside = parse_bool_flag(
        parser.get("SETTINGS", "fixup_dir_allow_outside_repo", fallback="false"),
        False
    )
    if not allow_outside:
        if fixup_path != repo_root and repo_root not in fixup_path.parents:
            raise ConfigError(f"fixup_dir 不允许在项目目录之外: {fixup_path}")

    if not fixup_path.exists():
        raise ConfigError(f"修补脚本目录不存在: {fixup_path}")

    report_dir = parser.get("SETTINGS", "report_dir", fallback="main_reports").strip() or "main_reports"
    report_path = (repo_root / report_dir).resolve()

    log_level = parser.get("SETTINGS", "log_level", fallback="AUTO").strip().upper() or "AUTO"
    auto_grant_enabled = parse_bool_flag(
        parser.get("SETTINGS", "fixup_auto_grant", fallback="true"),
        True
    )
    auto_grant_types = parse_fixup_auto_grant_types(
        parser.get("SETTINGS", "fixup_auto_grant_types", fallback="")
    )
    auto_grant_fallback = parse_bool_flag(
        parser.get("SETTINGS", "fixup_auto_grant_fallback", fallback="true"),
        True
    )
    auto_grant_cache_limit = parser.getint(
        "SETTINGS",
        "fixup_auto_grant_cache_limit",
        fallback=DEFAULT_FIXUP_AUTO_GRANT_CACHE_LIMIT
    )
    if auto_grant_cache_limit < 0:
        auto_grant_cache_limit = DEFAULT_FIXUP_AUTO_GRANT_CACHE_LIMIT
    fixup_settings = FixupAutoGrantSettings(
        enabled=auto_grant_enabled,
        types=auto_grant_types,
        fallback=auto_grant_fallback,
        cache_limit=auto_grant_cache_limit
    )
    max_sql_mb = parser.getint(
        "SETTINGS",
        "fixup_max_sql_file_mb",
        fallback=DEFAULT_FIXUP_MAX_SQL_FILE_MB
    )
    max_sql_bytes = None if max_sql_mb <= 0 else max_sql_mb * 1024 * 1024
    return ob_cfg, fixup_path, repo_root, log_level, report_path, fixup_settings, max_sql_bytes


def build_obclient_command(ob_cfg: Dict[str, str]) -> List[str]:
    """Assemble the obclient command line."""
    return [
        ob_cfg["executable"],
        "-h", ob_cfg["host"],
        "-P", ob_cfg["port"],
        "-u", ob_cfg["user_string"],
        f"-p{ob_cfg['password']}",
        "--prompt", "fixup>",
        "--silent",
    ]


def iter_sql_files_recursive(base_dir: Path) -> List[Path]:
    try:
        return sorted(
            [path for path in base_dir.rglob("*.sql") if path.is_file()],
            key=lambda p: str(p)
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
        if p.is_dir() and p.name != DONE_DIR_NAME and p.name.lower() not in exclude_dirs
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
                            if not any(fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p)
                                      for p in glob_patterns):
                                continue
                            files_with_layer.append((layer_idx, sql_file))
                    continue
                if include_dirs and dir_name not in include_dirs:
                    continue
                if dir_name not in subdirs:
                    continue

                for sql_file in iter_sql_files_recursive(subdirs[dir_name]):
                    if not sql_file.is_file():
                        continue
                    rel_str = str(sql_file.relative_to(fixup_dir))
                    if not any(fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p)
                              for p in glob_patterns):
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
            if include_dirs and dir_name not in include_dirs:
                continue
                
            for sql_file in iter_sql_files_recursive(subdirs[dir_name]):
                if not sql_file.is_file():
                    continue
                rel_str = str(sql_file.relative_to(fixup_dir))
                if not any(fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p) 
                          for p in glob_patterns):
                    continue
                files_with_layer.append((999, sql_file))  # Unknown layer
    else:
        # Keep non-smart execution order aligned with dependency-aware layers.
        priority = [
            "sequence", "table", "table_alter", "view_prereq_grants", "grants",
            "view", "synonym", "view_post_grants", "materialized_view",
            "type", "package", "procedure", "function",
            "type_body", "package_body", "name_collision", "constraint", "index", "trigger",
            "job", "schedule",
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
                        if not any(fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p)
                                  for p in glob_patterns):
                            continue
                        files_with_layer.append((idx, sql_file))
                    seen.add(grant_dir)
                continue
            if include_dirs and name not in include_dirs:
                continue
            if name in subdirs:
                for sql_file in iter_sql_files_recursive(subdirs[name]):
                    if not sql_file.is_file():
                        continue
                    rel_str = str(sql_file.relative_to(fixup_dir))
                    if not any(fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p)
                              for p in glob_patterns):
                        continue
                    files_with_layer.append((idx, sql_file))
                seen.add(name)
        
        # Remaining directories
        for name in sorted(subdirs.keys()):
            if name in seen:
                continue
            if is_grant_dir(name) and name not in grant_dirs:
                continue
            if include_dirs and name not in include_dirs:
                continue
            for sql_file in iter_sql_files_recursive(subdirs[name]):
                if not sql_file.is_file():
                    continue
                rel_str = str(sql_file.relative_to(fixup_dir))
                if not any(fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p) 
                          for p in glob_patterns):
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
    stem = path.stem
    if "." not in stem:
        return None, None
    parts = stem.split(".")
    if len(parts) < 2:
        return None, None
    return normalize_identifier(parts[0]), normalize_identifier(parts[1])


def normalize_object_type(raw: str) -> str:
    return (raw or "").strip().upper().replace("_", " ")


def parse_bool_flag(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


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


def extract_execution_error(result: subprocess.CompletedProcess) -> Optional[str]:
    error_msg = extract_sql_error(result.stderr) or extract_sql_error(result.stdout)
    if error_msg:
        return error_msg
    if result.returncode != 0:
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
    flat = " ".join(statement.split())
    match = RE_GRANT_ON.match(flat)
    if not match:
        return "-"
    schema, name = parse_object_token(match.group("object"))
    if schema:
        return f"{schema}.{name}"
    return name


def grant_statement_has_option(statement: str) -> bool:
    if not statement:
        return False
    return "WITH GRANT OPTION" in statement.upper()


def format_privilege_label(privilege: str, grant_option: bool) -> str:
    if not grant_option:
        return privilege
    return f"{privilege} WITH GRANT OPTION"


def requires_grant_option(grantee: str, target_full: str, target_type: str) -> bool:
    if not grantee or not target_full or not target_type:
        return False
    target_schema, _ = split_full_name(target_full)
    if not target_schema:
        return False
    if target_schema.upper() == grantee.upper():
        return False
    return target_type.upper() in GRANT_OPTION_TYPES


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
    if not stripped or stripped.startswith("#") or stripped.startswith("[") or stripped.startswith("-"):
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


def parse_view_chain_file_meta(path: Path) -> Dict[str, List[List[Tuple[str, str, Tuple[str, ...]]]]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
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
    if not stripped or stripped.startswith("#") or stripped.startswith("[") or stripped.startswith("-"):
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
        lines = path.read_text(encoding="utf-8").splitlines()
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
    name_index: Dict[str, List[Path]]
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
    fallback_name_index: Dict[str, List[Path]]
) -> Tuple[Optional[Path], Optional[str]]:
    primary = select_fixup_script_for_node(node, object_index, name_index)
    if primary:
        return primary, "fixup"
    fallback = select_fixup_script_for_node(node, fallback_object_index, fallback_name_index)
    if fallback:
        return fallback, "done"
    return None, None


def build_view_dependency_graph(
    chains: List[List[Tuple[str, str]]]
) -> Tuple[Set[Tuple[str, str]], Dict[Tuple[str, str], Set[Tuple[str, str]]]]:
    nodes: Set[Tuple[str, str]] = set()
    edges: Dict[Tuple[str, str], Set[Tuple[str, str]]] = defaultdict(set)
    for chain in chains:
        for idx, node in enumerate(chain):
            nodes.add(node)
            if idx + 1 < len(chain):
                edges[node].add(chain[idx + 1])
    return nodes, edges


def topo_sort_nodes(
    nodes: Set[Tuple[str, str]],
    edges: Dict[Tuple[str, str], Set[Tuple[str, str]]]
) -> Tuple[List[Tuple[str, str]], List[List[Tuple[str, str]]]]:
    visited: Set[Tuple[str, str]] = set()
    visiting: Set[Tuple[str, str]] = set()
    order: List[Tuple[str, str]] = []
    cycles: List[List[Tuple[str, str]]] = []

    def dfs(node: Tuple[str, str], stack: List[Tuple[str, str]]) -> None:
        if node in visiting:
            cycle_start = stack.index(node) if node in stack else 0
            cycles.append(stack[cycle_start:] + [node])
            return
        if node in visited:
            return
        visiting.add(node)
        for ref in sorted(edges.get(node, set())):
            dfs(ref, stack + [node])
        visiting.remove(node)
        visited.add(node)
        order.append(node)

    for node in sorted(nodes):
        if node not in visited:
            dfs(node, [])
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
    run_dirs.sort(key=lambda p: run_ts_re.search(p.name).group(1) if run_ts_re.search(p.name) else "", reverse=True)
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
        lines = path.read_text(encoding="utf-8").splitlines()
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
    chains_by_view: Dict[str, List[List[Tuple[str, str]]]]
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
    timeout: Optional[int]
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
            report_dir
        )
        return None
    grant_index_miss = build_grant_index(
        fixup_dir,
        set(exclude_dirs),
        include_dirs={"grants_miss"}
    )
    grant_index_all = build_grant_index(
        fixup_dir,
        set(exclude_dirs),
        include_dirs={"grants_all"}
    )
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
        stats=stats
    )
    log.info(
        "[AUTO-GRANT] 启用: types=%s fallback=%s cache_limit=%d deps=%d",
        ",".join(sorted(fixup_settings.types)),
        "true" if fixup_settings.fallback else "false",
        fixup_settings.cache_limit,
        sum(len(v) for v in dep_map.values())
    )
    return ctx


def build_auto_grant_plan_for_object(
    ctx: AutoGrantContext,
    obj_full: str,
    obj_type: str
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
        required_priv = GRANT_PRIVILEGE_BY_TYPE.get(ref_type_u)
        if not required_priv:
            continue
        ref_schema, _ = split_full_name(ref_full_u)
        if not ref_schema or ref_schema.upper() == grantee_schema.upper():
            continue
        require_option = requires_grant_option(grantee_schema, ref_full_u, ref_type_u)
        blocked = plan_object_grant_for_dependency(
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
            sql_lines
        ) or blocked
    return plan_lines, sql_lines, blocked


def execute_auto_grant_for_object(
    ctx: AutoGrantContext,
    obj_full: str,
    obj_type: str,
    label: str
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
    plan_lines, sql_lines, blocked = build_auto_grant_plan_for_object(ctx, obj_full_u, obj_type_u)
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
        log.warning("%s [AUTO-GRANT] %s(%s) 授权失败 %d/%d", label, obj_full, obj_type_u, len(summary.failures), summary.statements)
    else:
        ctx.stats.executed += summary.statements
        log.info("%s [AUTO-GRANT] %s(%s) 授权成功 %d", label, obj_full, obj_type_u, summary.statements)
    return summary.statements, blocked


def reset_auto_grant_round_cache(ctx: Optional[AutoGrantContext], round_num: int) -> int:
    if not ctx:
        return 0
    cleared = len(ctx.blocked_objects)
    if cleared:
        ctx.blocked_objects.clear()
        log.info("[AUTO-GRANT] 第 %d 轮开始，已清理上一轮阻断缓存 %d 项。", round_num, cleared)
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
    use_planned: bool = True
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
    exists_cache: Dict[Tuple[str, str], bool],
    planned_objects: Set[Tuple[str, str]]
) -> int:
    removed = 0
    for full_name, obj_type in planned_objects:
        key = (normalize_identifier(full_name), normalize_object_type(obj_type))
        if key in exists_cache:
            exists_cache.pop(key, None)
            removed += 1
    return removed


def load_roles_for_grantee(
    obclient_cmd: List[str],
    timeout: Optional[int],
    grantee: str,
    roles_cache: Dict[str, Set[str]]
) -> Set[str]:
    grantee_u = (grantee or "").upper()
    if not grantee_u:
        return set()
    cached = roles_cache.get(grantee_u)
    if cached is not None:
        return cached
    sql = (
        "SELECT GRANTED_ROLE FROM DBA_ROLE_PRIVS "
        f"WHERE GRANTEE='{escape_sql_literal(grantee_u)}'"
    )
    roles = query_single_column(obclient_cmd, sql, timeout, "GRANTED_ROLE")
    roles_cache[grantee_u] = roles
    return roles


def load_tab_privs_for_identity(
    obclient_cmd: List[str],
    timeout: Optional[int],
    identity: str,
    owner: str,
    name: str,
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]]
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
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]]
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
    sys_privs_cache: Dict[str, Set[str]]
) -> Set[str]:
    identity_u = (identity or "").upper()
    if not identity_u:
        return set()
    cached = sys_privs_cache.get(identity_u)
    if cached is not None:
        return cached
    sql = (
        "SELECT PRIVILEGE FROM DBA_SYS_PRIVS "
        f"WHERE GRANTEE='{escape_sql_literal(identity_u)}'"
    )
    privs = query_single_column(obclient_cmd, sql, timeout, "PRIVILEGE")
    sys_privs_cache[identity_u] = privs
    return privs


def has_required_privilege(
    obclient_cmd: List[str],
    timeout: Optional[int],
    grantee: str,
    ref_full: str,
    required_priv: str,
    roles_cache: Dict[str, Set[str]],
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]],
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]],
    sys_privs_cache: Dict[str, Set[str]],
    planned_object_privs: Set[Tuple[str, str, str]],
    planned_object_privs_with_option: Set[Tuple[str, str, str]],
    planned_sys_privs: Set[Tuple[str, str]],
    require_grant_option: bool = False
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
    implied = SYS_PRIV_IMPLICATIONS.get(required_u, set())
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
    edges: Dict[Tuple[str, str], Set[Tuple[str, str]]]
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
    sql_lines: List[str]
) -> bool:
    priv_label = format_privilege_label(required_priv, require_grant_option)
    if has_required_privilege(
        obclient_cmd,
        timeout,
        grantee,
        target_full,
        required_priv,
        roles_cache,
        tab_privs_cache,
        tab_privs_grantable_cache,
        sys_privs_cache,
        planned_object_privs,
        planned_object_privs_with_option,
        planned_sys_privs,
        require_grant_option=require_grant_option
    ):
        plan_lines.append(f"GRANT OK: {grantee} has {priv_label} on {target_full}")
        return False

    entries, source_label = find_grant_entries_by_priority(
        grantee,
        target_full,
        required_priv,
        grant_index_miss,
        grant_index_all,
        require_grant_option=require_grant_option
    )
    if not entries:
        if allow_fallback:
            auto_stmt = build_auto_grant_statement(
                grantee,
                target_full,
                required_priv,
                with_grant_option=require_grant_option
            )
            if auto_stmt:
                stmt_key = normalize_statement_key(auto_stmt)
                if stmt_key not in planned_statements:
                    planned_statements.add(stmt_key)
                    plan_lines.append(f"GRANT AUTO: {priv_label} on {target_full} to {grantee}")
                    sql_lines.append("-- SOURCE: auto-generated")
                    sql_lines.append(auto_stmt.rstrip().rstrip(";") + ";")
                    planned_object_privs.add((grantee.upper(), required_priv.upper(), target_full.upper()))
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
                    planned_object_privs_with_option.add((entry.grantee, priv, entry.object_name.upper()))
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
    visited_views: Set[Tuple[str, str]]
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
        blocked = plan_object_grant_for_dependency(
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
            sql_lines
        ) or blocked

        if target_type.upper() in GRANT_OPTION_TYPES:
            blocked = ensure_view_owner_grant_option(
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
                visited_views
            ) or blocked

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
    max_sql_file_bytes: Optional[int]
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
            require_option = requires_grant_option(dep_schema, target_full, target_type)
            if require_option and target_type.upper() in GRANT_OPTION_TYPES:
                blocked = ensure_view_owner_grant_option(
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
                    grant_option_views
                ) or blocked

            blocked = plan_object_grant_for_dependency(
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
                sql_lines
            ) or blocked

        exists = check_object_exists(
            obclient_cmd,
            timeout,
            dep_full,
            dep_type,
            exists_cache,
            planned_objects
        )
        if exists is None:
            plan_lines.append(f"BLOCK: 无法确认对象是否存在 {dep_full}({dep_type})")
            blocked = True
            continue
        if exists:
            plan_lines.append(f"EXISTS: {dep_full}({dep_type})")
            continue
        ddl_path, ddl_source = select_fixup_script_for_node_with_fallback(
            node,
            object_index,
            name_index,
            done_object_index,
            done_name_index
        )
        if not ddl_path:
            plan_lines.append(f"BLOCK: 缺少 DDL for {dep_full}({dep_type})")
            blocked = True
            continue
        ddl_text, ddl_error = read_sql_text_with_limit(ddl_path, max_sql_file_bytes)
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

    def flush_buffer() -> None:
        statement = "".join(buffer).strip()
        if statement:
            statements.append(statement)
        buffer.clear()

    for line in sql_text.splitlines(keepends=True):
        if not slash_block and (RE_BLOCK_START.match(line) or RE_ANON_BLOCK_START.match(line)):
            slash_block = True
        stripped = line.strip()
        if not in_single and not in_double and block_comment_depth == 0 and stripped == "/":
            buffer.append(line)
            flush_buffer()
            slash_block = False
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

    if buffer:
        flush_buffer()

    return statements


def run_sql(obclient_cmd: List[str], sql_text: str, timeout: Optional[int]) -> subprocess.CompletedProcess:
    """Execute SQL text by piping it to obclient."""
    try:
        return subprocess.run(
            obclient_cmd,
            input=sql_text,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise ConfigError(f"obclient 不存在或不可执行: {exc}") from exc
    except PermissionError as exc:
        raise ConfigError(f"obclient 权限不足: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"调用 obclient 失败: {exc}") from exc


def execute_sql_statements(
    obclient_cmd: List[str],
    sql_text: str,
    timeout: Optional[int]
) -> ExecutionSummary:
    statements = split_sql_statements(sql_text)
    failures: List[StatementFailure] = []
    current_schema: Optional[str] = None

    for idx, statement in enumerate(statements, start=1):
        if not statement.strip():
            continue
        match = CURRENT_SCHEMA_PATTERN.match(statement.strip())
        if match:
            current_schema = match.group("schema")
        statement_to_run = statement
        if current_schema and not match:
            statement_to_run = f"ALTER SESSION SET CURRENT_SCHEMA = {current_schema};\n{statement}"
        try:
            result = run_sql(obclient_cmd, statement_to_run, timeout)
        except subprocess.TimeoutExpired:
            timeout_label = "no-timeout" if timeout is None else f"> {timeout} 秒"
            failures.append(StatementFailure(idx, f"执行超时 ({timeout_label})", statement_to_run))
            continue

        error_msg = extract_execution_error(result)
        if error_msg:
            failures.append(StatementFailure(idx, error_msg, statement_to_run))

    return ExecutionSummary(statements=len(statements), failures=failures)


def check_obclient_connectivity(
    obclient_cmd: List[str],
    timeout: Optional[int]
) -> Tuple[bool, str]:
    """Run a lightweight connectivity check for obclient."""
    summary = execute_sql_statements(obclient_cmd, "SELECT 1 FROM DUAL;", timeout)
    if summary.failures:
        err = summary.failures[0].error if summary.failures else "执行失败"
        return False, err
    return True, ""


def run_query_lines(
    obclient_cmd: List[str],
    sql_text: str,
    timeout: Optional[int]
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


def query_count(
    obclient_cmd: List[str],
    sql_text: str,
    timeout: Optional[int]
) -> Optional[int]:
    ok, lines, _err = run_query_lines(obclient_cmd, sql_text, timeout)
    if not ok:
        return None
    for line in lines:
        token = line.split("\t", 1)[0].strip()
        if token.isdigit():
            return int(token)
    return 0


def query_single_column(
    obclient_cmd: List[str],
    sql_text: str,
    timeout: Optional[int],
    column_name: str
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
    obclient_cmd: List[str],
    sql_text: str,
    timeout: Optional[int],
    column_name: str
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


def query_existing_schemas(
    obclient_cmd: List[str],
    timeout: Optional[int]
) -> Tuple[Set[str], str]:
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
    obclient_cmd: List[str],
    timeout: Optional[int],
    login_user: str
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
        "PRIVILEGE"
    )
    if ok_direct:
        effective.update(direct_values)

    ok_roles, role_values, _ = query_single_column_values(
        obclient_cmd,
        f"SELECT GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE = '{_escape(user_u)}';",
        timeout,
        "GRANTED_ROLE"
    )
    if ok_roles and role_values:
        role_list = sorted(r for r in role_values if r)
        literals = ",".join("'" + _escape(r) + "'" for r in role_list)
        ok_role_privs, role_priv_values, _ = query_single_column_values(
            obclient_cmd,
            f"SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE IN ({literals});",
            timeout,
            "PRIVILEGE"
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
    files_with_layer: List[Tuple[int, Path]],
    current_user: str,
    target_schemas: Set[str]
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
    files_with_layer: List[Tuple[int, Path]]
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
            len(summary.existing_schemas)
        )
    else:
        log.warning("目标库 schema 查询失败，无法提前判断缺失 schema。")
    if summary.missing_schemas:
        log.warning(
            "前置检查发现缺失 schema (%d): %s",
            len(summary.missing_schemas),
            ", ".join(sorted(summary.missing_schemas))
        )
    if summary.required_sys_privileges:
        log.info(
            "跨 schema 预估需要系统权限: %s",
            ", ".join(sorted(summary.required_sys_privileges))
        )
    if summary.missing_sys_privileges:
        log.warning(
            "前置检查发现可能缺少系统权限 (%d): %s",
            len(summary.missing_sys_privileges),
            ", ".join(sorted(summary.missing_sys_privileges))
        )
    if report_path:
        log.info("前置检查清单: %s", report_path)


def build_fixup_object_index(
    files_with_layer: List[Tuple[int, Path]]
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
    root_dir: Path,
    include_dirs: Optional[Set[str]] = None,
    exclude_dirs: Optional[Set[str]] = None
) -> List[Path]:
    files: List[Path] = []
    if not root_dir.exists():
        return files
    include_set = {d.lower() for d in include_dirs or set()}
    exclude_set = {d.lower() for d in exclude_dirs or set()}
    for subdir in sorted(root_dir.iterdir()):
        if not subdir.is_dir():
            continue
        if subdir.name.lower() in exclude_set:
            continue
        if include_set and subdir.name.lower() not in include_set:
            continue
        for sql_file in iter_sql_files_recursive(subdir):
            if sql_file.is_file():
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
    fixup_dir: Path,
    exclude_dirs: Set[str],
    include_dirs: Optional[Set[str]] = None
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
                content = grant_file.read_text(encoding="utf-8")
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
                        grant_type=grant_type
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
    grantee: str,
    object_full: str,
    required_priv: str,
    with_grant_option: bool = False
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


def parse_grant_statement(statement: str) -> Optional[Tuple[str, Tuple[str, ...], Optional[str], List[str]]]:
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


def select_dependency_script(
    schema: Optional[str],
    name: str,
    view_schema: Optional[str],
    object_index: Dict[Tuple[str, str], List[Path]],
    name_index: Dict[str, List[Path]]
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
    view_schema: Optional[str]
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
    require_grant_option: bool = False
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
    grant_index: GrantIndex,
    grantee: str,
    required_priv: str
) -> List[GrantEntry]:
    implied = SYS_PRIV_IMPLICATIONS.get((required_priv or "").upper(), set())
    if not implied:
        return []
    entries = grant_index.by_grantee_sys.get(grantee, [])
    if not entries:
        return []
    implied_upper = {p.upper() for p in implied}
    return [
        entry for entry in entries
        if any(priv.upper() in implied_upper for priv in entry.privileges)
    ]


def find_grant_entries_by_priority(
    grantee: str,
    target_full: str,
    required_priv: str,
    grant_index_miss: GrantIndex,
    grant_index_all: GrantIndex,
    require_grant_option: bool = False
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
        grant_index_miss, grantee, required_priv
    )
    if entries:
        return entries, "grants_miss"
    entries = select_system_grant_entries_for_priv(
        grant_index_all, grantee, required_priv
    )
    if entries:
        return entries, "grants_all"
    return [], ""


def apply_grant_entries(
    obclient_cmd: List[str],
    entries: List[GrantEntry],
    timeout: Optional[int],
    applied_grants: Set[str]
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
    blocked: bool,
    skipped: bool,
    view_exists: Optional[bool],
    failure_count: int
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
    error_message: str
) -> bool:
    if len(entries) >= limit:
        return False
    error_code = parse_error_code(error_message)
    object_name = parse_grant_object(statement)
    message = " ".join((error_message or "").split())
    if len(message) > 200:
        message = message[:200] + "..."
    entries.append(
        ErrorReportEntry(
            file_path=relative_path,
            statement_index=statement_index,
            error_code=error_code,
            object_name=object_name,
            message=message or "-"
        )
    )
    return True


def write_error_report(
    entries: List[ErrorReportEntry],
    fixup_dir: Path,
    limit: int,
    truncated: bool
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
        "FILE | STMT_INDEX | ERROR_CODE | OBJECT | MESSAGE"
    ]
    for entry in entries:
        lines.append(
            f"{entry.file_path} | {entry.statement_index} | {entry.error_code} | {entry.object_name} | {entry.message}"
        )
    report_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return report_path


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
    state_ledger: Optional[FixupStateLedger] = None
) -> Tuple[ScriptResult, ExecutionSummary, int, int, bool]:
    relative_path = sql_path.relative_to(repo_root)
    sql_text, read_error = read_sql_text_with_limit(sql_path, max_sql_file_bytes)
    if read_error:
        msg = read_error
        log.error("%s %s -> ERROR (%s)", label_prefix, relative_path, msg)
        failure = StatementFailure(0, msg, "")
        return ScriptResult(relative_path, "ERROR", msg, layer), ExecutionSummary(0, [failure]), 0, 0, False
    fingerprint = FixupStateLedger.fingerprint(sql_text or "")
    if state_ledger and state_ledger.is_completed(relative_path, fingerprint):
        msg = "状态账本命中，跳过重复执行"
        log.warning("%s %s -> SKIP (%s)", label_prefix, relative_path, msg)
        return ScriptResult(relative_path, "SKIPPED", msg, layer), ExecutionSummary(0, []), 0, 0, False

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
        stripped = statement.strip()
        is_grant = stripped.upper().startswith("GRANT ")
        # grants_* 文件仅保留 GRANT 的重试语义，避免非 GRANT 语句反复执行。
        if not is_grant:
            skipped_non_grant_count += 1
            continue
        executed_count += 1

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
            if is_grant:
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
            skipped_non_grant_count
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
            return ScriptResult(relative_path, "ERROR", move_note.strip(), layer), ExecutionSummary(executed_count, [failure]), removed_count, 0, truncated
        if state_ledger:
            state_ledger.clear(relative_path)
        log.info(
            "%s %s -> OK %s (已清理授权 %d 条)",
            label_prefix,
            relative_path,
            move_note,
            removed_count
        )
        return ScriptResult(relative_path, "SUCCESS", move_note.strip(), layer), summary, removed_count, 0, truncated

    # Rewrite file with remaining statements (non-grants + failed grants)
    rewritten = "\n\n".join(stmt.strip() for stmt in kept_statements if stmt.strip()).rstrip()
    try:
        tmp_path = sql_path.with_suffix(sql_path.suffix + ".tmp")
        tmp_path.write_text(rewritten + "\n", encoding="utf-8")
        tmp_path.replace(sql_path)
        log.info(
            "%s %s -> FAIL (%d/%d statements), 保留失败语句 %d 条, 已清理授权 %d 条",
            label_prefix,
            relative_path,
            len(failures),
            executed_count,
            len(kept_statements),
            removed_count
        )
    except Exception as exc:
        log.warning("%s %s -> 重写失败: %s", label_prefix, relative_path, str(exc)[:200])

    first_error = failures[0].error if failures else "执行失败"
    return ScriptResult(relative_path, "FAILED", first_error, layer), summary, removed_count, len(kept_statements), truncated


def execute_script_with_summary(
    obclient_cmd: List[str],
    sql_path: Path,
    repo_root: Path,
    done_dir: Path,
    timeout: Optional[int],
    layer: int,
    label_prefix: str,
    max_sql_file_bytes: Optional[int],
    state_ledger: Optional[FixupStateLedger] = None
) -> Tuple[ScriptResult, ExecutionSummary]:
    relative_path = sql_path.relative_to(repo_root)
    sql_text, read_error = read_sql_text_with_limit(sql_path, max_sql_file_bytes)
    if read_error:
        msg = read_error
        log.error("%s %s -> ERROR (%s)", label_prefix, relative_path, msg)
        return ScriptResult(relative_path, "ERROR", msg, layer), ExecutionSummary(0, [StatementFailure(0, msg, "")])
    fingerprint = FixupStateLedger.fingerprint(sql_text or "")
    if state_ledger and state_ledger.is_completed(relative_path, fingerprint):
        msg = "状态账本命中，跳过重复执行"
        log.warning("%s %s -> SKIP (%s)", label_prefix, relative_path, msg)
        return ScriptResult(relative_path, "SKIPPED", msg, layer), ExecutionSummary(0, [])

    if not (sql_text or "").strip():
        log.warning("%s %s -> SKIP (文件为空)", label_prefix, relative_path)
        return ScriptResult(relative_path, "SKIPPED", "文件为空", layer), ExecutionSummary(0, [])

    summary = execute_sql_statements(obclient_cmd, sql_text or "", timeout=timeout)
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
            return ScriptResult(relative_path, "ERROR", move_note.strip(), layer), ExecutionSummary(summary.statements, [failure])
        if state_ledger:
            state_ledger.clear(relative_path)
        log.info("%s %s -> OK %s", label_prefix, relative_path, move_note)
        return ScriptResult(relative_path, "SUCCESS", move_note.strip(), layer), summary

    first_error = summary.failures[0].error if summary.failures else "执行失败"
    log.warning(
        "%s %s -> FAIL (%d/%d statements)",
        label_prefix,
        relative_path,
        len(summary.failures),
        summary.statements
    )
    for failure in summary.failures[:3]:
        log.warning("  [%d] %s", failure.index, safe_first_line(failure.error, 200, "执行失败"))
    return ScriptResult(relative_path, "FAILED", first_error, layer), summary


def query_invalid_objects(
    obclient_cmd: List[str],
    timeout: Optional[int],
    allowed_owners: Optional[Set[str]] = None
) -> List[Tuple[str, str, str]]:
    """
    Query INVALID objects from OceanBase.
    
    Returns:
        List of (owner, object_name, object_type) tuples
    """
    owner_filter = ""
    if allowed_owners:
        owners = sorted({owner.strip().upper() for owner in allowed_owners if owner and owner.strip()})
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
        for line in result.stdout.strip().splitlines():
            parts = line.split('\t')
            if len(parts) >= 3:
                invalid_objects.append((parts[0].strip(), parts[1].strip(), parts[2].strip()))
        
        return invalid_objects
    except Exception as exc:
        log.warning("查询 INVALID 对象失败: %s", exc)
        return []


def is_object_invalid(
    obclient_cmd: List[str],
    timeout: Optional[int],
    owner: str,
    obj_name: str,
    obj_type: str
) -> Optional[bool]:
    sql = (
        "SELECT COUNT(*) FROM DBA_OBJECTS "
        f"WHERE OWNER='{escape_sql_literal(owner)}' "
        f"AND OBJECT_NAME='{escape_sql_literal(obj_name)}' "
        f"AND OBJECT_TYPE='{escape_sql_literal(obj_type)}' "
        "AND STATUS='INVALID'"
    )
    count = query_count(obclient_cmd, sql, timeout)
    if count is None:
        return None
    return count > 0


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


def recompile_invalid_objects(
    obclient_cmd: List[str],
    timeout: Optional[int],
    max_retries: int = MAX_RECOMPILE_RETRIES,
    allowed_owners: Optional[Set[str]] = None
) -> Tuple[int, int]:
    """
    Recompile INVALID objects multiple times until all are VALID or max retries reached.
    
    Returns:
        (total_recompiled, remaining_invalid)
    """
    total_recompiled = 0
    
    for retry in range(max_retries):
        invalid_objects = query_invalid_objects(obclient_cmd, timeout, allowed_owners=allowed_owners)
        if not invalid_objects:
            return total_recompiled, 0
        
        log.info(
            "重编译轮次 %d/%d, INVALID=%d",
            retry + 1,
            max_retries,
            len(invalid_objects)
        )
        
        recompiled_this_round = 0
        for owner, obj_name, obj_type in invalid_objects:
            compile_sql = build_compile_statement(owner, obj_name, obj_type)
            if not compile_sql:
                log.info("  SKIP %s.%s (%s): unsupported compile type", owner, obj_name, obj_type)
                continue
            try:
                result = run_sql(obclient_cmd, compile_sql, timeout)
                error_msg = extract_execution_error(result)
                if not error_msg:
                    still_invalid = is_object_invalid(obclient_cmd, timeout, owner, obj_name, obj_type)
                    if still_invalid is False:
                        recompiled_this_round += 1
                        log.info("  OK %s.%s (%s)", owner, obj_name, obj_type)
                    elif still_invalid is True:
                        log.warning("  FAIL %s.%s (%s): still INVALID after COMPILE", owner, obj_name, obj_type)
                    else:
                        log.warning("  WARN %s.%s (%s): 无法确认编译后状态，未计入成功", owner, obj_name, obj_type)
                else:
                    log.warning(
                        "  FAIL %s.%s (%s): %s",
                        owner,
                        obj_name,
                        obj_type,
                        str(error_msg)[:100]
                    )
            except Exception as e:
                log.warning(
                    "  FAIL %s.%s (%s): %s",
                    owner,
                    obj_name,
                    obj_type,
                    str(e)[:100]
                )
        
        total_recompiled += recompiled_this_round
        
        if recompiled_this_round == 0:
            # No progress, stop retrying
            break
    
    # Final check
    final_invalid = query_invalid_objects(obclient_cmd, timeout, allowed_owners=allowed_owners)
    return total_recompiled, len(final_invalid)


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
          注意: grants_deferred/ 默认跳过，需在补齐对象后显式执行
        
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
    
    return parser.parse_args()


def parse_csv_args(arg_list: List[str]) -> List[str]:
    values: List[str] = []
    for item in arg_list:
        if not item:
            continue
        values.extend([p.strip() for p in item.split(",") if p.strip()])
    return values


def main() -> None:
    """
    Main entry point with optional iterative fixup support.
    
    New --iterative flag enables multi-round execution with:
    - Automatic retry of failed scripts
    - Convergence detection
    - Error pattern analysis
    - Progress tracking across rounds
    """
    args = parse_args()
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
            merged = set(d.lower() for d in only_dirs) | set(d.lower() for d in mapped_dirs)
            only_dirs = sorted(merged)
        else:
            only_dirs = [d.lower() for d in mapped_dirs]
    else:
        only_dirs = [d.lower() for d in only_dirs] if only_dirs else []
    
    exclude_dirs = [d.lower() for d in exclude_dirs]
    default_excludes = {"tables_unsupported", "unsupported", "constraint_validate_later", "grants_deferred"}
    if not getattr(args, "allow_table_create", False):
        # Safety first: table create scripts are risky in migration workflows
        # because they can create empty target tables if OMS data load is skipped.
        default_excludes.add("table")
    exclude_set = set(exclude_dirs) | default_excludes
    if only_dirs:
        exclude_set -= set(only_dirs)
    if not getattr(args, "allow_table_create", False):
        # Keep table excluded unless explicit opt-in, even with --only-dirs table.
        exclude_set.add("table")
    exclude_dirs = sorted(exclude_set)

    if "table" in only_dirs and not getattr(args, "allow_table_create", False):
        log.warning(
            "检测到 --only-dirs/--only-types 包含 table，但默认安全策略已禁用 table 执行。"
            "如需执行建表脚本，请显式添加 --allow-table-create。"
        )
    if not getattr(args, "allow_table_create", False):
        log.warning("安全模式: 默认跳过 fixup_scripts/table/（防止误建空表）。")
    if "grants_deferred" not in only_dirs:
        log.warning("安全模式: 默认跳过 fixup_scripts/grants_deferred/（需对象补齐后再执行）。")
    
    # Load configuration
    try:
        ob_cfg, fixup_dir, repo_root, log_level, report_dir, fixup_settings, max_sql_file_bytes = load_ob_config(
            config_arg.resolve()
        )
    except ConfigError as exc:
        log.error("配置错误: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.error("致命错误: 无法读取配置: %s", exc)
        sys.exit(1)

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
    
    # Check if iterative mode requested via config or args
    iterative_mode = getattr(args, 'iterative', False)
    max_rounds = getattr(args, 'max_rounds', 10)
    min_progress = getattr(args, 'min_progress', 1)
    hot_reload_runtime = init_fixup_hot_reload_runtime(config_arg.resolve())
    if hot_reload_runtime and hot_reload_runtime.mode == "round" and not iterative_mode:
        log.warning(
            "config_hot_reload_mode=round 仅在 --iterative 下生效；本次运行不会热加载。"
        )

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
                    max_sql_file_bytes
                )
            elif iterative_mode:
                run_iterative_fixup(
                    args, ob_cfg, fixup_dir, repo_root, report_dir,
                    only_dirs, exclude_dirs,
                    fixup_settings,
                    max_sql_file_bytes,
                    max_rounds, min_progress,
                    hot_reload_runtime=hot_reload_runtime
                )
            else:
                run_single_fixup(
                    args, ob_cfg, fixup_dir, repo_root, report_dir,
                    only_dirs, exclude_dirs,
                    fixup_settings,
                    max_sql_file_bytes
                )
    except ConfigError as exc:
        log.error("执行失败: %s", exc)
        sys.exit(1)


def run_single_fixup(
    args,
    ob_cfg: Dict[str, str],
    fixup_dir: Path,
    repo_root: Path,
    report_dir: Path,
    only_dirs: List[str],
    exclude_dirs: List[str],
    fixup_settings: FixupAutoGrantSettings,
    max_sql_file_bytes: Optional[int]
) -> None:
    """Original single-round fixup execution (backward compatible)."""
    
    log_section("修补脚本执行器")
    log.info("配置文件: %s", Path(args.config).resolve())
    log.info("日志级别: %s", logging.getLevelName(logging.getLogger().level))
    log.info("项目主页: %s (问题反馈: %s)", REPO_URL, REPO_ISSUES_URL)
    
    done_dir = fixup_dir / DONE_DIR_NAME
    done_dir.mkdir(exist_ok=True)
    state_ledger = FixupStateLedger(fixup_dir)
    
    # Collect SQL files
    files_with_layer = collect_sql_files_by_layer(
        fixup_dir,
        smart_order=args.smart_order,
        include_dirs=set(only_dirs) if only_dirs else None,
        exclude_dirs=set(exclude_dirs),
        glob_patterns=args.glob_patterns or None,
    )
    
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
    precheck_summary = build_fixup_precheck_summary(ob_cfg, obclient_cmd, ob_timeout, files_with_layer)
    precheck_report = write_fixup_precheck_report(fixup_dir, precheck_summary)
    log_fixup_precheck(precheck_summary, precheck_report)
    auto_grant_ctx = init_auto_grant_context(
        fixup_settings,
        report_dir,
        fixup_dir,
        exclude_dirs,
        obclient_cmd,
        ob_timeout
    )
    
    total_scripts = len(files_with_layer)
    width = len(str(total_scripts)) or 1
    results: List[ScriptResult] = []
    error_entries: List[ErrorReportEntry] = []
    error_truncated = False
    
    log_section("执行配置")
    log.info("目录: %s", fixup_dir)
    log.info("模式: %s", "依赖感知排序 (SMART ORDER)" if args.smart_order else "标准优先级排序")
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
        
        if is_grant_dir(sql_path.parent.name):
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
                state_ledger=state_ledger
            )
            error_truncated = error_truncated or truncated
            results.append(result)
        else:
            obj_type = DIR_OBJECT_TYPE_MAP.get(sql_path.parent.name.lower())
            obj_schema, obj_name = parse_object_identity_from_path(sql_path)
            obj_full = (
                f"{obj_schema}.{obj_name}" if obj_schema and obj_name else None
            )
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
                state_ledger=state_ledger
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
                            failure.error
                        )
                continue

            first_error = summary.failures[0].error if summary.failures else result.message
            error_type = classify_sql_error(first_error)
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
                    f"{label} (grant)"
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
                    state_ledger=state_ledger
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
                            failure.error
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
                    failure.error
                )
    
    # Recompilation phase
    total_recompiled = 0
    remaining_invalid = 0
    if args.recompile:
        log_subsection("重编译阶段")
        total_recompiled, remaining_invalid = recompile_invalid_objects(
            obclient_cmd, ob_timeout, args.max_retries, allowed_owners=recompile_owners
        )

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
    
    if args.recompile:
        log_subsection("重编译统计")
        log.info("重编译成功 : %d", total_recompiled)
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
                "SKIPPED": "○ 跳过"
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
        error_entries,
        fixup_dir,
        DEFAULT_ERROR_REPORT_LIMIT,
        error_truncated
    )
    if report_path:
        log.info("错误报告已输出: %s", report_path)
    state_ledger.flush()

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
    max_sql_file_bytes: Optional[int]
) -> None:
    log_section("VIEW 链路自动修复")
    log.info("配置文件: %s", Path(args.config).resolve())
    log.info("报告目录: %s", report_dir)
    log.info("项目主页: %s (问题反馈: %s)", REPO_URL, REPO_ISSUES_URL)
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
    object_index, name_index = build_fixup_object_index(files_with_layer)
    done_dir = fixup_dir / DONE_DIR_NAME
    done_object_index: Dict[Tuple[str, str], List[Path]] = {}
    done_name_index: Dict[str, List[Path]] = {}
    done_files = collect_sql_files_from_root(
        done_dir,
        include_dirs=set(only_dirs) if only_dirs else None,
        exclude_dirs=set(exclude_dirs) if exclude_dirs else None
    )
    if done_files:
        done_object_index, done_name_index = build_fixup_object_index(
            [(0, path) for path in done_files]
        )

    # 根据用户 --only-dirs 决定使用哪些 grant 目录
    only_dirs_set = set(only_dirs) if only_dirs else set()
    use_grants_miss = not only_dirs_set or "grants_miss" in only_dirs_set or "grants" in only_dirs_set
    use_grants_all = not only_dirs_set or "grants_all" in only_dirs_set
    
    grant_index_miss = build_grant_index(
        fixup_dir,
        set(exclude_dirs),
        include_dirs={"grants_miss"} if use_grants_miss else set()
    )
    grant_index_all = build_grant_index(
        fixup_dir,
        set(exclude_dirs),
        include_dirs={"grants_all"} if use_grants_all else set()
    )

    plan_dir = fixup_dir / "view_chain_plans"
    sql_dir = fixup_dir / "view_chain_sql"
    plan_dir.mkdir(parents=True, exist_ok=True)
    sql_dir.mkdir(parents=True, exist_ok=True)

    obclient_cmd = build_obclient_command(ob_cfg)
    ob_timeout = resolve_timeout_value(ob_cfg.get("timeout"))
    ok_conn, conn_err = check_obclient_connectivity(obclient_cmd, ob_timeout)
    if not ok_conn:
        log.error("OBClient 连接检查失败: %s", conn_err)
        log.error("请确认网络连通性/账号权限/obclient 可用性后重试。")
        sys.exit(1)
    precheck_summary = build_fixup_precheck_summary(ob_cfg, obclient_cmd, ob_timeout, files_with_layer)
    precheck_report = write_fixup_precheck_report(fixup_dir, precheck_summary)
    log_fixup_precheck(precheck_summary, precheck_report)

    exists_cache: Dict[Tuple[str, str], bool] = {}
    roles_cache: Dict[str, Set[str]] = LimitedCache(fixup_settings.cache_limit)
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]] = LimitedCache(fixup_settings.cache_limit)
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]] = LimitedCache(fixup_settings.cache_limit)
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

    log.info("读取 VIEW 依赖链: %d", total_views)

    allow_fallback = bool(fixup_settings.fallback)

    for idx, view_full in enumerate(sorted(chains_by_view.keys()), start=1):
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
            use_planned=False
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
                max_sql_file_bytes
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

        plan_content = "\n".join(
            ["# VIEW chain autofix plan"] + chain_summary + ["", "# Steps:"] + plan_lines
        ).rstrip() + "\n"
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

        summary = execute_sql_statements(obclient_cmd, sql_text, ob_timeout)
        invalidate_exists_cache(exists_cache, planned_objects | {(view_key, root_type)})
        post_exists = check_object_exists(
            obclient_cmd,
            ob_timeout,
            view_key,
            root_type,
            exists_cache,
            planned_objects,
            use_planned=False
        )
        status = classify_view_chain_status(
            blocked=False,
            skipped=False,
            view_exists=post_exists,
            failure_count=len(summary.failures)
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
            executed_views += 1
            log.info("%s [VIEW_CHAIN] %s 执行成功 (%d statements)。", label, view_key, summary.statements)
        elif status == "PARTIAL":
            partial_views += 1
            log.warning(
                "%s [VIEW_CHAIN] %s 部分成功 (%d/%d statements)。",
                label,
                view_key,
                len(summary.failures),
                summary.statements
            )
        else:
            failed_views += 1
            log.warning(
                "%s [VIEW_CHAIN] %s 执行失败 (%d/%d statements)。",
                label,
                view_key,
                len(summary.failures),
                summary.statements
            )

    log_section("VIEW 链路修复完成")
    log.info("视图总数: %d", total_views)
    log.info("执行成功: %d", executed_views)
    log.info("部分成功: %d", partial_views)
    log.info("跳过已存在: %d", skipped_views)
    log.info("阻塞跳过: %d", blocked_views)
    log.info("执行失败: %d", failed_views)

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
    hot_reload_runtime: Optional[FixupHotReloadRuntime] = None
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
    log.info("")
    
    done_dir = fixup_dir / DONE_DIR_NAME
    done_dir.mkdir(exist_ok=True)
    state_ledger = FixupStateLedger(fixup_dir)
    
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
        current_fixup_settings,
        report_dir,
        fixup_dir,
        exclude_dirs,
        obclient_cmd,
        ob_timeout
    )
    error_entries: List[ErrorReportEntry] = []
    error_truncated = False
    
    round_num = 0
    cumulative_success = 0
    cumulative_failed = 0
    active_failed_paths: Set[Path] = set()
    recompile_owners: Set[str] = set()
    
    all_round_results = []
    
    while round_num < max_rounds:
        round_num += 1
        current_ob_cfg, current_fixup_settings, current_max_sql_file_bytes, fixup_settings_changed = apply_fixup_hot_reload_at_round(
            hot_reload_runtime,
            round_num,
            current_ob_cfg,
            fixup_dir,
            report_dir,
            current_fixup_settings,
            current_max_sql_file_bytes
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
                        ob_timeout
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
        stale_failed = [path for path in sorted(active_failed_paths, key=str) if not path.exists()]
        if stale_failed:
            for path in stale_failed:
                active_failed_paths.discard(path)
            log.info("历史失败脚本已不存在，已从失败集合移除: %d", len(stale_failed))
        
        if not files_with_layer:
            log.info("✓ 所有脚本已成功执行！")
            break
        if round_num == 1:
            precheck_summary = build_fixup_precheck_summary(ob_cfg, obclient_cmd, ob_timeout, files_with_layer)
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

            if sql_path in pre_executed:
                log.info("%s %s -> SKIP (已由依赖解析执行)", label, relative_path)
                continue

            if is_grant_dir(sql_path.parent.name):
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
                    state_ledger=state_ledger
            )
                error_truncated = error_truncated or truncated
                round_results.append(result)
                continue

            obj_type = DIR_OBJECT_TYPE_MAP.get(sql_path.parent.name.lower())
            obj_schema, obj_name = parse_object_identity_from_path(sql_path)
            obj_full = (
                f"{obj_schema}.{obj_name}" if obj_schema and obj_name else None
            )
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
                state_ledger=state_ledger
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
                            failure.error
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
                            missing_schema,
                            missing_name,
                            view_schema,
                            object_index,
                            name_index
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
                                state_ledger=state_ledger
                            )
                            pre_executed.add(dep_path)
                            round_results.append(dep_result)
                            handled = dep_result.status == "SUCCESS"
                        else:
                            log.warning(
                                "%s %s -> 无法解析依赖对象 %s",
                                label,
                                relative_path,
                                missing_name
                            )
                    else:
                        log.warning("%s %s -> 缺失对象未解析", label, relative_path)

            if error_type == FailureType.PERMISSION_DENIED:
                if auto_grant_ctx and obj_full and obj_type:
                    applied, _blocked = execute_auto_grant_for_object(
                        auto_grant_ctx,
                        obj_full,
                        obj_type,
                        f"{label} (grant)"
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
                    state_ledger=state_ledger
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
                            failure.error
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
                        failure.error
                    )
        
        # Round summary
        round_success = sum(1 for r in round_results if r.status == "SUCCESS")
        round_failed = sum(1 for r in round_results if r.status in ("FAILED", "ERROR"))
        round_skipped = sum(1 for r in round_results if r.status == "SKIPPED")
        
        cumulative_success += round_success
        for item in round_results:
            if item.status in ("FAILED", "ERROR"):
                active_failed_paths.add(item.path)
            else:
                active_failed_paths.discard(item.path)
        cumulative_failed = len(active_failed_paths)
        
        log_subsection(f"第 {round_num} 轮结果")
        log.info("本轮成功: %d", round_success)
        log.info("本轮失败: %d", round_failed)
        log.info("本轮跳过: %d", round_skipped)
        log.info("累计成功: %d", cumulative_success)
        log.info("累计失败: %d", cumulative_failed)
        log.info("")
        
        all_round_results.append({
            'round': round_num,
            'success': round_success,
            'failed': round_failed,
            'results': round_results
        })
        
        # Convergence check
        if round_success == 0:
            log.warning("本轮无新成功脚本，停止迭代。")
            
            # Analyze remaining failures
            failures_by_type = analyze_failure_patterns(round_results)
            if failures_by_type:
                log_failure_analysis(failures_by_type)
            
            break
        
        if round_success < min_progress:
            log.warning(f"本轮成功数 ({round_success}) 低于最小进展阈值 ({min_progress})，停止迭代。")
            break
        
        # Recompile after each round if enabled
        if args.recompile:
            log_subsection("轮次重编译")
            recomp, invalid = recompile_invalid_objects(
                obclient_cmd, ob_timeout, 2, allowed_owners=recompile_owners  # Fewer retries per round
            )
            if recomp > 0:
                log.info("重编译成功 %d 个对象", recomp)
    
    # Final recompilation
    total_recompiled = 0
    remaining_invalid = 0
    if args.recompile:
        log_section("最终重编译")
        total_recompiled, remaining_invalid = recompile_invalid_objects(
            obclient_cmd, ob_timeout, args.max_retries, allowed_owners=recompile_owners
        )

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
    
    if args.recompile:
        log_subsection("最终重编译统计")
        log.info("重编译成功: %d", total_recompiled)
        log.info("仍为INVALID: %d", remaining_invalid)
    
    # Final failure analysis
    if round_num > 0 and all_round_results:
        final_results = all_round_results[-1]['results']
        failures_by_type = analyze_failure_patterns(final_results)
        if failures_by_type:
            log_failure_analysis(failures_by_type)
    
    # Per-round breakdown
    if len(all_round_results) > 1:
        log_subsection("各轮执行统计")
        for round_data in all_round_results:
            log.info("第 %d 轮: 成功 %d, 失败 %d",
                    round_data['round'],
                    round_data['success'],
                    round_data['failed'])

    report_path = write_error_report(
        error_entries,
        fixup_dir,
        DEFAULT_ERROR_REPORT_LIMIT,
        error_truncated
    )
    if report_path:
        log.info("错误报告已输出: %s", report_path)
    reload_report_path = write_fixup_hot_reload_events_report(fixup_dir, hot_reload_runtime)
    if reload_report_path:
        log.info("配置热加载事件已输出: %s", reload_report_path)
    state_ledger.flush()

    log_section("执行结束")
    
    exit_code = 0 if len(active_failed_paths) == 0 else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
