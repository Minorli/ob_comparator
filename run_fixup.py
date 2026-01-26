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
    --only-dirs       : Filter by subdirectories
    --only-types      : Filter by object types
    --glob            : Filter by filename patterns
"""

from __future__ import annotations

import argparse
import configparser
import fnmatch
import json
import logging
import re
import shutil
import subprocess
import sys
import textwrap
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

CONFIG_DEFAULT_PATH = "config.ini"
DEFAULT_FIXUP_DIR = "fixup_scripts"
DONE_DIR_NAME = "done"
DEFAULT_OBCLIENT_TIMEOUT = 60
DEFAULT_FIXUP_TIMEOUT = 3600
DEFAULT_ERROR_REPORT_LIMIT = 200
MAX_RECOMPILE_RETRIES = 5
REPO_URL = "https://github.com/Minorli/ob_comparator"
REPO_ISSUES_URL = f"{REPO_URL}/issues"

LOG_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FILE_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
LOG_SECTION_WIDTH = 80

CURRENT_SCHEMA_PATTERN = re.compile(
    r'ALTER\s+SESSION\s+SET\s+CURRENT_SCHEMA\s*=\s*(?P<schema>"[^"]+"|[A-Z0-9_$#]+)',
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
    except Exception:
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
        except Exception:
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


init_console_logging()
log = logging.getLogger(__name__)


# Error classification for intelligent retry
class FailureType:
    """Classification of SQL execution failures for retry logic."""
    MISSING_OBJECT = "missing_object"        # Dependency object doesn't exist -> retryable
    PERMISSION_DENIED = "permission_denied"  # Insufficient privileges -> needs grants
    SYNTAX_ERROR = "syntax_error"            # SQL syntax error -> needs DDL fix
    DATA_CONFLICT = "data_conflict"          # Unique/constraint violation -> needs data cleanup
    DUPLICATE_OBJECT = "duplicate_object"    # Object already exists -> can skip
    INVALID_IDENTIFIER = "invalid_identifier" # Column/table name error -> needs DDL fix
    NAME_IN_USE = "name_in_use"              # Name already used -> needs resolution
    TIMEOUT = "timeout"                       # Execution timeout -> may retry
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
    if any(code in stderr_upper for code in ['ORA-00942', 'ORA-04043', 'ORA-06512']):
        if 'TABLE OR VIEW DOES NOT EXIST' in stderr_upper or 'OBJECT DOES NOT EXIST' in stderr_upper:
            return FailureType.MISSING_OBJECT
    if 'TABLE OR VIEW DOES NOT EXIST' in stderr_upper or 'OBJECT DOES NOT EXIST' in stderr_upper:
        return FailureType.MISSING_OBJECT
    
    # Permission denied (needs grant scripts)
    if 'ORA-01031' in stderr_upper or 'ORA-01720' in stderr_upper or 'INSUFFICIENT PRIVILEGES' in stderr_upper:
        return FailureType.PERMISSION_DENIED
    
    # Data conflict (unique constraint violation)
    if 'ORA-00001' in stderr_upper or 'UNIQUE CONSTRAINT' in stderr_upper:
        return FailureType.DATA_CONFLICT
    
    # Invalid identifier (DDL needs fix)
    if 'ORA-00904' in stderr_upper:
        return FailureType.INVALID_IDENTIFIER
    
    # Name already in use (object exists)
    if 'ORA-00955' in stderr_upper or 'NAME IS ALREADY USED' in stderr_upper:
        return FailureType.NAME_IN_USE
    
    # Syntax errors (DDL needs fix)
    if any(code in stderr_upper for code in ['ORA-00900', 'ORA-00901', 'ORA-00902', 'ORA-00903']):
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
        log.info("   建议: 检查并执行 fixup_scripts/grants_miss/ 下的授权脚本 (全量审计见 grants_all)")
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
    
    # Unknown errors
    if FailureType.UNKNOWN in failures_by_type:
        items = failures_by_type[FailureType.UNKNOWN]
        log.info("❓ 未知错误: %d 个", len(items))
        log.info("   建议: 查看详细错误信息进行诊断")
        if len(items) <= 3:
            for item in items[:3]:
                msg_preview = item.message.splitlines()[0][:80] if item.message else "无错误信息"
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
    ["grants"],                                      # Layer 3: Grants BEFORE dependent objects
    ["view", "synonym"],                             # Layer 4: Simple dependent objects
    ["materialized_view"],                           # Layer 5: MVIEWs
    ["type"],                                        # Layer 6: Types (specs)
    ["package"],                                     # Layer 7: Package specs
    ["procedure", "function"],                       # Layer 8: Standalone routines
    ["type_body", "package_body"],                   # Layer 9: Type/package bodies
    ["constraint", "index"],                         # Layer 10: Constraints and indexes
    ["trigger"],                                     # Layer 11: Triggers (last)
    ["job", "schedule"],                             # Layer 12: Jobs
]

GRANT_DIRS = {"grants", "grants_miss", "grants_all"}

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
    "TRIGGER": "EXECUTE",
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
    "TRIGGER",
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
    else:
        if "grants_miss" in available and "grants_miss" not in exclude_set:
            grant_dirs.append("grants_miss")
        elif "grants" in available and "grants" not in exclude_set:
            grant_dirs.append("grants")

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
    stats: AutoGrantStats


def load_ob_config(config_path: Path) -> Tuple[Dict[str, str], Path, Path, str, Path, FixupAutoGrantSettings]:
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
    ob_cfg["port"] = str(int(ob_cfg["port"]))

    try:
        fixup_raw = parser.get("SETTINGS", "fixup_cli_timeout", fallback="").strip()
        if fixup_raw:
            fixup_timeout = int(fixup_raw)
        else:
            fixup_timeout = parser.getint("SETTINGS", "obclient_timeout", fallback=DEFAULT_FIXUP_TIMEOUT)
        if fixup_timeout is None or fixup_timeout < 0:
            fixup_timeout = DEFAULT_FIXUP_TIMEOUT
    except Exception:
        fixup_timeout = DEFAULT_FIXUP_TIMEOUT
    ob_cfg["timeout"] = None if fixup_timeout == 0 else fixup_timeout

    repo_root = config_path.parent.resolve()
    fixup_dir = parser.get("SETTINGS", "fixup_dir", fallback=DEFAULT_FIXUP_DIR).strip()
    fixup_path = (repo_root / fixup_dir).resolve()

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
    fixup_settings = FixupAutoGrantSettings(
        enabled=auto_grant_enabled,
        types=auto_grant_types,
        fallback=auto_grant_fallback
    )
    return ob_cfg, fixup_path, repo_root, log_level, report_path, fixup_settings


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
    
    files_with_layer: List[Tuple[int, Path]] = []
    
    if smart_order:
        # Use dependency layers
        for layer_idx, layer_dirs in enumerate(DEPENDENCY_LAYERS):
            for dir_name in layer_dirs:
                if dir_name == "grants":
                    for grant_dir in grant_dirs:
                        if grant_dir not in subdirs:
                            continue
                        for sql_file in sorted(subdirs[grant_dir].glob("*.sql")):
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

                for sql_file in sorted(subdirs[dir_name].glob("*.sql")):
                    if not sql_file.is_file():
                        continue
                    rel_str = str(sql_file.relative_to(fixup_dir))
                    if not any(fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p)
                              for p in glob_patterns):
                        continue
                    files_with_layer.append((layer_idx, sql_file))
        
        # Add remaining directories not in DEPENDENCY_LAYERS
        all_layer_dirs = {d for layer in DEPENDENCY_LAYERS for d in layer}
        all_layer_dirs.update(grant_dirs)
        for dir_name in sorted(subdirs.keys()):
            if dir_name in all_layer_dirs:
                continue
            if is_grant_dir(dir_name) and dir_name not in grant_dirs:
                continue
            if include_dirs and dir_name not in include_dirs:
                continue
                
            for sql_file in sorted(subdirs[dir_name].glob("*.sql")):
                if not sql_file.is_file():
                    continue
                rel_str = str(sql_file.relative_to(fixup_dir))
                if not any(fnmatch.fnmatch(rel_str, p) or fnmatch.fnmatch(sql_file.name, p) 
                          for p in glob_patterns):
                    continue
                files_with_layer.append((999, sql_file))  # Unknown layer
    else:
        # Original priority order (backward compatible)
        priority = [
            "sequence", "table", "table_alter", "constraint", "index",
            "view", "materialized_view", "synonym", "procedure", "function",
            "package", "package_body", "type", "type_body", "trigger",
            "job", "schedule", "grants",
        ]
        
        seen = set()
        for idx, name in enumerate(priority):
            if name == "grants":
                for grant_dir in grant_dirs:
                    if grant_dir not in subdirs:
                        continue
                    for sql_file in sorted(subdirs[grant_dir].glob("*.sql")):
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
                for sql_file in sorted(subdirs[name].glob("*.sql")):
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
            for sql_file in sorted(subdirs[name].glob("*.sql")):
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
    stripped = re.sub(r"/\*.*?\*/", "", statement, flags=re.DOTALL)
    for line in stripped.splitlines():
        line_strip = line.strip()
        if not line_strip:
            continue
        if line_strip.startswith("--"):
            continue
        if "--" in line_strip:
            line_strip = line_strip.split("--", 1)[0].strip()
        if line_strip:
            return False
    return True


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
    match = RE_CHAIN_NODE.search(token or "")
    if not match:
        return None
    raw_name = (match.group("name") or "").strip()
    raw_meta = (match.group("meta") or "").strip()
    if not raw_name or not raw_meta:
        return None
    obj_type = raw_meta.split("|", 1)[0].strip().upper()
    name = normalize_identifier(raw_name)
    if not name or not obj_type:
        return None
    return name, obj_type


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
    try:
        candidates.extend(report_dir.glob(f"{prefix}_*.txt"))
        candidates.extend(report_dir.glob(f"run_*/{prefix}_*.txt"))
    except OSError:
        return None
    if not candidates:
        return None
    ts_re = re.compile(rf"{re.escape(prefix)}_(\d{{8}}_\d{{6}})")

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
        log.warning("[AUTO-GRANT] 未找到 dependency_chains/VIEWs_chain，自动补权限跳过。")
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
        roles_cache={},
        tab_privs_cache={},
        tab_privs_grantable_cache={},
        sys_privs_cache={},
        planned_statements=set(),
        planned_object_privs=set(),
        planned_object_privs_with_option=set(),
        planned_sys_privs=set(),
        applied_grants=set(),
        stats=stats
    )
    log.info(
        "[AUTO-GRANT] 启用: types=%s fallback=%s deps=%d",
        ",".join(sorted(fixup_settings.types)),
        "true" if fixup_settings.fallback else "false",
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
    obj_type_u = normalize_object_type(obj_type)
    if obj_type_u not in ctx.settings.types:
        ctx.stats.skipped += 1
        return 0, False
    plan_lines, sql_lines, blocked = build_auto_grant_plan_for_object(ctx, obj_full, obj_type_u)
    if blocked:
        ctx.stats.blocked += 1
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
    planned_objects: Set[Tuple[str, str]]
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
        try:
            ddl_text = ddl_path.read_text(encoding="utf-8").rstrip()
        except OSError as exc:
            plan_lines.append(f"BLOCK: 读取 DDL 失败 {ddl_path} ({exc})")
            blocked = True
            continue
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
    in_block_comment = False
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
        if not in_single and not in_double and not in_block_comment and stripped == "/":
            buffer.append(line)
            flush_buffer()
            slash_block = False
            continue

        idx = 0
        while idx < len(line):
            ch = line[idx]
            nxt = line[idx + 1] if idx + 1 < len(line) else ""

            if in_block_comment:
                buffer.append(ch)
                if ch == "*" and nxt == "/":
                    buffer.append(nxt)
                    idx += 2
                    in_block_comment = False
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
                    in_block_comment = True
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
    return subprocess.run(
        obclient_cmd,
        input=sql_text,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


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
        match = CURRENT_SCHEMA_PATTERN.search(statement)
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

        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            message = stderr or "执行失败"
            failures.append(StatementFailure(idx, message, statement_to_run))

    return ExecutionSummary(statements=len(statements), failures=failures)


def run_query_lines(
    obclient_cmd: List[str],
    sql_text: str,
    timeout: Optional[int]
) -> Tuple[bool, List[str], str]:
    try:
        result = run_sql(obclient_cmd, sql_text, timeout)
    except subprocess.TimeoutExpired:
        return False, [], "TimeoutExpired"
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        return False, [], stderr or "执行失败"
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
        for sql_file in sorted(subdir.glob("*.sql")):
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
        for grant_file in sorted(grants_path.glob("*.sql")):
            try:
                content = grant_file.read_text(encoding="utf-8")
            except Exception:
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
    suffix = " WITH GRANT OPTION" if with_grant_option else ""
    return f"GRANT {priv_u} ON {object_u} TO {grantee_u}{suffix};"


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
            log.warning("[GRANT] 执行超时: %s", entry.statement.splitlines()[0][:160])
            continue
        if result.returncode == 0:
            applied_grants.add(key)
            applied += 1
            continue
        failed += 1
        stderr = (result.stderr or "").strip()
        preview = stderr.splitlines()[0] if stderr else "执行失败"
        log.warning("[GRANT] 执行失败: %s", preview[:160])

    return applied, failed


def resolve_timeout_value(raw_timeout: Optional[int]) -> Optional[int]:
    if raw_timeout is None:
        return None
    try:
        return int(raw_timeout)
    except Exception:
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
    error_limit: int
) -> Tuple[ScriptResult, ExecutionSummary, int, int, bool]:
    relative_path = sql_path.relative_to(repo_root)
    try:
        sql_text = sql_path.read_text(encoding="utf-8")
    except Exception as exc:
        msg = f"读取文件失败: {exc}"
        log.error("%s %s -> ERROR (%s)", label_prefix, relative_path, msg)
        failure = StatementFailure(0, msg, "")
        return ScriptResult(relative_path, "ERROR", msg, layer), ExecutionSummary(0, [failure]), 0, 0, False

    statements = split_sql_statements(sql_text)
    kept_statements: List[str] = []
    failures: List[StatementFailure] = []
    executed_count = 0
    removed_count = 0
    truncated = False

    for statement in statements:
        if is_comment_only_statement(statement):
            continue
        executed_count += 1
        is_grant = statement.lstrip().upper().startswith("GRANT ")

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

        if result.returncode == 0:
            if is_grant:
                removed_count += 1
            else:
                kept_statements.append(statement)
            continue

        stderr = (result.stderr or "").strip()
        message = stderr or "执行失败"
        failures.append(StatementFailure(executed_count, message, statement))
        kept_statements.append(statement)
        if not truncated:
            truncated = not record_error_entry(
                error_entries, error_limit, relative_path, executed_count, statement, message
            )

    summary = ExecutionSummary(executed_count, failures)

    if executed_count == 0:
        log.warning("%s %s -> SKIP (文件为空)", label_prefix, relative_path)
        return ScriptResult(relative_path, "SKIPPED", "文件为空", layer), summary, 0, 0, truncated

    if summary.success:
        move_note = ""
        try:
            target_dir = done_dir / sql_path.parent.name
            target_dir.mkdir(parents=True, exist_ok=True)
            target_path = target_dir / sql_path.name
            shutil.move(str(sql_path), target_path)
            move_note = f"(已移至 done/{sql_path.parent.name}/)"
        except Exception as exc:
            move_note = f"(移动失败: {exc})"
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
    label_prefix: str
) -> Tuple[ScriptResult, ExecutionSummary]:
    relative_path = sql_path.relative_to(repo_root)
    try:
        sql_text = sql_path.read_text(encoding="utf-8")
    except Exception as exc:
        msg = f"读取文件失败: {exc}"
        log.error("%s %s -> ERROR (%s)", label_prefix, relative_path, msg)
        return ScriptResult(relative_path, "ERROR", msg, layer), ExecutionSummary(0, [StatementFailure(0, msg, "")])

    if not sql_text.strip():
        log.warning("%s %s -> SKIP (文件为空)", label_prefix, relative_path)
        return ScriptResult(relative_path, "SKIPPED", "文件为空", layer), ExecutionSummary(0, [])

    summary = execute_sql_statements(obclient_cmd, sql_text, timeout=timeout)
    if summary.statements == 0:
        log.warning("%s %s -> SKIP (文件无有效语句)", label_prefix, relative_path)
        return ScriptResult(relative_path, "SKIPPED", "文件无有效语句", layer), summary

    if summary.success:
        move_note = ""
        try:
            target_dir = done_dir / sql_path.parent.name
            target_dir.mkdir(parents=True, exist_ok=True)
            target_path = target_dir / sql_path.name
            shutil.move(str(sql_path), target_path)
            move_note = f"(已移至 done/{sql_path.parent.name}/)"
        except Exception as exc:
            move_note = f"(移动失败: {exc})"
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
        preview = failure.error.splitlines()[0] if failure.error else "执行失败"
        log.warning("  [%d] %s", failure.index, preview[:200])
    return ScriptResult(relative_path, "FAILED", first_error, layer), summary


def query_invalid_objects(obclient_cmd: List[str], timeout: Optional[int]) -> List[Tuple[str, str, str]]:
    """
    Query INVALID objects from OceanBase.
    
    Returns:
        List of (owner, object_name, object_type) tuples
    """
    sql = """
    SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
    FROM DBA_OBJECTS
    WHERE STATUS = 'INVALID'
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
    except Exception:
        return []


def build_compile_statement(owner: str, obj_name: str, obj_type: str) -> Optional[str]:
    if not owner or not obj_name or not obj_type:
        return None
    obj_type_u = obj_type.strip().upper()
    owner_u = owner.strip()
    name_u = obj_name.strip()
    if obj_type_u in {"VIEW", "MATERIALIZED VIEW", "TYPE BODY"}:
        return None
    if obj_type_u == "PACKAGE BODY":
        return f"ALTER PACKAGE {owner_u}.{name_u} COMPILE BODY;"
    if obj_type_u in {"PACKAGE", "TYPE", "PROCEDURE", "FUNCTION", "TRIGGER"}:
        return f"ALTER {obj_type_u} {owner_u}.{name_u} COMPILE;"
    return None


def recompile_invalid_objects(
    obclient_cmd: List[str],
    timeout: Optional[int],
    max_retries: int = MAX_RECOMPILE_RETRIES
) -> Tuple[int, int]:
    """
    Recompile INVALID objects multiple times until all are VALID or max retries reached.
    
    Returns:
        (total_recompiled, remaining_invalid)
    """
    total_recompiled = 0
    
    for retry in range(max_retries):
        invalid_objects = query_invalid_objects(obclient_cmd, timeout)
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
                if result.returncode == 0:
                    recompiled_this_round += 1
                    log.info("  OK %s.%s (%s)", owner, obj_name, obj_type)
                else:
                    log.warning(
                        "  FAIL %s.%s (%s): %s",
                        owner,
                        obj_name,
                        obj_type,
                        result.stderr.strip()[:100]
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
    final_invalid = query_invalid_objects(obclient_cmd, timeout)
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
        
        保留原有功能：
          --only-dirs    : 按子目录过滤
          --only-types   : 按对象类型过滤
          --glob         : 按文件名模式过滤

        项目信息：
          主页: {repo_url}
          反馈: {issues_url}
        """
    ).format(repo_url=REPO_URL, issues_url=REPO_ISSUES_URL)
    
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
    default_excludes = {"tables_unsupported", "unsupported"}
    exclude_set = set(exclude_dirs) | default_excludes
    if only_dirs:
        exclude_set -= set(only_dirs)
    exclude_dirs = sorted(exclude_set)
    
    # Load configuration
    try:
        ob_cfg, fixup_dir, repo_root, log_level, report_dir, fixup_settings = load_ob_config(
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
    
    # Check if iterative mode requested via config or args
    iterative_mode = getattr(args, 'iterative', False)
    max_rounds = getattr(args, 'max_rounds', 10)
    min_progress = getattr(args, 'min_progress', 1)
    
    if getattr(args, "view_chain_autofix", False):
        run_view_chain_autofix(
            args,
            ob_cfg,
            fixup_dir,
            repo_root,
            report_dir,
            only_dirs,
            exclude_dirs,
            fixup_settings
        )
    elif iterative_mode:
        run_iterative_fixup(
            args, ob_cfg, fixup_dir, repo_root, report_dir,
            only_dirs, exclude_dirs,
            fixup_settings,
            max_rounds, min_progress
        )
    else:
        run_single_fixup(
            args, ob_cfg, fixup_dir, repo_root, report_dir,
            only_dirs, exclude_dirs,
            fixup_settings
        )


def run_single_fixup(
    args,
    ob_cfg: Dict[str, str],
    fixup_dir: Path,
    repo_root: Path,
    report_dir: Path,
    only_dirs: List[str],
    exclude_dirs: List[str],
    fixup_settings: FixupAutoGrantSettings
) -> None:
    """Original single-round fixup execution (backward compatible)."""
    
    log_section("修补脚本执行器")
    log.info("配置文件: %s", Path(args.config).resolve())
    log.info("日志级别: %s", logging.getLevelName(logging.getLogger().level))
    log.info("项目主页: %s (问题反馈: %s)", REPO_URL, REPO_ISSUES_URL)
    
    done_dir = fixup_dir / DONE_DIR_NAME
    done_dir.mkdir(exist_ok=True)
    
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
    
    obclient_cmd = build_obclient_command(ob_cfg)
    ob_timeout = resolve_timeout_value(ob_cfg.get("timeout"))
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
        
        try:
            sql_text = sql_path.read_text(encoding="utf-8")
        except Exception as exc:
            msg = f"读取文件失败: {exc}"
            results.append(ScriptResult(relative_path, "ERROR", msg, layer))
            log.error("%s %s -> ERROR (%s)", label, relative_path, msg)
            continue
        
        if not sql_text.strip():
            results.append(ScriptResult(relative_path, "SKIPPED", "文件为空", layer))
            log.warning("%s %s -> SKIP (文件为空)", label, relative_path)
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
                DEFAULT_ERROR_REPORT_LIMIT
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
            summary = execute_sql_statements(obclient_cmd, sql_text, timeout=ob_timeout)
            if summary.statements == 0:
                results.append(ScriptResult(relative_path, "SKIPPED", "文件为空", layer))
                log.warning("%s %s -> SKIP (文件为空)", label, relative_path)
                continue

            if summary.success:
                move_note = ""
                try:
                    target_dir = done_dir / sql_path.parent.name
                    target_dir.mkdir(parents=True, exist_ok=True)
                    target_path = target_dir / sql_path.name
                    shutil.move(str(sql_path), target_path)
                    move_note = f"(已移至 done/{sql_path.parent.name}/)"
                except Exception as exc:
                    move_note = f"(移动失败: {exc})"

                results.append(ScriptResult(relative_path, "SUCCESS", move_note.strip(), layer))
                log.info("%s %s -> OK %s", label, relative_path, move_note)
            else:
                first_error = summary.failures[0].error if summary.failures else "执行失败"
                error_type = classify_sql_error(first_error)
                retried = False
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
                        f"{label} (retry)"
                    )
                    if applied > 0:
                        retry_summary = execute_sql_statements(
                            obclient_cmd,
                            sql_text,
                            timeout=ob_timeout
                        )
                        if retry_summary.success:
                            retried = True
                            move_note = ""
                            try:
                                target_dir = done_dir / sql_path.parent.name
                                target_dir.mkdir(parents=True, exist_ok=True)
                                target_path = target_dir / sql_path.name
                                shutil.move(str(sql_path), target_path)
                                move_note = f"(已移至 done/{sql_path.parent.name}/)"
                            except Exception as exc:
                                move_note = f"(移动失败: {exc})"
                            results.append(ScriptResult(relative_path, "SUCCESS", move_note.strip(), layer))
                            log.info("%s %s -> OK %s", label, relative_path, move_note)
                        else:
                            summary = retry_summary
                if not retried:
                    results.append(ScriptResult(relative_path, "FAILED", first_error, layer))
                    log.error(
                        "%s %s -> FAIL (%d/%d statements)",
                        label,
                        relative_path,
                        len(summary.failures),
                        summary.statements
                    )
                    for failure in summary.failures[:3]:
                        preview = failure.error.splitlines()[0] if failure.error else "执行失败"
                        log.error("  [%d] %s", failure.index, preview[:200])
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
            obclient_cmd, ob_timeout, args.max_retries
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
                msg = item.message.splitlines()[0][:100] if item.message else ""
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
    fixup_settings: FixupAutoGrantSettings
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

    exists_cache: Dict[Tuple[str, str], bool] = {}
    roles_cache: Dict[str, Set[str]] = {}
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]] = {}
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]] = {}
    sys_privs_cache: Dict[str, Set[str]] = {}
    planned_statements: Set[str] = set()
    planned_object_privs: Set[Tuple[str, str, str]] = set()
    planned_object_privs_with_option: Set[Tuple[str, str, str]] = set()
    planned_sys_privs: Set[Tuple[str, str]] = set()
    planned_objects: Set[Tuple[str, str]] = set()

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
                planned_objects
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
        for chain in chains:
            chain_summary.append("# - " + " -> ".join(f"{n[0]}({n[1]})" for n in chain))

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
            log.info("  %s [%s]", view_key, status)
            for reason in reasons[:5]:
                log.info("    - %s", reason)
            if len(reasons) > 5:
                log.info("    - ... 还有 %d 条", len(reasons) - 5)

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
    max_rounds: int = 10,
    min_progress: int = 1
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
    
    obclient_cmd = build_obclient_command(ob_cfg)
    ob_timeout = resolve_timeout_value(ob_cfg.get("timeout"))
    auto_grant_ctx = init_auto_grant_context(
        fixup_settings,
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
    
    all_round_results = []
    
    while round_num < max_rounds:
        round_num += 1
        
        log_section(f"第 {round_num}/{max_rounds} 轮")
        
        # Collect pending SQL files (excluding done/)
        files_with_layer = collect_sql_files_by_layer(
            fixup_dir,
            smart_order=args.smart_order,
            include_dirs=set(only_dirs) if only_dirs else None,
            exclude_dirs=set(exclude_dirs),
            glob_patterns=args.glob_patterns or None,
        )
        
        if not files_with_layer:
            log.info("✓ 所有脚本已成功执行！")
            break

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
                    DEFAULT_ERROR_REPORT_LIMIT
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
                label
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
                                "[DEPS]"
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
                    f"{label} (retry)"
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
        cumulative_failed = round_failed  # Only count current failures
        
        log_subsection(f"第 {round_num} 轮结果")
        log.info("本轮成功: %d", round_success)
        log.info("本轮失败: %d", round_failed)
        log.info("本轮跳过: %d", round_skipped)
        log.info("累计成功: %d", cumulative_success)
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
                obclient_cmd, ob_timeout, 2  # Fewer retries per round
            )
            if recomp > 0:
                log.info("重编译成功 %d 个对象", recomp)
    
    # Final recompilation
    total_recompiled = 0
    remaining_invalid = 0
    if args.recompile:
        log_section("最终重编译")
        total_recompiled, remaining_invalid = recompile_invalid_objects(
            obclient_cmd, ob_timeout, args.max_retries
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
    log.info("剩余失败: %d", cumulative_failed)
    
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

    log_section("执行结束")
    
    exit_code = 0 if cumulative_failed == 0 else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
