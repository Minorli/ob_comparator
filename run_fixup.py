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
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

CONFIG_DEFAULT_PATH = "config.ini"
DEFAULT_FIXUP_DIR = "fixup_scripts"
DONE_DIR_NAME = "done"
DEFAULT_OBCLIENT_TIMEOUT = 60
MAX_RECOMPILE_RETRIES = 5
REPO_URL = "https://github.com/Minorli/ob_comparator"
REPO_ISSUES_URL = f"{REPO_URL}/issues"

LOG_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FILE_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
LOG_SECTION_WIDTH = 80


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
        if 'TABLE OR VIEW DOES NOT EXIST' in stderr_upper or 'DOES NOT EXIST' in stderr_upper:
            return FailureType.MISSING_OBJECT
    
    # Permission denied (needs grant scripts)
    if 'ORA-01031' in stderr_upper or 'INSUFFICIENT PRIVILEGES' in stderr_upper:
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
        log.info("   建议: 检查并执行 fixup_scripts/grants/ 下的授权脚本")
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

# Execution priority for dependency-aware ordering
DEPENDENCY_LAYERS = [
    ["sequence"],                                    # Layer 0: No dependencies
    ["table"],                                       # Layer 1: Base tables
    ["table_alter"],                                 # Layer 2: Table modifications
    ["grants"],                                      # Layer 3: Grants BEFORE dependent objects
    ["view", "synonym"],                             # Layer 4: Simple dependent objects
    ["materialized_view"],                           # Layer 5: MVIEWs
    ["procedure", "function"],                       # Layer 6: Standalone routines
    ["package", "type"],                             # Layer 7: Package specs and types
    ["package_body", "type_body"],                   # Layer 8: Package/type bodies
    ["constraint", "index"],                         # Layer 9: Constraints and indexes
    ["trigger"],                                     # Layer 10: Triggers (last)
    ["job", "schedule"],                             # Layer 11: Jobs
]


class ConfigError(Exception):
    """Custom exception for configuration issues."""


@dataclass
class ScriptResult:
    path: Path
    status: str  # SUCCESS, FAILED, ERROR, SKIPPED
    message: str = ""
    layer: int = -1


def load_ob_config(config_path: Path) -> Tuple[Dict[str, str], Path, Path, str]:
    """Load OceanBase connection info and fixup directory from config.ini."""
    parser = configparser.ConfigParser()
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
        obclient_timeout = parser.getint("SETTINGS", "obclient_timeout", fallback=DEFAULT_OBCLIENT_TIMEOUT)
        obclient_timeout = obclient_timeout if obclient_timeout > 0 else DEFAULT_OBCLIENT_TIMEOUT
    except Exception:
        obclient_timeout = DEFAULT_OBCLIENT_TIMEOUT
    ob_cfg["timeout"] = obclient_timeout

    repo_root = config_path.parent.resolve()
    fixup_dir = parser.get("SETTINGS", "fixup_dir", fallback=DEFAULT_FIXUP_DIR).strip()
    fixup_path = (repo_root / fixup_dir).resolve()

    if not fixup_path.exists():
        raise ConfigError(f"修补脚本目录不存在: {fixup_path}")

    log_level = parser.get("SETTINGS", "log_level", fallback="INFO").strip().upper() or "INFO"
    return ob_cfg, fixup_path, repo_root, log_level


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
    
    files_with_layer: List[Tuple[int, Path]] = []
    
    if smart_order:
        # Use dependency layers
        for layer_idx, layer_dirs in enumerate(DEPENDENCY_LAYERS):
            for dir_name in layer_dirs:
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
        for dir_name in sorted(subdirs.keys()):
            if dir_name in all_layer_dirs:
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


def run_sql(obclient_cmd: List[str], sql_text: str, timeout: int) -> subprocess.CompletedProcess:
    """Execute SQL text by piping it to obclient."""
    return subprocess.run(
        obclient_cmd,
        input=sql_text,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


def query_invalid_objects(obclient_cmd: List[str], timeout: int) -> List[Tuple[str, str, str]]:
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


def recompile_invalid_objects(
    obclient_cmd: List[str],
    timeout: int,
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
            compile_sql = f"ALTER {obj_type} {owner}.{obj_name} COMPILE;"
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
    
    # Load configuration
    try:
        ob_cfg, fixup_dir, repo_root, log_level = load_ob_config(config_arg.resolve())
    except ConfigError as exc:
        log.error("配置错误: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.error("致命错误: 无法读取配置: %s", exc)
        sys.exit(1)

    level_name = log_level or "INFO"
    if not hasattr(logging, level_name):
        log.warning("未知 log_level: %s，使用 INFO", level_name)
        level_name = "INFO"
    level = getattr(logging, level_name)
    set_console_log_level(level)
    
    # Check if iterative mode requested via config or args
    iterative_mode = getattr(args, 'iterative', False)
    max_rounds = getattr(args, 'max_rounds', 10)
    min_progress = getattr(args, 'min_progress', 1)
    
    if iterative_mode:
        run_iterative_fixup(
            args, ob_cfg, fixup_dir, repo_root, 
            only_dirs, exclude_dirs,
            max_rounds, min_progress
        )
    else:
        run_single_fixup(
            args, ob_cfg, fixup_dir, repo_root,
            only_dirs, exclude_dirs
        )


def run_single_fixup(
    args,
    ob_cfg: Dict[str, str],
    fixup_dir: Path,
    repo_root: Path,
    only_dirs: List[str],
    exclude_dirs: List[str]
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
    ob_timeout = int(ob_cfg.get("timeout", DEFAULT_OBCLIENT_TIMEOUT))
    
    total_scripts = len(files_with_layer)
    width = len(str(total_scripts)) or 1
    results: List[ScriptResult] = []
    
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
    log.info("共发现 SQL 文件: %d", total_scripts)

    log_section("开始执行")
    
    # Execute scripts
    current_layer = -1
    for idx, (layer, sql_path) in enumerate(files_with_layer, start=1):
        if args.smart_order and layer != current_layer:
            current_layer = layer
            layer_name = "未知层" if layer == 999 else f"第 {layer} 层"
            log_subsection(f"执行层 {layer_name}")
        
        relative_path = sql_path.relative_to(repo_root)
        label = f"[{idx:0{width}}/{total_scripts}]"
        
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
        
        try:
            result = run_sql(obclient_cmd, sql_text, timeout=ob_timeout)
            if result.returncode == 0:
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
                stderr = (result.stderr or "").strip()
                results.append(ScriptResult(relative_path, "FAILED", stderr, layer))
                log.error("%s %s -> FAIL", label, relative_path)
                if stderr:
                    first_line = stderr.splitlines()[0] if stderr.splitlines() else stderr
                    log.error("  %s", first_line[:200])
        except subprocess.TimeoutExpired:
            msg = f"执行超时 (> {ob_timeout} 秒)"
            results.append(ScriptResult(relative_path, "FAILED", msg, layer))
            log.error("%s %s -> TIMEOUT", label, relative_path)
            log.error("  %s", msg)
    
    # Recompilation phase
    total_recompiled = 0
    remaining_invalid = 0
    if args.recompile:
        log_subsection("重编译阶段")
        total_recompiled, remaining_invalid = recompile_invalid_objects(
            obclient_cmd, ob_timeout, args.max_retries
        )
    
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

    log_section("执行结束")
    
    exit_code = 0 if failed == 0 else 1
    sys.exit(exit_code)


def run_iterative_fixup(
    args,
    ob_cfg: Dict[str, str],
    fixup_dir: Path,
    repo_root: Path,
    only_dirs: List[str],
    exclude_dirs: List[str],
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
    ob_timeout = int(ob_cfg.get("timeout", DEFAULT_OBCLIENT_TIMEOUT))
    
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
        
        total_scripts = len(files_with_layer)
        log.info("待处理脚本: %d 个", total_scripts)
        
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
            label = f"[{idx:0{width}}/{total_scripts}]"
            
            try:
                sql_text = sql_path.read_text(encoding="utf-8")
            except Exception as exc:
                msg = f"读取文件失败: {exc}"
                round_results.append(ScriptResult(relative_path, "ERROR", msg, layer))
                log.error("%s %s -> ERROR (%s)", label, relative_path, msg)
                continue
            
            if not sql_text.strip():
                round_results.append(ScriptResult(relative_path, "SKIPPED", "文件为空", layer))
                log.warning("%s %s -> SKIP (文件为空)", label, relative_path)
                continue
            
            try:
                result = run_sql(obclient_cmd, sql_text, timeout=ob_timeout)
                if result.returncode == 0:
                    move_note = ""
                    try:
                        target_dir = done_dir / sql_path.parent.name
                        target_dir.mkdir(parents=True, exist_ok=True)
                        target_path = target_dir / sql_path.name
                        shutil.move(str(sql_path), target_path)
                        move_note = f"(已移至 done/{sql_path.parent.name}/)"
                    except Exception as exc:
                        move_note = f"(移动失败: {exc})"

                    round_results.append(ScriptResult(relative_path, "SUCCESS", move_note.strip(), layer))
                    log.info("%s %s -> OK %s", label, relative_path, move_note)
                else:
                    stderr = (result.stderr or "").strip()
                    round_results.append(ScriptResult(relative_path, "FAILED", stderr, layer))
                    error_type = classify_sql_error(stderr)
                    
                    # Only log details for non-retryable errors
                    if error_type in [FailureType.SYNTAX_ERROR, FailureType.PERMISSION_DENIED, FailureType.UNKNOWN]:
                        log.error("%s %s -> FAIL", label, relative_path)
                        if stderr:
                            first_line = stderr.splitlines()[0] if stderr.splitlines() else stderr
                            log.error("  %s", first_line[:200])
                    else:
                        # Retryable errors - just warning
                        log.warning("%s %s -> FAIL (将在下轮重试)", label, relative_path)
                        
            except subprocess.TimeoutExpired:
                msg = f"执行超时 (> {ob_timeout} 秒)"
                round_results.append(ScriptResult(relative_path, "FAILED", msg, layer))
                log.error("%s %s -> TIMEOUT", label, relative_path)
                log.error("  %s", msg)
        
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
    
    log_section("执行结束")
    
    exit_code = 0 if cumulative_failed == 0 else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
