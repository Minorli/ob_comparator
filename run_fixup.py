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


def load_ob_config(config_path: Path) -> Tuple[Dict[str, str], Path, Path]:
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

    return ob_cfg, fixup_path, repo_root


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
        
        print(f"\n[重编译] 第 {retry + 1}/{max_retries} 轮，发现 {len(invalid_objects)} 个 INVALID 对象")
        
        recompiled_this_round = 0
        for owner, obj_name, obj_type in invalid_objects:
            compile_sql = f"ALTER {obj_type} {owner}.{obj_name} COMPILE;"
            try:
                result = run_sql(obclient_cmd, compile_sql, timeout)
                if result.returncode == 0:
                    recompiled_this_round += 1
                    print(f"  ✓ {owner}.{obj_name} ({obj_type})")
                else:
                    print(f"  ✗ {owner}.{obj_name} ({obj_type}): {result.stderr.strip()[:100]}")
            except Exception as e:
                print(f"  ✗ {owner}.{obj_name} ({obj_type}): {str(e)[:100]}")
        
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
        
        保留原有功能：
          --only-dirs    : 按子目录过滤
          --only-types   : 按对象类型过滤
          --glob         : 按文件名模式过滤
        """
    )
    
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
        print(f"[警告] 未识别的对象类型: {', '.join(unknown_types)}", file=sys.stderr)
    
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
        ob_cfg, fixup_dir, repo_root = load_ob_config(config_arg.resolve())
    except ConfigError as exc:
        print(f"[配置错误] {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"[致命错误] 无法读取配置: {exc}", file=sys.stderr)
        sys.exit(1)
    
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
        print(f"[提示] 目录 {fixup_dir} 中未找到任何 *.sql 文件。")
        return
    
    obclient_cmd = build_obclient_command(ob_cfg)
    ob_timeout = int(ob_cfg.get("timeout", DEFAULT_OBCLIENT_TIMEOUT))
    
    total_scripts = len(files_with_layer)
    width = len(str(total_scripts)) or 1
    results: List[ScriptResult] = []
    
    # Print header
    header = "=" * 70
    print(header)
    print("开始执行修补脚本")
    print(f"目录: {fixup_dir}")
    if args.smart_order:
        print("模式: 依赖感知排序 (SMART ORDER)")
    else:
        print("模式: 标准优先级排序")
    if args.recompile:
        print(f"重编译: 启用 (最多 {args.max_retries} 次重试)")
    if only_dirs:
        print(f"子目录过滤: {sorted(set(only_dirs))}")
    if exclude_dirs:
        print(f"跳过子目录: {sorted(set(exclude_dirs))}")
    if args.glob_patterns:
        print(f"文件过滤: {args.glob_patterns}")
    print(f"共发现 SQL 文件: {total_scripts}")
    print(header)
    
    # Execute scripts
    current_layer = -1
    for idx, (layer, sql_path) in enumerate(files_with_layer, start=1):
        if args.smart_order and layer != current_layer:
            current_layer = layer
            layer_name = "未知层" if layer == 999 else f"第 {layer} 层"
            print(f"\n{'='*70}")
            print(f"{layer_name}")
            print(f"{'='*70}")
        
        relative_path = sql_path.relative_to(repo_root)
        label = f"[{idx:0{width}}/{total_scripts}]"
        
        try:
            sql_text = sql_path.read_text(encoding="utf-8")
        except Exception as exc:
            msg = f"读取文件失败: {exc}"
            results.append(ScriptResult(relative_path, "ERROR", msg, layer))
            print(f"{label} {relative_path} -> 错误")
            print(f"    {msg}")
            continue
        
        if not sql_text.strip():
            results.append(ScriptResult(relative_path, "SKIPPED", "文件为空", layer))
            print(f"{label} {relative_path} -> 跳过 (文件为空)")
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
                print(f"{label} {relative_path} -> ✓ 成功 {move_note}")
            else:
                stderr = (result.stderr or "").strip()
                results.append(ScriptResult(relative_path, "FAILED", stderr, layer))
                print(f"{label} {relative_path} -> ✗ 失败")
                if stderr:
                    # Print first line of error
                    first_line = stderr.splitlines()[0] if stderr.splitlines() else stderr
                    print(f"    {first_line[:200]}")
        except subprocess.TimeoutExpired:
            msg = f"执行超时 (> {ob_timeout} 秒)"
            results.append(ScriptResult(relative_path, "FAILED", msg, layer))
            print(f"{label} {relative_path} -> ✗ 失败")
            print(f"    {msg}")
    
    # Recompilation phase
    total_recompiled = 0
    remaining_invalid = 0
    if args.recompile:
        print(f"\n{'='*70}")
        print("重编译阶段")
        print(f"{'='*70}")
        total_recompiled, remaining_invalid = recompile_invalid_objects(
            obclient_cmd, ob_timeout, args.max_retries
        )
    
    # Summary
    executed = sum(1 for r in results if r.status != "SKIPPED")
    success = sum(1 for r in results if r.status == "SUCCESS")
    failed = sum(1 for r in results if r.status in ("FAILED", "ERROR"))
    skipped = sum(1 for r in results if r.status == "SKIPPED")
    
    print(f"\n{'='*70}")
    print("执行结果汇总")
    print(f"{'='*70}")
    print(f"扫描脚本数 : {total_scripts}")
    print(f"实际执行数 : {executed}")
    print(f"成功       : {success}")
    print(f"失败       : {failed}")
    print(f"跳过       : {skipped}")
    
    if args.recompile:
        print(f"\n重编译统计:")
        print(f"  重编译成功 : {total_recompiled}")
        print(f"  仍为INVALID: {remaining_invalid}")
        if remaining_invalid > 0:
            print(f"  提示: 运行 'SELECT * FROM DBA_OBJECTS WHERE STATUS=\\'INVALID\\';' 查看详情")
    
    # Detailed table
    if results:
        print(f"\n{'='*70}")
        print("详细结果")
        print(f"{'='*70}")
        
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
            
            print(f"\n{status_label} ({len(items)}):")
            for item in items[:20]:  # Limit to first 20
                msg = item.message.splitlines()[0][:100] if item.message else ""
                print(f"  {item.path}")
                if msg:
                    print(f"    {msg}")
            
            if len(items) > 20:
                print(f"  ... 还有 {len(items) - 20} 个")
    
    print(f"{'='*70}\n")
    
    exit_code = 0 if failed == 0 else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
