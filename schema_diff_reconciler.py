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

数据库对象对比工具 (V0.9.7 - Dump-Once, Compare-Locally + 依赖 + ALTER 修补 + 注释校验)
---------------------------------------------------------------------------
功能概要：
1. 对比 Oracle (源) 与 OceanBase (目标) 的：
   - TABLE, VIEW, MATERIALIZED VIEW
   - PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, SYNONYM
   - JOB, SCHEDULE, TYPE, TYPE BODY
   - INDEX, CONSTRAINT (PK/UK/FK)
   - SEQUENCE, TRIGGER

2. 对比规则：
   - TABLE：校验列名集合（忽略 OMS_* 列），并检查 VARCHAR/VARCHAR2 长度是否落在 [ceil(src*1.5), ceil(src*2.5)] 区间。
   - TABLE/列注释：基于 DBA_TAB_COMMENTS / DBA_COL_COMMENTS 对比 Remap 后的表/列注释（可通过 check_comments 开关关闭）。
   - VIEW/MVIEW/PLSQL/SYNONYM/JOB/SCHEDULE/TYPE：对比是否存在。
   - INDEX / CONSTRAINT：校验存在性与列组合（含唯一性/约束类型）。
   - SEQUENCE / TRIGGER：校验存在性；依赖：映射后生成期望依赖并对比目标端。

3. 性能架构 (V0.9.7 核心)：
   - OceanBase 侧采用“一次转储，本地对比”：
       使用少量 obclient 调用，分别 dump：
         DBA_OBJECTS
         DBA_TAB_COLUMNS
         DBA_INDEXES / DBA_IND_COLUMNS
         DBA_CONSTRAINTS / DBA_CONS_COLUMNS
         DBA_TRIGGERS
         DBA_SEQUENCES
       后续所有对比均在 Python 内存数据结构中完成。
   - 避免 V12 中在循环中大量调用 obclient 的性能黑洞。

4. 目标端订正 SQL 生成：
   - 缺失对象：
       TABLE / VIEW / PROCEDURE / FUNCTION / PACKAGE / PACKAGE BODY / SYNONYM /
       INDEX / CONSTRAINT / SEQUENCE / TRIGGER
       → 生成对应的 CREATE 语句脚本。
   - TABLE 列不匹配：
       → 生成 ALTER TABLE ADD/MODIFY（长度不足）脚本；
       → 对“多余列”生成注释掉的 DROP COLUMN 建议语句。
   - 依赖缺失 → 生成 ALTER ... COMPILE；跨 schema 调用/源端权限 → 生成 GRANT 脚本（受 generate_grants 控制）。
   - 所有脚本写入 fixup_scripts 目录下相应子目录，需人工审核后在 OceanBase 执行。

5. 健壮性：
   - 所有 obclient 调用增加 timeout（从 config.ini 的 [SETTINGS] -> obclient_timeout 读取，默认 60 秒）。
   - Instant Client / dbcat / JAVA_HOME / Remap 前置校验，发现致命问题立即终止。
"""

import argparse
import configparser
import textwrap
import subprocess
import sys
import logging
import math
import re
from contextlib import contextmanager

__version__ = "0.9.7"
__author__ = "Minor Li"
REPO_URL = "https://github.com/Minorli/ob_comparator"
REPO_ISSUES_URL = f"{REPO_URL}/issues"
import os
import threading
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid
import shutil
import time
import tempfile
from collections import defaultdict, OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Dict, Set, List, Tuple, Optional, NamedTuple, Callable
import textwrap

# 尝试导入 oracledb，如果失败则提示安装
try:
    import oracledb
except ImportError:
    print("错误: 未找到 'oracledb' 库。", file=sys.stderr)
    print("请先安装: pip install oracledb", file=sys.stderr)
    sys.exit(1)

# Rich 渲染写入文件时需要的简化字符映射，便于 vi/less 查看
BOX_ASCII_TRANS = str.maketrans({
    "┏": "+", "┓": "+", "┗": "+", "┛": "+", "┣": "+", "┫": "+", "┳": "+", "┻": "+", "╋": "+",
    "━": "-", "┃": "|", "─": "-", "│": "|",
    "┌": "+", "┐": "+", "└": "+", "┘": "+", "├": "+", "┤": "+", "┴": "+", "┬": "+", "┼": "+",
    "═": "-", "║": "|", "╔": "+", "╗": "+", "╚": "+", "╝": "+", "╠": "+", "╣": "+", "╦": "+", "╩": "+", "╬": "+",
})

# 简易 ANSI 转义去除（不依赖 rich 的 strip_ansi，兼容低版本 wheel）
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[mGKF]")

def strip_ansi_text(text: str) -> str:
    return ANSI_RE.sub("", text)

# --- 日志配置 ---
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


def set_console_log_level(root_logger: logging.Logger, level: int) -> None:
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

RUN_PHASE_ORDER: Tuple[str, ...] = (
    "加载配置与初始化",
    "对象映射准备",
    "OceanBase 元数据转储",
    "Oracle 元数据转储",
    "主对象校验",
    "扩展对象校验",
    "依赖/授权校验",
    "修补脚本生成",
    "报告输出",
)


class RunPhaseInfo(NamedTuple):
    name: str
    duration: Optional[float]
    status: str


class RunSummary(NamedTuple):
    start_time: datetime
    end_time: datetime
    total_seconds: float
    phases: List[RunPhaseInfo]
    actions_done: List[str]
    actions_skipped: List[str]
    findings: List[str]
    attention: List[str]
    next_steps: List[str]


class RunSummaryContext(NamedTuple):
    start_time: datetime
    start_perf: float
    phase_durations: Dict[str, float]
    phase_skip_reasons: Dict[str, str]
    enabled_primary_types: Set[str]
    enabled_extra_types: Set[str]
    print_only_types: Set[str]
    total_checked: int
    enable_dependencies_check: bool
    enable_comment_check: bool
    enable_grant_generation: bool
    enable_schema_mapping_infer: bool
    fixup_enabled: bool
    fixup_dir: str
    dependency_chain_file: Optional[Path]
    view_chain_file: Optional[Path]
    trigger_list_summary: Optional[Dict[str, object]]
    report_start_perf: float


@contextmanager
def phase_timer(phase: str, durations: Dict[str, float]):
    start = time.perf_counter()
    try:
        yield
    finally:
        durations[phase] = durations.get(phase, 0.0) + (time.perf_counter() - start)


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m{sec:.0f}s"
    hours, rem = divmod(minutes, 60)
    return f"{int(hours)}h{int(rem)}m{sec:.0f}s"


def setup_run_logging(settings: Dict, timestamp: str) -> Optional[Path]:
    """
    为每次运行创建日志文件：
      - 日志目录默认 logs，可在 config.ini 的 [SETTINGS]->log_dir 覆盖
      - 控制台默认 INFO（可用 log_level 覆盖）
      - 文件记录 DEBUG 及以上，包含推导细节
    """
    try:
        log_dir_setting = (settings.get("log_dir") or "logs").strip() or "logs"
        log_dir = Path(log_dir_setting)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"run_{timestamp}.log"

        root_logger = logging.getLogger()
        # 允许 DEBUG 记录进入文件，但控制台按 log_level 过滤
        root_logger.setLevel(logging.DEBUG)
        existing_files = [
            handler for handler in root_logger.handlers
            if isinstance(handler, logging.FileHandler)
            and getattr(handler, "baseFilename", "") == str(log_file)
        ]
        if not existing_files:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(logging.Formatter(LOG_FILE_FORMAT, datefmt=LOG_TIME_FORMAT))
            root_logger.addHandler(file_handler)

        level_name = (settings.get("log_level") or "INFO").strip().upper()
        console_level = getattr(logging, level_name, logging.INFO)
        set_console_log_level(root_logger, console_level)

        log.info("本次运行日志将输出到: %s", log_file.resolve())
        log.info("日志级别: console=%s, file=DEBUG", logging.getLevelName(console_level))
        return log_file
    except Exception as exc:
        log.warning("初始化日志文件失败，将仅输出到控制台: %s", exc)
        return None

# --- 类型别名 ---
OraConfig = Dict[str, str]
ObConfig = Dict[str, str]
RemapRules = Dict[str, str]
SourceObjectMap = Dict[str, Set[str]]  # {'OWNER.OBJ': {'TYPE1', 'TYPE2'}}
FullObjectMapping = Dict[str, Dict[str, str]]  # {'OWNER.OBJ': {'TYPE': 'TGT_OWNER.OBJ'}}
MasterCheckList = List[Tuple[str, str, str]]  # [(src_name, tgt_name, type)]
ReportResults = Dict[str, List]
PackageCompareResults = Dict[str, object]
# object_counts_summary keys: oracle/oceanbase/missing/extra -> {OBJECT_TYPE: count}
ObjectCountSummary = Dict[str, Dict[str, int]]
# 源端依赖集合的简化元组：(dep_owner, dep_name, dep_type, ref_owner, ref_name, ref_type)
SourceDependencyTuple = Tuple[str, str, str, str, str, str]
SourceDependencySet = Set[SourceDependencyTuple]
# 依赖图节点与图结构，用于递归推导目标 schema
DependencyNode = Tuple[str, str]  # (OWNER.OBJECT, OBJECT_TYPE)
DependencyGraph = Dict[DependencyNode, Set[DependencyNode]]
# 递归推导时每个节点的“最终引用表”缓存
TransitiveTableCache = Dict[DependencyNode, Set[str]]
RemapConflictMap = Dict[Tuple[str, str], str]  # {(SRC_FULL, TYPE): reason}
BlacklistEntry = Tuple[str, str]  # (BLACK_TYPE, DATA_TYPE)
BlacklistTableMap = Dict[Tuple[str, str], Set[BlacklistEntry]]  # (OWNER, TABLE) -> {(BLACK_TYPE, DATA_TYPE)}
class BlacklistReportRow(NamedTuple):
    schema: str
    table: str
    black_type: str
    data_type: str
    reason: str
    status: str
    detail: str


class DdlCleanReportRow(NamedTuple):
    obj_type: str
    obj_full: str
    replaced: int
    samples: List[Tuple[str, str]]

# --- 全局 obclient timeout（秒），由配置初始化 ---
OBC_TIMEOUT: int = 60

# --- 模型定义 ---
class ObMetadata(NamedTuple):
    """
    一次性从 OceanBase dump 出来的元数据，用于本地对比。
    """
    objects_by_type: Dict[str, Set[str]]                 # OBJECT_TYPE -> {OWNER.OBJ}
    tab_columns: Dict[Tuple[str, str], Dict[str, Dict]]   # (OWNER, TABLE_NAME) -> {COLUMN_NAME: {type, length, etc.}}
    indexes: Dict[Tuple[str, str], Dict[str, Dict]]      # (OWNER, TABLE_NAME) -> {INDEX_NAME: {uniqueness, columns[list]}}
    constraints: Dict[Tuple[str, str], Dict[str, Dict]]  # (OWNER, TABLE_NAME) -> {CONS_NAME: {type, columns[list]}}
    triggers: Dict[Tuple[str, str], Dict[str, Dict]]     # (OWNER, TABLE_NAME) -> {TRG_NAME: {event, status}}
    sequences: Dict[str, Set[str]]                       # SEQUENCE_OWNER -> {SEQUENCE_NAME}
    roles: Set[str]                                      # DBA_ROLES -> {ROLE}
    table_comments: Dict[Tuple[str, str], Optional[str]] # (OWNER, TABLE_NAME) -> COMMENT
    column_comments: Dict[Tuple[str, str], Dict[str, Optional[str]]]  # (OWNER, TABLE_NAME) -> {COLUMN_NAME: COMMENT}
    comments_complete: bool                              # 元数据是否完整加载（两端失败则跳过注释校验）
    object_statuses: Dict[Tuple[str, str, str], str]     # (OWNER, OBJECT_NAME, OBJECT_TYPE) -> STATUS
    package_errors: Dict[Tuple[str, str, str], "PackageErrorInfo"]  # (OWNER, NAME, TYPE) -> error summary
    package_errors_complete: bool                        # 目标端错误信息是否完整


class OracleMetadata(NamedTuple):
    """
    源端 Oracle 的元数据缓存，避免在循环中重复查询。
    """
    table_columns: Dict[Tuple[str, str], Dict[str, Dict]]   # (OWNER, TABLE_NAME) -> 列定义
    indexes: Dict[Tuple[str, str], Dict[str, Dict]]        # (OWNER, TABLE_NAME) -> 索引
    constraints: Dict[Tuple[str, str], Dict[str, Dict]]    # (OWNER, TABLE_NAME) -> 约束
    triggers: Dict[Tuple[str, str], Dict[str, Dict]]       # (OWNER, TABLE_NAME) -> 触发器
    sequences: Dict[str, Set[str]]                         # OWNER -> {SEQUENCE_NAME}
    table_comments: Dict[Tuple[str, str], Optional[str]]   # (OWNER, TABLE_NAME) -> COMMENT
    column_comments: Dict[Tuple[str, str], Dict[str, Optional[str]]]  # (OWNER, TABLE_NAME) -> {COLUMN_NAME: COMMENT}
    comments_complete: bool                                # 注释元数据是否加载完成
    blacklist_tables: BlacklistTableMap                    # (OWNER, TABLE) -> {(BLACK_TYPE, DATA_TYPE)}
    object_privileges: List["OracleObjectPrivilege"]         # DBA_TAB_PRIVS (对象权限)
    sys_privileges: List["OracleSysPrivilege"]               # DBA_SYS_PRIVS (系统权限)
    role_privileges: List["OracleRolePrivilege"]             # DBA_ROLE_PRIVS (角色授权)
    role_metadata: Dict[str, "OracleRoleInfo"]               # DBA_ROLES 角色元数据
    system_privilege_map: Set[str]                           # SYSTEM_PRIVILEGE_MAP
    table_privilege_map: Set[str]                            # TABLE_PRIVILEGE_MAP
    object_statuses: Dict[Tuple[str, str, str], str]         # (OWNER, OBJECT_NAME, OBJECT_TYPE) -> STATUS
    package_errors: Dict[Tuple[str, str, str], "PackageErrorInfo"]  # (OWNER, NAME, TYPE) -> error summary
    package_errors_complete: bool                            # 源端错误信息是否完整


class DependencyRecord(NamedTuple):
    owner: str
    name: str
    object_type: str
    referenced_owner: str
    referenced_name: str
    referenced_type: str


class DependencyIssue(NamedTuple):
    dependent: str
    dependent_type: str
    referenced: str
    referenced_type: str
    reason: str


class SynonymMeta(NamedTuple):
    owner: str
    name: str
    table_owner: str
    table_name: str
    db_link: Optional[str]


DependencyReport = Dict[str, List[DependencyIssue]]


class OracleObjectPrivilege(NamedTuple):
    grantee: str
    owner: str
    object_name: str
    object_type: str
    privilege: str
    grantable: bool


class OracleSysPrivilege(NamedTuple):
    grantee: str
    privilege: str
    admin_option: bool


class OracleRolePrivilege(NamedTuple):
    grantee: str
    role: str
    admin_option: bool


class OracleRoleInfo(NamedTuple):
    role: str
    authentication_type: str
    password_required: bool
    oracle_maintained: Optional[bool]


class ObjectGrantEntry(NamedTuple):
    privilege: str
    object_full: str
    grantable: bool


class SystemGrantEntry(NamedTuple):
    privilege: str
    admin_option: bool


class RoleGrantEntry(NamedTuple):
    role: str
    admin_option: bool


class FilteredGrantEntry(NamedTuple):
    category: str  # SYSTEM | OBJECT
    grantee: str
    privilege: str
    object_full: str
    reason: str


class GrantPlan(NamedTuple):
    object_grants: Dict[str, Set[ObjectGrantEntry]]
    sys_privs: Dict[str, Set[SystemGrantEntry]]
    role_privs: Dict[str, Set[RoleGrantEntry]]
    role_ddls: List[str]
    filtered_grants: List[FilteredGrantEntry]


class ObGrantCatalog(NamedTuple):
    object_privs: Set[Tuple[str, str, str]]          # (GRANTEE, PRIV, OWNER.OBJ)
    object_privs_grantable: Set[Tuple[str, str, str]]  # grantable subset
    sys_privs: Set[Tuple[str, str]]                  # (GRANTEE, PRIV)
    sys_privs_admin: Set[Tuple[str, str]]            # admin_option subset
    role_privs: Set[Tuple[str, str]]                 # (GRANTEE, ROLE)
    role_privs_admin: Set[Tuple[str, str]]           # admin_option subset


class ColumnLengthIssue(NamedTuple):
    column: str
    src_length: int
    tgt_length: int
    limit_length: int  # 下限或上限（根据 issue 标识）
    issue: str         # 'short' | 'oversize'


class ColumnTypeIssue(NamedTuple):
    column: str
    src_type: str
    tgt_type: str
    expected_type: str


class PackageErrorInfo(NamedTuple):
    count: int
    first_error: str


class PackageCompareRow(NamedTuple):
    src_full: str
    obj_type: str
    src_status: str
    tgt_full: str
    tgt_status: str
    result: str
    error_count: int
    first_error: str


# --- 对象类型常量 ---
PRIMARY_OBJECT_TYPES: Tuple[str, ...] = (
    'TABLE',
    'VIEW',
    'MATERIALIZED VIEW',
    'PROCEDURE',
    'FUNCTION',
    'PACKAGE',
    'PACKAGE BODY',
    'SYNONYM',
    'JOB',
    'SCHEDULE',
    'TYPE',
    'TYPE BODY'
)

PRINT_ONLY_PRIMARY_TYPES: Tuple[str, ...] = (
    'MATERIALIZED VIEW'
)

PRINT_ONLY_PRIMARY_REASONS: Dict[str, str] = {
    'MATERIALIZED VIEW': "OB 暂不支持 MATERIALIZED VIEW，仅打印不校验"
}

PACKAGE_OBJECT_TYPES: Tuple[str, ...] = (
    'PACKAGE',
    'PACKAGE BODY'
)

# 这些类型不参与 schema 推导（除非显式 remap）
NO_INFER_SCHEMA_TYPES: Set[str] = {
    'VIEW',
    'MATERIALIZED VIEW',
    'TRIGGER',
    'PACKAGE',
    'PACKAGE BODY'
}

BLACKLIST_REASON_BY_TYPE: Dict[str, str] = {
    'SPE': "表字段存在不支持的类型，不支持创建，不需要生成DDL",
    'TEMP_TABLE': "临时表，不支持创建，不需要生成DDL",
    'TEMPORARY_TABLE': "源表是临时表，不需要生成DDL",
    'DIY': "表中字段存在自定义类型，不支持创建，不需要生成DDL",
    'LOB_OVERSIZE': "表中存在的LOB字段体积超过512 MiB，可以在目标端创建表，但是 OMS 不支持同步",
    'LONG': "LONG/LONG RAW 需人工转换为 CLOB/BLOB",
    'DBLINK': "源表可能是 IOT 表或者外部表，不需要生成DDL"
}

# 主对象中除 TABLE 外均做存在性验证
PRIMARY_EXISTENCE_ONLY_TYPES: Tuple[str, ...] = tuple(
    obj for obj in PRIMARY_OBJECT_TYPES if obj != 'TABLE' and obj not in PRINT_ONLY_PRIMARY_TYPES
)

# 额外纳入 remap/依赖但不做列级主检查的对象
DEPENDENCY_EXTRA_OBJECT_TYPES: Tuple[str, ...] = (
    'TRIGGER',
    'SEQUENCE',
    'INDEX'
)

ALL_TRACKED_OBJECT_TYPES: Tuple[str, ...] = tuple(
    sorted(set(PRIMARY_OBJECT_TYPES) | set(DEPENDENCY_EXTRA_OBJECT_TYPES))
)

EXTRA_OBJECT_CHECK_TYPES: Tuple[str, ...] = (
    'INDEX',
    'CONSTRAINT',
    'SEQUENCE',
    'TRIGGER'
)

# 注释比对时批量 IN 子句的大小，避免 ORA-01795
COMMENT_BATCH_SIZE = 200
# Oracle IN 列表最大表达式数量为 1000，预留余量
ORACLE_IN_BATCH_SIZE = 900
# 授权规模提示阈值（对象权限条数）
GRANT_WARN_THRESHOLD = 200000

# OceanBase 目标端自动生成且需在列对比中忽略的 OMS 列
IGNORED_OMS_COLUMNS: Tuple[str, ...] = (
    "OMS_OBJECT_NUMBER",
    "OMS_RELATIVE_FNO",
    "OMS_BLOCK_NUMBER",
    "OMS_ROW_NUMBER",
)


def is_ignored_oms_column(col_name: Optional[str], col_meta: Optional[Dict] = None) -> bool:
    """
    OceanBase 端迁移工具可能添加 OMS_* 列（VISIBLE/INVISIBLE 均可）。
    只要列名命中已知 OMS_* 集合就忽略，不再依赖 hidden 标记。
    """
    if not col_name:
        return False
    col_u = col_name.strip('"').upper()
    return col_u in IGNORED_OMS_COLUMNS


def is_ignored_source_column(col_name: Optional[str], col_meta: Optional[Dict] = None) -> bool:
    """
    源端列忽略规则：
    - 命中 OMS_* 忽略名单的列
    - Oracle hidden/virtual 等隐藏列（如支持 HIDDEN_COLUMN 字段）
    """
    if is_ignored_oms_column(col_name, col_meta):
        return True
    if col_meta and col_meta.get("hidden"):
        return True
    return False


VARCHAR_LEN_MIN_MULTIPLIER = 1.5  # 目标端 VARCHAR/2 长度需 >= ceil(src * 1.5)
VARCHAR_LEN_OVERSIZE_MULTIPLIER = 2.5  # 超过该倍数认为“过大”，需要提示


def normalize_black_type(value: Optional[str]) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def normalize_black_data_type(value: Optional[str]) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def blacklist_reason(black_type: Optional[str]) -> str:
    black_type_u = normalize_black_type(black_type)
    if not black_type_u:
        return "未知黑名单类型"
    return BLACKLIST_REASON_BY_TYPE.get(black_type_u, "未知黑名单类型")


def is_long_type(data_type: Optional[str]) -> bool:
    dt = (data_type or "").strip().upper()
    return dt in ("LONG", "LONG RAW")


def map_long_type_to_ob(data_type: Optional[str]) -> str:
    dt = (data_type or "").strip().upper()
    if dt == "LONG":
        return "CLOB"
    if dt == "LONG RAW":
        return "BLOB"
    return dt

def is_oms_index(name: str, columns: List[str]) -> bool:
    """识别迁移工具自动生成的 OMS_* 唯一索引，忽略之。"""
    name_u = (name or "").strip('"').upper()
    cols_u = [c.strip('"').upper() for c in (columns or []) if c]
    if not cols_u:
        return False
    # 检查名称是否以 _OMS_ROWID 结尾
    if not name_u.endswith("_OMS_ROWID"):
        return False
    
    # 检查列集合是否包含所有标准 OMS 列
    cols_set = set(cols_u)
    oms_cols_set = set(IGNORED_OMS_COLUMNS)
    
    # 如果包含所有4个OMS列，则认为是OMS索引（允许有额外列）
    return oms_cols_set.issubset(cols_set)


def is_ob_notnull_constraint(name: Optional[str]) -> bool:
    """
    识别 OceanBase Oracle 模式下自动生成的 *_OBNOTNULL_* CHECK 约束。
    这些约束用于保证 PK 列非空，Oracle 侧不会显式出现，报告统计应忽略以避免误判“多余约束”。
    """
    return "OBNOTNULL" in (name or "").upper()

OBJECT_COUNT_TYPES: Tuple[str, ...] = (
    'TABLE',
    'VIEW',
    'SYNONYM',
    'TRIGGER',
    'SEQUENCE',
    'PROCEDURE',
    'FUNCTION',
    'PACKAGE',
    'PACKAGE BODY',
    'TYPE',
    'TYPE BODY',
    'MATERIALIZED VIEW',
    'JOB',
    'SCHEDULE',
    'INDEX',
    'CONSTRAINT'
)


def parse_bool_flag(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ('1', 'true', 'yes', 'y', 'on')


def parse_type_list(
    raw_value: str,
    allowed: Set[str],
    label: str,
    default_all: bool = True
) -> Set[str]:
    if not raw_value or not raw_value.strip():
        return set(allowed) if default_all else set()
    parsed = {item.strip().upper() for item in raw_value.split(',') if item.strip()}
    unknown = parsed - allowed
    if unknown:
        log.warning("配置 %s 包含未知类型 %s，将被忽略。", label, sorted(unknown))
    return parsed & allowed


def parse_csv_set(raw_value: Optional[str]) -> Set[str]:
    """
    解析逗号分隔列表为大写集合。
    """
    if not raw_value or not str(raw_value).strip():
        return set()
    return {item.strip().upper() for item in str(raw_value).split(',') if item.strip()}


def chunk_list(items: List[str], size: int) -> List[List[str]]:
    """Split list into batches of given size."""
    return [items[i:i + size] for i in range(0, len(items), size)]


def build_bind_placeholders(count: int, offset: int = 0) -> str:
    if count <= 0:
        return ""
    return ",".join(f":{i+1+offset}" for i in range(count))


def iter_in_chunks(items: List[str], batch_size: int = ORACLE_IN_BATCH_SIZE) -> List[Tuple[str, List[str]]]:
    chunks: List[Tuple[str, List[str]]] = []
    for chunk in chunk_list(items, batch_size):
        if not chunk:
            continue
        chunks.append((build_bind_placeholders(len(chunk)), chunk))
    return chunks


# --- 扩展检查结果结构 ---
class IndexMismatch(NamedTuple):
    table: str
    missing_indexes: Set[str]
    extra_indexes: Set[str]
    detail_mismatch: List[str]


class ConstraintMismatch(NamedTuple):
    table: str
    missing_constraints: Set[str]
    extra_constraints: Set[str]
    detail_mismatch: List[str]


class SequenceMismatch(NamedTuple):
    src_schema: str
    tgt_schema: str
    missing_sequences: Set[str]
    extra_sequences: Set[str]
    note: Optional[str] = None
    missing_mappings: Optional[List[Tuple[str, str]]] = None


class TriggerMismatch(NamedTuple):
    table: str
    missing_triggers: Set[str]
    extra_triggers: Set[str]
    detail_mismatch: List[str]
    missing_mappings: Optional[List[Tuple[str, str]]] = None


class TriggerListReportRow(NamedTuple):
    entry: str
    status: str
    detail: str


class CommentMismatch(NamedTuple):
    table: str
    table_comment: Optional[Tuple[str, str]]  # (src, tgt) when different
    column_comment_diffs: List[Tuple[str, str, str]]  # (column, src_comment, tgt_comment)
    missing_columns: Set[str]
    extra_columns: Set[str]


ExtraCheckResults = Dict[str, List]


def normalize_column_sequence(columns: Optional[List[str]]) -> Tuple[str, ...]:
    if not columns:
        return tuple()
    seen: Set[str] = set()
    normalized: List[str] = []
    for col in columns:
        col_u = (col or '').upper()
        if not col_u:
            continue
        if col_u in seen:
            continue
        seen.add(col_u)
        normalized.append(col_u)
    return tuple(normalized)


def normalize_comment_text(text: Optional[str]) -> str:
    """
    统一注释文本：去除首尾空白、折叠多余空白，降低换行/制表差异的噪声。
    """
    if text is None:
        return ""
    # 去除控制字符，统一换行，并折叠空白
    sanitized = re.sub(r"[\x00-\x1f\x7f]", " ", str(text))
    collapsed = " ".join(sanitized.replace("\r\n", "\n").replace("\r", "\n").split())
    normalized = collapsed.strip()
    if normalized.upper() in {"NULL", "<NULL>", "NONE"}:
        return ""
    return normalized


def shorten_comment_preview(text: str, limit: int = 120) -> str:
    """
    将注释压缩为单行便于展示，控制长度。
    """
    if not text:
        return "<空>"
    single_line = text.replace("\n", "\\n")
    return single_line if len(single_line) <= limit else single_line[:limit - 3] + "..."

GRANT_PRIVILEGE_BY_TYPE: Dict[str, str] = {
    'TABLE': 'SELECT',
    'VIEW': 'SELECT',
    'MATERIALIZED VIEW': 'SELECT',
    'SYNONYM': 'SELECT',
    'SEQUENCE': 'SELECT',
    'TYPE': 'EXECUTE',
    'TYPE BODY': 'EXECUTE',
    'PROCEDURE': 'EXECUTE',
    'FUNCTION': 'EXECUTE',
    'PACKAGE': 'EXECUTE',
    'PACKAGE BODY': 'EXECUTE'
}

# OceanBase Oracle 模式下常用对象权限（缺省白名单，可在配置中覆盖）
DEFAULT_SUPPORTED_OBJECT_PRIVS: Set[str] = {
    'SELECT',
    'INSERT',
    'UPDATE',
    'DELETE',
    'REFERENCES',
    'EXECUTE'
}

ROLE_AUTH_WARN_TYPES: Set[str] = {
    'PASSWORD',
    'EXTERNAL',
    'GLOBAL'
}

# 系统权限对对象权限的兜底映射（用于过滤已满足的 GRANT 建议）
SYS_PRIV_IMPLICATIONS: Dict[str, Set[str]] = {
    # SELECT ANY TABLE/SEQUENCE/DICTIONARY 可覆盖跨 schema SELECT
    'SELECT': {
        'SELECT ANY TABLE',
        'SELECT ANY SEQUENCE',
        'SELECT ANY DICTIONARY',
    },
    # EXECUTE ANY PROCEDURE/TYPE 可覆盖跨 schema EXECUTE
    'EXECUTE': {
        'EXECUTE ANY PROCEDURE',
        'EXECUTE ANY TYPE',
    },
    # REFERENCES ANY TABLE 可覆盖跨 schema REFERENCES
    'REFERENCES': {
        'REFERENCES ANY TABLE',
    },
}

DDL_OBJECT_TYPE_OVERRIDE: Dict[str, Tuple[str, bool]] = {
    'PROCEDURE': ('PROCEDURE', True),
    'FUNCTION': ('FUNCTION', True),
    'PACKAGE': ('PACKAGE', True),
    'PACKAGE BODY': ('PACKAGE BODY', True),
    'TRIGGER': ('TRIGGER', True)
}
DBCAT_OPTION_MAP: Dict[str, str] = {
    'TABLE': '--table',
    'VIEW': '--view',
    'MATERIALIZED VIEW': '--mview',
    'PROCEDURE': '--procedure',
    'FUNCTION': '--function',
    'PACKAGE': '--package',
    'PACKAGE BODY': '--package-body',
    'SYNONYM': '--synonym',
    'SEQUENCE': '--sequence',
    'TRIGGER': '--trigger',
    'TYPE': '--type',
    'TYPE BODY': '--type-body',
    'MVIEW LOG': '--mview-log',
    'TABLEGROUP': '--tablegroup'
}

DBCAT_OUTPUT_DIR_HINTS: Dict[str, Tuple[str, ...]] = {
    'TABLE': ('TABLE',),
    'VIEW': ('VIEW',),
    'MATERIALIZED VIEW': ('MVIEW', 'MATERIALIZED VIEW'),
    'PROCEDURE': ('PROCEDURE',),
    'FUNCTION': ('FUNCTION',),
    'PACKAGE': ('PACKAGE',),
    'PACKAGE BODY': ('PACKAGE BODY', 'PACKAGE_BODY'),
    'SYNONYM': ('SYNONYM',),
    'SEQUENCE': ('SEQUENCE',),
    'TRIGGER': ('TRIGGER',),
    'TYPE': ('TYPE',),
    'TYPE BODY': ('TYPE BODY', 'TYPE_BODY'),
    'MVIEW LOG': ('MVIEW LOG', 'MVIEW_LOG'),
    'TABLEGROUP': ('TABLEGROUP',)
}
DBCAT_DIR_TO_TYPE: Dict[str, str] = {
    hint.upper(): obj_type
    for obj_type, hints in DBCAT_OUTPUT_DIR_HINTS.items()
    for hint in hints
}

# ====================== 通用配置和基础函数 ======================

def load_config(config_file: str) -> Tuple[OraConfig, ObConfig, Dict]:
    """读取 config.ini 配置文件"""
    log.info(f"正在加载配置文件: {config_file}")
    config = configparser.ConfigParser()
    if not config.read(config_file):
        log.error(f"严重错误: 配置文件 {config_file} 未找到或无法读取。")
        sys.exit(1)

    try:
        ora_cfg = dict(config['ORACLE_SOURCE'])
        ob_cfg = dict(config['OCEANBASE_TARGET'])
        settings = dict(config['SETTINGS'])

        schemas_raw = settings.get('source_schemas', '')
        schemas_list = [s.strip().upper() for s in schemas_raw.split(',') if s.strip()]
        if not schemas_list:
            log.error(f"严重错误: [SETTINGS] 中的 'source_schemas' 未配置或为空。")
            sys.exit(1)
        settings['source_schemas_list'] = schemas_list

        # remap 规则文件（可为空，表示按 1:1 映射）
        settings.setdefault('remap_file', '')

        # fixup 脚本目录
        settings.setdefault('fixup_dir', 'fixup_scripts')
        # obclient 超时时间 (秒)
        settings.setdefault('obclient_timeout', '60')
        # 报告输出目录
        settings.setdefault('report_dir', 'main_reports')
        # Oracle Instant Client 目录 (Thick Mode)
        settings.setdefault('oracle_client_lib_dir', '')
        # dbcat 相关配置
        settings.setdefault('dbcat_bin', '')
        settings.setdefault('dbcat_from', '')
        settings.setdefault('dbcat_to', '')
        settings.setdefault('dbcat_output_dir', 'dbcat_output')
        settings.setdefault('dbcat_no_cal_dep', 'false')
        settings.setdefault('dbcat_query_meta_thread', '')
        settings.setdefault('dbcat_progress_interval', '15')
        settings.setdefault('java_home', os.environ.get('JAVA_HOME', ''))
        # fixup 定向生成选项
        settings.setdefault('fixup_schemas', '')
        settings.setdefault('fixup_types', '')
        settings.setdefault('trigger_list', '')
        settings.setdefault('generate_grants', 'true')
        settings.setdefault('grant_tab_privs_scope', 'owner')
        settings.setdefault('grant_merge_privileges', 'true')
        settings.setdefault('grant_merge_grantees', 'true')
        settings.setdefault('grant_supported_sys_privs', '')
        settings.setdefault('grant_supported_object_privs', '')
        settings.setdefault('grant_include_oracle_maintained_roles', 'false')
        # 检查范围开关
        settings.setdefault('check_primary_types', '')
        settings.setdefault('check_extra_types', '')
        settings.setdefault('check_dependencies', 'true')
        settings.setdefault('print_dependency_chains', 'true')
        settings.setdefault('check_comments', 'true')
        settings.setdefault('infer_schema_mapping', 'true')
        settings.setdefault('ddl_punct_sanitize', 'true')
        settings.setdefault('dbcat_chunk_size', '150')
        settings.setdefault('fixup_workers', '')
        settings.setdefault('progress_log_interval', '10')
        settings.setdefault('report_width', '160')  # 报告宽度，避免nohup时被截断为80
        # 运行日志目录与级别
        settings.setdefault('log_dir', 'logs')
        settings.setdefault('log_level', 'INFO')

        enabled_primary_types = parse_type_list(
            settings.get('check_primary_types', ''),
            set(PRIMARY_OBJECT_TYPES),
            'check_primary_types'
        )
        enabled_extra_types = parse_type_list(
            settings.get('check_extra_types', ''),
            set(EXTRA_OBJECT_CHECK_TYPES),
            'check_extra_types'
        )
        settings['enabled_primary_types'] = enabled_primary_types
        settings['enabled_extra_types'] = enabled_extra_types
        settings['enable_dependencies_check'] = parse_bool_flag(
            settings.get('check_dependencies', 'true'),
            True
        )
        settings['enable_grant_generation'] = parse_bool_flag(
            settings.get('generate_grants', 'true'),
            True
        )
        settings['grant_tab_privs_scope'] = settings.get('grant_tab_privs_scope', 'owner').strip().lower()
        settings['grant_supported_sys_privs_set'] = parse_csv_set(settings.get('grant_supported_sys_privs', ''))
        settings['grant_supported_object_privs_set'] = parse_csv_set(
            settings.get('grant_supported_object_privs', '')
        )
        settings['grant_include_oracle_maintained_roles'] = parse_bool_flag(
            settings.get('grant_include_oracle_maintained_roles', 'false'),
            False
        )
        settings['enable_comment_check'] = parse_bool_flag(
            settings.get('check_comments', 'true'),
            True
        )
        settings['enable_ddl_punct_sanitize'] = parse_bool_flag(
            settings.get('ddl_punct_sanitize', 'true'),
            True
        )
        settings['print_dependency_chains'] = parse_bool_flag(
            settings.get('print_dependency_chains', 'true'),
            True
        )
        settings['enable_schema_mapping_infer'] = parse_bool_flag(
            settings.get('infer_schema_mapping', 'true'),
            True
        )
        settings['fixup_schema_list'] = [
            s.strip().upper() for s in settings.get('fixup_schemas', '').split(',') if s.strip()
        ]
        # 注意：fixup 类型默认值需要包含 CONSTRAINT，否则约束订正 SQL 不会生成
        settings['fixup_type_set'] = parse_type_list(
            settings.get('fixup_types', ''),
            set(ALL_TRACKED_OBJECT_TYPES) | set(EXTRA_OBJECT_CHECK_TYPES),
            'fixup_types'
        )
        try:
            cpu_default = max(1, os.cpu_count() or 1)
            settings['fixup_workers'] = int(settings.get('fixup_workers') or min(12, cpu_default))
            if settings['fixup_workers'] <= 0:
                settings['fixup_workers'] = min(12, cpu_default)
        except (TypeError, ValueError):
            cpu_default = max(1, os.cpu_count() or 1)
            settings['fixup_workers'] = min(12, cpu_default)
        try:
            settings['dbcat_chunk_size'] = int(settings.get('dbcat_chunk_size', '150'))
        except ValueError:
            settings['dbcat_chunk_size'] = 150
        settings['dbcat_no_cal_dep'] = parse_bool_flag(settings.get('dbcat_no_cal_dep', 'false'), False)
        try:
            settings['dbcat_query_meta_thread'] = int(settings.get('dbcat_query_meta_thread') or 0)
        except (TypeError, ValueError):
            settings['dbcat_query_meta_thread'] = 0
        if settings['dbcat_query_meta_thread'] < 0:
            settings['dbcat_query_meta_thread'] = 0
        try:
            settings['dbcat_progress_interval'] = int(settings.get('dbcat_progress_interval', '15'))
        except (TypeError, ValueError):
            settings['dbcat_progress_interval'] = 15
        if settings['dbcat_progress_interval'] < 0:
            settings['dbcat_progress_interval'] = 0

        global OBC_TIMEOUT
        try:
            OBC_TIMEOUT = int(settings['obclient_timeout'])
        except ValueError:
            OBC_TIMEOUT = 60

        log.info(f"成功加载配置，将扫描 {len(schemas_list)} 个源 schema。")
        log.info(f"obclient 超时时间: {OBC_TIMEOUT} 秒")
        log.warning(
            "注意：程序将从 DBA_* 视图读取 Oracle/OceanBase 元数据，请确保运行账号具备 DBA/SELECT ANY DICTIONARY/SELECT_CATALOG_ROLE 等等价权限，否则结果将不完整。"
        )
        return ora_cfg, ob_cfg, settings
    except KeyError as e:
        log.error(f"严重错误: 配置文件中缺少必要的部分: {e}")
        sys.exit(1)


def validate_runtime_paths(settings: Dict, ob_cfg: ObConfig) -> None:
    """在正式连接前，对关键路径和依赖做友好校验与提示。"""
    errors: List[str] = []
    warnings: List[str] = []

    # obclient 可执行文件
    obclient_path = Path(ob_cfg.get('executable', '')).expanduser()
    if not obclient_path.exists():
        errors.append(
            f"未找到 obclient 可执行文件: {obclient_path}。请在 config.ini 的 [OCEANBASE_TARGET] 中配置 executable 绝对路径。"
        )
    elif not os.access(obclient_path, os.X_OK):
        warnings.append(f"obclient 路径存在但不可执行: {obclient_path}，请检查权限。")

    # remap 文件
    remap_file = settings.get('remap_file', '').strip()
    if remap_file and not Path(remap_file).expanduser().exists():
        warnings.append(f"Remap 文件不存在: {remap_file}（将按 1:1 继续，可确认路径是否正确）。")

    trigger_list_path = settings.get('trigger_list', '').strip()
    if trigger_list_path and not Path(trigger_list_path).expanduser().exists():
        warnings.append(
            f"trigger_list 文件不存在: {trigger_list_path}（将记录在报告中并回退全量触发器生成）。"
        )

    # dbcat / JAVA_HOME 仅在生成 fixup 时提示
    generate_fixup_enabled = settings.get('generate_fixup', 'true').strip().lower() in ('true', '1', 'yes')
    if generate_fixup_enabled:
        dbcat_bin = settings.get('dbcat_bin', '').strip()
        if not dbcat_bin:
            warnings.append("generate_fixup 已开启，但未配置 dbcat_bin；如需生成订正 SQL，请在 [SETTINGS] 中填写 dbcat 目录或 bin/dbcat 路径。")
        else:
            dbcat_path = Path(dbcat_bin).expanduser()
            if not dbcat_path.exists():
                errors.append(f"dbcat 路径不存在: {dbcat_path}。请确认路径或关闭 generate_fixup。")
            else:
                candidate = dbcat_path / "bin" / "dbcat" if dbcat_path.is_dir() else dbcat_path
                if not candidate.exists():
                    warnings.append(f"未在 {dbcat_path} 下找到 dbcat 可执行文件（期望 bin/dbcat 或可执行文件）；生成脚本可能失败。")

        java_home = settings.get('java_home', '').strip() or os.environ.get('JAVA_HOME', '')
        if not java_home:
            warnings.append("generate_fixup 已开启，但 JAVA_HOME 未配置；dbcat 运行可能失败。")
        elif not Path(java_home).expanduser().exists():
            warnings.append(f"JAVA_HOME 指向的目录不存在: {java_home}，请确认 JDK 路径。")

    # 提示输出目录
    for key in ('fixup_dir', 'report_dir', 'dbcat_output_dir'):
        val = settings.get(key, '').strip()
        if not val:
            continue
        p = Path(val).expanduser()
        if not p.exists():
            warnings.append(f"目录 {p} 不存在，将在运行时尝试创建。请确保有写权限。")

    if warnings:
        for msg in warnings:
            log.warning(msg)
    if errors:
        for msg in errors:
            log.error(msg)
        log.error("关键路径缺失或不可用，已终止。请按提示修复后重试。")
        sys.exit(1)


def run_config_wizard(config_path: Path) -> None:
    """
    交互式向导：在缺失或无效配置时提示用户输入并回写 config.ini。
    若标准输入不可用则直接退出，以免阻塞自动化流水线。
    """
    if not sys.stdin.isatty():
        log.error("交互式向导需要可用的标准输入/终端。请在可交互环境运行或直接编辑 config.ini。")
        sys.exit(1)

    cfg = configparser.ConfigParser()
    if config_path.exists():
        cfg.read(config_path, encoding="utf-8")
        log.info("已加载现有配置，将检查缺失/无效项后写回: %s", config_path)
    else:
        log.warning("未找到配置文件，将创建: %s", config_path)

    for section in ("ORACLE_SOURCE", "OCEANBASE_TARGET", "SETTINGS"):
        if not cfg.has_section(section):
            cfg[section] = {}

    def _prompt_field(
        section: str,
        key: str,
        message: str,
        *,
        default: Optional[str] = None,
        required: bool = False,
        validator: Optional[Callable[[str], Tuple[bool, str]]] = None,
        transform: Optional[Callable[[str], str]] = None,
    ) -> str:
        current = cfg.get(section, key, fallback="").strip()
        display_default = current or (default or "")
        while True:
            user_input = input(f"{message} [{display_default}]: ").strip()
            value = user_input or display_default
            if required and not value:
                print("该项必填，请输入。")
                continue
            if validator:
                ok, reason = validator(value)
                if not ok:
                    print(f"无效输入: {reason}")
                    continue
            if transform:
                value = transform(value)
            cfg[section][key] = value
            return value

    def _validate_path_exists(p: str) -> Tuple[bool, str]:
        path = Path(p).expanduser()
        return (path.exists(), "路径不存在") if p else (False, "为空")

    def _validate_exec_path(p: str) -> Tuple[bool, str]:
        path = Path(p).expanduser()
        if not path.exists():
            return False, "路径不存在"
        if not os.access(path, os.X_OK):
            return False, "文件不可执行"
        return True, ""

    def _validate_positive_int(val: str) -> Tuple[bool, str]:
        try:
            return int(val) > 0, "需要正整数"
        except ValueError:
            return False, "需要正整数"

    def _validate_grant_scope(val: str) -> Tuple[bool, str]:
        v = val.strip().lower()
        if v in ("owner", "owner_or_grantee"):
            return True, ""
        return False, "仅支持 owner 或 owner_or_grantee"

    def _bool_transform(val: str) -> str:
        v = val.strip().lower()
        if v in ("true", "1", "yes", "y", "on"):
            return "true"
        if v in ("false", "0", "no", "n", "off"):
            return "false"
        return val or "true"

    print("\n=== 交互式配置向导 (空回车使用括号内默认值) ===")

    # ORACLE_SOURCE
    _prompt_field("ORACLE_SOURCE", "user", "Oracle 用户 (ORACLE_SOURCE.user)", required=True)
    _prompt_field("ORACLE_SOURCE", "password", "Oracle 密码 (ORACLE_SOURCE.password)", required=True)
    _prompt_field("ORACLE_SOURCE", "dsn", "Oracle DSN (host:port/service_name)", required=True)

    # OCEANBASE_TARGET
    _prompt_field(
        "OCEANBASE_TARGET",
        "executable",
        "obclient 可执行文件路径",
        validator=_validate_exec_path,
        required=True,
    )
    _prompt_field("OCEANBASE_TARGET", "host", "OceanBase 主机名/IP", required=True)
    _prompt_field(
        "OCEANBASE_TARGET",
        "port",
        "OceanBase 端口",
        default="2883",
        validator=_validate_positive_int,
        required=True,
    )
    _prompt_field("OCEANBASE_TARGET", "user_string", "-u 参数（含租户/库）", required=True)
    _prompt_field("OCEANBASE_TARGET", "password", "OceanBase 密码", required=True)

    # SETTINGS (关键路径与开关)
    _prompt_field(
        "SETTINGS",
        "oracle_client_lib_dir",
        "Oracle Instant Client 目录 (libclntsh.so 所在)",
        validator=_validate_path_exists,
        required=True,
    )
    _prompt_field(
        "SETTINGS",
        "source_schemas",
        "源 schema 列表 (逗号分隔)",
        required=True,
    )
    _prompt_field(
        "SETTINGS",
        "remap_file",
        "Remap 文件路径 (可选，默认 remap_rules.txt)",
        default="remap_rules.txt",
    )
    _prompt_field(
        "SETTINGS",
        "generate_fixup",
        "是否生成目标端订正 SQL (true/false)",
        default=cfg.get("SETTINGS", "generate_fixup", fallback="true"),
        transform=_bool_transform,
    )
    _prompt_field(
        "SETTINGS",
        "generate_grants",
        "是否生成授权脚本并附加到修复 DDL (true/false)",
        default=cfg.get("SETTINGS", "generate_grants", fallback="true"),
        transform=_bool_transform,
    )
    _prompt_field(
        "SETTINGS",
        "grant_tab_privs_scope",
        "授权抽取范围 (owner/owner_or_grantee)",
        default=cfg.get("SETTINGS", "grant_tab_privs_scope", fallback="owner"),
        validator=_validate_grant_scope,
    )
    _prompt_field(
        "SETTINGS",
        "grant_merge_privileges",
        "授权合并: 合并同一对象的多权限 (true/false)",
        default=cfg.get("SETTINGS", "grant_merge_privileges", fallback="true"),
        transform=_bool_transform,
    )
    _prompt_field(
        "SETTINGS",
        "grant_merge_grantees",
        "授权合并: 合并同权限的多 grantee (true/false)",
        default=cfg.get("SETTINGS", "grant_merge_grantees", fallback="true"),
        transform=_bool_transform,
    )
    _prompt_field(
        "SETTINGS",
        "grant_supported_sys_privs",
        "支持的系统权限清单 (逗号分隔，留空自动探测)",
        default=cfg.get("SETTINGS", "grant_supported_sys_privs", fallback=""),
    )
    _prompt_field(
        "SETTINGS",
        "grant_supported_object_privs",
        "支持的对象权限清单 (逗号分隔，留空使用默认)",
        default=cfg.get("SETTINGS", "grant_supported_object_privs", fallback=""),
    )
    _prompt_field(
        "SETTINGS",
        "grant_include_oracle_maintained_roles",
        "是否生成 ORACLE_MAINTAINED 角色 (true/false)",
        default=cfg.get("SETTINGS", "grant_include_oracle_maintained_roles", fallback="false"),
        transform=_bool_transform,
    )
    _prompt_field(
        "SETTINGS",
        "check_dependencies",
        "是否校验依赖 (true/false)",
        default=cfg.get("SETTINGS", "check_dependencies", fallback="true"),
        transform=_bool_transform,
    )
    _prompt_field(
        "SETTINGS",
        "check_comments",
        "是否比对表/列注释 (true/false)",
        default=cfg.get("SETTINGS", "check_comments", fallback="true"),
        transform=_bool_transform,
    )
    _prompt_field(
        "SETTINGS",
        "infer_schema_mapping",
        "是否自动推导 schema 映射 (true/false，默认 false，建议保持 false)",
        default=cfg.get("SETTINGS", "infer_schema_mapping", fallback="true"),
        transform=_bool_transform,
    )
    _prompt_field(
        "SETTINGS",
        "dbcat_chunk_size",
        "dbcat 单批对象数量 (默认 150，可适当增大)",
        default=cfg.get("SETTINGS", "dbcat_chunk_size", fallback="150"),
        validator=_validate_positive_int,
    )
    _prompt_field(
        "SETTINGS",
        "check_primary_types",
        "主对象过滤 (留空为全量，例如 TABLE,VIEW)",
        default=cfg.get("SETTINGS", "check_primary_types", fallback=""),
    )
    _prompt_field(
        "SETTINGS",
        "check_extra_types",
        "扩展对象过滤 (留空或 index,constraint,sequence,trigger)",
        default=cfg.get("SETTINGS", "check_extra_types", fallback="index,constraint,sequence,trigger"),
    )
    _prompt_field(
        "SETTINGS",
        "fixup_dir",
        "订正 SQL 输出目录",
        default=cfg.get("SETTINGS", "fixup_dir", fallback="fixup_scripts"),
    )
    _prompt_field(
        "SETTINGS",
        "fixup_schemas",
        "限定生成订正 SQL 的目标 schema 列表 (逗号分隔，留空为全部)",
        default=cfg.get("SETTINGS", "fixup_schemas", fallback=""),
    )
    _prompt_field(
        "SETTINGS",
        "fixup_types",
        "限定生成订正 SQL 的对象类型 (留空为全部，如 TABLE,VIEW,TRIGGER)",
        default=cfg.get("SETTINGS", "fixup_types", fallback=""),
    )
    _prompt_field(
        "SETTINGS",
        "trigger_list",
        "可选：触发器清单文件 (SCHEMA.TRIGGER_NAME，每行一条)",
        default=cfg.get("SETTINGS", "trigger_list", fallback=""),
    )
    _prompt_field(
        "SETTINGS",
        "report_dir",
        "报告输出目录",
        default=cfg.get("SETTINGS", "report_dir", fallback="main_reports"),
    )
    _prompt_field(
        "SETTINGS",
        "dbcat_output_dir",
        "dbcat 输出缓存目录",
        default=cfg.get("SETTINGS", "dbcat_output_dir", fallback="dbcat_output"),
    )
    _prompt_field(
        "SETTINGS",
        "obclient_timeout",
        "obclient 超时（秒）",
        default=cfg.get("SETTINGS", "obclient_timeout", fallback="60"),
        validator=_validate_positive_int,
    )
    _prompt_field(
        "SETTINGS",
        "cli_timeout",
        "dbcat CLI 超时（秒）",
        default=cfg.get("SETTINGS", "cli_timeout", fallback="600"),
        validator=_validate_positive_int,
    )

    # 只有生成 fixup 时才校验 dbcat/JAVA_HOME
    gen_fixup_val = cfg.get("SETTINGS", "generate_fixup", fallback="true").lower()
    gen_fixup_enabled = gen_fixup_val in ("true", "1", "yes", "y", "on")
    if gen_fixup_enabled:
        _prompt_field(
            "SETTINGS",
            "dbcat_bin",
            "dbcat 路径（目录或 bin/dbcat 可执行文件）",
            validator=_validate_path_exists,
            required=True,
        )
        _prompt_field(
            "SETTINGS",
            "java_home",
            "JAVA_HOME (dbcat 需要)",
            default=cfg.get("SETTINGS", "java_home", fallback=os.environ.get("JAVA_HOME", "")),
            validator=_validate_path_exists,
            required=True,
        )
        _prompt_field(
            "SETTINGS",
            "dbcat_from",
            "dbcat from profile",
            default=cfg.get("SETTINGS", "dbcat_from", fallback="oracle19c"),
            required=True,
        )
        _prompt_field(
            "SETTINGS",
            "dbcat_to",
            "dbcat to profile",
            default=cfg.get("SETTINGS", "dbcat_to", fallback="oboracle422"),
            required=True,
        )

    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as fp:
        cfg.write(fp)
    log.info("配置已保存: %s", config_path)


def load_remap_rules(file_path: str) -> RemapRules:
    """从 txt 文件加载 remap 规则"""
    log.info(f"正在加载 Remap 规则文件: {file_path}")
    rules: RemapRules = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                if '=' not in line:
                    log.warning(f"  [规则警告] 第 {i+1} 行格式错误，已跳过: {line}")
                    continue

                try:
                    src_obj, tgt_obj = line.split('=', 1)
                    src_obj = src_obj.strip().upper()
                    tgt_obj = tgt_obj.strip().upper()
                    if not src_obj or not tgt_obj or '.' not in src_obj or '.' not in tgt_obj:
                        log.warning(f"  [规则警告] 第 {i+1} 行格式无效 (必须为 'SCHEMA.OBJ')，已跳过: {line}")
                        continue
                    rules[src_obj] = tgt_obj
                except Exception:
                    log.warning(f"  [规则警告] 第 {i+1} 行解析失败，已跳过: {line}")

    except FileNotFoundError:
        log.warning(f"  [警告] Remap 文件 {file_path} 未找到。将按 1:1 规则继续。")
        return {}

    log.info(f"加载了 {len(rules)} 条 Remap 规则。")
    return rules


def parse_trigger_list_file(
    file_path: str
) -> Tuple[Set[str], List[Tuple[int, str, str]], List[Tuple[int, str]], int, Optional[str]]:
    """
    解析 trigger_list 文件，每行格式为 SCHEMA.TRIGGER_NAME。
    返回 (entries, invalid_entries, duplicate_entries, total_lines, error).
    """
    entries: Set[str] = set()
    invalid_entries: List[Tuple[int, str, str]] = []
    duplicate_entries: List[Tuple[int, str]] = []
    total_lines = 0
    if not file_path:
        return entries, invalid_entries, duplicate_entries, total_lines, None

    path = Path(file_path).expanduser()
    try:
        with path.open("r", encoding="utf-8") as fp:
            for line_no, raw in enumerate(fp, start=1):
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "#" in line:
                    line = line.split("#", 1)[0].strip()
                    if not line:
                        continue
                total_lines += 1
                if "." not in line:
                    invalid_entries.append((line_no, raw.strip(), "缺少 schema 前缀 (SCHEMA.TRIGGER_NAME)"))
                    continue
                schema, name = line.split(".", 1)
                schema = schema.strip().strip('"')
                name = name.strip().strip('"')
                if not schema or not name:
                    invalid_entries.append((line_no, raw.strip(), "schema 或 trigger 名称为空"))
                    continue
                full_name = f"{schema.upper()}.{name.upper()}"
                if full_name in entries:
                    duplicate_entries.append((line_no, full_name))
                    continue
                entries.add(full_name)
        return entries, invalid_entries, duplicate_entries, total_lines, None
    except FileNotFoundError:
        return set(), [], [], 0, f"文件不存在: {path}"
    except OSError as exc:
        return set(), [], [], 0, f"读取失败: {exc}"


def build_trigger_full_set(
    triggers: Dict[Tuple[str, str], Dict[str, Dict]]
) -> Set[str]:
    """将元数据中的触发器转换为 OWNER.TRIGGER_NAME 集合。"""
    full_set: Set[str] = set()
    for (owner, _), trg_map in triggers.items():
        owner_u = (owner or "").upper()
        for trg_name, info in (trg_map or {}).items():
            trg_owner = (info.get("owner") or owner_u).upper()
            name_u = (trg_name or "").upper()
            if trg_owner and name_u:
                full_set.add(f"{trg_owner}.{name_u}")
    return full_set


def collect_missing_trigger_mappings(
    extra_results: ExtraCheckResults
) -> Tuple[Dict[str, str], Set[str], int]:
    """
    从 trigger_mismatched 结果中提取缺失触发器映射。
    返回 (src_to_tgt_map, missing_targets, total_missing).
    """
    src_to_tgt: Dict[str, str] = {}
    missing_targets: Set[str] = set()
    total_missing = 0
    for item in extra_results.get("trigger_mismatched", []):
        total_missing += len(item.missing_triggers)
        for tgt_full in item.missing_triggers:
            if tgt_full:
                missing_targets.add(tgt_full.upper())
        if item.missing_mappings:
            for src_full, tgt_full in item.missing_mappings:
                if not src_full or not tgt_full:
                    continue
                src_u = src_full.upper()
                tgt_u = tgt_full.upper()
                src_to_tgt[src_u] = tgt_u
                missing_targets.add(tgt_u)
    return src_to_tgt, missing_targets, total_missing


def build_trigger_list_report(
    trigger_list_path: str,
    entries: Set[str],
    invalid_entries: List[Tuple[int, str, str]],
    duplicate_entries: List[Tuple[int, str]],
    total_lines: int,
    read_error: Optional[str],
    extra_results: ExtraCheckResults,
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    full_object_mapping: FullObjectMapping,
    trigger_check_enabled: bool
) -> Tuple[List[TriggerListReportRow], Dict[str, object]]:
    """
    基于 trigger_list 与缺失触发器结果构造报告行与汇总信息。
    """
    summary: Dict[str, object] = {
        "enabled": True,
        "path": str(trigger_list_path),
        "total_lines": total_lines,
        "valid_entries": len(entries),
        "invalid_entries": len(invalid_entries),
        "duplicate_entries": len(duplicate_entries),
        "selected_missing": 0,
        "missing_not_listed": 0,
        "not_found": 0,
        "not_missing": 0,
        "check_disabled": False,
        "error": read_error or "",
        "fallback_full": False,
        "fallback_reason": ""
    }
    rows: List[TriggerListReportRow] = []

    if read_error:
        summary["fallback_full"] = True
        summary["fallback_reason"] = "read_error"
        _, _, total_missing = collect_missing_trigger_mappings(extra_results)
        summary["missing_not_listed"] = total_missing
        rows.append(TriggerListReportRow(
            entry=str(trigger_list_path),
            status="ERROR",
            detail=read_error
        ))
        return rows, summary

    for line_no, raw, reason in invalid_entries:
        rows.append(TriggerListReportRow(
            entry=f"line {line_no}: {raw}",
            status="INVALID",
            detail=reason
        ))
    for line_no, entry in duplicate_entries:
        rows.append(TriggerListReportRow(
            entry=f"line {line_no}: {entry}",
            status="DUPLICATE",
            detail="重复条目"
        ))

    if not trigger_check_enabled:
        summary["check_disabled"] = True
        for entry in sorted(entries):
            rows.append(TriggerListReportRow(
                entry=entry,
                status="CHECK_DISABLED",
                detail="TRIGGER 未启用检查，无法判定缺失状态"
            ))
        return rows, summary

    if not entries:
        summary["fallback_full"] = True
        summary["fallback_reason"] = "empty_list"
        _, _, total_missing = collect_missing_trigger_mappings(extra_results)
        summary["missing_not_listed"] = total_missing
        rows.append(TriggerListReportRow(
            entry=str(trigger_list_path),
            status="EMPTY",
            detail="清单为空，已回退全量触发器"
        ))
        return rows, summary

    src_to_tgt, missing_targets, total_missing = collect_missing_trigger_mappings(extra_results)
    missing_src_set = set(src_to_tgt.keys())
    missing_by_tgt = {tgt: src for src, tgt in src_to_tgt.items()}
    source_triggers = build_trigger_full_set(oracle_meta.triggers or {})
    target_triggers = build_trigger_full_set(ob_meta.triggers or {})

    selected_missing_targets: Set[str] = set()
    not_found = 0
    not_missing = 0

    for entry in sorted(entries):
        entry_u = entry.upper()
        if entry_u in missing_src_set:
            tgt_full = src_to_tgt.get(entry_u)
            if tgt_full:
                selected_missing_targets.add(tgt_full)
            rows.append(TriggerListReportRow(
                entry=entry_u,
                status="SELECTED_MISSING",
                detail=f"目标缺失: {tgt_full}" if tgt_full else "目标缺失"
            ))
            continue
        if entry_u in missing_targets:
            src_full = missing_by_tgt.get(entry_u)
            selected_missing_targets.add(entry_u)
            rows.append(TriggerListReportRow(
                entry=entry_u,
                status="SELECTED_MISSING",
                detail=f"源触发器: {src_full}" if src_full else "目标缺失"
            ))
            continue
        if entry_u not in source_triggers:
            if entry_u in target_triggers:
                not_missing += 1
                rows.append(TriggerListReportRow(
                    entry=entry_u,
                    status="EXISTS_IN_TARGET",
                    detail="目标端已存在"
                ))
            else:
                not_found += 1
                rows.append(TriggerListReportRow(
                    entry=entry_u,
                    status="NOT_FOUND_IN_SOURCE",
                    detail="源端未找到触发器"
                ))
            continue

        target_full = get_mapped_target(full_object_mapping, entry_u, 'TRIGGER') or entry_u
        if target_full.upper() in target_triggers:
            not_missing += 1
            rows.append(TriggerListReportRow(
                entry=entry_u,
                status="EXISTS_IN_TARGET",
                detail=f"目标端已存在: {target_full.upper()}"
            ))
        else:
            not_missing += 1
            rows.append(TriggerListReportRow(
                entry=entry_u,
                status="NOT_MISSING_OR_OUT_OF_SCOPE",
                detail="未在缺失清单中或不在本次校验范围"
            ))

    summary["selected_missing"] = len(selected_missing_targets)
    summary["missing_not_listed"] = max(0, total_missing - len(selected_missing_targets))
    summary["not_found"] = not_found
    summary["not_missing"] = not_missing
    return rows, summary


def init_oracle_client_from_settings(settings: Dict) -> None:
    """根据配置初始化 Oracle Thick Mode 并提示环境变量设置。"""
    client_dir = settings.get('oracle_client_lib_dir', '').strip()
    if not client_dir:
        log.error("严重错误: 未在 [SETTINGS] 中配置 oracle_client_lib_dir。")
        log.error("请在 config.ini 中添加例如: oracle_client_lib_dir = /home/user/instantclient_19_28")
        sys.exit(1)

    client_path = Path(client_dir).expanduser()
    if not client_path.exists():
        log.error(f"严重错误: 指定的 Oracle Instant Client 目录不存在: {client_path}")
        sys.exit(1)

    ld_path = os.environ.get('LD_LIBRARY_PATH') or '<未设置>'
    log.info(f"准备使用 Oracle Instant Client 目录: {client_path}")
    log.info("如遇 libnnz19.so 等库缺失，请先执行:")
    log.info(f"  export LD_LIBRARY_PATH=\"{client_path}:${{LD_LIBRARY_PATH}}\"")
    log.info(f"当前 LD_LIBRARY_PATH: {ld_path}")

    try:
        oracledb.init_oracle_client(lib_dir=str(client_path))
    except Exception as exc:
        log.error("严重错误: Oracle Thick Mode 初始化失败。")
        log.error("请确认 instant client 路径和 LD_LIBRARY_PATH 设置正确。")
        log.error(f"错误详情: {exc}")
        sys.exit(1)


def get_source_objects(
    ora_cfg: OraConfig,
    schemas_list: List[str],
    object_types: Optional[Set[str]] = None
) -> SourceObjectMap:
    """
    从 Oracle 源端获取所有需要纳入 remap/依赖分析的对象：
      TABLE / VIEW / MATERIALIZED VIEW / PROCEDURE / FUNCTION / PACKAGE / PACKAGE BODY /
      SYNONYM / JOB / SCHEDULE / TYPE / TYPE BODY / TRIGGER / SEQUENCE / INDEX
    object_types: 可选的类型过滤集合（只查询指定类型）
    """
    log.info(f"正在连接 Oracle 源端: {ora_cfg['dsn']}...")

    enabled_types = {t.upper() for t in (object_types or set(ALL_TRACKED_OBJECT_TYPES))}
    enabled_types &= set(ALL_TRACKED_OBJECT_TYPES)
    include_synonyms = 'SYNONYM' in enabled_types
    object_types_for_objects = set(enabled_types)
    if include_synonyms:
        object_types_for_objects.discard('SYNONYM')
    if not object_types_for_objects and not include_synonyms:
        log.warning("未启用任何可管理对象类型，源端对象列表为空。")
        return {}
    object_types_clause = ",".join(f"'{obj}'" for obj in sorted(object_types_for_objects)) if object_types_for_objects else ""

    source_objects: SourceObjectMap = defaultdict(set)
    mview_pairs: Set[Tuple[str, str]] = set()
    table_pairs: Set[Tuple[str, str]] = set()
    skipped_iot = 0
    added_synonyms = 0
    added_public_synonyms = 0

    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as connection:
            log.info("Oracle 连接成功。正在查询源对象列表...")
            if object_types_for_objects:
                sql_tpl = """
                    SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
                    FROM DBA_OBJECTS
                    WHERE OWNER IN ({placeholders})
                      AND OBJECT_TYPE IN (
                          {object_types_clause}
                      )
                """
                with connection.cursor() as cursor:
                    for placeholders, chunk in iter_in_chunks(schemas_list):
                        sql = sql_tpl.format(
                            placeholders=placeholders,
                            object_types_clause=object_types_clause
                        )
                        cursor.execute(sql, chunk)
                        for row in cursor:
                            owner = (row[0] or '').strip().upper()
                            obj_name = (row[1] or '').strip().upper()
                            obj_type = (row[2] or '').strip().upper()
                            if not owner or not obj_name or not obj_type:
                                continue
                            if obj_name.startswith("SYS_IOT_OVER_"):
                                skipped_iot += 1
                                continue
                            full_name = f"{owner}.{obj_name}"
                            source_objects[full_name].add(obj_type)
            if include_synonyms:
                synonym_owners = sorted(set(s.upper() for s in schemas_list) | {"PUBLIC"})
                target_owners = sorted({s.upper() for s in schemas_list})
                if target_owners:
                    synonym_chunks = chunk_list(synonym_owners, ORACLE_IN_BATCH_SIZE)
                    target_chunks = chunk_list(target_owners, ORACLE_IN_BATCH_SIZE)
                    sql_tpl = """
                        SELECT OWNER, SYNONYM_NAME, TABLE_OWNER, TABLE_NAME
                        FROM DBA_SYNONYMS
                        WHERE OWNER IN ({owner_ph})
                          AND TABLE_OWNER IN ({target_ph})
                          AND TABLE_NAME IS NOT NULL
                    """
                    with connection.cursor() as cursor:
                        for owner_chunk in synonym_chunks:
                            owner_ph = build_bind_placeholders(len(owner_chunk))
                            for target_chunk in target_chunks:
                                target_ph = build_bind_placeholders(len(target_chunk), offset=len(owner_chunk))
                                sql = sql_tpl.format(owner_ph=owner_ph, target_ph=target_ph)
                                cursor.execute(sql, owner_chunk + target_chunk)
                                for row in cursor:
                                    owner = (row[0] or '').strip().upper()
                                    syn_name = (row[1] or '').strip().upper()
                                    table_owner = (row[2] or '').strip().upper()
                                    table_name = (row[3] or '').strip().upper()
                                    if not owner or not syn_name or not table_owner or not table_name:
                                        continue
                                    full_name = f"{owner}.{syn_name}"
                                    source_objects[full_name].add('SYNONYM')
                                    if owner == 'PUBLIC':
                                        added_public_synonyms += 1
                                    else:
                                        added_synonyms += 1
            # 精确认定物化视图集合，避免误删真实表
            with connection.cursor() as cursor:
                for placeholders, chunk in iter_in_chunks(schemas_list):
                    cursor.execute(
                        f"SELECT OWNER, MVIEW_NAME FROM DBA_MVIEWS WHERE OWNER IN ({placeholders})",
                        chunk
                    )
                    for row in cursor:
                        owner = (row[0] or '').strip().upper()
                        name = (row[1] or '').strip().upper()
                        if owner and name:
                            mview_pairs.add((owner, name))
            with connection.cursor() as cursor:
                for placeholders, chunk in iter_in_chunks(schemas_list):
                    cursor.execute(
                        f"SELECT OWNER, TABLE_NAME FROM DBA_TABLES WHERE OWNER IN ({placeholders})",
                        chunk
                    )
                    for row in cursor:
                        owner = (row[0] or '').strip().upper()
                        name = (row[1] or '').strip().upper()
                        if owner and name:
                            if name.startswith("SYS_IOT_OVER_"):
                                skipped_iot += 1
                                continue
                            table_pairs.add((owner, name))
    except oracledb.Error as e:
        log.error(f"严重错误: 连接或查询 Oracle 失败: {e}")
        sys.exit(1)

    # Materialized View 在 DBA_OBJECTS 中通常会同时作为 TABLE 出现，去重以避免误将 MV 当成 TABLE 校验/抽取。
    mview_dedup = 0
    mview_table_keep = 0
    pure_tables = table_pairs - mview_pairs  # DBA_TABLES 也包含 MVIEW，这里只保留真实 TABLE
    for full_name, types in source_objects.items():
        if 'MATERIALIZED VIEW' in types and 'TABLE' in types:
            try:
                owner, name = full_name.split('.', 1)
            except ValueError:
                continue
            key = (owner.upper(), name.upper())
            # 只有确定该对象存在于 DBA_MVIEWS 且不在“纯表”列表时，才移除 TABLE 标记
            if key in mview_pairs and key not in pure_tables:
                types.discard('TABLE')
                mview_dedup += 1
            elif key in pure_tables:
                mview_table_keep += 1
                log.warning(
                    "检测到同名 TABLE 与 MATERIALIZED VIEW，保留 TABLE 校验: %s",
                    full_name
                )
    if mview_dedup:
        log.info(
            "检测到 %d 个 MATERIALIZED VIEW 同时出现在 TABLE 列表中，已按 MVIEW 处理并移除重复 TABLE 类型。",
            mview_dedup
        )

    total_objects = sum(len(types) for types in source_objects.values())
    log.info(
        "从 Oracle 成功获取 %d 个受管对象 (包含主对象与扩展对象)。",
        total_objects
    )
    if include_synonyms:
        log.info(
            "已纳入同义词 %d 个（含 PUBLIC %d 个），仅保留指向 source_schemas 的同义词。",
            added_synonyms + added_public_synonyms,
            added_public_synonyms
        )
    if skipped_iot:
        log.info("已跳过 %d 个 SYS_IOT_OVER_* IOT 表，不参与对比或修补脚本生成。", skipped_iot)
    return dict(source_objects)


# 依附对象到父表的映射类型（触发器/同义词等需要跟随父表 schema）
ObjectParentMap = Dict[str, str]  # {SCHEMA.OBJECT_NAME: SCHEMA.TABLE_NAME}


def get_object_parent_tables(
    ora_cfg: OraConfig,
    schemas_list: List[str],
    enabled_object_types: Optional[Set[str]] = None
) -> ObjectParentMap:
    """
    获取依附对象（TRIGGER/SYNONYM/INDEX/CONSTRAINT 等）所属的父表。
    返回 {SCHEMA.OBJECT_NAME: SCHEMA.TABLE_NAME} 映射：
      - INDEX/CONSTRAINT/SEQUENCE 等依附对象跟随父表 schema
      - TRIGGER 仅用于依赖推导（触发器自身 schema 不随父表 remap）
    """
    parent_map: ObjectParentMap = {}
    enabled_types = {t.upper() for t in (enabled_object_types or set(ALL_TRACKED_OBJECT_TYPES))}
    include_triggers = 'TRIGGER' in enabled_types
    include_synonyms = 'SYNONYM' in enabled_types
    include_indexes = 'INDEX' in enabled_types
    include_constraints = 'CONSTRAINT' in enabled_types
    
    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as connection:
            # 获取触发器所属的表
            if include_triggers:
                sql_tpl = """
                    SELECT OWNER, TRIGGER_NAME, TABLE_OWNER, TABLE_NAME
                    FROM DBA_TRIGGERS
                    WHERE OWNER IN ({placeholders})
                      AND TABLE_NAME IS NOT NULL
                      AND BASE_OBJECT_TYPE IN ('TABLE', 'VIEW')
                """
                with connection.cursor() as cursor:
                    for placeholders, chunk in iter_in_chunks(schemas_list):
                        sql = sql_tpl.format(placeholders=placeholders)
                        cursor.execute(sql, chunk)
                        for row in cursor:
                            owner = (row[0] or '').strip().upper()
                            trigger_name = (row[1] or '').strip().upper()
                            table_owner = (row[2] or '').strip().upper()
                            table_name = (row[3] or '').strip().upper()
                            if owner and trigger_name and table_owner and table_name:
                                trigger_key = f"{owner}.{trigger_name}"
                                table_key = f"{table_owner}.{table_name}"
                                parent_map[trigger_key] = table_key

            # 获取同义词指向的表/视图，使同义词也能跟随父表 schema
            if include_synonyms:
                sql_tpl = """
                    SELECT OWNER, SYNONYM_NAME, TABLE_OWNER, TABLE_NAME
                    FROM DBA_SYNONYMS
                    WHERE OWNER IN ({placeholders})
                      AND TABLE_OWNER IS NOT NULL
                      AND TABLE_NAME IS NOT NULL
                """
                with connection.cursor() as cursor:
                    for placeholders, chunk in iter_in_chunks(schemas_list):
                        sql = sql_tpl.format(placeholders=placeholders)
                        cursor.execute(sql, chunk)
                        for row in cursor:
                            owner = (row[0] or '').strip().upper()
                            syn_name = (row[1] or '').strip().upper()
                            table_owner = (row[2] or '').strip().upper()
                            table_name = (row[3] or '').strip().upper()
                            if owner and syn_name and table_owner and table_name:
                                syn_key = f"{owner}.{syn_name}"
                                table_key = f"{table_owner}.{table_name}"
                                parent_map[syn_key] = table_key

            # 获取索引所属的表
            if include_indexes:
                sql_tpl = """
                    SELECT OWNER, INDEX_NAME, TABLE_OWNER, TABLE_NAME
                    FROM DBA_INDEXES
                    WHERE OWNER IN ({placeholders})
                      AND TABLE_OWNER IS NOT NULL
                      AND TABLE_NAME IS NOT NULL
                """
                with connection.cursor() as cursor:
                    for placeholders, chunk in iter_in_chunks(schemas_list):
                        sql = sql_tpl.format(placeholders=placeholders)
                        cursor.execute(sql, chunk)
                        for row in cursor:
                            owner = (row[0] or '').strip().upper()
                            index_name = (row[1] or '').strip().upper()
                            table_owner = (row[2] or '').strip().upper()
                            table_name = (row[3] or '').strip().upper()
                            if owner and index_name and table_owner and table_name:
                                index_key = f"{owner}.{index_name}"
                                table_key = f"{table_owner}.{table_name}"
                                parent_map[index_key] = table_key

            # 获取约束所属的表
            if include_constraints:
                sql_tpl = """
                    SELECT OWNER, CONSTRAINT_NAME, TABLE_NAME
                    FROM DBA_CONSTRAINTS
                    WHERE OWNER IN ({placeholders})
                      AND TABLE_NAME IS NOT NULL
                """
                with connection.cursor() as cursor:
                    for placeholders, chunk in iter_in_chunks(schemas_list):
                        sql = sql_tpl.format(placeholders=placeholders)
                        cursor.execute(sql, chunk)
                        for row in cursor:
                            owner = (row[0] or '').strip().upper()
                            cons_name = (row[1] or '').strip().upper()
                            table_name = (row[2] or '').strip().upper()
                            if owner and cons_name and table_name:
                                cons_key = f"{owner}.{cons_name}"
                                table_key = f"{owner}.{table_name}"
                                parent_map[cons_key] = table_key

            log.info("已获取 %d 个依附对象的父表映射（触发器/同义词/索引/约束）。", len(parent_map))
    except oracledb.Error as e:
        log.warning(f"获取依附对象父表映射失败: {e}")
    
    return parent_map


def load_synonym_metadata(
    ora_cfg: OraConfig,
    schemas_list: List[str],
    allowed_target_schemas: Optional[List[str]] = None
) -> Dict[Tuple[str, str], SynonymMeta]:
    """
    快速读取同义词定义，避免逐个 DBMS_METADATA 调用。
    返回 {(OWNER, SYNONYM_NAME): SynonymMeta}
    allowed_target_schemas: 仅保留指向这些 schema 的同义词（用于过滤系统 PUBLIC 同义词等无关对象）。
    """
    if not schemas_list:
        return {}

    allowed_targets = {s.upper() for s in (allowed_target_schemas or [])}
    owners = sorted(set(s.upper() for s in schemas_list) | {"PUBLIC"})
    sql_tpl = """
        SELECT OWNER, SYNONYM_NAME, TABLE_OWNER, TABLE_NAME, DB_LINK
        FROM DBA_SYNONYMS
        WHERE OWNER IN ({owner_ph})
          AND TABLE_OWNER IS NOT NULL
          AND TABLE_NAME IS NOT NULL
          {target_filter}
    """

    result: Dict[Tuple[str, str], SynonymMeta] = {}
    skipped_public = 0
    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as connection:
            with connection.cursor() as cursor:
                owner_chunks = chunk_list(owners, ORACLE_IN_BATCH_SIZE)
                target_chunks = chunk_list(sorted(allowed_targets), ORACLE_IN_BATCH_SIZE) if allowed_targets else []
                if not allowed_targets:
                    for owner_chunk in owner_chunks:
                        owner_ph = build_bind_placeholders(len(owner_chunk))
                        sql = sql_tpl.format(owner_ph=owner_ph, target_filter="")
                        cursor.execute(sql, owner_chunk)
                        for row in cursor:
                            owner = (row[0] or '').strip().upper()
                            name = (row[1] or '').strip().upper()
                            table_owner = (row[2] or '').strip().upper()
                            table_name = (row[3] or '').strip().upper()
                            db_link = (row[4] or '').strip().upper() if row[4] else None
                            if not owner or not name or not table_name:
                                continue
                            key = (owner, name)
                            result[key] = SynonymMeta(
                                owner=owner,
                                name=name,
                                table_owner=table_owner,
                                table_name=table_name,
                                db_link=db_link
                            )
                else:
                    for owner_chunk in owner_chunks:
                        owner_ph = build_bind_placeholders(len(owner_chunk))
                        for target_chunk in target_chunks:
                            target_ph = build_bind_placeholders(len(target_chunk), offset=len(owner_chunk))
                            target_filter = f"AND TABLE_OWNER IN ({target_ph})"
                            sql = sql_tpl.format(owner_ph=owner_ph, target_filter=target_filter)
                            cursor.execute(sql, owner_chunk + target_chunk)
                            for row in cursor:
                                owner = (row[0] or '').strip().upper()
                                name = (row[1] or '').strip().upper()
                                table_owner = (row[2] or '').strip().upper()
                                table_name = (row[3] or '').strip().upper()
                                db_link = (row[4] or '').strip().upper() if row[4] else None
                                if not owner or not name or not table_name:
                                    continue
                                if owner == 'PUBLIC' and table_owner and table_owner.upper() not in allowed_targets:
                                    skipped_public += 1
                                    continue
                                key = (owner, name)
                                result[key] = SynonymMeta(
                                    owner=owner,
                                    name=name,
                                    table_owner=table_owner,
                                    table_name=table_name,
                                    db_link=db_link
                                )
    except oracledb.Error as exc:
        log.warning("读取同义词元数据失败，将回退 DBMS_METADATA：%s", exc)

    target_hint = ",".join(sorted(allowed_targets)) if allowed_targets else "<ALL>"
    log.info(
        "已缓存 %d 个同义词元数据（OWNER IN %s，TABLE_OWNER IN %s）。",
        len(result),
        ",".join(owners),
        target_hint
    )
    return result


def validate_remap_rules(
    remap_rules: RemapRules,
    source_objects: SourceObjectMap,
    remap_file_path: Optional[str] = None
) -> List[str]:
    """检查 remap 规则中的源对象是否存在于 Oracle source_objects 中，并清洗无效条目。"""
    log.info("正在验证 Remap 规则...")
    remap_keys = set(remap_rules.keys())
    source_keys = set(source_objects.keys())
    body_aliases = {
        f"{name} BODY"
        for name, obj_types in source_objects.items()
        if any(obj_type.upper() in ('PACKAGE BODY', 'TYPE BODY') for obj_type in obj_types)
    }
    source_keys_with_alias = source_keys | body_aliases

    extraneous_keys = sorted(list(remap_keys - source_keys_with_alias))

    if extraneous_keys:
        log.warning(f"  [规则警告] 在 remap_rules.txt 中发现了 {len(extraneous_keys)} 个无效的源对象。")
        log.warning("  (这些对象在源端 Oracle (config.ini 中配置的 schema) 中未找到)")
        for key in extraneous_keys:
            log.warning(f"    - 无效条目: {key}")
        # 将无效规则另存，不修改原始 remap 文件
        if remap_file_path:
            remap_path = Path(remap_file_path).expanduser()
            try:
                raw_lines = remap_path.read_text(encoding="utf-8").splitlines()
            except OSError as exc:
                log.warning("  [规则警告] 无法读取 remap 文件以清洗无效条目: %s", exc)
            else:
                removed: List[str] = []
                extra_set = set(extraneous_keys)
                for line in raw_lines:
                    stripped = line.strip()
                    if not stripped or stripped.startswith("#") or "=" not in stripped:
                        continue
                    src_part = stripped.split("=", 1)[0].strip().upper()
                    if src_part in extra_set:
                        removed.append(line)

                if removed:
                    invalid_path = remap_path.with_name(
                        f"{remap_path.stem}_invalid{remap_path.suffix or '.txt'}"
                    )
                    try:
                        invalid_path.write_text("\n".join(removed) + "\n", encoding="utf-8")
                    except OSError as exc:
                        log.warning("  [规则警告] 写入无效 remap 条目文件失败: %s", exc)
                    else:
                        log.warning(
                            "  [规则警告] 检出 %d 条无效 remap 规则并保存到: %s (原 remap_rules 未修改)",
                            len(removed),
                            invalid_path
                        )
    else:
        log.info("Remap 规则验证通过，所有规则中的源对象均存在。")

    # 从内存映射中移除无效规则，避免后续继续使用
    for key in extraneous_keys:
        remap_rules.pop(key, None)

    return extraneous_keys


def strip_body_suffix(name: str) -> str:
    text = name.rstrip()
    if text.upper().endswith(' BODY'):
        return text[:-5].rstrip()
    return text


def derive_schema_mapping_from_rules(remap_rules: RemapRules) -> Dict[str, str]:
    """
    基于 remap_rules 推导 schema 级别的映射：
      如果某个源 schema 只映射到唯一的目标 schema，则作为默认映射；
      避免多对多/多对一的模糊情况。
    """
    schema_targets: Dict[str, Set[str]] = defaultdict(set)
    for src_full, tgt_full in remap_rules.items():
        if '.' not in src_full or '.' not in tgt_full:
            continue
        src_schema, _ = src_full.split('.', 1)
        tgt_schema, _ = tgt_full.split('.', 1)
        schema_targets[src_schema.upper()].add(tgt_schema.upper())

    schema_mapping: Dict[str, str] = {}
    for src_schema, tgt_set in schema_targets.items():
        if len(tgt_set) == 1:
            schema_mapping[src_schema] = next(iter(tgt_set))
    return schema_mapping


def infer_dominant_schema_from_rules(
    remap_rules: RemapRules,
    src_schema: str,
    source_objects: Optional[SourceObjectMap] = None
) -> Optional[str]:
    """
    基于 remap_rules 中属于同一源 schema 的表映射，推导出现次数最多的目标 schema。
    在 remap_rules 只有 TABLE 映射的场景下，可用于让依附对象（如 SEQUENCE/SYNONYM）
    跟随父表的主流目标 schema。
    """
    src_schema_u = src_schema.upper()
    counts: Dict[str, int] = defaultdict(int)
    table_keys: Optional[Set[str]] = None
    explicit_tables: Set[str] = set()
    if source_objects:
        table_keys = {
            name.upper()
            for name, types in source_objects.items()
            if any(t.upper() == 'TABLE' for t in types)
        }
    for src_full, tgt_full in remap_rules.items():
        if '.' not in src_full or '.' not in tgt_full:
            continue
        src_full_u = src_full.upper()
        if table_keys is not None and src_full_u not in table_keys:
            continue
        explicit_tables.add(src_full_u)
        s_schema, _ = src_full_u.split('.', 1)
        if s_schema != src_schema_u:
            continue
        t_schema, _ = tgt_full.split('.', 1)
        counts[t_schema.upper()] += 1
    if table_keys is not None:
        # Treat tables missing in remap_rules as 1:1 mappings to their source schema.
        for src_full_u in table_keys:
            if src_full_u in explicit_tables:
                continue
            s_schema, _ = src_full_u.split('.', 1)
            if s_schema != src_schema_u:
                continue
            counts[s_schema] += 1
    if not counts:
        return None
    max_count = max(counts.values())
    candidates = [schema for schema, c in counts.items() if c == max_count]
    if len(candidates) == 1:
        return candidates[0]
    return None


def infer_sequence_target_schema_from_dependents(
    seq_full: str,
    remap_rules: RemapRules,
    source_objects: Optional[SourceObjectMap] = None,
    schema_mapping: Optional[Dict[str, str]] = None,
    object_parent_map: Optional[ObjectParentMap] = None,
    dependency_graph: Optional[DependencyGraph] = None,
    transitive_table_cache: Optional[TransitiveTableCache] = None,
    source_dependencies: Optional[SourceDependencySet] = None,
    remap_conflicts: Optional[RemapConflictMap] = None,
    _path: Optional[Set[Tuple[str, str]]] = None
) -> Tuple[Optional[str], bool]:
    """
    基于引用该 SEQUENCE 的对象推导目标 schema。

    返回 (schema, conflict):
      - schema: 推导结果（仅 schema）
      - conflict: 若引用对象 remap 到多个 schema，标记冲突，避免继续回退推导
    """
    if '.' not in seq_full or not source_dependencies:
        return None, False

    seq_full_u = seq_full.upper()
    dependents: List[Tuple[str, str, str]] = []
    for dep_owner, dep_name, dep_type, ref_owner, ref_name, ref_type in source_dependencies:
        if (ref_type or "").upper() != 'SEQUENCE':
            continue
        ref_full = f"{ref_owner}.{ref_name}".upper()
        if ref_full != seq_full_u:
            continue
        dep_full = f"{dep_owner}.{dep_name}".upper()
        dep_type_u = (dep_type or "").upper()
        if dep_full and dep_type_u:
            dependents.append((dep_full, dep_type_u, dep_owner))

    if not dependents:
        return None, False

    remapped_targets: Set[str] = set()
    for dep_full, dep_type_u, dep_owner in dependents:
        if dep_type_u == 'TRIGGER' and object_parent_map:
            parent_table = object_parent_map.get(dep_full.upper())
            if parent_table:
                dep_target = resolve_remap_target(
                    parent_table,
                    'TABLE',
                    remap_rules,
                    source_objects=source_objects,
                    schema_mapping=schema_mapping,
                    object_parent_map=object_parent_map,
                    dependency_graph=dependency_graph,
                    transitive_table_cache=transitive_table_cache,
                    source_dependencies=source_dependencies,
                    remap_conflicts=remap_conflicts,
                    _path=_path
                ) or parent_table
            else:
                dep_target = dep_full
        else:
            dep_target = resolve_remap_target(
                dep_full,
                dep_type_u,
                remap_rules,
                source_objects=source_objects,
                schema_mapping=schema_mapping,
                object_parent_map=object_parent_map,
                dependency_graph=dependency_graph,
                transitive_table_cache=transitive_table_cache,
                source_dependencies=source_dependencies,
                remap_conflicts=remap_conflicts,
                _path=_path
            ) or dep_full
        if dep_target is None:
            if remap_conflicts and (dep_full.upper(), dep_type_u) in remap_conflicts:
                return None, True
            dep_target = dep_full
        if '.' not in dep_target:
            continue
        tgt_schema = dep_target.split('.', 1)[0].upper()
        if tgt_schema != dep_owner.upper():
            remapped_targets.add(tgt_schema)

    if remapped_targets:
        if len(remapped_targets) == 1:
            inferred_schema = next(iter(remapped_targets))
            log.debug(
                "[推导] SEQUENCE %s 被 remap 对象引用，推导目标 schema: %s",
                seq_full_u, inferred_schema
            )
            return inferred_schema, False
        log.warning(
            "[推导] SEQUENCE %s 被多个 remap schema 引用 (%s)，无法自动推导，请显式配置 remap。",
            seq_full_u, sorted(remapped_targets)
        )
        return None, True

    return None, False


def infer_target_schema_from_direct_dependencies(
    src_name: str,
    obj_type: str,
    remap_rules: RemapRules,
    source_objects: Optional[SourceObjectMap] = None,
    schema_mapping: Optional[Dict[str, str]] = None,
    object_parent_map: Optional[ObjectParentMap] = None,
    dependency_graph: Optional[DependencyGraph] = None,
    transitive_table_cache: Optional[TransitiveTableCache] = None,
    source_dependencies: Optional[SourceDependencySet] = None,
    remap_conflicts: Optional[RemapConflictMap] = None,
    *,
    ignore_public_synonyms: bool = True,
    _path: Optional[Set[Tuple[str, str]]] = None
) -> Tuple[Optional[str], bool]:
    """
    基于对象的“直接引用”推导目标 schema（不限于 TABLE/MVIEW）。

    Returns:
        (schema, conflict)
        - schema: 唯一目标 schema
        - conflict: 若引用对象映射到多个 schema，标记冲突
    """
    if not source_dependencies or '.' not in src_name:
        return None, False

    src_name_u = src_name.upper()
    obj_type_u = (obj_type or "").upper()
    candidate_types = {obj_type_u}
    if obj_type_u in ('PACKAGE BODY', 'TYPE BODY'):
        candidate_types.add(obj_type_u.replace(' BODY', ''))

    remapped_targets: Set[str] = set()
    for dep_owner, dep_name, dep_type, ref_owner, ref_name, ref_type in source_dependencies:
        dep_full = f"{dep_owner}.{dep_name}".upper()
        dep_type_u = (dep_type or "").upper()
        if dep_full != src_name_u or dep_type_u not in candidate_types:
            continue
        ref_owner_u = (ref_owner or "").upper()
        ref_type_u = (ref_type or "").upper()
        if ignore_public_synonyms and ref_type_u == 'SYNONYM' and ref_owner_u == 'PUBLIC':
            continue
        ref_full = f"{ref_owner}.{ref_name}".upper()
        ref_target = resolve_remap_target(
            ref_full,
            ref_type_u,
            remap_rules,
            source_objects=source_objects,
            schema_mapping=schema_mapping,
            object_parent_map=object_parent_map,
            dependency_graph=dependency_graph,
            transitive_table_cache=transitive_table_cache,
            source_dependencies=source_dependencies,
            remap_conflicts=remap_conflicts,
            _path=_path
        )
        if ref_target is None:
            if remap_conflicts and (ref_full.upper(), ref_type_u) in remap_conflicts:
                return None, True
            ref_target = ref_full
        if '.' not in ref_target:
            continue
        tgt_schema = ref_target.split('.', 1)[0].upper()
        if tgt_schema != ref_owner_u:
            remapped_targets.add(tgt_schema)

    if remapped_targets:
        if len(remapped_targets) == 1:
            inferred_schema = next(iter(remapped_targets))
            log.debug(
                "[推导] %s (%s) 直接引用对象映射到 %s -> 推导目标 schema: %s",
                src_name_u, obj_type_u, inferred_schema, inferred_schema
            )
            return inferred_schema, False
        log.warning(
            "[推导] %s (%s) 直接引用对象映射到多个 schema (%s)，无法自动推导，请显式配置 remap。",
            src_name_u, obj_type_u, sorted(remapped_targets)
        )
        return None, True

    return None, False


def build_dependency_graph(source_dependencies: Optional[SourceDependencySet]) -> DependencyGraph:
    """
    将源端依赖集合构建为依赖图：
      (DEP_FULL, DEP_TYPE) -> {(REF_FULL, REF_TYPE), ...}
    """
    graph: Dict[DependencyNode, Set[DependencyNode]] = defaultdict(set)
    if not source_dependencies:
        return {}
    for dep_owner, dep_name, dep_type, ref_owner, ref_name, ref_type in source_dependencies:
        dep_full = f"{dep_owner}.{dep_name}".upper()
        ref_full = f"{ref_owner}.{ref_name}".upper()
        dep_type_u = (dep_type or "").upper()
        ref_type_u = (ref_type or "").upper()
        if not dep_full or not ref_full or not dep_type_u or not ref_type_u:
            continue
        graph[(dep_full, dep_type_u)].add((ref_full, ref_type_u))
    return dict(graph)


def precompute_transitive_table_cache(
    dependency_graph: DependencyGraph,
    *,
    object_parent_map: Optional[ObjectParentMap] = None
) -> TransitiveTableCache:
    """
    基于依赖图一次性预计算所有节点“最终引用的 TABLE/MVIEW 集合”，用于一对多 remap 推导。

    使用自底向上的单调传播（反向边 + 队列），避免每个对象重复 DFS：
      transitive(node) = direct_tables(node) ∪ ⋃ transitive(child)

    返回:
      {(OWNER.OBJ, TYPE): {TABLE_OWNER.TABLE, ...}, ...}
    """
    if not dependency_graph:
        return {}

    from collections import deque

    reverse_graph: Dict[DependencyNode, Set[DependencyNode]] = defaultdict(set)
    transitive: Dict[DependencyNode, Set[str]] = defaultdict(set)
    full_to_nodes: Dict[str, List[DependencyNode]] = defaultdict(list)

    # 先收集所有节点（包括仅被引用的节点），构建反向图
    for dep_node, refs in dependency_graph.items():
        full_to_nodes[dep_node[0].upper()].append(dep_node)
        if dep_node not in transitive:
            transitive[dep_node] = set()
        for ref_node in refs:
            reverse_graph[ref_node].add(dep_node)
            full_to_nodes[ref_node[0].upper()].append(ref_node)
            if ref_node not in transitive:
                transitive[ref_node] = set()

    # 直接引用的 TABLE/MVIEW
    for dep_node, refs in dependency_graph.items():
        for ref_full, ref_type in refs:
            ref_type_u = (ref_type or "").upper()
            if ref_type_u in ("TABLE", "MATERIALIZED VIEW"):
                transitive[dep_node].add(ref_full.upper())

    # 依附对象的父表直接视为引用
    if object_parent_map:
        for dep_full, parent_full in object_parent_map.items():
            if not parent_full or "." not in parent_full:
                continue
            parent_full_u = parent_full.upper()
            for node in full_to_nodes.get(dep_full.upper(), []):
                transitive[node].add(parent_full_u)

    # 队列传播（只在新增时继续向上游推送）
    queue = deque([n for n, tbls in transitive.items() if tbls])
    while queue:
        node = queue.popleft()
        tables_here = transitive.get(node)
        if not tables_here:
            continue
        for dep_node in reverse_graph.get(node, set()):
            existing = transitive[dep_node]
            new_tables = tables_here - existing
            if not new_tables:
                continue
            existing.update(new_tables)
            queue.append(dep_node)

    return dict(transitive)


def collect_transitive_referenced_tables(
    start_full: str,
    start_type: str,
    dependency_graph: DependencyGraph,
    *,
    object_parent_map: Optional[ObjectParentMap] = None
) -> Set[str]:
    """
    沿依赖图递归下探，收集 start 对象最终引用的 TABLE/MVIEW。
    对于 SYNONYM/TRIGGER 等依附对象，会额外通过 object_parent_map 下探到父表。
    返回集合元素为 OWNER.OBJ（大写）。
    """
    if not dependency_graph or '.' not in start_full:
        return set()

    start_node: DependencyNode = (start_full.upper(), (start_type or "").upper())
    visited: Set[DependencyNode] = set()
    tables: Set[str] = set()
    stack: List[DependencyNode] = [start_node]

    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        full_name, obj_type_u = node

        if object_parent_map:
            parent = object_parent_map.get(full_name.upper())
            if parent and '.' in parent:
                tables.add(parent.upper())

        for ref_full, ref_type_u in dependency_graph.get(node, set()):
            ref_full_u = ref_full.upper()
            ref_type_upper = ref_type_u.upper()
            if ref_type_upper in ('TABLE', 'MATERIALIZED VIEW'):
                tables.add(ref_full_u)
                continue
            stack.append((ref_full_u, ref_type_upper))

    return tables


def infer_target_schema_from_dependencies(
    src_name: str,
    obj_type: str,
    remap_rules: RemapRules,
    dependency_graph: Optional[DependencyGraph] = None,
    object_parent_map: Optional[ObjectParentMap] = None,
    transitive_table_cache: Optional[TransitiveTableCache] = None
) -> Tuple[Optional[str], bool]:
    """
    基于对象的依赖关系递归推导目标 schema（用于 one-to-many 场景）。

    逻辑：
    1. 沿依赖图递归下探，收集该对象（含嵌套依赖）最终引用的 TABLE/MVIEW
    2. 对每张表取 remap 目标（未显式 remap 的表视为 1:1）
    3. 统计目标 schema 出现次数，唯一最多者作为推导结果

    Returns:
        (target_full, conflict)
        - target_full: 推导出的目标全名 "TGT_SCHEMA.OBJ"；无法推导则返回 None
        - conflict: 是否发生多 schema 冲突
    """
    if '.' not in src_name or not dependency_graph:
        return None, False

    src_name_u = src_name.upper()
    src_schema, src_obj = src_name_u.split('.', 1)

    node: DependencyNode = (src_name_u, (obj_type or "").upper())
    referenced_tables: Set[str] = set()
    if transitive_table_cache is not None:
        cached = transitive_table_cache.get(node)
        if cached:
            referenced_tables = set(cached)
    if not referenced_tables:
        referenced_tables = collect_transitive_referenced_tables(
            src_name_u,
            obj_type,
            dependency_graph,
            object_parent_map=object_parent_map
        )
    if not referenced_tables:
        return None, False

    target_schema_counts: Dict[str, int] = defaultdict(int)
    for table_full in referenced_tables:
        table_full_u = table_full.upper()
        table_target = remap_rules.get(table_full_u) or table_full_u
        if '.' in table_target:
            tgt_schema = table_target.split('.', 1)[0].upper()
        else:
            tgt_schema = table_full_u.split('.', 1)[0].upper()
        target_schema_counts[tgt_schema] += 1

    if not target_schema_counts:
        return None, False

    max_count = max(target_schema_counts.values())
    candidate_schemas = [s for s, c in target_schema_counts.items() if c == max_count]

    if len(candidate_schemas) == 1:
        inferred_schema = candidate_schemas[0]
        log.debug(
            "[推导] %s (%s) 递归引用 %d 个表/MVIEW，其中 %d 个在 %s -> 推导目标: %s.%s",
            src_name_u, obj_type, len(referenced_tables), max_count, inferred_schema, inferred_schema, src_obj
        )
        return f"{inferred_schema}.{src_obj}", False

    log.debug(
        "[推导] %s (%s) 引用的表/MVIEW 分散在多个 schema，无法推导: %s",
        src_name_u, obj_type, candidate_schemas
    )
    return None, True


def resolve_remap_target(
    src_name: str,
    obj_type: str,
    remap_rules: RemapRules,
    source_objects: Optional[SourceObjectMap] = None,
    schema_mapping: Optional[Dict[str, str]] = None,
    object_parent_map: Optional[ObjectParentMap] = None,
    dependency_graph: Optional[DependencyGraph] = None,
    transitive_table_cache: Optional[TransitiveTableCache] = None,
    source_dependencies: Optional[SourceDependencySet] = None,
    remap_conflicts: Optional[RemapConflictMap] = None,
    _path: Optional[Set[Tuple[str, str]]] = None
) -> Optional[str]:
    """
    解析对象的 remap 目标：
    1. 优先查找 remap_rules 中的显式规则
    2. 对于依附对象（INDEX/CONSTRAINT/SEQUENCE/SYNONYM），使用父表的 remap 目标 schema
    3. 对于独立对象，尝试基于依赖分析推导（一对多场景）
    4. 对于其他非 TABLE 对象，尝试使用 schema_mapping（多对一、一对一场景）
    
    object_parent_map: 依附对象到父表的映射
    source_dependencies: 源端依赖关系，用于智能推导
    """
    obj_type_u = obj_type.upper()
    src_name_u = src_name.upper()
    path = _path if _path is not None else set()
    node = (src_name_u, obj_type_u)
    if node in path:
        return None
    path.add(node)
    def _record_conflict(reason: str) -> None:
        if remap_conflicts is None:
            return
        key = (src_name_u, obj_type_u)
        if key not in remap_conflicts:
            remap_conflicts[key] = reason

    try:
        candidate_keys: List[str] = [src_name]
        if obj_type_u in ('PACKAGE BODY', 'TYPE BODY'):
            candidate_keys.insert(0, f"{src_name} BODY")
        for key in candidate_keys:
            if key in remap_rules:
                tgt = remap_rules[key].strip()
                if obj_type_u in ('PACKAGE BODY', 'TYPE BODY'):
                    return strip_body_suffix(tgt)
                return tgt

        if obj_type_u in NO_INFER_SCHEMA_TYPES:
            return src_name_u

        if obj_type_u == 'SYNONYM' and '.' in src_name:
            src_schema, src_obj = src_name.split('.', 1)
            if src_schema.upper() == 'PUBLIC':
                return f"PUBLIC.{src_obj.upper()}"

        if obj_type_u == 'SYNONYM' and '.' in src_name:
            src_schema, src_obj = src_name.split('.', 1)
            inferred_schema, conflict = infer_target_schema_from_direct_dependencies(
                src_name,
                obj_type,
                remap_rules,
                source_objects=source_objects,
                schema_mapping=schema_mapping,
                object_parent_map=object_parent_map,
                dependency_graph=dependency_graph,
                transitive_table_cache=transitive_table_cache,
                source_dependencies=source_dependencies,
                remap_conflicts=remap_conflicts,
                _path=path
            )
            if inferred_schema:
                return f"{inferred_schema}.{src_obj}"
            if conflict:
                _record_conflict("同义词直接依赖映射到多个 schema，无法自动推导")
                return None

        # 对于依附对象（INDEX/CONSTRAINT/SEQUENCE/SYNONYM），使用父表的 remap 目标 schema
        if '.' in src_name and object_parent_map:
            parent_table = object_parent_map.get(src_name.upper())
            if parent_table:
                parent_target = remap_rules.get(parent_table.upper())
                if not parent_target and schema_mapping and '.' in parent_table:
                    parent_schema, parent_obj = parent_table.split('.', 1)
                    mapped_schema = schema_mapping.get(parent_schema.upper())
                    if mapped_schema:
                        parent_target = f"{mapped_schema.upper()}.{parent_obj}"
                if not parent_target and obj_type_u == 'SYNONYM' and source_objects:
                    parent_types = {t.upper() for t in source_objects.get(parent_table.upper(), set())}
                    preferred_types = (
                        'TABLE', 'VIEW', 'MATERIALIZED VIEW', 'SEQUENCE',
                        'SYNONYM', 'FUNCTION', 'PROCEDURE', 'PACKAGE',
                        'TYPE', 'TRIGGER'
                    )
                    for parent_type in preferred_types:
                        if parent_type not in parent_types:
                            continue
                        parent_target = resolve_remap_target(
                            parent_table.upper(),
                            parent_type,
                            remap_rules,
                            source_objects=source_objects,
                            schema_mapping=schema_mapping,
                            object_parent_map=object_parent_map,
                            dependency_graph=dependency_graph,
                            transitive_table_cache=transitive_table_cache,
                            source_dependencies=source_dependencies,
                            _path=path
                        )
                        if parent_target:
                            break
                if parent_target:
                    tgt_schema = parent_target.split('.', 1)[0].upper()
                    src_obj = src_name.split('.', 1)[1]
                    return f"{tgt_schema}.{src_obj}"

        # 对于 SEQUENCE，优先根据依赖对象的 remap 结果推导
        if '.' in src_name and obj_type_u == 'SEQUENCE':
            src_schema, src_obj = src_name.split('.', 1)
            inferred_schema, conflict = infer_sequence_target_schema_from_dependents(
                src_name,
                remap_rules,
                source_objects=source_objects,
                schema_mapping=schema_mapping,
                object_parent_map=object_parent_map,
                dependency_graph=dependency_graph,
                transitive_table_cache=transitive_table_cache,
                source_dependencies=source_dependencies,
                remap_conflicts=remap_conflicts,
                _path=path
            )
            if inferred_schema:
                return f"{inferred_schema}.{src_obj}"
            if conflict:
                _record_conflict("SEQUENCE 被多个 remap schema 引用，无法自动推导")
                return None

        # 已处理 SEQUENCE 的依赖推导，后续不再进行通用直接依赖推导

        # 针对 SEQUENCE / SYNONYM，在 remap_rules 仅包含 TABLE 映射时，使用该 schema 的主流目标 schema
        if '.' in src_name and obj_type_u in ('SEQUENCE', 'SYNONYM'):
            src_schema, src_obj = src_name.split('.', 1)
            dominant_schema = infer_dominant_schema_from_rules(remap_rules, src_schema, source_objects)
            if dominant_schema:
                return f"{dominant_schema}.{src_obj}"

        # 对于独立对象，优先基于直接依赖对象的 remap 推导（不限于 TABLE/MVIEW）
        if '.' in src_name and obj_type_u not in ('TABLE', 'SEQUENCE', 'SYNONYM'):
            src_schema, src_obj = src_name.split('.', 1)
            inferred_schema, conflict = infer_target_schema_from_direct_dependencies(
                src_name,
                obj_type,
                remap_rules,
                source_objects=source_objects,
                schema_mapping=schema_mapping,
                object_parent_map=object_parent_map,
                dependency_graph=dependency_graph,
                transitive_table_cache=transitive_table_cache,
                source_dependencies=source_dependencies,
                remap_conflicts=remap_conflicts,
                _path=path
            )
            if inferred_schema:
                return f"{inferred_schema}.{src_obj}"
            if conflict:
                _record_conflict("直接依赖映射到多个 schema，无法自动推导")
                return None

        # 对于独立对象（VIEW/PROCEDURE/FUNCTION/PACKAGE等），尝试基于依赖分析递归推导
        if '.' in src_name and obj_type_u != 'TABLE':
            dep_graph = dependency_graph
            if dep_graph is None and source_dependencies:
                dep_graph = build_dependency_graph(source_dependencies)
            if dep_graph:
                inferred, conflict = infer_target_schema_from_dependencies(
                    src_name,
                    obj_type,
                    remap_rules,
                    dep_graph,
                    object_parent_map=object_parent_map,
                    transitive_table_cache=transitive_table_cache
                )
                if inferred:
                    return inferred
                if conflict:
                    _record_conflict("递归依赖映射到多个 schema，无法自动推导")
                    return None
            
            # 回退到schema映射推导（适用于多对一、一对一场景）
            src_schema, src_obj = src_name.split('.', 1)
            src_schema_u = src_schema.upper()
            
            if schema_mapping:
                tgt_schema = schema_mapping.get(src_schema_u)
                if tgt_schema:
                    return f"{tgt_schema}.{src_obj}"

        return None
    finally:
        path.remove(node)


def generate_master_list(
    source_objects: SourceObjectMap,
    remap_rules: RemapRules,
    enabled_primary_types: Optional[Set[str]] = None,
    schema_mapping: Optional[Dict[str, str]] = None,
    precomputed_mapping: Optional[FullObjectMapping] = None,
    object_parent_map: Optional[ObjectParentMap] = None,
    transitive_table_cache: Optional[TransitiveTableCache] = None,
    source_dependencies: Optional[SourceDependencySet] = None,
    dependency_graph: Optional[DependencyGraph] = None,
    remap_conflicts: Optional[RemapConflictMap] = None
) -> MasterCheckList:
    """
    生成“最终校验清单”并检测 "多对一" 映射。
    仅保留 PRIMARY_OBJECT_TYPES 中的主对象，用于主校验。
    """
    log.info("正在生成主校验清单 (应用 Remap 规则)...")
    master_list: MasterCheckList = []

    target_tracker: Dict[Tuple[str, str], str] = {}

    allowed_primary = enabled_primary_types or set(PRIMARY_OBJECT_TYPES)

    for src_name, obj_types in source_objects.items():
        src_name_u = src_name.upper()
        for obj_type in sorted(obj_types):
            obj_type_u = obj_type.upper()
            if obj_type_u not in allowed_primary:
                continue
            if remap_conflicts and (src_name_u, obj_type_u) in remap_conflicts:
                continue

            if precomputed_mapping and src_name_u in precomputed_mapping:
                tgt_name = precomputed_mapping[src_name_u].get(obj_type_u, src_name_u)
            else:
                tgt_name = resolve_remap_target(
                    src_name_u,
                    obj_type_u,
                    remap_rules,
                    source_objects=source_objects,
                    schema_mapping=schema_mapping,
                    object_parent_map=object_parent_map,
                    dependency_graph=dependency_graph,
                    transitive_table_cache=transitive_table_cache,
                    source_dependencies=source_dependencies,
                    remap_conflicts=remap_conflicts
                )
                if remap_conflicts and (src_name_u, obj_type_u) in remap_conflicts:
                    continue
                tgt_name = tgt_name or src_name_u
            tgt_name_u = tgt_name.upper()

            key = (tgt_name_u, obj_type_u)
            if key in target_tracker:
                existing_src = target_tracker[key]
                if existing_src != src_name_u:
                    log.warning(
                        "检测到多对一映射: 目标 %s (类型 %s) 已由 %s 映射，当前 %s 将回退为 1:1 映射。",
                        tgt_name_u, obj_type_u, existing_src, src_name_u
                    )
                    tgt_name_u = src_name_u
                    tgt_name = src_name_u
                    key = (tgt_name_u, obj_type_u)

            target_tracker[key] = src_name_u
            master_list.append((src_name_u, tgt_name_u, obj_type_u))

    log.info(f"主校验清单生成完毕，共 {len(master_list)} 个待校验项。")
    return master_list


def build_full_object_mapping(
    source_objects: SourceObjectMap,
    remap_rules: RemapRules,
    schema_mapping: Optional[Dict[str, str]] = None,
    object_parent_map: Optional[ObjectParentMap] = None,
    transitive_table_cache: Optional[TransitiveTableCache] = None,
    source_dependencies: Optional[SourceDependencySet] = None,
    dependency_graph: Optional[DependencyGraph] = None,
    enabled_types: Optional[Set[str]] = None,
    remap_conflicts: Optional[RemapConflictMap] = None
) -> FullObjectMapping:
    """
    为所有受管对象建立映射 (源 -> 目标)。
    返回 {'SRC.OBJ': {'TYPE': 'TGT.OBJ'}}
    
    object_parent_map: 依附对象到父表的映射，用于 one-to-many schema 拆分场景
    source_dependencies: 源端依赖关系，用于智能推导目标schema
    enabled_types: 若提供，仅处理这些对象类型
    """
    def _enforce_paired_objects_same_target(
        src_full: str,
        type_map: Dict[str, str]
    ) -> None:
        """
        PACKAGE/PACKAGE BODY、TYPE/TYPE BODY 必须保持同 schema 同名。
        若推导结果不一致：
          - 若 PACKAGE/TYPE 有显式 remap 规则，优先显式规则；
          - 否则若 BODY 有显式规则，优先 BODY；
          - 否则优先 BODY 的推导（通常更依赖真实表）。
        """

        def _fix_pair(primary: str, body: str) -> None:
            if primary not in type_map or body not in type_map:
                return
            prim_tgt = type_map.get(primary)
            body_tgt = type_map.get(body)
            if not prim_tgt or not body_tgt or prim_tgt.upper() == body_tgt.upper():
                return

            explicit_primary = src_full in remap_rules
            explicit_body = explicit_primary or f"{src_full} BODY" in remap_rules

            if explicit_primary:
                chosen = prim_tgt
            elif explicit_body:
                chosen = body_tgt
            else:
                chosen = body_tgt or prim_tgt

            log.warning(
                "检测到 %s/%s 推导目标不一致 (%s vs %s)，已强制统一为 %s。",
                primary, body, prim_tgt, body_tgt, chosen
            )
            type_map[primary] = chosen.upper()
            type_map[body] = chosen.upper()

        _fix_pair("PACKAGE", "PACKAGE BODY")
        _fix_pair("TYPE", "TYPE BODY")

    mapping: FullObjectMapping = {}
    target_tracker: Dict[Tuple[str, str], str] = {}

    enabled_types_u = {t.upper() for t in enabled_types} if enabled_types else None
    conflict_map = remap_conflicts

    for src_name, obj_types in source_objects.items():
        src_name_u = src_name.upper()
        local_map: Dict[str, str] = {}

        # 先为该对象的所有类型计算目标
        for obj_type in sorted(obj_types):
            obj_type_u = obj_type.upper()
            if enabled_types_u and obj_type_u not in enabled_types_u:
                continue
            tgt_name = resolve_remap_target(
                src_name_u,
                obj_type_u,
                remap_rules,
                source_objects=source_objects,
                schema_mapping=schema_mapping,
                object_parent_map=object_parent_map,
                dependency_graph=dependency_graph,
                transitive_table_cache=transitive_table_cache,
                source_dependencies=source_dependencies,
                remap_conflicts=conflict_map
            )
            if tgt_name is None:
                if conflict_map and (src_name_u, obj_type_u) in conflict_map:
                    continue
                tgt_name = src_name_u
            local_map[obj_type_u] = tgt_name.upper()

        if not local_map:
            continue

        # 强制配对对象统一目标
        _enforce_paired_objects_same_target(src_name_u, local_map)

        # 多对一冲突检测：配对对象需整体回退
        paired_groups = [("PACKAGE", "PACKAGE BODY"), ("TYPE", "TYPE BODY")]
        paired_handled: Set[str] = set()
        for primary, body in paired_groups:
            if primary not in local_map and body not in local_map:
                continue
            paired_handled.update([primary, body])
            tgt = local_map.get(primary) or local_map.get(body) or src_name_u

            conflict_src: Optional[str] = None
            for t in (primary, body):
                if t not in local_map:
                    continue
                key = (tgt, t)
                existing_src = target_tracker.get(key)
                if existing_src and existing_src != src_name_u:
                    conflict_src = existing_src
                    break

            if conflict_src:
                log.warning(
                    "检测到多对一映射: 目标 %s (类型 %s/%s) 已由 %s 映射，当前 %s 整体回退为 1:1。",
                    tgt, primary, body, conflict_src, src_name_u
                )
                tgt = src_name_u
                for t in (primary, body):
                    if t in local_map:
                        local_map[t] = tgt

            for t in (primary, body):
                if t in local_map:
                    target_tracker[(local_map[t], t)] = src_name_u

        # 其余类型按单独规则检测
        for obj_type_u, tgt_name_u in local_map.items():
            if obj_type_u in paired_handled:
                continue
            key = (tgt_name_u, obj_type_u)
            existing_src = target_tracker.get(key)
            if existing_src and existing_src != src_name_u:
                log.warning(
                    "检测到多对一映射: 目标 %s (类型 %s) 已由 %s 映射，当前 %s 回退为 1:1 映射。",
                    tgt_name_u, obj_type_u, existing_src, src_name_u
                )
                tgt_name_u = src_name_u
                local_map[obj_type_u] = tgt_name_u
            target_tracker[key] = src_name_u

        mapping[src_name_u] = local_map

    return mapping


def get_mapped_target(
    full_object_mapping: FullObjectMapping,
    src_full_name: str,
    obj_type: str
) -> Optional[str]:
    src_key = src_full_name.upper()
    obj_type_u = obj_type.upper()
    type_map = full_object_mapping.get(src_key)
    if not type_map:
        return None
    return type_map.get(obj_type_u)


def find_mapped_target_any_type(
    full_object_mapping: FullObjectMapping,
    src_full_name: str,
    preferred_types: Optional[Tuple[str, ...]] = None
) -> Optional[str]:
    """
    在不知道对象类型的情况下，根据可选的类型优先级查找映射的目标名。
    先按 preferred_types 顺序查找，找不到再回退到该源对象的任意映射值。
    """
    preferred_types = preferred_types or ()
    src_key = src_full_name.upper()
    for obj_type in preferred_types:
        mapped = get_mapped_target(full_object_mapping, src_key, obj_type)
        if mapped:
            return mapped
    type_map = full_object_mapping.get(src_key)
    if not type_map:
        return None
    # 回退时取确定性的最小值，避免 set/插入顺序导致的随机性
    return sorted(type_map.values())[0] if type_map else None


def ensure_mapping_entry(
    full_object_mapping: FullObjectMapping,
    src_full_name: str,
    obj_type: str,
    tgt_full_name: str
) -> None:
    src_key = src_full_name.upper()
    obj_type_u = obj_type.upper()
    tgt_full = tgt_full_name.upper()
    full_object_mapping.setdefault(src_key, {})[obj_type_u] = tgt_full


def find_source_by_target(
    full_object_mapping: FullObjectMapping,
    tgt_full_name: str,
    obj_type: str
) -> Optional[str]:
    obj_type_u = obj_type.upper()
    tgt_u = tgt_full_name.upper()
    for src_name, type_map in full_object_mapping.items():
        target = type_map.get(obj_type_u)
        if target and target.upper() == tgt_u:
            return src_name
    return None


def collect_table_pairs(master_list: MasterCheckList, use_target: bool = False) -> Set[Tuple[str, str]]:
    """
    提取 master_list 中的 (schema, table) 集合。
    use_target=True 时基于目标端表名，否则使用源端。
    """
    pairs: Set[Tuple[str, str]] = set()
    for src_name, tgt_name, obj_type in master_list:
        if obj_type.upper() != 'TABLE':
            continue
        name = tgt_name if use_target else src_name
        if '.' not in name:
            continue
        schema, table = name.split('.', 1)
        pairs.add((schema.upper(), table.upper()))
    return pairs


def build_table_target_map(master_list: MasterCheckList) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """
    基于 master_list 构造源表 -> 目标表映射。
    """
    mapping: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for src_name, tgt_name, obj_type in master_list:
        if obj_type.upper() != 'TABLE':
            continue
        src_key = parse_full_object_name(src_name)
        tgt_key = parse_full_object_name(tgt_name)
        if src_key and tgt_key:
            mapping[src_key] = tgt_key
    return mapping


def build_schema_mapping(master_list: MasterCheckList) -> Dict[str, str]:
    """
    基于 master_list 中 TABLE 映射，推导 schema 映射：
      如果同一 src_schema 只映射到唯一一个 tgt_schema，则使用该映射；
      否则 (映射到多个目标 schema)，退回 src_schema 本身 (1:1)。
    """
    mapping_tmp: Dict[str, Set[str]] = {}
    for src_name, tgt_name, obj_type in master_list:
        if obj_type.upper() != 'TABLE':
            continue
        try:
            src_schema, _ = src_name.split('.', 1)
            tgt_schema, _ = tgt_name.split('.', 1)
        except ValueError:
            continue
        mapping_tmp.setdefault(src_schema.upper(), set()).add(tgt_schema.upper())

    final_mapping: Dict[str, str] = {}
    one_to_many_schemas: List[Tuple[str, Set[str]]] = []
    
    for src_schema, tgt_set in mapping_tmp.items():
        if len(tgt_set) == 1:
            final_mapping[src_schema] = next(iter(tgt_set))
        else:
            # 一对多映射：源schema的表分散到多个目标schema
            final_mapping[src_schema] = src_schema
            one_to_many_schemas.append((src_schema, tgt_set))
    
    # 输出schema映射摘要
    if final_mapping:
        log.info("Schema映射推导完成，共 %d 个源schema:", len(final_mapping))
        for src_s, tgt_s in sorted(final_mapping.items()):
            if src_s == tgt_s:
                log.info("  %s -> %s (1:1或一对多场景)", src_s, tgt_s)
            else:
                log.info("  %s -> %s", src_s, tgt_s)
    
    # 提示：一对多场景下的推导策略
    if one_to_many_schemas:
        log_subsection("一对多 schema 映射场景")
        log.info("检测到一对多 schema 映射场景（源schema的表分散到多个目标schema）：")
        for src_schema, tgt_set in one_to_many_schemas:
            log.info("  %s -> %s", src_schema, sorted(tgt_set))
        log.info("推导策略：")
        log.info("  1. 独立对象（VIEW/PROCEDURE/FUNCTION/PACKAGE等）：")
        log.info("     - 优先通过依赖分析推导（分析对象引用的表，选择出现最多的目标schema）")
        log.info("     - 如果依赖推导失败，需要在 remap_rules.txt 中显式指定")
        log.info("  2. 依附对象（INDEX/CONSTRAINT/SEQUENCE）：")
        log.info("     - 自动跟随父表的 schema，无需显式指定")
        log.info("  3. TRIGGER：")
        log.info("     - 默认保持源 schema，除非在 remap_rules.txt 中显式指定")

    return final_mapping


def compute_schema_coverage(
    configured_source_schemas: List[str],
    source_objects: SourceObjectMap,
    expected_target_schemas: Set[str],
    ob_meta: ObMetadata
) -> Dict[str, List[str]]:
    """
    计算 schema 层面的覆盖情况：
      - 源端配置了但未在元数据中找到对象的 schema
      说明：目标端可能是“超集”，因此不检查“额外 schema”或“目标缺失 schema”。
    """
    cfg_src_set = {s.upper() for s in configured_source_schemas}
    src_seen = {name.split('.', 1)[0].upper() for name in source_objects.keys() if '.' in name}
    source_missing = sorted(cfg_src_set - src_seen)

    expected_tgt_set = {s.upper() for s in expected_target_schemas}
    tgt_seen: Set[str] = set()
    for type_set in ob_meta.objects_by_type.values():
        for full_name in type_set:
            if '.' in full_name:
                tgt_seen.add(full_name.split('.', 1)[0].upper())

    target_missing = sorted(expected_tgt_set - tgt_seen)
    target_extra = sorted(tgt_seen - expected_tgt_set)
    hints: List[str] = []
    if target_missing:
        hints.append(
            f"目标端缺失 schema: {', '.join(target_missing)}（可能未创建或权限不足）"
        )

    return {
        "source_missing": source_missing,
        "target_missing": target_missing,
        "target_extra": target_extra,
        "target_missing_schema_hint": hints
    }


def compute_object_counts(
    full_object_mapping: FullObjectMapping,
    ob_meta: ObMetadata,
    oracle_meta: OracleMetadata,
    monitored_types: Tuple[str, ...] = OBJECT_COUNT_TYPES
) -> ObjectCountSummary:
    """
    基于“期望对象集合”统计各类型的：源端数量、目标端命中数量、缺失数量、额外数量。
    目标端数量仅统计“期望对象”的命中数，避免“缺 1 张表 + 额外 1 张表”被误判为数量一致。
    """
    expected_by_type: Dict[str, Set[str]] = {t.upper(): set() for t in monitored_types}
    for _src, type_map in full_object_mapping.items():
        for obj_type, tgt_name in type_map.items():
            obj_type_u = obj_type.upper()
            if obj_type_u not in expected_by_type:
                continue
            expected_by_type[obj_type_u].add(tgt_name.upper())

    invalid_targets_by_type: Dict[str, Set[str]] = defaultdict(set)
    for src_full, type_map in full_object_mapping.items():
        if "." not in src_full:
            continue
        src_schema, src_obj = src_full.split(".", 1)
        src_schema_u = src_schema.upper()
        src_obj_u = src_obj.upper()
        for obj_type in PACKAGE_OBJECT_TYPES:
            tgt_name = type_map.get(obj_type)
            if not tgt_name:
                continue
            status = oracle_meta.object_statuses.get((src_schema_u, src_obj_u, obj_type))
            if normalize_object_status(status) == "INVALID":
                invalid_targets_by_type[obj_type].add(tgt_name.upper())

    actual_by_type: Dict[str, Set[str]] = {
        t.upper(): {name.upper() for name in ob_meta.objects_by_type.get(t.upper(), set())}
        for t in monitored_types if t != 'CONSTRAINT'
    }

    summary: ObjectCountSummary = {
        "oracle": {},
        "oceanbase": {},
        "missing": {},
        "extra": {}
    }

    issue_types: List[str] = []
    for obj_type in monitored_types:
        obj_type_u = obj_type.upper()
        
        if obj_type_u == 'CONSTRAINT':
            # Constraints are not in DBA_OBJECTS, so they need special handling
            expected_set = {
                cons_name
                for cons_map in oracle_meta.constraints.values()
                for cons_name in cons_map
            }
            actual_set = {
                cons_name
                for cons_map in ob_meta.constraints.values()
                for cons_name in cons_map
            }
        else:
            expected_set = expected_by_type.get(obj_type_u, set())
            if obj_type_u in invalid_targets_by_type:
                expected_set = expected_set - invalid_targets_by_type[obj_type_u]
            actual_set = actual_by_type.get(obj_type_u, set())

        # For constraints and indexes, names can be system-generated. A simple name comparison is not enough.
        # This count is a rough estimation. The detailed mismatch is more important.
        # Here we count based on what's found in meta, not remapped names, for simplicity.
        if obj_type_u in ('CONSTRAINT', 'INDEX'):
            if obj_type_u == 'CONSTRAINT':
                def _count_pkukfk(cons_maps: Dict[Tuple[str, str], Dict[str, Dict]]) -> int:
                    cnt = 0
                    for cons_map in cons_maps.values():
                        for info in cons_map.values():
                            ctype = (info.get("type") or "").upper()
                            if ctype in ('P', 'U', 'R'):
                                cnt += 1
                    return cnt

                src_count = _count_pkukfk(oracle_meta.constraints)
                tgt_count = _count_pkukfk(ob_meta.constraints)
            else: # INDEX
                src_count = sum(len(v) for v in oracle_meta.indexes.values())
                tgt_count = sum(len(v) for v in ob_meta.indexes.values())
            
            summary["oracle"][obj_type_u] = src_count
            summary["oceanbase"][obj_type_u] = tgt_count
            summary["missing"][obj_type_u] = max(0, src_count - tgt_count)
            summary["extra"][obj_type_u] = max(0, tgt_count - src_count)
            if src_count != tgt_count:
                issue_types.append(obj_type_u)
            continue

        matched = expected_set & actual_set
        missing_set = expected_set - actual_set
        extra_set = actual_set - expected_set

        summary["oracle"][obj_type_u] = len(expected_set)
        summary["oceanbase"][obj_type_u] = len(matched)
        summary["missing"][obj_type_u] = len(missing_set)
        summary["extra"][obj_type_u] = len(extra_set)

        if missing_set or extra_set:
            issue_types.append(obj_type_u)

    if issue_types:
        log.warning(
            "检查汇总: 基于 remap 后的期望集合，以下类型存在缺失或多余对象: %s",
            ", ".join(issue_types)
        )
    else:
        log.info("检查汇总: 所有关注对象类型的数量与期望一致（不计入额外对象）。")

    return summary




# ====================== obclient + 一次性元数据转储 ======================

def obclient_run_sql(ob_cfg: ObConfig, sql_query: str) -> Tuple[bool, str, str]:
    """运行 obclient CLI 命令并返回 (Success, stdout, stderr)，带 timeout。"""
    command_args = [
        ob_cfg['executable'],
        '-h', ob_cfg['host'],
        '-P', ob_cfg['port'],
        '-u', ob_cfg['user_string'],
        '-p' + ob_cfg['password'],
        '-ss',  # Silent 模式
        '-e', sql_query
    ]

    try:
        result = subprocess.run(
            command_args,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore',
            timeout=OBC_TIMEOUT
        )

        if result.returncode != 0 or (result.stderr and "Warning" not in result.stderr):
            log.error(f"  [OBClient 错误] SQL: {sql_query.strip()} | 错误: {result.stderr.strip()}")
            return False, "", result.stderr.strip()

        return True, result.stdout.strip(), ""

    except subprocess.TimeoutExpired:
        log.error(f"严重错误: obclient 执行超时 (>{OBC_TIMEOUT} 秒)。请检查网络/OB 状态或调大 obclient_timeout。")
        return False, "", "TimeoutExpired"
    except FileNotFoundError:
        log.error(f"严重错误: 未找到 obclient 可执行文件: {ob_cfg['executable']}")
        log.error("请检查 config.ini 中的 [OCEANBASE_TARGET] -> executable 路径。")
        sys.exit(1)
    except Exception as e:
        log.error(f"严重错误: 执行 subprocess 时发生未知错误: {e}")
        return False, "", str(e)


def dump_ob_metadata(
    ob_cfg: ObConfig,
    target_schemas: Set[str],
    tracked_object_types: Optional[Set[str]] = None,
    include_tab_columns: bool = True,
    include_indexes: bool = True,
    include_constraints: bool = True,
    include_triggers: bool = True,
    include_sequences: bool = True,
    include_comments: bool = True,
    include_roles: bool = False,
    target_table_pairs: Optional[Set[Tuple[str, str]]] = None
) -> ObMetadata:
    """
    一次性从 OceanBase dump 所有需要的元数据，返回 ObMetadata。
    如果任何关键视图查询失败，则视为致命错误并退出。
    """
    if not target_schemas:
        log.warning("目标 schema 集合为空，OB 元数据转储将返回空结构。")
        return ObMetadata(
            objects_by_type={},
            tab_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=False,
            object_statuses={},
            package_errors={},
            package_errors_complete=False
        )

    owners_in_list = sorted(target_schemas)
    owners_in = ",".join(f"'{s}'" for s in owners_in_list)
    owners_in_objects_list = list(owners_in_list)
    if 'PUBLIC' in target_schemas and '__PUBLIC' not in owners_in_objects_list:
        owners_in_objects_list.append('__PUBLIC')
    owners_in_objects = ",".join(f"'{s}'" for s in sorted(owners_in_objects_list))

    # --- 1. DBA_OBJECTS ---
    objects_by_type: Dict[str, Set[str]] = {}
    object_statuses: Dict[Tuple[str, str, str], str] = {}
    object_types_filter = tracked_object_types or set(ALL_TRACKED_OBJECT_TYPES)
    if not object_types_filter:
        object_types_filter = {'TABLE'}
    object_types_clause = ",".join(f"'{obj}'" for obj in sorted(object_types_filter))

    sql = f"""
        SELECT OWNER, OBJECT_NAME, OBJECT_TYPE, STATUS
        FROM DBA_OBJECTS
        WHERE UPPER(OWNER) IN ({owners_in_objects})
          AND OBJECT_TYPE IN (
              {object_types_clause}
          )
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.error("无法从 OB 读取 DBA_OBJECTS，程序退出。")
        sys.exit(1)

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 3:
                continue
            owner = parts[0].strip().upper()
            name = parts[1].strip().upper()
            obj_type = parts[2].strip().upper()
            status = parts[3].strip().upper() if len(parts) > 3 else "UNKNOWN"
            if obj_type == 'SYNONYM' and owner == '__PUBLIC':
                owner = 'PUBLIC'
            full = f"{owner}.{name}"
            objects_by_type.setdefault(obj_type, set()).add(full)
            object_statuses[(owner, name, obj_type)] = status or "UNKNOWN"

    # 补充 DBA_TYPES (部分 OB 环境中 TYPE 不出现在 DBA_OBJECTS)
    # 注意：DBA_TYPES.TYPECODE=OBJECT 仅表示对象类型，本身不代表存在 TYPE BODY，
    # 过去直接据此推断 TYPE BODY 会导致误报，因此这里只补 TYPE。
    if 'TYPE' in object_types_filter or 'TYPE BODY' in object_types_filter:
        sql_types = f"""
            SELECT OWNER, TYPE_NAME, TYPECODE
            FROM DBA_TYPES
            WHERE OWNER IN ({owners_in})
        """
        ok, out, err = obclient_run_sql(ob_cfg, sql_types)
        if not ok:
            log.warning("读取 DBA_TYPES 失败，TYPE 检查可能不完整: %s", err)
        elif out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 2:
                    continue
                owner, name = parts[0].strip().upper(), parts[1].strip().upper()
                full = f"{owner}.{name}"
                objects_by_type.setdefault('TYPE', set()).add(full)

    # 仅在显式启用 TYPE BODY 检查时，通过 DBA_SOURCE 探测真实 TYPE BODY
    if 'TYPE BODY' in object_types_filter:
        sql_type_body = f"""
            SELECT OWNER, NAME
            FROM DBA_SOURCE
            WHERE OWNER IN ({owners_in})
              AND TYPE = 'TYPE BODY'
            GROUP BY OWNER, NAME
        """
        ok, out, err = obclient_run_sql(ob_cfg, sql_type_body)
        if not ok:
            log.warning("读取 DBA_SOURCE(TYPE BODY) 失败，将不补充 TYPE BODY 元数据: %s", err)
        elif out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 2:
                    continue
                owner, name = parts[0].strip().upper(), parts[1].strip().upper()
                full = f"{owner}.{name}"
                objects_by_type.setdefault('TYPE BODY', set()).add(full)

    package_errors: Dict[Tuple[str, str, str], PackageErrorInfo] = {}
    package_errors_complete = True
    if 'PACKAGE' in object_types_filter or 'PACKAGE BODY' in object_types_filter:
        sql_pkg_err = f"""
            SELECT OWNER, NAME, TYPE, LINE, POSITION, TEXT
            FROM DBA_ERRORS
            WHERE OWNER IN ({owners_in})
              AND TYPE IN ('PACKAGE', 'PACKAGE BODY')
            ORDER BY OWNER, NAME, TYPE, SEQUENCE
        """
        ok, out, err = obclient_run_sql(ob_cfg, sql_pkg_err)
        if not ok:
            package_errors_complete = False
            log.warning("读取 OB DBA_ERRORS 失败，包错误信息将为空: %s", err)
        elif out:
            temp_errors: Dict[Tuple[str, str, str], Dict[str, object]] = defaultdict(
                lambda: {"count": 0, "first_error": ""}
            )
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 6:
                    continue
                owner = parts[0].strip().upper()
                name = parts[1].strip().upper()
                err_type = parts[2].strip().upper()
                err_line = parts[3].strip()
                err_pos = parts[4].strip()
                err_text = normalize_error_text(parts[5])
                if not owner or not name or not err_type:
                    continue
                key = (owner, name, err_type)
                entry = temp_errors[key]
                entry["count"] = int(entry["count"]) + 1
                if not entry["first_error"]:
                    prefix = f"L{err_line}:{err_pos} " if err_line or err_pos else ""
                    entry["first_error"] = f"{prefix}{err_text}".strip()
            for key, info in temp_errors.items():
                package_errors[key] = PackageErrorInfo(
                    count=int(info.get("count") or 0),
                    first_error=str(info.get("first_error") or "")
                )

    # --- 2. DBA_TAB_COLUMNS ---
    sql_cols_ext = f"""
        SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, NULLABLE,
               REPLACE(REPLACE(REPLACE(DATA_DEFAULT, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ') AS DATA_DEFAULT
        FROM DBA_TAB_COLUMNS
        WHERE OWNER IN ({owners_in})
    """
    sql_cols_basic = f"""
        SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, NULLABLE, DATA_DEFAULT
        FROM DBA_TAB_COLUMNS
        WHERE OWNER IN ({owners_in})
    """

    tab_columns: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    if include_tab_columns:
        ok, out, err = obclient_run_sql(ob_cfg, sql_cols_ext)
        if not ok:
            log.warning("读取 OB DBA_TAB_COLUMNS(含默认值清洗)失败，将回退基础查询：%s", err)
            ok, out, err = obclient_run_sql(ob_cfg, sql_cols_basic)
            if not ok:
                log.error("无法从 OB 读取 DBA_TAB_COLUMNS，程序退出。")
                sys.exit(1)

        if out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 7:
                    continue
                owner = parts[0].strip().upper()
                table = parts[1].strip().upper()
                col = parts[2].strip().upper()
                dtype = parts[3].strip().upper()
                char_len = parts[4].strip()
                nullable = parts[5].strip()
                default = parts[6].strip()
                key = (owner, table)
                tab_columns.setdefault(key, {})[col] = {
                    "data_type": dtype,
                    "char_length": int(char_len) if char_len.isdigit() else None,
                    "nullable": nullable,
                    "data_default": default,
                    "hidden": False
                }

    # --- 2.b 注释 (DBA_TAB_COMMENTS / DBA_COL_COMMENTS) ---
    table_comments: Dict[Tuple[str, str], Optional[str]] = {}
    column_comments: Dict[Tuple[str, str], Dict[str, Optional[str]]] = {}
    comments_complete = False
    if include_comments:
        target_pairs = target_table_pairs or set()
        if not target_pairs:
            comments_complete = True
        else:
            comment_keys = sorted(f"{owner}.{table}" for owner, table in target_pairs)
            comments_complete = True

            for chunk in chunk_list(comment_keys, COMMENT_BATCH_SIZE):
                key_clause = ",".join(f"'{val}'" for val in chunk)
                sql_tab_cmt = f"""
                    SELECT OWNER, TABLE_NAME,
                           REPLACE(REPLACE(REPLACE(COMMENTS, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ') AS COMMENTS
                    FROM DBA_TAB_COMMENTS
                    WHERE OWNER||'.'||TABLE_NAME IN ({key_clause})
                """
                ok, out, err = obclient_run_sql(ob_cfg, sql_tab_cmt)
                if not ok:
                    log.warning("无法从 OB 读取 DBA_TAB_COMMENTS，注释比对将跳过：%s", err)
                    comments_complete = False
                    break
                if out:
                    for line in out.splitlines():
                        parts = line.split('\t')
                        if len(parts) < 3:
                            continue
                        owner = parts[0].strip().upper()
                        table = parts[1].strip().upper()
                        comment = parts[2].strip() if len(parts) >= 3 else None
                        table_comments[(owner, table)] = comment

            if comments_complete:
                for chunk in chunk_list(comment_keys, COMMENT_BATCH_SIZE):
                    key_clause = ",".join(f"'{val}'" for val in chunk)
                    sql_col_cmt = f"""
                        SELECT OWNER, TABLE_NAME, COLUMN_NAME,
                               REPLACE(REPLACE(REPLACE(COMMENTS, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ') AS COMMENTS
                        FROM DBA_COL_COMMENTS
                        WHERE OWNER||'.'||TABLE_NAME IN ({key_clause})
                    """
                    ok, out, err = obclient_run_sql(ob_cfg, sql_col_cmt)
                    if not ok:
                        log.warning("无法从 OB 读取 DBA_COL_COMMENTS，注释比对将跳过：%s", err)
                        comments_complete = False
                        break
                    if out:
                        for line in out.splitlines():
                            parts = line.split('\t')
                            if len(parts) < 4:
                                continue
                            owner = parts[0].strip().upper()
                            table = parts[1].strip().upper()
                            column = parts[2].strip().upper()
                            comment = parts[3].strip() if len(parts) >= 4 else None
                            column_comments.setdefault((owner, table), {})[column] = comment
            if comments_complete and target_pairs and not table_comments and not column_comments:
                log.warning("OB 端注释查询未返回任何记录，可能缺少权限，注释比对将跳过。")
                comments_complete = False

    # --- 3. DBA_INDEXES ---
    indexes: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    if include_indexes:
        sql = f"""
            SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, UNIQUENESS
            FROM DBA_INDEXES
            WHERE TABLE_OWNER IN ({owners_in})
        """
        ok, out, err = obclient_run_sql(ob_cfg, sql)
        if not ok:
            log.error("无法从 OB 读取 DBA_INDEXES，程序退出。")
            sys.exit(1)

        if out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 4:
                    continue
                t_owner, t_name, idx_name, uniq = (
                    parts[0].strip().upper(),
                    parts[1].strip().upper(),
                    parts[2].strip().upper(),
                    parts[3].strip().upper()
                )
                key = (t_owner, t_name)
                indexes.setdefault(key, {})[idx_name] = {
                    "uniqueness": uniq,
                    "columns": []
                }

        # --- 4. DBA_IND_COLUMNS ---
        sql = f"""
            SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_NAME, COLUMN_POSITION
            FROM DBA_IND_COLUMNS
            WHERE TABLE_OWNER IN ({owners_in})
            ORDER BY TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_POSITION
        """
        ok, out, err = obclient_run_sql(ob_cfg, sql)
        if not ok:
            log.error("无法从 OB 读取 DBA_IND_COLUMNS，程序退出。")
            sys.exit(1)

        if out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 5:
                    continue
                t_owner, t_name, idx_name, col_name = (
                    parts[0].strip().upper(),
                    parts[1].strip().upper(),
                    parts[2].strip().upper(),
                    parts[3].strip().upper()
                )
                key = (t_owner, t_name)
                # 只有在 DBA_INDEXES 已有记录时才补充列，避免虚构 UNKNOWN 索引
                idx_info = indexes.get(key, {}).get(idx_name)
                if idx_info is None:
                    log.debug("索引 %s.%s.%s 未出现在 DBA_INDEXES，跳过列信息。", t_owner, t_name, idx_name)
                    continue
                idx_info["columns"].append(col_name)

        # 过滤 OMS_* 自动索引
        for key in list(indexes.keys()):
            pruned = {}
            for idx_name, info in indexes[key].items():
                cols = info.get("columns") or []
                if is_oms_index(idx_name, cols):
                    continue
                pruned[idx_name] = info
            if pruned:
                indexes[key] = pruned
            else:
                del indexes[key]

    # --- 5. DBA_CONSTRAINTS (P/U/R) ---
    constraints: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    if include_constraints:
        sql_ext = f"""
            SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, R_OWNER, R_CONSTRAINT_NAME
            FROM DBA_CONSTRAINTS
            WHERE OWNER IN ({owners_in})
              AND CONSTRAINT_TYPE IN ('P','U','R')
              AND STATUS = 'ENABLED'
        """
        ok, out, err = obclient_run_sql(ob_cfg, sql_ext)
        support_fk_ref = ok
        if not ok:
            log.warning("读取 OB DBA_CONSTRAINTS(含引用信息)失败，将回退为基础字段：%s", err)
            sql = f"""
                SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE
                FROM DBA_CONSTRAINTS
                WHERE OWNER IN ({owners_in})
                  AND CONSTRAINT_TYPE IN ('P','U','R')
                  AND STATUS = 'ENABLED'
            """
            ok, out, err = obclient_run_sql(ob_cfg, sql)
            if not ok:
                log.error("无法从 OB 读取 DBA_CONSTRAINTS，程序退出。")
                sys.exit(1)

        if out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 4:
                    continue
                owner = parts[0].strip().upper()
                table = parts[1].strip().upper()
                cons_name = parts[2].strip().upper()
                ctype = parts[3].strip().upper()
                r_owner = parts[4].strip().upper() if support_fk_ref and len(parts) >= 5 else None
                r_cons = parts[5].strip().upper() if support_fk_ref and len(parts) >= 6 else None
                key = (owner, table)
                constraints.setdefault(key, {})[cons_name] = {
                    "type": ctype,
                    "columns": [],
                    "r_owner": r_owner if ctype == "R" else None,
                    "r_constraint": r_cons if ctype == "R" else None,
                }

        # --- 6. DBA_CONS_COLUMNS ---
        sql = f"""
            SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, POSITION
            FROM DBA_CONS_COLUMNS
            WHERE OWNER IN ({owners_in})
            ORDER BY OWNER, TABLE_NAME, CONSTRAINT_NAME, POSITION
        """
        ok, out, err = obclient_run_sql(ob_cfg, sql)
        if not ok:
            log.error("无法从 OB 读取 DBA_CONS_COLUMNS，程序退出。")
            sys.exit(1)

        if out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 5:
                    continue
                owner, table, cons_name, col_name = (
                    parts[0].strip().upper(),
                    parts[1].strip().upper(),
                    parts[2].strip().upper(),
                    parts[3].strip().upper()
                )
                key = (owner, table)
                if key not in constraints:
                    constraints[key] = {}
                if cons_name not in constraints[key]:
                    constraints[key][cons_name] = {"type": "UNKNOWN", "columns": [], "r_owner": None, "r_constraint": None}
                constraints[key][cons_name]["columns"].append(col_name)

        # 为外键补齐被引用表信息（若 OB 提供 R_OWNER/R_CONSTRAINT_NAME）
        if support_fk_ref:
            cons_table_lookup: Dict[Tuple[str, str], Tuple[str, str]] = {}
            for (owner, table), cons_map in constraints.items():
                for cons_name, info in cons_map.items():
                    ctype = (info.get("type") or "").upper()
                    if ctype in ('P', 'U'):
                        cons_table_lookup[(owner, cons_name)] = (owner, table)
            for (_owner, _table), cons_map in constraints.items():
                for cons_name, info in cons_map.items():
                    ctype = (info.get("type") or "").upper()
                    if ctype != 'R':
                        continue
                    r_owner_u = (info.get("r_owner") or "").upper()
                    r_cons_u = (info.get("r_constraint") or "").upper()
                    if not r_owner_u or not r_cons_u:
                        continue
                    ref_table = cons_table_lookup.get((r_owner_u, r_cons_u))
                    if ref_table:
                        info["ref_table_owner"], info["ref_table_name"] = ref_table

        # 过滤 OceanBase 自动生成的 *_OBNOTNULL_* CHECK 约束
        if constraints:
            pruned_constraints: Dict[Tuple[str, str], Dict[str, Dict]] = {}
            removed_cnt = 0
            for key, cons_map in constraints.items():
                kept: Dict[str, Dict] = {}
                for cons_name, info in cons_map.items():
                    if is_ob_notnull_constraint(cons_name):
                        removed_cnt += 1
                        continue
                    kept[cons_name] = info
                if kept:
                    pruned_constraints[key] = kept
            if removed_cnt:
                log.info("[CONSTRAINT] 已忽略 %d 条 OceanBase 自动 OBNOTNULL 约束。", removed_cnt)
            constraints = pruned_constraints

    # --- 7. DBA_TRIGGERS ---
    triggers: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    if include_triggers:
        sql = f"""
            SELECT OWNER, TABLE_OWNER, TABLE_NAME, TRIGGER_NAME, TRIGGERING_EVENT, STATUS
            FROM DBA_TRIGGERS
            WHERE TABLE_OWNER IN ({owners_in})
        """
        ok, out, err = obclient_run_sql(ob_cfg, sql)
        if not ok:
            log.error("无法从 OB 读取 DBA_TRIGGERS，程序退出。")
            sys.exit(1)

        if out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 6:
                    continue
                trg_owner = parts[0].strip().upper()
                t_owner = parts[1].strip().upper()
                t_name = parts[2].strip().upper()
                trg_name = parts[3].strip().upper()
                ev = parts[4].strip()
                status = parts[5].strip() if len(parts) > 5 else ""
                key = (t_owner, t_name)
                triggers.setdefault(key, {})[trg_name] = {
                    "event": ev,
                    "status": status,
                    "owner": trg_owner or t_owner
                }

    # --- 8. DBA_SEQUENCES ---
    sequences: Dict[str, Set[str]] = {}
    if include_sequences:
        sql = f"""
            SELECT SEQUENCE_OWNER, SEQUENCE_NAME
            FROM DBA_SEQUENCES
            WHERE SEQUENCE_OWNER IN ({owners_in})
        """
        ok, out, err = obclient_run_sql(ob_cfg, sql)
        if not ok:
            log.error("无法从 OB 读取 DBA_SEQUENCES，程序退出。")
            sys.exit(1)

        if out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 2:
                    continue
                owner, seq_name = parts[0].strip().upper(), parts[1].strip().upper()
                sequences.setdefault(owner, set()).add(seq_name)

    roles: Set[str] = set()
    # --- 9. DBA_ROLES ---
    if include_roles:
        sql = "SELECT ROLE FROM DBA_ROLES"
        ok, out, err = obclient_run_sql(ob_cfg, sql)
        if not ok:
            log.warning("读取 DBA_ROLES 失败，角色列表可能不完整: %s", err)
        elif out:
            for line in out.splitlines():
                role = (line or "").strip().upper()
                if role:
                    roles.add(role)

    log.info("OceanBase 元数据转储完成 (根据开关加载 DBA_OBJECTS/列/索引/约束/触发器/序列/注释)。")
    return ObMetadata(
        objects_by_type=objects_by_type,
        tab_columns=tab_columns,
        indexes=indexes,
        constraints=constraints,
        triggers=triggers,
        sequences=sequences,
        roles=roles,
        table_comments=table_comments,
        column_comments=column_comments,
        comments_complete=comments_complete,
        object_statuses=object_statuses,
        package_errors=package_errors,
        package_errors_complete=package_errors_complete
    )


def load_ob_supported_sys_privs(ob_cfg: ObConfig) -> Set[str]:
    """
    读取 OceanBase 支持的系统权限集合（以 DBA_SYS_PRIVS 中 SYS 拥有的权限为准）。
    """
    privs: Set[str] = set()
    sql = "SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE='SYS'"
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.warning("读取 OceanBase DBA_SYS_PRIVS 失败，系统权限过滤将基于 Oracle 合法性校验: %s", err)
        return set()
    if out:
        for line in out.splitlines():
            name = (line or "").strip().upper()
            if name:
                privs.add(name)
    return privs


def load_ob_roles(ob_cfg: ObConfig) -> Optional[Set[str]]:
    """
    读取 OceanBase 侧角色列表，用于避免重复 CREATE ROLE。
    """
    roles: Set[str] = set()
    sql = "SELECT ROLE FROM DBA_ROLES"
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.warning("读取 OceanBase DBA_ROLES 失败，将无法避免重复 CREATE ROLE: %s", err)
        return None
    if out:
        for line in out.splitlines():
            role = (line or "").strip().upper()
            if role:
                roles.add(role)
    if not roles:
        log.warning("OB 端角色列表为空，授权 grantee 角色过滤可能不完整。")
    return roles


def load_ob_users(ob_cfg: ObConfig) -> Optional[Set[str]]:
    """
    读取 OceanBase 侧用户列表，用于过滤授权目标。
    优先使用 DBA_USERS，失败时回退 ALL_USERS。
    """
    users: Set[str] = set()
    sql = "SELECT USERNAME FROM DBA_USERS"
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.warning("读取 OceanBase DBA_USERS 失败，将尝试 ALL_USERS: %s", err)
        sql = "SELECT USERNAME FROM ALL_USERS"
        ok, out, err = obclient_run_sql(ob_cfg, sql)
        if not ok:
            log.warning("读取 OceanBase ALL_USERS 失败，将跳过授权目标用户过滤: %s", err)
            return None
    if out:
        for line in out.splitlines():
            name = (line or "").strip().upper()
            if name:
                users.add(name)
    if not users:
        log.warning("OB 端用户列表为空，授权 grantee 用户过滤可能不完整。")
    return users


def load_ob_grant_catalog(
    ob_cfg: ObConfig,
    grantees: Set[str]
) -> Optional[ObGrantCatalog]:
    """
    读取 OceanBase 端权限目录，用于缺失授权计算。
    仅检查直接授权（DBA_TAB_PRIVS / DBA_SYS_PRIVS / DBA_ROLE_PRIVS）。
    """
    if not grantees:
        return ObGrantCatalog(set(), set(), set(), set(), set(), set())

    def _sql_list(vals: List[str]) -> str:
        safe_vals = [v.replace("'", "''") for v in vals if v]
        return ",".join(f"'{v}'" for v in safe_vals)

    object_privs: Set[Tuple[str, str, str]] = set()
    object_privs_grantable: Set[Tuple[str, str, str]] = set()
    sys_privs: Set[Tuple[str, str]] = set()
    sys_privs_admin: Set[Tuple[str, str]] = set()
    role_privs: Set[Tuple[str, str]] = set()
    role_privs_admin: Set[Tuple[str, str]] = set()

    try:
        for chunk in chunk_list(sorted(grantees), 900):
            grantee_list = _sql_list([g.upper() for g in chunk if g])
            if not grantee_list:
                continue

            tab_sql = textwrap.dedent(f"""
                SELECT GRANTEE, PRIVILEGE, OWNER, TABLE_NAME, GRANTABLE
                FROM DBA_TAB_PRIVS
                WHERE GRANTEE IN ({grantee_list})
            """).strip()
            ok, out, err = obclient_run_sql(ob_cfg, tab_sql)
            if not ok:
                log.warning("[GRANT_MISS] 读取 DBA_TAB_PRIVS 失败: %s", err)
                return None
            if out:
                for line in out.splitlines():
                    parts = line.split('\t')
                    if len(parts) < 5:
                        continue
                    grantee = (parts[0] or "").strip().upper()
                    priv = (parts[1] or "").strip().upper()
                    owner = (parts[2] or "").strip().upper()
                    name = (parts[3] or "").strip().upper()
                    grantable = (parts[4] or "").strip().upper() == "YES"
                    if not grantee or not priv or not owner or not name:
                        continue
                    key = (grantee, priv, f"{owner}.{name}")
                    object_privs.add(key)
                    if grantable:
                        object_privs_grantable.add(key)

            sys_sql = textwrap.dedent(f"""
                SELECT GRANTEE, PRIVILEGE, ADMIN_OPTION
                FROM DBA_SYS_PRIVS
                WHERE GRANTEE IN ({grantee_list})
            """).strip()
            ok, out, err = obclient_run_sql(ob_cfg, sys_sql)
            if not ok:
                log.warning("[GRANT_MISS] 读取 DBA_SYS_PRIVS 失败: %s", err)
                return None
            if out:
                for line in out.splitlines():
                    parts = line.split('\t')
                    if len(parts) < 3:
                        continue
                    grantee = (parts[0] or "").strip().upper()
                    priv = (parts[1] or "").strip().upper()
                    admin = (parts[2] or "").strip().upper() == "YES"
                    if not grantee or not priv:
                        continue
                    key = (grantee, priv)
                    sys_privs.add(key)
                    if admin:
                        sys_privs_admin.add(key)

            role_sql = textwrap.dedent(f"""
                SELECT GRANTEE, GRANTED_ROLE, ADMIN_OPTION
                FROM DBA_ROLE_PRIVS
                WHERE GRANTEE IN ({grantee_list})
            """).strip()
            ok, out, err = obclient_run_sql(ob_cfg, role_sql)
            if not ok:
                log.warning("[GRANT_MISS] 读取 DBA_ROLE_PRIVS 失败: %s", err)
                return None
            if out:
                for line in out.splitlines():
                    parts = line.split('\t')
                    if len(parts) < 3:
                        continue
                    grantee = (parts[0] or "").strip().upper()
                    role = (parts[1] or "").strip().upper()
                    admin = (parts[2] or "").strip().upper() == "YES"
                    if not grantee or not role:
                        continue
                    key = (grantee, role)
                    role_privs.add(key)
                    if admin:
                        role_privs_admin.add(key)
    except Exception as exc:  # pragma: no cover
        log.warning("[GRANT_MISS] 读取 OB 权限目录失败: %s", exc)
        return None

    return ObGrantCatalog(
        object_privs=object_privs,
        object_privs_grantable=object_privs_grantable,
        sys_privs=sys_privs,
        sys_privs_admin=sys_privs_admin,
        role_privs=role_privs,
        role_privs_admin=role_privs_admin
    )


# ====================== Oracle 侧辅助函数 ======================

def load_oracle_role_privileges(
    ora_conn,
    base_grantees: Set[str]
) -> Tuple[List[OracleRolePrivilege], Set[str]]:
    """
    读取 DBA_ROLE_PRIVS，并递归展开角色授予链路。
    返回 (role_grants, discovered_roles)。
    """
    role_grants: List[OracleRolePrivilege] = []
    discovered_roles: Set[str] = set()
    pending: Set[str] = {g.upper() for g in base_grantees if g}
    seen: Set[str] = set()

    if not pending:
        return role_grants, discovered_roles

    while pending:
        batch = sorted(pending - seen)
        if not batch:
            break
        seen.update(batch)
        sql_tpl = """
            SELECT GRANTEE, GRANTED_ROLE, ADMIN_OPTION
            FROM DBA_ROLE_PRIVS
            WHERE GRANTEE IN ({placeholders})
        """
        with ora_conn.cursor() as cursor:
            for placeholders, chunk in iter_in_chunks(batch):
                sql = sql_tpl.format(placeholders=placeholders)
                cursor.execute(sql, chunk)
                for row in cursor:
                    grantee = (row[0] or "").strip().upper()
                    role = (row[1] or "").strip().upper()
                    admin_opt = (row[2] or "").strip().upper() == "YES"
                    if not grantee or not role:
                        continue
                    role_grants.append(OracleRolePrivilege(grantee, role, admin_opt))
                    if role not in discovered_roles:
                        discovered_roles.add(role)
                        if role not in seen:
                            pending.add(role)

    return role_grants, discovered_roles


def load_oracle_roles(ora_conn) -> Dict[str, OracleRoleInfo]:
    """
    读取 DBA_ROLES，用于生成 CREATE ROLE DDL。
    """
    roles: Dict[str, OracleRoleInfo] = {}

    def _supports_oracle_maintained() -> bool:
        try:
            with ora_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM DBA_TAB_COLUMNS
                    WHERE OWNER = 'SYS'
                      AND TABLE_NAME = 'DBA_ROLES'
                      AND COLUMN_NAME = 'ORACLE_MAINTAINED'
                """)
                row = cursor.fetchone()
                return bool(row and row[0] and int(row[0]) > 0)
        except oracledb.Error:
            return False

    try:
        has_om = _supports_oracle_maintained()
        select_cols = "ROLE, AUTHENTICATION_TYPE, PASSWORD_REQUIRED"
        if has_om:
            select_cols += ", ORACLE_MAINTAINED"
        sql = f"SELECT {select_cols} FROM DBA_ROLES"
        with ora_conn.cursor() as cursor:
            cursor.execute(sql)
            for row in cursor:
                role = (row[0] or "").strip().upper()
                if not role:
                    continue
                auth_type = (row[1] or "").strip().upper()
                pwd_required = (row[2] or "").strip().upper() == "YES"
                oracle_maintained: Optional[bool] = None
                if has_om and len(row) > 3:
                    oracle_maintained = (row[3] or "").strip().upper() == "Y"
                roles[role] = OracleRoleInfo(
                    role=role,
                    authentication_type=auth_type,
                    password_required=pwd_required,
                    oracle_maintained=oracle_maintained
                )
    except oracledb.Error as exc:
        log.warning("读取 DBA_ROLES 失败，角色 DDL 将仅基于授权引用推断: %s", exc)
        return {}

    return roles


def load_oracle_system_privilege_map(ora_conn) -> Set[str]:
    """
    读取 SYSTEM_PRIVILEGE_MAP，提供 Oracle 侧系统权限全集。
    """
    privs: Set[str] = set()
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute("SELECT NAME FROM SYSTEM_PRIVILEGE_MAP")
            for row in cursor:
                name = (row[0] or "").strip().upper()
                if name:
                    privs.add(name)
    except oracledb.Error as exc:
        log.warning("读取 SYSTEM_PRIVILEGE_MAP 失败，将跳过 Oracle 系统权限合法性校验: %s", exc)
    return privs


def load_oracle_table_privilege_map(ora_conn) -> Set[str]:
    """
    读取 TABLE_PRIVILEGE_MAP，提供 Oracle 侧对象权限全集。
    """
    privs: Set[str] = set()
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute("SELECT NAME FROM TABLE_PRIVILEGE_MAP")
            for row in cursor:
                name = (row[0] or "").strip().upper()
                if name:
                    privs.add(name)
    except oracledb.Error as exc:
        log.warning("读取 TABLE_PRIVILEGE_MAP 失败，将跳过 Oracle 对象权限合法性校验: %s", exc)
    return privs


def load_oracle_sys_privileges(
    ora_conn,
    grantees: Set[str]
) -> List[OracleSysPrivilege]:
    """
    读取 DBA_SYS_PRIVS。
    """
    sys_privs: List[OracleSysPrivilege] = []
    if not grantees:
        return sys_privs

    for chunk in chunk_list(sorted(grantees), ORACLE_IN_BATCH_SIZE):
        placeholders = ",".join(f":{i+1}" for i in range(len(chunk)))
        sql = f"""
            SELECT GRANTEE, PRIVILEGE, ADMIN_OPTION
            FROM DBA_SYS_PRIVS
            WHERE GRANTEE IN ({placeholders})
        """
        with ora_conn.cursor() as cursor:
            cursor.execute(sql, chunk)
            for row in cursor:
                grantee = (row[0] or "").strip().upper()
                privilege = (row[1] or "").strip().upper()
                admin_opt = (row[2] or "").strip().upper() == "YES"
                if not grantee or not privilege:
                    continue
                sys_privs.append(OracleSysPrivilege(grantee, privilege, admin_opt))

    return sys_privs


def load_oracle_tab_privileges(
    ora_conn,
    owners: Set[str],
    grantees: Set[str],
    scope: str = "owner"
) -> List[OracleObjectPrivilege]:
    """
    读取 DBA_TAB_PRIVS，按 OWNER 或 OWNER+GRANTEE 范围过滤后去重。
    """
    def _supports_type_column() -> bool:
        try:
            with ora_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM DBA_TAB_COLUMNS
                    WHERE OWNER = 'SYS'
                      AND TABLE_NAME = 'DBA_TAB_PRIVS'
                      AND COLUMN_NAME = 'TYPE'
                """)
                row = cursor.fetchone()
                return bool(row and row[0] and int(row[0]) > 0)
        except oracledb.Error:
            return False

    def _load_for(column: str, values: Set[str], has_type: bool) -> List[OracleObjectPrivilege]:
        results: List[OracleObjectPrivilege] = []
        if not values:
            return results
        select_cols = "GRANTEE, OWNER, TABLE_NAME, PRIVILEGE, GRANTABLE"
        if has_type:
            select_cols += ", TYPE"
        for chunk in chunk_list(sorted(values), ORACLE_IN_BATCH_SIZE):
            placeholders = ",".join(f":{i+1}" for i in range(len(chunk)))
            sql = f"""
                SELECT {select_cols}
                FROM DBA_TAB_PRIVS
                WHERE {column} IN ({placeholders})
            """
            with ora_conn.cursor() as cursor:
                cursor.execute(sql, chunk)
                for row in cursor:
                    grantee = (row[0] or "").strip().upper()
                    owner = (row[1] or "").strip().upper()
                    obj_name = (row[2] or "").strip().upper()
                    privilege = (row[3] or "").strip().upper()
                    grantable = (row[4] or "").strip().upper() == "YES"
                    obj_type = ""
                    if has_type and len(row) > 5:
                        obj_type = (row[5] or "").strip().upper()
                    if not grantee or not owner or not obj_name or not privilege:
                        continue
                    results.append(OracleObjectPrivilege(
                        grantee=grantee,
                        owner=owner,
                        object_name=obj_name,
                        object_type=obj_type,
                        privilege=privilege,
                        grantable=grantable
                    ))
        return results

    has_type = _supports_type_column()
    scope_u = (scope or "owner").strip().lower()
    if scope_u not in ("owner", "owner_or_grantee"):
        log.warning("未知 grant_tab_privs_scope=%s，回退为 owner。", scope)
        scope_u = "owner"

    results: List[OracleObjectPrivilege] = []
    results.extend(_load_for("OWNER", owners, has_type))
    if scope_u == "owner_or_grantee":
        results.extend(_load_for("GRANTEE", grantees, has_type))

    deduped: Dict[Tuple[str, str, str, str, str, bool], OracleObjectPrivilege] = {}
    for item in results:
        key = (
            item.grantee.upper(),
            item.owner.upper(),
            item.object_name.upper(),
            (item.object_type or "").upper(),
            item.privilege.upper(),
            bool(item.grantable)
        )
        deduped.setdefault(key, item)

    return list(deduped.values())

def dump_oracle_metadata(
    ora_cfg: OraConfig,
    master_list: MasterCheckList,
    settings: Dict,
    include_indexes: bool = True,
    include_constraints: bool = True,
    include_triggers: bool = True,
    include_sequences: bool = True,
    include_comments: bool = True,
    include_blacklist: bool = True,
    include_privileges: bool = True
) -> OracleMetadata:
    """
    预先加载 Oracle 端所需的所有元数据，避免在校验/修补阶段频繁查询。
    """
    table_pairs: Set[Tuple[str, str]] = collect_table_pairs(master_list)
    owner_set: Set[str] = {schema for schema, _ in table_pairs}

    owners = sorted(owner_set)
    seq_owners = sorted({s.upper() for s in settings.get('source_schemas_list', [])})
    source_schema_set: Set[str] = {s.upper() for s in settings.get('source_schemas_list', []) if s}
    privilege_owners: Set[str] = set(source_schema_set)

    if not owners and not seq_owners:
        log.warning("未检测到需要加载的 Oracle schema，返回空元数据。")
        return OracleMetadata(
            table_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            table_comments={},
            column_comments={},
            comments_complete=False,
            blacklist_tables={},
            object_privileges=[],
            sys_privileges=[],
            role_privileges=[],
            role_metadata={},
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={},
            package_errors={},
            package_errors_complete=False
        )

    log.info("正在批量加载 Oracle 元数据 (DBA_TAB_COLUMNS/DBA_INDEXES/DBA_CONSTRAINTS/DBA_TRIGGERS/DBA_SEQUENCES)...")
    table_columns: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    indexes: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    constraints: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    triggers: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    sequences: Dict[str, Set[str]] = {}
    roles: Set[str] = set()
    table_comments: Dict[Tuple[str, str], Optional[str]] = {}
    column_comments: Dict[Tuple[str, str], Dict[str, Optional[str]]] = {}
    comments_complete = False
    blacklist_tables: BlacklistTableMap = {}
    object_privileges: List[OracleObjectPrivilege] = []
    sys_privileges: List[OracleSysPrivilege] = []
    role_privileges: List[OracleRolePrivilege] = []
    role_metadata: Dict[str, OracleRoleInfo] = {}
    system_privilege_map: Set[str] = set()
    table_privilege_map: Set[str] = set()
    object_statuses: Dict[Tuple[str, str, str], str] = {}
    package_errors: Dict[Tuple[str, str, str], PackageErrorInfo] = {}
    package_errors_complete = True

    def _safe_upper(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        try:
            return value.upper()
        except AttributeError:
            return str(value).upper()

    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as ora_conn:
            owner_chunks = chunk_list(owners, ORACLE_IN_BATCH_SIZE)
            seq_owner_chunks = chunk_list(seq_owners, ORACLE_IN_BATCH_SIZE)
            need_package_status = any(
                (obj_type or "").upper() in PACKAGE_OBJECT_TYPES
                for _, _, obj_type in master_list
            )
            if owners or need_package_status:
                # 检测是否支持 HIDDEN_COLUMN 字段（部分低版本/权限受限环境不存在）
                support_hidden_col = False
                try:
                    with ora_conn.cursor() as cursor:
                        cursor.execute("""
                            SELECT COUNT(*)
                            FROM DBA_TAB_COLUMNS
                            WHERE OWNER = 'SYS'
                              AND TABLE_NAME = 'DBA_TAB_COLUMNS'
                              AND COLUMN_NAME = 'HIDDEN_COLUMN'
                        """)
                        count_row = cursor.fetchone()
                        support_hidden_col = bool(count_row and count_row[0] and int(count_row[0]) > 0)
                except oracledb.Error as e:
                    log.info("无法探测 HIDDEN_COLUMN 支持，默认不读取 hidden 标记：%s", e)
                    support_hidden_col = False

                # 列定义
                def _load_ora_tab_columns_sql(include_hidden: bool) -> str:
                    hidden_col = ", NVL(TO_CHAR(HIDDEN_COLUMN),'NO') AS HIDDEN_COLUMN" if include_hidden else ""
                    return f"""
                        SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
                               DATA_LENGTH, DATA_PRECISION, DATA_SCALE,
                               NULLABLE, DATA_DEFAULT, CHAR_USED, CHAR_LENGTH{hidden_col}
                        FROM DBA_TAB_COLUMNS
                        WHERE OWNER IN ({{owners_clause}})
                    """

                def _parse_tab_column_row(row, include_hidden: bool) -> Dict:
                    return {
                        "data_type": row[3],
                        "data_length": row[4],
                        "data_precision": row[5],
                        "data_scale": row[6],
                        "nullable": row[7],
                        "data_default": row[8],
                        "char_used": row[9],
                        "char_length": row[10],
                        "hidden": (row[11] if include_hidden and len(row) > 11 else "NO") == "YES" if include_hidden else False
                    }

                try:
                    sql_tpl = _load_ora_tab_columns_sql(include_hidden=support_hidden_col)
                    with ora_conn.cursor() as cursor:
                        for owner_chunk in owner_chunks:
                            owners_clause = build_bind_placeholders(len(owner_chunk))
                            sql = sql_tpl.format(owners_clause=owners_clause)
                            cursor.execute(sql, owner_chunk)
                            for row in cursor:
                                owner = _safe_upper(row[0])
                                table = _safe_upper(row[1])
                                col = _safe_upper(row[2])
                                if not owner or not table or not col:
                                    continue
                                key = (owner, table)
                                if key not in table_pairs:
                                    continue
                                table_columns.setdefault(key, {})[col] = _parse_tab_column_row(row, support_hidden_col)
                except oracledb.Error as e:
                    if support_hidden_col:
                        log.info("读取 DBA_TAB_COLUMNS(含 hidden) 失败，尝试不含 hidden：%s", e)
                        support_hidden_col = False
                        sql_tpl = _load_ora_tab_columns_sql(include_hidden=False)
                        with ora_conn.cursor() as cursor:
                            for owner_chunk in owner_chunks:
                                owners_clause = build_bind_placeholders(len(owner_chunk))
                                sql = sql_tpl.format(owners_clause=owners_clause)
                                cursor.execute(sql, owner_chunk)
                                for row in cursor:
                                    owner = _safe_upper(row[0])
                                    table = _safe_upper(row[1])
                                    col = _safe_upper(row[2])
                                    if not owner or not table or not col:
                                        continue
                                    key = (owner, table)
                                    if key not in table_pairs:
                                        continue
                                    table_columns.setdefault(key, {})[col] = _parse_tab_column_row(row, False)
                    else:
                        raise

                # 索引
                if include_indexes:
                    sql_idx_tpl = """
                        SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, UNIQUENESS
                        FROM DBA_INDEXES
                        WHERE TABLE_OWNER IN ({owners_clause})
                    """
                    with ora_conn.cursor() as cursor:
                        for owner_chunk in owner_chunks:
                            owners_clause = build_bind_placeholders(len(owner_chunk))
                            sql_idx = sql_idx_tpl.format(owners_clause=owners_clause)
                            cursor.execute(sql_idx, owner_chunk)
                            for row in cursor:
                                owner = _safe_upper(row[0])
                                table = _safe_upper(row[1])
                                if not owner or not table:
                                    continue
                                key = (owner, table)
                                if key not in table_pairs:
                                    continue
                                idx_name = _safe_upper(row[2])
                                if not idx_name:
                                    continue
                                indexes.setdefault(key, {})[idx_name] = {
                                    "uniqueness": (row[3] or "").upper(),
                                    "columns": []
                                }

                    sql_idx_cols_tpl = """
                        SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_NAME
                        FROM DBA_IND_COLUMNS
                        WHERE TABLE_OWNER IN ({owners_clause})
                        ORDER BY TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_POSITION
                    """
                    with ora_conn.cursor() as cursor:
                        for owner_chunk in owner_chunks:
                            owners_clause = build_bind_placeholders(len(owner_chunk))
                            sql_idx_cols = sql_idx_cols_tpl.format(owners_clause=owners_clause)
                            cursor.execute(sql_idx_cols, owner_chunk)
                            for row in cursor:
                                owner = _safe_upper(row[0])
                                table = _safe_upper(row[1])
                                if not owner or not table:
                                    continue
                                key = (owner, table)
                                if key not in table_pairs:
                                    continue
                                idx_name = _safe_upper(row[2])
                                col_name = _safe_upper(row[3])
                                if not idx_name or not col_name:
                                    continue
                                indexes.setdefault(key, {}).setdefault(
                                    idx_name, {"uniqueness": "UNKNOWN", "columns": []}
                                )["columns"].append(col_name)

                # 约束
                if include_constraints:
                    sql_cons_tpl = """
                        SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, R_OWNER, R_CONSTRAINT_NAME
                        FROM DBA_CONSTRAINTS
                        WHERE OWNER IN ({owners_clause})
                          AND CONSTRAINT_TYPE IN ('P','U','R')
                          AND STATUS = 'ENABLED'
                    """
                    with ora_conn.cursor() as cursor:
                        for owner_chunk in owner_chunks:
                            owners_clause = build_bind_placeholders(len(owner_chunk))
                            sql_cons = sql_cons_tpl.format(owners_clause=owners_clause)
                            cursor.execute(sql_cons, owner_chunk)
                            for row in cursor:
                                owner = _safe_upper(row[0])
                                table = _safe_upper(row[1])
                                if not owner or not table:
                                    continue
                                key = (owner, table)
                                if key not in table_pairs:
                                    continue
                                name = _safe_upper(row[2])
                                if not name:
                                    continue
                                constraints.setdefault(key, {})[name] = {
                                    "type": (row[3] or "").upper(),
                                    "columns": [],
                                    "r_owner": _safe_upper(row[4]) if row[4] else None,
                                    "r_constraint": _safe_upper(row[5]) if row[5] else None,
                                }

                    sql_cons_cols_tpl = """
                        SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME
                        FROM DBA_CONS_COLUMNS
                        WHERE OWNER IN ({owners_clause})
                        ORDER BY OWNER, TABLE_NAME, CONSTRAINT_NAME, POSITION
                    """
                    with ora_conn.cursor() as cursor:
                        for owner_chunk in owner_chunks:
                            owners_clause = build_bind_placeholders(len(owner_chunk))
                            sql_cons_cols = sql_cons_cols_tpl.format(owners_clause=owners_clause)
                            cursor.execute(sql_cons_cols, owner_chunk)
                            for row in cursor:
                                owner = _safe_upper(row[0])
                                table = _safe_upper(row[1])
                                if not owner or not table:
                                    continue
                                key = (owner, table)
                                if key not in table_pairs:
                                    continue
                                cons_name = _safe_upper(row[2])
                                col_name = _safe_upper(row[3])
                                if not cons_name or not col_name:
                                    continue
                                constraints.setdefault(key, {}).setdefault(
                                    cons_name, {"type": "UNKNOWN", "columns": []}
                                )["columns"].append(col_name)

                    # 为外键补齐被引用表信息 (基于约束引用)
                    cons_table_lookup: Dict[Tuple[str, str], Tuple[str, str]] = {}
                    for (owner, table), cons_map in constraints.items():
                        for cons_name, info in cons_map.items():
                            ctype = (info.get("type") or "").upper()
                            if ctype in ('P', 'U'):
                                cons_table_lookup[(owner, cons_name)] = (owner, table)
                    for (owner, _), cons_map in constraints.items():
                        for cons_name, info in cons_map.items():
                            ctype = (info.get("type") or "").upper()
                            if ctype != 'R':
                                continue
                            r_owner = (info.get("r_owner") or "").upper()
                            r_cons = (info.get("r_constraint") or "").upper()
                            if not r_owner or not r_cons:
                                continue
                            ref_table = cons_table_lookup.get((r_owner, r_cons))
                            if ref_table:
                                info["ref_table_owner"], info["ref_table_name"] = ref_table

                # 触发器
                if include_triggers:
                    sql_trg_tpl = """
                        SELECT OWNER, TABLE_OWNER, TABLE_NAME, TRIGGER_NAME, TRIGGERING_EVENT, STATUS
                        FROM DBA_TRIGGERS
                        WHERE TABLE_OWNER IN ({owners_clause})
                    """
                    with ora_conn.cursor() as cursor:
                        for owner_chunk in owner_chunks:
                            owners_clause = build_bind_placeholders(len(owner_chunk))
                            sql_trg = sql_trg_tpl.format(owners_clause=owners_clause)
                            cursor.execute(sql_trg, owner_chunk)
                            for row in cursor:
                                trg_owner = _safe_upper(row[0])
                                owner = _safe_upper(row[1])
                                table = _safe_upper(row[2])
                                if not owner or not table:
                                    continue
                                key = (owner, table)
                                if key not in table_pairs:
                                    continue
                                trg_name = _safe_upper(row[3])
                                if not trg_name:
                                    continue
                                triggers.setdefault(key, {})[trg_name] = {
                                    "event": row[4],
                                    "status": row[5],
                                    "owner": trg_owner or owner
                                }

                if need_package_status:
                    pkg_owners = sorted(source_schema_set | set(owners))
                    if pkg_owners:
                        pkg_owner_chunks = chunk_list(pkg_owners, ORACLE_IN_BATCH_SIZE)
                        sql_pkg_tpl = """
                            SELECT OWNER, OBJECT_NAME, OBJECT_TYPE, STATUS
                            FROM DBA_OBJECTS
                            WHERE OWNER IN ({owners_clause})
                              AND OBJECT_TYPE IN ('PACKAGE', 'PACKAGE BODY')
                        """
                        with ora_conn.cursor() as cursor:
                            for owner_chunk in pkg_owner_chunks:
                                owners_clause = build_bind_placeholders(len(owner_chunk))
                                sql_pkg = sql_pkg_tpl.format(owners_clause=owners_clause)
                                cursor.execute(sql_pkg, owner_chunk)
                                for row in cursor:
                                    owner = _safe_upper(row[0])
                                    name = _safe_upper(row[1])
                                    obj_type = _safe_upper(row[2])
                                    status = _safe_upper(row[3]) if row[3] else "UNKNOWN"
                                    if not owner or not name or not obj_type:
                                        continue
                                    object_statuses[(owner, name, obj_type)] = status or "UNKNOWN"

                        try:
                            sql_pkg_err_tpl = """
                                SELECT OWNER, NAME, TYPE, LINE, POSITION, TEXT
                                FROM DBA_ERRORS
                                WHERE OWNER IN ({owners_clause})
                                  AND TYPE IN ('PACKAGE', 'PACKAGE BODY')
                                ORDER BY OWNER, NAME, TYPE, SEQUENCE
                            """
                            temp_errors: Dict[Tuple[str, str, str], Dict[str, object]] = defaultdict(
                                lambda: {"count": 0, "first_error": ""}
                            )
                            with ora_conn.cursor() as cursor:
                                for owner_chunk in pkg_owner_chunks:
                                    owners_clause = build_bind_placeholders(len(owner_chunk))
                                    sql_pkg_err = sql_pkg_err_tpl.format(owners_clause=owners_clause)
                                    cursor.execute(sql_pkg_err, owner_chunk)
                                    for row in cursor:
                                        owner = _safe_upper(row[0])
                                        name = _safe_upper(row[1])
                                        err_type = _safe_upper(row[2])
                                        err_line = str(row[3]).strip() if row[3] is not None else ""
                                        err_pos = str(row[4]).strip() if row[4] is not None else ""
                                        err_text = normalize_error_text(row[5] if len(row) > 5 else "")
                                        if not owner or not name or not err_type:
                                            continue
                                        key = (owner, name, err_type)
                                        entry = temp_errors[key]
                                        entry["count"] = int(entry["count"]) + 1
                                        if not entry["first_error"]:
                                            prefix = f"L{err_line}:{err_pos} " if err_line or err_pos else ""
                                            entry["first_error"] = f"{prefix}{err_text}".strip()
                            for key, info in temp_errors.items():
                                package_errors[key] = PackageErrorInfo(
                                    count=int(info.get("count") or 0),
                                    first_error=str(info.get("first_error") or "")
                                )
                        except oracledb.Error as e:
                            package_errors_complete = False
                            log.warning("读取 Oracle DBA_ERRORS 失败，包错误信息将为空: %s", e)

                if include_comments:
                    if not table_pairs:
                        comments_complete = True
                    else:
                        comment_keys = sorted(f"{owner}.{table}" for owner, table in table_pairs)
                        comments_complete = True
                        try:
                            with ora_conn.cursor() as cursor:
                                for chunk in chunk_list(comment_keys, COMMENT_BATCH_SIZE):
                                    if not chunk:
                                        continue
                                    placeholders = build_bind_placeholders(len(chunk))
                                    sql_cmt = f"""
                                        SELECT OWNER, TABLE_NAME, COMMENTS
                                        FROM DBA_TAB_COMMENTS
                                        WHERE OWNER||'.'||TABLE_NAME IN ({placeholders})
                                    """
                                    cursor.execute(sql_cmt, chunk)
                                    for row in cursor:
                                        owner = _safe_upper(row[0])
                                        table = _safe_upper(row[1])
                                        if not owner or not table:
                                            continue
                                        table_comments[(owner, table)] = row[2]

                                for chunk in chunk_list(comment_keys, COMMENT_BATCH_SIZE):
                                    if not chunk:
                                        continue
                                    placeholders = build_bind_placeholders(len(chunk))
                                    sql_cmt_col = f"""
                                        SELECT OWNER, TABLE_NAME, COLUMN_NAME, COMMENTS
                                        FROM DBA_COL_COMMENTS
                                        WHERE OWNER||'.'||TABLE_NAME IN ({placeholders})
                                    """
                                    cursor.execute(sql_cmt_col, chunk)
                                    for row in cursor:
                                        owner = _safe_upper(row[0])
                                        table = _safe_upper(row[1])
                                        column = _safe_upper(row[2])
                                        if not owner or not table or not column:
                                            continue
                                        column_comments.setdefault((owner, table), {})[column] = row[3]
                        except oracledb.Error as e:
                            comments_complete = False
                            log.warning("读取 DBA_TAB_COMMENTS/DBA_COL_COMMENTS 失败，将跳过注释比对：%s", e)
                        if comments_complete and table_pairs and not table_comments and not column_comments:
                            log.warning("Oracle 端注释查询未返回任何记录，可能缺少权限，注释比对将跳过。")
                            comments_complete = False

                if include_blacklist:
                    blacklist_available = True
                    try:
                        total_blacklist = 0
                        with ora_conn.cursor() as cursor:
                            sql_blacklist_count_tpl = """
                                SELECT COUNT(*)
                                FROM OMS_USER.TMP_BLACK_TABLE
                                WHERE OWNER IN ({owners_clause})
                            """
                            for owner_chunk in owner_chunks:
                                owners_clause = build_bind_placeholders(len(owner_chunk))
                                sql_blacklist_count = sql_blacklist_count_tpl.format(owners_clause=owners_clause)
                                cursor.execute(sql_blacklist_count, owner_chunk)
                                row = cursor.fetchone()
                                total_blacklist += int(row[0]) if row and row[0] is not None else 0
                        if total_blacklist <= 0:
                            log.warning("未检测到 OMS_USER.TMP_BLACK_TABLE 黑名单记录（当前 schema）。")
                        else:
                            log.info("检测到黑名单表记录 %d 条（当前 schema），将用于过滤缺失表规则。", total_blacklist)
                    except oracledb.Error as e:
                        blacklist_available = False
                        err_msg = str(e)
                        if any(code in err_msg for code in ("ORA-00942", "ORA-04043")):
                            log.warning("未检测到 OMS_USER.TMP_BLACK_TABLE（黑名单过滤已跳过）。")
                        else:
                            log.warning("读取 OMS_USER.TMP_BLACK_TABLE 失败，将跳过黑名单过滤：%s", e)

                    if blacklist_available:
                        sql_blacklist_tpl = """
                            SELECT OWNER, TABLE_NAME, DATA_TYPE, BLACK_TYPE
                            FROM OMS_USER.TMP_BLACK_TABLE
                            WHERE OWNER IN ({owners_clause})
                        """
                        try:
                            with ora_conn.cursor() as cursor:
                                for owner_chunk in owner_chunks:
                                    owners_clause = build_bind_placeholders(len(owner_chunk))
                                    sql_blacklist = sql_blacklist_tpl.format(owners_clause=owners_clause)
                                    cursor.execute(sql_blacklist, owner_chunk)
                                    for row in cursor:
                                        owner = _safe_upper(row[0])
                                        table = _safe_upper(row[1])
                                        if not owner or not table:
                                            continue
                                        key = (owner, table)
                                        if key not in table_pairs:
                                            continue
                                        data_type = normalize_black_data_type(row[2])
                                        black_type = normalize_black_type(row[3]) or "UNKNOWN"
                                        blacklist_tables.setdefault(key, set()).add((black_type, data_type))
                        except oracledb.Error as e:
                            log.warning("读取 OMS_USER.TMP_BLACK_TABLE 失败，将跳过黑名单过滤：%s", e)

            if include_privileges:
                try:
                    base_grantees = set(source_schema_set)
                    role_privileges, role_names = load_oracle_role_privileges(ora_conn, base_grantees)
                    grantee_scope = set(base_grantees) | set(role_names) | {"PUBLIC"}
                    sys_privileges = load_oracle_sys_privileges(ora_conn, base_grantees | set(role_names))
                    object_privileges = load_oracle_tab_privileges(
                        ora_conn,
                        privilege_owners,
                        grantee_scope,
                        settings.get('grant_tab_privs_scope', 'owner')
                    )
                    role_metadata = load_oracle_roles(ora_conn)
                    system_privilege_map = load_oracle_system_privilege_map(ora_conn)
                    table_privilege_map = load_oracle_table_privilege_map(ora_conn)
                    log.info(
                        "Oracle 权限元数据加载完成：对象权限=%d, 系统权限=%d, 角色授权=%d, 角色数=%d, scope=%s",
                        len(object_privileges),
                        len(sys_privileges),
                        len(role_privileges),
                        len(role_names),
                        settings.get('grant_tab_privs_scope', 'owner')
                    )
                except oracledb.Error as e:
                    log.warning("读取 Oracle 权限元数据失败，将跳过授权生成：%s", e)
                    object_privileges = []
                    sys_privileges = []
                    role_privileges = []
                    role_metadata = {}
                    system_privilege_map = set()
                    table_privilege_map = set()

            if seq_owners and include_sequences:
                with ora_conn.cursor() as cursor:
                    sql_seq_tpl = """
                        SELECT SEQUENCE_OWNER, SEQUENCE_NAME
                        FROM DBA_SEQUENCES
                        WHERE SEQUENCE_OWNER IN ({owners_clause})
                    """
                    for owner_chunk in seq_owner_chunks:
                        owners_clause = build_bind_placeholders(len(owner_chunk))
                        sql_seq = sql_seq_tpl.format(owners_clause=owners_clause)
                        cursor.execute(sql_seq, owner_chunk)
                        for row in cursor:
                            owner = _safe_upper(row[0])
                            seq_name = _safe_upper(row[1])
                            if not owner or not seq_name:
                                continue
                            sequences.setdefault(owner, set()).add(seq_name)

    except oracledb.Error as e:
        log.error(f"严重错误: 批量获取 Oracle 元数据失败: {e}")
        sys.exit(1)

    log.info(
        "Oracle 元数据加载完成：列=%d, 索引表=%d, 约束表=%d, 触发器表=%d, 序列schema=%d, 注释表=%d, 黑名单表=%d",
        len(table_columns),
        len(indexes),
        len(constraints),
        len(triggers),
        len(sequences),
        len(table_comments),
        len(blacklist_tables)
    )

    return OracleMetadata(
        table_columns=table_columns,
        indexes=indexes,
        constraints=constraints,
        triggers=triggers,
        sequences=sequences,
        table_comments=table_comments,
        column_comments=column_comments,
        comments_complete=comments_complete,
        blacklist_tables=blacklist_tables,
        object_privileges=object_privileges,
        sys_privileges=sys_privileges,
        role_privileges=role_privileges,
        role_metadata=role_metadata,
        system_privilege_map=system_privilege_map,
        table_privilege_map=table_privilege_map,
        object_statuses=object_statuses,
        package_errors=package_errors,
        package_errors_complete=package_errors_complete
    )


def load_oracle_dependencies(
    ora_cfg: OraConfig,
    schemas_list: List[str],
    object_types: Optional[Set[str]] = None,
    include_external_refs: bool = False
) -> List[DependencyRecord]:
    """
    从 Oracle 批量读取源 schema 的依赖关系（可选包含外部引用）。
    """
    if not schemas_list:
        return []

    owners = sorted({s.upper() for s in schemas_list})
    enabled_types = {t.upper() for t in (object_types or set(ALL_TRACKED_OBJECT_TYPES))}
    enabled_types &= set(ALL_TRACKED_OBJECT_TYPES)
    if not enabled_types:
        log.info("未启用依赖分析对象类型，跳过 Oracle 依赖读取。")
        return []
    types_clause = ",".join(f"'{t}'" for t in sorted(enabled_types))
    owner_chunks = chunk_list(owners, ORACLE_IN_BATCH_SIZE)
    if include_external_refs:
        sql_tpl = """
            SELECT OWNER, NAME, TYPE, REFERENCED_OWNER, REFERENCED_NAME, REFERENCED_TYPE
            FROM DBA_DEPENDENCIES
            WHERE OWNER IN ({owner_ph})
              AND TYPE IN ({types_clause})
              AND REFERENCED_TYPE IN ({types_clause})
        """
    else:
        sql_tpl = """
            SELECT OWNER, NAME, TYPE, REFERENCED_OWNER, REFERENCED_NAME, REFERENCED_TYPE
            FROM DBA_DEPENDENCIES
            WHERE OWNER IN ({owner_ph})
              AND REFERENCED_OWNER IN ({ref_ph})
              AND TYPE IN ({types_clause})
              AND REFERENCED_TYPE IN ({types_clause})
        """

    records: List[DependencyRecord] = []
    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as connection:
            with connection.cursor() as cursor:
                if include_external_refs:
                    for owner_chunk in owner_chunks:
                        owner_ph = build_bind_placeholders(len(owner_chunk))
                        sql = sql_tpl.format(owner_ph=owner_ph, types_clause=types_clause)
                        cursor.execute(sql, owner_chunk)
                        for row in cursor:
                            owner = (row[0] or '').strip().upper()
                            name = (row[1] or '').strip().upper()
                            obj_type = (row[2] or '').strip().upper()
                            ref_owner = (row[3] or '').strip().upper()
                            ref_name = (row[4] or '').strip().upper()
                            ref_type = (row[5] or '').strip().upper()
                            if not owner or not name or not ref_owner or not ref_name:
                                continue
                            records.append(DependencyRecord(
                                owner=owner,
                                name=name,
                                object_type=obj_type,
                                referenced_owner=ref_owner,
                                referenced_name=ref_name,
                                referenced_type=ref_type
                            ))
                else:
                    for owner_chunk in owner_chunks:
                        owner_ph = build_bind_placeholders(len(owner_chunk))
                        for ref_chunk in owner_chunks:
                            ref_ph = build_bind_placeholders(len(ref_chunk), offset=len(owner_chunk))
                            sql = sql_tpl.format(owner_ph=owner_ph, ref_ph=ref_ph, types_clause=types_clause)
                            cursor.execute(sql, owner_chunk + ref_chunk)
                            for row in cursor:
                                owner = (row[0] or '').strip().upper()
                                name = (row[1] or '').strip().upper()
                                obj_type = (row[2] or '').strip().upper()
                                ref_owner = (row[3] or '').strip().upper()
                                ref_name = (row[4] or '').strip().upper()
                                ref_type = (row[5] or '').strip().upper()
                                if not owner or not name or not ref_owner or not ref_name:
                                    continue
                                records.append(DependencyRecord(
                                    owner=owner,
                                    name=name,
                                    object_type=obj_type,
                                    referenced_owner=ref_owner,
                                    referenced_name=ref_name,
                                    referenced_type=ref_type
                                ))
    except oracledb.Error as exc:
        log.error(f"严重错误: 加载 Oracle 依赖信息失败: {exc}")
        sys.exit(1)

    log.info("Oracle 依赖信息加载完成，共 %d 条记录。", len(records))
    return records


def load_ob_dependencies(
    ob_cfg: ObConfig,
    target_schemas: Set[str],
    object_types: Optional[Set[str]] = None
) -> Set[Tuple[str, str, str, str]]:
    """
    通过 obclient 读取 OceanBase 侧的依赖信息。
    返回集合 { (OWNER.OBJ, TYPE, REF_OWNER.OBJ, REF_TYPE) }
    """
    if not target_schemas:
        return set()

    owners_in = ",".join(f"'{s}'" for s in sorted(target_schemas))
    enabled_types = {t.upper() for t in (object_types or set(ALL_TRACKED_OBJECT_TYPES))}
    enabled_types &= set(ALL_TRACKED_OBJECT_TYPES)
    if not enabled_types:
        return set()
    types_clause = ",".join(f"'{t}'" for t in sorted(enabled_types))

    sql = f"""
        SELECT OWNER, NAME, TYPE, REFERENCED_OWNER, REFERENCED_NAME, REFERENCED_TYPE
        FROM DBA_DEPENDENCIES
        WHERE OWNER IN ({owners_in})
          AND REFERENCED_OWNER IN ({owners_in})
          AND TYPE IN ({types_clause})
          AND REFERENCED_TYPE IN ({types_clause})
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.error("无法从 OB 读取 DBA_DEPENDENCIES，程序退出。")
        sys.exit(1)

    result: Set[Tuple[str, str, str, str]] = set()
    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 6:
                continue
            owner = parts[0].strip().upper()
            name = parts[1].strip().upper()
            obj_type = parts[2].strip().upper()
            ref_owner = parts[3].strip().upper()
            ref_name = parts[4].strip().upper()
            ref_type = parts[5].strip().upper()
            if not owner or not name or not ref_owner or not ref_name:
                continue
            result.add((
                f"{owner}.{name}",
                obj_type,
                f"{ref_owner}.{ref_name}",
                ref_type
            ))

    log.info("OceanBase 依赖信息加载完成，共 %d 条记录。", len(result))
    return result


def build_expected_dependency_pairs(
    dependencies: List[DependencyRecord],
    full_mapping: FullObjectMapping
) -> Tuple[Set[Tuple[str, str, str, str]], List[DependencyIssue]]:
    """
    将源端依赖映射到目标端 (schema/object 名已替换)。
    返回 (期望依赖集合, 被跳过的依赖列表)。
    """
    expected: Set[Tuple[str, str, str, str]] = set()
    skipped: List[DependencyIssue] = []

    for dep in dependencies:
        dep_key = f"{dep.owner}.{dep.name}".upper()
        ref_key = f"{dep.referenced_owner}.{dep.referenced_name}".upper()
        dep_target = get_mapped_target(full_mapping, dep_key, dep.object_type)
        ref_target = get_mapped_target(full_mapping, ref_key, dep.referenced_type)

        if dep_target is None:
            skipped.append(DependencyIssue(
                dependent=dep_key,
                dependent_type=dep.object_type.upper(),
                referenced=ref_key,
                referenced_type=dep.referenced_type.upper(),
                reason="源对象未纳入受管范围或缺少 remap 规则，无法建立依赖。"
            ))
            continue
        if ref_target is None:
            skipped.append(DependencyIssue(
                dependent=dep_key,
                dependent_type=dep.object_type.upper(),
                referenced=ref_key,
                referenced_type=dep.referenced_type.upper(),
                reason="被依赖对象未纳入受管范围或缺少 remap 规则，无法建立依赖。"
            ))
            continue

        expected.add((
            dep_target.upper(),
            dep.object_type.upper(),
            ref_target.upper(),
            dep.referenced_type.upper()
        ))

    return expected, skipped


def to_raw_dependency_pairs(
    dependencies: List[DependencyRecord]
) -> Set[Tuple[str, str, str, str]]:
    """
    将 DependencyRecord 列表转换为 (dep_full, dep_type, ref_full, ref_type) 集合（源端视角）。
    """
    raw_pairs: Set[Tuple[str, str, str, str]] = set()
    for dep in dependencies:
        dep_key = f"{dep.owner}.{dep.name}".upper()
        ref_key = f"{dep.referenced_owner}.{dep.referenced_name}".upper()
        raw_pairs.add((dep_key, dep.object_type.upper(), ref_key, dep.referenced_type.upper()))
    return raw_pairs


def export_dependency_chains(
    expected_pairs: Set[Tuple[str, str, str, str]],
    output_path: Path,
    source_pairs: Optional[Set[Tuple[str, str, str, str]]] = None
) -> Optional[Path]:
    """
    输出依赖链，支持：
      - 源端（Oracle）依赖链
      - 目标端（Remap 后）依赖链
    依赖链会“下探”直到终点（无进一步依赖或 TABLE/MVIEW），并打印每条路径。
    """
    if not expected_pairs and not source_pairs:
        return None

    def _build_chains(pairs: Set[Tuple[str, str, str, str]], label: str) -> Tuple[List[str], List[str]]:
        if not pairs:
            return [], []
        graph: Dict[str, Set[str]] = defaultdict(set)  # dependent -> {referenced}
        type_map: Dict[str, str] = {}
        reverse_refs: Dict[str, Set[str]] = defaultdict(set)

        for dep_name, dep_type, ref_name, ref_type in pairs:
            dep = dep_name.upper()
            ref = ref_name.upper()
            graph[dep].add(ref)
            reverse_refs[ref].add(dep)
            type_map.setdefault(dep, dep_type.upper())
            type_map.setdefault(ref, ref_type.upper())

        # 选择根节点（未被其他对象引用的节点）；若无根，则全部节点皆为起点
        roots = [n for n in type_map.keys() if n not in reverse_refs]
        if not roots:
            roots = sorted(type_map.keys())

        chains: List[str] = []
        cycles: List[str] = []

        def dfs(node: str, path: List[Tuple[str, str]], seen: Set[str]) -> None:
            node_u = node.upper()
            obj_type = type_map.get(node_u, "UNKNOWN")
            if node_u in seen:
                cycle_path = " -> ".join([f"{n}({t})" for n, t in path] + [f"{node_u}(CYCLE)"])
                cycles.append(cycle_path)
                return
            path_next = path + [(node_u, obj_type)]
            refs = sorted(graph.get(node_u, set()))
            # 终点条件：无引用或到达 TABLE/MATERIALIZED VIEW
            if not refs or obj_type in ("TABLE", "MATERIALIZED VIEW"):
                chains.append(" -> ".join(f"{n}({t})" for n, t in path_next))
                return
            for ref in refs:
                dfs(ref, path_next, seen | {node_u})

        for root in sorted(roots):
            dfs(root, [], set())

        return chains, cycles

    target_chains, target_cycles = _build_chains(expected_pairs, "TARGET")
    source_chains, source_cycles = _build_chains(source_pairs or set(), "SOURCE") if source_pairs else ([], [])

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open('w', encoding='utf-8') as f:
            f.write("# 依赖链下探（终点为 TABLE/MVIEW 或无进一步依赖）\n")
            f.write(f"# 目标端依赖数: {len(expected_pairs)}, 源端依赖数: {len(source_pairs or [])}\n\n")

            if source_chains:
                f.write("[SOURCE - ORACLE] 依赖链:\n")
                for idx, line in enumerate(source_chains, 1):
                    f.write(f"{idx:05d}. {line}\n")
                if source_cycles:
                    f.write("\n[SOURCE] 检测到依赖环:\n")
                    for cyc in source_cycles:
                        f.write(f"- {cyc}\n")
                f.write("\n")

            f.write("[TARGET - REMAPPED] 依赖链:\n")
            for idx, line in enumerate(target_chains, 1):
                f.write(f"{idx:05d}. {line}\n")
            if target_cycles:
                f.write("\n[TARGET] 检测到依赖环:\n")
                for cyc in target_cycles:
                    f.write(f"- {cyc}\n")
            f.write("\n")
    except OSError as exc:
        log.warning("写入依赖链文件失败: %s", exc)
        return None

    return output_path


def build_target_to_source_mapping(
    full_object_mapping: FullObjectMapping
) -> Dict[Tuple[str, str], str]:
    mapping: Dict[Tuple[str, str], str] = {}
    for src_full, type_map in full_object_mapping.items():
        src_full_u = (src_full or "").upper()
        for obj_type, tgt_full in type_map.items():
            key = ((tgt_full or "").upper(), (obj_type or "").upper())
            if not key[0] or not key[1]:
                continue
            mapping.setdefault(key, src_full_u)
    return mapping


def infer_type_from_mapping(
    full_object_mapping: FullObjectMapping,
    source_full: str,
    preferred_types: Tuple[str, ...]
) -> Optional[str]:
    type_map = full_object_mapping.get((source_full or "").upper(), {})
    for obj_type in preferred_types:
        if obj_type in type_map:
            return obj_type
    if type_map:
        return sorted(type_map.keys())[0]
    return None


def resolve_synonym_chain_target(
    synonym_full: str,
    synonym_type: str,
    target_to_source: Dict[Tuple[str, str], str],
    synonym_meta: Optional[Dict[Tuple[str, str], SynonymMeta]],
    full_object_mapping: FullObjectMapping,
    remap_rules: RemapRules
) -> Tuple[Optional[str], Optional[str]]:
    if (synonym_type or "").upper() != "SYNONYM":
        return None, None
    if not synonym_meta:
        return None, None
    syn_full_u = (synonym_full or "").upper()
    src_full = target_to_source.get((syn_full_u, "SYNONYM"), syn_full_u)
    if '.' not in src_full:
        return None, None
    owner, name = src_full.split('.', 1)
    meta = synonym_meta.get((owner, name))
    if not meta or not meta.table_owner or not meta.table_name:
        return None, None
    if meta.db_link:
        return None, None
    target_source = f"{meta.table_owner}.{meta.table_name}".upper()
    mapped = find_mapped_target_any_type(
        full_object_mapping,
        target_source,
        preferred_types=("TABLE", "VIEW", "MATERIALIZED VIEW", "SYNONYM", "FUNCTION", "PROCEDURE", "PACKAGE", "TYPE")
    ) or remap_rules.get(target_source) or target_source
    target_type = infer_type_from_mapping(
        full_object_mapping,
        target_source,
        ("TABLE", "VIEW", "MATERIALIZED VIEW", "SYNONYM", "FUNCTION", "PROCEDURE", "PACKAGE", "TYPE")
    ) or "UNKNOWN"
    return (mapped or "").upper(), target_type


def build_view_fixup_chains(
    view_targets: List[str],
    dependency_pairs: Set[Tuple[str, str, str, str]],
    full_object_mapping: FullObjectMapping,
    remap_rules: RemapRules,
    synonym_meta: Optional[Dict[Tuple[str, str], SynonymMeta]] = None,
    ob_meta: Optional[ObMetadata] = None,
    ob_grant_catalog: Optional[ObGrantCatalog] = None,
    max_depth: int = 30
) -> Tuple[List[str], List[str]]:
    if not view_targets:
        return [], []

    graph: Dict[DependencyNode, Set[DependencyNode]] = defaultdict(set)
    all_nodes: Set[DependencyNode] = set()

    for dep_name, dep_type, ref_name, ref_type in dependency_pairs:
        dep_node = ((dep_name or "").upper(), (dep_type or "").upper())
        ref_node = ((ref_name or "").upper(), (ref_type or "").upper())
        if not dep_node[0] or not dep_node[1] or not ref_node[0] or not ref_node[1]:
            continue
        graph[dep_node].add(ref_node)
        all_nodes.add(dep_node)
        all_nodes.add(ref_node)

    target_to_source = build_target_to_source_mapping(full_object_mapping)
    synonym_target_map: Dict[DependencyNode, DependencyNode] = {}
    for node in sorted(all_nodes):
        if node[1] != "SYNONYM":
            continue
        target_full, target_type = resolve_synonym_chain_target(
            node[0],
            node[1],
            target_to_source,
            synonym_meta,
            full_object_mapping,
            remap_rules
        )
        if not target_full or not target_type:
            continue
        target_node = (target_full, target_type)
        graph[node].add(target_node)
        synonym_target_map[node] = target_node

    def _exists(node: DependencyNode) -> str:
        if not ob_meta:
            return "UNKNOWN"
        obj_type = (node[1] or "").upper()
        obj_full = (node[0] or "").upper()
        obj_set = ob_meta.objects_by_type.get(obj_type)
        if obj_set is None:
            return "UNKNOWN"
        return "EXISTS" if obj_full in obj_set else "MISSING"

    def _grant_status(dep_node: DependencyNode, ref_node: DependencyNode) -> str:
        if not ob_grant_catalog:
            return "GRANT_UNKNOWN"
        if dep_node in synonym_target_map and synonym_target_map.get(dep_node) == ref_node:
            return "GRANT_NA"
        dep_full = (dep_node[0] or "").upper()
        if '.' not in dep_full:
            return "GRANT_UNKNOWN"
        dep_schema = dep_full.split('.', 1)[0]

        ref_full, ref_type = ref_node
        if ref_node in synonym_target_map:
            ref_full, ref_type = synonym_target_map[ref_node]
        ref_full_u = (ref_full or "").upper()
        if '.' not in ref_full_u:
            return "GRANT_UNKNOWN"
        ref_schema = ref_full_u.split('.', 1)[0]
        if dep_schema == ref_schema:
            return "GRANT_OK"

        required_priv = GRANT_PRIVILEGE_BY_TYPE.get((ref_type or "").upper())
        if not required_priv:
            return "GRANT_UNKNOWN"
        obj_key = (dep_schema, required_priv, ref_full_u)
        if obj_key in ob_grant_catalog.object_privs or obj_key in ob_grant_catalog.object_privs_grantable:
            return "GRANT_OK"
        implied = SYS_PRIV_IMPLICATIONS.get(required_priv, set())
        for sys_priv in implied:
            sys_key = (dep_schema, sys_priv)
            if sys_key in ob_grant_catalog.sys_privs or sys_key in ob_grant_catalog.sys_privs_admin:
                return "GRANT_OK"
        return "GRANT_MISSING"

    def _format_chain(path: List[DependencyNode]) -> str:
        parts: List[str] = []
        for idx, node in enumerate(path):
            obj_type = (node[1] or "UNKNOWN").upper()
            exists = _exists(node)
            grant_status = "GRANT_NA" if idx == 0 else _grant_status(path[idx - 1], node)
            parts.append(f"{node[0]}[{obj_type}|{exists}|{grant_status}]")
        return " -> ".join(parts)

    chains: List[str] = []
    cycles: List[str] = []

    def dfs(node: DependencyNode, path: List[DependencyNode], seen: Set[DependencyNode]) -> None:
        if node in seen:
            cycles.append(_format_chain(path + [node]) + " (CYCLE)")
            return
        if len(path) >= max_depth:
            chains.append(_format_chain(path) + " -> ... (DEPTH_LIMIT)")
            return
        refs = sorted(graph.get(node, set()))
        if not refs:
            chains.append(_format_chain(path))
            return
        for ref in refs:
            dfs(ref, path + [ref], seen | {node})

    for view_full in sorted({v.upper() for v in view_targets if v}):
        view_node = (view_full, "VIEW")
        if view_node not in graph:
            chains.append(_format_chain([view_node]))
            continue
        dfs(view_node, [view_node], set())

    return chains, cycles


def export_view_fixup_chains(
    view_targets: List[str],
    dependency_pairs: Set[Tuple[str, str, str, str]],
    output_path: Path,
    full_object_mapping: FullObjectMapping,
    remap_rules: RemapRules,
    synonym_meta: Optional[Dict[Tuple[str, str], SynonymMeta]] = None,
    ob_meta: Optional[ObMetadata] = None,
    ob_grant_catalog: Optional[ObGrantCatalog] = None
) -> Optional[Path]:
    chains, cycles = build_view_fixup_chains(
        view_targets,
        dependency_pairs,
        full_object_mapping,
        remap_rules,
        synonym_meta=synonym_meta,
        ob_meta=ob_meta,
        ob_grant_catalog=ob_grant_catalog
    )
    if not chains:
        return None

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open('w', encoding='utf-8') as f:
            f.write("# VIEW fixup dependency chains\n")
            f.write("# 格式: OWNER.OBJ[TYPE|EXISTS|GRANT_STATUS]\n")
            f.write(f"# views={len(set(view_targets))}, chains={len(chains)}, cycles={len(cycles)}\n\n")
            for idx, line in enumerate(chains, 1):
                f.write(f"{idx:05d}. {line}\n")
            if cycles:
                f.write("\n[CYCLES]\n")
                for cyc in cycles:
                    f.write(f"- {cyc}\n")
        return output_path
    except OSError as exc:
        log.warning("写入 VIEWs_chain 文件失败: %s", exc)
        return None


def check_dependencies_against_ob(
    expected_pairs: Set[Tuple[str, str, str, str]],
    actual_pairs: Set[Tuple[str, str, str, str]],
    skipped: List[DependencyIssue],
    ob_meta: ObMetadata
) -> DependencyReport:
    """
    对比目标端依赖关系，返回缺失/多余/跳过的依赖项。
    """
    report: DependencyReport = {
        "missing": [],
        "unexpected": [],
        "skipped": skipped
    }

    def object_exists(full_name: str, obj_type: str) -> bool:
        return full_name in ob_meta.objects_by_type.get(obj_type.upper(), set())

    def build_missing_reason(dep_name: str, dep_type: str, ref_name: str, ref_type: str) -> str:
        dep_obj = f"{dep_name} ({dep_type})"
        ref_obj = f"{ref_name} ({ref_type})"
        dep_schema = dep_name.split('.', 1)[0]
        ref_schema = ref_name.split('.', 1)[0]
        cross_schema_note = ""
        if dep_schema != ref_schema:
            cross_schema_note = " 跨 schema 依赖，请确认 remap 后的授权（SELECT/EXECUTE/REFERENCES）或同义词已就绪。"

        if dep_type in {"FUNCTION", "PROCEDURE"}:
            action = (
                f"依赖关系未建立：在 OceanBase 执行 ALTER {dep_type} {dep_name} COMPILE；"
                f"如仍失败，请检查 {dep_obj} 中对 {ref_obj} 的调用及授权/Remap。"
            )
        elif dep_type in {"PACKAGE", "PACKAGE BODY"}:
            action = (
                f"依赖关系未建立：执行 ALTER PACKAGE {dep_name} COMPILE 及 ALTER PACKAGE {dep_name} COMPILE BODY，"
                f"确认包定义能够访问 {ref_obj}。"
            )
        elif dep_type == "TRIGGER":
            action = (
                f"依赖关系未建立：执行 ALTER TRIGGER {dep_name} COMPILE，"
                f"确认触发器引用的对象 {ref_obj} 已存在且可访问。"
            )
        elif dep_type in {"VIEW", "MATERIALIZED VIEW"}:
            action = (
                f"依赖关系未建立：请 CREATE OR REPLACE {dep_type} {dep_name}，"
                f"确保所有底层对象（如 {ref_obj}）已存在，再执行 ALTER {dep_type} {dep_name} COMPILE。"
            )
        elif dep_type == "SYNONYM":
            action = (
                f"依赖关系未建立：请重新创建同义词（CREATE OR REPLACE SYNONYM {dep_name} FOR {ref_name}），"
                f"确认 remap 目标和授权正确。"
            )
        elif dep_type in {"TYPE", "TYPE BODY"}:
            compile_stmt = f"ALTER TYPE {dep_name} COMPILE{' BODY' if dep_type == 'TYPE BODY' else ''}"
            action = (
                f"依赖关系未建立：先创建/校验 TYPE 定义，再执行 {compile_stmt}，"
                f"确保 {ref_obj} 已存在且可访问。"
            )
        elif dep_type == "INDEX":
            action = (
                f"依赖关系未建立：请重建索引 {dep_obj}，"
                f"检查索引表达式或函数中对 {ref_obj} 的引用是否有效。"
            )
        elif dep_type == "SEQUENCE":
            action = (
                f"依赖关系未建立：请重新创建序列 {dep_obj}，"
                f"检查同义词或授权设置是否让 {ref_obj} 可见。"
            )
        else:
            action = (
                f"依赖关系未建立：请重新部署 {dep_obj}，"
                f"确认定义中对 {ref_obj} 的引用与 remap/授权保持一致。"
            )

        return action + cross_schema_note

    missing_pairs = expected_pairs - actual_pairs
    extra_pairs = actual_pairs - expected_pairs

    for dep_name, dep_type, ref_name, ref_type in sorted(missing_pairs):
        dep_obj = f"{dep_name} ({dep_type})"
        ref_obj = f"{ref_name} ({ref_type})"
        if not object_exists(dep_name, dep_type):
            reason = f"依赖对象 {dep_obj} 在目标端缺失，请补齐该对象后再重新编译依赖。"
        elif not object_exists(ref_name, ref_type):
            reason = f"被依赖对象 {ref_obj} 在目标端缺失，请先创建/迁移该对象，再重新部署 {dep_obj}。"
        else:
            reason = build_missing_reason(dep_name, dep_type, ref_name, ref_type)
        report["missing"].append(DependencyIssue(
            dependent=dep_name,
            dependent_type=dep_type,
            referenced=ref_name,
            referenced_type=ref_type,
            reason=reason
        ))

    for dep_name, dep_type, ref_name, ref_type in sorted(extra_pairs):
        dep_obj = f"{dep_name} ({dep_type})"
        ref_obj = f"{ref_name} ({ref_type})"
        reason = (
            f"OceanBase 中存在额外依赖 {dep_obj} -> {ref_obj}，"
            f"请确认是否需要保留或清理。"
        )
        report["unexpected"].append(DependencyIssue(
            dependent=dep_name,
            dependent_type=dep_type,
            referenced=ref_name,
            referenced_type=ref_type,
            reason=reason
        ))

    return report


def compute_required_grants(
    expected_pairs: Set[Tuple[str, str, str, str]]
) -> Dict[str, Set[Tuple[str, str]]]:
    grants: Dict[str, Set[Tuple[str, str]]] = defaultdict(set)

    for dep_full, dep_type, ref_full, ref_type in expected_pairs:
        dep_schema, _ = dep_full.split('.', 1)
        ref_schema, _ = ref_full.split('.', 1)
        if dep_schema == ref_schema:
            continue
        privilege = GRANT_PRIVILEGE_BY_TYPE.get(ref_type.upper())
        if not privilege:
            continue
        grants[dep_schema].add((privilege, ref_full))
        # 对外键依赖的表补充 REFERENCES 权限，便于创建 FK
        if ref_type.upper() == 'TABLE' and dep_type.upper() == 'TABLE':
            grants[dep_schema].add(('REFERENCES', ref_full))

    return grants


def filter_existing_required_grants(
    required_grants: Dict[str, Set[Tuple[str, str]]],
    ob_cfg: ObConfig
) -> Dict[str, Set[Tuple[str, str]]]:
    """
    过滤已在 OceanBase 侧满足的 GRANT 建议。

    检查来源：
      1) DBA_TAB_PRIVS 直接对象权限（含对 ROLE 的授权）
      2) DBA_SYS_PRIVS 系统权限（如 SELECT ANY TABLE / EXECUTE ANY PROCEDURE）
      3) DBA_ROLE_PRIVS 角色授予（用于透传 1/2 的授权）

    若任一查询失败，将保守返回原始 required_grants。
    """
    if not required_grants:
        return required_grants

    def _sql_list(vals: Set[str]) -> str:
        safe_vals = [v.replace("'", "''") for v in vals if v]
        return ",".join(f"'{v}'" for v in sorted(safe_vals))

    grantees: Set[str] = {g.upper() for g in required_grants.keys() if g}
    required_entries: List[Tuple[str, str, str]] = []
    owners: Set[str] = set()
    names: Set[str] = set()

    for grantee, entries in required_grants.items():
        g_u = (grantee or "").upper()
        for priv, obj in entries:
            p_u = (priv or "").upper()
            o_u = (obj or "").upper()
            if not g_u or not p_u or not o_u or '.' not in o_u:
                continue
            owner, name = o_u.split('.', 1)
            owners.add(owner)
            names.add(name)
            required_entries.append((g_u, p_u, o_u))

    if not required_entries:
        return required_grants

    roles_by_grantee: Dict[str, Set[str]] = defaultdict(set)
    roles: Set[str] = set()
    try:
        role_sql = f"SELECT GRANTEE, GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE IN ({_sql_list(grantees)})"
        ok, out, _err = obclient_run_sql(ob_cfg, role_sql)
        if ok and out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 2:
                    continue
                gr = parts[0].strip().upper()
                role = parts[1].strip().upper()
                if gr and role:
                    roles_by_grantee[gr].add(role)
                    roles.add(role)
    except Exception as exc:  # pragma: no cover
        log.warning("[GRANT_FILTER] 读取 DBA_ROLE_PRIVS 失败，将仅基于直接授权过滤: %s", exc)

    identities: Set[str] = grantees | roles

    tab_privs: Set[Tuple[str, str, str]] = set()
    try:
        tab_sql = textwrap.dedent(f"""
            SELECT GRANTEE, PRIVILEGE, OWNER, TABLE_NAME
            FROM DBA_TAB_PRIVS
            WHERE GRANTEE IN ({_sql_list(identities)})
              AND OWNER IN ({_sql_list(owners)})
              AND TABLE_NAME IN ({_sql_list(names)})
        """).strip()
        ok, out, _err = obclient_run_sql(ob_cfg, tab_sql)
        if ok and out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 4:
                    continue
                gr = parts[0].strip().upper()
                pv = parts[1].strip().upper()
                ow = parts[2].strip().upper()
                nm = parts[3].strip().upper()
                if gr and pv and ow and nm:
                    tab_privs.add((gr, pv, f"{ow}.{nm}"))
    except Exception as exc:  # pragma: no cover
        log.warning("[GRANT_FILTER] 读取 DBA_TAB_PRIVS 失败，跳过过滤: %s", exc)
        return required_grants

    sys_privs: Dict[str, Set[str]] = defaultdict(set)
    try:
        sys_sql = f"SELECT GRANTEE, PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE IN ({_sql_list(identities)})"
        ok, out, _err = obclient_run_sql(ob_cfg, sys_sql)
        if ok and out:
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) < 2:
                    continue
                gr = parts[0].strip().upper()
                pv = parts[1].strip().upper()
                if gr and pv:
                    sys_privs[gr].add(pv)
    except Exception as exc:  # pragma: no cover
        log.warning("[GRANT_FILTER] 读取 DBA_SYS_PRIVS 失败，将仅基于对象授权过滤: %s", exc)

    def _sys_satisfies(identity: str, required_priv: str) -> bool:
        implied = SYS_PRIV_IMPLICATIONS.get(required_priv, set())
        if not implied:
            return False
        return any(pv in sys_privs.get(identity, set()) for pv in implied)

    def _has_priv(grantee: str, required_priv: str, obj_full: str) -> bool:
        if (grantee, required_priv, obj_full) in tab_privs:
            return True
        for role in roles_by_grantee.get(grantee, set()):
            if (role, required_priv, obj_full) in tab_privs:
                return True
        if _sys_satisfies(grantee, required_priv):
            return True
        for role in roles_by_grantee.get(grantee, set()):
            if _sys_satisfies(role, required_priv):
                return True
        return False

    filtered: Dict[str, Set[Tuple[str, str]]] = {}
    removed = 0
    for grantee, entries in required_grants.items():
        g_u = (grantee or "").upper()
        remaining: Set[Tuple[str, str]] = set()
        for priv, obj in entries:
            p_u = (priv or "").upper()
            o_u = (obj or "").upper()
            if g_u and p_u and o_u and '.' in o_u and _has_priv(g_u, p_u, o_u):
                removed += 1
                continue
            remaining.add((priv, obj))
        if remaining:
            filtered[g_u] = remaining

    if removed:
        log.info("[GRANT_FILTER] 已过滤 %d 条已存在的授权建议。", removed)
    return filtered


def filter_missing_grant_entries(
    object_grants_by_grantee: Dict[str, Set[ObjectGrantEntry]],
    sys_privs_by_grantee: Dict[str, Set[SystemGrantEntry]],
    role_privs_by_grantee: Dict[str, Set[RoleGrantEntry]],
    ob_catalog: Optional[ObGrantCatalog]
) -> Tuple[
    Dict[str, Set[ObjectGrantEntry]],
    Dict[str, Set[SystemGrantEntry]],
    Dict[str, Set[RoleGrantEntry]]
]:
    """
    基于 OB 权限目录过滤已存在的授权，返回缺失授权集合。
    如果 ob_catalog 为空，返回原始集合（缺失集合=全量）。
    """
    if ob_catalog is None:
        return object_grants_by_grantee, sys_privs_by_grantee, role_privs_by_grantee

    miss_obj: Dict[str, Set[ObjectGrantEntry]] = defaultdict(set)
    miss_sys: Dict[str, Set[SystemGrantEntry]] = defaultdict(set)
    miss_role: Dict[str, Set[RoleGrantEntry]] = defaultdict(set)

    obj_basic = ob_catalog.object_privs
    obj_grantable = ob_catalog.object_privs_grantable
    sys_basic = ob_catalog.sys_privs
    sys_admin = ob_catalog.sys_privs_admin
    role_basic = ob_catalog.role_privs
    role_admin = ob_catalog.role_privs_admin

    for grantee, entries in object_grants_by_grantee.items():
        g_u = (grantee or "").upper()
        if not g_u:
            continue
        for entry in entries:
            priv_u = (entry.privilege or "").upper()
            obj_u = (entry.object_full or "").upper()
            if not priv_u or not obj_u:
                continue
            key = (g_u, priv_u, obj_u)
            if entry.grantable:
                if key in obj_grantable:
                    continue
            else:
                if key in obj_basic or key in obj_grantable:
                    continue
            miss_obj[g_u].add(entry)

    for grantee, entries in sys_privs_by_grantee.items():
        g_u = (grantee or "").upper()
        if not g_u:
            continue
        for entry in entries:
            priv_u = (entry.privilege or "").upper()
            if not priv_u:
                continue
            key = (g_u, priv_u)
            if entry.admin_option:
                if key in sys_admin:
                    continue
            else:
                if key in sys_basic or key in sys_admin:
                    continue
            miss_sys[g_u].add(entry)

    for grantee, entries in role_privs_by_grantee.items():
        g_u = (grantee or "").upper()
        if not g_u:
            continue
        for entry in entries:
            role_u = (entry.role or "").upper()
            if not role_u:
                continue
            key = (g_u, role_u)
            if entry.admin_option:
                if key in role_admin:
                    continue
            else:
                if key in role_basic or key in role_admin:
                    continue
            miss_role[g_u].add(entry)

    return miss_obj, miss_sys, miss_role


PRIVILEGE_TYPE_PRIORITY: Tuple[str, ...] = (
    'TABLE',
    'VIEW',
    'MATERIALIZED VIEW',
    'SEQUENCE',
    'TYPE',
    'PACKAGE',
    'FUNCTION',
    'PROCEDURE',
    'TRIGGER',
    'INDEX',
    'SYNONYM',
    'JOB',
    'SCHEDULE'
)


def normalize_privilege_object_type(obj_type: Optional[str]) -> str:
    if not obj_type:
        return ""
    obj_type_u = str(obj_type).strip().upper()
    if obj_type_u in ("PACKAGE BODY", "TYPE BODY"):
        return obj_type_u.replace(" BODY", "")
    return obj_type_u


def infer_privilege_object_type(
    src_full: str,
    source_objects: Optional[SourceObjectMap]
) -> str:
    if not source_objects:
        return ""
    types = source_objects.get(src_full.upper())
    if not types:
        return ""
    types_u = {t.upper() for t in types}
    for cand in PRIVILEGE_TYPE_PRIORITY:
        if cand in types_u:
            return cand
    return sorted(types_u)[0] if types_u else ""


def build_role_name_set(
    role_privileges: List[OracleRolePrivilege],
    source_schema_set: Set[str]
) -> Set[str]:
    roles: Set[str] = {item.role.upper() for item in role_privileges if item.role}
    for item in role_privileges:
        grantee = (item.grantee or "").upper()
        if grantee and grantee not in source_schema_set:
            roles.add(grantee)
    return roles


def remap_grantee_schema(
    grantee: str,
    schema_mapping: Optional[Dict[str, str]],
    role_names: Set[str]
) -> str:
    g_u = (grantee or "").upper()
    if not g_u:
        return g_u
    if g_u == "PUBLIC" or g_u in role_names:
        return g_u
    if schema_mapping and g_u in schema_mapping:
        return schema_mapping[g_u].upper()
    return g_u


def resolve_privilege_target(
    src_full: str,
    obj_type: str,
    full_object_mapping: FullObjectMapping,
    remap_rules: RemapRules,
    source_objects: Optional[SourceObjectMap],
    schema_mapping: Optional[Dict[str, str]],
    object_parent_map: Optional[ObjectParentMap],
    dependency_graph: Optional[DependencyGraph],
    transitive_table_cache: Optional[TransitiveTableCache],
    source_dependencies: Optional[SourceDependencySet],
    source_schema_set: Set[str],
    remap_conflicts: Optional[RemapConflictMap] = None
) -> Optional[str]:
    src_full_u = src_full.upper()
    obj_type_u = normalize_privilege_object_type(obj_type)
    if not obj_type_u:
        obj_type_u = infer_privilege_object_type(src_full_u, source_objects) or obj_type_u
    if remap_conflicts and (src_full_u, obj_type_u) in remap_conflicts:
        return None
    target = get_mapped_target(full_object_mapping, src_full_u, obj_type_u)
    if target:
        return target.upper()
    target = resolve_remap_target(
        src_full_u,
        obj_type_u or obj_type,
        remap_rules,
        source_objects=source_objects,
        schema_mapping=schema_mapping,
        object_parent_map=object_parent_map,
        dependency_graph=dependency_graph,
        transitive_table_cache=transitive_table_cache,
        source_dependencies=source_dependencies,
        remap_conflicts=None
    )
    if target:
        return target.upper()
    src_schema = src_full_u.split('.', 1)[0] if '.' in src_full_u else src_full_u
    if src_schema in source_schema_set:
        return None
    return src_full_u


def resolve_synonym_dependency(
    ref_full: str,
    synonym_meta: Optional[Dict[Tuple[str, str], SynonymMeta]],
    source_objects: Optional[SourceObjectMap]
) -> Tuple[str, str]:
    ref_full_u = ref_full.upper()
    if not synonym_meta or '.' not in ref_full_u:
        return ref_full_u, "SYNONYM"
    owner, name = ref_full_u.split('.', 1)
    visited: Set[Tuple[str, str]] = set()
    cur_owner = owner
    cur_name = name
    for _ in range(12):
        key = (cur_owner, cur_name)
        if key in visited:
            break
        visited.add(key)
        meta = synonym_meta.get(key)
        if not meta or not meta.table_name or not meta.table_owner:
            break
        if meta.db_link:
            break
        target_full = f"{meta.table_owner}.{meta.table_name}".upper()
        target_key = (meta.table_owner.upper(), meta.table_name.upper())
        if target_key in synonym_meta:
            cur_owner, cur_name = target_key
            continue
        obj_type = infer_privilege_object_type(target_full, source_objects) or "TABLE"
        return target_full, obj_type
    obj_type = infer_privilege_object_type(ref_full_u, source_objects) or "SYNONYM"
    return ref_full_u, obj_type


def build_dependency_pairs_for_grants(
    dependencies: List[DependencyRecord],
    full_mapping: FullObjectMapping,
    remap_rules: RemapRules,
    source_objects: Optional[SourceObjectMap],
    schema_mapping: Optional[Dict[str, str]],
    object_parent_map: Optional[ObjectParentMap],
    dependency_graph: Optional[DependencyGraph],
    transitive_table_cache: Optional[TransitiveTableCache],
    source_dependencies: Optional[SourceDependencySet],
    source_schema_set: Set[str],
    remap_conflicts: Optional[RemapConflictMap],
    synonym_meta: Optional[Dict[Tuple[str, str], SynonymMeta]],
    progress_interval: float = 10.0
) -> Set[Tuple[str, str, str, str]]:
    expected: Set[Tuple[str, str, str, str]] = set()
    if not dependencies:
        return expected

    graph: Dict[DependencyNode, Set[DependencyNode]] = defaultdict(set)
    for dep in dependencies:
        dep_full = f"{dep.owner}.{dep.name}".upper()
        dep_type = (dep.object_type or "").upper()
        ref_full = f"{dep.referenced_owner}.{dep.referenced_name}".upper()
        ref_type = (dep.referenced_type or "").upper()
        if not dep_full or not dep_type or not ref_full or not ref_type:
            continue
        graph[(dep_full, dep_type)].add((ref_full, ref_type))

    target_cache: Dict[Tuple[str, str], Optional[str]] = {}
    transitive_cache: Dict[DependencyNode, Set[DependencyNode]] = {}
    view_types = {"VIEW", "MATERIALIZED VIEW"}

    def resolve_target_cached(src_full: str, obj_type: str) -> Optional[str]:
        src_full_u = src_full.upper()
        obj_type_u = normalize_privilege_object_type(obj_type) or (obj_type or "").upper()
        key = (src_full_u, obj_type_u)
        if key in target_cache:
            return target_cache[key]
        target = resolve_privilege_target(
            src_full_u,
            obj_type_u,
            full_mapping,
            remap_rules,
            source_objects,
            schema_mapping,
            object_parent_map,
            dependency_graph,
            transitive_table_cache,
            source_dependencies,
            source_schema_set,
            remap_conflicts
        )
        target_cache[key] = target
        return target

    def add_pair(dep_full: str, dep_type: str, ref_full: str, ref_type: str) -> None:
        dep_full_u = dep_full.upper()
        dep_type_u = (dep_type or "").upper()
        if remap_conflicts and (dep_full_u, dep_type_u) in remap_conflicts:
            return
        dep_target = resolve_target_cached(dep_full_u, dep_type_u)
        if not dep_target:
            return
        ref_full_u = ref_full.upper()
        ref_type_u = (ref_type or "").upper()
        if ref_type_u == "SYNONYM":
            ref_full_u, ref_type_u = resolve_synonym_dependency(ref_full_u, synonym_meta, source_objects)
        ref_target = resolve_target_cached(ref_full_u, ref_type_u)
        if not ref_target:
            return
        if '.' not in dep_target or '.' not in ref_target:
            return
        expected.add((dep_target.upper(), dep_type_u, ref_target.upper(), ref_type_u))

    def get_transitive_refs(node: DependencyNode) -> Set[DependencyNode]:
        if node in transitive_cache:
            return transitive_cache[node]
        seen: Set[DependencyNode] = set()
        stack: List[DependencyNode] = list(graph.get(node, set()))
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            for nxt in graph.get(cur, set()):
                if nxt not in seen:
                    stack.append(nxt)
        transitive_cache[node] = seen
        return seen

    total = len(dependencies)
    if total:
        log.info("[GRANT] 依赖推导授权处理中，共 %d 条依赖记录。", total)
    last_log = time.time()
    progress_interval = max(1.0, progress_interval)

    for idx, dep in enumerate(dependencies, 1):
        dep_full = f"{dep.owner}.{dep.name}".upper()
        dep_type = (dep.object_type or "").upper()
        ref_full = f"{dep.referenced_owner}.{dep.referenced_name}".upper()
        ref_type = (dep.referenced_type or "").upper()
        if not dep_full or not dep_type or not ref_full or not ref_type:
            continue
        add_pair(dep_full, dep_type, ref_full, ref_type)
        if dep_type in view_types:
            for ref_full_t, ref_type_t in get_transitive_refs((dep_full, dep_type)):
                add_pair(dep_full, dep_type, ref_full_t, ref_type_t)
        if total and (idx == total or (time.time() - last_log) >= progress_interval):
            pct = idx * 100.0 / total if total else 100.0
            log.info("[GRANT] 依赖推导进度 %d/%d (%.1f%%)。", idx, total, pct)
            last_log = time.time()

    return expected


def build_grant_plan(
    oracle_meta: OracleMetadata,
    full_mapping: FullObjectMapping,
    remap_rules: RemapRules,
    source_objects: Optional[SourceObjectMap],
    schema_mapping: Optional[Dict[str, str]],
    object_parent_map: Optional[ObjectParentMap],
    dependency_graph: Optional[DependencyGraph],
    transitive_table_cache: Optional[TransitiveTableCache],
    source_dependencies: Optional[SourceDependencySet],
    source_schema_set: Set[str],
    remap_conflicts: Optional[RemapConflictMap],
    synonym_meta: Optional[Dict[Tuple[str, str], SynonymMeta]],
    supported_sys_privs: Optional[Set[str]] = None,
    supported_object_privs: Optional[Set[str]] = None,
    oracle_sys_privs_map: Optional[Set[str]] = None,
    oracle_obj_privs_map: Optional[Set[str]] = None,
    oracle_roles: Optional[Dict[str, OracleRoleInfo]] = None,
    ob_roles: Optional[Set[str]] = None,
    ob_users: Optional[Set[str]] = None,
    include_oracle_maintained_roles: bool = False,
    dependencies: Optional[List[DependencyRecord]] = None,
    progress_interval: float = 10.0
) -> GrantPlan:
    object_grants: Dict[str, Set[ObjectGrantEntry]] = defaultdict(set)
    sys_privs: Dict[str, Set[SystemGrantEntry]] = defaultdict(set)
    role_privs: Dict[str, Set[RoleGrantEntry]] = defaultdict(set)
    role_ddls: List[str] = []
    filtered_grants: List[FilteredGrantEntry] = []

    schema_mapping = schema_mapping or {}
    role_names = build_role_name_set(oracle_meta.role_privileges, source_schema_set)
    skipped_object_privs = 0
    target_cache: Dict[Tuple[str, str], Optional[str]] = {}
    obj_type_cache: Dict[str, str] = {}
    grantee_cache: Dict[str, str] = {}
    supported_sys_privs = {p.upper() for p in (supported_sys_privs or set())}
    if not supported_object_privs:
        supported_object_privs = set(DEFAULT_SUPPORTED_OBJECT_PRIVS)
    supported_object_privs = {p.upper() for p in supported_object_privs}
    oracle_sys_privs_map = oracle_sys_privs_map or oracle_meta.system_privilege_map or set()
    oracle_obj_privs_map = oracle_obj_privs_map or oracle_meta.table_privilege_map or set()
    oracle_roles = oracle_roles or oracle_meta.role_metadata or {}
    ob_roles_input = ob_roles
    ob_users_input = ob_users
    ob_roles = {r.upper() for r in (ob_roles or set())}
    ob_users = {u.upper() for u in (ob_users or set())}
    ob_roles_loaded = ob_roles_input is not None
    ob_users_loaded = ob_users_input is not None
    role_candidates = {r.upper() for r in role_names}
    role_candidates.update({r.upper() for r in oracle_roles.keys()})
    missing_role_grantees: Set[str] = set()
    missing_user_grantees: Set[str] = set()
    skipped_missing_grants = 0

    def map_grantee(grantee: str) -> str:
        g_u = (grantee or "").upper()
        if not g_u:
            return g_u
        cached = grantee_cache.get(g_u)
        if cached is not None:
            return cached
        mapped = remap_grantee_schema(g_u, schema_mapping, role_names)
        grantee_cache[g_u] = mapped
        return mapped

    def resolve_target_cached(src_full: str, obj_type: str) -> Optional[str]:
        src_full_u = src_full.upper()
        obj_type_u = normalize_privilege_object_type(obj_type) or (obj_type or "").upper()
        key = (src_full_u, obj_type_u)
        if key in target_cache:
            return target_cache[key]
        target = resolve_privilege_target(
            src_full_u,
            obj_type_u,
            full_mapping,
            remap_rules,
            source_objects,
            schema_mapping,
            object_parent_map,
            dependency_graph,
            transitive_table_cache,
            source_dependencies,
            source_schema_set,
            remap_conflicts
        )
        target_cache[key] = target
        return target

    view_grant_targets: Set[str] = set()
    if oracle_meta.object_privileges:
        view_types = {"VIEW", "MATERIALIZED VIEW"}
        for item in oracle_meta.object_privileges:
            owner_u = (item.owner or "").upper()
            grantee_u = (item.grantee or "").upper()
            if not owner_u or not grantee_u or grantee_u == owner_u:
                continue
            obj_name_u = (item.object_name or "").upper()
            if not obj_name_u:
                continue
            src_full = f"{owner_u}.{obj_name_u}"
            obj_type = normalize_privilege_object_type(item.object_type)
            if not obj_type:
                obj_type = infer_privilege_object_type(src_full, source_objects) or ""
            if obj_type.upper() not in view_types:
                continue
            target = resolve_target_cached(src_full, obj_type)
            if target:
                view_grant_targets.add(target.upper())

    def record_filtered(category: str, grantee: str, privilege: str, obj_full: str, reason: str) -> None:
        filtered_grants.append(FilteredGrantEntry(
            category=category,
            grantee=grantee,
            privilege=privilege,
            object_full=obj_full,
            reason=reason
        ))

    def is_supported_sys_priv(privilege: str) -> Tuple[bool, Optional[str]]:
        priv_u = (privilege or "").upper()
        if oracle_sys_privs_map and priv_u not in oracle_sys_privs_map:
            return False, "NOT_IN_ORACLE_SYSTEM_PRIVILEGE_MAP"
        if supported_sys_privs and priv_u not in supported_sys_privs:
            return False, "UNSUPPORTED_SYS_PRIV_IN_OB"
        return True, None

    def is_supported_object_priv(privilege: str) -> Tuple[bool, Optional[str]]:
        priv_u = (privilege or "").upper()
        if oracle_obj_privs_map and priv_u not in oracle_obj_privs_map:
            return False, "NOT_IN_ORACLE_OBJECT_PRIVILEGE_MAP"
        if supported_object_privs and priv_u not in supported_object_privs:
            return False, "UNSUPPORTED_OBJECT_PRIV_IN_OB"
        return True, None

    def add_object_grant_entry(grantee: str, privilege: str, object_full: str, grantable: bool) -> None:
        grantee_u = (grantee or "").upper()
        object_u = (object_full or "").upper()
        priv_u = (privilege or "").upper()
        if not grantee_u or not object_u or not priv_u:
            return
        if grantable:
            object_grants[grantee_u].discard(ObjectGrantEntry(priv_u, object_u, False))
            object_grants[grantee_u].add(ObjectGrantEntry(priv_u, object_u, True))
            return
        if ObjectGrantEntry(priv_u, object_u, True) in object_grants[grantee_u]:
            return
        object_grants[grantee_u].add(ObjectGrantEntry(priv_u, object_u, False))

    def format_grantee_list(items: Set[str], limit: int = 30) -> str:
        if not items:
            return ""
        sorted_items = sorted(items)
        if len(sorted_items) <= limit:
            return ", ".join(sorted_items)
        return ", ".join(sorted_items[:limit]) + f"...(+{len(sorted_items) - limit})"

    def grantee_exists(grantee: str) -> bool:
        g_u = (grantee or "").upper()
        if not g_u:
            return False
        if g_u == "PUBLIC":
            return True
        if not ob_roles_loaded and not ob_users_loaded:
            return True
        if ob_users_loaded and g_u in ob_users:
            return True
        if ob_roles_loaded and g_u in ob_roles:
            return True
        is_role = g_u in role_candidates
        if is_role:
            if ob_roles_loaded:
                missing_role_grantees.add(g_u)
                return False
            return True
        if ob_users_loaded:
            missing_user_grantees.add(g_u)
            return False
        return True

    # 1) Source object grants (DBA_TAB_PRIVS)
    total_obj = len(oracle_meta.object_privileges)
    if total_obj:
        log.info("[GRANT] 正在处理对象权限 %d 条...", total_obj)
        if total_obj >= GRANT_WARN_THRESHOLD:
            log.warning(
                "[GRANT] 检测到对象授权规模较大（%d 条），授权规划可能耗时较久，请耐心等待。",
                total_obj
            )
    last_log = time.time()
    progress_interval = max(1.0, progress_interval)

    for idx, item in enumerate(oracle_meta.object_privileges, 1):
        grantee_u = map_grantee(item.grantee)
        if not grantee_exists(grantee_u):
            skipped_missing_grants += 1
            continue
        src_full = f"{item.owner}.{item.object_name}".upper()
        priv_u = (item.privilege or "").upper()
        ok, reason = is_supported_object_priv(priv_u)
        if not ok:
            record_filtered("OBJECT", grantee_u, priv_u, src_full, reason or "UNSUPPORTED_OBJECT_PRIV")
            if total_obj and (idx == total_obj or (time.time() - last_log) >= progress_interval):
                pct = idx * 100.0 / total_obj if total_obj else 100.0
                log.info("[GRANT] 对象权限进度 %d/%d (%.1f%%)。", idx, total_obj, pct)
                last_log = time.time()
            continue
        obj_type = normalize_privilege_object_type(item.object_type)
        if not obj_type:
            obj_type = obj_type_cache.get(src_full, "")
            if not obj_type:
                obj_type = infer_privilege_object_type(src_full, source_objects) or ""
                obj_type_cache[src_full] = obj_type
        target = resolve_target_cached(src_full, obj_type)
        if not target:
            skipped_object_privs += 1
            continue
        add_object_grant_entry(grantee_u, priv_u, target.upper(), bool(item.grantable))
        if total_obj and (idx == total_obj or (time.time() - last_log) >= progress_interval):
            pct = idx * 100.0 / total_obj if total_obj else 100.0
            log.info("[GRANT] 对象权限进度 %d/%d (%.1f%%)。", idx, total_obj, pct)
            last_log = time.time()

    if skipped_object_privs:
        log.warning("[GRANT] 已跳过 %d 条无法映射的对象授权。", skipped_object_privs)

    # 2) System and role grants
    if oracle_meta.sys_privileges:
        log.info("[GRANT] 正在处理系统权限 %d 条...", len(oracle_meta.sys_privileges))
    for item in oracle_meta.sys_privileges:
        grantee_u = map_grantee(item.grantee)
        if not grantee_u:
            continue
        if not grantee_exists(grantee_u):
            skipped_missing_grants += 1
            continue
        priv_u = (item.privilege or "").upper()
        ok, reason = is_supported_sys_priv(priv_u)
        if not ok:
            record_filtered("SYSTEM", grantee_u, priv_u, "", reason or "UNSUPPORTED_SYS_PRIV")
            continue
        sys_privs[grantee_u].add(SystemGrantEntry(priv_u, bool(item.admin_option)))

    if oracle_meta.role_privileges:
        log.info("[GRANT] 正在处理角色授权 %d 条...", len(oracle_meta.role_privileges))
    for item in oracle_meta.role_privileges:
        grantee_u = map_grantee(item.grantee)
        if not grantee_u or not item.role:
            continue
        if not grantee_exists(grantee_u):
            skipped_missing_grants += 1
            continue
        role_privs[grantee_u].add(RoleGrantEntry(item.role.upper(), bool(item.admin_option)))

    # 3) Dependency-derived grants
    dep_records = dependencies or []
    if dep_records:
        expected_pairs = build_dependency_pairs_for_grants(
            dep_records,
            full_mapping,
            remap_rules,
            source_objects,
            schema_mapping,
            object_parent_map,
            dependency_graph,
            transitive_table_cache,
            source_dependencies,
            source_schema_set,
            remap_conflicts,
            synonym_meta,
            progress_interval=progress_interval
        )
        for dep_full, dep_type, ref_full, ref_type in expected_pairs:
            if '.' not in dep_full or '.' not in ref_full:
                continue
            dep_schema = dep_full.split('.', 1)[0]
            ref_schema = ref_full.split('.', 1)[0]
            if dep_schema == ref_schema:
                continue
            if not grantee_exists(dep_schema):
                skipped_missing_grants += 1
                continue
            grantable_for_view = dep_full in view_grant_targets and dep_type.upper() in {"VIEW", "MATERIALIZED VIEW"}
            privilege = GRANT_PRIVILEGE_BY_TYPE.get(ref_type.upper())
            if privilege:
                ok, reason = is_supported_object_priv(privilege)
                if ok:
                    add_object_grant_entry(dep_schema, privilege, ref_full.upper(), grantable_for_view)
                else:
                    record_filtered("OBJECT", dep_schema, privilege, ref_full.upper(), reason or "UNSUPPORTED_OBJECT_PRIV")
            if ref_type.upper() == "TABLE" and dep_type.upper() == "TABLE":
                ok, reason = is_supported_object_priv("REFERENCES")
                if ok:
                    add_object_grant_entry(dep_schema, "REFERENCES", ref_full.upper(), False)
                else:
                    record_filtered("OBJECT", dep_schema, "REFERENCES", ref_full.upper(), reason or "UNSUPPORTED_OBJECT_PRIV")

    if missing_user_grantees:
        log.warning(
            "[GRANT] 目标端缺失用户 %d 个，已跳过相关授权: %s",
            len(missing_user_grantees),
            format_grantee_list(missing_user_grantees)
        )
    if missing_role_grantees:
        log.warning(
            "[GRANT] 目标端缺失角色 %d 个，已跳过相关授权: %s",
            len(missing_role_grantees),
            format_grantee_list(missing_role_grantees)
        )
    if skipped_missing_grants and (missing_user_grantees or missing_role_grantees):
        log.warning(
            "[GRANT] 共跳过 %d 条授权，请先在目标端创建对应用户/角色后重新生成。",
            skipped_missing_grants
        )

    # 4) Role DDL (user-defined roles referenced by grants)
    referenced_roles: Set[str] = set()
    known_roles: Set[str] = {r.upper() for r in oracle_roles.keys()}
    referenced_roles.update({item.role.upper() for item in oracle_meta.role_privileges if item.role})
    if not known_roles:
        known_roles = set(referenced_roles)
    if known_roles:
        for item in oracle_meta.role_privileges:
            grantee = (item.grantee or "").upper()
            if grantee in known_roles:
                referenced_roles.add(grantee)
        for item in oracle_meta.sys_privileges:
            grantee = (item.grantee or "").upper()
            if grantee in known_roles:
                referenced_roles.add(grantee)
        for item in oracle_meta.object_privileges:
            grantee = (item.grantee or "").upper()
            if grantee in known_roles:
                referenced_roles.add(grantee)

    skipped_existing: Set[str] = set()
    skipped_oracle_maintained: Set[str] = set()
    role_warnings: List[str] = []
    for role in sorted(referenced_roles):
        role_u = (role or "").upper()
        if not role_u or role_u == "PUBLIC":
            continue
        info = oracle_roles.get(role_u)
        if info and info.oracle_maintained and not include_oracle_maintained_roles:
            skipped_oracle_maintained.add(role_u)
            continue
        if role_u in ob_roles:
            skipped_existing.add(role_u)
            continue
        ddl_lines: List[str] = []
        if info:
            meta = [
                f"AUTHENTICATION_TYPE={info.authentication_type or '-'}",
                f"PASSWORD_REQUIRED={'YES' if info.password_required else 'NO'}"
            ]
            if info.oracle_maintained is not None:
                meta.append(f"ORACLE_MAINTAINED={'Y' if info.oracle_maintained else 'N'}")
            ddl_lines.append(f"-- ROLE: {role_u} ({', '.join(meta)})")
            if info.authentication_type in ROLE_AUTH_WARN_TYPES or info.password_required:
                role_warnings.append(role_u)
                ddl_lines.append("-- NOTE: 该角色需要密码/外部认证，已生成 NOT IDENTIFIED，需人工补充。")
        else:
            ddl_lines.append(f"-- ROLE: {role_u} (metadata unavailable)")
        ddl_lines.append(f"CREATE ROLE {role_u};")
        role_ddls.extend(ddl_lines)

    if role_ddls:
        log.info("[GRANT] 需生成角色 DDL %d 条。", sum(1 for line in role_ddls if line.startswith("CREATE ROLE ")))
    if skipped_existing:
        log.info("[GRANT] 已存在于 OB 的角色（跳过创建）: %s", ", ".join(sorted(skipped_existing)))
    if skipped_oracle_maintained:
        log.info("[GRANT] Oracle 维护角色已跳过创建: %s", ", ".join(sorted(skipped_oracle_maintained)))
    if role_warnings:
        log.warning("[GRANT] 以下角色需要密码/外部认证，请人工处理: %s", ", ".join(sorted(set(role_warnings))))

    return GrantPlan(
        object_grants=object_grants,
        sys_privs=sys_privs,
        role_privs=role_privs,
        role_ddls=role_ddls,
        filtered_grants=filtered_grants
    )


# ====================== TABLE / VIEW / 其他主对象校验 ======================

def check_primary_objects(
    master_list: MasterCheckList,
    extraneous_rules: List[str],
    ob_meta: ObMetadata,
    oracle_meta: OracleMetadata,
    enabled_primary_types: Optional[Set[str]] = None,
    print_only_types: Optional[Set[str]] = None
) -> ReportResults:
    """
    核心主对象校验：
      - TABLE: 存在性 + 列名集合 (忽略 OMS_OBJECT_NUMBER/OMS_RELATIVE_FNO/OMS_BLOCK_NUMBER/OMS_ROW_NUMBER)
      - LONG/LONG RAW 列要求目标端类型为 CLOB/BLOB
      - VIEW / PROCEDURE / FUNCTION / SYNONYM: 只校验存在性
      - MATERIALIZED VIEW: 默认仅打印不校验（若未被禁用）
      - PACKAGE / PACKAGE BODY: 单独走包有效性对比，不在这里处理
    """
    results: ReportResults = {
        "missing": [],
        "mismatched": [],
        "ok": [],
        "skipped": [],
        "extraneous": extraneous_rules,
        "extra_targets": []
    }

    if not master_list:
        log.info("主校验清单为空，没有需要校验的对象。")
        return results

    log_subsection("主对象校验 (TABLE/VIEW/PROC/FUNC/SYNONYM + PACKAGE 有效性)")

    allowed_types = enabled_primary_types or set(PRIMARY_OBJECT_TYPES)
    print_only_types_u = {t.upper() for t in (print_only_types or set())}

    total = len(master_list)
    expected_targets: Dict[str, Set[str]] = defaultdict(set)
    for i, (src_name, tgt_name, obj_type) in enumerate(master_list):

        if (i + 1) % 100 == 0:
            pct = (i + 1) * 100.0 / total if total else 100.0
            log.info("主对象校验进度 %d/%d (%.1f%%)...", i + 1, total, pct)

        obj_type_u = obj_type.upper()
        try:
            src_schema, src_obj = src_name.split('.')
            tgt_schema, tgt_obj = tgt_name.split('.')
        except ValueError:
            log.warning(f"  [跳过] 对象名格式不正确: src='{src_name}', tgt='{tgt_name}'")
            continue

        src_schema_u = src_schema.upper()
        src_obj_u = src_obj.upper()
        tgt_schema_u = tgt_schema.upper()
        tgt_obj_u = tgt_obj.upper()
        full_tgt = f"{tgt_schema_u}.{tgt_obj_u}"
        if obj_type_u not in allowed_types:
            continue

        if obj_type_u in print_only_types_u:
            reason = PRINT_ONLY_PRIMARY_REASONS.get(obj_type_u, "仅打印不校验")
            results['skipped'].append((obj_type_u, full_tgt, src_name, reason))
            continue

        if obj_type_u in PACKAGE_OBJECT_TYPES:
            # PACKAGE/PKG BODY 使用独立有效性对比逻辑
            continue

        expected_targets[obj_type_u].add(full_tgt)

        if obj_type_u == 'TABLE':
            # 1) OB 是否存在 TABLE
            ob_tables = ob_meta.objects_by_type.get('TABLE', set())
            if full_tgt not in ob_tables:
                results['missing'].append(('TABLE', full_tgt, src_name))
                continue

            # 2) 列级别详细对比 (VARCHAR/VARCHAR2 需 >= 源端长度 * 1.5 向上取整)
            src_cols_details = oracle_meta.table_columns.get((src_schema_u, src_obj_u))
            tgt_cols_details = ob_meta.tab_columns.get((tgt_schema_u, tgt_obj_u), {})

            if src_cols_details is None:
                results['mismatched'].append((
                    'TABLE',
                    f"{full_tgt} (源端列信息获取失败)",
                    set(),
                    set(),
                    [],
                    []
                ))
                continue

            src_col_names = {
                col for col, meta in src_cols_details.items()
                if not is_ignored_source_column(col, meta)
            }
            tgt_col_names = {
                col for col, meta in tgt_cols_details.items()
                if not is_ignored_oms_column(col, meta)
            }

            missing_in_tgt = src_col_names - tgt_col_names
            extra_in_tgt = tgt_col_names - src_col_names
            length_mismatches: List[ColumnLengthIssue] = []
            type_mismatches: List[ColumnTypeIssue] = []

            # 显式提示被忽略名单外的 OMS_* 列属于“多余列”
            extra_oms = {c for c in extra_in_tgt if c.upper().startswith("OMS_")}
            if extra_oms:
                log.debug("表 %s 发现额外 OMS_* 列: %s", full_tgt, sorted(extra_oms))

            # 检查公共列的长度
            common_cols = src_col_names & tgt_col_names
            for col_name in common_cols:
                src_info = src_cols_details[col_name]
                tgt_info = tgt_cols_details[col_name]

                src_dtype = (src_info.get("data_type") or "").upper()
                tgt_dtype = (tgt_info.get("data_type") or "").upper()

                if is_long_type(src_dtype):
                    expected_type = map_long_type_to_ob(src_dtype)
                    if (tgt_dtype or "UNKNOWN") != expected_type:
                        type_mismatches.append(
                            ColumnTypeIssue(
                                col_name,
                                src_dtype or "UNKNOWN",
                                tgt_dtype or "UNKNOWN",
                                expected_type
                            )
                        )
                    continue

                if src_dtype in ('VARCHAR2', 'VARCHAR'):
                    src_len = src_info.get("char_length") or src_info.get("data_length")
                    tgt_len = tgt_info.get("char_length") or tgt_info.get("data_length")

                    try:
                        src_len_int = int(src_len)
                        tgt_len_int = int(tgt_len)
                    except (TypeError, ValueError):
                        continue

                    # 区分BYTE和CHAR语义：CHAR_USED='C'表示CHAR语义，其他为BYTE语义
                    src_char_used = (src_info.get("char_used") or "").strip().upper()
                    
                    if src_char_used == 'C':
                        # CHAR语义：要求长度完全一致
                        if tgt_len_int != src_len_int:
                            length_mismatches.append(
                                ColumnLengthIssue(col_name, src_len_int, tgt_len_int, src_len_int, 'char_mismatch')
                            )
                    else:
                        # BYTE语义：需要放大1.5倍
                        expected_min_len = int(math.ceil(src_len_int * VARCHAR_LEN_MIN_MULTIPLIER))
                        oversize_cap_len = int(math.ceil(src_len_int * VARCHAR_LEN_OVERSIZE_MULTIPLIER))
                        if tgt_len_int < expected_min_len:
                            length_mismatches.append(
                                ColumnLengthIssue(col_name, src_len_int, tgt_len_int, expected_min_len, 'short')
                            )
                        elif tgt_len_int > oversize_cap_len:
                            length_mismatches.append(
                                ColumnLengthIssue(col_name, src_len_int, tgt_len_int, oversize_cap_len, 'oversize')
                            )

            if not missing_in_tgt and not extra_in_tgt and not length_mismatches and not type_mismatches:
                results['ok'].append(('TABLE', full_tgt))
            else:
                results['mismatched'].append((
                    'TABLE',
                    full_tgt,
                    missing_in_tgt,
                    extra_in_tgt,
                    length_mismatches,
                    type_mismatches
                ))

        elif obj_type_u in PRIMARY_EXISTENCE_ONLY_TYPES:
            ob_set = ob_meta.objects_by_type.get(obj_type_u, set())
            if full_tgt in ob_set:
                results['ok'].append((obj_type_u, full_tgt))
            else:
                results['missing'].append((obj_type_u, full_tgt, src_name))

        else:
            # 不在主对比范围的类型直接忽略
            continue

    # 记录目标端多出的对象（任何受管类型）
    for obj_type in sorted(allowed_types - print_only_types_u):
        actual = ob_meta.objects_by_type.get(obj_type, set())
        expected = expected_targets.get(obj_type, set())
        extras = sorted(actual - expected)
        for tgt in extras:
            results['extra_targets'].append((obj_type, tgt))

    return results


def supplement_missing_views_from_mapping(
    tv_results: ReportResults,
    full_object_mapping: FullObjectMapping,
    ob_meta: ObMetadata,
    enabled_primary_types: Optional[Set[str]] = None
) -> int:
    """
    当主对象校验未能产出 VIEW 缺失清单时，基于映射+目标端对象集补齐。
    用于保障 fixup/report 能进入 VIEW 生成流程。
    """
    enabled_types = {t.upper() for t in (enabled_primary_types or set(PRIMARY_OBJECT_TYPES))}
    if 'VIEW' not in enabled_types:
        return 0

    missing_list = tv_results.get("missing", [])
    existing_missing = {
        (src_name.upper(), tgt_name.upper())
        for obj_type, tgt_name, src_name in missing_list
        if (obj_type or "").upper() == "VIEW"
    }

    ob_views = {name.upper() for name in ob_meta.objects_by_type.get("VIEW", set())}
    expected_pairs: List[Tuple[str, str]] = []
    for src_full, type_map in full_object_mapping.items():
        tgt_full = type_map.get("VIEW")
        if not tgt_full or "." not in tgt_full or "." not in src_full:
            continue
        expected_pairs.append((src_full.upper(), tgt_full.upper()))

    added = 0
    for src_full, tgt_full in expected_pairs:
        if tgt_full in ob_views:
            continue
        if (src_full, tgt_full) in existing_missing:
            continue
        missing_list.append(("VIEW", tgt_full, src_full))
        existing_missing.add((src_full, tgt_full))
        added += 1

    if added:
        log.warning("[VIEW] 缺失视图清单补齐 %d 条（基于映射与目标端对象集）。", added)
    return added


# ====================== 扩展：索引 / 约束 / 序列 / 触发器 ======================

def normalize_object_status(status: Optional[str]) -> str:
    if status is None:
        return "UNKNOWN"
    status_u = str(status).strip().upper()
    return status_u if status_u else "UNKNOWN"


def normalize_error_text(text: Optional[str]) -> str:
    if text is None:
        return ""
    cleaned = str(text).replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return " ".join(cleaned.split())


def compare_package_objects(
    master_list: MasterCheckList,
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    enabled_primary_types: Optional[Set[str]] = None
) -> PackageCompareResults:
    """
    对 PACKAGE / PACKAGE BODY 做存在性 + VALID/INVALID 对比，并记录错误信息（若可用）。
    SOURCE_INVALID 不计入 mismatch 统计，但会列出详情。
    """
    enabled_types = {t.upper() for t in (enabled_primary_types or set(PRIMARY_OBJECT_TYPES))}
    if not (set(PACKAGE_OBJECT_TYPES) & enabled_types):
        return {"rows": [], "summary": {}}

    rows: List[PackageCompareRow] = []
    summary: Dict[str, int] = defaultdict(int)
    ob_pkg_set = ob_meta.objects_by_type.get("PACKAGE", set())
    ob_pkg_body_set = ob_meta.objects_by_type.get("PACKAGE BODY", set())

    for src_name, tgt_name, obj_type in master_list:
        obj_type_u = obj_type.upper()
        if obj_type_u not in PACKAGE_OBJECT_TYPES:
            continue
        if "." not in src_name or "." not in tgt_name:
            continue
        src_schema, src_obj = src_name.split(".", 1)
        tgt_schema, tgt_obj = tgt_name.split(".", 1)
        src_schema_u = src_schema.upper()
        src_obj_u = src_obj.upper()
        tgt_schema_u = tgt_schema.upper()
        tgt_obj_u = tgt_obj.upper()
        src_full = f"{src_schema_u}.{src_obj_u}"
        tgt_full = f"{tgt_schema_u}.{tgt_obj_u}"

        src_status = normalize_object_status(
            oracle_meta.object_statuses.get((src_schema_u, src_obj_u, obj_type_u))
        )
        if obj_type_u == "PACKAGE":
            tgt_exists = tgt_full in ob_pkg_set
        else:
            tgt_exists = tgt_full in ob_pkg_body_set
        tgt_status = "MISSING"
        if tgt_exists:
            tgt_status = normalize_object_status(
                ob_meta.object_statuses.get((tgt_schema_u, tgt_obj_u, obj_type_u))
            )

        error_count = 0
        first_error = ""

        if src_status == "INVALID":
            result = "SOURCE_INVALID"
            err = oracle_meta.package_errors.get((src_schema_u, src_obj_u, obj_type_u))
            if err:
                error_count = err.count
                first_error = err.first_error
        elif not tgt_exists:
            result = "MISSING_TARGET"
        else:
            if src_status == "VALID" and tgt_status == "VALID":
                result = "OK"
            elif tgt_status == "INVALID":
                result = "TARGET_INVALID"
                err = ob_meta.package_errors.get((tgt_schema_u, tgt_obj_u, obj_type_u))
                if err:
                    error_count = err.count
                    first_error = err.first_error
            else:
                result = "STATUS_MISMATCH"

        summary[result] += 1
        rows.append(PackageCompareRow(
            src_full=src_full,
            obj_type=obj_type_u,
            src_status=src_status,
            tgt_full=tgt_full,
            tgt_status=tgt_status,
            result=result,
            error_count=error_count,
            first_error=first_error
        ))

    return {
        "rows": rows,
        "summary": dict(summary),
        "diff_rows": [row for row in rows if row.result != "OK"]
    }

def compare_indexes_for_table(
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str
) -> Tuple[bool, Optional[IndexMismatch]]:
    src_key = (src_schema.upper(), src_table.upper())
    src_idx = oracle_meta.indexes.get(src_key)
    tgt_key = (tgt_schema.upper(), tgt_table.upper())
    tgt_idx = ob_meta.indexes.get(tgt_key, {})

    # 源端元数据为 None 时，视为源端该表无索引（或元数据加载为空），继续比较并标记目标端 extra
    if src_idx is None:
        src_idx = {}

    tgt_constraints = ob_meta.constraints.get(tgt_key, {})
    constraint_index_cols: Set[Tuple[str, ...]] = {
        normalize_column_sequence(cons.get("columns"))
        for cons in tgt_constraints.values()
        if (cons.get("type") or "").upper() in ("P", "U")
    }

    def build_index_map(entries: Dict[str, Dict]) -> Dict[Tuple[str, ...], Dict[str, Set[str]]]:
        result: Dict[Tuple[str, ...], Dict[str, Set[str]]] = {}
        for name, info in entries.items():
            cols = normalize_column_sequence(info.get("columns"))
            if not cols:
                continue
            uniq = (info.get("uniqueness") or "").upper()
            bucket = result.setdefault(cols, {"names": set(), "uniq": set()})
            bucket["names"].add(name)
            bucket["uniq"].add(uniq)
        return result

    src_map = build_index_map(src_idx)
    tgt_map = build_index_map(tgt_idx)

    def rep_name(entry_map: Dict[Tuple[str, ...], Dict[str, Set[str]]], cols: Tuple[str, ...]) -> str:
        names = entry_map.get(cols, {}).get("names") or []
        return next(iter(names), f"{cols}")

    missing_cols = set(src_map.keys()) - set(tgt_map.keys())
    extra_cols = set(tgt_map.keys()) - set(src_map.keys())

    # 处理同名索引但SYS_NC列名不同的情况
    def normalize_sys_nc_columns(cols: Tuple[str, ...]) -> Tuple[str, ...]:
        """
        将SYS_NC开头的列名标准化为通用形式。
        Oracle可能生成多种格式：SYS_NC00001$, SYS_NC_OID$, SYS_NC_ROWINFO$ 等
        """
        normalized = []
        for col in cols:
            # 匹配多种SYS_NC格式：
            # - SYS_NC<digits>$ (如 SYS_NC00001$)
            # - SYS_NC_<WORD>$ (如 SYS_NC_OID$, SYS_NC_ROWINFO$)
            if re.match(r'^SYS_NC\d+\$', col) or re.match(r'^SYS_NC_[A-Z_]+\$', col):
                normalized.append('SYS_NC$')  # 标准化为通用形式
            else:
                normalized.append(col)
        return tuple(normalized)

    def has_same_named_index(src_cols: Tuple[str, ...], tgt_cols: Tuple[str, ...]) -> bool:
        """检查是否存在同名索引"""
        src_names = src_map.get(src_cols, {}).get("names", set())
        tgt_names = tgt_map.get(tgt_cols, {}).get("names", set())
        return bool(src_names & tgt_names)  # 有交集说明存在同名索引

    def is_sys_nc_only_diff(src_cols: Tuple[str, ...], tgt_cols: Tuple[str, ...]) -> bool:
        """检查是否仅SYS_NC列名不同"""
        return normalize_sys_nc_columns(src_cols) == normalize_sys_nc_columns(tgt_cols)

    # 找出因SYS_NC列名不同而被误判的同名索引
    sys_nc_matched_pairs = []
    for src_cols in list(missing_cols):
        for tgt_cols in list(extra_cols):
            if (has_same_named_index(src_cols, tgt_cols) and 
                is_sys_nc_only_diff(src_cols, tgt_cols)):
                sys_nc_matched_pairs.append((src_cols, tgt_cols))
                missing_cols.discard(src_cols)
                extra_cols.discard(tgt_cols)
                break

    detail_mismatch: List[str] = []

    for cols in set(src_map.keys()) & set(tgt_map.keys()):
        src_uniq = src_map[cols]["uniq"]
        tgt_uniq = tgt_map[cols]["uniq"]
        if src_uniq != tgt_uniq:
            # 检查是否是源端NONUNIQUE变成目标端UNIQUE的情况
            src_has_nonunique = "NONUNIQUE" in src_uniq
            tgt_has_unique = "UNIQUE" in tgt_uniq
            
            # 如果源端是NONUNIQUE，目标端是UNIQUE，且该列集有约束支撑，则认为是正常的
            if src_has_nonunique and tgt_has_unique and cols in constraint_index_cols:
                log.debug("索引列 %s: 源端NONUNIQUE变目标端UNIQUE，有约束支撑，视为正常迁移", list(cols))
                continue
            
            # 其他唯一性不一致情况仍然报告
            detail_mismatch.append(
                f"索引列 {list(cols)} 唯一性不一致 (源 {sorted(src_uniq)}, 目标 {sorted(tgt_uniq)})。"
            )

    filtered_missing_cols: Set[Tuple[str, ...]] = set()
    for cols in missing_cols:
        # 如果已有 PK/UK 约束覆盖了同一列集，则视为已有唯一性支持，不再要求单独索引
        if cols in constraint_index_cols:
            continue
        filtered_missing_cols.add(cols)

    missing = {rep_name(src_map, cols) for cols in filtered_missing_cols}
    extra = {rep_name(tgt_map, cols) for cols in extra_cols}

    for cols in filtered_missing_cols:
        detail_mismatch.append(
            f"索引列 {list(cols)} 在目标端未找到。"
        )

    for cols in extra_cols:
        detail_mismatch.append(
            f"目标端存在额外索引列集 {list(cols)}。"
        )

    all_good = (not missing) and (not extra) and not detail_mismatch
    if all_good:
        return True, None
    else:
        return False, IndexMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_indexes=missing,
            extra_indexes=extra,
            detail_mismatch=detail_mismatch
        )


def compare_constraints_for_table(
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
    full_object_mapping: FullObjectMapping
) -> Tuple[bool, Optional[ConstraintMismatch]]:
    src_key = (src_schema.upper(), src_table.upper())
    src_cons = oracle_meta.constraints.get(src_key)
    tgt_key = (tgt_schema.upper(), tgt_table.upper())
    tgt_cons = ob_meta.constraints.get(tgt_key, {})

    # 源端元数据为 None 时，视为源端该表无约束（或元数据加载为空），继续比较并标记目标端 extra
    if src_cons is None:
        src_cons = {}

    detail_mismatch: List[str] = []
    missing: Set[str] = set()
    extra: Set[str] = set()

    source_all_cols: Set[Tuple[str, ...]] = {
        normalize_column_sequence(cons.get("columns"))
        for cons in src_cons.values()
    }

    def bucket_pk_uk(cons_dict: Dict[str, Dict]) -> Dict[str, List[Tuple[Tuple[str, ...], str]]]:
        buckets: Dict[str, List[Tuple[Tuple[str, ...], str]]] = {'P': [], 'U': []}
        for name, cons in cons_dict.items():
            ctype = (cons.get("type") or "").upper()
            if ctype not in buckets:
                continue
            cols = normalize_column_sequence(cons.get("columns"))
            buckets[ctype].append((cols, name))
        return buckets

    def bucket_fk(
        cons_dict: Dict[str, Dict],
        *,
        is_source: bool
    ) -> List[Tuple[Tuple[str, ...], str, Optional[str]]]:
        entries: List[Tuple[Tuple[str, ...], str, Optional[str]]] = []
        for name, cons in cons_dict.items():
            ctype = (cons.get("type") or "").upper()
            if ctype != 'R':
                continue
            cols = normalize_column_sequence(cons.get("columns"))
            ref_full: Optional[str] = None
            ref_owner = cons.get("ref_table_owner")
            ref_name = cons.get("ref_table_name")
            if ref_owner and ref_name:
                ref_full_raw = f"{str(ref_owner).upper()}.{str(ref_name).upper()}"
                if is_source:
                    # 源端FK引用：应用remap规则
                    mapped = get_mapped_target(full_object_mapping, ref_full_raw, 'TABLE')
                    ref_full = (mapped or ref_full_raw).upper()
                else:
                    # 目标端FK引用：使用原始名称（目标端已经是remapped之后的名称）
                    ref_full = ref_full_raw.upper()
            entries.append((cols, name, ref_full))
        return entries

    grouped_src_pkuk = bucket_pk_uk(src_cons)
    grouped_tgt_pkuk = bucket_pk_uk(tgt_cons)
    grouped_src_fk = bucket_fk(src_cons, is_source=True)
    grouped_tgt_fk = bucket_fk(tgt_cons, is_source=False)

    def match_constraints(
        label: str,
        src_list: List[Tuple[Tuple[str, ...], str]],
        tgt_list: List[Tuple[Tuple[str, ...], str]]
    ) -> None:
        tgt_used = [False] * len(tgt_list)
        for cols, name in src_list:
            found_idx = None
            for idx, (t_cols, _t_name) in enumerate(tgt_list):
                if tgt_used[idx]:
                    continue
                if cols == t_cols:
                    found_idx = idx
                    tgt_used[idx] = True
                    break
            if found_idx is None:
                missing.add(name)
                detail_mismatch.append(
                    f"{label}: 源约束 {name} (列 {list(cols)}) 在目标端未找到。"
                )
        for idx, used in enumerate(tgt_used):
            if not used:
                extra_name = tgt_list[idx][1]
                extra_cols = tgt_list[idx][0]
                if extra_cols in source_all_cols:
                    continue
                if "_OMS_ROWID" in (extra_name or ""):
                    continue
                extra.add(extra_name)
                detail_mismatch.append(
                    f"{label}: 目标端存在额外约束 {extra_name} (列 {list(extra_cols)})。"
                )

    def match_foreign_keys(
        src_list: List[Tuple[Tuple[str, ...], str, Optional[str]]],
        tgt_list: List[Tuple[Tuple[str, ...], str, Optional[str]]]
    ) -> None:
        tgt_used = [False] * len(tgt_list)
        tgt_by_cols: Dict[Tuple[str, ...], Set[Optional[str]]] = defaultdict(set)
        for cols, _name, ref in tgt_list:
            tgt_by_cols[cols].add(ref)

        for cols, name, src_ref in src_list:
            found_idx = None
            for idx, (t_cols, _t_name, t_ref) in enumerate(tgt_list):
                if tgt_used[idx]:
                    continue
                if cols != t_cols:
                    continue
                if src_ref and t_ref and src_ref != t_ref:
                    continue
                found_idx = idx
                tgt_used[idx] = True
                break
            if found_idx is None:
                missing.add(name)
                if src_ref and cols in tgt_by_cols and (src_ref not in tgt_by_cols[cols]):
                    detail_mismatch.append(
                        f"FOREIGN KEY: 源约束 {name} (列 {list(cols)}) 引用 {src_ref}，但目标端同列集引用 {sorted(tgt_by_cols[cols])}。"
                    )
                else:
                    detail_mismatch.append(
                        f"FOREIGN KEY: 源约束 {name} (列 {list(cols)}) 在目标端未找到。"
                    )

        for idx, used in enumerate(tgt_used):
            if not used:
                extra_name, extra_cols, extra_ref = tgt_list[idx]
                if extra_cols in source_all_cols:
                    continue
                if "_OMS_ROWID" in (extra_name or ""):
                    continue
                extra.add(extra_name)
                ref_note = f" 引用 {extra_ref}" if extra_ref else ""
                detail_mismatch.append(
                    f"FOREIGN KEY: 目标端存在额外约束 {extra_name} (列 {list(extra_cols)}){ref_note}。"
                )

    match_constraints("PRIMARY KEY", grouped_src_pkuk.get('P', []), grouped_tgt_pkuk.get('P', []))
    match_constraints("UNIQUE KEY", grouped_src_pkuk.get('U', []), grouped_tgt_pkuk.get('U', []))
    match_foreign_keys(grouped_src_fk, grouped_tgt_fk)

    all_good = (not missing) and (not extra) and (not detail_mismatch)
    if all_good:
        return True, None
    else:
        return False, ConstraintMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_constraints=missing,
            extra_constraints=extra,
            detail_mismatch=detail_mismatch
        )


def compare_sequences_for_schema(
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    src_schema: str,
    tgt_schema: str
) -> Tuple[bool, Optional[SequenceMismatch]]:
    src_seqs = oracle_meta.sequences.get(src_schema.upper())
    if src_seqs is None:
        log.warning(f"[序列检查] 未找到 {src_schema} 的 Oracle 序列元数据。")
        tgt_seqs_snapshot = ob_meta.sequences.get(tgt_schema.upper(), set())
        
        # 改进：区分元数据加载失败 vs schema确实没有序列
        # 检查该schema是否在已加载的对象元数据中出现（说明schema存在）
        schema_u = src_schema.upper()
        schema_has_objects = any(
            owner == schema_u
            for owner, _ in oracle_meta.table_columns.keys()
        )
        if not schema_has_objects:
            for owner, _ in oracle_meta.indexes.keys():
                if owner == schema_u:
                    schema_has_objects = True
                    break
        if not schema_has_objects:
            for owner, _ in oracle_meta.constraints.keys():
                if owner == schema_u:
                    schema_has_objects = True
                    break
        if not schema_has_objects:
            for owner, _ in oracle_meta.triggers.keys():
                if owner == schema_u:
                    schema_has_objects = True
                    break
        if not schema_has_objects:
            for owner, _ in oracle_meta.table_comments.keys():
                if owner == schema_u:
                    schema_has_objects = True
                    break
        if not schema_has_objects:
            for owner, _ in oracle_meta.column_comments.keys():
                if owner == schema_u:
                    schema_has_objects = True
                    break

        if not schema_has_objects:
            # Schema不存在于元数据中，可能是配置错误或权限问题，跳过比较
            note = f"源端schema {src_schema} 未在Oracle元数据中找到，跳过序列比较。"
            log.info(note)
            return True, None  # 跳过，不报告不一致
        
        # Schema存在但sequences为None，说明确实没有序列（或DBA_SEQUENCES查询为空）
        note = (
            f"Oracle schema {src_schema} 中无序列定义"
            f"（DBA_SEQUENCES未返回记录，可能schema确实无序列或权限不足）。"
        )
        if tgt_seqs_snapshot:
            note += f" 目标端存在序列：{', '.join(sorted(tgt_seqs_snapshot))}。"
            # 源端没有，目标端有，报告为额外序列
            return False, SequenceMismatch(
                src_schema=src_schema,
                tgt_schema=tgt_schema,
                missing_sequences=set(),
                extra_sequences=tgt_seqs_snapshot,
                note=note,
                missing_mappings=[]
            )
        else:
            # 双方都没有序列，认为一致
            log.debug(f"[序列检查] 源端和目标端都无序列，认为一致。")
            return True, None

    tgt_seqs = ob_meta.sequences.get(tgt_schema.upper(), set())

    missing = src_seqs - tgt_seqs
    extra = tgt_seqs - src_seqs
    all_good = (not missing) and (not extra)
    if all_good:
        return True, None
    else:
        return False, SequenceMismatch(
            src_schema=src_schema,
            tgt_schema=tgt_schema,
            missing_sequences=missing,
            extra_sequences=extra,
            note=None,
            missing_mappings=[
                (f"{src_schema.upper()}.{seq}", f"{tgt_schema.upper()}.{seq}")
                for seq in sorted(missing)
            ]
        )


def compare_triggers_for_table(
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
    full_object_mapping: FullObjectMapping
) -> Tuple[bool, Optional[TriggerMismatch]]:
    src_key = (src_schema.upper(), src_table.upper())
    src_trg = oracle_meta.triggers.get(src_key) or {}
    tgt_key = (tgt_schema.upper(), tgt_table.upper())
    tgt_trg = ob_meta.triggers.get(tgt_key, {})

    # 源端没有触发器（空字典或空集合）
    if not src_trg:
        # 如果目标端也没有触发器，则视为一致
        if not tgt_trg:
            return True, None
        # 源端确实没有触发器但目标端有，记录目标端额外的触发器
        extra_triggers: Set[str] = set()
        for name, info in tgt_trg.items():
            owner_u = (info.get("owner") or tgt_schema).upper()
            extra_triggers.add(f"{owner_u}.{name.upper()}")
        return False, TriggerMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_triggers=set(),
            extra_triggers=extra_triggers,
            detail_mismatch=[f"源端无触发器，目标端存在额外触发器: {', '.join(sorted(extra_triggers))}"],
            missing_mappings=[]
        )

    src_names_raw = set(src_trg.keys())
    tgt_full_names: Set[str] = set()
    tgt_info_map: Dict[str, Dict] = {}
    for name, info in tgt_trg.items():
        owner_u = (info.get("owner") or tgt_schema).upper()
        name_u = name.upper()
        full = f"{owner_u}.{name_u}"
        tgt_full_names.add(full)
        tgt_info_map[full] = info

    src_target_full: Set[str] = set()
    target_name_map: Dict[str, Tuple[str, str, str, str]] = {}
    for name in src_names_raw:
        info = src_trg.get(name) or {}
        trg_owner = (info.get("owner") or src_schema).upper()
        name_u = name.upper()
        src_full = f"{trg_owner}.{name_u}"
        mapped = get_mapped_target(full_object_mapping, src_full, 'TRIGGER')
        if mapped and '.' in mapped:
            tgt_owner, tgt_name = mapped.split('.', 1)
            tgt_owner_u = tgt_owner.upper()
            tgt_name_u = tgt_name.upper()
        else:
            tgt_owner_u = trg_owner or src_schema.upper()
            tgt_name_u = name_u
            ensure_mapping_entry(
                full_object_mapping,
                src_full,
                'TRIGGER',
                f"{tgt_owner_u}.{tgt_name_u}"
            )
        tgt_full = f"{tgt_owner_u}.{tgt_name_u}"
        src_target_full.add(tgt_full)
        target_name_map[tgt_full] = (trg_owner, name_u, tgt_owner_u, tgt_name_u)

    missing = src_target_full - tgt_full_names
    extra = tgt_full_names - src_target_full
    detail_mismatch: List[str] = []
    missing_mappings: List[Tuple[str, str]] = []

    for tgt_full in sorted(missing):
        tgt_parts = tgt_full.split('.', 1)
        tgt_name_fallback = tgt_parts[1] if len(tgt_parts) > 1 else tgt_full
        src_owner, src_name, tgt_owner, tgt_name = target_name_map.get(
            tgt_full,
            (src_schema.upper(), tgt_name_fallback, tgt_schema.upper(), tgt_name_fallback)
        )
        missing_mappings.append(
            (
                f"{src_owner}.{src_name}",
                f"{tgt_owner}.{tgt_name}"
            )
        )

    common = src_target_full & tgt_full_names
    for tgt_full in common:
        src_info = target_name_map.get(tgt_full)
        src_info_name = src_info[1] if src_info else tgt_full.split('.', 1)[1]
        s = src_trg.get(src_info_name) or {}
        t = tgt_info_map.get(tgt_full, {})
        display_name = tgt_full
        if (s.get("event") or "").strip() != (t.get("event") or "").strip():
            detail_mismatch.append(
                f"{display_name}: 触发事件不一致 (src={s.get('event')}, tgt={t.get('event')})"
            )
        if (s.get("status") or "").strip() != (t.get("status") or "").strip():
            detail_mismatch.append(
                f"{display_name}: 状态不一致 (src={s.get('status')}, tgt={t.get('status')})"
            )

    all_good = (not missing) and (not extra) and (not detail_mismatch)
    if all_good:
        return True, None
    else:
        return False, TriggerMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_triggers=missing,
            extra_triggers=extra,
            detail_mismatch=detail_mismatch,
            missing_mappings=missing_mappings
        )


def check_extra_objects(
    settings: Dict,
    master_list: MasterCheckList,
    ob_meta: ObMetadata,
    oracle_meta: OracleMetadata,
    full_object_mapping: FullObjectMapping,
    enabled_extra_types: Optional[Set[str]] = None
) -> ExtraCheckResults:
    """
    基于 master_list (TABLE 映射) 检查：
      - 索引
      - 约束 (PK/UK/FK)
      - 触发器
    基于 schema 映射检查：
      - 序列
    """
    extra_results: ExtraCheckResults = {
        "index_ok": [],
        "index_mismatched": [],
        "constraint_ok": [],
        "constraint_mismatched": [],
        "sequence_ok": [],
        "sequence_mismatched": [],
        "trigger_ok": [],
        "trigger_mismatched": [],
    }

    enabled_types = {t.upper() for t in (enabled_extra_types or set(EXTRA_OBJECT_CHECK_TYPES))}

    if not master_list:
        return extra_results

    if not enabled_types:
        log.info("已根据配置跳过扩展对象校验 (索引/约束/序列/触发器)。")
        return extra_results

    log_subsection("扩展对象校验 (索引/约束/序列/触发器)")

    # 1) 针对每个 TABLE 做索引/约束/触发器校验
    total_tables = sum(1 for _, _, t in master_list if t.upper() == 'TABLE')
    done_tables = 0

    for src_name, tgt_name, obj_type in master_list:
        if obj_type.upper() != 'TABLE':
            continue

        done_tables += 1
        if done_tables % 100 == 0:
            pct = done_tables * 100.0 / total_tables if total_tables else 100.0
            log.info("扩展校验进度 %d/%d (%.1f%%)...", done_tables, total_tables, pct)

        try:
            src_schema, src_table = src_name.split('.')
            tgt_schema, tgt_table = tgt_name.split('.')
        except ValueError:
            continue

        # 索引
        if 'INDEX' in enabled_types:
            ok_idx, idx_mis = compare_indexes_for_table(
                oracle_meta, ob_meta,
                src_schema, src_table,
                tgt_schema, tgt_table
            )
            if ok_idx:
                extra_results["index_ok"].append(tgt_name)
            elif idx_mis:
                extra_results["index_mismatched"].append(idx_mis)

        # 约束
        if 'CONSTRAINT' in enabled_types:
            ok_cons, cons_mis = compare_constraints_for_table(
                oracle_meta, ob_meta,
                src_schema, src_table,
                tgt_schema, tgt_table,
                full_object_mapping
            )
            if ok_cons:
                extra_results["constraint_ok"].append(tgt_name)
            elif cons_mis:
                extra_results["constraint_mismatched"].append(cons_mis)

        # 触发器
        if 'TRIGGER' in enabled_types:
            ok_trg, trg_mis = compare_triggers_for_table(
                oracle_meta, ob_meta,
                src_schema, src_table,
                tgt_schema, tgt_table,
                full_object_mapping
            )
            if ok_trg:
                extra_results["trigger_ok"].append(tgt_name)
            elif trg_mis:
                extra_results["trigger_mismatched"].append(trg_mis)

    # 2) 序列校验（考虑 remap 后的目标 schema）
    sequence_groups: Dict[Tuple[str, str], List[Tuple[str, str]]] = defaultdict(list)
    tgt_schema_all_expected: Dict[str, Set[str]] = defaultdict(set)
    if 'SEQUENCE' in enabled_types:
        for src_schema, seq_names in oracle_meta.sequences.items():
            src_schema_u = src_schema.upper()
            for seq_name in seq_names:
                seq_name_u = seq_name.upper()
                src_full = f"{src_schema_u}.{seq_name_u}"
                mapped = get_mapped_target(full_object_mapping, src_full, 'SEQUENCE')
                tgt_full = mapped or src_full
                if '.' not in tgt_full:
                    tgt_schema_u = src_schema_u
                    tgt_name_u = seq_name_u
                else:
                    tgt_schema_u, tgt_name_u = tgt_full.split('.', 1)
                    tgt_schema_u = tgt_schema_u.upper()
                    tgt_name_u = tgt_name_u.upper()
                sequence_groups[(src_schema_u, tgt_schema_u)].append((seq_name_u, tgt_name_u))
                tgt_schema_all_expected[tgt_schema_u].add(tgt_name_u)

        for (src_schema_u, tgt_schema_u), entries in sequence_groups.items():
            expected_tgt_names = {tgt_name for _, tgt_name in entries}
            actual_tgt_names = {name.upper() for name in ob_meta.sequences.get(tgt_schema_u, set())}
            all_expected_for_tgt = tgt_schema_all_expected[tgt_schema_u]

            missing_src = {
                src_name for src_name, tgt_name in entries
                if tgt_name not in actual_tgt_names
            }
            extra_tgt = actual_tgt_names - all_expected_for_tgt

            mapping_label = f"{src_schema_u}->{tgt_schema_u}"
            if not missing_src and not extra_tgt:
                extra_results["sequence_ok"].append(mapping_label)
            else:
                missing_map = [
                    (f"{src_schema_u}.{src_name}", f"{tgt_schema_u}.{tgt_name}")
                    for src_name, tgt_name in entries
                    if tgt_name not in actual_tgt_names
                ]
                extra_results["sequence_mismatched"].append(SequenceMismatch(
                    src_schema=src_schema_u,
                    tgt_schema=tgt_schema_u,
                    missing_sequences=missing_src,
                    extra_sequences=extra_tgt,
                    note=None,
                    missing_mappings=missing_map
                ))

    return extra_results


# ====================== 注释一致性检查 ======================

def check_comments(
    master_list: MasterCheckList,
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    enable_comment_check: bool = True
) -> Dict[str, object]:
    results: Dict[str, object] = {
        "ok": [],
        "mismatched": [],
        "skipped_reason": None
    }

    if not enable_comment_check:
        results["skipped_reason"] = "根据配置关闭注释比对。"
        return results

    if not master_list:
        results["skipped_reason"] = "无表对象可供注释比对。"
        return results

    if not any(obj_type.upper() == 'TABLE' for _, _, obj_type in master_list):
        results["skipped_reason"] = "当前清单未包含 TABLE，对应注释比对已跳过。"
        return results

    if not oracle_meta.comments_complete:
        results["skipped_reason"] = "未成功加载 Oracle 注释元数据，已跳过注释比对。"
        return results

    if not ob_meta.comments_complete:
        results["skipped_reason"] = "未成功加载 OceanBase 注释元数据，已跳过注释比对。"
        return results

    for src_name, tgt_name, obj_type in master_list:
        if obj_type.upper() != 'TABLE':
            continue
        try:
            src_schema, src_table = src_name.split('.')
            tgt_schema, tgt_table = tgt_name.split('.')
        except ValueError:
            continue

        src_key = (src_schema.upper(), src_table.upper())
        tgt_key = (tgt_schema.upper(), tgt_table.upper())

        src_table_cmt = normalize_comment_text(oracle_meta.table_comments.get(src_key))
        tgt_table_cmt = normalize_comment_text(ob_meta.table_comments.get(tgt_key))
        table_diff = src_table_cmt != tgt_table_cmt

        src_col_cmts = oracle_meta.column_comments.get(src_key, {})
        tgt_col_cmts = ob_meta.column_comments.get(tgt_key, {})

        src_col_meta = oracle_meta.table_columns.get(src_key, {}) or {}
        hidden_src_cols = {c for c, m in src_col_meta.items() if m.get("hidden")}

        # 过滤OMS列后计算缺失和额外的列注释
        missing_cols = {
            col for col in src_col_cmts.keys()
            if col not in tgt_col_cmts
            and col not in hidden_src_cols
            and not is_ignored_oms_column(col)
        }
        extra_cols = {
            col for col in tgt_col_cmts.keys()
            if col not in src_col_cmts and not is_ignored_oms_column(col)
        }

        # 额外验证：确保OMS列被完全过滤
        oms_filtered_extra = {col for col in extra_cols if not is_ignored_oms_column(col)}
        if len(oms_filtered_extra) != len(extra_cols):
            log.debug("表 %s.%s: 从额外列注释中过滤了 %d 个OMS列", 
                     tgt_key[0], tgt_key[1], len(extra_cols) - len(oms_filtered_extra))
            extra_cols = oms_filtered_extra

        column_diffs: List[Tuple[str, str, str]] = []
        for col in (src_col_cmts.keys() & tgt_col_cmts.keys()):
            if is_ignored_oms_column(col) or col in hidden_src_cols:
                continue
            src_cmt = normalize_comment_text(src_col_cmts.get(col))
            tgt_cmt = normalize_comment_text(tgt_col_cmts.get(col))
            if src_cmt != tgt_cmt:
                column_diffs.append((col, src_cmt, tgt_cmt))

        if table_diff or column_diffs or missing_cols or extra_cols:
            results["mismatched"].append(CommentMismatch(
                table=f"{tgt_key[0]}.{tgt_key[1]}",
                table_comment=(src_table_cmt, tgt_table_cmt) if table_diff else None,
                column_comment_diffs=column_diffs,
                missing_columns=missing_cols,
                extra_columns=extra_cols
            ))
        else:
            results["ok"].append(f"{tgt_key[0]}.{tgt_key[1]}")

    return results


# ====================== DDL 抽取 & ALTER 级别修补 ======================

def parse_oracle_dsn(dsn: str) -> Tuple[str, str, Optional[str]]:
    try:
        host_port, service = dsn.split('/', 1)
        host, port = host_port.split(':', 1)
        return host.strip(), port.strip(), service.strip()
    except ValueError:
        log.error("严重错误: 无法解析 Oracle DSN (host:port/service_name): %s", dsn)
        sys.exit(1)


def collect_oracle_env_info(ora_cfg: OraConfig) -> Dict[str, str]:
    """采集 Oracle 端基本信息（版本/容器/服务名/地址/用户）。"""
    info: Dict[str, str] = {}
    info["user"] = ora_cfg.get("user", "")
    info["dsn"] = ora_cfg.get("dsn", "")
    try:
        host, port, service = parse_oracle_dsn(ora_cfg.get("dsn", ""))
        info["host"] = host
        info["port"] = port
        info["service_name"] = service
    except Exception:
        pass

    try:
        with oracledb.connect(
            user=ora_cfg["user"],
            password=ora_cfg["password"],
            dsn=ora_cfg["dsn"]
        ) as conn:
            info["version"] = getattr(conn, "version", "") or info.get("version", "")
            with conn.cursor() as cursor:
                try:
                    cursor.execute("SELECT sys_context('userenv','con_name') FROM dual")
                    row = cursor.fetchone()
                    if row and row[0]:
                        info["container"] = str(row[0])
                except Exception:
                    pass
                try:
                    cursor.execute("SELECT cdb FROM v$database")
                    row = cursor.fetchone()
                    if row and row[0] is not None:
                        info["cdb_mode"] = "CDB" if str(row[0]).strip().upper() == "YES" else "Non-CDB"
                except Exception:
                    pass
                try:
                    cursor.execute("SELECT sys_context('userenv','service_name') FROM dual")
                    row = cursor.fetchone()
                    if row and row[0]:
                        info["service_name"] = str(row[0])
                except Exception:
                    pass
    except Exception as exc:
        log.warning("Oracle 基本信息获取失败（将使用配置值作为兜底）：%s", exc)

    return info


def parse_ob_status_output(output: str) -> Dict[str, str]:
    """解析 obclient status 输出中的关键信息。"""
    info: Dict[str, str] = {}
    for line in (output or "").splitlines():
        if ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = key.strip().lower()
        val = val.strip()
        if not val:
            continue
        if key.startswith("server version"):
            info["version"] = val
        elif key == "current database":
            info["current_database"] = val
        elif key == "connection id":
            info["connection_id"] = val
        elif key == "ssl":
            info["ssl"] = val
    return info


def collect_ob_env_info(ob_cfg: ObConfig) -> Dict[str, str]:
    """采集 OceanBase 端基本信息（版本/连接/用户）。"""
    configured_user = ob_cfg.get("user_string", "")
    info: Dict[str, str] = {
        "host": ob_cfg.get("host", ""),
        "port": ob_cfg.get("port", ""),
        "configured_user": configured_user,
        "current_user": configured_user
    }

    status_cmd = [
        ob_cfg["executable"],
        "-h", ob_cfg["host"],
        "-P", ob_cfg["port"],
        "-u", ob_cfg["user_string"],
        "-p" + ob_cfg["password"],
        "-e", "status"
    ]
    try:
        result = subprocess.run(
            status_cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=OBC_TIMEOUT
        )
        if result.returncode == 0:
            info.update(parse_ob_status_output(result.stdout))
        else:
            log.warning("obclient status 调用失败(%s): %s", result.returncode, result.stderr.strip())
    except Exception as exc:
        log.warning("获取 obclient status 信息失败（将尝试 SQL 查询）：%s", exc)

    if "version" not in info or not info["version"].strip():
        ok, stdout, _ = obclient_run_sql(ob_cfg, "SELECT OB_VERSION() FROM DUAL")
        if ok and stdout.strip():
            info["version"] = stdout.strip().splitlines()[0]

    version_raw = info.get("version", "")
    if version_raw:
        matches = list(re.finditer(r'\([^()]*\)', version_raw))
        if len(matches) > 1:
            second = matches[1]
            version_raw = version_raw[:second.start()] + version_raw[second.end():]
        info["version"] = " ".join(version_raw.split())

    return info


def resolve_dbcat_cli(settings: Dict) -> Path:
    bin_path = settings.get('dbcat_bin', '').strip()
    if not bin_path:
        log.error("严重错误: 未配置 dbcat_bin，请在 config.ini 的 [SETTINGS] 中指定 dbcat 目录。")
        sys.exit(1)
    cli_path = Path(bin_path)
    if cli_path.is_dir():
        cli_path = cli_path / 'bin' / 'dbcat'
    if not cli_path.exists():
        log.error("严重错误: 找不到 dbcat 可执行文件: %s", cli_path)
        sys.exit(1)
    return cli_path


def locate_dbcat_schema_dir(base_dir: Path, schema: str) -> Optional[Path]:
    schema_upper = schema.upper()
    if base_dir.name.upper().startswith(f"{schema_upper}_"):
        return base_dir
    direct = base_dir / schema
    if direct.exists():
        return direct
    for child in base_dir.iterdir():
        if not child.is_dir():
            continue
        if child.name.upper().startswith(f"{schema_upper}_"):
            return child
        candidate = child / schema
        if candidate.exists():
            return candidate
    return None


def find_dbcat_object_file(schema_dir: Path, object_type: str, object_name: str) -> Optional[Path]:
    name_upper = object_name.upper()
    hints = DBCAT_OUTPUT_DIR_HINTS.get(object_type.upper(), ())
    for hint in hints:
        candidate = schema_dir / hint / f"{name_upper}-schema.sql"
        if candidate.exists():
            return candidate
    matches = list(schema_dir.rglob(f"{name_upper}-schema.sql"))
    if matches:
        hint_upper = tuple(h.upper() for h in hints if h)
        for candidate in matches:
            parent_names = {parent.name.upper() for parent in candidate.parents}
            if hint_upper:
                if any(h in parent_names for h in hint_upper):
                    return candidate
            else:
                return candidate
    return None


def build_dbcat_file_index(schema_dir: Path) -> Dict[str, Dict[str, Path]]:
    """
    为 dbcat 输出构建一次性索引，避免对每个对象都遍历深目录。
    返回: {OBJECT_TYPE: {OBJECT_NAME: Path}}
    """
    index: Dict[str, Dict[str, Path]] = defaultdict(dict)
    try:
        for path in schema_dir.rglob("*-schema.sql"):
            name_upper = path.name.replace("-schema.sql", "").upper()
            obj_type = None
            for parent in path.parents:
                hint_type = DBCAT_DIR_TO_TYPE.get(parent.name.upper())
                if hint_type:
                    obj_type = hint_type
                    break
            if not obj_type:
                continue
            if name_upper not in index[obj_type]:
                index[obj_type][name_upper] = path
    except OSError as exc:
        log.warning("[dbcat] 遍历目录 %s 失败: %s", schema_dir, exc)
    return index


def build_dbcat_global_index(
    base_output: Path,
    schema_names: Set[str]
) -> Dict[str, Dict[str, Dict[str, Path]]]:
    """
    针对 dbcat_output 下所有历史 run 目录构建全局索引，降低深层遍历次数。
    返回: {SCHEMA: {OBJECT_TYPE: {OBJECT_NAME: Path}}}
    """
    schema_upper = {s.upper() for s in schema_names}
    global_index: Dict[str, Dict[str, Dict[str, Path]]] = defaultdict(lambda: defaultdict(dict))
    if not base_output.exists():
        return {}

    try:
        for path in base_output.rglob("*-schema.sql"):
            name_upper = path.name.replace("-schema.sql", "").upper()
            obj_type = None
            for parent in path.parents:
                hint_type = DBCAT_DIR_TO_TYPE.get(parent.name.upper())
                if hint_type:
                    obj_type = hint_type
                    break
            if not obj_type:
                continue
            schema_name = None
            for parent in path.parents:
                pname = parent.name.upper()
                if pname in schema_upper:
                    schema_name = pname
                    break
                # 兼容 schema_xxxx_xxxx 这种前缀匹配
                for candidate in schema_upper:
                    if pname.startswith(f"{candidate}_"):
                        schema_name = candidate
                        break
                if schema_name:
                    break
            if not schema_name:
                continue
            if name_upper not in global_index[schema_name][obj_type]:
                global_index[schema_name][obj_type][name_upper] = path
    except OSError as exc:
        log.warning("[dbcat] 构建全局索引失败: %s", exc)
    total_files = sum(len(obj_map) for schema_map in global_index.values() for obj_map in schema_map.values())
    if total_files:
        log.info("[dbcat] 全局索引构建完成，覆盖 %d 个对象文件。", total_files)
    return global_index


# ====================== 扁平化缓存目录 ======================

FLAT_CACHE_DIR_NAME = "cache"
FLAT_CACHE_INDEX_FILE = "index.json"

# 索引结构: {SCHEMA: {OBJECT_TYPE: [OBJECT_NAME, ...]}}
FlatCacheIndex = Dict[str, Dict[str, List[str]]]


def get_flat_cache_path(base_output: Path, schema: str, obj_type: str, obj_name: str) -> Path:
    """
    返回扁平化缓存的文件路径: dbcat_output/cache/SCHEMA/OBJECT_TYPE/name.sql
    """
    return base_output / FLAT_CACHE_DIR_NAME / schema.upper() / obj_type.upper() / f"{obj_name.upper()}.sql"


def get_flat_cache_index_path(base_output: Path) -> Path:
    """返回索引文件路径: dbcat_output/cache/index.json"""
    return base_output / FLAT_CACHE_DIR_NAME / FLAT_CACHE_INDEX_FILE


def load_flat_cache_index(base_output: Path) -> FlatCacheIndex:
    """
    加载扁平缓存的 JSON 索引文件。
    索引结构: {SCHEMA: {OBJECT_TYPE: [OBJECT_NAME, ...]}}
    """
    index_path = get_flat_cache_index_path(base_output)
    if not index_path.exists():
        return {}
    try:
        with open(index_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get("objects", {})
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("[dbcat] 加载缓存索引失败 %s: %s", index_path, exc)
        return {}


def save_flat_cache_index(base_output: Path, index: FlatCacheIndex) -> bool:
    """
    保存扁平缓存的 JSON 索引文件。
    """
    index_path = get_flat_cache_index_path(base_output)
    try:
        index_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "version": 1,
            "updated_at": datetime.now().isoformat(),
            "objects": index
        }
        with open(index_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except OSError as exc:
        log.warning("[dbcat] 保存缓存索引失败 %s: %s", index_path, exc)
        return False


def update_flat_cache_index(
    base_output: Path,
    schema: str,
    obj_type: str,
    obj_name: str,
    index: Optional[FlatCacheIndex] = None
) -> FlatCacheIndex:
    """
    更新索引，添加单个对象。如果传入 index 则直接修改，否则先加载再更新。
    """
    if index is None:
        index = load_flat_cache_index(base_output)
    
    schema_u = schema.upper()
    obj_type_u = obj_type.upper()
    obj_name_u = obj_name.upper()
    
    if schema_u not in index:
        index[schema_u] = {}
    if obj_type_u not in index[schema_u]:
        index[schema_u][obj_type_u] = []
    if obj_name_u not in index[schema_u][obj_type_u]:
        index[schema_u][obj_type_u].append(obj_name_u)
    
    return index


def load_from_flat_cache(
    base_output: Path,
    schema_requests: Dict[str, Dict[str, Set[str]]],
    accumulator: Dict[str, Dict[str, Dict[str, str]]],
    source_meta: Optional[Dict[Tuple[str, str, str], Tuple[str, float]]] = None,
    parallel_workers: int = 1
) -> int:
    """
    从扁平化缓存目录加载 DDL。
    parallel_workers: 并行读取线程数，默认1（顺序读取），建议4-8用于慢速磁盘
    """
    flat_cache = base_output / FLAT_CACHE_DIR_NAME
    if not flat_cache.exists():
        return 0
    
    index_start = time.time()
    cache_index = load_flat_cache_index(base_output)
    index_elapsed = time.time() - index_start
    if index_elapsed > 1.0:
        log.warning("[性能] 加载缓存索引耗时 %.2fs，磁盘IO可能较慢", index_elapsed)
    
    # 收集所有需要加载的文件
    load_tasks: List[Tuple[str, str, str, Path]] = []
    for schema in list(schema_requests.keys()):
        schema_u = schema.upper()
        type_map = schema_requests.get(schema) or {}
        schema_index = cache_index.get(schema_u, {})
        
        for obj_type in list(type_map.keys()):
            obj_type_u = obj_type.upper()
            names = type_map[obj_type]
            cached_names = set(schema_index.get(obj_type_u, []))
            
            for name in names:
                name_u = name.upper()
                if cached_names and name_u not in cached_names:
                    continue
                file_path = get_flat_cache_path(base_output, schema, obj_type, name)
                if file_path.exists():
                    load_tasks.append((schema, obj_type, name, file_path))
    
    if not load_tasks:
        return 0
    
    loaded_count = 0
    total_read_time = 0.0
    slow_files = []
    results_lock = threading.Lock()
    
    def _load_file(task: Tuple[str, str, str, Path]) -> Optional[Tuple]:
        schema, obj_type, name, file_path = task
        try:
            start = time.time()
            ddl_text = file_path.read_text('utf-8')
            elapsed = time.time() - start
            return (schema, obj_type, name, ddl_text, elapsed)
        except OSError as e:
            log.debug("[缓存] 读取失败 %s: %s", file_path, e)
            return None
    
    # 并行或顺序加载
    if parallel_workers > 1 and len(load_tasks) > 20:
        log.info("[缓存] 并行加载 %d 个文件 (workers=%d)...", len(load_tasks), parallel_workers)
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            for result in executor.map(_load_file, load_tasks):
                if result:
                    schema, obj_type, name, ddl_text, elapsed = result
                    with results_lock:
                        accumulator.setdefault(schema.upper(), {}).setdefault(obj_type.upper(), {})[name.upper()] = ddl_text
                        if source_meta:
                            source_meta[(schema.upper(), obj_type.upper(), name.upper())] = ("flat_cache", elapsed)
                        total_read_time += elapsed
                        if elapsed > 0.5:
                            slow_files.append((f"{schema}.{name}", elapsed))
                        loaded_count += 1
    else:
        for task in load_tasks:
            result = _load_file(task)
            if result:
                schema, obj_type, name, ddl_text, elapsed = result
                accumulator.setdefault(schema.upper(), {}).setdefault(obj_type.upper(), {})[name.upper()] = ddl_text
                if source_meta:
                    source_meta[(schema.upper(), obj_type.upper(), name.upper())] = ("flat_cache", elapsed)
                total_read_time += elapsed
                if elapsed > 0.5:
                    slow_files.append((f"{schema}.{name}", elapsed))
                loaded_count += 1
    
    # 更新schema_requests
    for schema, obj_type, name, _ in load_tasks:
        type_map = schema_requests.get(schema, {})
        if obj_type in type_map:
            type_map[obj_type].discard(name)
            if not type_map[obj_type]:
                del type_map[obj_type]
        if not type_map and schema in schema_requests:
            del schema_requests[schema]
    
    # 性能诊断
    if loaded_count > 0:
        avg_time = total_read_time / loaded_count
        if avg_time > 0.1:
            log.warning(
                "[性能警告] 缓存加载平均 %.3fs/文件 (%.2fs/%d文件) - 磁盘IO慢",
                avg_time, total_read_time, loaded_count
            )
            log.warning("[建议] 1) 使用本地SSD  2) 设置cache_parallel_workers=4-8  3) 或禁用缓存")
        if slow_files:
            log.warning("[性能] %d个文件>0.5s，前5个：", len(slow_files))
            for obj_name, elapsed in slow_files[:5]:
                log.warning("  %s: %.2fs", obj_name, elapsed)
    
    return loaded_count


def save_to_flat_cache(
    base_output: Path,
    schema: str,
    obj_type: str,
    obj_name: str,
    ddl_content: str
) -> bool:
    """
    将 DDL 保存到扁平化缓存目录。
    """
    file_path = get_flat_cache_path(base_output, schema, obj_type, obj_name)
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 验证：检查DDL是否包含异常字符
        if '\x00' in ddl_content:
            log.warning("[dbcat] DDL包含NULL字符，已清理: %s.%s", schema, obj_name)
            ddl_content = ddl_content.replace('\x00', '')
        
        file_path.write_text(ddl_content, encoding='utf-8')
        
        # 验证：读回并比较
        readback = file_path.read_text(encoding='utf-8')
        if readback != ddl_content:
            log.error("[dbcat] 缓存验证失败: %s.%s (写入%d字节，读回%d字节)",
                     schema, obj_name, len(ddl_content), len(readback))
            return False
        
        return True
    except OSError as exc:
        log.warning("[dbcat] 保存到扁平缓存失败 %s: %s", file_path, exc)
        return False


def normalize_dbcat_run_to_flat_cache(
    base_output: Path,
    run_dir: Path,
    schema: str,
    results: Dict[str, Dict[str, str]],
    cleanup_run_dir: bool = True
) -> int:
    """
    将 dbcat 运行结果整理到扁平化缓存目录，并更新 JSON 索引。
    
    Args:
        base_output: dbcat_output 基目录
        run_dir: 本次 dbcat 运行的输出目录（带时间戳）
        schema: schema 名称
        results: 本次导出的 DDL 内容 {OBJECT_TYPE: {OBJECT_NAME: ddl_content}}
        cleanup_run_dir: 是否在整理完成后删除原始运行目录
    
    Returns:
        保存的文件数量
    """
    # 先加载现有索引
    cache_index = load_flat_cache_index(base_output)
    
    saved_count = 0
    for obj_type, name_map in results.items():
        for obj_name, ddl_content in name_map.items():
            if save_to_flat_cache(base_output, schema, obj_type, obj_name, ddl_content):
                # 更新内存中的索引
                update_flat_cache_index(base_output, schema, obj_type, obj_name, cache_index)
                saved_count += 1
    
    # 批量保存索引（只写一次磁盘）
    if saved_count:
        save_flat_cache_index(base_output, cache_index)
        log.info("[dbcat] 已将 %d 个对象 DDL 整理到扁平缓存 %s/cache/%s/",
                 saved_count, base_output, schema.upper())
    
    # 清理原始运行目录
    if cleanup_run_dir and run_dir.exists():
        try:
            shutil.rmtree(run_dir)
            log.info("[dbcat] 已清理临时目录 %s", run_dir)
        except OSError as exc:
            log.warning("[dbcat] 清理临时目录失败 %s: %s", run_dir, exc)
    
    return saved_count


def load_cached_dbcat_results(
    base_output: Path,
    schema_requests: Dict[str, Dict[str, Set[str]]],
    accumulator: Dict[str, Dict[str, Dict[str, str]]],
    source_meta: Optional[Dict[Tuple[str, str, str], Tuple[str, float]]] = None,
    global_index: Optional[Dict[str, Dict[str, Dict[str, Path]]]] = None
) -> None:
    if not base_output.exists():
        return

    if global_index is None:
        global_index = build_dbcat_global_index(base_output, set(schema_requests.keys()))

    def _read_and_record(schema: str, obj_type: str, name: str, file_path: Path) -> bool:
        try:
            start_time = time.time()
            ddl_text = file_path.read_text('utf-8')
            elapsed = time.time() - start_time
        except OSError:
            return False
        accumulator.setdefault(schema, {}).setdefault(obj_type, {})[name] = ddl_text
        if source_meta is not None:
            source_meta[(schema, obj_type, name)] = ("cache", elapsed)
        return True

    # 先使用全局索引直接命中，避免逐 run 深层遍历
    preload_hit = 0
    for schema in list(schema_requests.keys()):
        type_map = schema_requests.get(schema) or {}
        for obj_type in list(type_map.keys()):
            names = type_map[obj_type]
            satisfied: Set[str] = set()
            for name in list(names):
                file_path = (
                    global_index
                    .get(schema.upper(), {})
                    .get(obj_type.upper(), {})
                    .get(name.upper())
                )
                if file_path and file_path.exists():
                    if _read_and_record(schema.upper(), obj_type.upper(), name.upper(), file_path):
                        satisfied.add(name)
                        preload_hit += 1
            names -= satisfied
            if not names:
                type_map.pop(obj_type, None)
        if not type_map:
            schema_requests.pop(schema, None)

    if preload_hit:
        log.info("[dbcat] 全局索引命中 %d 个对象，已直接载入缓存。", preload_hit)

    if not schema_requests:
        return

    candidates: List[Path] = [p for p in base_output.iterdir() if p.is_dir()]

    def _safe_mtime(p: Path) -> float:
        try:
            return p.stat().st_mtime
        except OSError:
            return 0.0

    run_dirs = sorted(candidates, key=_safe_mtime, reverse=True)

    for run_dir in run_dirs:
        if not schema_requests:
            break
        for schema in list(schema_requests.keys()):
            schema_dir = locate_dbcat_schema_dir(run_dir, schema)
            if not schema_dir or not schema_dir.exists():
                continue
            file_index = global_index.get(schema.upper()) or build_dbcat_file_index(schema_dir)
            type_map = schema_requests[schema]
            for obj_type in list(type_map.keys()):
                names = type_map[obj_type]
                satisfied: Set[str] = set()
                for name in list(names):
                    file_path = file_index.get(obj_type.upper(), {}).get(name.upper())
                    if file_path is None:
                        file_path = find_dbcat_object_file(schema_dir, obj_type, name)
                    if not file_path or not file_path.exists():
                        continue
                    try:
                        start_time = time.time()
                        ddl_text = file_path.read_text('utf-8')
                        elapsed = time.time() - start_time
                    except OSError:
                        continue
                    schema_u = schema.upper()
                    obj_type_u = obj_type.upper()
                    name_u = name.upper()
                    accumulator.setdefault(schema_u, {}).setdefault(obj_type_u, {})[name_u] = ddl_text
                    if source_meta is not None:
                        key = (schema_u, obj_type_u, name_u)
                        source_meta[key] = ("cache", elapsed)
                    satisfied.add(name)
                names -= satisfied
                if not names:
                    del type_map[obj_type]
            if not type_map:
                del schema_requests[schema]


def fetch_dbcat_schema_objects(
    ora_cfg: OraConfig,
    settings: Dict,
    schema_requests: Dict[str, Dict[str, Set[str]]]
) -> Tuple[Dict[str, Dict[str, Dict[str, str]]], Dict[Tuple[str, str, str], Tuple[str, float]]]:
    results: Dict[str, Dict[str, Dict[str, str]]] = {}
    source_meta: Dict[Tuple[str, str, str], Tuple[str, float]] = {}
    if not schema_requests:
        return results, source_meta

    base_output = Path(settings.get('dbcat_output_dir', 'dbcat_output'))
    ensure_dir(base_output)
    
    # 获取并行加载配置
    cache_parallel_workers = int(settings.get('cache_parallel_workers', 1))
    
    # 1) 优先从扁平缓存加载
    before_req = sum(len(names) for type_map in schema_requests.values() for names in type_map.values())
    flat_loaded = load_from_flat_cache(base_output, schema_requests, results, source_meta, cache_parallel_workers)
    if flat_loaded:
        log.info("[dbcat] 从扁平缓存加载 %d 个对象 DDL。", flat_loaded)
    
    # 2) 扁平缓存未命中的，尝试从旧的层级缓存加载（兼容）
    after_flat = sum(len(names) for type_map in schema_requests.values() for names in type_map.values())
    if after_flat > 0:
        global_index = build_dbcat_global_index(base_output, set(schema_requests.keys()))
        load_cached_dbcat_results(base_output, schema_requests, results, source_meta, global_index)
        after_legacy = sum(len(names) for type_map in schema_requests.values() for names in type_map.values())
        legacy_loaded = after_flat - after_legacy
        if legacy_loaded:
            log.info("[dbcat] 从层级缓存加载 %d 个对象 DDL。", legacy_loaded)
    
    after_req = sum(len(names) for type_map in schema_requests.values() for names in type_map.values())
    total_loaded = before_req - after_req
    if total_loaded:
        log.info("[dbcat] 缓存总计加载 %d 个对象 DDL，剩余待导出 %d。", total_loaded, after_req)
    else:
        log.info("[dbcat] 缓存未命中，将调用 dbcat 导出 %d 个对象。", after_req)

    if not schema_requests:
        return results, source_meta

    host, port, service = parse_oracle_dsn(ora_cfg['dsn'])
    dbcat_cli = resolve_dbcat_cli(settings)
    java_home = settings.get('java_home') or os.environ.get('JAVA_HOME')
    if not java_home:
        log.error("严重错误: 需要 JAVA_HOME 才能运行 dbcat，请在环境或 config.ini 中配置。")
        sys.exit(1)

    max_chunk = int(settings.get('dbcat_chunk_size', 150)) or 150
    cleanup_run = settings.get('dbcat_cleanup_run_dirs', 'true').lower() in ('true', '1', 'yes')
    cli_timeout = int(settings.get('cli_timeout', 600))
    dbcat_from = settings.get('dbcat_from', '')
    dbcat_to = settings.get('dbcat_to', '')
    dbcat_no_cal_dep = parse_bool_flag(settings.get('dbcat_no_cal_dep', 'false'), False)
    dbcat_query_meta_thread = int(settings.get('dbcat_query_meta_thread') or 0)
    dbcat_progress_interval = int(settings.get('dbcat_progress_interval') or 0)
    if dbcat_query_meta_thread < 0:
        dbcat_query_meta_thread = 0
    if dbcat_progress_interval < 1:
        dbcat_progress_interval = 0
    
    # 并行导出的 worker 数量，默认 4，防止打爆主机
    parallel_workers = int(settings.get('dbcat_parallel_workers', 4))
    parallel_workers = max(1, min(parallel_workers, 8))  # 限制在 1-8 之间
    
    # 准备每个 schema 的导出任务
    schema_tasks: List[Tuple[str, List[Tuple[str, str, List[str]]]]] = []
    for schema in list(schema_requests.keys()):
        type_map = schema_requests.get(schema)
        if not type_map:
            continue
        prepared: List[Tuple[str, str, List[str]]] = []
        for obj_type, names in type_map.items():
            option = DBCAT_OPTION_MAP.get(obj_type.upper())
            if not option:
                continue
            if obj_type.upper() == "MATERIALIZED VIEW":
                log.info("[dbcat] 跳过 MATERIALIZED VIEW 自动导出 (dbcat 不支持 --mview)，需要时请手工处理。")
                continue
            name_list = sorted(set(n.upper() for n in names if n))
            if not name_list:
                continue
            prepared.append((option, obj_type.upper(), name_list))
        if prepared:
            schema_tasks.append((schema, prepared))
    
    if not schema_tasks:
        return results, source_meta
    
    # 用于保护共享数据的锁
    results_lock = threading.Lock()
    error_occurred = threading.Event()
    
    def _export_single_schema(schema: str, prepared: List[Tuple[str, str, List[str]]]) -> Optional[str]:
        """导出单个 schema 的所有对象，返回错误信息或 None"""
        if error_occurred.is_set():
            return None
            
        run_dir = base_output / f"{schema}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        ensure_dir(run_dir)
        chunk_time_map: Dict[Tuple[str, str, str], float] = {}
        
        def _run_dbcat_chunk(
            option: str,
            chunk_names: List[str],
            obj_type: str,
            chunk_idx: int,
            total_chunks: int
        ) -> Optional[str]:
            cmd = [
                str(dbcat_cli),
                'convert',
                '-H', host,
                '-P', port,
                '-u', ora_cfg['user'],
                '-p', ora_cfg['password'],
                '-D', schema,
                '--from', dbcat_from,
                '--to', dbcat_to,
                '--file-per-object',
                '-f', str(run_dir)
            ]
            if service:
                cmd.extend(['--service-name', service])
            if dbcat_no_cal_dep:
                cmd.append('--no-cal-dep')
            if dbcat_query_meta_thread:
                cmd.extend(['--query-meta-thread', str(dbcat_query_meta_thread)])
            cmd.extend([option, ','.join(chunk_names)])

            env = os.environ.copy()
            env['JAVA_HOME'] = java_home
            env.setdefault('JRE_HOME', java_home)

            log.info(
                "[dbcat] 导出 schema=%s option=%s chunk=%d/%d 对象数=%d...",
                schema, option, chunk_idx, total_chunks, len(chunk_names)
            )
            start_time = time.time()
            try:
                with tempfile.TemporaryFile() as stdout_buf, tempfile.TemporaryFile() as stderr_buf:
                    proc = subprocess.Popen(
                        cmd,
                        stdout=stdout_buf,
                        stderr=stderr_buf,
                        env=env
                    )
                    last_log = start_time
                    while True:
                        if proc.poll() is not None:
                            break
                        now = time.time()
                        elapsed = now - start_time
                        if cli_timeout and elapsed > cli_timeout:
                            proc.kill()
                            proc.wait(timeout=5)
                            return f"[dbcat] 转换 schema={schema} 超时 ({cli_timeout}s)"
                        if dbcat_progress_interval and (now - last_log) >= dbcat_progress_interval:
                            log.info(
                                "[dbcat] 导出 schema=%s option=%s chunk=%d/%d 仍在运行 (已耗时 %.1fs)...",
                                schema, option, chunk_idx, total_chunks, elapsed
                            )
                            last_log = now
                        time.sleep(0.5)
                    elapsed = time.time() - start_time
                    stdout_buf.seek(0)
                    stderr_buf.seek(0)
                    stdout_text = stdout_buf.read().decode('utf-8', errors='ignore')
                    stderr_text = stderr_buf.read().decode('utf-8', errors='ignore')
                    if proc.returncode != 0:
                        return f"[dbcat] 转换 schema={schema} 失败: {stderr_text or stdout_text}"
                log.info(
                    "[dbcat] 导出 schema=%s option=%s chunk=%d/%d 完成，用时 %.2fs。",
                    schema, option, chunk_idx, total_chunks, elapsed
                )
                # 将批次总耗时平均分配给每个对象
                avg_elapsed = elapsed / len(chunk_names) if chunk_names else elapsed
                for obj_name in chunk_names:
                    key = (schema.upper(), obj_type.upper(), obj_name.upper())
                    chunk_time_map[key] = avg_elapsed
                return None
            except Exception as e:
                return f"[dbcat] 转换 schema={schema} 异常: {e}"
        
        # 执行所有 chunk
        for option, obj_type, name_list in prepared:
            chunks = [name_list[i:i + max_chunk] for i in range(0, len(name_list), max_chunk)]
            total_chunks = len(chunks)
            for idx, chunk in enumerate(chunks, start=1):
                if error_occurred.is_set():
                    return None
                err = _run_dbcat_chunk(option, chunk, obj_type, idx, total_chunks)
                if err:
                    error_occurred.set()
                    return err
        
        # 读取导出结果
        schema_dir = locate_dbcat_schema_dir(run_dir, schema)
        if not schema_dir:
            error_occurred.set()
            return f"[dbcat] 未在输出目录 {run_dir} 下找到 schema={schema} 的 DDL。"
        
        file_index = build_dbcat_file_index(schema_dir)
        schema_result: Dict[str, Dict[str, str]] = {}
        local_source_meta: Dict[Tuple[str, str, str], Tuple[str, float]] = {}
        
        for option, obj_type, name_list in prepared:
            type_result = schema_result.setdefault(obj_type, {})
            for obj_name in name_list:
                file_path = file_index.get(obj_type.upper(), {}).get(obj_name.upper())
                if file_path is None:
                    file_path = find_dbcat_object_file(schema_dir, obj_type, obj_name)
                if not file_path or not file_path.exists():
                    log.warning("[dbcat] 未找到对象 %s.%s (%s) 的 DDL 文件。", schema, obj_name, obj_type)
                    continue
                try:
                    read_start = time.time()
                    ddl_text = file_path.read_text('utf-8')
                    read_elapsed = time.time() - read_start
                except OSError as exc:
                    log.warning("[dbcat] 读取 %s 失败: %s", file_path, exc)
                    continue
                obj_name_u = obj_name.upper()
                type_result[obj_name_u] = ddl_text
                key = (schema.upper(), obj_type.upper(), obj_name_u)
                local_source_meta[key] = ("dbcat_run", chunk_time_map.get(key, read_elapsed))
        
        # 整理到扁平缓存
        normalize_dbcat_run_to_flat_cache(
            base_output, run_dir, schema, schema_result, cleanup_run_dir=cleanup_run
        )
        
        # 合并结果到共享数据
        with results_lock:
            results.setdefault(schema.upper(), {}).update(schema_result)
            source_meta.update(local_source_meta)
        
        return None
    
    # 并行执行导出
    if len(schema_tasks) == 1 or parallel_workers == 1:
        # 单个 schema 或单线程，直接顺序执行
        for schema, prepared in schema_tasks:
            err = _export_single_schema(schema, prepared)
            if err:
                log.error(err)
                sys.exit(1)
    else:
        # 多 schema 并行执行
        log.info("[dbcat] 启用并行导出 (workers=%d)，共 %d 个 schema 待导出。", 
                 parallel_workers, len(schema_tasks))
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            futures = {
                executor.submit(_export_single_schema, schema, prepared): schema
                for schema, prepared in schema_tasks
            }
            for future in as_completed(futures):
                schema = futures[future]
                try:
                    err = future.result()
                    if err:
                        log.error(err)
                        # 取消其他任务
                        for f in futures:
                            f.cancel()
                        sys.exit(1)
                except Exception as exc:
                    log.error("[dbcat] schema=%s 导出异常: %s", schema, exc)
                    sys.exit(1)

    return results, source_meta


def setup_metadata_session(ora_conn):
    plsql = """
    BEGIN
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE',FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS',TRUE);
    END;
    """
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(plsql)
    except oracledb.Error as e:
        log.warning(f"[DDL] 设置 DBMS_METADATA transform 失败: {e}")


DDL_OBJ_TYPE_MAPPING = {
    'PACKAGE BODY': 'PACKAGE_BODY',
    'MATERIALIZED VIEW': 'MATERIALIZED_VIEW',
    'TYPE BODY': 'TYPE_BODY'
}

CREATE_OBJECT_PATTERNS = {
    'TABLE': r'TABLE',
    'VIEW': r'(?:FORCE\s+)?VIEW',
    'MATERIALIZED VIEW': r'(?:FORCE\s+)?MATERIALIZED\s+VIEW',
    'PROCEDURE': r'PROCEDURE',
    'FUNCTION': r'FUNCTION',
    'PACKAGE': r'PACKAGE',
    'PACKAGE BODY': r'PACKAGE\s+BODY',
    'SYNONYM': r'(?:PUBLIC\s+)?SYNONYM',
    'SEQUENCE': r'SEQUENCE',
    'TRIGGER': r'TRIGGER',
    'TYPE': r'TYPE',
    'TYPE BODY': r'TYPE\s+BODY',
    'JOB': r'JOB',
    'SCHEDULE': r'SCHEDULE',
    'INDEX': r'(?:UNIQUE\s+|BITMAP\s+)?INDEX'
}


def get_oceanbase_version(ob_cfg: ObConfig) -> Optional[str]:
    """获取OceanBase版本号"""
    sql = "SELECT OB_VERSION() FROM DUAL"
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok or not out:
        log.warning("无法获取OceanBase版本信息: %s", err)
        return None
    
    # 解析版本号，OB_VERSION()直接返回版本号如 "4.2.5.7"
    for line in out.splitlines():
        line = line.strip()
        if line and line != 'OB_VERSION()':  # 跳过列标题
            # 检查是否是版本号格式 (数字.数字.数字.数字)
            if '.' in line and line.replace('.', '').replace('-', '').isdigit():
                return line.split('-')[0]  # 去掉可能的后缀
    return None


def compare_version(version1: str, version2: str) -> int:
    """比较版本号，返回 -1(v1<v2), 0(v1==v2), 1(v1>v2)"""
    try:
        v1_parts = [int(x) for x in version1.split('.')]
        v2_parts = [int(x) for x in version2.split('.')]
        
        # 补齐长度
        max_len = max(len(v1_parts), len(v2_parts))
        v1_parts.extend([0] * (max_len - len(v1_parts)))
        v2_parts.extend([0] * (max_len - len(v2_parts)))
        
        for i in range(max_len):
            if v1_parts[i] < v2_parts[i]:
                return -1
            elif v1_parts[i] > v2_parts[i]:
                return 1
        return 0
    except (ValueError, AttributeError):
        return 0


def clean_view_ddl_for_oceanbase(ddl: str, ob_version: Optional[str] = None) -> str:
    """
    清理Oracle VIEW DDL，使其兼容OceanBase
    
    Args:
        ddl: Oracle VIEW的DDL语句
        ob_version: OceanBase版本号
    
    Returns:
        清理后的DDL
    """
    if not ddl:
        return ddl
    
    cleaned_ddl = ddl

    # 先移除 WITH CHECK OPTION 的 CONSTRAINT 名称（保留 CHECK OPTION 本身）
    cleaned_ddl = re.sub(
        r'(\bWITH\s+CHECK\s+OPTION)\s+CONSTRAINT\s+("(?:""|[^"])*"|[A-Za-z0-9_#$]+)',
        r'\1',
        cleaned_ddl,
        flags=re.IGNORECASE
    )
    # 兜底移除尾部残留的 CONSTRAINT 名称
    cleaned_ddl = re.sub(
        r'\s+CONSTRAINT\s+("(?:""|[^"])*"|[A-Za-z0-9_#$]+)\s*(;)?\s*$',
        r'\2',
        cleaned_ddl,
        flags=re.IGNORECASE
    )

    # 需要移除的关键字模式
    patterns_to_remove = [
        # EDITIONABLE 在所有版本都需要移除
        r'\s+EDITIONABLE\s+',
        r'\s+NONEDITIONABLE\s+',
        # BEQUEATH 子句
        r'\s+BEQUEATH\s+(?:CURRENT_USER|DEFINER)',
        # SHARING 子句 (Oracle 12c+)
        r'\s+SHARING\s*=\s*(?:METADATA|DATA|EXTENDED\s+DATA|NONE)',
        # DEFAULT COLLATION 子句
        r'\s+DEFAULT\s+COLLATION\s+\w+',
        # CONTAINER 子句
        r'\s+CONTAINER_MAP\s*',
        r'\s+CONTAINERS_DEFAULT\s*',
    ]
    
    # 版本相关的清理
    remove_check_option = True
    if ob_version:
        # 如果版本 >= 4.2.5.7，可保留 WITH CHECK OPTION
        remove_check_option = compare_version(ob_version, "4.2.5.7") < 0
    if remove_check_option:
        patterns_to_remove.append(r'\s+WITH\s+CHECK\s+OPTION')

    for pattern in patterns_to_remove:
        cleaned_ddl = re.sub(pattern, ' ', cleaned_ddl, flags=re.IGNORECASE)
    
    # 清理多余的空格
    cleaned_ddl = re.sub(r'\s+', ' ', cleaned_ddl)
    cleaned_ddl = cleaned_ddl.strip()
    
    return cleaned_ddl


class SqlMasker:
    """
    辅助类：用于对 SQL 中的字符串字面量和注释进行掩码处理。
    防止正则替换时误伤字符串或注释内的内容。
    """
    def __init__(self, sql: str):
        self.original_sql = sql
        self.masked_sql = sql
        self.literals: Dict[str, str] = {}
        self.comments: Dict[str, str] = {}
        self._mask()

    def _mask(self):
        # 1. Mask String Literals: 'text'
        # 注意: Oracle 字符串内部的单引号转义为 ''
        def mask_str(match):
            key = f"###STR_{len(self.literals)}###"
            self.literals[key] = match.group(0)
            return key
        
        self.masked_sql = re.sub(r"'(?:''|[^'])*'", mask_str, self.masked_sql)

        # 2. Mask Block Comments: /* ... */
        def mask_block_cmt(match):
            key = f"###CMT_BLK_{len(self.comments)}###"
            self.comments[key] = match.group(0)
            return key
        
        self.masked_sql = re.sub(r'/\*.*?\*/', mask_block_cmt, self.masked_sql, flags=re.DOTALL)

        # 3. Mask Line Comments: -- ...
        def mask_line_cmt(match):
            key = f"###CMT_LN_{len(self.comments)}###"
            self.comments[key] = match.group(0)
            return key
        
        self.masked_sql = re.sub(r'--.*?$', mask_line_cmt, self.masked_sql, flags=re.MULTILINE)

    def unmask(self, sql: str) -> str:
        # 恢复掩码内容
        for k, v in self.comments.items():
            sql = sql.replace(k, v)
        for k, v in self.literals.items():
            sql = sql.replace(k, v)
        return sql


class SqlPunctuationMasker:
    """
    针对全角标点清洗的掩码器：
    - 屏蔽字符串字面量、注释、双引号标识符
    - 避免清洗时误改语义
    """
    def __init__(self, sql: str):
        self.original_sql = sql
        self.masked_sql = sql
        self.literals: Dict[str, str] = {}
        self.comments: Dict[str, str] = {}
        self.quoted_identifiers: Dict[str, str] = {}
        self._mask()

    def _mask_pattern(self, pattern: str, store: Dict[str, str], prefix: str, flags: int = 0) -> None:
        def _repl(match):
            key = f"###{prefix}_{len(store)}###"
            store[key] = match.group(0)
            return key
        self.masked_sql = re.sub(pattern, _repl, self.masked_sql, flags=flags)

    def _mask(self) -> None:
        # 1) 字符串字面量
        self._mask_pattern(r"'(?:''|[^'])*'", self.literals, "PUNC_STR")
        # 2) 块注释
        self._mask_pattern(r"/\*.*?\*/", self.comments, "PUNC_CMT_BLK", flags=re.DOTALL)
        # 3) 行注释
        self._mask_pattern(r"--.*?$", self.comments, "PUNC_CMT_LN", flags=re.MULTILINE)
        # 4) 双引号标识符
        self._mask_pattern(r'"(?:\"\"|[^"])*"', self.quoted_identifiers, "PUNC_QID")

    def unmask(self, sql: str) -> str:
        for k, v in self.quoted_identifiers.items():
            sql = sql.replace(k, v)
        for k, v in self.comments.items():
            sql = sql.replace(k, v)
        for k, v in self.literals.items():
            sql = sql.replace(k, v)
        return sql


ASCII_PUNCT_FOR_FULLWIDTH = "!#$%&()*+,-./:;<=>?@[\\]^_`{|}~"
FULLWIDTH_PUNCT_REPLACEMENTS = {
    chr(ord(ch) + 0xFEE0): ch for ch in ASCII_PUNCT_FOR_FULLWIDTH
}
FULLWIDTH_PUNCT_REPLACEMENTS.update({
    "\u3000": " ",  # IDEOGRAPHIC SPACE
    "\u3001": ",",  # IDEOGRAPHIC COMMA
    "\u3002": ".",  # IDEOGRAPHIC PERIOD
    "\u3010": "[",  # LEFT BLACK LENTICULAR BRACKET
    "\u3011": "]",  # RIGHT BLACK LENTICULAR BRACKET
})

PLSQL_PUNCT_SANITIZE_TYPES = {
    "PROCEDURE",
    "FUNCTION",
    "PACKAGE",
    "PACKAGE BODY",
    "TYPE",
    "TYPE BODY",
    "TRIGGER",
}


def sanitize_plsql_punctuation(
    ddl: str,
    obj_type: str,
    sample_limit: int = 5
) -> Tuple[str, int, List[Tuple[str, str]]]:
    """
    将PL/SQL DDL中的全角标点替换为半角，避免目标端解析失败。
    保护字符串字面量、注释、双引号标识符不被替换。

    Returns:
        (sanitized_ddl, replaced_count, samples)
    """
    if not ddl:
        return ddl, 0, []
    if (obj_type or "").upper() not in PLSQL_PUNCT_SANITIZE_TYPES:
        return ddl, 0, []

    masker = SqlPunctuationMasker(ddl)
    masked = masker.masked_sql
    replaced = 0
    samples: List[Tuple[str, str]] = []
    out_chars: List[str] = []

    for ch in masked:
        repl = FULLWIDTH_PUNCT_REPLACEMENTS.get(ch)
        if repl is not None:
            replaced += 1
            if len(samples) < sample_limit and (ch, repl) not in samples:
                samples.append((ch, repl))
            out_chars.append(repl)
        else:
            out_chars.append(ch)

    sanitized = masker.unmask("".join(out_chars))
    return sanitized, replaced, samples


def _find_inline_comment_split(segment: str) -> Optional[int]:
    if not segment.startswith("--"):
        return None
    # Try to detect the next column token after a collapsed inline comment.
    m = re.search(r"\s+(?:[A-Za-z_][A-Za-z0-9_#$]*|\"[^\"]+\")\.", segment)
    if m:
        return m.start()
    m = re.search(r"\s+FROM\b", segment, flags=re.IGNORECASE)
    if m:
        return m.start()
    return None


def fix_inline_comment_collapse(ddl: str) -> str:
    """
    修复 SELECT 列表中因换行被压缩导致的行内注释吞行问题。
    仅在检测到注释后仍有列/FROM 关键字时插入换行。
    """
    if not ddl or "--" not in ddl:
        return ddl
    out: List[str] = []
    i = 0
    in_single = False
    in_double = False
    in_block = False
    while i < len(ddl):
        ch = ddl[i]
        nxt = ddl[i + 1] if i + 1 < len(ddl) else ""

        if in_block:
            if ch == "*" and nxt == "/":
                out.append("*/")
                i += 2
                in_block = False
                continue
            out.append(ch)
            i += 1
            continue

        if in_single:
            out.append(ch)
            if ch == "'" and nxt == "'":
                out.append(nxt)
                i += 2
                continue
            if ch == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            out.append(ch)
            if ch == '"' and nxt == '"':
                out.append(nxt)
                i += 2
                continue
            if ch == '"':
                in_double = False
            i += 1
            continue

        if ch == "'" and not in_double:
            in_single = True
            out.append(ch)
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = True
            out.append(ch)
            i += 1
            continue
        if ch == "/" and nxt == "*":
            in_block = True
            out.append("/*")
            i += 2
            continue

        if ch == "-" and nxt == "-":
            line_end = ddl.find("\n", i)
            if line_end == -1:
                line_end = len(ddl)
            segment = ddl[i:line_end]
            split_at = _find_inline_comment_split(segment)
            if split_at is not None:
                out.append(segment[:split_at])
                out.append("\n")
                i += split_at
                continue
            out.append(segment)
            i = line_end
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def repair_split_identifiers(ddl: str, column_names: Set[str]) -> str:
    """
    修复被错误拆分的列标识符，例如 TOT_P ERM -> TOT_PERM。
    仅当拼接后命中视图列元数据时才会修复。
    """
    if not ddl or not column_names:
        return ddl
    names_upper = {c.upper() for c in column_names if c}
    masker = SqlMasker(ddl)
    masked = masker.masked_sql
    chars = list(masked)
    length = len(masked)
    i = 0

    def _is_ident_start(ch: str) -> bool:
        return ch.isalpha() or ch == "_"

    def _is_ident_char(ch: str) -> bool:
        return ch.isalnum() or ch in "_#$"

    while i < length:
        ch = masked[i]
        if not _is_ident_start(ch):
            i += 1
            continue
        if i > 0 and masked[i - 1] == '"':
            i += 1
            continue
        j = i + 1
        while j < length and _is_ident_char(masked[j]):
            j += 1
        k = j
        while k < length and masked[k].isspace():
            k += 1
        if k == j or k >= length or not _is_ident_start(masked[k]):
            i = j
            continue
        if masked[k - 1] == '"' or (k + 1 < length and masked[k + 1] == '"'):
            i = j
            continue
        l = k + 1
        while l < length and _is_ident_char(masked[l]):
            l += 1
        combined = f"{masked[i:j]}{masked[k:l]}".upper()
        if combined in names_upper:
            for idx in range(j, k):
                chars[idx] = ""
            i = l
            continue
        i = j

    return masker.unmask("".join(chars))


def sanitize_view_ddl(ddl: str, column_names: Optional[Set[str]] = None) -> str:
    """
    对 VIEW DDL 进行质量修复：修复行内注释吞行、拆分列名。
    """
    if not ddl:
        return ddl
    cleaned = fix_inline_comment_collapse(ddl)
    if column_names:
        cleaned = repair_split_identifiers(cleaned, column_names)
    return cleaned


def extract_view_dependencies(ddl: str, default_schema: Optional[str] = None) -> Set[str]:
    """
    从 VIEW DDL 中提取依赖的对象名（表/视图/同义词等）。
    改进版：
    - 使用 SqlMasker 保护字符串和注释
    - 支持提取 FROM/JOIN 后逗号分隔的多个表
    """
    dependencies: Set[str] = set()
    if not ddl:
        return dependencies

    masker = SqlMasker(ddl)
    clean_sql = masker.masked_sql

    # 归一化空白
    clean_sql = re.sub(r'\s+', ' ', clean_sql)

    # 关键词正则
    start_keywords = r'(?:FROM|JOIN|UPDATE|INTO|MERGE\s+INTO)'
    stopper_keywords = [
        'WHERE', 'GROUP', 'HAVING', 'ORDER', 'UNION', 'INTERSECT', 'MINUS', 'EXCEPT',
        'START', 'CONNECT', 'MODEL', 'WINDOW', 'FETCH', 'OFFSET', 'FOR',
        'ON', 'USING', 'LEFT', 'RIGHT', 'FULL', 'INNER', 'CROSS', 'NATURAL',
        'SELECT', 'SET', 'VALUES', 'RETURNING', 'AS'
    ]
    stopper_pattern = r'\b(?:' + '|'.join(stopper_keywords) + r')\b'

    combined_pattern = re.compile(rf'\b{start_keywords}\b', re.IGNORECASE)
    stopper_regex = re.compile(stopper_pattern + r'|' + rf'\b{start_keywords}\b', re.IGNORECASE)

    current_pos = 0
    while True:
        match = combined_pattern.search(clean_sql, current_pos)
        if not match:
            break
        
        content_start = match.end()
        stop_match = stopper_regex.search(clean_sql, content_start)
        
        if stop_match:
            content_end = stop_match.start()
            next_scan_pos = stop_match.start()
        else:
            content_end = len(clean_sql)
            next_scan_pos = len(clean_sql)

        segment = clean_sql[content_start:content_end]
        
        # 处理逗号分隔的表列表
        parts = segment.split(',')
        for part in parts:
            part = part.strip()
            # 简单过滤：忽略括号开头的子查询
            if not part or part.startswith('('):
                continue
                
            # 取第一个 token (忽略别名)
            first_token = part.split()[0]
            candidate = first_token.replace('"', '').upper()
            
            if '@' in candidate:
                candidate = candidate.split('@')[0]
            
            if not candidate or candidate == 'DUAL':
                continue
            
            # 简单的合法性检查
            if not re.match(r'^[A-Z0-9_\$#\.]+$', candidate):
                continue
                
            if '.' in candidate:
                dependencies.add(candidate)
            elif default_schema:
                dependencies.add(f"{default_schema.upper()}.{candidate}")

        current_pos = next_scan_pos
        # 防止死循环
        if current_pos <= match.start():
            current_pos = match.end()

    return dependencies


def remap_view_dependencies(
    ddl: str, 
    view_schema: str,
    remap_rules: RemapRules,
    full_object_mapping: FullObjectMapping
) -> str:
    """
    根据remap规则重写VIEW DDL中的依赖对象引用
    改进：使用 SqlMasker 确保只替换 SQL 代码，不替换注释/字符串
    """
    if not ddl:
        return ddl
    
    # 提取依赖对象（无 schema 的引用按 view_schema 兜底）
    dependencies = extract_view_dependencies(ddl, default_schema=view_schema)

    # 构建替换映射（既替换全名，也替换同 schema 下无前缀引用）
    replacements: Dict[str, str] = {}
    view_schema_u = (view_schema or "").upper()
    for dep in dependencies:
        tgt_name = find_mapped_target_any_type(
            full_object_mapping,
            dep,
            preferred_types=("TABLE", "VIEW", "MATERIALIZED VIEW", "SYNONYM", "FUNCTION")
        ) or remap_rules.get(dep)
        if not tgt_name:
            continue
        dep_u = dep.upper()
        tgt_u = tgt_name.upper()
        replacements[dep_u] = tgt_u
        if '.' in dep_u:
            dep_schema, dep_obj = dep_u.split('.', 1)
            if dep_schema == view_schema_u:
                # 无前缀引用也替换为全名(或目标名)，避免跨 schema 迁移后失效
                replacements.setdefault(dep_obj, tgt_u)

    if not replacements:
        return ddl

    # 使用 Masker 保护
    masker = SqlMasker(ddl)
    working_sql = masker.masked_sql

    for src_ref in sorted(replacements.keys(), key=len, reverse=True):
        tgt_ref = replacements[src_ref]
        if '.' in src_ref:
            pattern = re.compile(
                rf'(?<![A-Z0-9_\$#"]){re.escape(src_ref)}(?![A-Z0-9_\$#"])',
                re.IGNORECASE
            )
        else:
            # Unqualified identifier: avoid replacing the tail of an already-qualified reference
            pattern = re.compile(
                rf'(?<![A-Z0-9_\$#"\.]){re.escape(src_ref)}(?![A-Z0-9_\$#"])',
                re.IGNORECASE
            )
        working_sql = pattern.sub(tgt_ref, working_sql)

    return masker.unmask(working_sql)


def remap_synonym_target(
    ddl: str,
    remap_rules: RemapRules,
    full_object_mapping: FullObjectMapping
) -> str:
    """
    将 SYNONYM 的 FOR 子句指向 remap 后的目标对象（支持跨 schema，如 PUBLIC 同义词）。
    """
    if not ddl:
        return ddl

    pattern = re.compile(
        r'\bFOR\s+("?[A-Z0-9_\$#]+"?(?:\s*\.\s*"?[A-Z0-9_\$#]+"?)?)',
        re.IGNORECASE
    )

    def _format_target(target: str, quoted_like_src: bool) -> str:
        parts = target.upper().split('.', 1)
        if len(parts) != 2:
            return target.upper()
        if quoted_like_src:
            return '.'.join(f'"{p}"' for p in parts)
        return target.upper()

    def _repl(match: re.Match) -> str:
        raw_target = match.group(1)
        normalized = raw_target.replace('"', '').replace(' ', '').upper()
        if '.' not in normalized:
            return match.group(0)

        mapped = find_mapped_target_any_type(
            full_object_mapping,
            normalized,
            preferred_types=("TABLE", "VIEW", "MATERIALIZED VIEW", "SYNONYM")
        ) or remap_rules.get(normalized)

        if not mapped or '.' not in mapped:
            return match.group(0)

        new_target = _format_target(mapped, '"' in raw_target)
        return f"FOR {new_target}"

    return pattern.sub(_repl, ddl, count=1)


def normalize_public_synonym_name(ddl: str, synonym_name: str) -> str:
    """
    确保 PUBLIC SYNONYM 的名称不带 schema 前缀。
    将 "CREATE OR REPLACE PUBLIC SYNONYM SCHEMA.NAME" 归一为 "CREATE OR REPLACE PUBLIC SYNONYM NAME"。
    """
    if not ddl or not synonym_name:
        return ddl
    name_u = synonym_name.upper().split('.', 1)[-1]
    pattern = re.compile(
        r'(CREATE\s+(?:OR\s+REPLACE\s+)?PUBLIC\s+SYNONYM\s+)(?:"?[A-Z0-9_\$#]+"?\s*\.)?"?([A-Z0-9_\$#]+)"?',
        re.IGNORECASE
    )

    def _repl(match: re.Match) -> str:
        return f"{match.group(1)}{name_u}"

    return pattern.sub(_repl, ddl, count=1)


def oracle_get_views_ddl_batch(
    ora_cfg: OraConfig,
    view_objects: List[Tuple[str, str]]  # [(schema, view_name), ...]
) -> Dict[Tuple[str, str], str]:
    """
    批量获取VIEW的DDL，使用DBMS_METADATA
    
    Args:
        ora_cfg: Oracle连接配置
        view_objects: 视图对象列表 [(schema, view_name), ...]
    
    Returns:
        {(schema, view_name): ddl_text}
    """
    if not view_objects:
        return {}
    
    log.info("[VIEW] 正在批量获取 %d 个VIEW的DDL (使用DBMS_METADATA)...", len(view_objects))
    
    results = {}
    
    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as connection:
            setup_metadata_session(connection)
            
            for schema, view_name in view_objects:
                try:
                    ddl = oracle_get_ddl(connection, 'VIEW', schema, view_name)
                    if ddl:
                        results[(schema, view_name)] = ddl
                        log.debug("[VIEW] 成功获取 %s.%s 的DDL", schema, view_name)
                    else:
                        log.warning("[VIEW] 未能获取 %s.%s 的DDL", schema, view_name)
                except Exception as exc:
                    log.warning("[VIEW] 获取 %s.%s DDL失败: %s", schema, view_name, exc)
                    
    except Exception as exc:
        log.error("[VIEW] 批量获取VIEW DDL时连接失败: %s", exc)
    
    log.info("[VIEW] 成功获取 %d/%d 个VIEW的DDL", len(results), len(view_objects))
    return results


def oracle_get_ddl(ora_conn, obj_type: str, owner: str, name: str) -> Optional[str]:
    sql = "SELECT DBMS_METADATA.GET_DDL(:1, :2, :3) FROM DUAL"
    obj_type_norm = DDL_OBJ_TYPE_MAPPING.get(obj_type.upper(), obj_type.upper())
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(sql, [obj_type_norm, name.upper(), owner.upper()])
            row = cursor.fetchone()
            if not row or row[0] is None:
                return None
            return str(row[0])
    except (oracledb.Error, Exception) as e:
        log.warning(f"[DDL] 获取 {obj_type} {owner}.{name} DDL 失败: {e}")
        return None


# 批量获取 DDL 的对象类型（仅支持这些类型）
BATCH_DDL_ALLOWED_TYPES = {
    'TABLE', 'VIEW', 'MATERIALIZED VIEW',
    'PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY',
    'SYNONYM', 'SEQUENCE', 'TRIGGER',
    'TYPE', 'TYPE BODY'
}


def oracle_get_ddl_batch(
    ora_cfg: OraConfig,
    objects: List[Tuple[str, str, str]]  # [(schema, obj_type, obj_name), ...]
) -> Dict[Tuple[str, str, str], str]:
    """
    批量获取多个对象的 DDL，在一次连接中完成。
    
    Args:
        ora_cfg: Oracle 连接配置
        objects: 对象列表 [(schema, obj_type, obj_name), ...]
    
    Returns:
        {(schema, obj_type, obj_name): ddl_text, ...}
    """
    if not objects:
        return {}
    
    # 过滤不支持的类型
    valid_objects = [
        (s.upper(), t.upper(), n.upper()) 
        for s, t, n in objects 
        if t.upper() in BATCH_DDL_ALLOWED_TYPES
    ]
    if not valid_objects:
        return {}
    
    results: Dict[Tuple[str, str, str], str] = {}
    failed_count = 0
    
    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as conn:
            # 设置 DBMS_METADATA 参数
            setup_metadata_session(conn)
            
            log.info("[DDL] 批量获取 %d 个对象的 DBMS_METADATA DDL...", len(valid_objects))
            start_time = time.time()
            
            with conn.cursor() as cursor:
                sql = "SELECT DBMS_METADATA.GET_DDL(:1, :2, :3) FROM DUAL"
                for schema, obj_type, obj_name in valid_objects:
                    obj_type_norm = DDL_OBJ_TYPE_MAPPING.get(obj_type, obj_type)
                    try:
                        cursor.execute(sql, [obj_type_norm, obj_name, schema])
                        row = cursor.fetchone()
                        if row and row[0] is not None:
                            results[(schema, obj_type, obj_name)] = str(row[0])
                    except oracledb.Error as e:
                        err_msg = str(e).upper()
                        # 忽略对象不存在的错误
                        if not any(code in err_msg for code in ("ORA-31603", "ORA-04043", "ORA-00942")):
                            failed_count += 1
                            if failed_count <= 3:
                                log.warning("[DDL] 获取 %s.%s (%s) 失败: %s", schema, obj_name, obj_type, e)
            
            elapsed = time.time() - start_time
            log.info("[DDL] 批量获取完成，成功 %d/%d，用时 %.2fs。", 
                     len(results), len(valid_objects), elapsed)
    except oracledb.Error as e:
        log.error("[DDL] 批量获取 DDL 连接失败: %s", e)
    
    return results


def adjust_ddl_for_object(
    ddl: str,
    src_schema: str,
    src_name: str,
    tgt_schema: str,
    tgt_name: str,
    extra_identifiers: Optional[List[Tuple[Tuple[str, str], Tuple[str, str]]]] = None,
    obj_type: Optional[str] = None
) -> str:
    """
    依据 remap 结果调整 DBMS_METADATA 生成的 DDL：
      - 先替换主对象 (schema+name)
      - 再按需替换依赖对象 (如索引/触发器引用的表)
      - 若发生 remap，确保 CREATE 语句显式带上目标 schema
    extra_identifiers: [ ((src_schema, src_name), (tgt_schema, tgt_name)), ... ]
    obj_type: 供定位 CREATE 语句使用的对象类型
    """
    src_schema_u = (src_schema or "").upper()
    src_name_u = (src_name or "").upper()
    tgt_schema_u = (tgt_schema or "").upper()
    tgt_name_u = (tgt_name or "").upper()
    mapping_changed = (src_schema_u != tgt_schema_u) or (src_name_u != tgt_name_u)

    def replace_identifier(text: str, src_s: str, src_n: str, tgt_s: str, tgt_n: str) -> str:
        if not src_s or not src_n or not tgt_s or not tgt_n:
            return text
        src_s_u = src_s.upper()
        src_n_u = src_n.upper()
        tgt_s_u = tgt_s.upper()
        tgt_n_u = tgt_n.upper()

        pattern_quoted = re.compile(
            rf'"{re.escape(src_s_u)}"\."{re.escape(src_n_u)}"',
            re.IGNORECASE
        )
        pattern_unquoted = re.compile(
            rf'\b{re.escape(src_s_u)}\.{re.escape(src_n_u)}\b',
            re.IGNORECASE
        )

        text = pattern_quoted.sub(f'"{tgt_s_u}"."{tgt_n_u}"', text)
        text = pattern_unquoted.sub(f'{tgt_s_u}.{tgt_n_u}', text)
        return text

    def replace_unqualified_identifier(text: str, src_n: str, tgt_s: str, tgt_n: str) -> str:
        """
        当源对象在自身 schema 内被 remap 到其他 schema 时，源 DDL 中的无前缀引用
        会错误地落到当前 schema。这里尽量只在“疑似对象引用”的上下文中替换，避免误伤列名/变量。
        """
        if not src_n or not tgt_s or not tgt_n:
            return text
        src_n_u = src_n.upper()
        tgt_s_u = tgt_s.upper()
        tgt_n_u = tgt_n.upper()
        tgt_full = f"{tgt_s_u}.{tgt_n_u}"

        name_pattern = re.compile(rf'\b{re.escape(src_n_u)}\b', re.IGNORECASE)
        token_pattern = re.compile(r'[A-Z_][A-Z0-9_\$#]*', re.IGNORECASE)
        stop_tokens = {
            'SELECT', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'CONNECT', 'START',
            'WITH', 'UNION', 'INTERSECT', 'MINUS', 'EXCEPT',
            'WHEN', 'THEN', 'ELSE', 'BEGIN', 'DECLARE', 'IS', 'AS', 'LOOP', 'END',
            'FETCH', 'CLOSE', 'OPEN', 'VALUES', 'SET', 'RETURN', 'CASE', 'OVER',
            'PARTITION', 'CHECK', 'CONSTRAINT', 'PRIMARY', 'FOREIGN', 'UNIQUE',
            'DEFAULT'
        }

        def _has_insert_or_merge(tokens: List[str]) -> bool:
            """向前寻找 INSERT/MERGE（在遇到 stop token 前）。"""
            for tok in reversed(tokens):
                if tok in stop_tokens:
                    return False
                if tok in ('INSERT', 'MERGE'):
                    return True
            return False

        def _nearest_context(tokens: List[str]) -> Optional[str]:
            """
            从后往前查找最近的上下文关键词（FROM/JOIN/UPDATE/DELETE/TRUNCATE/TABLE/INTO/USING/ON）。
            遇到 stop token 则终止。
            """
            for tok in reversed(tokens):
                if tok in stop_tokens:
                    return None
                if tok in ('FROM', 'JOIN', 'UPDATE', 'DELETE', 'TRUNCATE', 'TABLE'):
                    return tok
                if tok == 'INTO':
                    return tok
                if tok == 'USING':
                    return 'USING'
                if tok == 'ON':
                    return 'ON'
                if tok == 'REFERENCES':
                    return 'REFERENCES'
            return None

        def _looks_like_namespace(after: str) -> bool:
            """
            判断是否是 pkg.func / seq.NEXTVAL 这类“名称后跟点”的调用。
            允许 NEXTVAL/CURRVAL，或后续紧跟 ( / . 认为是包/类型访问。
            """
            m = re.match(r'\s*\.\s*([A-Z_][A-Z0-9_\$#]*)', after, re.IGNORECASE)
            if not m:
                return False
            next_token = m.group(1).upper()
            if next_token in ('NEXTVAL', 'CURRVAL'):
                return True
            rest = after[m.end():].lstrip()
            return rest.startswith('(') or rest.startswith('.')

        def _ddl_on_context(prefix: str) -> bool:
            """
            判断是否位于 DDL 中的 ON 子句（如 CREATE INDEX ... ON / TRIGGER ... ON）。
            仅在 ON 紧跟对象名场景下允许替换，避免 JOIN ... ON 条件被误替换。
            """
            prefix_upper = prefix.upper().rstrip()
            if not prefix_upper.endswith(' ON'):
                return False
            return bool(
                re.search(r'\bINDEX\b', prefix_upper)
                or re.search(r'\bTRIGGER\b', prefix_upper)
                or re.search(r'\bMATERIALIZED\s+VIEW\s+LOG\b', prefix_upper)
            )

        def _repl(match: re.Match) -> str:
            start, end = match.span()
            # 向前查首个非空白字符，若为 '.' 则已限定 schema
            idx = start - 1
            while idx >= 0 and text[idx].isspace():
                idx -= 1
            if idx >= 0 and text[idx] == '"':
                idx -= 1
                while idx >= 0 and text[idx].isspace():
                    idx -= 1
            if idx >= 0 and text[idx] == '.':
                return match.group(0)

            after = text[end:]
            if _looks_like_namespace(after):
                return tgt_full

            tokens_before = [t.upper() for t in token_pattern.findall(text[:start])]
            context = _nearest_context(tokens_before)
            if not context:
                prefix = text[max(0, start - 160):start]
                if _ddl_on_context(prefix):
                    return tgt_full
                return match.group(0)
            if context == 'INTO' and not _has_insert_or_merge(tokens_before):
                return match.group(0)
            if context == 'USING' and not _has_insert_or_merge(tokens_before):
                return match.group(0)
            if context == 'ON':
                prefix = text[max(0, start - 160):start]
                if not _ddl_on_context(prefix):
                    return match.group(0)
            if context == 'REFERENCES':
                prefix = text[max(0, start - 80):start].upper().rstrip()
                if not prefix.endswith('REFERENCES'):
                    return match.group(0)
                return tgt_full
            return tgt_full

        return name_pattern.sub(_repl, text)

    result = replace_identifier(ddl, src_schema, src_name, tgt_schema, tgt_name)
    
    # 处理主对象的裸名引用（如 END package_name）
    if mapping_changed and src_name_u != tgt_name_u:
        pattern = re.compile(rf'\b{re.escape(src_name_u)}\b', re.IGNORECASE)
        def _repl_main(match: re.Match) -> str:
            start = match.start()
            idx = start - 1
            while idx >= 0 and result[idx].isspace():
                idx -= 1
            if idx >= 0 and result[idx] == '"':
                idx -= 1
                while idx >= 0 and result[idx].isspace():
                    idx -= 1
            if idx >= 0 and result[idx] == '.':
                return match.group(0)
            return tgt_name_u
        result = pattern.sub(_repl_main, result)

    if extra_identifiers:
        # 构建快速查找字典：{(src_schema, src_obj): (tgt_schema, tgt_obj)}
        replacement_dict: Dict[Tuple[str, str], Tuple[str, str]] = {}
        for (src_pair, tgt_pair) in extra_identifiers:
            key = (src_pair[0].upper(), src_pair[1].upper())
            replacement_dict[key] = (tgt_pair[0].upper(), tgt_pair[1].upper())
        
        # 只对字典中存在的对象执行替换（避免循环所有规则）
        # 使用原有的 replace_identifier 逻辑，但只处理实际需要替换的
        for (src_s, src_o), (tgt_s, tgt_o) in replacement_dict.items():
            # 检查DDL中是否包含这个对象引用（快速预检）
            src_s_u = src_s.upper()
            src_o_u = src_o.upper()
            if src_s_u not in result.upper() or src_o_u not in result.upper():
                continue
            
            # 执行精确替换
            result = replace_identifier(result, src_s, src_o, tgt_s, tgt_o)
        
        # 处理裸名引用（无schema前缀）
        # 规则：
        # - 仅处理“源 schema 内的引用”（裸名默认解析到 CURRENT_SCHEMA）
        # - 当依赖对象的目标 schema != 主对象目标 schema，或目标名称发生变化时，需要补全/替换
        src_schema_u = src_schema.upper()
        main_tgt_schema_u = tgt_schema_u
        for (src_s, src_o), (tgt_s, tgt_o) in replacement_dict.items():
            if src_s.upper() != src_schema_u:
                continue
            src_o_u = src_o.upper()
            if src_o_u == src_name.upper():
                continue
            tgt_s_u = tgt_s.upper()
            tgt_o_u = tgt_o.upper()
            # 若依赖最终落在主对象目标 schema 且名称未变，裸名仍可正确解析
            if tgt_s_u == main_tgt_schema_u and tgt_o_u == src_o_u:
                continue
            if src_o_u not in result.upper():
                continue
            result = replace_unqualified_identifier(result, src_o, tgt_s_u, tgt_o_u)

    def qualify_main_object_creation(text: str) -> str:
        """在 remap 后为主对象的 CREATE 语句补全 schema 前缀。"""
        if not obj_type or not mapping_changed:
            return text
        type_pattern = CREATE_OBJECT_PATTERNS.get(obj_type.upper())
        if not type_pattern:
            return text
        create_prefix = (
            r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?'
            r'(?:FORCE\s+)?'
            r'(?:EDITIONABLE\s+|NONEDITIONABLE\s+)?'
            + type_pattern +
            r'\s+'
        )
        candidates = [tgt_name_u]
        if src_name_u and src_name_u != tgt_name_u:
            candidates.append(src_name_u)

        for cand in candidates:
            pattern = re.compile(
                create_prefix + rf'(?P<name>"?{re.escape(cand)}"?)(?!\s*\.)',
                re.IGNORECASE | re.MULTILINE
            )

            def _repl(match: re.Match) -> str:
                name_txt = match.group('name')
                if '.' in name_txt:
                    return match.group(0)
                return match.group(0).replace(name_txt, f"{tgt_schema_u}.{tgt_name_u}", 1)

            new_text = pattern.sub(_repl, text, count=1)
            if new_text != text:
                return new_text
        return text

    if mapping_changed:
        # PUBLIC SYNONYM 不应在对象名上补 schema 前缀，只需要改名
        if obj_type and obj_type.upper() == 'SYNONYM' and tgt_schema_u == 'PUBLIC':
            return result
        result = qualify_main_object_creation(result)

    return result


DELIMITER_LINE_PATTERN = re.compile(r'^\s*DELIMITER\b.*$', re.IGNORECASE)
BLOCK_END_PATTERN = re.compile(r'^\s*\$\$\s*;?\s*$', re.IGNORECASE)


def cleanup_dbcat_wrappers(ddl: str) -> str:
    """
    dbcat 在导出 PL/SQL 时可能使用 DELIMITER/$$ 包裹。
    这些标记在 OceanBase (Oracle 模式) 中无效，需要移除。
    """
    lines = []
    for line in ddl.splitlines():
        if DELIMITER_LINE_PATTERN.match(line):
            continue
        if BLOCK_END_PATTERN.match(line):
            lines.append('/')
            continue
        lines.append(line)
    return "\n".join(lines)


def prepend_set_schema(ddl: str, schema: str) -> str:
    """
    在 ddl 前加上 ALTER SESSION SET CURRENT_SCHEMA，避免对象落到错误的 schema。
    若已存在 set current schema 指令则不重复添加。
    """
    schema_u = schema.upper()
    lines = ddl.splitlines()
    head = "\n".join(lines[:3]).lower()
    if 'set current_schema' in head:
        return ddl
    prefix = f"ALTER SESSION SET CURRENT_SCHEMA = {schema_u};"
    return "\n".join([prefix, ddl])


USING_INDEX_PATTERN_WITH_OPTIONS = re.compile(
    r'USING\s+INDEX\s*\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)\s*(ENABLE|DISABLE)',
    re.IGNORECASE
)
USING_INDEX_PATTERN_SIMPLE = re.compile(
    r'USING\s+INDEX\s+(ENABLE|DISABLE)',
    re.IGNORECASE
)
MV_REFRESH_ON_DEMAND_PATTERN = re.compile(r'\s+ON\s+DEMAND', re.IGNORECASE)


def clean_plsql_ending(ddl: str) -> str:
    """
    清理PL/SQL对象结尾的语法问题
    
    问题：Oracle允许 END XXXX; 后跟单独的 ; 和 /，但OceanBase要求只能是 /
    修复：移除 END 语句后多余的分号，保留最后的 /
    """
    if not ddl:
        return ddl
    
    lines = ddl.split('\n')
    cleaned_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # 检查是否是 END 语句
        if re.match(r'^\s*END\s+\w+\s*;\s*$', line, re.IGNORECASE):
            cleaned_lines.append(lines[i])  # 保留原始格式的 END 语句
            i += 1
            
            # 跳过后续的单独分号行
            while i < len(lines):
                next_line = lines[i].strip()
                if next_line == ';':
                    i += 1  # 跳过单独的分号
                elif next_line == '/':
                    cleaned_lines.append(lines[i])  # 保留斜杠
                    i += 1
                    break
                elif next_line == '':
                    cleaned_lines.append(lines[i])  # 保留空行
                    i += 1
                else:
                    # 遇到其他内容，停止处理
                    break
        else:
            cleaned_lines.append(lines[i])
            i += 1
    
    return '\n'.join(cleaned_lines)


def clean_extra_semicolons(ddl: str) -> str:
    """
    清理多余的分号
    
    问题：某些语句可能有连续的分号 ;;
    修复：将连续分号替换为单个分号
    """
    if not ddl:
        return ddl
    
    # 替换连续的分号为单个分号
    cleaned = re.sub(r';+', ';', ddl)
    return cleaned


def clean_extra_dots(ddl: str) -> str:
    """
    清理多余的点号
    
    问题：对象名可能有多余的点，如 SCHEMA..TABLE
    修复：将连续点号替换为单个点号
    """
    if not ddl:
        return ddl
    
    # 替换连续的点号为单个点号
    cleaned = re.sub(r'\.+', '.', ddl)
    return cleaned


def clean_trailing_whitespace(ddl: str) -> str:
    """
    清理行尾空白字符
    """
    if not ddl:
        return ddl
    
    lines = ddl.split('\n')
    cleaned_lines = [line.rstrip() for line in lines]
    return '\n'.join(cleaned_lines)


def clean_empty_lines(ddl: str) -> str:
    """
    清理多余的空行（保留适当的空行）
    """
    if not ddl:
        return ddl
    
    # 将连续的多个空行替换为最多2个空行
    cleaned = re.sub(r'\n\s*\n\s*\n+', '\n\n\n', ddl)
    return cleaned


def extract_trigger_table_references(ddl: str) -> Set[str]:
    """
    从触发器DDL中提取引用的表名
    
    Args:
        ddl: 触发器的DDL语句
    
    Returns:
        引用表名的集合，格式为 SCHEMA.TABLE_NAME
    """
    if not ddl:
        return set()
    
    table_refs = set()
    
    # 提取 ON 子句中的表名（触发器定义的表）
    on_pattern = r'\bON\s+("?[A-Z0-9_\$#]+"?(?:\s*\.\s*"?[A-Z0-9_\$#]+"?)?)'
    matches = re.findall(on_pattern, ddl, re.IGNORECASE)
    for match in matches:
        table_name = match.strip().strip('"').upper()
        if '.' in table_name:
            table_refs.add(table_name)
    
    # 提取触发器体中的表引用（INSERT INTO, UPDATE, DELETE FROM等）
    body_patterns = [
        r'\bINSERT\s+INTO\s+("?[A-Z0-9_\$#]+"?(?:\s*\.\s*"?[A-Z0-9_\$#]+"?)?)',
        r'\bUPDATE\s+("?[A-Z0-9_\$#]+"?(?:\s*\.\s*"?[A-Z0-9_\$#]+"?)?)',
        r'\bDELETE\s+FROM\s+("?[A-Z0-9_\$#]+"?(?:\s*\.\s*"?[A-Z0-9_\$#]+"?)?)',
        r'\bFROM\s+("?[A-Z0-9_\$#]+"?(?:\s*\.\s*"?[A-Z0-9_\$#]+"?)?)',
        r'\bJOIN\s+("?[A-Z0-9_\$#]+"?(?:\s*\.\s*"?[A-Z0-9_\$#]+"?)?)',
    ]
    
    for pattern in body_patterns:
        matches = re.findall(pattern, ddl, re.IGNORECASE)
        for match in matches:
            table_name = match.strip().strip('"').upper()
            if '.' in table_name and not table_name.startswith(':'):  # 排除绑定变量
                table_refs.add(table_name)
    
    return table_refs


def remap_trigger_table_references(
    ddl: str,
    full_object_mapping: FullObjectMapping
) -> str:
    """
    根据remap规则重写触发器DDL中的表引用
    
    Args:
        ddl: 原始触发器DDL
        full_object_mapping: 完整的对象映射
    
    Returns:
        重写后的DDL
    """
    if not ddl:
        return ddl
    
    # 提取表引用
    table_refs = extract_trigger_table_references(ddl)
    
    # 构建替换映射
    replacements = {}
    for table_ref in table_refs:
        # 查找该表的目标映射
        tgt_name = find_mapped_target_any_type(
            full_object_mapping,
            table_ref,
            preferred_types=("TABLE",)
        )
        if tgt_name:
            replacements[table_ref] = tgt_name

    # 执行替换
    result_ddl = ddl
    for src_ref, tgt_ref in replacements.items():
        # 使用词边界确保精确匹配，避免部分匹配
        pattern = r'\b' + re.escape(src_ref) + r'\b'
        result_ddl = re.sub(pattern, tgt_ref, result_ddl, flags=re.IGNORECASE)
        log.debug("[TRIGGER] 重映射表引用: %s -> %s", src_ref, tgt_ref)
    
    return result_ddl


def remap_plsql_object_references(
    ddl: str,
    obj_type: str,
    full_object_mapping: FullObjectMapping,
    source_schema: Optional[str] = None
) -> str:
    """
    重映射PL/SQL对象（PROCEDURE、FUNCTION、PACKAGE等）中的对象引用
    改进：
    - 支持 source_schema 以解析本地未限定引用
    - 使用 SqlMasker 保护
    """
    if not ddl:
        return ddl
    
    obj_type_upper = obj_type.upper()
    
    # 触发器需要特殊处理表引用 (保留原逻辑，但可增强)
    if obj_type_upper == 'TRIGGER':
        return remap_trigger_table_references(ddl, full_object_mapping)
    
    masker = SqlMasker(ddl)
    working_sql = masker.masked_sql
    
    # 收集需要替换的引用
    replacements = {}
    preferred_types = (
        "TABLE", "VIEW", "MATERIALIZED VIEW", "SEQUENCE",
        "SYNONYM", "PACKAGE", "PACKAGE BODY", "FUNCTION",
        "PROCEDURE", "TYPE", "TYPE BODY", "TRIGGER"
    )

    # 1. 查找 SCHEMA.OBJECT 格式的引用
    ref_pattern = r'\b([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)\b'
    matches = re.findall(ref_pattern, working_sql, re.IGNORECASE)
    for match in matches:
        ref_name = match.strip().strip('"').upper()
        if '.' in ref_name:
            tgt_name = find_mapped_target_any_type(
                full_object_mapping,
                ref_name,
                preferred_types=preferred_types
            )
            if tgt_name and tgt_name.upper() != ref_name:
                replacements[ref_name] = tgt_name.upper()

    # 2. 查找未限定引用 (如果提供了 source_schema)
    if source_schema:
        # 查找所有可能的标识符
        ident_pattern = r'\b([A-Z_][A-Z0-9_\$#]*)\b'
        candidates = set(re.findall(ident_pattern, working_sql, re.IGNORECASE))
        
        # 排除保留字 (简单列表)
        reserved = {'BEGIN', 'END', 'IF', 'THEN', 'ELSE', 'LOOP', 'COMMIT', 'ROLLBACK', 'SELECT', 'FROM', 'WHERE', 'AND', 'OR'}
        
        for cand in candidates:
            cand_u = cand.upper()
            if cand_u in reserved:
                continue
                
            # 假设它是 source_schema 下的对象
            full_src = f"{source_schema.upper()}.{cand_u}"
            tgt_name = find_mapped_target_any_type(
                full_object_mapping,
                full_src,
                preferred_types=preferred_types
            )
            
            if tgt_name:
                # 仅当目标全名与当前不一致时替换
                # 例如 TAB -> TGT.TAB
                tgt_u = tgt_name.upper()
                if tgt_u != cand_u:
                     replacements[cand_u] = tgt_u

    # 执行替换
    if replacements:
        for src_ref in sorted(replacements.keys(), key=len, reverse=True):
            tgt_ref = replacements[src_ref]
            
            if '.' in src_ref:
                pattern = r'\b' + re.escape(src_ref) + r'\b'
                working_sql = re.sub(pattern, tgt_ref, working_sql, flags=re.IGNORECASE)
            else:
                 # 未限定引用，需确保不匹配已限定引用的尾部
                 pattern = r'(?<![A-Z0-9_\$#"\.])\b' + re.escape(src_ref) + r'\b'
                 working_sql = re.sub(pattern, tgt_ref, working_sql, flags=re.IGNORECASE)
            
            log.debug("[%s] 重映射对象引用: %s -> %s", obj_type_upper, src_ref, tgt_ref)
    
    return masker.unmask(working_sql)


def clean_oracle_hints(ddl: str) -> str:
    """移除Oracle特有的Hint语法"""
    if not ddl:
        return ddl
    return re.sub(r'/\*\+[^*]*\*/', '', ddl, flags=re.DOTALL)


def clean_storage_clauses(ddl: str) -> str:
    """移除Oracle特有的存储子句"""
    if not ddl:
        return ddl
    
    # 移除STORAGE子句
    cleaned = re.sub(r'\s+STORAGE\s*\([^)]+\)', '', ddl, flags=re.IGNORECASE)
    
    # 移除TABLESPACE子句（OceanBase可能不完全兼容）
    cleaned = re.sub(r'\s+TABLESPACE\s+\w+', '', cleaned, flags=re.IGNORECASE)
    
    return cleaned


def clean_pragma_statements(ddl: str) -> str:
    """移除OceanBase不支持的PRAGMA语句"""
    if not ddl:
        return ddl
    
    # 移除PRAGMA AUTONOMOUS_TRANSACTION
    cleaned = re.sub(r'\s*PRAGMA\s+AUTONOMOUS_TRANSACTION\s*;', '', ddl, flags=re.IGNORECASE)
    
    # 移除其他可能的PRAGMA语句
    cleaned = re.sub(r'\s*PRAGMA\s+\w+[^;]*;', '', cleaned, flags=re.IGNORECASE)
    
    return cleaned


def clean_oracle_specific_syntax(ddl: str) -> str:
    """清理Oracle特有语法"""
    if not ddl:
        return ddl
    
    # 移除BFILE数据类型引用
    cleaned = re.sub(r'\bBFILE\b', 'BLOB', ddl, flags=re.IGNORECASE)
    
    # 移除XMLTYPE特殊语法
    cleaned = re.sub(r'\s+XMLTYPE\s+COLUMN\s+\w+\s+XMLSCHEMA[^;]*', '', cleaned, flags=re.IGNORECASE)
    
    return cleaned


def clean_editionable_flags(ddl: str) -> str:
    """移除 EDITIONABLE / NONEDITIONABLE 关键字（OceanBase 不需要）。"""
    if not ddl:
        return ddl
    cleaned = re.sub(r'\b(?:NON)?EDITIONABLE\b', ' ', ddl, flags=re.IGNORECASE)
    return re.sub(r'[ \t]+', ' ', cleaned)


def clean_sequence_unsupported_options(ddl: str) -> str:
    """移除 OceanBase 不支持的 SEQUENCE 选项"""
    if not ddl:
        return ddl
    cleaned = ddl
    # 删除 NOKEEP / NOSCALE / GLOBAL 选项，保留其余内容
    for token in ("NOKEEP", "NOSCALE", "GLOBAL"):
        cleaned = re.sub(rf"\s*\b{token}\b", " ", cleaned, flags=re.IGNORECASE)
    # 收敛多余空格
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r" \n", "\n", cleaned)
    return cleaned


def clean_semicolon_before_slash(ddl: str) -> str:
    """
    移除单独一行的分号，其后紧跟 / 的情况（常见于 PL/SQL 导出）。
    """
    if not ddl:
        return ddl
    lines = ddl.splitlines()
    cleaned: List[str] = []
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped == ';':
            j = i + 1
            while j < len(lines) and lines[j].strip() == '':
                j += 1
            if j < len(lines) and lines[j].strip() == '/':
                i += 1
                continue
        cleaned.append(lines[i])
        i += 1
    return "\n".join(cleaned)


# DDL清理规则配置（更新为包含生产环境规则）
DDL_CLEANUP_RULES = {
    # PL/SQL对象需要特殊的结尾处理和PRAGMA清理
    'PLSQL_OBJECTS': {
        'types': ['PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY', 'TYPE', 'TYPE BODY', 'TRIGGER'],
        'rules': [
            clean_plsql_ending,
            clean_semicolon_before_slash,
            clean_pragma_statements,
            clean_oracle_hints,
            clean_oracle_specific_syntax,
            clean_extra_semicolons,
            clean_extra_dots,
            clean_trailing_whitespace,
            clean_empty_lines,
        ]
    },
    
    # 表对象需要存储子句清理
    'TABLE_OBJECTS': {
        'types': ['TABLE'],
        'rules': [
            clean_storage_clauses,
            clean_oracle_hints,
            clean_oracle_specific_syntax,
            clean_extra_semicolons,
            clean_extra_dots,
            clean_trailing_whitespace,
            clean_empty_lines,
        ]
    },
    
    # SEQUENCE 对象需要移除 OceanBase 不支持的选项
    'SEQUENCE_OBJECTS': {
        'types': ['SEQUENCE'],
        'rules': [
            clean_sequence_unsupported_options,
            clean_oracle_hints,
            clean_oracle_specific_syntax,
            clean_extra_semicolons,
            clean_extra_dots,
            clean_trailing_whitespace,
            clean_empty_lines,
        ]
    },
    
    # 其他对象的通用清理
    'GENERAL_OBJECTS': {
        'types': ['VIEW', 'MATERIALIZED VIEW', 'SYNONYM'],
        'rules': [
            clean_editionable_flags,
            clean_oracle_hints,
            clean_oracle_specific_syntax,
            clean_extra_semicolons,
            clean_extra_dots,
            clean_trailing_whitespace,
            clean_empty_lines,
        ]
    }
}


def apply_ddl_cleanup_rules(ddl: str, obj_type: str) -> str:
    """
    根据对象类型应用相应的DDL清理规则
    
    Args:
        ddl: 原始DDL
        obj_type: 对象类型
    
    Returns:
        清理后的DDL
    """
    if not ddl:
        return ddl
    
    obj_type_upper = obj_type.upper()
    
    # 确定使用哪套规则
    rules_to_apply = []
    
    for rule_set_name, rule_set in DDL_CLEANUP_RULES.items():
        if obj_type_upper in rule_set['types']:
            rules_to_apply = rule_set['rules']
            break
    
    # 如果没有匹配的规则，使用通用规则
    if not rules_to_apply:
        rules_to_apply = DDL_CLEANUP_RULES['GENERAL_OBJECTS']['rules']
    
    # 依次应用所有规则
    cleaned_ddl = ddl
    for rule_func in rules_to_apply:
        try:
            cleaned_ddl = rule_func(cleaned_ddl)
        except Exception as exc:
            log.warning("DDL清理规则 %s 执行失败: %s", rule_func.__name__, exc)
    
    return cleaned_ddl


def add_custom_cleanup_rule(rule_name: str, obj_types: List[str], rule_func: Callable[[str], str]):
    """
    动态添加自定义清理规则
    
    Args:
        rule_name: 规则名称
        obj_types: 适用的对象类型列表
        rule_func: 清理函数，接受DDL字符串，返回清理后的DDL
    
    Example:
        def my_custom_rule(ddl: str) -> str:
            return ddl.replace("OLD_SYNTAX", "NEW_SYNTAX")
        
        add_custom_cleanup_rule("my_rule", ["PROCEDURE", "FUNCTION"], my_custom_rule)
    """
    # 创建新的规则集或更新现有规则集
    rule_set_name = f"CUSTOM_{rule_name.upper()}"
    
    if rule_set_name not in DDL_CLEANUP_RULES:
        DDL_CLEANUP_RULES[rule_set_name] = {
            'types': [t.upper() for t in obj_types],
            'rules': []
        }
    
    # 添加规则函数
    DDL_CLEANUP_RULES[rule_set_name]['rules'].append(rule_func)
    
    log.info("已添加自定义DDL清理规则: %s，适用于对象类型: %s", rule_name, obj_types)


def normalize_ddl_for_ob(ddl: str) -> str:
    """
    清理 DBMS_METADATA 的输出，使其更适合在 OceanBase (Oracle 模式) 上执行：
      - 移除 "USING INDEX ... ENABLE/DISABLE" 之类 Oracle 专有语法
    未来如有更多不兼容语法，可在此扩展。
    """
    ddl = USING_INDEX_PATTERN_WITH_OPTIONS.sub(lambda m: m.group(1), ddl)
    ddl = USING_INDEX_PATTERN_SIMPLE.sub(lambda m: m.group(1), ddl)
    ddl = MV_REFRESH_ON_DEMAND_PATTERN.sub('', ddl)
    return ddl


def enforce_schema_for_ddl(ddl: str, schema: str, obj_type: str) -> str:
    obj_type_u = obj_type.upper()
    if obj_type_u not in DDL_OBJECT_TYPE_OVERRIDE:
        return ddl

    set_stmt = f"ALTER SESSION SET CURRENT_SCHEMA = {schema.upper()};"
    lines = ddl.splitlines()
    insert_idx = 0

    if lines and lines[0].strip().upper().startswith('DELIMITER'):
        insert_idx = 1
        while insert_idx < len(lines) and not lines[insert_idx].strip():
            insert_idx += 1

    lines.insert(insert_idx, set_stmt)
    return "\n".join(lines)


CONSTRAINT_ENABLE_VALIDATE_PATTERN = re.compile(
    r'\s+ENABLE\s+VALIDATE',
    re.IGNORECASE
)
CONSTRAINT_ENABLE_PATTERN = re.compile(
    r'\s+ENABLE(?=\s*;)',
    re.IGNORECASE
)

ENABLE_NOVALIDATE_PATTERN = re.compile(
    r'\s*\bENABLE\s+NOVALIDATE\b',
    re.IGNORECASE
)


def strip_constraint_enable(ddl: str) -> str:
    ddl = CONSTRAINT_ENABLE_VALIDATE_PATTERN.sub(' VALIDATE', ddl)
    ddl = CONSTRAINT_ENABLE_PATTERN.sub('', ddl)
    return ddl


def strip_enable_novalidate(ddl: str) -> str:
    """
    移除行内的 ENABLE NOVALIDATE 关键字组合，以适配 OB 的 CREATE TABLE。
    """
    cleaned_lines: List[str] = []
    for line in ddl.splitlines():
        cleaned = ENABLE_NOVALIDATE_PATTERN.sub('', line)
        cleaned_lines.append(cleaned.rstrip())
    return "\n".join(cleaned_lines)


def split_ddl_statements(ddl: str) -> List[str]:
    """
    以较稳健的方式按顶层分号切分 DDL：
    - 忽略字符串/注释内的分号
    - 简单识别 BEGIN/END 块，在块内不切分
    """
    if not ddl:
        return []

    statements: List[str] = []
    current: List[str] = []
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    begin_depth = 0
    token_buf: List[str] = []

    i = 0
    n = len(ddl)
    while i < n:
        ch = ddl[i]
        nxt = ddl[i + 1] if i + 1 < n else ''

        # 处理行注释
        if in_line_comment:
            current.append(ch)
            if ch == '\n':
                in_line_comment = False
            i += 1
            continue

        # 处理块注释
        if in_block_comment:
            current.append(ch)
            if ch == '*' and nxt == '/':
                current.append(nxt)
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        # 非字符串时识别注释起始
        if not in_single and not in_double:
            if ch == '-' and nxt == '-':
                in_line_comment = True
                current.append(ch)
                current.append(nxt)
                i += 2
                continue
            if ch == '/' and nxt == '*':
                in_block_comment = True
                current.append(ch)
                current.append(nxt)
                i += 2
                continue

        # 处理字符串（单/双引号）
        if not in_double and ch == "'":
            current.append(ch)
            if in_single and nxt == "'":
                # Oracle 单引号转义 ''
                current.append(nxt)
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue

        if not in_single and ch == '"':
            current.append(ch)
            if in_double and nxt == '"':
                current.append(nxt)
                i += 2
                continue
            in_double = not in_double
            i += 1
            continue

        # 累积 token 用于 BEGIN/END 深度探测
        if not in_single and not in_double:
            if ch.isalnum() or ch in ('_', '$', '#'):
                token_buf.append(ch)
            else:
                if token_buf:
                    token = ''.join(token_buf).upper()
                    if token == 'BEGIN':
                        begin_depth += 1
                    elif token == 'END' and begin_depth > 0:
                        begin_depth -= 1
                    token_buf = []

        current.append(ch)

        # 顶层分号切分
        if (not in_single and not in_double and begin_depth == 0 and ch == ';'):
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []

        i += 1

    if token_buf:
        token = ''.join(token_buf).upper()
        if token == 'BEGIN':
            begin_depth += 1
        elif token == 'END' and begin_depth > 0:
            begin_depth -= 1

    tail = ''.join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def extract_statements_for_names(
    ddl: str,
    names: Set[str],
    predicate: Callable[[str], bool]
) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {name.upper(): [] for name in names}
    if not ddl:
        return result

    statements = split_ddl_statements(ddl)
    for stmt in statements:
        stmt_upper = stmt.upper()
        if not predicate(stmt_upper):
            continue
        for name in names:
            name_u = name.upper()
            if (
                f'"{name_u}"' in stmt_upper
                or re.search(rf'\b{re.escape(name_u)}\b', stmt_upper)
            ):
                result.setdefault(name_u, []).append(stmt.strip())
    return result


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def write_fixup_file(
    base_dir: Path,
    subdir: str,
    filename: str,
    content: str,
    header_comment: str,
    grants_to_add: Optional[List[str]] = None,
    extra_comments: Optional[List[str]] = None
):
    target_dir = base_dir / subdir
    ensure_dir(target_dir)
    file_path = target_dir / filename
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(f"-- {header_comment}\n")
        f.write("-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。\n\n")
        if extra_comments:
            for line in extra_comments:
                if line:
                    f.write(f"-- {line}\n")
            f.write("\n")
        
        body = content.strip()
        f.write(body)
        f.write('\n')
        tail = body.rstrip()
        if tail and not tail.endswith((';', '/')):
            f.write(';\n')

        if grants_to_add:
            f.write('\n-- 自动追加相关授权语句\n')
            for grant_stmt in sorted(grants_to_add):
                f.write(f"{grant_stmt}\n")

    log.info(f"[FIXUP] 生成目标端订正 SQL: {file_path}")


def format_oracle_column_type(
    info: Dict,
    *,
    override_length: Optional[int] = None,
    prefer_ob_varchar: bool = False
) -> str:
    """
    Render an Oracle column definition using available metadata without dropping
    precision/scale/length/semantics.
    """
    raw_dt = (info.get("data_type") or "").strip()
    dt = raw_dt.upper()
    prec = info.get("data_precision")
    scale = info.get("data_scale")
    data_length = info.get("data_length")
    char_length = info.get("char_length")
    char_used = (info.get("char_used") or "").strip().upper()

    def strip_byte_suffix(type_literal: str, base_type: str) -> str:
        if base_type not in ("VARCHAR2", "VARCHAR"):
            return type_literal
        return re.sub(r'\s+BYTE\b', '', type_literal, flags=re.IGNORECASE)

    def apply_varchar_pref(type_literal: str) -> str:
        literal = type_literal
        if prefer_ob_varchar and literal.startswith("VARCHAR2"):
            literal = "VARCHAR" + literal[len("VARCHAR2"):]
        literal = strip_byte_suffix(literal, "VARCHAR2")
        literal = strip_byte_suffix(literal, "VARCHAR")
        return literal

    if is_long_type(dt):
        return map_long_type_to_ob(dt)

    # If data_type already carries explicit precision/length (e.g., TIMESTAMP(6)), respect it.
    if '(' in dt and override_length is None:
        return apply_varchar_pref(dt)

    # Length semantics suffix for VARCHAR/VARCHAR2 (CHAR vs BYTE)
    def _char_suffix(base_type: str) -> str:
        if base_type not in ("VARCHAR", "VARCHAR2"):
            return ""
        if char_used == "C":
            return " CHAR"
        # BYTE 语义不需要显式指定（OceanBase 默认就是 BYTE）
        return ""

    # Choose effective length with optional override
    def _pick_length(default_len: Optional[int]) -> Optional[int]:
        return override_length if override_length is not None else default_len

    # NUMBER-like
    if dt in ("NUMBER", "DECIMAL", "NUMERIC"):
        if prec is not None:
            if scale is not None:
                return f"{dt}({int(prec)},{int(scale)})"
            return f"{dt}({int(prec)})"
        if scale is not None:
            return f"{dt}({int(scale)})"
        return dt

    # FLOAT
    if dt == "FLOAT":
        if prec is not None:
            return f"{dt}({int(prec)})"
        return dt

    # TIMESTAMP family
    if dt.startswith("TIMESTAMP"):
        if "WITH LOCAL TIME ZONE" in dt:
            suffix = " WITH LOCAL TIME ZONE"
        elif "WITH TIME ZONE" in dt:
            suffix = " WITH TIME ZONE"
        else:
            suffix = ""
        base = "TIMESTAMP"
        if scale is not None:
            return f"{base}({int(scale)}){suffix}"
        return f"{base}{suffix}"

    # INTERVAL family
    if dt.startswith("INTERVAL YEAR"):
        if prec is not None or scale is not None:
            year_prec = int(prec) if prec is not None else 2
            return f"INTERVAL YEAR({year_prec}) TO MONTH"
        return "INTERVAL YEAR TO MONTH"
    if dt.startswith("INTERVAL DAY"):
        if prec is not None or scale is not None:
            day_prec = int(prec) if prec is not None else 2
            frac_prec = int(scale) if scale is not None else 6
            return f"INTERVAL DAY({day_prec}) TO SECOND({frac_prec})"
        return "INTERVAL DAY TO SECOND"

    # VARCHAR/VARCHAR2 with length semantics (CHAR vs BYTE)
    if dt in ("VARCHAR", "VARCHAR2"):
        ln = _pick_length(char_length if char_used == "C" else (char_length or data_length))
        if ln is not None:
            return apply_varchar_pref(f"{dt}({int(ln)}){_char_suffix(dt)}")
        return apply_varchar_pref(dt)
    
    # CHAR type (separate from VARCHAR length semantics)
    if dt == "CHAR":
        ln = _pick_length(char_length if char_used == "C" else (char_length or data_length))
        if ln is not None:
            suffix = " CHAR" if char_used == "C" else ""
            return f"{dt}({int(ln)}){suffix}"
        return dt

    # National character types (length is character-based; no CHAR/BYTE suffix)
    if dt in ("NCHAR", "NVARCHAR2"):
        ln = _pick_length(char_length or data_length)
        if ln is not None:
            return f"{dt}({int(ln)})"
        return dt

    # Binary/ROWID with length
    if dt in ("RAW", "VARBINARY"):
        ln = _pick_length(data_length)
        if ln is not None:
            return f"{dt}({int(ln)})"
        return dt

    if dt == "UROWID":
        ln = _pick_length(data_length)
        if ln is not None:
            return f"{dt}({int(ln)})"
        return dt

    # Fallback
    return apply_varchar_pref(dt)


def inflate_table_varchar_lengths(
    ddl: str,
    src_schema: str,
    src_table: str,
    oracle_meta: OracleMetadata
) -> str:
    """
    将表 DDL 中 VARCHAR/VARCHAR2 列的长度放大到 ceil(src*1.5)，避免修补后再次被长度校验拦截。
    仅在元数据可用且实际长度不足时修改。
    """
    col_meta = oracle_meta.table_columns.get((src_schema.upper(), src_table.upper()))
    if not col_meta:
        return ddl

    replacements = 0
    updated = ddl

    for col_name, info in col_meta.items():
        dtype = (info.get("data_type") or "").strip().upper()
        if dtype not in ("VARCHAR2", "VARCHAR"):
            continue
        
        # 只对BYTE语义的列进行放大，CHAR语义保持原样
        char_used = (info.get("char_used") or "").strip().upper()
        if char_used == 'C':
            continue
        
        src_len = info.get("char_length") or info.get("data_length")
        try:
            src_len_int = int(src_len)
        except (TypeError, ValueError):
            continue
        min_len = int(math.ceil(src_len_int * VARCHAR_LEN_MIN_MULTIPLIER))

        col_pat = re.escape(col_name)
        pattern = re.compile(
            rf'(?P<prefix>"{col_pat}"\s+|{col_pat}\s+)'
            rf'(?P<dtype>VARCHAR2|VARCHAR)\s*\(\s*(?P<len>\d+)\s*\)'
            rf'(?P<suffix>\s*(?:BYTE|CHAR)?)',
            re.IGNORECASE
        )

        def _repl(match: re.Match) -> str:
            nonlocal replacements
            current_len = int(match.group("len"))
            if current_len >= min_len:
                return match.group(0)
            replacements += 1
            prefix = match.group("prefix")
            dtype_literal = match.group("dtype")
            suffix = match.group("suffix") or ""
            return f"{prefix}{dtype_literal}({min_len}){suffix}"

        updated, _ = pattern.subn(_repl, updated, count=1)

    if replacements:
        log.info("[DDL] 放大 %s.%s 中 %d 个 VARCHAR 列至校验下限。", src_schema, src_table, replacements)
    return updated


def generate_alter_for_table_columns(
    oracle_meta: OracleMetadata,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
    missing_cols: Set[str],
    extra_cols: Set[str],
    length_mismatches: List[ColumnLengthIssue],
    type_mismatches: List[ColumnTypeIssue]
) -> Optional[str]:
    """
    为一个具体的表生成 ALTER TABLE 脚本：
      - 对 missing_cols 生成 ADD COLUMN
      - 对 extra_cols 生成注释掉的 DROP COLUMN 建议
      - 对 length_mismatches 生成 MODIFY COLUMN
      - 对 type_mismatches (LONG/LONG RAW) 生成 MODIFY COLUMN
    """
    if not missing_cols and not extra_cols and not length_mismatches and not type_mismatches:
        return None

    col_details = oracle_meta.table_columns.get((src_schema.upper(), src_table.upper()))
    if col_details is None:
        log.warning(f"[ALTER] 未找到 {src_schema}.{src_table} 的列元数据，跳过 ALTER 生成。")
        return None

    lines: List[str] = []
    tgt_schema_u = tgt_schema.upper()
    tgt_table_u = tgt_table.upper()

    # 缺失列：ADD
    if missing_cols:
        lines.append(f"-- 源端存在而目标端缺失的列，将通过 ALTER TABLE ADD 补齐：")
        for col in sorted(missing_cols):
            info = col_details.get(col)
            if not info:
                lines.append(f"-- WARNING: 源端未找到列 {col} 的详细定义，需手工补充。")
                continue

            col_u = col.upper()
            override_len = None
            dtype = (info.get("data_type") or "").upper()
            if dtype in ("VARCHAR2", "VARCHAR"):
                src_len = info.get("char_length") or info.get("data_length")
                try:
                    src_len_int = int(src_len)
                    override_len = int(math.ceil(src_len_int * VARCHAR_LEN_MIN_MULTIPLIER))
                except (TypeError, ValueError):
                    override_len = None

            col_type = format_oracle_column_type(
                info,
                override_length=override_len,
                prefer_ob_varchar=True
            )
            default_clause = ""
            default_val = info.get("data_default")
            if default_val is not None:
                default_str = str(default_val).strip()
                if default_str:
                    default_clause = f" DEFAULT {default_str}"

            nullable_clause = " NOT NULL" if (info.get("nullable") == "N") else ""

            lines.append(
                f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} "
                f"ADD ({col_u} {col_type}{default_clause}{nullable_clause});"
            )

    # 为新增列生成注释语句（过滤OMS列）
    col_comments = oracle_meta.column_comments.get((src_schema.upper(), src_table.upper()), {})
    added_col_comments: List[str] = []
    for col in sorted(missing_cols):
        col_u = col.upper()
        # 跳过OMS列的注释
        if is_ignored_oms_column(col_u):
            continue
        comment = col_comments.get(col_u)
        if comment:
            # 转义单引号
            comment_escaped = comment.replace("'", "''")
            added_col_comments.append(
                f"COMMENT ON COLUMN {tgt_schema_u}.{tgt_table_u}.{col_u} IS '{comment_escaped}';"
            )
    if added_col_comments:
        lines.append("")
        lines.append("-- 新增列的注释：")
        lines.extend(added_col_comments)

    # 长度不匹配：MODIFY
    if length_mismatches:
        lines.append("")
        lines.append("-- 列长度不匹配：")
        for issue in length_mismatches:
            col_name, src_len, tgt_len, limit_len, issue_type = issue
            info = col_details.get(col_name)
            if not info:
                continue

            if issue_type == 'char_mismatch':
                # CHAR语义：要求长度完全一致
                modified_type = format_oracle_column_type(
                    info,
                    override_length=src_len,
                    prefer_ob_varchar=True
                )
                lines.append(
                    f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} "
                    f"MODIFY ({col_name.upper()} {modified_type}); "
                    f"-- CHAR语义，源长度: {src_len}, 目标长度: {tgt_len}, 要求一致"
                )
            elif issue_type == 'short':
                # BYTE语义：放大到校验下限
                modified_type = format_oracle_column_type(
                    info,
                    override_length=limit_len,
                    prefer_ob_varchar=True
                )
                lines.append(
                    f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} "
                    f"MODIFY ({col_name.upper()} {modified_type}); "
                    f"-- BYTE语义，源长度: {src_len}, 目标长度: {tgt_len}, 期望下限: {limit_len}"
                )
            else:
                lines.append(
                    f"-- WARNING: {col_name.upper()} 长度过大 (源={src_len}, 目标={tgt_len}, "
                    f"建议上限={limit_len})，请人工评估是否需要收敛。"
                )

    # 类型不匹配：MODIFY (LONG/LONG RAW -> CLOB/BLOB)
    if type_mismatches:
        lines.append("")
        lines.append("-- 列类型不匹配 (LONG/LONG RAW)：")
        for issue in type_mismatches:
            col_name, src_type, tgt_type, expected_type = issue
            info = col_details.get(col_name)
            if not info:
                continue
            lines.append(
                f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} "
                f"MODIFY ({col_name.upper()} {expected_type}); "
                f"-- 源类型: {src_type}, 目标类型: {tgt_type}"
            )

    # 多余列：DROP（注释掉，供人工评估）
    if extra_cols:
        lines.append("")
        lines.append("-- 目标端存在而源端不存在的列，以下 DROP COLUMN 为建议操作，请谨慎执行：")
        for col in sorted(extra_cols):
            col_u = col.upper()
            lines.append(
                f"-- ALTER TABLE {tgt_schema_u}.{tgt_table_u} "
                f"DROP COLUMN {col_u};"
            )

    return "\n".join(lines) if lines else None


def generate_fixup_scripts(
    ora_cfg: OraConfig,
    ob_cfg: ObConfig,
    settings: Dict,
    tv_results: ReportResults,
    extra_results: ExtraCheckResults,
    master_list: MasterCheckList,
    oracle_meta: OracleMetadata,
    full_object_mapping: FullObjectMapping,
    remap_rules: RemapRules,
    grant_plan: Optional[GrantPlan] = None,
    enable_grant_generation: bool = True,
    dependency_report: Optional[DependencyReport] = None,
    ob_meta: Optional[ObMetadata] = None,
    expected_dependency_pairs: Optional[Set[Tuple[str, str, str, str]]] = None,
    synonym_metadata: Optional[Dict[Tuple[str, str], SynonymMeta]] = None,
    trigger_filter_entries: Optional[Set[str]] = None,
    trigger_filter_enabled: bool = False,
    package_results: Optional[PackageCompareResults] = None,
    report_dir: Optional[Path] = None,
    report_timestamp: Optional[str] = None
):
    """
    基于校验结果生成 fixup_scripts DDL 脚本，并按依赖顺序排列：
      1. SEQUENCE
      2. TABLE (CREATE)
      3. TABLE (ALTER - for column diffs)
      4. VIEW / MATERIALIZED VIEW 等代码对象
      5. INDEX
      6. CONSTRAINT
    7. TRIGGER
    8. 依赖重编译 (ALTER ... COMPILE)
    9. 授权脚本 (对象/角色/系统)

    如果配置了 trigger_list，则仅生成清单中列出的触发器脚本。
    """
    try:
        progress_log_interval = float(settings.get('progress_log_interval', 10))
    except (TypeError, ValueError):
        progress_log_interval = 10.0
    progress_log_interval = max(1.0, progress_log_interval)
    synonym_meta_map = synonym_metadata or {}
    trigger_filter_set = {t.upper() for t in (trigger_filter_entries or set())}
    if trigger_filter_enabled:
        log.info("[FIXUP] 已启用 trigger_list 过滤，清单条目数=%d。", len(trigger_filter_set))
        if not trigger_filter_set:
            log.warning("[FIXUP] trigger_list 为空，已回退全量 TRIGGER 生成。")
            trigger_filter_enabled = False
    allowed_synonym_targets = {s.upper() for s in settings.get('source_schemas_list', [])}

    base_dir = Path(settings.get('fixup_dir', 'fixup_scripts')).expanduser()
    log.info("[FIXUP] 准备生成修补脚本，目标目录=%s", base_dir.resolve())
    view_chain_file: Optional[Path] = None

    if not master_list:
        log.info("[FIXUP] master_list 为空，未生成目标端订正 SQL。")
        return None

    ensure_dir(base_dir)
    safe_to_clean = False
    try:
        base_resolved = base_dir.resolve()
        run_root = Path.cwd().resolve()
        safe_to_clean = (not base_dir.is_absolute()) or (run_root == base_resolved or run_root in base_resolved.parents)
    except Exception:
        safe_to_clean = not base_dir.is_absolute()

    if safe_to_clean:
        removed_files = 0
        removed_dirs = 0
        log.info("[FIXUP] 正在清理旧脚本目录: %s", base_dir.resolve())
        for child in base_dir.iterdir():
            if child.is_file():
                child.unlink()
                removed_files += 1
            elif child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
                removed_dirs += 1
        log.info("[FIXUP] 旧脚本清理完成: files=%d, dirs=%d", removed_files, removed_dirs)
    else:
        log.warning(
            "[FIXUP] fixup_dir=%s 位于运行目录之外，已跳过自动清理以避免误删。",
            base_dir.resolve()
        )

    log.info(f"[FIXUP] 目标端订正 SQL 将生成到目录: {base_dir.resolve()}")
    if not settings.get('enable_ddl_punct_sanitize', True):
        log.info("[DDL_CLEAN] 已关闭 PL/SQL 全角标点清洗。")

    table_map: Dict[str, str] = {
        tgt_name: src_name
        for (src_name, tgt_name, obj_type) in master_list
        if obj_type.upper() == 'TABLE'
    }

    object_replacements: List[Tuple[Tuple[str, str], Tuple[str, str]]] = []
    replacement_set: Set[Tuple[str, str, str, str]] = set()
    for src_name, type_map in full_object_mapping.items():
        for tgt_name in type_map.values():
            try:
                src_schema, src_object = src_name.split('.', 1)
                tgt_schema, tgt_object = tgt_name.split('.', 1)
            except ValueError:
                continue
            key = (src_schema.upper(), src_object.upper(), tgt_schema.upper(), tgt_object.upper())
            if key in replacement_set:
                continue
            object_replacements.append(((key[0], key[1]), (key[2], key[3])))
            replacement_set.add(key)

    all_replacements = list(object_replacements)
    # 预构建按schema索引的替换表，加速 lookup
    replacements_by_schema: Dict[str, List[Tuple[Tuple[str, str], Tuple[str, str]]]] = defaultdict(list)
    for (src_s, src_o), (tgt_s, tgt_o) in all_replacements:
        for sch in (src_s.upper(), tgt_s.upper()):
            replacements_by_schema[sch].append(((src_s, src_o), (tgt_s, tgt_o)))

    def get_relevant_replacements(src_schema: str) -> List[Tuple[Tuple[str, str], Tuple[str, str]]]:
        """
        返回与指定源schema相关的replacements（已按schema预索引，加速匹配）。
        相关规则包括：
        1. 源schema匹配的规则
        2. 目标schema匹配的规则（处理跨schema引用）
        """
        return replacements_by_schema.get(src_schema.upper(), [])

    obj_type_to_dir = {
        'TABLE': 'table',
        'VIEW': 'view',
        'MATERIALIZED VIEW': 'materialized_view',
        'PROCEDURE': 'procedure',
        'FUNCTION': 'function',
        'PACKAGE': 'package',
        'PACKAGE BODY': 'package_body',
        'SYNONYM': 'synonym',
        'JOB': 'job',
        'SCHEDULE': 'schedule',
        'TYPE': 'type',
        'TYPE BODY': 'type_body',
        'SEQUENCE': 'sequence',
        'TRIGGER': 'trigger'
    }

    fixup_schema_filter: Set[str] = set(settings.get('fixup_schema_list', []))
    fixup_type_filter: Set[str] = set(settings.get('fixup_type_set', []))
    fixup_schema_used_source_match = False

    ddl_source_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    ddl_source_lock = threading.Lock()

    def mark_source(obj_type: str, source: str) -> None:
        with ddl_source_lock:
            ddl_source_stats[obj_type.upper()][source] += 1

    ddl_clean_records: List[DdlCleanReportRow] = []
    ddl_clean_lock = threading.Lock()

    def record_ddl_clean(
        obj_type: str,
        obj_full: str,
        replaced: int,
        samples: List[Tuple[str, str]]
    ) -> None:
        if replaced <= 0:
            return
        row = DdlCleanReportRow(
            obj_type=obj_type.upper(),
            obj_full=obj_full,
            replaced=replaced,
            samples=samples or []
        )
        with ddl_clean_lock:
            ddl_clean_records.append(row)

    def source_tag(label: str) -> str:
        return f"[{label}]"

    def allow_fixup(obj_type: str, tgt_schema: str, src_schema: Optional[str] = None) -> bool:
        nonlocal fixup_schema_used_source_match
        obj_type_u = obj_type.upper()
        schema_u = tgt_schema.upper() if tgt_schema else ""
        if fixup_type_filter and obj_type_u not in fixup_type_filter:
            return False
        if not fixup_schema_filter:
            return True
        if schema_u in fixup_schema_filter:
            return True
        src_schema_u = (src_schema or "").upper()
        if src_schema_u and src_schema_u in fixup_schema_filter:
            if not fixup_schema_used_source_match:
                log.info(
                    "[FIXUP] fixup_schemas 命中源 schema=%s (目标=%s)，已放行生成。",
                    src_schema_u,
                    schema_u or "-"
                )
                fixup_schema_used_source_match = True
            return True
        return False

    def fetch_ddl_with_timing(schema: str, obj_type: str, obj_name: str) -> Tuple[Optional[str], str, float]:
        """
        返回 (DDL, 来源标签, 耗时秒)，来源标签为:
          - DBCAT_CACHE / DBCAT_RUN / DBCAT_UNKNOWN
          - DBMS_METADATA
          - MISSING
        """
        key = (schema.upper(), obj_type.upper(), obj_name.upper())
        start_time = time.time()

        # 快速路径：同义词使用缓存元数据直接生成，避免逐个 DBMS_METADATA
        if obj_type.upper() == 'SYNONYM':
            syn_meta = synonym_meta_map.get((schema.upper(), obj_name.upper()))
            if syn_meta and syn_meta.table_name:
                syn_name = syn_meta.name.split('.', 1)[-1]
                target = syn_meta.table_owner
                if target:
                    target = f"{target}.{syn_meta.table_name}"
                else:
                    target = syn_meta.table_name
                if syn_meta.db_link:
                    target = f"{target}@{syn_meta.db_link}"
                if syn_meta.owner == 'PUBLIC':
                    ddl = f"CREATE OR REPLACE PUBLIC SYNONYM {syn_name} FOR {target};"
                else:
                    ddl = f"CREATE OR REPLACE SYNONYM {syn_meta.owner}.{syn_name} FOR {target};"
                elapsed = time.time() - start_time
                log.info(
                    "[DDL_FETCH] %s.%s (%s) 来源=META_SYN 耗时=%.3fs",
                    schema, obj_name, obj_type, elapsed
                )
                return ddl, "META_SYN", elapsed

        # VIEW 固定使用 DBMS_METADATA，忽略 dbcat 输出
        if obj_type.upper() == 'VIEW':
            ddl = get_fallback_ddl(schema, obj_type, obj_name)
            source_label = "DBMS_METADATA" if ddl else "MISSING"
            elapsed = time.time() - start_time
            log.info(
                "[DDL_FETCH] %s.%s (%s) 来源=%s 耗时=%.3fs",
                schema, obj_name, obj_type, source_label, elapsed
            )
            return ddl, source_label, elapsed

        ddl = (
            dbcat_data
            .get(schema.upper(), {})
            .get(obj_type.upper(), {})
            .get(obj_name.upper())
        )
        source_label = "MISSING"
        elapsed_hint = None

        if ddl:
            meta = ddl_source_meta.get(key)
            if meta:
                source_label = "DBCAT_CACHE" if meta[0] == "cache" else "DBCAT_RUN"
                # 对于缓存，使用实际读取耗时；对于dbcat_run，使用记录的平均耗时
                if meta[0] == "cache":
                    elapsed_hint = time.time() - start_time
                else:
                    elapsed_hint = meta[1]
            else:
                # DDL存在但无元数据，可能是缓存部分加载
                source_label = "DBCAT"
                elapsed_hint = time.time() - start_time
        else:
            ddl = get_fallback_ddl(schema, obj_type, obj_name)
            if ddl:
                source_label = "DBMS_METADATA"
            else:
                source_label = "MISSING"

        elapsed = elapsed_hint if elapsed_hint is not None else (time.time() - start_time)
        
        # 只在非缓存或耗时较长时输出详细日志
        if source_label != "DBCAT_CACHE" or elapsed > 0.1:
            log.info(
                "[DDL_FETCH] %s.%s (%s) 来源=%s 耗时=%.3fs",
                schema, obj_name, obj_type, source_label, elapsed
            )
        return ddl, source_label, elapsed

    def build_progress_tracker(total: int, label: str) -> Callable[[Optional[str]], None]:
        state = {"done": 0, "last": time.time()}
        state_lock = threading.Lock()
        if total:
            log.info("%s 总计 %d 个。", label, total)

        def _tick(extra: Optional[str] = None) -> None:
            if total <= 0:
                return
            with state_lock:
                state["done"] += 1
                now = time.time()
                should_log = state["done"] == total or (now - state["last"]) >= progress_log_interval
                if should_log:
                    state["last"] = now
                    suffix = f" [{extra}]" if extra else ""
                    pct = state["done"] * 100.0 / total if total else 100.0
                    log.info("%s 进度 %d/%d (%.1f%%)%s", label, state["done"], total, pct, suffix)

        return _tick

    def build_compile_order(
        tasks: Dict[Tuple[str, str, str], Set[str]],
        expected_pairs: Optional[Set[Tuple[str, str, str, str]]]
    ) -> List[Tuple[str, str, str]]:
        if not tasks:
            return []
        node_lookup: Dict[Tuple[str, str], Tuple[str, str, str]] = {}
        for key in tasks.keys():
            node_lookup[(f"{key[0]}.{key[1]}", key[2])] = key

        graph: Dict[Tuple[str, str, str], Set[Tuple[str, str, str]]] = defaultdict(set)
        if expected_pairs:
            for dep_name, dep_type, ref_name, ref_type in expected_pairs:
                dep_key = node_lookup.get((dep_name, dep_type))
                ref_key = node_lookup.get((ref_name, ref_type))
                if dep_key and ref_key and dep_key != ref_key:
                    graph[dep_key].add(ref_key)

        order: List[Tuple[str, str, str]] = []
        visiting: Set[Tuple[str, str, str]] = set()
        visited: Set[Tuple[str, str, str]] = set()
        cycles: List[str] = []

        def dfs(node: Tuple[str, str, str], stack: List[str]) -> None:
            if node in visited:
                return
            if node in visiting:
                cycles.append(" -> ".join(stack + [f"{node[0]}.{node[1]} ({node[2]})"]))
                return
            visiting.add(node)
            for dep in graph.get(node, set()):
                dfs(dep, stack + [f"{node[0]}.{node[1]} ({node[2]})"])
            visiting.remove(node)
            visited.add(node)
            order.append(node)

        for node in sorted(tasks.keys()):
            dfs(node, [])

        if cycles:
            log.warning("[FIXUP] 发现依赖环，编译顺序将按拓扑结果执行: %s", " | ".join(cycles))

        return order if order else list(sorted(tasks.keys()))

    grant_enabled = bool(enable_grant_generation)
    if grant_plan is None:
        grant_plan = GrantPlan(
            object_grants={},
            sys_privs={},
            role_privs={},
            role_ddls=[],
            filtered_grants=[]
        )
    if not grant_enabled:
        grant_plan = GrantPlan(
            object_grants={},
            sys_privs={},
            role_privs={},
            role_ddls=[],
            filtered_grants=[]
        )

    object_grants_by_grantee: Dict[str, Set[ObjectGrantEntry]] = defaultdict(set)
    sys_privs_by_grantee: Dict[str, Set[SystemGrantEntry]] = defaultdict(set)
    role_privs_by_grantee: Dict[str, Set[RoleGrantEntry]] = defaultdict(set)

    if grant_enabled:
        for grantee, entries in grant_plan.object_grants.items():
            if entries:
                object_grants_by_grantee[(grantee or "").upper()].update(entries)
        for grantee, entries in grant_plan.sys_privs.items():
            if entries:
                sys_privs_by_grantee[(grantee or "").upper()].update(entries)
        for grantee, entries in grant_plan.role_privs.items():
            if entries:
                role_privs_by_grantee[(grantee or "").upper()].update(entries)

    object_grant_lookup: Dict[str, List[str]] = {}
    grants_by_owner: Dict[str, Set[str]] = {}
    merge_privileges = parse_bool_flag(settings.get('grant_merge_privileges', 'true'), True)
    merge_grantees = parse_bool_flag(settings.get('grant_merge_grantees', 'true'), True)

    def format_object_grant(grantee: str, entry: ObjectGrantEntry) -> str:
        stmt = f"GRANT {entry.privilege.upper()} ON {entry.object_full.upper()} TO {grantee.upper()}"
        if entry.grantable:
            stmt += " WITH GRANT OPTION"
        return stmt + ";"

    def format_object_grant_stmt(
        privileges: List[str],
        object_full: str,
        grantees: List[str],
        grantable: bool
    ) -> str:
        priv_part = ", ".join(privileges)
        grantee_part = ", ".join(grantees)
        stmt = f"GRANT {priv_part} ON {object_full.upper()} TO {grantee_part}"
        if grantable:
            stmt += " WITH GRANT OPTION"
        return stmt + ";"

    def format_sys_grant(grantee: str, entry: SystemGrantEntry) -> str:
        stmt = f"GRANT {entry.privilege.upper()} TO {grantee.upper()}"
        if entry.admin_option:
            stmt += " WITH ADMIN OPTION"
        return stmt + ";"

    def format_role_grant(grantee: str, entry: RoleGrantEntry) -> str:
        stmt = f"GRANT {entry.role.upper()} TO {grantee.upper()}"
        if entry.admin_option:
            stmt += " WITH ADMIN OPTION"
        return stmt + ";"

    def add_object_grant(grantee: str, privilege: str, object_full: str, grantable: bool = False) -> None:
        if not grant_enabled:
            return
        grantee_u = (grantee or "").upper()
        object_u = (object_full or "").upper()
        privilege_u = (privilege or "").upper()
        if not grantee_u or not object_u or not privilege_u:
            return
        if grantable:
            object_grants_by_grantee[grantee_u].discard(ObjectGrantEntry(
                privilege=privilege_u,
                object_full=object_u,
                grantable=False
            ))
            object_grants_by_grantee[grantee_u].add(ObjectGrantEntry(
                privilege=privilege_u,
                object_full=object_u,
                grantable=True
            ))
            return
        if ObjectGrantEntry(privilege_u, object_u, True) in object_grants_by_grantee[grantee_u]:
            return
        object_grants_by_grantee[grantee_u].add(ObjectGrantEntry(
            privilege=privilege_u,
            object_full=object_u,
            grantable=False
        ))

    def collect_grants_for_object(target_full: str) -> List[str]:
        if not grant_enabled:
            return []
        return sorted(set(object_grant_lookup.get(target_full.upper(), [])))

    def build_object_grant_statements_for(
        grants_by_grantee: Dict[str, Set[ObjectGrantEntry]]
    ) -> Tuple[int, int, Dict[str, List[str]], Dict[str, Set[str]]]:
        object_grant_lookup_local: Dict[str, List[str]] = defaultdict(list)
        grants_by_owner_local: Dict[str, Set[str]] = defaultdict(set)
        raw_count = sum(len(v) for v in grants_by_grantee.values())
        if not raw_count:
            return 0, 0, dict(object_grant_lookup_local), dict(grants_by_owner_local)
        object_index: Dict[str, Dict[bool, Dict[str, Set[str]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        for grantee, entries in grants_by_grantee.items():
            grantee_u = (grantee or "").upper()
            if not grantee_u:
                continue
            for entry in entries:
                if not entry or not entry.object_full:
                    continue
                obj_full_u = entry.object_full.upper()
                object_index[obj_full_u][bool(entry.grantable)][grantee_u].add(entry.privilege.upper())

        merged_count = 0
        for obj_full, grantable_map in object_index.items():
            owner = obj_full.split('.', 1)[0] if '.' in obj_full else obj_full
            for grantable, grantee_map in grantable_map.items():
                if merge_grantees:
                    if merge_privileges:
                        group_by_privs: Dict[Tuple[str, ...], Set[str]] = defaultdict(set)
                        for grantee_u, privs in grantee_map.items():
                            priv_key = tuple(sorted(privs))
                            group_by_privs[priv_key].add(grantee_u)
                        for priv_key, grantees in group_by_privs.items():
                            stmt = format_object_grant_stmt(
                                list(priv_key),
                                obj_full,
                                sorted(grantees),
                                grantable
                            )
                            object_grant_lookup_local[obj_full].append(stmt)
                            grants_by_owner_local[owner].add(stmt)
                            merged_count += 1
                    else:
                        group_by_priv: Dict[str, Set[str]] = defaultdict(set)
                        for grantee_u, privs in grantee_map.items():
                            for priv in privs:
                                group_by_priv[priv].add(grantee_u)
                        for priv, grantees in group_by_priv.items():
                            stmt = format_object_grant_stmt(
                                [priv],
                                obj_full,
                                sorted(grantees),
                                grantable
                            )
                            object_grant_lookup_local[obj_full].append(stmt)
                            grants_by_owner_local[owner].add(stmt)
                            merged_count += 1
                else:
                    for grantee_u, privs in grantee_map.items():
                        if merge_privileges:
                            stmt = format_object_grant_stmt(
                                sorted(privs),
                                obj_full,
                                [grantee_u],
                                grantable
                            )
                            object_grant_lookup_local[obj_full].append(stmt)
                            grants_by_owner_local[owner].add(stmt)
                            merged_count += 1
                        else:
                            for priv in sorted(privs):
                                stmt = format_object_grant_stmt(
                                    [priv],
                                    obj_full,
                                    [grantee_u],
                                    grantable
                                )
                                object_grant_lookup_local[obj_full].append(stmt)
                                grants_by_owner_local[owner].add(stmt)
                                merged_count += 1
        return raw_count, merged_count, dict(object_grant_lookup_local), dict(grants_by_owner_local)

    def pre_add_cross_schema_grants(
        constraint_tasks: List[Tuple[ConstraintMismatch, str, str, str, str]],
        trigger_tasks: List[Tuple[str, str, str, str, str, str, str]]
    ) -> int:
        if not grant_enabled:
            return 0
        added = 0
        for item, src_schema, src_table, tgt_schema, _tgt_table in constraint_tasks:
            cons_map = oracle_meta.constraints.get((src_schema.upper(), src_table.upper()), {})
            for cons_name in item.missing_constraints:
                cons_meta = cons_map.get(cons_name.upper())
                if not cons_meta:
                    continue
                ctype = (cons_meta.get("type") or "").upper()
                if ctype != "R":
                    continue
                ref_owner = cons_meta.get("ref_table_owner") or cons_meta.get("r_owner")
                ref_table = cons_meta.get("ref_table_name")
                if not ref_owner or not ref_table:
                    continue
                ref_owner_u = ref_owner.upper()
                ref_table_u = ref_table.upper()
                ref_src_full = f"{ref_owner_u}.{ref_table_u}"
                ref_tgt_full = get_mapped_target(full_object_mapping, ref_src_full, 'TABLE') or ref_src_full
                if '.' in ref_tgt_full:
                    ref_tgt_schema, ref_tgt_table = ref_tgt_full.split('.', 1)
                else:
                    ref_tgt_schema, ref_tgt_table = ref_owner_u, ref_table_u
                if ref_tgt_schema.upper() != tgt_schema.upper():
                    add_object_grant(tgt_schema, "REFERENCES", f"{ref_tgt_schema}.{ref_tgt_table}")
                    added += 1

        for _ss, _tn, ts, _to, _st, tts, tt in trigger_tasks:
            if tts and tt and ts and tts.upper() != ts.upper():
                required_priv = GRANT_PRIVILEGE_BY_TYPE.get('TABLE', 'SELECT')
                add_object_grant(ts, required_priv, f"{tts}.{tt}")
                added += 1

        return added

    schema_requests: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    unsupported_types: Set[str] = set()
    public_synonym_fallback: Set[Tuple[str, str]] = set()

    def _trigger_allowed(src_full: Optional[str], tgt_full: Optional[str]) -> bool:
        if not trigger_filter_enabled:
            return True
        if not trigger_filter_set:
            return False
        src_u = (src_full or "").upper()
        tgt_u = (tgt_full or "").upper()
        return (src_u in trigger_filter_set) or (tgt_u in trigger_filter_set)

    def queue_request(schema: str, obj_type: str, obj_name: str) -> None:
        obj_type_u = obj_type.upper()
        schema_u = schema.upper()
        obj_name_u = obj_name.upper()
        if obj_type_u == 'VIEW':
            # VIEW 固定走 DBMS_METADATA，不加入 dbcat 导出队列
            return
        if schema_u == 'PUBLIC' and obj_type_u == 'SYNONYM':
            public_synonym_fallback.add((schema_u, obj_name_u))
            return
        if obj_type_u not in DBCAT_OPTION_MAP:
            unsupported_types.add(obj_type_u)
            return
        schema_requests[schema_u][obj_type_u].add(obj_name_u)

    missing_tables: List[Tuple[str, str, str, str]] = []
    other_missing_objects: List[Tuple[str, str, str, str, str]] = []
    missing_total_by_type: Dict[str, int] = defaultdict(int)
    missing_allowed_by_type: Dict[str, int] = defaultdict(int)

    for (obj_type, tgt_name, src_name) in tv_results.get('missing', []):
        obj_type_u = obj_type.upper()
        if '.' not in src_name or '.' not in tgt_name:
            continue
        src_schema, src_obj = src_name.split('.', 1)
        tgt_schema, tgt_obj = tgt_name.split('.', 1)
        if obj_type_u == 'SYNONYM':
            src_schema_u = src_schema.upper()
            src_obj_u = src_obj.upper()
            src_key = (src_schema_u, src_obj_u)
            src_full = f"{src_schema_u}.{src_obj_u}"
            syn_meta = synonym_meta_map.get(src_key)
            if syn_meta and allowed_synonym_targets and syn_meta.table_owner:
                table_owner_u = syn_meta.table_owner.upper()
                if table_owner_u not in allowed_synonym_targets and src_full not in remap_rules:
                    log.info(
                        "[FIXUP] 跳过同义词 %s.%s（table_owner=%s 不在 source_schemas 范围内）。",
                        src_schema, src_obj, table_owner_u
                    )
                    continue
            if src_schema_u == 'PUBLIC' and not syn_meta and src_full not in remap_rules:
                log.info(
                    "[FIXUP] 跳过 PUBLIC 同义词 %s.%s（table_owner 不在 source_schemas 范围内）。",
                    src_schema, src_obj
                )
                continue
        missing_total_by_type[obj_type_u] += 1
        if not allow_fixup(obj_type_u, tgt_schema, src_schema):
            continue
        missing_allowed_by_type[obj_type_u] += 1
        queue_request(src_schema, obj_type_u, src_obj)
        if obj_type_u == 'TABLE':
            missing_tables.append((src_schema, src_obj, tgt_schema, tgt_obj))
        else:
            other_missing_objects.append((obj_type_u, src_schema, src_obj, tgt_schema, tgt_obj))

    if fixup_schema_filter or fixup_type_filter:
        filtered = []
        for obj_type_u in sorted(missing_total_by_type.keys()):
            total = missing_total_by_type[obj_type_u]
            allowed = missing_allowed_by_type.get(obj_type_u, 0)
            if allowed != total:
                filtered.append(f"{obj_type_u}={allowed}/{total}")
        if filtered:
            log.info("[FIXUP] fixup_types/fixup_schemas 生效: %s", ", ".join(filtered))

    if package_results:
        for row in package_results.get("rows", []):
            if row.result != "MISSING_TARGET":
                continue
            if row.src_status == "INVALID":
                log.info(
                    "[FIXUP] 跳过源端 INVALID 的 %s %s (不生成 DDL)。",
                    row.obj_type, row.src_full
                )
                continue
            if "." not in row.src_full or "." not in row.tgt_full:
                continue
            src_schema, src_obj = row.src_full.split(".", 1)
            tgt_schema, tgt_obj = row.tgt_full.split(".", 1)
            obj_type_u = row.obj_type.upper()
            if not allow_fixup(obj_type_u, tgt_schema, src_schema):
                continue
            queue_request(src_schema, obj_type_u, src_obj)
            other_missing_objects.append((obj_type_u, src_schema, src_obj, tgt_schema, tgt_obj))

    sequence_tasks: List[Tuple[str, str, str, str]] = []
    for seq_mis in extra_results.get('sequence_mismatched', []):
        src_schema = seq_mis.src_schema.upper()
        for seq_name in sorted(seq_mis.missing_sequences):
            seq_name_u = seq_name.upper()
            src_full = f"{src_schema}.{seq_name_u}"
            mapped = get_mapped_target(full_object_mapping, src_full, 'SEQUENCE')
            if mapped and '.' in mapped:
                tgt_schema, tgt_name = mapped.split('.')
            else:
                tgt_schema = seq_mis.tgt_schema.upper()
                tgt_name = seq_name_u
            if not allow_fixup('SEQUENCE', tgt_schema, src_schema):
                continue
            queue_request(src_schema, 'SEQUENCE', seq_name_u)
            sequence_tasks.append((src_schema, seq_name_u, tgt_schema, tgt_name))

    index_tasks: List[Tuple[IndexMismatch, str, str, str, str]] = []
    for item in extra_results.get('index_mismatched', []):
        table_str = item.table.split()[0]
        if '.' not in table_str:
            continue
        src_name = table_map.get(table_str)
        if not src_name or '.' not in src_name:
            continue
        src_schema, src_table = src_name.split('.')
        tgt_schema, tgt_table = table_str.split('.')
        if not allow_fixup('INDEX', tgt_schema, src_schema):
            continue
        queue_request(src_schema, 'TABLE', src_table)
        index_tasks.append((item, src_schema, src_table, tgt_schema.upper(), tgt_table.upper()))

    constraint_tasks: List[Tuple[ConstraintMismatch, str, str, str, str]] = []
    for item in extra_results.get('constraint_mismatched', []):
        table_str = item.table.split()[0]
        if '.' not in table_str:
            continue
        src_name = table_map.get(table_str)
        if not src_name or '.' not in src_name:
            continue
        src_schema, src_table = src_name.split('.')
        tgt_schema, tgt_table = table_str.split('.')
        if not allow_fixup('CONSTRAINT', tgt_schema, src_schema):
            continue
        queue_request(src_schema, 'TABLE', src_table)
        constraint_tasks.append((item, src_schema, src_table, tgt_schema.upper(), tgt_table.upper()))

    trigger_tasks: List[Tuple[str, str, str, str, str, str, str]] = []
    for item in extra_results.get('trigger_mismatched', []):
        table_str = item.table.split()[0]
        if '.' not in table_str:
            continue
        src_name = table_map.get(table_str)
        if not src_name or '.' not in src_name:
            continue
        src_schema, src_table = src_name.split('.', 1)
        tgt_schema, tgt_table = table_str.split('.', 1)
        # 优先使用缺失映射对（源->目标），确保 dbcat 按源名导出
        if item.missing_mappings:
            for src_full, tgt_full in item.missing_mappings:
                if '.' not in src_full or '.' not in tgt_full:
                    continue
                src_schema_u, src_trg = src_full.split('.', 1)
                tgt_schema_final, tgt_obj = tgt_full.split('.', 1)
                if not _trigger_allowed(src_full, tgt_full):
                    continue
                if not allow_fixup('TRIGGER', tgt_schema_final, src_schema_u):
                    continue
                queue_request(src_schema_u, 'TRIGGER', src_trg)
                trigger_tasks.append((src_schema_u, src_trg, tgt_schema_final, tgt_obj, src_table, tgt_schema, tgt_table))
        else:
            for trg_name in sorted(item.missing_triggers):
                trg_name_u = trg_name.upper()
                src_full = f"{src_schema.upper()}.{trg_name_u}"
                mapped = get_mapped_target(full_object_mapping, src_full, 'TRIGGER')
                if mapped and '.' in mapped:
                    tgt_schema_final, tgt_obj = mapped.split('.')
                else:
                    tgt_schema_final = tgt_schema.upper()
                    tgt_obj = trg_name_u
                tgt_full = f"{tgt_schema_final}.{tgt_obj}"
                if not _trigger_allowed(src_full, tgt_full):
                    continue
                if not allow_fixup('TRIGGER', tgt_schema_final, src_schema):
                    continue
                queue_request(src_schema, 'TRIGGER', trg_name_u)
                trigger_tasks.append((src_schema, trg_name_u, tgt_schema_final, tgt_obj, src_table, tgt_schema, tgt_table))

    other_missing_summary: Dict[str, int] = defaultdict(int)
    for ot, _, _, _, _ in other_missing_objects:
        other_missing_summary[ot] += 1
    index_total = sum(len(item.missing_indexes) for item, _, _, _, _ in index_tasks)
    constraint_total = sum(len(item.missing_constraints) for item, _, _, _, _ in constraint_tasks)
    total_missing_scripts = (
        len(sequence_tasks)
        + len(missing_tables)
        + len(other_missing_objects)
        + index_total
        + constraint_total
        + len(trigger_tasks)
    )
    other_summary = ", ".join(f"{k}={v}" for k, v in sorted(other_missing_summary.items())) or "无"
    log.info(
        "[FIXUP] 待生成缺失对象/脚本总计 %d 个: TABLE=%d, 其他=%d (%s), INDEX=%d, CONSTRAINT=%d, SEQUENCE=%d, TRIGGER=%d",
        total_missing_scripts,
        len(missing_tables),
        len(other_missing_objects),
        other_summary,
        index_total,
        constraint_total,
        len(sequence_tasks),
        len(trigger_tasks)
    )
    log.info("[FIXUP] 进度日志间隔 %.0f 秒，可通过 progress_log_interval 配置。", progress_log_interval)
    if public_synonym_fallback:
        log.info(
            "[FIXUP] 检测到 %d 个 PUBLIC 同义词，将跳过 dbcat，优先使用同义词元数据；缺失时再用 DBMS_METADATA 兜底。",
            len(public_synonym_fallback)
        )

    ob_grant_catalog: Optional[ObGrantCatalog] = None
    object_grants_missing_by_grantee: Dict[str, Set[ObjectGrantEntry]] = {}
    sys_privs_missing_by_grantee: Dict[str, Set[SystemGrantEntry]] = {}
    role_privs_missing_by_grantee: Dict[str, Set[RoleGrantEntry]] = {}

    if grant_enabled:
        pre_added = pre_add_cross_schema_grants(constraint_tasks, trigger_tasks)
        if pre_added:
            log.info("[GRANT] 预追加跨 schema 授权 %d 条 (FK/Trigger)。", pre_added)
        expected_grantees = set(object_grants_by_grantee.keys())
        expected_grantees.update(sys_privs_by_grantee.keys())
        expected_grantees.update(role_privs_by_grantee.keys())
        ob_grant_catalog = load_ob_grant_catalog(ob_cfg, expected_grantees)
        if ob_grant_catalog is None:
            log.warning("[GRANT_MISS] OB 权限读取失败，缺失授权将退化为全量输出。")
        (
            object_grants_missing_by_grantee,
            sys_privs_missing_by_grantee,
            role_privs_missing_by_grantee
        ) = filter_missing_grant_entries(
            object_grants_by_grantee,
            sys_privs_by_grantee,
            role_privs_by_grantee,
            ob_grant_catalog
        )
        if object_grants_by_grantee:
            raw_count, merged_count, object_grant_lookup, grants_by_owner = build_object_grant_statements_for(
                object_grants_by_grantee
            )
            log.info(
                "[GRANT] 对象授权合并: 原始=%d, 合并后=%d, merge_privileges=%s, merge_grantees=%s",
                raw_count,
                merged_count,
                "true" if merge_privileges else "false",
                "true" if merge_grantees else "false"
            )

    # 分离VIEW对象和其他对象（需要在使用前定义）
    view_missing_objects: List[Tuple[str, str, str, str]] = []
    non_view_missing_objects: List[Tuple[str, str, str, str, str]] = []
    
    for (obj_type, src_schema, src_obj, tgt_schema, tgt_obj) in other_missing_objects:
        if obj_type.upper() == 'VIEW':
            view_missing_objects.append((src_schema, src_obj, tgt_schema, tgt_obj))
        else:
            non_view_missing_objects.append((obj_type, src_schema, src_obj, tgt_schema, tgt_obj))

    if report_dir and report_timestamp and view_missing_objects:
        view_targets = [f"{tgt_schema}.{tgt_obj}" for _, _, tgt_schema, tgt_obj in view_missing_objects]
        dep_pairs = expected_dependency_pairs or set()
        if dep_pairs:
            view_chain_path = Path(report_dir) / f"VIEWs_chain_{report_timestamp}.txt"
            view_chain_file = export_view_fixup_chains(
                view_targets,
                dep_pairs,
                view_chain_path,
                full_object_mapping,
                remap_rules,
                synonym_meta=synonym_meta_map,
                ob_meta=ob_meta,
                ob_grant_catalog=ob_grant_catalog
            )
            if view_chain_file:
                log.info("VIEW fixup 依赖链已输出: %s", view_chain_file)
            else:
                log.info("VIEW fixup 依赖链输出已跳过（无链路或写入失败）。")

    dbcat_data, ddl_source_meta = fetch_dbcat_schema_objects(ora_cfg, settings, schema_requests)

    # 预取所有可能需要 fallback 的 DDL（dbcat 未命中的对象）
    fallback_ddl_cache: Dict[Tuple[str, str, str], str] = {}
    
    # 收集需要 fallback 的对象（dbcat 缺失时由 DBMS_METADATA 兜底）
    fallback_needed: List[Tuple[str, str, str]] = []
    for obj_type, src_schema, src_obj, tgt_schema, tgt_obj in non_view_missing_objects:
        # 检查 dbcat_data 中是否有
        if not dbcat_data.get(src_schema.upper(), {}).get(obj_type.upper(), {}).get(src_obj.upper()):
            fallback_needed.append((src_schema, obj_type, src_obj))

    # 视图任务（VIEW 固定使用 DBMS_METADATA）
    for src_schema, src_obj, _, _ in view_missing_objects:
        fallback_needed.append((src_schema, 'VIEW', src_obj))
    
    # 序列任务
    for src_schema, src_seq, tgt_schema, tgt_seq in sequence_tasks:
        if not dbcat_data.get(src_schema.upper(), {}).get('SEQUENCE', {}).get(src_seq.upper()):
            fallback_needed.append((src_schema, 'SEQUENCE', src_seq))
    
    # 触发器任务
    for src_schema, trg_name, tgt_schema, tgt_obj, _src_table, _tgt_schema, _tgt_table in trigger_tasks:
        if not dbcat_data.get(src_schema.upper(), {}).get('TRIGGER', {}).get(trg_name.upper()):
            fallback_needed.append((src_schema, 'TRIGGER', trg_name))
    
    # 批量预取
    if fallback_needed:
        log.info("[FIXUP] 预取 %d 个可能需要 DBMS_METADATA 兜底的对象...", len(fallback_needed))
        fallback_ddl_cache = oracle_get_ddl_batch(ora_cfg, fallback_needed)

    oracle_conn = None
    oracle_conn_lock = threading.Lock()

    def _is_not_found_error(exc: Exception) -> bool:
        msg = str(exc).upper()
        return any(code in msg for code in ("ORA-31603", "ORA-04043", "ORA-00942", "ORA-06512"))

    def get_fallback_ddl(schema: str, obj_type: str, obj_name: str) -> Optional[str]:
        """当 dbcat 缺失 DDL 时尝试使用 DBMS_METADATA 兜底，优先使用预取缓存。"""
        nonlocal oracle_conn
        allowed_types = BATCH_DDL_ALLOWED_TYPES
        if obj_type.upper() not in allowed_types:
            return None
        
        # 优先使用预取缓存
        cache_key = (schema.upper(), obj_type.upper(), obj_name.upper())
        if cache_key in fallback_ddl_cache:
            return fallback_ddl_cache[cache_key]
        
        # 缓存未命中，单独获取（兜底）
        with oracle_conn_lock:
            try:
                if oracle_conn is None:
                    oracle_conn = oracledb.connect(
                        user=ora_cfg['user'],
                        password=ora_cfg['password'],
                        dsn=ora_cfg['dsn']
                    )
                    setup_metadata_session(oracle_conn)
                return oracle_get_ddl(oracle_conn, obj_type, schema, obj_name)
            except Exception as exc:
                if _is_not_found_error(exc):
                    log.info("[DDL] DBMS_METADATA 未找到 %s.%s (%s)，跳过兜底。", schema, obj_name, obj_type)
                    return None
                log.warning("[DDL] DBMS_METADATA 获取 %s.%s (%s) 失败: %s", schema, obj_name, obj_type, exc)
                return None

    def _is_dbcat_unsupported_table(ddl: str) -> bool:
        """判断 dbcat 抓取的 TABLE DDL 是否包含 unsupported 提示。"""
        if not ddl:
            return False
        return "UNSUPPORTED" in ddl.upper()

    table_ddl_cache: Dict[Tuple[str, str], str] = {}
    for schema, type_map in dbcat_data.items():
        for table_name, ddl in type_map.get('TABLE', {}).items():
            table_ddl_cache[(schema, table_name)] = ddl

    def build_index_from_meta(
        src_schema: str,
        src_table: str,
        tgt_schema: str,
        tgt_table: str,
        idx_name: str
    ) -> Optional[str]:
        meta = oracle_meta.indexes.get((src_schema.upper(), src_table.upper()), {}).get(idx_name.upper())
        if not meta:
            return None
        cols = meta.get("columns") or []
        if not cols:
            return None
        uniq = (meta.get("uniqueness") or "").upper() == "UNIQUE"
        col_list = ", ".join(cols)
        prefix = "UNIQUE " if uniq else ""
        return f"CREATE {prefix}INDEX {tgt_schema.upper()}.{idx_name.upper()} ON {tgt_schema.upper()}.{tgt_table.upper()} ({col_list});"

    worker_count = max(1, int(settings.get('fixup_workers', 1)))
    if worker_count == 1:
        log.info("[FIXUP] 并发未启用 (worker=1)，可通过 fixup_workers 配置提高到 8 或 12。")
    else:
        log.info("[FIXUP] 启用并发生成 (worker=%d)，可通过 fixup_workers 配置调整。", worker_count)

    def run_tasks(tasks: List[Callable[[], None]], label: str) -> None:
        if not tasks:
            return
        if worker_count <= 1:
            for task in tasks:
                try:
                    task()
                except Exception as exc:
                    log.error("[FIXUP] 任务 %s 失败: %s", label, exc)
            return

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(task) for task in tasks]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    log.error("[FIXUP] 任务 %s 失败: %s", label, exc)

    log.info("[FIXUP] (1/9) 正在生成 SEQUENCE 脚本...")
    seq_progress = build_progress_tracker(len(sequence_tasks), "[FIXUP] (1/9) SEQUENCE")
    seq_jobs: List[Callable[[], None]] = []
    for src_schema, seq_name, tgt_schema, tgt_name in sequence_tasks:
        def _job(ss=src_schema, sn=seq_name, ts=tgt_schema, tn=tgt_name):
            try:
                fetch_result = fetch_ddl_with_timing(ss, 'SEQUENCE', sn)
                if len(fetch_result) != 3:
                    log.error("[FIXUP] SEQUENCE fetch_ddl_with_timing 返回了 %d 个值，期望 3 个: %s", len(fetch_result), fetch_result)
                    return
                ddl, ddl_source_label, _elapsed = fetch_result
                if not ddl:
                    log.warning("[FIXUP] 未找到 SEQUENCE %s.%s 的 dbcat DDL。", ss, sn)
                    mark_source('SEQUENCE', 'missing')
                    return
                if ddl_source_label.startswith("DBCAT"):
                    mark_source('SEQUENCE', 'dbcat')
                elif ddl_source_label == "DBMS_METADATA":
                    mark_source('SEQUENCE', 'fallback')
                else:
                    mark_source('SEQUENCE', 'missing')
                ddl_adj = adjust_ddl_for_object(
                    ddl,
                    ss,
                    sn,
                    ts,
                    tn,
                    extra_identifiers=get_relevant_replacements(ss),
                    obj_type='SEQUENCE'
                )
                ddl_adj = cleanup_dbcat_wrappers(ddl_adj)
                ddl_adj = prepend_set_schema(ddl_adj, ts)
                ddl_adj = normalize_ddl_for_ob(ddl_adj)
                ddl_adj = apply_ddl_cleanup_rules(ddl_adj, 'SEQUENCE')
                ddl_adj = strip_constraint_enable(ddl_adj)
                filename = f"{ts}.{tn}.sql"
                header = f"修补缺失的 SEQUENCE {ts}.{tn} (源: {ss}.{sn})"
                grants_for_seq = collect_grants_for_object(f"{ts}.{tn}")
                log.info("[FIXUP]%s 写入 SEQUENCE 脚本: %s", source_tag(ddl_source_label), filename)
                write_fixup_file(
                    base_dir,
                    'sequence',
                    filename,
                    ddl_adj,
                    header,
                    grants_to_add=grants_for_seq if grants_for_seq else None
                )
            finally:
                seq_progress()
        seq_jobs.append(_job)
    run_tasks(seq_jobs, "SEQUENCE")

    log.info("[FIXUP] (2/9) 正在生成缺失的 TABLE CREATE 脚本...")
    table_jobs: List[Callable[[], None]] = []
    table_total = len(missing_tables)
    table_progress_tick = build_progress_tracker(table_total, "[FIXUP] (2/9) TABLE")
    table_counts: Dict[str, int] = defaultdict(int)
    table_progress = {"done": 0}
    table_lock = threading.Lock()

    def _record_table_progress(source_label: str) -> None:
        with table_lock:
            table_counts[source_label] += 1
            table_progress["done"] += 1
            stats = ", ".join(f"{k}={v}" for k, v in sorted(table_counts.items()))
        table_progress_tick(stats or "no scripts")

    for src_schema, src_table, tgt_schema, tgt_table in missing_tables:
        def _job(ss=src_schema, st=src_table, ts=tgt_schema, tt=tgt_table):
            progress_label = "unknown"
            try:
                fetch_result = fetch_ddl_with_timing(ss, 'TABLE', st)
                if len(fetch_result) != 3:
                    log.error("[FIXUP] TABLE fetch_ddl_with_timing 返回了 %d 个值，期望 3 个: %s", len(fetch_result), fetch_result)
                    return
                ddl, ddl_source_label, _elapsed = fetch_result
                if not ddl:
                    log.warning("[FIXUP] 未找到 TABLE %s.%s 的 dbcat DDL。", ss, st)
                    mark_source('TABLE', 'missing')
                    progress_label = "missing"
                    return
                # 如果 dbcat 返回 unsupported，尝试 DBMS_METADATA 兜底，直接暴露给用户
                if _is_dbcat_unsupported_table(ddl):
                    fallback_ddl = get_fallback_ddl(ss, 'TABLE', st)
                    if fallback_ddl:
                        ddl = fallback_ddl
                        ddl_source_label = "DBMS_METADATA"
                        mark_source('TABLE', 'fallback')
                        log.info("[FIXUP] TABLE %s.%s 的 dbcat DDL 为 unsupported，已使用 DBMS_METADATA 兜底。", ss, st)
                    else:
                        log.warning("[FIXUP] TABLE %s.%s 的 dbcat DDL 为 unsupported，DBMS_METADATA 兜底失败，仍输出原始 DDL 供人工处理。", ss, st)
                if ddl_source_label.startswith("DBCAT"):
                    mark_source('TABLE', 'dbcat')
                elif ddl_source_label == "DBMS_METADATA":
                    mark_source('TABLE', 'fallback')
                else:
                    mark_source('TABLE', 'missing')
                ddl_adj = adjust_ddl_for_object(
                    ddl,
                    ss,
                    st,
                    ts,
                    tt,
                    extra_identifiers=get_relevant_replacements(ss),
                    obj_type='TABLE'
                )
                ddl_adj = inflate_table_varchar_lengths(ddl_adj, ss, st, oracle_meta)
                ddl_adj = cleanup_dbcat_wrappers(ddl_adj)
                ddl_adj = prepend_set_schema(ddl_adj, ts)
                ddl_adj = normalize_ddl_for_ob(ddl_adj)
                ddl_adj = apply_ddl_cleanup_rules(ddl_adj, 'TABLE')
                ddl_adj = strip_constraint_enable(ddl_adj)
                ddl_adj = strip_enable_novalidate(ddl_adj)
                filename = f"{ts}.{tt}.sql"
                header = f"修补缺失的 TABLE {ts}.{tt} (源: {ss}.{st})"
                grants_for_table = collect_grants_for_object(f"{ts}.{tt}")
                log.info("[FIXUP]%s 写入 TABLE 脚本: %s", source_tag(ddl_source_label), filename)
                write_fixup_file(
                    base_dir,
                    'table',
                    filename,
                    ddl_adj,
                    header,
                    grants_to_add=grants_for_table if grants_for_table else None
                )
                progress_label = ddl_source_label.lower()
            finally:
                _record_table_progress(progress_label)
        table_jobs.append(_job)
    run_tasks(table_jobs, "TABLE")

    log.info("[FIXUP] (3/9) 正在生成 TABLE ALTER 脚本...")
    for (obj_type, tgt_name, missing_cols, extra_cols, length_mismatches, type_mismatches) in tv_results.get('mismatched', []):
        if obj_type.upper() != 'TABLE' or "获取失败" in tgt_name:
            continue
        src_name = table_map.get(tgt_name)
        if not src_name:
            continue
        src_schema, src_table = src_name.split('.')
        tgt_schema, tgt_table = tgt_name.split('.')
        alter_sql = generate_alter_for_table_columns(
            oracle_meta,
            src_schema,
            src_table,
            tgt_schema,
            tgt_table,
            missing_cols,
            extra_cols,
            length_mismatches,
            type_mismatches
        )
        if alter_sql:
            alter_sql = prepend_set_schema(alter_sql, tgt_schema)
            filename = f"{tgt_schema}.{tgt_table}.alter_columns.sql"
            header = f"基于列差异的 ALTER TABLE 订正 SQL: {tgt_schema}.{tgt_table} (源: {src_schema}.{src_table})"
            write_fixup_file(base_dir, 'table_alter', filename, alter_sql, header)

    # 获取OceanBase版本
    ob_version = get_oceanbase_version(ob_cfg)
    if ob_version:
        log.info("[VIEW] 检测到OceanBase版本: %s", ob_version)
    else:
        log.warning("[VIEW] 无法获取OceanBase版本，将使用保守的DDL清理策略")

    log.info("[FIXUP] (4/9) 正在生成 VIEW / MATERIALIZED VIEW / 其他对象脚本...")
    
    # 处理VIEW对象 - 使用简化的拓扑排序
    if view_missing_objects:
        log.info("[FIXUP] (4a/9) 正在排序 %d 个VIEW依赖关系...", len(view_missing_objects))
        
        # Simple topological sort using already-fetched DDLs
        try:
            # Step 1: Fetch all VIEW DDLs first
            view_ddl_map = {}  # (src_schema, src_obj) -> DDL
            for src_schema, src_obj, tgt_schema, tgt_obj in view_missing_objects:
                fetch_result = fetch_ddl_with_timing(src_schema, 'VIEW', src_obj)
                if len(fetch_result) == 3:
                    raw_ddl, _, _ = fetch_result
                    if raw_ddl:
                        view_ddl_map[(src_schema, src_obj)] = raw_ddl
            
            # Step 2: Build dependency graph
            view_deps = {}  # (tgt_schema, tgt_obj) -> set of (tgt_schema, tgt_obj) dependencies
            src_to_tgt = {(s_sch, s_obj): (t_sch, t_obj) 
                         for s_sch, s_obj, t_sch, t_obj in view_missing_objects}
            tgtfull_to_tuple = {f"{t_sch}.{t_obj}".upper(): (t_sch, t_obj)
                               for _, _, t_sch, t_obj in view_missing_objects}
            
            for src_schema, src_obj, tgt_schema, tgt_obj in view_missing_objects:
                tgt_key = (tgt_schema, tgt_obj)
                view_deps[tgt_key] = set()
                
                ddl = view_ddl_map.get((src_schema, src_obj), "")
                if ddl:
                    # Extract dependencies
                    try:
                        dependencies = extract_view_dependencies(ddl, src_schema)
                        for dep in dependencies:
                            dep_upper = dep.upper()
                            # Check if this dependency is another VIEW in our list
                            for (s_sch_d, s_obj_d), (t_sch_d, t_obj_d) in src_to_tgt.items():
                                if f"{s_sch_d}.{s_obj_d}".upper() == dep_upper:
                                    view_deps[tgt_key].add((t_sch_d, t_obj_d))
                                    break
                    except Exception as e:
                        log.debug(f"[FIXUP] 提取 VIEW {src_schema}.{src_obj} 依赖失败: {e}")
            
            # Step 3: Topological sort using Kahn's algorithm
            from collections import deque
            in_degree = defaultdict(int)
            dep_graph = defaultdict(set)  # dependency -> [dependents]
            
            for view_key, deps in view_deps.items():
                if view_key not in in_degree:
                    in_degree[view_key] = 0
                for dep_key in deps:
                    in_degree[view_key] += 1
                    dep_graph[dep_key].add(view_key)
            
            queue = deque([v for v in view_deps.keys() if in_degree[v] == 0])
            sorted_view_tuples = []
            
            while queue:
                current = queue.popleft()
                sorted_view_tuples.append(current)
                for dependent in dep_graph.get(current, set()):
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)
            
            # Check for cycles
            if len(sorted_view_tuples) < len(view_deps):
                circular = [v for v, d in in_degree.items() if d > 0]
                log.warning(f"[FIXUP] 发现 {len(circular)} 个循环依赖的VIEW，将最后创建")
                sorted_view_tuples.extend(circular)
            
            # Map back to original format
            tgt_to_orig = {(t_sch, t_obj): (s_sch, s_obj, t_sch, t_obj)
                          for s_sch, s_obj, t_sch, t_obj in view_missing_objects}
            view_missing_objects = [tgt_to_orig[v] for v in sorted_view_tuples if v in tgt_to_orig]
            
            log.info("[FIXUP] VIEW拓扑排序完成：依赖对象将优先创建")
            
        except Exception as e:
            log.warning(f"[FIXUP] VIEW拓扑排序失败: {e}, 使用原始顺序")
        
        log.info("[FIXUP] 正在生成 %d 个VIEW脚本...", len(view_missing_objects))
        for src_schema, src_obj, tgt_schema, tgt_obj in view_missing_objects:
            try:
                fetch_result = fetch_ddl_with_timing(src_schema, 'VIEW', src_obj)
                if len(fetch_result) != 3:
                    log.error("[FIXUP] VIEW fetch_ddl_with_timing 返回了 %d 个值，期望 3 个: %s", len(fetch_result), fetch_result)
                    continue
                raw_ddl, ddl_source_label, _elapsed = fetch_result
                if not raw_ddl:
                    log.warning("[FIXUP] 未找到 VIEW %s.%s 的 DDL。", src_schema, src_obj)
                    mark_source('VIEW', 'missing')
                    continue
                if ddl_source_label.startswith("DBCAT"):
                    mark_source('VIEW', 'dbcat')
                elif ddl_source_label == "DBMS_METADATA":
                    mark_source('VIEW', 'fallback')
                else:
                    mark_source('VIEW', 'missing')
                
                # 清理DDL使其兼容OceanBase
                cleaned_ddl = clean_view_ddl_for_oceanbase(raw_ddl, ob_version)

                # 修复 dbcat DDL 的注释/列名异常
                col_meta = oracle_meta.table_columns.get((src_schema.upper(), src_obj.upper()), {}) or {}
                cleaned_ddl = sanitize_view_ddl(cleaned_ddl, set(col_meta.keys()))
                
                # 重写依赖对象引用
                remapped_ddl = remap_view_dependencies(
                    cleaned_ddl,
                    src_schema,
                    remap_rules,
                    full_object_mapping
                )
                
                # 调整schema和对象名
                final_ddl = adjust_ddl_for_object(
                    remapped_ddl,
                    src_schema,
                    src_obj,
                    tgt_schema,
                    tgt_obj,
                    extra_identifiers=get_relevant_replacements(src_schema),
                    obj_type='VIEW'
                )
                
                # 最终清理
                final_ddl = cleanup_dbcat_wrappers(final_ddl)
                final_ddl = prepend_set_schema(final_ddl, tgt_schema)
                final_ddl = normalize_ddl_for_ob(final_ddl)
                final_ddl = apply_ddl_cleanup_rules(final_ddl, 'VIEW')
                final_ddl = strip_constraint_enable(final_ddl)
                final_ddl = enforce_schema_for_ddl(final_ddl, tgt_schema, 'VIEW')
                
                # 确保DDL以分号结尾
                if not final_ddl.rstrip().endswith(';'):
                    final_ddl = final_ddl.rstrip() + ';'
                
                # 写入文件
                filename = f"{tgt_schema}.{tgt_obj}.sql"
                header = f"修补缺失的 VIEW {tgt_obj} (源: {src_schema}.{src_obj})"
                grants_for_view = collect_grants_for_object(f"{tgt_schema}.{tgt_obj}")
                log.info("[FIXUP]%s 写入 VIEW 脚本: %s", source_tag(ddl_source_label), filename)
                write_fixup_file(
                    base_dir,
                    'view',
                    filename,
                    final_ddl,
                    header,
                    grants_to_add=grants_for_view if grants_for_view else None
                )
                
            except Exception as exc:
                log.error("[FIXUP] 处理 VIEW %s.%s 时出错: %s", src_schema, src_obj, exc)

    # 处理非VIEW对象
    other_progress = build_progress_tracker(len(non_view_missing_objects), "[FIXUP] (4b/9) 其他对象")
    other_jobs: List[Callable[[], None]] = []
    for (obj_type, src_schema, src_obj, tgt_schema, tgt_obj) in non_view_missing_objects:
        def _job(ot=obj_type, ss=src_schema, so=src_obj, ts=tgt_schema, to=tgt_obj):
            try:
                fetch_result = fetch_ddl_with_timing(ss, ot, so)
                if len(fetch_result) != 3:
                    log.error("[FIXUP] fetch_ddl_with_timing 返回了 %d 个值，期望 3 个: %s", len(fetch_result), fetch_result)
                    return
                ddl, ddl_source_label, _elapsed = fetch_result
                if not ddl:
                    log.warning("[FIXUP] 未找到 %s %s.%s 的 dbcat DDL。", ot, ss, so)
                    mark_source(ot, 'missing')
                    return
                if ddl_source_label.startswith("DBCAT"):
                    mark_source(ot, 'dbcat')
                elif ddl_source_label == "DBMS_METADATA":
                    mark_source(ot, 'fallback')
                else:
                    mark_source(ot, 'missing')
                ddl_adj = adjust_ddl_for_object(
                    ddl,
                    ss,
                    so,
                    ts,
                    to,
                    extra_identifiers=get_relevant_replacements(ss),
                    obj_type=ot
                )
                if ot.upper() == 'SYNONYM':
                    ddl_adj = remap_synonym_target(
                        ddl_adj,
                        remap_rules,
                        full_object_mapping
                    )
                    if ts.upper() == 'PUBLIC':
                        ddl_adj = normalize_public_synonym_name(ddl_adj, to)
                # 重映射PL/SQL对象中的对象引用
                if ot.upper() in ['PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY', 'TYPE', 'TYPE BODY']:
                    ddl_adj = remap_plsql_object_references(ddl_adj, ot, full_object_mapping, source_schema=ss)
                ddl_adj = cleanup_dbcat_wrappers(ddl_adj)
                
                # PUBLIC SYNONYM 不需要 ALTER SESSION SET CURRENT_SCHEMA = PUBLIC（多余）
                # 但用户schema的SYNONYM需要ALTER SESSION来确保对象在正确的schema创建
                if not (ot.upper() == 'SYNONYM' and ts.upper() == 'PUBLIC'):
                    ddl_adj = prepend_set_schema(ddl_adj, ts)
                
                ddl_adj = normalize_ddl_for_ob(ddl_adj)
                ddl_adj = apply_ddl_cleanup_rules(ddl_adj, ot)
                clean_notes = None
                if settings.get('enable_ddl_punct_sanitize', True):
                    ddl_adj, replaced, samples = sanitize_plsql_punctuation(ddl_adj, ot)
                    if replaced:
                        obj_full = f"{ts}.{to}"
                        sample_text = ", ".join(f"{src}->{dst}" for src, dst in samples)
                        suffix = f" 示例: {sample_text}" if sample_text else ""
                        log.info(
                            "[DDL_CLEAN] %s %s 全角标点清洗 %d 处。%s",
                            ot,
                            obj_full,
                            replaced,
                            suffix
                        )
                        record_ddl_clean(ot, obj_full, replaced, samples)
                        clean_notes = [f"DDL_CLEAN: 全角标点清洗 {replaced} 处。{suffix}"]
                ddl_adj = strip_constraint_enable(ddl_adj)
                ddl_adj = enforce_schema_for_ddl(ddl_adj, ts, ot)
                
                # --- Find and prepare grants for this object ---
                grants_for_this_object = collect_grants_for_object(f"{ts}.{to}")

                subdir = obj_type_to_dir.get(ot, ot.lower())
                filename = f"{ts}.{to}.sql"
                header = f"修补缺失的 {ot} {ts}.{to} (源: {ss}.{so})"
                log.info("[FIXUP]%s 写入 %s 脚本: %s", source_tag(ddl_source_label), ot, filename)
                write_fixup_file(
                    base_dir,
                    subdir,
                    filename,
                    ddl_adj,
                    header,
                    grants_to_add=grants_for_this_object,
                    extra_comments=clean_notes
                )
            finally:
                other_progress()
        other_jobs.append(_job)
    run_tasks(other_jobs, "OTHER_OBJECTS")

    log.info("[FIXUP] (5/9) 正在生成 INDEX 脚本...")
    index_progress = build_progress_tracker(index_total, "[FIXUP] (5/9) INDEX")
    index_jobs: List[Callable[[], None]] = []
    for item, src_schema, src_table, tgt_schema, tgt_table in index_tasks:
        def _job(it=item, ss=src_schema, st=src_table, ts=tgt_schema, tt=tgt_table):
            table_ddl = table_ddl_cache.get((ss.upper(), st.upper()))
            if not table_ddl:
                log.warning("[FIXUP] 未找到 TABLE %s.%s 的 dbcat DDL，将尝试基于元数据重建索引。", ss, st)

            def index_predicate(stmt_upper: str) -> bool:
                return 'CREATE' in stmt_upper and ' INDEX ' in stmt_upper

            extracted = extract_statements_for_names(table_ddl, it.missing_indexes, index_predicate) if table_ddl else {}
            for idx_name in sorted(it.missing_indexes):
                idx_name_u = idx_name.upper()
                try:
                    statements = extracted.get(idx_name_u) or []
                    source_label = "DBCAT" if table_ddl else "META"
                    if not statements:
                        fallback_stmt = build_index_from_meta(ss, st, ts, tt, idx_name_u)
                        if fallback_stmt:
                            statements = [fallback_stmt]
                            log.info("[FIXUP][META] 使用元数据重建索引 %s.%s。", ts, idx_name_u)
                            source_label = "META"
                            mark_source('INDEX', 'fallback')
                        else:
                            mark_source('INDEX', 'missing')
                            log.warning("[FIXUP] 未在 TABLE %s.%s 的 DDL 中找到索引 %s，且无元数据可重建。", ss, st, idx_name_u)
                            continue
                    else:
                        mark_source('INDEX', 'dbcat')
                    ddl_lines: List[str] = []
                    for stmt in statements:
                        ddl_adj = adjust_ddl_for_object(
                            stmt,
                            ss,
                            st,
                            ts,
                            tt,
                            extra_identifiers=get_relevant_replacements(ss),
                            obj_type='INDEX'
                        )
                        ddl_adj = normalize_ddl_for_ob(ddl_adj)
                        ddl_lines.append(ddl_adj if ddl_adj.endswith(';') else ddl_adj + ';')
                    content = prepend_set_schema("\n".join(ddl_lines), ts)
                    filename = f"{ts}.{idx_name_u}.sql"
                    header = f"修补缺失的 INDEX {idx_name_u} (表: {ts}.{tt})"
                    log.info("[FIXUP]%s 写入 INDEX 脚本: %s", source_tag(source_label), filename)
                    write_fixup_file(base_dir, 'index', filename, content, header)
                finally:
                    index_progress()
        index_jobs.append(_job)
    run_tasks(index_jobs, "INDEX")

    log.info("[FIXUP] (6/9) 正在生成 CONSTRAINT 脚本...")
    constraint_progress = build_progress_tracker(constraint_total, "[FIXUP] (6/9) CONSTRAINT")
    constraint_jobs: List[Callable[[], None]] = []
    for item, src_schema, src_table, tgt_schema, tgt_table in constraint_tasks:
        def _job(it=item, ss=src_schema, st=src_table, ts=tgt_schema, tt=tgt_table):
            table_ddl = table_ddl_cache.get((ss.upper(), st.upper()))
            if not table_ddl:
                log.warning("[FIXUP] 未找到 TABLE %s.%s 的 dbcat DDL，将尝试基于元数据重建约束。", ss, st)

            def constraint_predicate(stmt_upper: str) -> bool:
                return 'ALTER TABLE' in stmt_upper and 'CONSTRAINT' in stmt_upper

            extracted = extract_statements_for_names(table_ddl, it.missing_constraints, constraint_predicate) if table_ddl else {}
            for cons_name in sorted(it.missing_constraints):
                cons_name_u = cons_name.upper()
                grants_for_constraint: Set[str] = set()
                try:
                    statements = extracted.get(cons_name_u) or []
                    source_label = "DBCAT" if statements else "META"
                    cons_meta = oracle_meta.constraints.get((ss.upper(), st.upper()), {}).get(cons_name_u)
                    ctype = (cons_meta or {}).get("type", "").upper()
                    cols = cons_meta.get("columns") if cons_meta else []
                    # FK 引用表的 remap 映射，用于替换 DDL 中的 REFERENCES 子句
                    fk_ref_replacements: List[Tuple[Tuple[str, str], Tuple[str, str]]] = []
                    # 针对外键，准备 REFERENCES 授权并收集引用表的 remap 映射
                    if cons_meta and ctype == 'R':
                        ref_owner = cons_meta.get("ref_table_owner") or cons_meta.get("r_owner")
                        ref_table = cons_meta.get("ref_table_name")
                        if ref_owner and ref_table:
                            ref_owner_u = ref_owner.upper()
                            ref_table_u = ref_table.upper()
                            ref_src_full = f"{ref_owner_u}.{ref_table_u}"
                            ref_tgt_full = get_mapped_target(full_object_mapping, ref_src_full, 'TABLE') or ref_src_full
                            # 如果引用表被 remap，添加到替换列表
                            if '.' in ref_tgt_full:
                                ref_tgt_schema, ref_tgt_table = ref_tgt_full.split('.', 1)
                                ref_tgt_schema_u = ref_tgt_schema.upper()
                                ref_tgt_table_u = ref_tgt_table.upper()
                                if (ref_owner_u, ref_table_u) != (ref_tgt_schema_u, ref_tgt_table_u):
                                    fk_ref_replacements.append(
                                        ((ref_owner_u, ref_table_u), (ref_tgt_schema_u, ref_tgt_table_u))
                                    )
                                    log.info(
                                        "[FIXUP] FK 约束 %s 引用表 %s.%s -> %s.%s",
                                        cons_name_u, ref_owner_u, ref_table_u, ref_tgt_schema_u, ref_tgt_table_u
                                    )
                            # 跨 schema 外键需要 REFERENCES 授权
                            if ref_tgt_full.upper().split('.')[0] != ts.upper():
                                add_object_grant(ts, "REFERENCES", ref_tgt_full)
                                if grant_enabled:
                                    grants_for_constraint.add(format_object_grant(
                                        ts,
                                        ObjectGrantEntry("REFERENCES", ref_tgt_full.upper(), False)
                                    ))
                    # Fallback: PK/UK 可能内联在 CREATE TABLE 中，尝试用元数据重建
                    if not statements:
                        cols_join = ", ".join(c for c in cols if c)
                        if cols_join and ctype in ('P', 'U'):
                            add_clause = "PRIMARY KEY" if ctype == 'P' else "UNIQUE"
                            stmt = (
                                f"ALTER TABLE {ts}.{tt} "
                                f"ADD CONSTRAINT {cons_name_u} {add_clause} ({cols_join})"
                            )
                            statements = [stmt]
                            mark_source('CONSTRAINT', 'fallback')
                            source_label = "META"
                        elif cons_meta and ctype == 'R':
                            # 尝试基于元数据重建外键
                            ref_owner = cons_meta.get("ref_table_owner") or cons_meta.get("r_owner")
                            ref_table = cons_meta.get("ref_table_name")
                            ref_cons = cons_meta.get("r_constraint")
                            ref_cols: List[str] = []
                            if ref_owner and ref_table:
                                ref_owner_u = ref_owner.upper()
                                ref_table_u = ref_table.upper()
                                ref_key = (ref_owner_u, ref_table_u)
                                # 优先使用引用的约束列，否则取该表的 PK/UK
                                if ref_cons:
                                    ref_cons_u = ref_cons.upper()
                                    ref_info = oracle_meta.constraints.get(ref_key, {}).get(ref_cons_u, {})
                                    ref_cols = ref_info.get("columns") or []
                                if not ref_cols:
                                    ref_tbl_cons = oracle_meta.constraints.get(ref_key, {})
                                    for cand in ref_tbl_cons.values():
                                        ctype_cand = (cand.get("type") or "").upper()
                                        if ctype_cand in ("P", "U"):
                                            ref_cols = cand.get("columns") or []
                                            if ref_cols:
                                                break
                            if cols and ref_owner and ref_table and ref_cols:
                                ref_tgt_full = get_mapped_target(
                                    full_object_mapping,
                                    f"{ref_owner}.{ref_table}",
                                    'TABLE'
                                ) or f"{ref_owner}.{ref_table}"
                                if '.' in ref_tgt_full:
                                    ref_tgt_schema, ref_tgt_table = ref_tgt_full.split('.', 1)
                                else:
                                    ref_tgt_schema, ref_tgt_table = ref_owner.upper(), ref_table.upper()
                                stmt = (
                                    f"ALTER TABLE {ts}.{tt} "
                                    f"ADD CONSTRAINT {cons_name_u} FOREIGN KEY ({', '.join(cols)}) "
                                    f"REFERENCES {ref_tgt_schema}.{ref_tgt_table} ({', '.join(ref_cols)})"
                                )
                                statements = [stmt]
                                mark_source('CONSTRAINT', 'fallback')
                                source_label = "META"
                                # 跨 schema FK 需要 grant
                                if ref_tgt_schema != ts.upper():
                                    ref_full = f"{ref_tgt_schema}.{ref_tgt_table}"
                                    add_object_grant(ts, "REFERENCES", ref_full)
                                    if grant_enabled:
                                        grants_for_constraint.add(format_object_grant(
                                            ts,
                                            ObjectGrantEntry("REFERENCES", ref_full.upper(), False)
                                        ))
                        elif cons_meta:
                            log.warning(
                                "[FIXUP] 约束 %s 类型为 %s，无内联 DDL 可用，无法自动重建。",
                                cons_name_u, ctype or "UNKNOWN"
                            )
                    else:
                        mark_source('CONSTRAINT', 'dbcat')
                    if not statements:
                        mark_source('CONSTRAINT', 'missing')
                        log.warning("[FIXUP] 未在 TABLE %s.%s 的 DDL 中找到约束 %s。", ss, st, cons_name_u)
                        continue
                    ddl_lines: List[str] = []
                    # 合并相关的 replacements 和 FK 引用表的映射
                    constraint_replacements = get_relevant_replacements(ss) + fk_ref_replacements
                    for stmt in statements:
                        ddl_adj = adjust_ddl_for_object(
                            stmt,
                            ss,
                            st,
                            ts,
                            tt,
                            extra_identifiers=constraint_replacements
                        )
                        ddl_adj = normalize_ddl_for_ob(ddl_adj)
                        ddl_adj = strip_constraint_enable(ddl_adj)
                        ddl_adj = strip_enable_novalidate(ddl_adj)
                        ddl_lines.append(ddl_adj if ddl_adj.endswith(';') else ddl_adj + ';')
                    content = prepend_set_schema("\n".join(ddl_lines), ts)
                    filename = f"{ts}.{cons_name_u}.sql"
                    header = f"修补缺失的约束 {cons_name_u} (表: {ts}.{tt})"
                    log.info("[FIXUP]%s 写入 CONSTRAINT 脚本: %s", source_tag(source_label), filename)
                    write_fixup_file(
                        base_dir,
                        'constraint',
                        filename,
                        content,
                        header,
                        grants_to_add=sorted(grants_for_constraint) if grants_for_constraint else None
                    )
                finally:
                    constraint_progress()
        constraint_jobs.append(_job)
    run_tasks(constraint_jobs, "CONSTRAINT")

    log.info("[FIXUP] (7/9) 正在生成 TRIGGER 脚本...")
    trigger_progress = build_progress_tracker(len(trigger_tasks), "[FIXUP] (7/9) TRIGGER")
    trigger_jobs: List[Callable[[], None]] = []
    for src_schema, trg_name, tgt_schema, tgt_obj, src_table, tgt_table_schema, tgt_table in trigger_tasks:
        def _job(
            ss=src_schema,
            tn=trg_name,
            ts=tgt_schema,
            to=tgt_obj,
            st=src_table,
            tts=tgt_table_schema,
            tt=tgt_table
        ):
            try:
                fetch_result = fetch_ddl_with_timing(ss, 'TRIGGER', tn)
                if len(fetch_result) != 3:
                    log.error("[FIXUP] TRIGGER fetch_ddl_with_timing 返回了 %d 个值，期望 3 个: %s", len(fetch_result), fetch_result)
                    return
                ddl, ddl_source_label, _elapsed = fetch_result
                if not ddl:
                    log.warning("[FIXUP] 未找到 TRIGGER %s.%s 的 dbcat DDL。", ss, tn)
                    mark_source('TRIGGER', 'missing')
                    return
                if ddl_source_label.startswith("DBCAT"):
                    mark_source('TRIGGER', 'dbcat')
                elif ddl_source_label == "DBMS_METADATA":
                    mark_source('TRIGGER', 'fallback')
                else:
                    mark_source('TRIGGER', 'missing')
                extra_ids = get_relevant_replacements(ss)
                if st and tt and tts:
                    extra_ids = extra_ids + [((ss.upper(), st.upper()), (tts.upper(), tt.upper()))]
                ddl_adj = adjust_ddl_for_object(
                    ddl,
                    ss,
                    tn,
                    ts,
                    to,
                    extra_identifiers=extra_ids,
                    obj_type='TRIGGER'
                )
                # 重映射触发器中的表引用
                ddl_adj = remap_plsql_object_references(ddl_adj, 'TRIGGER', full_object_mapping, source_schema=ss)
                # 强化主对象与 ON 子句的 schema 替换（避免遗漏）
                def _rewrite_trigger_name_and_on(text: str) -> str:
                    # 替换 CREATE TRIGGER 段的 schema 前缀
                    name_pattern = re.compile(
                        rf'(CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+)"?{re.escape(ss)}"?\s*\.\s*"?{re.escape(tn)}"?',
                        re.IGNORECASE
                    )
                    text = name_pattern.sub(rf'\1{ts}.{to}', text, count=1)
                    # 替换 ON 子句表名
                    if st and tt and tts:
                        on_pattern = re.compile(
                            rf'(\bON\s+)("?\s*{re.escape(ss)}\s*"?\s*\.\s*)?"?{re.escape(st)}"?',
                            re.IGNORECASE
                        )
                        text = on_pattern.sub(rf'\1{tts}.{tt}', text, count=1)
                    return text
                ddl_adj = _rewrite_trigger_name_and_on(ddl_adj)
                ddl_adj = cleanup_dbcat_wrappers(ddl_adj)
                ddl_adj = prepend_set_schema(ddl_adj, ts)
                ddl_adj = apply_ddl_cleanup_rules(ddl_adj, 'TRIGGER')
                clean_notes = None
                if settings.get('enable_ddl_punct_sanitize', True):
                    ddl_adj, replaced, samples = sanitize_plsql_punctuation(ddl_adj, 'TRIGGER')
                    if replaced:
                        obj_full = f"{ts}.{to}"
                        sample_text = ", ".join(f"{src}->{dst}" for src, dst in samples)
                        suffix = f" 示例: {sample_text}" if sample_text else ""
                        log.info(
                            "[DDL_CLEAN] TRIGGER %s 全角标点清洗 %d 处。%s",
                            obj_full,
                            replaced,
                            suffix
                        )
                        record_ddl_clean('TRIGGER', obj_full, replaced, samples)
                        clean_notes = [f"DDL_CLEAN: 全角标点清洗 {replaced} 处。{suffix}"]
                ddl_adj = strip_constraint_enable(ddl_adj)
                ddl_adj = enforce_schema_for_ddl(ddl_adj, ts, 'TRIGGER')
                grants_for_trigger: Set[str] = set()
                if tts and tt and ts and tts.upper() != ts.upper():
                    table_full = f"{tts}.{tt}"
                    required_priv = GRANT_PRIVILEGE_BY_TYPE.get('TABLE', 'SELECT')
                    add_object_grant(ts, required_priv, table_full)
                    if grant_enabled:
                        grants_for_trigger.add(format_object_grant(
                            ts,
                            ObjectGrantEntry(required_priv.upper(), table_full.upper(), False)
                        ))
                if grant_enabled and ts and tts and tt:
                    table_full_u = f"{tts}.{tt}".upper()
                    for entry in object_grants_by_grantee.get(ts.upper(), set()):
                        if entry.object_full.upper() == table_full_u:
                            grants_for_trigger.add(format_object_grant(ts, entry))
                filename = f"{ts}.{to}.sql"
                header = f"修补缺失的触发器 {to} (源: {ss}.{tn})"
                log.info("[FIXUP]%s 写入 TRIGGER 脚本: %s", source_tag(ddl_source_label), filename)
                write_fixup_file(
                    base_dir,
                    'trigger',
                    filename,
                    ddl_adj,
                    header,
                    grants_to_add=sorted(grants_for_trigger) if grants_for_trigger else None,
                    extra_comments=clean_notes
                )
            finally:
                trigger_progress()
        trigger_jobs.append(_job)
    run_tasks(trigger_jobs, "TRIGGER")

    dep_report = dependency_report or {}
    compile_tasks: Dict[Tuple[str, str, str], Set[str]] = defaultdict(set)

    def _ob_object_exists(full_name: str, obj_type: str) -> bool:
        if ob_meta is None:
            return True
        return full_name.upper() in ob_meta.objects_by_type.get(obj_type.upper(), set())

    def _compile_statements(obj_type: str, obj_name: str) -> List[str]:
        obj_type_u = obj_type.upper()
        obj_name_u = obj_name.upper()
        if obj_type_u in ("FUNCTION", "PROCEDURE"):
            return [f"ALTER {obj_type_u} {obj_name_u} COMPILE;"]
        if obj_type_u in ("PACKAGE", "PACKAGE BODY"):
            return [
                f"ALTER PACKAGE {obj_name_u} COMPILE;",
                f"ALTER PACKAGE {obj_name_u} COMPILE BODY;"
            ]
        if obj_type_u == "TRIGGER":
            return [f"ALTER TRIGGER {obj_name_u} COMPILE;"]
        if obj_type_u in ("VIEW", "MATERIALIZED VIEW"):
            return [f"ALTER {obj_type_u} {obj_name_u} COMPILE;"]
        if obj_type_u == "TYPE":
            return [f"ALTER TYPE {obj_name_u} COMPILE;"]
        if obj_type_u == "TYPE BODY":
            return [f"ALTER TYPE {obj_name_u} COMPILE BODY;"]
        return []

    log.info("[FIXUP] (8/9) 正在生成依赖重编译脚本...")
    for issue in dep_report.get("missing", []):
        dep_name = (issue.dependent or "").upper()
        dep_type = (issue.dependent_type or "").upper()
        if not dep_name or not dep_type:
            continue
        if not _ob_object_exists(dep_name, dep_type):
            continue
        parts = dep_name.split('.', 1)
        if len(parts) != 2:
            continue
        schema_u, obj_u = parts[0], parts[1]
        if not allow_fixup(dep_type, schema_u):
            continue
        stmts = _compile_statements(dep_type, obj_u)
        if not stmts:
            continue
        compile_tasks[(schema_u, obj_u, dep_type)].update(stmts)

    if compile_tasks:
        compile_order = build_compile_order(compile_tasks, expected_dependency_pairs)
        for (schema_u, obj_u, dep_type) in compile_order:
            stmts = compile_tasks.get((schema_u, obj_u, dep_type)) or set()
            if not stmts:
                continue
            content = "\n".join(sorted(stmts))
            content = prepend_set_schema(content, schema_u)
            filename = f"{schema_u}.{obj_u}.compile.sql"
            header = f"依赖重编译 {dep_type} {schema_u}.{obj_u}"
            write_fixup_file(base_dir, 'compile', filename, content, header)
    else:
        log.info("[FIXUP] (8/9) 无需生成依赖重编译脚本。")

    if grant_enabled:
        grant_dir_all = 'grants_all'
        grant_dir_miss = 'grants_miss'

        if grant_plan.role_ddls:
            role_content = "\n".join(grant_plan.role_ddls).strip()
            if role_content:
                log.info("[FIXUP] (9/9) 正在生成角色 DDL 脚本...")
                write_fixup_file(
                    base_dir,
                    grant_dir_all,
                    "roles.sql",
                    role_content,
                    "角色 DDL (来自 Oracle 授权引用)"
                )
                write_fixup_file(
                    base_dir,
                    grant_dir_miss,
                    "roles.sql",
                    role_content,
                    "角色 DDL (来自 Oracle 授权引用)"
                )

        if object_grants_by_grantee or sys_privs_by_grantee or role_privs_by_grantee:
            log.info("[FIXUP] (9/9) 正在生成授权脚本...")
            # --- grants_all ---
            if grants_by_owner:
                for owner, stmts in sorted(grants_by_owner.items()):
                    if not stmts:
                        continue
                    content = prepend_set_schema("\n".join(sorted(stmts)), owner)
                    header = f"{owner} 对象权限授权"
                    write_fixup_file(
                        base_dir,
                        grant_dir_all,
                        f"{owner}.grants.sql",
                        content,
                        header
                    )

            privs_by_grantee: Dict[str, Set[str]] = defaultdict(set)
            for grantee, entries in sys_privs_by_grantee.items():
                for entry in entries:
                    privs_by_grantee[grantee.upper()].add(format_sys_grant(grantee, entry))
            for grantee, entries in role_privs_by_grantee.items():
                for entry in entries:
                    privs_by_grantee[grantee.upper()].add(format_role_grant(grantee, entry))

            for grantee, stmts in sorted(privs_by_grantee.items()):
                if not stmts:
                    continue
                content = "\n".join(sorted(stmts))
                header = f"{grantee} 系统/角色权限授权"
                write_fixup_file(
                    base_dir,
                    grant_dir_all,
                    f"{grantee}.privs.sql",
                    content,
                    header
                )

            # --- grants_miss ---
            miss_grants_by_owner: Dict[str, Set[str]] = {}
            if object_grants_missing_by_grantee:
                _raw_miss, _merged_miss, _obj_lookup_miss, miss_grants_by_owner = (
                    build_object_grant_statements_for(object_grants_missing_by_grantee)
                )

            if miss_grants_by_owner:
                for owner, stmts in sorted(miss_grants_by_owner.items()):
                    if not stmts:
                        continue
                    content = prepend_set_schema("\n".join(sorted(stmts)), owner)
                    header = f"{owner} 对象权限授权"
                    write_fixup_file(
                        base_dir,
                        grant_dir_miss,
                        f"{owner}.grants.sql",
                        content,
                        header
                    )

            miss_privs_by_grantee: Dict[str, Set[str]] = defaultdict(set)
            for grantee, entries in sys_privs_missing_by_grantee.items():
                for entry in entries:
                    miss_privs_by_grantee[grantee.upper()].add(format_sys_grant(grantee, entry))
            for grantee, entries in role_privs_missing_by_grantee.items():
                for entry in entries:
                    miss_privs_by_grantee[grantee.upper()].add(format_role_grant(grantee, entry))

            for grantee, stmts in sorted(miss_privs_by_grantee.items()):
                if not stmts:
                    continue
                content = "\n".join(sorted(stmts))
                header = f"{grantee} 系统/角色权限授权"
                write_fixup_file(
                    base_dir,
                    grant_dir_miss,
                    f"{grantee}.privs.sql",
                    content,
                    header
                )

            if (
                not grants_by_owner
                and not privs_by_grantee
                and not grant_plan.role_ddls
                and not miss_grants_by_owner
                and not miss_privs_by_grantee
            ):
                log.info("[FIXUP] (9/9) 无需生成授权脚本。")
        else:
            if not grant_plan.role_ddls:
                log.info("[FIXUP] (9/9) 无需生成授权脚本。")
    else:
        log.info("[FIXUP] (9/9) 授权脚本生成已关闭。")

    if oracle_conn:
        try:
            oracle_conn.close()
        except Exception:
            pass

    if ddl_source_stats:
        summary_lines: List[str] = []
        for obj_type, src_map in sorted(ddl_source_stats.items()):
            parts = []
            for label in ("dbcat", "fallback", "missing"):
                val = src_map.get(label, 0)
                if val:
                    parts.append(f"{label}={val}")
            if parts:
                summary_lines.append(f"{obj_type}: " + ", ".join(parts))
        if summary_lines:
            log.info("[FIXUP] DDL 来源统计: %s", " | ".join(summary_lines))

    if ddl_clean_records and report_dir and report_timestamp:
        clean_report_path = export_ddl_clean_report(ddl_clean_records, report_dir, report_timestamp)
        if clean_report_path:
            log.info("[DDL_CLEAN] 全角标点清洗报告已输出: %s", clean_report_path)

    if unsupported_types:
        log.warning(
            "[dbcat] 以下对象类型当前未集成自动导出，需人工处理: %s",
            ", ".join(sorted(unsupported_types))
        )
    return view_chain_file


# ====================== 报告输出 (Rich) ======================
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich.theme import Theme
except ImportError:
    print("错误: 未找到 'rich' 库。", file=sys.stderr)
    print("请先安装: pip install rich", file=sys.stderr)
    sys.exit(1)


def format_missing_mapping(src_name: str, tgt_name: str) -> str:
    """
    返回缺失对象的展示字符串：如需 remap 则展示 src=tgt，否则仅展示源名。
    """
    src_clean = (src_name or "").strip()
    tgt_clean = (tgt_name or "").strip()
    if not src_clean and not tgt_clean:
        return ""
    if not tgt_clean or src_clean.upper() == tgt_clean.upper():
        return src_clean or tgt_clean
    return f"{src_clean}={tgt_clean}"


def parse_full_object_name(name: str) -> Optional[Tuple[str, str]]:
    if not name or '.' not in name:
        return None
    parts = name.split('.', 1)
    schema = parts[0].strip().strip('"').upper()
    obj = parts[1].strip().strip('"').upper()
    if not schema or not obj:
        return None
    return schema, obj


def export_full_object_mapping(
    full_object_mapping: FullObjectMapping,
    output_path: Path
) -> Optional[Path]:
    """
    将最终推导的全量对象映射输出为纯文本，便于人工审计。
    每行格式：SRC_FULL<TAB>OBJECT_TYPE<TAB>TGT_FULL
    """
    if not full_object_mapping:
        return None
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        lines: List[str] = []
        for src_full in sorted(full_object_mapping.keys()):
            type_map = full_object_mapping.get(src_full, {})
            for obj_type in sorted(type_map.keys()):
                tgt_full = type_map[obj_type]
                lines.append(f"{src_full}\t{obj_type}\t{tgt_full}")
        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return output_path
    except OSError as exc:
        log.warning("写入全量对象映射文件失败 %s: %s", output_path, exc)
        return None


def export_remap_conflicts(
    remap_conflicts: RemapConflictMap,
    output_path: Path
) -> Optional[Path]:
    """
    输出无法自动推导的对象列表，便于提醒显式 remap。
    每行格式：SRC_FULL<TAB>OBJECT_TYPE<TAB>REASON
    """
    if not remap_conflicts:
        return None
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        lines: List[str] = ["# 无法自动推导的对象，请在 remap_rules.txt 中显式配置"]
        for (src_full, obj_type), reason in sorted(remap_conflicts.items()):
            lines.append(f"{src_full}\t{obj_type}\t{reason}")
        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return output_path
    except OSError as exc:
        log.warning("写入 remap_conflicts 文件失败 %s: %s", output_path, exc)
        return None


def export_missing_table_view_mappings(
    tv_results: ReportResults,
    report_dir: Path,
    blacklisted_tables: Optional[Set[Tuple[str, str]]] = None
) -> Optional[Path]:
    """
    将缺失的 TABLE/VIEW 映射按目标 schema 输出为文本，便于迁移工具直接消费。
    输出文件示例：SCHEMA_T.txt / SCHEMA_V.txt
    若提供 blacklisted_tables，则跳过黑名单 TABLE。
    """
    if not report_dir:
        return None

    output_dir = Path(report_dir) / "tables_views_miss"
    if output_dir.exists():
        shutil.rmtree(output_dir, ignore_errors=True)

    grouped: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: {"TABLE": [], "VIEW": []})
    for obj_type, tgt_name, src_name in tv_results.get("missing", []):
        obj_type_u = obj_type.upper()
        if obj_type_u not in ("TABLE", "VIEW"):
            continue
        if obj_type_u == "TABLE" and blacklisted_tables:
            src_key = parse_full_object_name(src_name)
            if src_key and src_key in blacklisted_tables:
                continue
        if "." not in tgt_name or "." not in src_name:
            continue
        tgt_schema = tgt_name.split(".")[0].upper()
        formatted = format_missing_mapping(src_name, tgt_name)
        if formatted:
            grouped[tgt_schema][obj_type_u].append(formatted)

    if not grouped:
        return None

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        log.warning("无法创建缺失 TABLE/VIEW 映射目录 %s: %s", output_dir, exc)
        return None

    for tgt_schema, type_map in sorted(grouped.items()):
        for obj_type, mappings in sorted(type_map.items()):
            if not mappings:
                continue
            suffix = "T" if obj_type == "TABLE" else "V"
            file_path = output_dir / f"{tgt_schema}_{suffix}.txt"
            lines = sorted(set(mappings))
            try:
                file_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            except OSError as exc:
                log.warning("写入缺失映射文件失败 %s: %s", file_path, exc)
    return output_dir


def derive_package_report_path(report_file: Path) -> Path:
    name = report_file.name
    if name.startswith("report_"):
        suffix = name[len("report_"):]
        return report_file.parent / f"package_compare_{suffix}"
    return report_file.parent / "package_compare.txt"


def export_package_compare_report(
    rows: List[PackageCompareRow],
    output_path: Path
) -> Optional[Path]:
    """
    输出 PACKAGE / PACKAGE BODY 校验明细。
    """
    if not rows or not output_path:
        return None
    def _package_sort_key(row: PackageCompareRow) -> Tuple[str, str, int, str, str]:
        parsed = parse_full_object_name(row.src_full) or parse_full_object_name(row.tgt_full)
        owner, name = parsed if parsed else ("", "")
        type_u = (row.obj_type or "").upper()
        type_rank = 0 if type_u == "PACKAGE" else 1 if type_u == "PACKAGE BODY" else 2
        return (owner, name, type_rank, row.src_full, row.tgt_full)

    rows_sorted = sorted(rows, key=_package_sort_key)
    delimiter = "|"
    header = delimiter.join([
        "SRC_FULL",
        "TYPE",
        "SRC_STATUS",
        "TGT_FULL",
        "TGT_STATUS",
        "RESULT",
        "ERROR_COUNT",
        "FIRST_ERROR"
    ])
    lines: List[str] = [
        "# PACKAGE/PACKAGE BODY 对比明细",
        f"# total={len(rows_sorted)}",
        f"# 分隔符: {delimiter}",
        f"# 字段说明: {header}",
        header
    ]
    for row in rows_sorted:
        first_error = normalize_error_text(row.first_error)
        lines.append(delimiter.join([
            row.src_full,
            row.obj_type,
            row.src_status,
            row.tgt_full,
            row.tgt_status,
            row.result,
            str(row.error_count),
            first_error
        ]))
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
        return output_path
    except OSError as exc:
        log.warning("写入 package_compare 报告失败 %s: %s", output_path, exc)
        return None


def export_ddl_clean_report(
    rows: List[DdlCleanReportRow],
    report_dir: Path,
    report_timestamp: Optional[str]
) -> Optional[Path]:
    """
    输出 PL/SQL 全角标点清洗报告。
    """
    if not report_dir or not rows or not report_timestamp:
        return None

    output_path = Path(report_dir) / f"ddl_punct_clean_{report_timestamp}.txt"
    rows_sorted = sorted(rows, key=lambda r: (r.obj_type, r.obj_full))
    total_replaced = sum(r.replaced for r in rows_sorted)
    lines: List[str] = [
        "# PL/SQL 全角标点清洗报告",
        f"# total_objects={len(rows_sorted)} total_replacements={total_replaced}",
        "# 字段说明: TYPE | OBJECT | REPLACED | SAMPLES"
    ]

    sample_texts: List[str] = []
    for row in rows_sorted:
        if row.samples:
            sample_texts.append(", ".join(f"{src}->{dst}" for src, dst in row.samples))
        else:
            sample_texts.append("-")

    type_w = max(len("TYPE"), max((len(r.obj_type) for r in rows_sorted), default=0))
    obj_w = max(len("OBJECT"), max((len(r.obj_full) for r in rows_sorted), default=0))
    replaced_w = max(len("REPLACED"), max((len(str(r.replaced)) for r in rows_sorted), default=0))
    sample_w = max(len("SAMPLES"), max((len(text) for text in sample_texts), default=0))

    header = (
        f"{'TYPE'.ljust(type_w)}  "
        f"{'OBJECT'.ljust(obj_w)}  "
        f"{'REPLACED'.rjust(replaced_w)}  "
        f"{'SAMPLES'.ljust(sample_w)}"
    )
    lines.append(header)
    lines.append(
        f"{'-' * type_w}  "
        f"{'-' * obj_w}  "
        f"{'-' * replaced_w}  "
        f"{'-' * sample_w}"
    )

    for row, sample in zip(rows_sorted, sample_texts):
        lines.append(
            f"{row.obj_type.ljust(type_w)}  "
            f"{row.obj_full.ljust(obj_w)}  "
            f"{str(row.replaced).rjust(replaced_w)}  "
            f"{sample.ljust(sample_w)}"
        )

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        content = "\n".join(lines).rstrip() + "\n"
        output_path.write_text(content, encoding="utf-8")
        return output_path
    except OSError as exc:
        log.warning("写入全角标点清洗报告失败 %s: %s", output_path, exc)
        return None


def evaluate_long_conversion_status(
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str
) -> Tuple[str, str, bool]:
    """
    校验 LONG/LONG RAW 列在目标端是否已转换为 CLOB/BLOB。
    返回 (status, detail, verified)。
    """
    tgt_full = f"{tgt_schema.upper()}.{tgt_table.upper()}"
    src_key = (src_schema.upper(), src_table.upper())
    tgt_key = (tgt_schema.upper(), tgt_table.upper())

    if not ob_meta or not ob_meta.objects_by_type:
        return "UNKNOWN", "目标端元数据缺失", False

    tgt_tables = ob_meta.objects_by_type.get("TABLE")
    if tgt_tables is None:
        return "UNKNOWN", "目标端未加载 TABLE 元数据", False

    if tgt_full not in tgt_tables:
        return "MISSING_TABLE", "目标端表不存在", False

    src_cols = oracle_meta.table_columns.get(src_key)
    if src_cols is None:
        return "UNKNOWN", "源端列元数据缺失", False

    long_cols: Dict[str, str] = {}
    for col, info in src_cols.items():
        src_type = (info.get("data_type") or "").strip().upper()
        if is_long_type(src_type):
            long_cols[col.upper()] = src_type

    if not long_cols:
        return "NO_LONG_COLUMNS", "源端未发现 LONG/LONG RAW 列", False

    if not ob_meta.tab_columns:
        return "UNKNOWN", "目标端未加载列元数据", False

    tgt_cols = ob_meta.tab_columns.get(tgt_key)
    if not tgt_cols:
        return "UNKNOWN", "目标端列元数据缺失", False

    missing_cols: List[str] = []
    mismatch_cols: List[str] = []
    for col, src_type in long_cols.items():
        tgt_info = tgt_cols.get(col)
        if not tgt_info:
            missing_cols.append(col)
            continue
        tgt_type = (tgt_info.get("data_type") or "").strip().upper() or "UNKNOWN"
        expected = map_long_type_to_ob(src_type)
        if tgt_type != expected:
            mismatch_cols.append(f"{col}({tgt_type}->{expected})")

    if missing_cols:
        return "MISSING_COLUMN", f"缺失列: {sorted(missing_cols)}", False
    if mismatch_cols:
        return "TYPE_MISMATCH", f"类型不匹配: {sorted(mismatch_cols)}", False

    return "VERIFIED", "已校验: LONG/LONG RAW 已转换为 CLOB/BLOB", True


def build_blacklist_report_rows(
    blacklist_tables: BlacklistTableMap,
    table_target_map: Dict[Tuple[str, str], Tuple[str, str]],
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata
) -> List[BlacklistReportRow]:
    """
    生成黑名单表报告行，LONG/LONG RAW 额外校验目标端转换情况。
    """
    rows: List[BlacklistReportRow] = []
    for (schema, table), entries in blacklist_tables.items():
        schema_u = schema.upper()
        table_u = table.upper()
        tgt_schema, tgt_table = table_target_map.get((schema_u, table_u), (schema_u, table_u))
        mapped_full = f"{tgt_schema.upper()}.{tgt_table.upper()}"
        src_full = f"{schema_u}.{table_u}"
        mapping_hint = ""
        if mapped_full != src_full:
            mapping_hint = f"目标端: {mapped_full}; "

        for black_type, data_type in sorted(entries):
            black_type_u = normalize_black_type(black_type) or "UNKNOWN"
            data_type_u = normalize_black_data_type(data_type) or "-"
            reason = blacklist_reason(black_type_u)
            status = "BLACKLISTED"
            detail = "-"

            if black_type_u == "LONG" or is_long_type(data_type_u):
                status, detail, verified = evaluate_long_conversion_status(
                    oracle_meta,
                    ob_meta,
                    schema_u,
                    table_u,
                    tgt_schema,
                    tgt_table
                )
                detail = f"{mapping_hint}{detail}" if mapping_hint and detail != "-" else detail
                if verified:
                    reason = "已校验: LONG/LONG RAW 已转换为 CLOB/BLOB"

            rows.append(
                BlacklistReportRow(
                    schema=schema_u,
                    table=table_u,
                    black_type=black_type_u,
                    data_type=data_type_u,
                    reason=reason,
                    status=status,
                    detail=detail
                )
            )
    return rows


def export_blacklist_tables(
    rows: List[BlacklistReportRow],
    report_dir: Path
) -> Optional[Path]:
    """
    将黑名单表输出为文本，按 schema 分组并标注原因与 LONG 校验状态。
    """
    if not report_dir or not rows:
        return None

    output_path = Path(report_dir) / "blacklist_tables.txt"
    grouped: Dict[str, List[BlacklistReportRow]] = defaultdict(list)
    for row in rows:
        grouped[row.schema.upper()].append(row)

    lines: List[str] = [
        "# 黑名单表清单（LONG/LONG RAW 将校验目标端转换情况）",
        "# 说明: 黑名单缺失表不会生成 tables_views_miss 规则",
        "# 字段说明: TABLE_FULL | BLACK_TYPE | DATA_TYPE | STATUS | DETAIL | REASON"
    ]
    for schema in sorted(grouped.keys()):
        schema_rows = sorted(
            grouped[schema],
            key=lambda r: (r.table, r.black_type, r.data_type)
        )
        formatted_rows: List[Tuple[str, str, str, str, str, str]] = []
        for row in schema_rows:
            formatted_rows.append((
                f"{schema}.{row.table}",
                row.black_type,
                row.data_type,
                row.status,
                row.detail,
                row.reason
            ))

        table_count = len({r[0] for r in formatted_rows})
        entry_count = len(formatted_rows)
        lines.append(f"[{schema}] (tables={table_count}, entries={entry_count})")

        table_w = max(len("TABLE_FULL"), max((len(r[0]) for r in formatted_rows), default=0))
        type_w = max(len("BLACK_TYPE"), max((len(r[1]) for r in formatted_rows), default=0))
        data_w = max(len("DATA_TYPE"), max((len(r[2]) for r in formatted_rows), default=0))
        status_w = max(len("STATUS"), max((len(r[3]) for r in formatted_rows), default=0))
        detail_w = max(len("DETAIL"), max((len(r[4]) for r in formatted_rows), default=0))

        header = (
            f"{'TABLE_FULL'.ljust(table_w)}  "
            f"{'BLACK_TYPE'.ljust(type_w)}  "
            f"{'DATA_TYPE'.ljust(data_w)}  "
            f"{'STATUS'.ljust(status_w)}  "
            f"{'DETAIL'.ljust(detail_w)}  "
            f"REASON"
        )
        sep = (
            f"{'-' * table_w}  "
            f"{'-' * type_w}  "
            f"{'-' * data_w}  "
            f"{'-' * status_w}  "
            f"{'-' * detail_w}  "
            f"{'-' * 6}"
        )
        lines.append(header)
        lines.append(sep)

        for table_full, black_type, data_type, status, detail, reason in formatted_rows:
            lines.append(
                f"{table_full.ljust(table_w)}  "
                f"{black_type.ljust(type_w)}  "
                f"{data_type.ljust(data_w)}  "
                f"{status.ljust(status_w)}  "
                f"{detail.ljust(detail_w)}  "
                f"{reason}"
            )
        lines.append("")

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        content = "\n".join(lines).rstrip() + "\n"
        output_path.write_text(content, encoding="utf-8")
        return output_path
    except OSError as exc:
        log.warning("写入黑名单表清单失败 %s: %s", output_path, exc)
        return None


def export_trigger_miss_report(
    rows: List[TriggerListReportRow],
    summary: Dict[str, object],
    report_dir: Path
) -> Optional[Path]:
    """
    输出触发器清单筛选报告。
    """
    if not report_dir or not summary or not summary.get("enabled"):
        return None

    output_path = Path(report_dir) / "trigger_miss.txt"
    lines: List[str] = [
        "# 触发器清单筛选报告 (trigger_list)",
        f"# trigger_list: {summary.get('path', '')}",
        (
            "# 汇总: total_lines={total_lines}, valid={valid_entries}, invalid={invalid_entries}, "
            "duplicate={duplicate_entries}, selected_missing={selected_missing}, "
            "missing_not_listed={missing_not_listed}, not_found={not_found}, not_missing={not_missing}"
        ).format(**{k: summary.get(k, 0) for k in [
            "total_lines", "valid_entries", "invalid_entries", "duplicate_entries",
            "selected_missing", "missing_not_listed", "not_found", "not_missing"
        ]})
    ]
    if summary.get("error"):
        lines.append(f"# ERROR: {summary.get('error')}")
    if summary.get("fallback_full"):
        reason = summary.get("fallback_reason") or "unknown"
        note = "清单不可用，已回退全量触发器"
        if reason == "empty_list":
            note = "清单为空，已回退全量触发器"
        elif reason == "read_error":
            note = "清单读取失败，已回退全量触发器"
        lines.append(f"# NOTE: {note}")
    if summary.get("check_disabled"):
        lines.append("# NOTE: TRIGGER 未启用检查，清单仅做格式校验。")
    lines.append("# 字段说明: ENTRY | STATUS | DETAIL")

    if rows:
        entry_w = max(len("ENTRY"), max((len(r.entry) for r in rows), default=0))
        status_w = max(len("STATUS"), max((len(r.status) for r in rows), default=0))
        header = f"{'ENTRY'.ljust(entry_w)}  {'STATUS'.ljust(status_w)}  DETAIL"
        lines.append(header)
        lines.append("-" * len(header))
        for row in rows:
            lines.append(f"{row.entry.ljust(entry_w)}  {row.status.ljust(status_w)}  {row.detail}")

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        content = "\n".join(lines).rstrip() + "\n"
        output_path.write_text(content, encoding="utf-8")
        return output_path
    except OSError as exc:
        log.warning("写入 trigger_miss 报告失败 %s: %s", output_path, exc)
        return None


def export_filtered_grants(
    filtered_grants: List[FilteredGrantEntry],
    report_dir: Path
) -> Optional[Path]:
    """
    输出被过滤掉的 GRANT 权限清单。
    """
    if not report_dir or not filtered_grants:
        return None

    output_path = Path(report_dir) / "filtered_grants.txt"
    rows = sorted(
        filtered_grants,
        key=lambda r: (r.category, r.grantee, r.privilege, r.object_full, r.reason)
    )
    lines: List[str] = [
        "# 被过滤掉的 GRANT 权限 (Oracle/OB 不兼容或未知权限)",
        f"# total={len(rows)}",
        "# 字段说明: CATEGORY | GRANTEE | PRIVILEGE | OBJECT | REASON"
    ]

    cat_w = max(len("CATEGORY"), max((len(r.category) for r in rows), default=0))
    grantee_w = max(len("GRANTEE"), max((len(r.grantee) for r in rows), default=0))
    priv_w = max(len("PRIVILEGE"), max((len(r.privilege) for r in rows), default=0))
    obj_w = max(len("OBJECT"), max((len(r.object_full or "-") for r in rows), default=0))

    header = (
        f"{'CATEGORY'.ljust(cat_w)}  "
        f"{'GRANTEE'.ljust(grantee_w)}  "
        f"{'PRIVILEGE'.ljust(priv_w)}  "
        f"{'OBJECT'.ljust(obj_w)}  "
        f"REASON"
    )
    lines.append(header)
    lines.append("-" * len(header))

    for row in rows:
        obj = row.object_full or "-"
        lines.append(
            f"{row.category.ljust(cat_w)}  "
            f"{row.grantee.ljust(grantee_w)}  "
            f"{row.privilege.ljust(priv_w)}  "
            f"{obj.ljust(obj_w)}  "
            f"{row.reason}"
        )

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        content = "\n".join(lines).rstrip() + "\n"
        output_path.write_text(content, encoding="utf-8")
        return output_path
    except OSError as exc:
        log.warning("写入 filtered_grants 报告失败 %s: %s", output_path, exc)
        return None


def collect_blacklisted_missing_tables(
    tv_results: ReportResults,
    blacklist_tables: BlacklistTableMap
) -> BlacklistTableMap:
    """
    从黑名单表中筛选出“当前缺失”的 TABLE，用于统计与输出。
    """
    if not tv_results or not blacklist_tables:
        return {}

    missing_tables: Set[Tuple[str, str]] = set()
    for obj_type, _, src_name in tv_results.get("missing", []):
        if obj_type.upper() != "TABLE":
            continue
        src_key = parse_full_object_name(src_name)
        if src_key:
            missing_tables.add(src_key)

    return {
        key: entries
        for key, entries in blacklist_tables.items()
        if key in missing_tables
    }


def build_run_summary(
    ctx: RunSummaryContext,
    tv_results: ReportResults,
    extra_results: ExtraCheckResults,
    comment_results: Dict[str, object],
    dependency_report: DependencyReport,
    remap_conflicts: List[Tuple[str, str, str]],
    extraneous_rules: List[str],
    blacklisted_missing_tables: Optional[BlacklistTableMap],
    report_file: Optional[Path],
    filtered_grants_path: Optional[Path] = None,
    filtered_grants_count: int = 0
) -> RunSummary:
    end_time = datetime.now()
    total_seconds = time.perf_counter() - ctx.start_perf

    phases: List[RunPhaseInfo] = []
    for phase in RUN_PHASE_ORDER:
        if phase in ctx.phase_durations:
            phases.append(RunPhaseInfo(phase, ctx.phase_durations[phase], "完成"))
        else:
            reason = ctx.phase_skip_reasons.get(phase, "跳过")
            phases.append(RunPhaseInfo(phase, None, reason))

    actions_done: List[str] = []
    actions_skipped: List[str] = []

    if ctx.total_checked > 0:
        actions_done.append(
            f"主对象校验: {', '.join(sorted(ctx.enabled_primary_types))} (校验对象 {ctx.total_checked})"
        )
    else:
        actions_skipped.append("主对象校验: 主校验清单为空")

    if ctx.print_only_types:
        actions_done.append(f"仅打印不校验: {', '.join(sorted(ctx.print_only_types))}")

    if ctx.enabled_extra_types:
        actions_done.append(f"扩展对象校验: {', '.join(sorted(ctx.enabled_extra_types))}")
    else:
        actions_skipped.append("扩展对象校验: check_extra_types 为空")

    comment_skip_reason = comment_results.get("skipped_reason")
    if ctx.enable_comment_check:
        if comment_skip_reason:
            actions_skipped.append(f"注释一致性校验: 跳过 ({comment_skip_reason})")
        else:
            actions_done.append("注释一致性校验: 启用")
    else:
        actions_skipped.append("注释一致性校验: check_comments=false")

    if ctx.enable_dependencies_check:
        actions_done.append("依赖校验: 启用")
        if ctx.dependency_chain_file:
            actions_done.append(f"依赖链路输出: {ctx.dependency_chain_file}")
        if ctx.view_chain_file:
            actions_done.append(f"VIEW fixup 链路输出: {ctx.view_chain_file}")
    else:
        actions_skipped.append("依赖校验: check_dependencies=false")

    if ctx.enable_schema_mapping_infer:
        actions_done.append("schema 推导: 启用")
    else:
        actions_skipped.append("schema 推导: infer_schema_mapping=false")

    if ctx.fixup_enabled:
        actions_done.append(f"修补脚本生成: 启用 (目录 {ctx.fixup_dir})")
    else:
        actions_skipped.append("修补脚本生成: generate_fixup=false")

    if ctx.enable_grant_generation:
        if ctx.fixup_enabled:
            actions_done.append(
                "授权脚本生成: 启用 (目录 {miss}, {all})".format(
                    miss=Path(ctx.fixup_dir) / 'grants_miss',
                    all=Path(ctx.fixup_dir) / 'grants_all'
                )
            )
        else:
            actions_skipped.append("授权脚本生成: generate_fixup=false")
    else:
        actions_skipped.append("授权脚本生成: generate_grants=false")

    trigger_summary = ctx.trigger_list_summary or {}
    if trigger_summary.get("enabled"):
        if trigger_summary.get("error"):
            actions_done.append(f"触发器清单: 读取失败，已回退全量触发器 ({trigger_summary.get('error')})")
        elif trigger_summary.get("check_disabled"):
            actions_skipped.append("触发器清单: TRIGGER 未启用检查，仅校验清单格式")
        elif trigger_summary.get("fallback_full"):
            actions_done.append("触发器清单: 为空或无有效条目，已回退全量触发器")
        else:
            actions_done.append(
                "触发器清单: 生效 (列表 {valid_entries}, 命中缺失 {selected_missing})".format(
                    valid_entries=trigger_summary.get("valid_entries", 0),
                    selected_missing=trigger_summary.get("selected_missing", 0)
                )
            )
    else:
        actions_skipped.append("触发器清单: 未配置")

    if report_file:
        actions_done.append(f"报告输出: {Path(report_file).resolve()}")

    missing_count = len(tv_results.get("missing", []))
    mismatched_count = len(tv_results.get("mismatched", []))
    extra_target_cnt = len(tv_results.get("extra_targets", []))
    skipped_count = len(tv_results.get("skipped", []))
    remap_conflict_cnt = len(remap_conflicts)
    extraneous_count = len(extraneous_rules)
    blacklist_missing_cnt = len(blacklisted_missing_tables or {})
    comment_mis_cnt = len(comment_results.get("mismatched", []))
    idx_mis_cnt = len(extra_results.get("index_mismatched", []))
    cons_mis_cnt = len(extra_results.get("constraint_mismatched", []))
    seq_mis_cnt = len(extra_results.get("sequence_mismatched", []))
    trg_mis_cnt = len(extra_results.get("trigger_mismatched", []))
    dep_missing_cnt = len(dependency_report.get("missing", []))
    dep_unexpected_cnt = len(dependency_report.get("unexpected", []))
    dep_skipped_cnt = len(dependency_report.get("skipped", []))
    findings: List[str] = [
        f"主对象: 缺失 {missing_count}, 不匹配 {mismatched_count}, 多余 {extra_target_cnt}, 仅打印 {skipped_count}"
    ]
    if extraneous_count:
        findings.append(f"无效 remap 规则: {extraneous_count}")
    if remap_conflict_cnt:
        findings.append(f"无法推导对象: {remap_conflict_cnt}")
    if blacklist_missing_cnt:
        findings.append(f"黑名单缺失表: {blacklist_missing_cnt}")
    if ctx.enable_comment_check and not comment_skip_reason:
        findings.append(f"注释差异: {comment_mis_cnt}")
    else:
        findings.append(f"注释校验: 跳过 ({comment_skip_reason or '未启用'})")
    if ctx.enabled_extra_types:
        findings.append(
            f"扩展对象差异: INDEX {idx_mis_cnt}, CONSTRAINT {cons_mis_cnt}, "
            f"SEQUENCE {seq_mis_cnt}, TRIGGER {trg_mis_cnt}"
        )
    if ctx.enable_dependencies_check:
        findings.append(f"依赖差异: 缺失 {dep_missing_cnt}, 额外 {dep_unexpected_cnt}, 跳过 {dep_skipped_cnt}")
    if filtered_grants_count:
        if filtered_grants_path:
            findings.append(f"权限兼容过滤: {filtered_grants_count} 条 (见 {filtered_grants_path})")
        else:
            findings.append(f"权限兼容过滤: {filtered_grants_count} 条")

    if trigger_summary.get("enabled"):
        if trigger_summary.get("fallback_full"):
            findings.append(
                "触发器清单: 回退全量，缺失触发器 {missing_not_listed}".format(
                    missing_not_listed=trigger_summary.get("missing_not_listed", 0)
                )
            )
        elif not trigger_summary.get("error"):
            findings.append(
                "触发器清单: 列表 {valid_entries}, 命中缺失 {selected_missing}, 未列出缺失 {missing_not_listed}, "
                "无效 {invalid_entries}, 未找到 {not_found}".format(
                    valid_entries=trigger_summary.get("valid_entries", 0),
                    selected_missing=trigger_summary.get("selected_missing", 0),
                    missing_not_listed=trigger_summary.get("missing_not_listed", 0),
                    invalid_entries=trigger_summary.get("invalid_entries", 0),
                    not_found=trigger_summary.get("not_found", 0)
                )
            )

    attention: List[str] = []
    if missing_count or mismatched_count or extra_target_cnt:
        attention.append("目标端结构与源端不一致，需要处理缺失/差异/多余对象。")
    if remap_conflict_cnt:
        attention.append("存在无法自动推导的对象，需要在 remap_rules.txt 显式配置。")
    if extraneous_count:
        attention.append("remap_rules.txt 存在无效条目，建议清理。")
    if ctx.enable_comment_check and comment_skip_reason:
        attention.append("注释一致性未完成校验，报告中的注释差异可能不完整。")
    if not ctx.enable_dependencies_check:
        attention.append("依赖校验已关闭，依赖差异可能未暴露。")
    if dep_missing_cnt or dep_unexpected_cnt:
        attention.append("依赖关系存在缺失或额外，需要补齐或清理。")
    if blacklist_missing_cnt:
        attention.append("存在黑名单表，未生成 OMS 规则。")
    if trigger_summary.get("error") or trigger_summary.get("invalid_entries"):
        attention.append("触发器清单存在读取失败或无效条目。")
    if not ctx.enable_grant_generation:
        attention.append("授权脚本生成已关闭，权限调整需人工确认。")
    elif ctx.enable_grant_generation and not ctx.fixup_enabled:
        attention.append("授权脚本生成依赖 generate_fixup=true，当前未输出授权脚本。")

    next_steps: List[str] = []
    if remap_conflict_cnt:
        next_steps.append("补充 remap_rules.txt，为无法推导对象显式配置映射。")
    if missing_count or mismatched_count or idx_mis_cnt or cons_mis_cnt or seq_mis_cnt or trg_mis_cnt:
        if ctx.fixup_enabled:
            next_steps.append(f"审核并执行 {ctx.fixup_dir} 中的修补脚本。")
        else:
            next_steps.append("如需自动生成修补脚本，请设置 generate_fixup=true。")
    if missing_count:
        next_steps.append("将 main_reports/tables_views_miss 下的 schema_T.txt / schema_V.txt 规则提供给 OMS 进行迁移。")
    if blacklist_missing_cnt:
        next_steps.append("查看 main_reports/blacklist_tables.txt，确认黑名单表处理方案。")
    if trigger_summary.get("invalid_entries") or trigger_summary.get("not_found"):
        next_steps.append("修正 trigger_list 清单内容后重新运行。")
    if dep_missing_cnt or dep_unexpected_cnt:
        next_steps.append("根据依赖差异报告补齐编译或授权。")
    if comment_mis_cnt:
        next_steps.append("确认注释差异是否需要修复。")
    if ctx.enable_grant_generation and ctx.fixup_enabled:
        next_steps.append(
            "审核 {miss} 中的授权脚本（全量审计见 {all}）。".format(
                miss=Path(ctx.fixup_dir) / 'grants_miss',
                all=Path(ctx.fixup_dir) / 'grants_all'
            )
        )

    return RunSummary(
        start_time=ctx.start_time,
        end_time=end_time,
        total_seconds=total_seconds,
        phases=phases,
        actions_done=actions_done,
        actions_skipped=actions_skipped,
        findings=findings,
        attention=attention,
        next_steps=next_steps
    )


def render_run_summary_panel(summary: RunSummary, width: int) -> Panel:
    def render_section(title: str, items: List[str], empty_text: str = "无") -> str:
        lines = [f"[bold]{title}[/bold]"]
        if not items:
            lines.append(f"- {empty_text}")
        else:
            lines.extend([f"- {item}" for item in items])
        return "\n".join(lines)

    phase_lines: List[str] = []
    for phase in summary.phases:
        if phase.duration is not None:
            phase_lines.append(f"- {phase.name}: {format_duration(phase.duration)}")
        else:
            phase_lines.append(f"- {phase.name}: 跳过 ({phase.status})")

    overview = "\n".join([
        "[bold]运行概览[/bold]",
        f"- 开始时间: {summary.start_time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 结束时间: {summary.end_time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 总耗时: {format_duration(summary.total_seconds)}"
    ])
    phases = "\n".join(["[bold]阶段耗时[/bold]"] + phase_lines)
    actions = render_section("本次执行", summary.actions_done, empty_text="无已执行动作")
    skipped = render_section("本次未执行", summary.actions_skipped, empty_text="无")
    findings = render_section("关键发现", summary.findings, empty_text="无")
    attention = render_section("需要注意", summary.attention, empty_text="无")
    next_steps = render_section("下一步建议", summary.next_steps, empty_text="无")

    text = "\n\n".join([overview, phases, actions, skipped, findings, attention, next_steps])
    return Panel.fit(text, title="[info]运行总结", border_style="info", width=width)


def log_run_summary(summary: RunSummary) -> None:
    log_section("运行总结")
    log.info("开始时间: %s", summary.start_time.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("结束时间: %s", summary.end_time.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("总耗时: %s", format_duration(summary.total_seconds))

    log_subsection("阶段耗时")
    for phase in summary.phases:
        if phase.duration is not None:
            log.info("%s: %s", phase.name, format_duration(phase.duration))
        else:
            log.info("%s: 跳过 (%s)", phase.name, phase.status)

    log_subsection("本次执行")
    for item in summary.actions_done:
        log.info("完成: %s", item)
    for item in summary.actions_skipped:
        log.info("跳过: %s", item)

    log_subsection("关键发现")
    for item in summary.findings:
        log.info("%s", item)

    log_subsection("需要注意")
    if summary.attention:
        for item in summary.attention:
            log.info("%s", item)
    else:
        log.info("无")

    log_subsection("下一步建议")
    if summary.next_steps:
        for item in summary.next_steps:
            log.info("%s", item)
    else:
        log.info("无")

def print_final_report(
    tv_results: ReportResults,
    total_checked: int,
    extra_results: Optional[ExtraCheckResults] = None,
    comment_results: Optional[Dict[str, object]] = None,
    dependency_report: Optional[DependencyReport] = None,
    report_file: Optional[Path] = None,
    object_counts_summary: Optional[ObjectCountSummary] = None,
    endpoint_info: Optional[Dict[str, Dict[str, str]]] = None,
    schema_summary: Optional[Dict[str, List[str]]] = None,
    settings: Optional[Dict] = None,
    blacklisted_missing_tables: Optional[BlacklistTableMap] = None,
    blacklist_report_rows: Optional[List[BlacklistReportRow]] = None,
    trigger_list_summary: Optional[Dict[str, object]] = None,
    trigger_list_rows: Optional[List[TriggerListReportRow]] = None,
    package_results: Optional[PackageCompareResults] = None,
    run_summary_ctx: Optional[RunSummaryContext] = None,
    filtered_grants: Optional[List[FilteredGrantEntry]] = None
):
    custom_theme = Theme({
        "ok": "green",
        "missing": "red",
        "mismatch": "yellow",
        "info": "cyan",
        "header": "bold magenta",
        "title": "bold white on blue"
    })
    # 从配置读取报告宽度，避免在nohup等非交互式环境下被截断为80
    if settings:
        try:
            report_width = int(settings.get('report_width', 160))
        except (TypeError, ValueError):
            report_width = 160
    else:
        report_width = 160
    console = Console(theme=custom_theme, record=report_file is not None, width=report_width)

    if extra_results is None:
        extra_results = {
            "index_ok": [], "index_mismatched": [], "constraint_ok": [],
            "constraint_mismatched": [], "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
    if comment_results is None:
        comment_results = {
            "ok": [],
            "mismatched": [],
            "skipped_reason": "未执行注释比对。"
        }
    if dependency_report is None:
        dependency_report = {
            "missing": [],
            "unexpected": [],
            "skipped": []
        }
    if schema_summary is None:
        schema_summary = {
            "source_missing": [],
            "target_missing": [],
            "target_extra": []
        }

    log.info("所有校验已完成。正在生成最终报告...")

    ok_count = len(tv_results['ok'])
    missing_count = len(tv_results['missing'])
    mismatched_count = len(tv_results['mismatched'])
    skipped_count = len(tv_results.get('skipped', []))
    remap_conflicts = tv_results.get('remap_conflicts', [])
    remap_conflict_cnt = len(remap_conflicts)
    extraneous_count = len(tv_results['extraneous'])
    idx_ok_cnt = len(extra_results.get("index_ok", []))
    idx_mis_cnt = len(extra_results.get("index_mismatched", []))
    cons_ok_cnt = len(extra_results.get("constraint_ok", []))
    cons_mis_cnt = len(extra_results.get("constraint_mismatched", []))
    seq_ok_cnt = len(extra_results.get("sequence_ok", []))
    seq_mis_cnt = len(extra_results.get("sequence_mismatched", []))
    trg_ok_cnt = len(extra_results.get("trigger_ok", []))
    trg_mis_cnt = len(extra_results.get("trigger_mismatched", []))
    comment_ok_cnt = len(comment_results.get("ok", []))
    comment_mis_cnt = len(comment_results.get("mismatched", []))
    comment_skip_reason = comment_results.get("skipped_reason")
    extra_target_cnt = len(tv_results.get('extra_targets', []))
    dep_missing_cnt = len(dependency_report.get("missing", []))
    dep_unexpected_cnt = len(dependency_report.get("unexpected", []))
    dep_skipped_cnt = len(dependency_report.get("skipped", []))
    source_missing_schema_cnt = len(schema_summary.get("source_missing", []))
    package_rows: List[PackageCompareRow] = []
    package_diff_rows: List[PackageCompareRow] = []
    package_summary: Dict[str, int] = {}
    if package_results:
        package_rows = list(package_results.get("rows") or [])
        package_diff_rows = list(package_results.get("diff_rows") or [])
        def _package_sort_key(row: PackageCompareRow) -> Tuple[str, str, int, str, str]:
            parsed = parse_full_object_name(row.src_full) or parse_full_object_name(row.tgt_full)
            owner, name = parsed if parsed else ("", "")
            type_u = (row.obj_type or "").upper()
            type_rank = 0 if type_u == "PACKAGE" else 1 if type_u == "PACKAGE BODY" else 2
            return (owner, name, type_rank, row.src_full, row.tgt_full)
        if package_diff_rows:
            package_diff_rows = sorted(package_diff_rows, key=_package_sort_key)
        package_summary = dict(package_results.get("summary") or {})
    package_missing_cnt = int(package_summary.get("MISSING_TARGET", 0) or 0)
    package_src_invalid_cnt = int(package_summary.get("SOURCE_INVALID", 0) or 0)
    package_tgt_invalid_cnt = int(package_summary.get("TARGET_INVALID", 0) or 0)
    package_status_mismatch_cnt = int(package_summary.get("STATUS_MISMATCH", 0) or 0)

    console.print(Panel.fit(f"[bold]数据库对象迁移校验报告 (V{__version__} - Rich)[/bold]", style="title"))
    console.print(f"[info]项目主页: {REPO_URL} | 问题反馈: {REPO_ISSUES_URL}[/info]")
    console.print("")

    section_width = 140
    count_table_kwargs: Dict[str, object] = {"width": section_width, "expand": False}
    TYPE_COL_WIDTH = 16
    OBJECT_COL_WIDTH = 42
    DETAIL_COL_WIDTH = 90

    def format_endpoint_block(info: Dict[str, str], is_oracle: bool) -> str:
        lines: List[str] = []
        if not info:
            return "无可用信息"
        if info.get("version"):
            lines.append(f"版本: {info['version']}")
        if is_oracle:
            if info.get("cdb_mode"):
                lines.append(f"CDB/PDB: {info['cdb_mode']}")
            if info.get("container"):
                lines.append(f"容器: {info['container']}")
            if info.get("service_name"):
                lines.append(f"服务名: {info['service_name']}")
        else:
            if info.get("current_database"):
                lines.append(f"当前库: {info['current_database']}")
            if info.get("connection_id"):
                lines.append(f"连接 ID: {info['connection_id']}")
            if info.get("ssl"):
                lines.append(f"SSL: {info['ssl']}")
        host = info.get("host")
        port = info.get("port")
        if host or port:
            lines.append(f"地址: {host or ''}:{port or ''}")
        user_label = "连接用户" if is_oracle else "当前用户"
        user_value = info.get("current_user") or info.get("user") or info.get("configured_user", "")
        if is_oracle and user_value:
            user_value = str(user_value).upper()
        lines.append(f"{user_label}: {user_value}")
        if is_oracle and info.get("dsn"):
            lines.append(f"DSN: {info['dsn']}")
        return "\n".join([line for line in lines if line.strip()]) or "无可用信息"

    if endpoint_info:
        src_info = endpoint_info.get("oracle", {})
        tgt_info = endpoint_info.get("oceanbase", {})
        env_table = Table(title="[header]源/目标环境", width=section_width)
        env_table.add_column("源 (Oracle)", width=section_width // 2)
        env_table.add_column("目标 (OceanBase)", width=section_width // 2)
        env_table.add_row(
            format_endpoint_block(src_info, True),
            format_endpoint_block(tgt_info, False)
        )
        console.print(env_table)
        console.print("")

    # --- 综合概要 ---
    summary_table = Table(
        title="[header]综合概要",
        show_header=False,
        box=None,
        width=section_width,
        pad_edge=False,
        padding=(0, 1)
    )
    summary_table.add_column("Category", justify="left", width=24, no_wrap=True)
    summary_table.add_column("Details", justify="left", width=section_width - 28)

    schema_text = Text()
    schema_text.append("源 schema 未获取到对象: ", style="mismatch")
    schema_text.append(f"{source_missing_schema_cnt}")
    summary_table.add_row("[bold]Schema 覆盖[/bold]", schema_text)

    blacklist_missing_cnt = len(blacklisted_missing_tables or {})
    primary_text = Text()
    primary_text.append(f"总计校验对象 (来自源库): {total_checked}\n")
    primary_text.append("一致: ", style="ok")
    primary_text.append(f"{ok_count}\n")
    primary_text.append("缺失: ", style="missing")
    primary_text.append(f"{missing_count}\n")
    primary_text.append("不匹配 (表列/长度): ", style="mismatch")
    primary_text.append(f"{mismatched_count}\n")
    primary_text.append("多余: ", style="mismatch")
    primary_text.append(f"{extra_target_cnt}\n")
    if skipped_count:
        primary_text.append("仅打印: ", style="info")
        primary_text.append(f"{skipped_count}\n")
    primary_text.append("无效规则: ", style="mismatch")
    primary_text.append(f"{extraneous_count}")
    if remap_conflict_cnt:
        primary_text.append("\n")
        primary_text.append("无法推导: ", style="mismatch")
        primary_text.append(f"{remap_conflict_cnt}")
    summary_table.add_row("[bold]主对象 (TABLE/VIEW/etc.)[/bold]", primary_text)

    if package_rows:
        pkg_text = Text()
        pkg_text.append("源端无效: ", style="mismatch")
        pkg_text.append(f"{package_src_invalid_cnt}\n")
        pkg_text.append("目标缺失: ", style="missing")
        pkg_text.append(f"{package_missing_cnt}\n")
        pkg_text.append("目标无效: ", style="mismatch")
        pkg_text.append(f"{package_tgt_invalid_cnt}\n")
        pkg_text.append("状态不一致: ", style="mismatch")
        pkg_text.append(f"{package_status_mismatch_cnt}")
        if report_file:
            pkg_report_hint = derive_package_report_path(Path(report_file))
            pkg_text.append("\n")
            pkg_text.append(f"详见: {pkg_report_hint.name}", style="info")
        summary_table.add_row("[bold]PACKAGE/PKG BODY[/bold]", pkg_text)

    comment_text = Text()
    if comment_skip_reason:
        comment_text.append(str(comment_skip_reason), style="info")
    else:
        comment_text.append("一致: ", style="ok")
        comment_text.append(f"{comment_ok_cnt}\n")
        comment_text.append("差异: ", style="mismatch")
        comment_text.append(f"{comment_mis_cnt}")
    summary_table.add_row("[bold]注释一致性[/bold]", comment_text)

    ext_text = Text()
    ext_text.append("索引: ", style="info")
    ext_text.append(f"一致 {idx_ok_cnt} / ", style="ok")
    ext_text.append(f"差异 {idx_mis_cnt}\n", style="mismatch")
    ext_text.append("约束: ", style="info")
    ext_text.append(f"一致 {cons_ok_cnt} / ", style="ok")
    ext_text.append(f"差异 {cons_mis_cnt}\n", style="mismatch")
    ext_text.append("序列: ", style="info")
    ext_text.append(f"一致 {seq_ok_cnt} / ", style="ok")
    ext_text.append(f"差异 {seq_mis_cnt}\n", style="mismatch")
    ext_text.append("触发器: ", style="info")
    ext_text.append(f"一致 {trg_ok_cnt} / ", style="ok")
    ext_text.append(f"差异 {trg_mis_cnt}", style="mismatch")
    summary_table.add_row("[bold]扩展对象 (INDEX/SEQ/etc.)[/bold]", ext_text)

    if trigger_list_summary and trigger_list_summary.get("enabled"):
        filter_text = Text()
        if trigger_list_summary.get("error"):
            filter_text.append(
                f"trigger_list 读取失败: {trigger_list_summary.get('error')} (已回退全量触发器)",
                style="mismatch"
            )
        elif trigger_list_summary.get("check_disabled"):
            filter_text.append("TRIGGER 未启用检查，清单仅做格式校验。", style="mismatch")
        elif trigger_list_summary.get("fallback_full"):
            filter_text.append("清单为空或无有效条目，已回退全量触发器。", style="info")
        else:
            filter_text.append("列表: ", style="info")
            filter_text.append(str(trigger_list_summary.get("valid_entries", 0)))
            filter_text.append("  命中缺失: ", style="info")
            filter_text.append(str(trigger_list_summary.get("selected_missing", 0)), style="ok")
            filter_text.append("  未列出缺失: ", style="info")
            filter_text.append(str(trigger_list_summary.get("missing_not_listed", 0)), style="mismatch")
            invalid_cnt = trigger_list_summary.get("invalid_entries", 0) or 0
            not_found_cnt = trigger_list_summary.get("not_found", 0) or 0
            not_missing_cnt = trigger_list_summary.get("not_missing", 0) or 0
            if invalid_cnt:
                filter_text.append("  无效: ", style="mismatch")
                filter_text.append(str(invalid_cnt), style="mismatch")
            if not_found_cnt:
                filter_text.append("  未找到: ", style="mismatch")
                filter_text.append(str(not_found_cnt), style="mismatch")
            if not_missing_cnt:
                filter_text.append("  非缺失: ", style="info")
                filter_text.append(str(not_missing_cnt), style="info")
        summary_table.add_row("[bold]触发器筛选[/bold]", filter_text)

    dep_text = Text()
    dep_text.append("缺失依赖: ", style="missing")
    dep_text.append(f"{dep_missing_cnt}  ")
    dep_text.append("额外依赖: ", style="mismatch")
    dep_text.append(f"{dep_unexpected_cnt}  ")
    dep_text.append("跳过: ", style="info")
    dep_text.append(f"{dep_skipped_cnt}")
    summary_table.add_row("[bold]依赖关系[/bold]", dep_text)

    console.print(summary_table)
    console.print("")
    console.print("")

    def summarize_actions() -> Panel:
        modify_counts = OrderedDict()
        modify_counts["TABLE (列差异修补)"] = len(tv_results.get('mismatched', []))

        addition_counts: Dict[str, int] = defaultdict(int)
        for obj_type, _, _ in tv_results.get('missing', []):
            addition_counts[obj_type.upper()] += 1
        for item in extra_results.get("index_mismatched", []):
            addition_counts["INDEX"] += len(item.missing_indexes)
        for item in extra_results.get("constraint_mismatched", []):
            addition_counts["CONSTRAINT"] += len(item.missing_constraints)
        for item in extra_results.get("sequence_mismatched", []):
            addition_counts["SEQUENCE"] += len(item.missing_sequences)
        for item in extra_results.get("trigger_mismatched", []):
            addition_counts["TRIGGER"] += len(item.missing_triggers)
        if trigger_list_summary and trigger_list_summary.get("enabled"):
            if not trigger_list_summary.get("fallback_full") and not trigger_list_summary.get("error"):
                addition_counts["TRIGGER"] = int(trigger_list_summary.get("selected_missing", 0) or 0)

        def format_block(title: str, data: OrderedDict) -> str:
            lines = [f"[bold]{title}[/bold]"]
            entries = [(k, v) for k, v in data.items() if v > 0]
            if not entries:
                lines.append("  - 无")
            else:
                for k, v in entries:
                    lines.append(f"  - {k}: {v}")
            return "\n".join(lines)

        def format_add_block(title: str, data_map: Dict[str, int]) -> str:
            lines = [f"[bold]{title}[/bold]"]
            entries = [(k, v) for k, v in sorted(data_map.items()) if v > 0]
            if not entries:
                lines.append("  - 无")
            else:
                for k, v in entries:
                    lines.append(f"  - {k}: {v}")
            return "\n".join(lines)

        text = "\n\n".join([
            format_block("需要在目标端修改的对象", modify_counts),
            format_add_block("需要在目标端新增的对象", addition_counts)
        ])
        return Panel.fit(text, title="[info]执行摘要", border_style="info", width=section_width)

    console.print(summarize_actions())

    if schema_summary:
        schema_table = Table(title="[header]0.a Schema 覆盖详情", width=section_width)
        schema_table.add_column("类别", style="info", width=36)
        schema_table.add_column("Schema 列表", style="info")
        has_row = False
        if schema_summary.get("source_missing"):
            schema_table.add_row(
                "源端未获取到对象",
                ", ".join(schema_summary["source_missing"])
            )
            has_row = True
        if has_row:
            console.print(schema_table)

    if object_counts_summary:
        count_table = Table(title="[header]0.b 检查汇总", **count_table_kwargs)
        count_table.add_column("对象类型", style="info", width=TYPE_COL_WIDTH)
        count_table.add_column("Oracle (应校验)", justify="right", width=18)
        count_table.add_column("OceanBase (命中)", justify="right", width=18)
        count_table.add_column("缺失", justify="right", width=8)
        count_table.add_column("多余", justify="right", width=8)
        oracle_counts = dict(object_counts_summary.get("oracle", {}))
        ob_counts = dict(object_counts_summary.get("oceanbase", {}))
        missing_counts = dict(object_counts_summary.get("missing", {}))
        extra_counts = dict(object_counts_summary.get("extra", {}))
        if blacklist_missing_cnt:
            table_missing = missing_counts.get("TABLE", 0)
            missing_counts["TABLE"] = max(0, table_missing - blacklist_missing_cnt)
            oracle_counts["TABLE (BLACKLIST)"] = blacklist_missing_cnt
            ob_counts["TABLE (BLACKLIST)"] = 0
            missing_counts["TABLE (BLACKLIST)"] = blacklist_missing_cnt
            extra_counts["TABLE (BLACKLIST)"] = 0
        count_types = sorted(set(oracle_counts) | set(ob_counts) | set(missing_counts) | set(extra_counts))
        for obj_type in count_types:
            ora_val = oracle_counts.get(obj_type, 0)
            ob_val = ob_counts.get(obj_type, 0)
            miss_val = missing_counts.get(obj_type, 0)
            extra_val = extra_counts.get(obj_type, 0)
            count_table.add_row(
                obj_type,
                str(ora_val),
                str(ob_val),
                f"[missing]{miss_val}[/missing]" if miss_val else "0",
                f"[mismatch]{extra_val}[/mismatch]" if extra_val else "0"
            )
        console.print(count_table)

    # --- 1. 缺失的主对象 ---
    if tv_results['missing']:
        table = Table(title=f"[header]1. 缺失的主对象 (共 {missing_count} 个) — 按目标 schema 分组[/header]", width=section_width)
        SCHEMA_COL_WIDTH = 18
        table.add_column("目标 Schema", style="info", width=SCHEMA_COL_WIDTH)
        table.add_column("类型", style="info", width=TYPE_COL_WIDTH)
        table.add_column("缺失对象 (源名[=目标名])", style="info")

        grouped_missing: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
        for obj_type, tgt_name, src_name in tv_results['missing']:
            tgt_schema = tgt_name.split('.', 1)[0] if '.' in tgt_name else tgt_name
            grouped_missing[tgt_schema.upper()].append((obj_type, tgt_name, src_name))

        grouped_items = sorted(grouped_missing.items())
        for tgt_schema, items in grouped_items:
            sorted_items = sorted(items, key=lambda x: (x[0], x[1], x[2]))
            for idx, (obj_type, tgt_name, src_name) in enumerate(sorted_items):
                table.add_row(
                    tgt_schema if idx == 0 else "",
                    f"[{obj_type}]",
                    format_missing_mapping(src_name, tgt_name),
                    end_section=(idx == len(sorted_items) - 1)
                )
        console.print(table)

    if tv_results.get('extra_targets'):
        extra_target_count = len(tv_results['extra_targets'])
        table = Table(title=f"[header]1.b 目标端多出的对象 (共 {extra_target_count} 个)", width=section_width)
        table.add_column("类型", style="info", width=TYPE_COL_WIDTH)
        table.add_column("目标对象(多余)", style="info")
        for obj_type, tgt_name in tv_results['extra_targets']:
            table.add_row(f"[{obj_type}]", tgt_name)
        console.print(table)

    if tv_results.get('skipped'):
        skipped_items = tv_results['skipped']
        table = Table(title=f"[header]1.c 仅打印未校验的主对象 (共 {len(skipped_items)} 个)", width=section_width)
        table.add_column("类型", style="info", width=TYPE_COL_WIDTH)
        table.add_column("对象 (源名[=目标名])", style="info", width=OBJECT_COL_WIDTH)
        table.add_column("原因", style="info", width=DETAIL_COL_WIDTH)
        for obj_type, tgt_name, src_name, reason in skipped_items:
            table.add_row(
                f"[{obj_type}]",
                format_missing_mapping(src_name, tgt_name),
                reason or ""
            )
        console.print(table)

    if package_diff_rows:
        table = Table(
            title=f"[header]1.d PACKAGE/PKG BODY 差异 (共 {len(package_diff_rows)} 个)",
            width=section_width
        )
        table.add_column("类型", style="info", width=TYPE_COL_WIDTH)
        table.add_column("对象 (源名[=目标名])", style="info", width=OBJECT_COL_WIDTH)
        table.add_column("源状态", style="info", width=12)
        table.add_column("目标状态", style="info", width=12)
        table.add_column("结果", style="info", width=16)
        table.add_column("错误摘要", style="info", width=DETAIL_COL_WIDTH)
        for row in package_diff_rows:
            error_hint = "-"
            if row.error_count:
                error_hint = f"{row.error_count} | {row.first_error}" if row.first_error else str(row.error_count)
            table.add_row(
                f"[{row.obj_type}]",
                format_missing_mapping(row.src_full, row.tgt_full),
                row.src_status,
                row.tgt_status,
                row.result,
                error_hint
            )
        console.print(table)

    if remap_conflicts:
        table = Table(title=f"[header]1.e 无法自动推导的对象 (共 {remap_conflict_cnt} 个)", width=section_width)
        table.add_column("类型", style="info", width=TYPE_COL_WIDTH)
        table.add_column("对象 (源端)", style="info", width=OBJECT_COL_WIDTH)
        table.add_column("原因", style="info", width=DETAIL_COL_WIDTH)
        for obj_type, src_name, reason in remap_conflicts:
            table.add_row(
                f"[{obj_type}]",
                src_name,
                reason or ""
            )
        console.print(table)

    # --- 2. 列不匹配的表 ---
    if tv_results['mismatched']:
        table = Table(title=f"[header]2. 不匹配的表 (共 {mismatched_count} 个)", width=section_width)
        table.add_column("表名", style="info", width=OBJECT_COL_WIDTH)
        table.add_column("差异详情", width=DETAIL_COL_WIDTH)
        for obj_type, tgt_name, missing, extra, length_mismatches, type_mismatches in tv_results['mismatched']:
            details = Text()
            if "获取失败" in tgt_name:
                details.append(f"源端列信息获取失败", style="missing")
            else:
                if missing:
                    details.append(f"- 缺失列: {sorted(list(missing))}\n", style="missing")
                if extra:
                    details.append(f"+ 多余列: {sorted(list(extra))}\n", style="mismatch")
                if type_mismatches:
                    details.append("* 类型不匹配 (LONG/LONG RAW):\n", style="mismatch")
                    for issue in type_mismatches:
                        col, src_type, tgt_type, expected_type = issue
                        details.append(
                            f"    - {col}: 源={src_type}, 目标={tgt_type}, 期望={expected_type}\n"
                        )
                if length_mismatches:
                    details.append("* 长度不匹配 (VARCHAR/2):\n", style="mismatch")
                    for issue in length_mismatches:
                        col, src_len, tgt_len, limit_len, issue_type = issue
                        if issue_type == 'char_mismatch':
                            details.append(
                                f"    - {col}: 源={src_len} CHAR, 目标={tgt_len}, 要求一致\n"
                            )
                        elif issue_type == 'short':
                            details.append(
                                f"    - {col}: 源={src_len} BYTE, 目标={tgt_len}, 期望下限={limit_len}\n"
                            )
                        else:
                            details.append(
                                f"    - {col}: 源={src_len} BYTE, 目标={tgt_len}, 上限允许={limit_len}\n"
                            )
            table.add_row(tgt_name, details)
        console.print(table)

    comment_mismatches = comment_results.get("mismatched", [])
    if comment_skip_reason:
        console.print(Panel.fit(str(comment_skip_reason), style="info", width=section_width))
    if comment_mismatches:
        table = Table(title=f"[header]3. 表/列注释一致性检查 (共 {len(comment_mismatches)} 张表差异)", width=section_width)
        table.add_column("表名", style="info", width=OBJECT_COL_WIDTH)
        table.add_column("差异详情", width=DETAIL_COL_WIDTH)
        for item in comment_mismatches:
            details = Text()
            if item.table_comment:
                src_cmt, tgt_cmt = item.table_comment
                details.append(
                    f"* 表注释不一致: src={shorten_comment_preview(src_cmt)}, "
                    f"tgt={shorten_comment_preview(tgt_cmt)}\n",
                    style="mismatch"
                )
            if item.missing_columns:
                details.append(f"- 缺失列注释: {sorted(item.missing_columns)}\n", style="missing")
            if item.extra_columns:
                details.append(f"+ 额外列注释: {sorted(item.extra_columns)}\n", style="mismatch")
            for col, src_cmt, tgt_cmt in item.column_comment_diffs:
                details.append(
                    f"  - {col}: src={shorten_comment_preview(src_cmt)}, "
                    f"tgt={shorten_comment_preview(tgt_cmt)}\n"
                )
            table.add_row(item.table, details)
        console.print(table)

    def render_missing_mapping_lines(mappings: List[Tuple[str, str]]) -> str:
        """格式化缺失对象的映射行，remap 时显示 src=tgt。"""
        formatted = [format_missing_mapping(src, tgt) for src, tgt in mappings]
        return "\n".join([item for item in formatted if item])

    def build_missing_text(
        mappings: List[Tuple[str, str]],
        has_missing: bool,
        include_header: bool = True
    ) -> Text:
        """根据缺失映射构造 Text，按需添加“- 缺失:”标题。"""
        if not has_missing:
            return Text("")
        lines = render_missing_mapping_lines(mappings)
        if not lines:
            return Text("")
        header = Text("- 缺失:\n", style="missing") if include_header else Text("")
        return header + Text(lines + "\n", style="missing")

    # --- 3. 扩展对象差异 ---
    def print_ext_mismatch_table(title, items, headers, render_func):
        if not items:
            return
        table = Table(title=f"[header]{title} (共 {len(items)} 项差异)", width=section_width)
        table.add_column(headers[0], style="info", width=OBJECT_COL_WIDTH)
        table.add_column(headers[1], width=DETAIL_COL_WIDTH)
        for item in items:
            table.add_row(*render_func(item))
        console.print(table)

    print_ext_mismatch_table(
        "5. 索引一致性检查", extra_results["index_mismatched"], ["表名", "差异详情"],
        lambda item: (
            Text(item.table),
            Text(f"- 缺失: {sorted(item.missing_indexes)}\n" if item.missing_indexes else "", style="missing") +
            Text(f"+ 多余: {sorted(item.extra_indexes)}\n" if item.extra_indexes else "", style="mismatch") +
            Text('\n'.join([f"* {d}" for d in item.detail_mismatch]))
        )
    )
    print_ext_mismatch_table(
        "6. 约束 (PK/UK/FK) 一致性检查", extra_results["constraint_mismatched"], ["表名", "差异详情"],
        lambda item: (
            Text(item.table),
            Text(f"- 缺失: {sorted(item.missing_constraints)}\n" if item.missing_constraints else "", style="missing") +
            Text(f"+ 多余: {sorted(item.extra_constraints)}\n" if item.extra_constraints else "", style="mismatch") +
            Text('\n'.join([f"* {d}" for d in item.detail_mismatch]))
        )
    )
    print_ext_mismatch_table(
        "7. 序列 (SEQUENCE) 一致性检查", extra_results["sequence_mismatched"], ["Schema 映射", "差异详情"],
        lambda item: (
            Text(f"{item.src_schema}->{item.tgt_schema}"),
            build_missing_text(item.missing_mappings or [], bool(item.missing_sequences))
            + (
                Text(f"+ 多余: {sorted(item.extra_sequences)}\n", style="mismatch")
                if item.extra_sequences else Text("")
            )
            + (Text(f"* {item.note}\n", style="missing") if item.note else Text(""))
        )
    )
    print_ext_mismatch_table(
        "8. 触发器 (TRIGGER) 一致性检查", extra_results["trigger_mismatched"], ["表名", "差异详情"],
        lambda item: (
            Text(item.table),
            build_missing_text(item.missing_mappings or [], bool(item.missing_triggers))
            + (
                Text(f"+ 多余: {sorted(item.extra_triggers)}\n", style="mismatch")
                if item.extra_triggers else Text("")
            )
            + Text('\n'.join([f"* {d}" for d in item.detail_mismatch]))
        )
    )

    dep_total = dep_missing_cnt + dep_unexpected_cnt + dep_skipped_cnt
    if dep_total:
        dep_table = Table(title=f"[header]9. 依赖关系校验 (共 {dep_total} 项)", width=section_width)
        dep_table.add_column("类别", style="info", width=12)
        dep_table.add_column("依赖对象", style="info", width=OBJECT_COL_WIDTH)
        dep_table.add_column("依赖类型", style="info", width=TYPE_COL_WIDTH)
        dep_table.add_column("被依赖对象", style="info", width=OBJECT_COL_WIDTH)
        dep_table.add_column("被依赖类型", style="info", width=TYPE_COL_WIDTH)
        dep_table.add_column("修复建议", width=DETAIL_COL_WIDTH)

        def render_dep_rows(label: str, entries: List[DependencyIssue], style: str) -> None:
            for issue in entries:
                dep_table.add_row(
                    f"[{style}]{label}[/{style}]",
                    issue.dependent,
                    issue.dependent_type,
                    issue.referenced,
                    issue.referenced_type,
                    issue.reason
                )

        render_dep_rows("缺失", dependency_report.get("missing", []), "missing")
        render_dep_rows("额外", dependency_report.get("unexpected", []), "mismatch")
        render_dep_rows("跳过", dependency_report.get("skipped", []), "info")
        console.print(dep_table)

    # --- 4. 无效 Remap 规则 ---
    if tv_results['extraneous']:
        table = Table(title=f"[header]4. 无效的 Remap 规则 (共 {extraneous_count} 个)", width=section_width)
        table.add_column("在 remap_rules.txt 中定义, 但在源端 Oracle 中未找到的对象", style="info", width=section_width - 6)
        for item in tv_results['extraneous']:
            table.add_row(item, style="mismatch")
        console.print(table)

    # --- 提示 ---
    fixup_panel = Panel.fit(
        "[bold]Fixup 脚本生成目录[/bold]\n\n"
        "fixup_scripts/table         : 缺失 TABLE 的 CREATE 脚本\n"
        "fixup_scripts/view          : 缺失 VIEW 的 CREATE 脚本\n"
        "fixup_scripts/materialized_view : MATERIALIZED VIEW 默认仅打印不生成\n"
        "fixup_scripts/procedure     : 缺失 PROCEDURE 的 CREATE 脚本\n"
        "fixup_scripts/function      : 缺失 FUNCTION 的 CREATE 脚本\n"
        "fixup_scripts/package       : 缺失 PACKAGE 的 CREATE 脚本\n"
        "fixup_scripts/package_body  : 缺失 PACKAGE BODY 的 CREATE 脚本\n"
        "fixup_scripts/synonym       : 缺失 SYNONYM 的 CREATE 脚本\n"
        "fixup_scripts/job           : 缺失 JOB 的 CREATE 脚本\n"
        "fixup_scripts/schedule      : 缺失 SCHEDULE 的 CREATE 脚本\n"
        "fixup_scripts/type          : 缺失 TYPE 的 CREATE 脚本\n"
        "fixup_scripts/type_body     : 缺失 TYPE BODY 的 CREATE 脚本\n"
        "fixup_scripts/index         : 缺失 INDEX 的 CREATE 脚本\n"
        "fixup_scripts/constraint    : 缺失约束的 CREATE 脚本\n"
        "fixup_scripts/sequence      : 缺失 SEQUENCE 的 CREATE 脚本\n"
        "fixup_scripts/trigger       : 缺失 TRIGGER 的 CREATE 脚本\n"
        "fixup_scripts/compile       : 依赖重编译脚本 (ALTER ... COMPILE)\n"
        "fixup_scripts/grants_miss   : 缺失授权脚本 (对象/角色/系统)\n"
        "fixup_scripts/grants_all    : 全量授权脚本 (对象/角色/系统)\n"
        "fixup_scripts/table_alter   : 列不匹配 TABLE 的 ALTER 修补脚本\n\n"
        "[bold]请在 OceanBase 执行前逐一人工审核上述脚本。[/bold]",
        title="[info]提示",
        border_style="info"
    )
    console.print(fixup_panel)
    run_summary: Optional[RunSummary] = None
    if run_summary_ctx:
        run_summary_ctx.phase_durations["报告输出"] = time.perf_counter() - run_summary_ctx.report_start_perf
        filtered_grants_count = len(filtered_grants or [])
        filtered_grants_path = None
        if filtered_grants_count and report_file:
            filtered_grants_path = Path(report_file).parent / "filtered_grants.txt"
        run_summary = build_run_summary(
            run_summary_ctx,
            tv_results,
            extra_results,
            comment_results,
            dependency_report,
            remap_conflicts,
            tv_results.get("extraneous", []),
            blacklisted_missing_tables,
            report_file,
            filtered_grants_path=filtered_grants_path,
            filtered_grants_count=filtered_grants_count
        )
        console.print(render_run_summary_panel(run_summary, section_width))

    console.print(Panel.fit("[bold]报告结束[/bold]", style="title"))

    if report_file:
        report_path = Path(report_file)
        blacklisted_table_keys = set((blacklisted_missing_tables or {}).keys())
        export_dir = export_missing_table_view_mappings(
            tv_results,
            report_path.parent,
            blacklisted_tables=blacklisted_table_keys
        )
        package_report_path = None
        if package_rows:
            package_report_path = export_package_compare_report(
                package_rows,
                derive_package_report_path(report_path)
            )
        blacklist_path = export_blacklist_tables(blacklist_report_rows or [], report_path.parent)
        trigger_miss_path = None
        if trigger_list_summary and trigger_list_summary.get("enabled"):
            trigger_miss_path = export_trigger_miss_report(
                trigger_list_rows or [],
                trigger_list_summary,
                report_path.parent
            )
        filtered_grants_path = export_filtered_grants(
            filtered_grants or [],
            report_path.parent
        )
        try:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_text = console.export_text(clear=False)
            # 生成便于 vi/less 查看且无颜色/框线的纯文本报告
            plain_text = strip_ansi_text(report_text).translate(BOX_ASCII_TRANS)
            report_path.write_text(plain_text, encoding='utf-8')
            console.print(f"[info]报告已保存(纯文本): {report_path}")
            if export_dir:
                log.info("缺失 TABLE/VIEW 映射已输出到: %s", export_dir)
            if package_report_path:
                log.info("PACKAGE 对比明细已输出到: %s", package_report_path)
            if blacklist_path:
                log.info("黑名单表清单已输出到: %s", blacklist_path)
            if trigger_miss_path:
                log.info("触发器清单筛选报告已输出到: %s", trigger_miss_path)
            if filtered_grants_path:
                log.info("过滤授权清单已输出到: %s", filtered_grants_path)
        except OSError as exc:
            console.print(f"[missing]报告写入失败: {exc}")

    return run_summary


# ====================== 主函数 ======================

def parse_cli_args() -> argparse.Namespace:
    """解析命令行参数，允许自定义 config.ini 路径并展示功能说明。"""
    desc = textwrap.dedent(
        f"""\
        OceanBase Comparator Toolkit v{__version__}
        - 一次转储，本地对比：Oracle Thick Mode + 少量 obclient 调用，全部比对在内存完成。
        - 覆盖对象：TABLE/VIEW/MVIEW/PLSQL/TYPE/JOB/SCHEDULE + INDEX/CONSTRAINT/SEQUENCE/TRIGGER。
        - 校验规则：表列名集合 + VARCHAR/VARCHAR2 长度窗口 [ceil(1.5x), ceil(2.5x)]；其余对象校验存在性/列组合。
        - 注释校验：基于 DBA_TAB_COMMENTS / DBA_COL_COMMENTS 的表/列注释一致性检查（可通过 check_comments 开关关闭）。
        - 依赖校验：加载 DBA_DEPENDENCIES，映射后对比，缺失则生成 ALTER ... COMPILE。
        - 授权生成：基于 DBA_TAB_PRIVS/DBA_SYS_PRIVS/DBA_ROLE_PRIVS + 依赖推导生成授权脚本。
        - Fix-up 输出：缺失对象 CREATE、表列 ALTER ADD/MODIFY、依赖 COMPILE、授权脚本，按类型落地到 fixup_scripts/*。
        """
    )
    epilog = textwrap.dedent(
        """\
        配置提示 (config.ini):
          [ORACLE_SOURCE] user/password/dsn (Thick Mode)
          [OCEANBASE_TARGET] executable/host/port/user_string/password (obclient)
          [SETTINGS] source_schemas, remap_file, oracle_client_lib_dir, dbcat_*，输出目录等
          可选开关：
            check_primary_types     限制主对象类型（默认全量）
            check_extra_types       限制扩展对象 (index,constraint,sequence,trigger)
            fixup_schemas           仅对指定目标 schema 生成订正 SQL（逗号分隔，留空为全部）
            fixup_types             仅生成指定对象类型的订正 SQL（留空为全部，例如 TABLE,TRIGGER）
            trigger_list            仅生成指定触发器清单 (每行 SCHEMA.TRIGGER_NAME)
            check_dependencies      true/false 控制依赖校验
            generate_grants         true/false 控制授权脚本生成
            grant_tab_privs_scope   owner/owner_or_grantee 控制 DBA_TAB_PRIVS 抽取范围
            grant_merge_privileges  true/false 合并同对象多权限授权
            grant_merge_grantees    true/false 合并同权限多 grantee 授权
            grant_supported_sys_privs    逗号分隔系统权限清单（留空自动探测）
            grant_supported_object_privs 逗号分隔对象权限清单（留空使用默认白名单）
            grant_include_oracle_maintained_roles true/false 是否生成 Oracle 维护角色
            generate_fixup          true/false 控制是否生成脚本

        用法示例:
          python schema_diff_reconciler.py                   # 使用当前目录 config.ini
          python schema_diff_reconciler.py /path/to/conf.ini # 指定配置
        输出:
          main_reports/report_<ts>.txt  Rich 报告文本
          main_reports/package_compare_<ts>.txt  PACKAGE/PKG BODY 对比明细
          main_reports/tables_views_miss/ 按 schema 输出缺失 TABLE/VIEW 规则 (schema_T.txt / schema_V.txt)
          main_reports/blacklist_tables.txt 黑名单表清单 (含 LONG 转换校验状态)
          main_reports/trigger_miss.txt  触发器清单筛选报告 (仅配置 trigger_list 时生成)
          main_reports/filtered_grants.txt 过滤掉的不兼容 GRANT 权限清单
          fixup_scripts/                按类型分类的订正 SQL
        项目信息:
          主页: {repo_url}
          反馈: {issues_url}
        """
    ).format(repo_url=REPO_URL, issues_url=REPO_ISSUES_URL)
    parser = argparse.ArgumentParser(
        description=desc,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "config",
        nargs="?",
        default="config.ini",
        help="config.ini path (default: ./config.ini)",
    )
    parser.add_argument(
        "--wizard",
        action="store_true",
        help="启动交互式配置向导：缺失/无效项时提示输入并写回配置，然后继续运行主流程。",
    )
    return parser.parse_args()


def main():
    """主执行函数"""
    args = parse_cli_args()
    config_file = args.config
    config_path = Path(config_file).resolve()
    run_start_time = datetime.now()
    run_start_perf = time.perf_counter()
    phase_durations: Dict[str, float] = OrderedDict()
    phase_skip_reasons: Dict[str, str] = {}

    if args.wizard:
        run_config_wizard(config_path)

    # 1) 加载配置 + 初始化
    with phase_timer("加载配置与初始化", phase_durations):
        ora_cfg, ob_cfg, settings = load_config(str(config_path))
        # 为本次运行初始化日志文件（尽量早，以便记录后续步骤）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        setup_run_logging(settings, timestamp)
        log_section("启动与配置")
        log.info("配置文件: %s", config_path)
        validate_runtime_paths(settings, ob_cfg)

        log.info("OceanBase Comparator Toolkit v%s", __version__)
        log.info("项目主页: %s (问题反馈: %s)", REPO_URL, REPO_ISSUES_URL)
        enabled_primary_types: Set[str] = set(settings.get('enabled_primary_types') or set(PRIMARY_OBJECT_TYPES))
        enabled_extra_types: Set[str] = set(settings.get('enabled_extra_types') or set(EXTRA_OBJECT_CHECK_TYPES))
        print_only_primary_types = set(PRINT_ONLY_PRIMARY_TYPES)
        print_only_types = enabled_primary_types & print_only_primary_types
        checked_primary_types = enabled_primary_types - print_only_types
        enabled_object_types = enabled_primary_types | enabled_extra_types
        enable_dependencies_check: bool = bool(settings.get('enable_dependencies_check', True))
        enable_comment_check: bool = bool(settings.get('enable_comment_check', True))
        enable_grant_generation: bool = bool(settings.get('enable_grant_generation', True))

        log.info(
            "本次启用的主对象类型: %s",
            ", ".join(sorted(enabled_primary_types))
        )
        if print_only_types:
            log.info(
                "以下主对象类型仅打印不校验: %s",
                ", ".join(sorted(print_only_types))
            )
        log.info(
            "本次启用的扩展校验: %s",
            ", ".join(sorted(enabled_extra_types)) if enabled_extra_types else "<无>"
        )
        if not enable_dependencies_check:
            log.info("已根据配置跳过依赖关系校验。")
        if not enable_comment_check:
            log.info("已根据配置关闭注释一致性校验。")
        if not enable_grant_generation:
            log.info("已根据配置关闭授权脚本生成。")

        # 初始化 Oracle Instant Client (Thick Mode)
        init_oracle_client_from_settings(settings)

        oracle_env_info = collect_oracle_env_info(ora_cfg)
        ob_env_info = collect_ob_env_info(ob_cfg)
        endpoint_info = {
            "oracle": oracle_env_info,
            "oceanbase": ob_env_info
        }

    generate_fixup_enabled = settings.get('generate_fixup', 'true').strip().lower() in ('true', '1', 'yes')
    if not enabled_extra_types:
        phase_skip_reasons["扩展对象校验"] = "check_extra_types 为空"
    if not enable_dependencies_check:
        phase_skip_reasons["依赖/授权校验"] = "check_dependencies=false"
    if not generate_fixup_enabled:
        phase_skip_reasons["修补脚本生成"] = "generate_fixup=false"

    log_section("对象映射准备")
    with phase_timer("对象映射准备", phase_durations):
        # 2) 加载 Remap 规则
        remap_rules = load_remap_rules(settings['remap_file'])

        # 3) 加载源端主对象 (TABLE/VIEW/PROC/FUNC/PACKAGE/PACKAGE BODY/SYNONYM)
        source_objects = get_source_objects(
            ora_cfg,
            settings['source_schemas_list']
        )

        # 4) 验证 Remap 规则
        extraneous_rules = validate_remap_rules(remap_rules, source_objects, settings.get("remap_file"))
        schema_mapping_from_tables: Optional[Dict[str, str]] = None
        
        # 4.1) 获取依附对象（如 TRIGGER）的父表映射，用于 one-to-many schema 拆分场景
        object_parent_map = get_object_parent_tables(
            ora_cfg,
            settings['source_schemas_list'],
            enabled_object_types=enabled_object_types
        )
        
        # 4.2) 加载源端依赖关系（用于智能推导一对多场景的目标 schema）
        oracle_dependencies_internal: List[DependencyRecord] = []
        oracle_dependencies_for_grants: List[DependencyRecord] = []
        source_dependencies_set: Optional[SourceDependencySet] = None
        source_schema_set = {s.upper() for s in settings.get("source_schemas_list", []) if s}
        # 依赖既用于缺失依赖校验，也用于 one-to-many remap 推导；grant 生成亦依赖依赖链
        infer_candidate_types = enabled_object_types - NO_INFER_SCHEMA_TYPES - {'TABLE', 'INDEX', 'CONSTRAINT'}
        need_dependency_infer = enable_dependencies_check or (
            bool(settings.get("enable_schema_mapping_infer", True)) and bool(infer_candidate_types)
        )
        need_grant_dependencies = enable_grant_generation and generate_fixup_enabled
        need_dependency_load = need_dependency_infer or enable_dependencies_check or need_grant_dependencies
        if need_dependency_load:
            include_external_refs = bool(need_grant_dependencies)
            oracle_dependencies_for_grants = load_oracle_dependencies(
                ora_cfg,
                settings['source_schemas_list'],
                object_types=enabled_object_types,
                include_external_refs=include_external_refs
            )
            if include_external_refs:
                oracle_dependencies_internal = [
                    dep for dep in oracle_dependencies_for_grants
                    if (dep.referenced_owner or "").upper() in source_schema_set
                ]
            else:
                oracle_dependencies_internal = list(oracle_dependencies_for_grants)
            if need_dependency_infer or enable_dependencies_check:
                # 转换为简化格式：(dep_owner, dep_name, dep_type, ref_owner, ref_name, ref_type)
                source_dependencies_set = {
                    (dep.owner.upper(), dep.name.upper(), dep.object_type.upper(),
                     dep.referenced_owner.upper(), dep.referenced_name.upper(), dep.referenced_type.upper())
                    for dep in oracle_dependencies_internal
                }
        dependency_graph: DependencyGraph = build_dependency_graph(source_dependencies_set) if source_dependencies_set else {}
        # 4.2.b) 预计算递归依赖表集合（性能优化：避免每对象 DFS）
        transitive_table_cache: Optional[TransitiveTableCache] = None
        if settings.get("enable_schema_mapping_infer") and dependency_graph:
            log.info("正在预计算依赖图的递归 TABLE/MVIEW 引用缓存以加速 remap 推导...")
            transitive_table_cache = precompute_transitive_table_cache(
                dependency_graph,
                object_parent_map=object_parent_map
            )
            log.info("递归依赖表缓存完成，共 %d 个节点。", len(transitive_table_cache))
        # 4.3) 缓存同义词元数据，供 PUBLIC 等大规模同义词快速生成 DDL
        synonym_meta = load_synonym_metadata(
            ora_cfg,
            settings['source_schemas_list'],
            allowed_target_schemas=settings['source_schemas_list']
        )

        # 5) 先不推导 schema，生成基础映射/清单，用于推导 TABLE 唯一映射
        remap_conflicts: RemapConflictMap = {}
        base_full_mapping = build_full_object_mapping(
            source_objects,
            remap_rules,
            schema_mapping=None,
            object_parent_map=object_parent_map,
            transitive_table_cache=transitive_table_cache,
            source_dependencies=source_dependencies_set,
            dependency_graph=dependency_graph,
            enabled_types=enabled_object_types,
            remap_conflicts=remap_conflicts
        )
        base_master_list = generate_master_list(
            source_objects,
            remap_rules,
            enabled_primary_types=enabled_primary_types,
            schema_mapping=None,
            precomputed_mapping=base_full_mapping,
            object_parent_map=object_parent_map,
            transitive_table_cache=transitive_table_cache,
            source_dependencies=source_dependencies_set,
            dependency_graph=dependency_graph,
            remap_conflicts=remap_conflicts
        )
        if settings.get("enable_schema_mapping_infer"):
            schema_mapping_from_tables = build_schema_mapping(base_master_list)
        schema_mapping_for_grants = derive_schema_mapping_from_rules(remap_rules)
        if schema_mapping_from_tables:
            schema_mapping_for_grants.update(schema_mapping_from_tables)

        # 6) 基于 TABLE 推导的 schema 映射（仅作用于非 TABLE 对象）+ 依赖分析，重建映射与校验清单
        full_object_mapping = build_full_object_mapping(
            source_objects,
            remap_rules,
            schema_mapping=schema_mapping_from_tables,
            object_parent_map=object_parent_map,
            transitive_table_cache=transitive_table_cache,
            source_dependencies=source_dependencies_set,
            dependency_graph=dependency_graph,
            enabled_types=enabled_object_types,
            remap_conflicts=remap_conflicts
        )
        master_list = generate_master_list(
            source_objects,
            remap_rules,
            enabled_primary_types=enabled_primary_types,
            schema_mapping=schema_mapping_from_tables,
            precomputed_mapping=full_object_mapping,
            object_parent_map=object_parent_map,
            transitive_table_cache=transitive_table_cache,
            source_dependencies=source_dependencies_set,
            dependency_graph=dependency_graph,
            remap_conflicts=remap_conflicts
        )
        expected_dependency_pairs: Set[Tuple[str, str, str, str]] = set()
        skipped_dependency_pairs: List[DependencyIssue] = []
        if enable_dependencies_check:
            expected_dependency_pairs, skipped_dependency_pairs = build_expected_dependency_pairs(
                oracle_dependencies_internal,
                full_object_mapping
            )
        target_schemas: Set[str] = set()
        for type_map in full_object_mapping.values():
            for tgt_name in type_map.values():
                try:
                    schema, _ = tgt_name.split('.', 1)
                    target_schemas.add(schema.upper())
                except ValueError:
                    continue
        target_table_pairs = collect_table_pairs(master_list, use_target=True)

    report_dir_setting = settings.get('report_dir', 'main_reports').strip() or 'main_reports'
    report_dir = Path(report_dir_setting)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"report_{timestamp}.txt"
    log.info(f"本次报告将输出到: {report_path}")

    # 输出全量 remap 推导结果，便于人工审核
    mapping_path = report_dir / f"object_mapping_{timestamp}.txt"
    mapping_written = export_full_object_mapping(full_object_mapping, mapping_path)
    if mapping_written:
        log.info("全量对象映射已输出: %s", mapping_written)

    remap_conflict_items: List[Tuple[str, str, str]] = []
    if remap_conflicts:
        remap_conflict_items = [
            (obj_type, src_full, reason)
            for (src_full, obj_type), reason in sorted(remap_conflicts.items())
        ]
        conflict_path = report_dir / f"remap_conflicts_{timestamp}.txt"
        conflict_written = export_remap_conflicts(remap_conflicts, conflict_path)
        if conflict_written:
            log.info("无法自动推导的对象已输出: %s", conflict_written)

    dependency_report: DependencyReport = {
        "missing": [],
        "unexpected": [],
        "skipped": skipped_dependency_pairs
    }
    dependency_chain_file: Optional[Path] = None
    view_chain_file: Optional[Path] = None
    grant_plan: Optional[GrantPlan] = None
    object_counts_summary: Optional[ObjectCountSummary] = None
    schema_summary: Optional[Dict[str, List[str]]] = None

    if not master_list:
        for phase in (
            "OceanBase 元数据转储",
            "Oracle 元数据转储",
            "主对象校验",
            "扩展对象校验",
            "依赖/授权校验",
            "修补脚本生成"
        ):
            phase_skip_reasons[phase] = "主校验清单为空"

        log.info("主校验清单为空，程序结束。")
        tv_results: ReportResults = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "remap_conflicts": remap_conflict_items,
            "extraneous": extraneous_rules,
            "extra_targets": []
        }
        package_results: PackageCompareResults = {"rows": [], "summary": {}, "diff_rows": []}
        extra_results: ExtraCheckResults = {
            "index_ok": [],
            "index_mismatched": [],
            "constraint_ok": [],
            "constraint_mismatched": [],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }
        comment_results = {
            "ok": [],
            "mismatched": [],
            "skipped_reason": "主校验清单为空，未执行注释比对。"
        }
        report_start_perf = time.perf_counter()
        trigger_summary_stub = None
        trigger_list_path = settings.get("trigger_list", "").strip()
        if trigger_list_path:
            trigger_summary_stub = {
                "enabled": True,
                "path": trigger_list_path,
                "valid_entries": 0,
                "selected_missing": 0,
                "missing_not_listed": 0,
                "invalid_entries": 0,
                "not_found": 0,
                "check_disabled": True,
                "error": ""
            }
        run_summary_ctx = RunSummaryContext(
            start_time=run_start_time,
            start_perf=run_start_perf,
            phase_durations=phase_durations,
            phase_skip_reasons=phase_skip_reasons,
            enabled_primary_types=enabled_primary_types,
            enabled_extra_types=enabled_extra_types,
            print_only_types=print_only_types,
            total_checked=0,
            enable_dependencies_check=enable_dependencies_check,
            enable_comment_check=enable_comment_check,
            enable_grant_generation=enable_grant_generation,
            enable_schema_mapping_infer=settings.get("enable_schema_mapping_infer", True),
            fixup_enabled=generate_fixup_enabled,
            fixup_dir=settings.get('fixup_dir', 'fixup_scripts') or 'fixup_scripts',
            dependency_chain_file=None,
            view_chain_file=None,
            trigger_list_summary=trigger_summary_stub,
            report_start_perf=report_start_perf
        )
        run_summary = print_final_report(
            tv_results,
            0,
            extra_results,
            comment_results,
            dependency_report,
            report_path,
            object_counts_summary,
            endpoint_info,
            schema_summary,
            settings,
            blacklisted_missing_tables={},
            blacklist_report_rows=[],
            trigger_list_summary=None,
            trigger_list_rows=None,
            package_results=package_results,
            run_summary_ctx=run_summary_ctx,
            filtered_grants=None
        )
        if run_summary:
            log_run_summary(run_summary)
        return

    log_section("元数据转储")
    with phase_timer("OceanBase 元数据转储", phase_durations):
        log_subsection("OceanBase 元数据")
        # 6) 计算目标端 schema 集合并一次性 dump OB 元数据
        tracked_types = set(checked_primary_types) | (set(enabled_extra_types) & set(ALL_TRACKED_OBJECT_TYPES))
        if not tracked_types:
            tracked_types = {'TABLE'}

        ob_meta = dump_ob_metadata(
            ob_cfg,
            target_schemas,
            tracked_object_types=tracked_types,
            include_tab_columns='TABLE' in enabled_primary_types,
            include_indexes='INDEX' in enabled_extra_types,
            include_constraints='CONSTRAINT' in enabled_extra_types,
            include_triggers='TRIGGER' in enabled_extra_types,
            include_sequences='SEQUENCE' in enabled_extra_types,
            include_comments=enable_comment_check,
            include_roles=enable_grant_generation,
            target_table_pairs=target_table_pairs if enable_comment_check else set()
        )
        ob_dependencies: Set[Tuple[str, str, str, str]] = set()
        if enable_dependencies_check:
            ob_dependencies = load_ob_dependencies(
                ob_cfg,
                target_schemas,
                object_types=enabled_object_types
            )

        schema_summary = compute_schema_coverage(
            settings['source_schemas_list'],
            source_objects,
            target_schemas,
            ob_meta
        )

    # 7) 主对象校验
    with phase_timer("Oracle 元数据转储", phase_durations):
        log_subsection("Oracle 元数据")
        oracle_meta = dump_oracle_metadata(
            ora_cfg,
            master_list,
            settings,
            include_indexes='INDEX' in enabled_extra_types,
            include_constraints='CONSTRAINT' in enabled_extra_types,
            include_triggers='TRIGGER' in enabled_extra_types,
            include_sequences='SEQUENCE' in enabled_extra_types,
            include_comments=enable_comment_check,
            include_privileges=enable_grant_generation
        )

        table_target_map = build_table_target_map(master_list)
        blacklist_report_rows = build_blacklist_report_rows(
            oracle_meta.blacklist_tables,
            table_target_map,
            oracle_meta,
            ob_meta
        )

    log_section("差异校验")
    monitored_types: Tuple[str, ...] = tuple(
        t for t in OBJECT_COUNT_TYPES
        if (t.upper() in checked_primary_types) or (t.upper() in enabled_extra_types)
    ) or ('TABLE',)

    with phase_timer("主对象校验", phase_durations):
        object_counts_summary = compute_object_counts(full_object_mapping, ob_meta, oracle_meta, monitored_types)
        tv_results = check_primary_objects(
            master_list,
            extraneous_rules,
            ob_meta,
            oracle_meta,
            enabled_primary_types,
            print_only_types
        )
        supplement_missing_views_from_mapping(
            tv_results,
            full_object_mapping,
            ob_meta,
            enabled_primary_types
        )
        tv_results["remap_conflicts"] = remap_conflict_items
        package_results = compare_package_objects(
            master_list,
            oracle_meta,
            ob_meta,
            enabled_primary_types
        )
        blacklisted_missing_tables = collect_blacklisted_missing_tables(
            tv_results,
            oracle_meta.blacklist_tables
        )
        comment_results = check_comments(
            master_list,
            oracle_meta,
            ob_meta,
            enable_comment_check
        )

    # 8) 扩展对象校验 (索引/约束/序列/触发器)
    if enabled_extra_types:
        with phase_timer("扩展对象校验", phase_durations):
            extra_results = check_extra_objects(
                settings,
                master_list,
                ob_meta,
                oracle_meta,
                full_object_mapping,
                enabled_extra_types
            )
    else:
        extra_results = check_extra_objects(
            settings,
            master_list,
            ob_meta,
            oracle_meta,
            full_object_mapping,
            enabled_extra_types
        )

    trigger_list_summary: Optional[Dict[str, object]] = None
    trigger_list_rows: Optional[List[TriggerListReportRow]] = None
    trigger_filter_entries: Optional[Set[str]] = None
    trigger_filter_enabled = False
    trigger_list_path = settings.get("trigger_list", "").strip()
    if trigger_list_path:
        entries, invalid_entries, duplicate_entries, total_lines, read_error = parse_trigger_list_file(trigger_list_path)
        trigger_list_rows, trigger_list_summary = build_trigger_list_report(
            trigger_list_path,
            entries,
            invalid_entries,
            duplicate_entries,
            total_lines,
            read_error,
            extra_results,
            oracle_meta,
            ob_meta,
            full_object_mapping,
            trigger_check_enabled='TRIGGER' in enabled_extra_types
        )
        trigger_filter_entries = entries
        if trigger_list_summary.get("error"):
            log.warning("trigger_list 读取失败，将回退全量触发器生成: %s", trigger_list_summary.get("error"))
            trigger_filter_enabled = False
        elif trigger_list_summary.get("check_disabled"):
            log.warning("TRIGGER 未启用检查，trigger_list 将不会用于缺失触发器筛选。")
            trigger_filter_enabled = False
        elif trigger_list_summary.get("fallback_full"):
            log.warning("trigger_list 为空或无有效条目，已回退全量触发器生成。")
            trigger_filter_enabled = False
        else:
            trigger_filter_enabled = True
            log.info(
                "trigger_list 生效: 列表=%d, 命中缺失=%d, 未列出缺失=%d。",
                trigger_list_summary.get("valid_entries", 0),
                trigger_list_summary.get("selected_missing", 0),
                trigger_list_summary.get("missing_not_listed", 0)
            )

    if enable_dependencies_check:
        with phase_timer("依赖/授权校验", phase_durations):
            dependency_report = check_dependencies_against_ob(
                expected_dependency_pairs,
                ob_dependencies,
                skipped_dependency_pairs,
                ob_meta
            )
            if settings.get("print_dependency_chains", True):
                dep_chain_path = report_dir / f"dependency_chains_{timestamp}.txt"
                source_dep_pairs = (
                    to_raw_dependency_pairs(oracle_dependencies_internal)
                    if oracle_dependencies_internal else set()
                )
                dependency_chain_file = export_dependency_chains(
                    expected_dependency_pairs,
                    dep_chain_path,
                    source_pairs=source_dep_pairs
                )
                if dependency_chain_file:
                    log.info("依赖链路已输出: %s", dependency_chain_file)
                else:
                    log.info("依赖链路输出已跳过（无数据或写入失败）。")
    else:
        dependency_report = {
            "missing": [],
            "unexpected": [],
            "skipped": []
        }

    log_section("修补脚本与报告")
    # 9) 生成目标端订正 SQL
    if generate_fixup_enabled:
        fixup_dir_label = settings.get('fixup_dir', 'fixup_scripts') or 'fixup_scripts'
        log.info('已开启修补脚本生成，开始写入 %s 目录...', fixup_dir_label)
        with phase_timer("修补脚本生成", phase_durations):
            if enable_grant_generation:
                try:
                    grant_progress_interval = float(settings.get('progress_log_interval', 10))
                except (TypeError, ValueError):
                    grant_progress_interval = 10.0
                grant_progress_interval = max(1.0, grant_progress_interval)
                supported_sys_privs = settings.get('grant_supported_sys_privs_set', set())
                if not supported_sys_privs:
                    supported_sys_privs = load_ob_supported_sys_privs(ob_cfg)
                supported_object_privs = settings.get('grant_supported_object_privs_set', set())
                if not supported_object_privs:
                    supported_object_privs = set(DEFAULT_SUPPORTED_OBJECT_PRIVS)
                include_oracle_maintained_roles = bool(settings.get('grant_include_oracle_maintained_roles', False))
                ob_roles: Optional[Set[str]] = None
                if ob_meta and ob_meta.roles:
                    ob_roles = set(ob_meta.roles)
                elif enable_grant_generation:
                    ob_roles = load_ob_roles(ob_cfg)
                ob_users = load_ob_users(ob_cfg) if enable_grant_generation else None
                if not supported_sys_privs:
                    log.warning("[GRANT] 未获取到 OceanBase 系统权限清单，将仅依据 Oracle 权限合法性过滤。")
                log.info(
                    "[GRANT] 权限过滤参数：sys_privs=%d, obj_privs=%d, include_oracle_maintained_roles=%s, ob_roles=%d, ob_users=%d",
                    len(supported_sys_privs),
                    len(supported_object_privs),
                    "true" if include_oracle_maintained_roles else "false",
                    len(ob_roles or set()),
                    len(ob_users or set())
                )
                log.info(
                    "[GRANT] 开始生成授权计划：对象权限=%d, 系统权限=%d, 角色授权=%d, 依赖记录=%d。",
                    len(oracle_meta.object_privileges),
                    len(oracle_meta.sys_privileges),
                    len(oracle_meta.role_privileges),
                    len(oracle_dependencies_for_grants)
                )
                grant_plan = build_grant_plan(
                    oracle_meta,
                    full_object_mapping,
                    remap_rules,
                    source_objects,
                    schema_mapping_for_grants,
                    object_parent_map,
                    dependency_graph,
                    transitive_table_cache,
                    source_dependencies_set,
                    source_schema_set,
                    remap_conflicts,
                    synonym_meta,
                    supported_sys_privs=supported_sys_privs,
                    supported_object_privs=supported_object_privs,
                    oracle_sys_privs_map=oracle_meta.system_privilege_map,
                    oracle_obj_privs_map=oracle_meta.table_privilege_map,
                    oracle_roles=oracle_meta.role_metadata,
                    ob_roles=ob_roles,
                    ob_users=ob_users,
                    include_oracle_maintained_roles=include_oracle_maintained_roles,
                    dependencies=oracle_dependencies_for_grants,
                    progress_interval=grant_progress_interval
                )
                object_grant_cnt = sum(len(v) for v in grant_plan.object_grants.values())
                sys_grant_cnt = sum(len(v) for v in grant_plan.sys_privs.values())
                role_grant_cnt = sum(len(v) for v in grant_plan.role_privs.values())
                log.info(
                    "[GRANT] 授权计划生成完成：对象权限=%d, 系统权限=%d, 角色授权=%d",
                    object_grant_cnt,
                    sys_grant_cnt,
                    role_grant_cnt
                )
                if grant_plan.filtered_grants:
                    log.warning(
                        "[GRANT] 已过滤不兼容权限 %d 条，将输出 main_reports/filtered_grants.txt。",
                        len(grant_plan.filtered_grants)
                    )
            view_chain_file = generate_fixup_scripts(
                ora_cfg,
                ob_cfg,
                settings,
                tv_results,
                extra_results,
                master_list,
                oracle_meta,
                full_object_mapping,
                remap_rules,
                grant_plan,
                enable_grant_generation,
                dependency_report,
                ob_meta,
                expected_dependency_pairs,
                synonym_meta,
                trigger_filter_entries,
                trigger_filter_enabled,
                package_results=package_results,
                report_dir=report_dir,
                report_timestamp=timestamp
            )
    else:
        log.info('已根据配置跳过修补脚本生成，仅打印对比报告。')
        if enable_grant_generation:
            log.info("[GRANT] generate_fixup=false，授权脚本生成已跳过。")

    # 10) 输出最终报告
    total_checked = sum(
        1 for _, _, obj_type in master_list
        if obj_type.upper() in checked_primary_types
    )
    report_start_perf = time.perf_counter()
    run_summary_ctx = RunSummaryContext(
        start_time=run_start_time,
        start_perf=run_start_perf,
        phase_durations=phase_durations,
        phase_skip_reasons=phase_skip_reasons,
        enabled_primary_types=enabled_primary_types,
        enabled_extra_types=enabled_extra_types,
        print_only_types=print_only_types,
        total_checked=total_checked,
        enable_dependencies_check=enable_dependencies_check,
        enable_comment_check=enable_comment_check,
        enable_grant_generation=enable_grant_generation,
        enable_schema_mapping_infer=settings.get("enable_schema_mapping_infer", True),
        fixup_enabled=generate_fixup_enabled,
        fixup_dir=settings.get('fixup_dir', 'fixup_scripts') or 'fixup_scripts',
        dependency_chain_file=dependency_chain_file,
        view_chain_file=view_chain_file,
        trigger_list_summary=trigger_list_summary,
        report_start_perf=report_start_perf
    )
    run_summary = print_final_report(
        tv_results,
        total_checked,
        extra_results,
        comment_results,
        dependency_report,
        report_path,
        object_counts_summary,
        endpoint_info,
        schema_summary,
        settings,
        blacklisted_missing_tables,
        blacklist_report_rows,
        trigger_list_summary,
        trigger_list_rows,
        package_results=package_results,
        run_summary_ctx=run_summary_ctx,
        filtered_grants=(grant_plan.filtered_grants if grant_plan else None)
    )
    if run_summary:
        log_run_summary(run_summary)


if __name__ == "__main__":
    main()
