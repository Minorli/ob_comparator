#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据库对象对比工具 (V11 - 扩展 + 修补脚本生成版)
对比 Oracle (源) 和 OceanBase (目标) 的 TABLE / VIEW 及相关对象，并生成 fix_up DDL 脚本。

在 V10 的基础上增加：
- 自动从 Oracle 使用 DBMS_METADATA.GET_DDL 抽取对象 DDL。
- 根据 remap 映射生成 OceanBase 端的“修补脚本”，保存在 fix_up/ 目录中：
  - fix_up/table/*.sql        -- 缺失 TABLE
  - fix_up/view/*.sql         -- 缺失 VIEW
  - fix_up/index/*.sql        -- 缺失 INDEX
  - fix_up/constraint/*.sql   -- 缺失 CONSTRAINT
  - fix_up/sequence/*.sql     -- 缺失 SEQUENCE
  - fix_up/trigger/*.sql      -- 缺失 TRIGGER
- 只负责“补齐”源端有、OB 端没有的对象，不负责自动 DROP 与复杂 ALTER。
"""

import configparser
import subprocess
import sys
import logging
import os
from pathlib import Path
from typing import Dict, Set, List, Tuple, Optional, NamedTuple

# 尝试导入 oracledb，如果失败则提示安装
try:
    import oracledb
except ImportError:
    print("错误: 未找到 'oracledb' 库。", file=sys.stderr)
    print("请先安装: pip install oracledb", file=sys.stderr)
    sys.exit(1)

# --- ANSI 颜色定义 ---
class Color:
    """定义 ANSI 颜色代码，用于美化输出"""
    if sys.stdout.isatty():
        GREEN = '\033[92m'
        YELLOW = '\033[93m'
        RED = '\033[91m'
        BOLD = '\033[1m'
        ENDC = '\033[0m'
    else:
        GREEN = YELLOW = RED = BOLD = ENDC = ""

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    stream=sys.stderr
)
log = logging.getLogger(__name__)

# --- 类型别名 ---
OraConfig = Dict[str, str]
ObConfig = Dict[str, str]
RemapRules = Dict[str, str]
SourceObjectMap = Dict[str, str]  # {'OWNER.OBJ': 'TYPE'}
MasterCheckList = List[Tuple[str, str, str]]  # [(src_name, tgt_name, type)]
ReportResults = Dict[str, List]


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
    schema: str
    missing_sequences: Set[str]
    extra_sequences: Set[str]


class TriggerMismatch(NamedTuple):
    table: str
    missing_triggers: Set[str]
    extra_triggers: Set[str]
    detail_mismatch: List[str]


ExtraCheckResults = Dict[str, List]


# ====================== 通用配置和基础函数 ======================

def load_config(config_file: str) -> Tuple[OraConfig, ObConfig, Dict]:
    """读取 db.ini 配置文件"""
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

        # 可配置 fix_up 目录（可选），默认 fix_up
        settings.setdefault('fixup_dir', 'fix_up')

        log.info(f"成功加载配置，将扫描 {len(schemas_list)} 个源 schema。")
        return ora_cfg, ob_cfg, settings
    except KeyError as e:
        log.error(f"严重错误: 配置文件中缺少必要的部分: {e}")
        sys.exit(1)


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


def get_source_objects(ora_cfg: OraConfig, schemas_list: List[str]) -> SourceObjectMap:
    """从 Oracle 源端获取所有 TABLE 和 VIEW 对象"""
    log.info(f"正在连接 Oracle 源端: {ora_cfg['dsn']}...")

    placeholders = ','.join([f":{i+1}" for i in range(len(schemas_list))])

    sql = f"""
        SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
        FROM ALL_OBJECTS
        WHERE OWNER IN ({placeholders})
          AND OBJECT_TYPE IN ('TABLE', 'VIEW')
    """

    source_objects: SourceObjectMap = {}

    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as connection:
            log.info("Oracle 连接成功。正在查询源对象列表...")
            with connection.cursor() as cursor:
                cursor.execute(sql, schemas_list)
                for row in cursor:
                    owner, obj_name, obj_type = row
                    full_name = f"{owner}.{obj_name}"
                    source_objects[full_name] = obj_type
    except oracledb.Error as e:
        log.error(f"严重错误: 连接或查询 Oracle 失败: {e}")
        sys.exit(1)

    log.info(f"从 Oracle 成功获取 {len(source_objects)} 个 (TABLE/VIEW) 对象。")
    return source_objects


def validate_remap_rules(remap_rules: RemapRules, source_objects: SourceObjectMap) -> List[str]:
    """检查 remap 规则中的源对象是否存在于 Oracle source_objects 中。"""
    log.info("正在验证 Remap 规则...")
    remap_keys = set(remap_rules.keys())
    source_keys = set(source_objects.keys())

    extraneous_keys = sorted(list(remap_keys - source_keys))

    if extraneous_keys:
        log.warning(f"  {Color.YELLOW}[规则警告] 在 remap_rules.txt 中发现了 {len(extraneous_keys)} 个无效的源对象。{Color.ENDC}")
        log.warning("  (这些对象在源端 Oracle (db.ini 中配置的 schema) 中未找到)")
        for key in extraneous_keys:
            log.warning(f"    - {Color.YELLOW}无效条目: {key}{Color.ENDC}")
    else:
        log.info("Remap 规则验证通过，所有规则中的源对象均存在。")

    return extraneous_keys


def generate_master_list(source_objects: SourceObjectMap, remap_rules: RemapRules) -> MasterCheckList:
    """生成“最终校验清单”并检测 "多对一" 映射 (仅 TABLE/VIEW)。"""
    log.info("正在生成主校验清单 (应用 Remap 规则)...")
    master_list: MasterCheckList = []

    target_tracker: Dict[str, str] = {}

    for src_name, obj_type in source_objects.items():
        if src_name in remap_rules:
            tgt_name = remap_rules[src_name]
        else:
            tgt_name = src_name

        if tgt_name in target_tracker:
            existing_src = target_tracker[tgt_name]
            log.error(f"{Color.RED}{'='*80}{Color.ENDC}")
            log.error(f"{Color.RED}                 !!! 致命配置错误 !!!{Color.ENDC}")
            log.error(f"{Color.RED}发现“多对一”映射。同一个目标对象 '{tgt_name}' 被映射了多次：{Color.ENDC}")
            log.error(f"  1. 源: '{existing_src}' -> 目标: '{tgt_name}'")
            log.error(f"  2. 源: '{src_name}' -> 目标: '{tgt_name}'")
            log.error("这会导致校验逻辑混乱。请检查您的 remap_rules.txt 文件，")
            log.error("确保每一个目标对象只被一个源对象所映射。")
            log.error(f"{Color.RED}{'='*80}{Color.ENDC}")
            sys.exit(1)

        target_tracker[tgt_name] = src_name
        master_list.append((src_name, tgt_name, obj_type))

    log.info(f"主校验清单生成完毕，共 {len(master_list)} 个待校验项。")
    return master_list


def obclient_run_sql(ob_cfg: ObConfig, sql_query: str) -> Tuple[bool, str, str]:
    """运行 obclient CLI 命令并返回 (Success, stdout, stderr)"""
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
            errors='ignore'
        )

        if result.returncode != 0 or (result.stderr and "Warning" not in result.stderr):
            log.warning(f"  [OBClient 错误] SQL: {sql_query.strip()} | 错误: {result.stderr.strip()}")
            return False, "", result.stderr.strip()

        return True, result.stdout.strip(), ""

    except FileNotFoundError:
        log.error(f"严重错误: 未找到 obclient 可执行文件: {ob_cfg['executable']}")
        log.error("请检查 db.ini 中的 [OCEANBASE_TARGET] -> executable 路径。")
        sys.exit(1)
    except Exception as e:
        log.error(f"严重错误: 执行 subprocess 时发生未知错误: {e}")
        return False, "", str(e)


def obclient_check_exists(ob_cfg: ObConfig, schema: str, name: str, obj_view: str, name_col: str) -> bool:
    """使用 obclient 检查一个对象是否存在"""
    sql = f"SELECT 1 FROM {obj_view} WHERE OWNER='{schema.upper()}' AND {name_col}='{name.upper()}'"
    success, stdout, _ = obclient_run_sql(ob_cfg, sql)
    return success and stdout == '1'


def obclient_get_columns(ob_cfg: ObConfig, schema: str, table_name: str) -> Optional[Set[str]]:
    """使用 obclient 获取一个表的列名集合"""
    sql = f"""
        SELECT COLUMN_NAME 
        FROM ALL_TAB_COLUMNS 
        WHERE OWNER='{schema.upper()}' AND TABLE_NAME='{table_name.upper()}'
    """
    success, stdout, stderr = obclient_run_sql(ob_cfg, sql)

    if not success:
        log.warning(f"  [列检查失败] 无法获取 {schema}.{table_name} 的列: {stderr}")
        return None

    if not stdout:
        return set()

    return set(line for line in stdout.split('\n') if line.strip())


def oracle_get_columns(ora_conn, schema: str, table_name: str) -> Optional[Set[str]]:
    """使用 oracledb 获取源端表的列名集合"""
    sql = "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER = :1 AND TABLE_NAME = :2"
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(sql, [schema.upper(), table_name.upper()])
            return set(row[0] for row in cursor)
    except oracledb.Error as e:
        log.warning(f"  [Oracle列检查失败] 无法获取 {schema}.{table_name} 的列: {e}")
        return None


# ====================== TABLE / VIEW 校验 (原有逻辑封装) ======================

def check_tables_and_views(
    ora_cfg: OraConfig,
    ob_cfg: ObConfig,
    settings: Dict,
    remap_rules: RemapRules,
    source_objects: SourceObjectMap,
    extraneous_rules: List[str],
) -> Tuple[MasterCheckList, ReportResults]:
    """
    封装原有 TABLE/VIEW 校验逻辑，返回 master_list 和 tv_results。
    """
    master_list = generate_master_list(source_objects, remap_rules)

    if not master_list:
        log.info("主校验清单为空，没有需要校验的对象。")
        return master_list, {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "extraneous": extraneous_rules
        }

    log.info("--- 开始执行 TABLE/VIEW 批量验证 (执行时间取决于对象数量) ---")

    results: ReportResults = {
        "missing": [],
        "mismatched": [],
        "ok": [],
        "extraneous": extraneous_rules
    }

    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as ora_conn:

            total = len(master_list)
            for i, (src_name, tgt_name, obj_type) in enumerate(master_list):

                if (i + 1) % 100 == 0:
                    log.info(f"  TABLE/VIEW 校验进度: {i+1} / {total} ...")

                try:
                    tgt_schema, tgt_obj_name = tgt_name.split('.')
                except ValueError:
                    log.warning(f"  [跳过] 目标名 '{tgt_name}' 格式不正确 (源: {src_name})")
                    continue

                # --- 分发器 ---
                if obj_type == 'VIEW':
                    exists = obclient_check_exists(ob_cfg, tgt_schema, tgt_obj_name, 'ALL_VIEWS', 'VIEW_NAME')
                    if not exists:
                        results['missing'].append((obj_type, tgt_name, src_name))
                    else:
                        results['ok'].append((obj_type, tgt_name))

                elif obj_type == 'TABLE':
                    exists = obclient_check_exists(ob_cfg, tgt_schema, tgt_obj_name, 'ALL_TABLES', 'TABLE_NAME')

                    if not exists:
                        results['missing'].append((obj_type, tgt_name, src_name))
                        continue

                    # --- 列名对比 (带过滤器) ---
                    src_schema, src_obj_name = src_name.split('.')

                    # 1. 获取源列
                    src_cols = oracle_get_columns(ora_conn, src_schema, src_obj_name)
                    # 2. 获取目标原始列
                    tgt_cols_raw = obclient_get_columns(ob_cfg, tgt_schema, tgt_obj_name)

                    if src_cols is None or tgt_cols_raw is None:
                        # 检查失败 (错误已在辅助函数中记录)
                        results['mismatched'].append((obj_type, f"{tgt_name} (列检查失败)", set(), set()))
                        continue

                    # 3. 过滤掉 'OMS_' 开头的列
                    tgt_cols = {
                        col for col in tgt_cols_raw
                        if not col.upper().startswith('OMS_')
                    }

                    # 4. 对比
                    if src_cols == tgt_cols:
                        results['ok'].append((obj_type, tgt_name))
                    else:
                        missing_in_tgt = src_cols - tgt_cols
                        extra_in_tgt = tgt_cols - src_cols
                        results['mismatched'].append((obj_type, tgt_name, missing_in_tgt, extra_in_tgt))

    except oracledb.Error as e:
        log.error(f"严重错误: 无法维护 Oracle 的持久连接: {e}")
        sys.exit(1)

    return master_list, results


# ====================== 扩展：索引 / 约束 / 序列 / 触发器 ======================

# ---- 索引 ----

def oracle_get_indexes_for_table(ora_conn, owner: str, table: str) -> Optional[Dict[str, Dict]]:
    """
    返回:
    {
      "IDX_NAME": {
         "uniqueness": "UNIQUE"/"NONUNIQUE",
         "columns": ["COL1", "COL2", ...]
      },
      ...
    }
    """
    owner = owner.upper()
    table = table.upper()
    idx_info: Dict[str, Dict] = {}

    sql_idx = """
        SELECT INDEX_NAME, UNIQUENESS
        FROM ALL_INDEXES
        WHERE TABLE_OWNER = :1 AND TABLE_NAME = :2
    """
    sql_cols = """
        SELECT INDEX_NAME, COLUMN_NAME, COLUMN_POSITION
        FROM ALL_IND_COLUMNS
        WHERE TABLE_OWNER = :1 AND TABLE_NAME = :2
        ORDER BY INDEX_NAME, COLUMN_POSITION
    """
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(sql_idx, [owner, table])
            for idx_name, uniqueness in cursor:
                idx_info[idx_name] = {"uniqueness": uniqueness, "columns": []}

            cursor.execute(sql_cols, [owner, table])
            for idx_name, col_name, pos in cursor:
                if idx_name in idx_info:
                    idx_info[idx_name]["columns"].append(col_name)
    except oracledb.Error as e:
        log.warning(f"[索引检查] Oracle 获取索引失败 {owner}.{table}: {e}")
        return None

    return idx_info


def ob_get_indexes_for_table(ob_cfg: ObConfig, owner: str, table: str) -> Optional[Dict[str, Dict]]:
    owner = owner.upper()
    table = table.upper()
    idx_info: Dict[str, Dict] = {}

    sql_idx = f"""
        SELECT INDEX_NAME, UNIQUENESS
        FROM ALL_INDEXES
        WHERE TABLE_OWNER = '{owner}' AND TABLE_NAME = '{table}'
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql_idx)
    if not ok:
        log.warning(f"[索引检查] OB 获取索引失败 {owner}.{table}: {err}")
        return None

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) >= 2:
                idx_name, uniq = parts[0], parts[1]
                idx_info[idx_name] = {"uniqueness": uniq, "columns": []}

    sql_cols = f"""
        SELECT INDEX_NAME, COLUMN_NAME, COLUMN_POSITION
        FROM ALL_IND_COLUMNS
        WHERE TABLE_OWNER = '{owner}' AND TABLE_NAME = '{table}'
        ORDER BY INDEX_NAME, COLUMN_POSITION
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql_cols)
    if not ok:
        log.warning(f"[索引检查] OB 获取索引列失败 {owner}.{table}: {err}")
        return None

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) >= 3:
                idx_name, col_name, pos = parts[0], parts[1], parts[2]
                if idx_name in idx_info:
                    idx_info[idx_name]["columns"].append(col_name)

    return idx_info


def compare_indexes_for_table(
    ora_conn,
    ob_cfg: ObConfig,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str
) -> Tuple[bool, Optional[IndexMismatch]]:
    src_idx = oracle_get_indexes_for_table(ora_conn, src_schema, src_table)
    tgt_idx = ob_get_indexes_for_table(ob_cfg, tgt_schema, tgt_table)

    if src_idx is None or tgt_idx is None:
        # 获取失败，视为不一致，但给出简单说明
        return False, IndexMismatch(
            table=f"{tgt_schema}.{tgt_table} (索引信息获取失败)",
            missing_indexes=set(),
            extra_indexes=set(),
            detail_mismatch=[]
        )

    src_names = set(src_idx.keys())
    tgt_names = set(tgt_idx.keys())

    missing = src_names - tgt_names
    extra = tgt_names - src_names
    detail_mismatch: List[str] = []

    common = src_names & tgt_names
    for idx in common:
        s = src_idx[idx]
        t = tgt_idx[idx]
        if s["uniqueness"] != t["uniqueness"]:
            detail_mismatch.append(
                f"{idx}: 唯一性不一致 (src={s['uniqueness']}, tgt={t['uniqueness']})"
            )
        if s["columns"] != t["columns"]:
            detail_mismatch.append(
                f"{idx}: 列顺序/集合不一致 (src={s['columns']}, tgt={t['columns']})"
            )

    all_good = (not missing) and (not extra) and (not detail_mismatch)
    if all_good:
        return True, None
    else:
        return False, IndexMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_indexes=missing,
            extra_indexes=extra,
            detail_mismatch=detail_mismatch
        )


# ---- 约束 ----

def oracle_get_constraints_for_table(ora_conn, owner: str, table: str) -> Optional[Dict[str, Dict]]:
    """
    返回:
    {
      "CONS_NAME": {
        "type": "P"/"U"/"R",
        "columns": ["C1", "C2", ...]
      }
    }
    只考虑主键/唯一键/外键 (P/U/R)，且 status=ENABLED。
    """
    owner = owner.upper()
    table = table.upper()
    cons: Dict[str, Dict] = {}

    sql_cons = """
        SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE
        FROM ALL_CONSTRAINTS
        WHERE OWNER = :1 AND TABLE_NAME = :2
          AND CONSTRAINT_TYPE IN ('P','U','R')
          AND STATUS = 'ENABLED'
    """
    sql_cols = """
        SELECT CONSTRAINT_NAME, COLUMN_NAME, POSITION
        FROM ALL_CONS_COLUMNS
        WHERE OWNER = :1 AND TABLE_NAME = :2
        ORDER BY CONSTRAINT_NAME, POSITION
    """
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(sql_cons, [owner, table])
            for name, ctype in cursor:
                cons[name] = {"type": ctype, "columns": []}

            cursor.execute(sql_cols, [owner, table])
            for name, col, pos in cursor:
                if name in cons:
                    cons[name]["columns"].append(col)
    except oracledb.Error as e:
        log.warning(f"[约束检查] Oracle 获取约束失败 {owner}.{table}: {e}")
        return None

    return cons


def ob_get_constraints_for_table(ob_cfg: ObConfig, owner: str, table: str) -> Optional[Dict[str, Dict]]:
    owner = owner.upper()
    table = table.upper()
    cons: Dict[str, Dict] = {}

    sql_cons = f"""
        SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE
        FROM ALL_CONSTRAINTS
        WHERE OWNER = '{owner}' AND TABLE_NAME = '{table}'
          AND CONSTRAINT_TYPE IN ('P','U','R')
          AND STATUS = 'ENABLED'
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql_cons)
    if not ok:
        log.warning(f"[约束检查] OB 获取约束失败 {owner}.{table}: {err}")
        return None

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) >= 2:
                name, ctype = parts[0], parts[1]
                cons[name] = {"type": ctype, "columns": []}

    sql_cols = f"""
        SELECT CONSTRAINT_NAME, COLUMN_NAME, POSITION
        FROM ALL_CONS_COLUMNS
        WHERE OWNER = '{owner}' AND TABLE_NAME = '{table}'
        ORDER BY CONSTRAINT_NAME, POSITION
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql_cols)
    if not ok:
        log.warning(f"[约束检查] OB 获取约束列失败 {owner}.{table}: {err}")
        return None

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) >= 3:
                name, col, pos = parts[0], parts[1], parts[2]
                if name in cons:
                    cons[name]["columns"].append(col)

    return cons


def compare_constraints_for_table(
    ora_conn,
    ob_cfg: ObConfig,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str
) -> Tuple[bool, Optional[ConstraintMismatch]]:
    src_cons = oracle_get_constraints_for_table(ora_conn, src_schema, src_table)
    tgt_cons = ob_get_constraints_for_table(ob_cfg, tgt_schema, tgt_table)

    if src_cons is None or tgt_cons is None:
        return False, ConstraintMismatch(
            table=f"{tgt_schema}.{tgt_table} (约束信息获取失败)",
            missing_constraints=set(),
            extra_constraints=set(),
            detail_mismatch=[]
        )

    src_names = set(src_cons.keys())
    tgt_names = set(tgt_cons.keys())

    missing = src_names - tgt_names
    extra = tgt_names - src_names
    detail_mismatch: List[str] = []

    common = src_names & tgt_names
    for name in common:
        s = src_cons[name]
        t = tgt_cons[name]
        if s["type"] != t["type"]:
            detail_mismatch.append(
                f"{name}: 类型不一致 (src={s['type']}, tgt={t['type']})"
            )
        if s["columns"] != t["columns"]:
            detail_mismatch.append(
                f"{name}: 列顺序/集合不一致 (src={s['columns']}, tgt={t['columns']})"
            )

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


# ---- 序列 ----

def oracle_get_sequences_for_schema(ora_conn, owner: str) -> Optional[Set[str]]:
    owner = owner.upper()
    sql = "SELECT SEQUENCE_NAME FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER = :1"
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(sql, [owner])
            return {row[0] for row in cursor}
    except oracledb.Error as e:
        log.warning(f"[序列检查] Oracle 获取序列失败 {owner}: {e}")
        return None


def ob_get_sequences_for_schema(ob_cfg: ObConfig, owner: str) -> Optional[Set[str]]:
    owner = owner.upper()
    sql = f"SELECT SEQUENCE_NAME FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER = '{owner}'"
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.warning(f"[序列检查] OB 获取序列失败 {owner}: {err}")
        return None
    if not out:
        return set()
    seqs = set()
    for line in out.splitlines():
        name = line.strip()
        if name:
            seqs.add(name)
    return seqs


def compare_sequences_for_schema(
    ora_conn,
    ob_cfg: ObConfig,
    src_schema: str,
    tgt_schema: str
) -> Tuple[bool, Optional[SequenceMismatch]]:
    src_seqs = oracle_get_sequences_for_schema(ora_conn, src_schema)
    tgt_seqs = ob_get_sequences_for_schema(ob_cfg, tgt_schema)

    if src_seqs is None or tgt_seqs is None:
        return False, SequenceMismatch(
            schema=f"{src_schema}->{tgt_schema} (序列信息获取失败)",
            missing_sequences=set(),
            extra_sequences=set()
        )

    missing = src_seqs - tgt_seqs
    extra = tgt_seqs - src_seqs
    all_good = (not missing) and (not extra)
    if all_good:
        return True, None
    else:
        return False, SequenceMismatch(
            schema=f"{src_schema}->{tgt_schema}",
            missing_sequences=missing,
            extra_sequences=extra
        )


# ---- 触发器 ----

def oracle_get_triggers_for_table(ora_conn, owner: str, table: str) -> Optional[Dict[str, Dict]]:
    """
    返回:
    {
      "TRG_NAME": {
        "event": "INSERT OR UPDATE",   # TRIGGERING_EVENT
        "status": "ENABLED"/"DISABLED"
      }
    }
    """
    owner = owner.upper()
    table = table.upper()
    trgs: Dict[str, Dict] = {}
    sql = """
        SELECT TRIGGER_NAME, TRIGGERING_EVENT, STATUS
        FROM ALL_TRIGGERS
        WHERE TABLE_OWNER = :1 AND TABLE_NAME = :2
    """
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(sql, [owner, table])
            for name, event, status in cursor:
                trgs[name] = {"event": event, "status": status}
    except oracledb.Error as e:
        log.warning(f"[触发器检查] Oracle 获取触发器失败 {owner}.{table}: {e}")
        return None
    return trgs


def ob_get_triggers_for_table(ob_cfg: ObConfig, owner: str, table: str) -> Optional[Dict[str, Dict]]:
    owner = owner.upper()
    table = table.upper()
    trgs: Dict[str, Dict] = {}
    sql = f"""
        SELECT TRIGGER_NAME, TRIGGERING_EVENT, STATUS
        FROM ALL_TRIGGERS
        WHERE TABLE_OWNER = '{owner}' AND TABLE_NAME = '{table}'
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.warning(f"[触发器检查] OB 获取触发器失败 {owner}.{table}: {err}")
        return None
    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) >= 3:
                name, event, status = parts[0], parts[1], parts[2]
                trgs[name] = {"event": event, "status": status}
    return trgs


def compare_triggers_for_table(
    ora_conn,
    ob_cfg: ObConfig,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str
) -> Tuple[bool, Optional[TriggerMismatch]]:
    src_trg = oracle_get_triggers_for_table(ora_conn, src_schema, src_table)
    tgt_trg = ob_get_triggers_for_table(ob_cfg, tgt_schema, tgt_table)

    if src_trg is None or tgt_trg is None:
        return False, TriggerMismatch(
            table=f"{tgt_schema}.{tgt_table} (触发器信息获取失败)",
            missing_triggers=set(),
            extra_triggers=set(),
            detail_mismatch=[]
        )

    src_names = set(src_trg.keys())
    tgt_names = set(tgt_trg.keys())

    missing = src_names - tgt_names
    extra = tgt_names - src_names
    detail_mismatch: List[str] = []

    common = src_names & tgt_names
    for name in common:
        s = src_trg[name]
        t = tgt_trg[name]
        if (s["event"] or "").strip() != (t["event"] or "").strip():
            detail_mismatch.append(
                f"{name}: 触发事件不一致 (src={s['event']}, tgt={t['event']})"
            )
        if (s["status"] or "").strip() != (t["status"] or "").strip():
            detail_mismatch.append(
                f"{name}: 状态不一致 (src={s['status']}, tgt={t['status']})"
            )

    all_good = (not missing) and (not extra) and (not detail_mismatch)
    if all_good:
        return True, None
    else:
        return False, TriggerMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_triggers=missing,
            extra_triggers=extra,
            detail_mismatch=detail_mismatch
        )


# ---- schema 映射 (用于序列/修复映射辅助) ----

def build_schema_mapping(master_list: MasterCheckList) -> Dict[str, str]:
    """
    基于 master_list 中 TABLE 映射，推导 schema 映射：
    - 如果同一 src_schema 只映射到唯一一个 tgt_schema，则使用该映射。
    - 否则 (例如映射到多个 tgt_schema)，退回到 src_schema 本身 (1:1)。
    """
    mapping_tmp: Dict[str, Set[str]] = {}
    for src_name, tgt_name, obj_type in master_list:
        if obj_type != 'TABLE':
            continue
        try:
            src_schema, _ = src_name.split('.')
            tgt_schema, _ = tgt_name.split('.')
        except ValueError:
            continue
        mapping_tmp.setdefault(src_schema, set()).add(tgt_schema)

    final_mapping: Dict[str, str] = {}
    for src_schema, tgt_set in mapping_tmp.items():
        if len(tgt_set) == 1:
            final_mapping[src_schema] = next(iter(tgt_set))
        else:
            final_mapping[src_schema] = src_schema  # 无法确定，退回 1:1
    return final_mapping


def check_extra_objects(
    ora_cfg: OraConfig,
    ob_cfg: ObConfig,
    settings: Dict,
    master_list: MasterCheckList
) -> ExtraCheckResults:
    """
    基于 master_list (TABLE 映射) 检查:
      - 索引
      - 约束
      - 触发器
    基于 schema 映射检查:
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

    if not master_list:
        return extra_results

    log.info("--- 开始执行扩展对象校验 (索引/约束/序列/触发器) ---")

    schema_map = build_schema_mapping(master_list)

    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as ora_conn:

            # 1) 针对每个 TABLE 做索引/约束/触发器校验
            total_tables = sum(1 for _, _, t in master_list if t == 'TABLE')
            done_tables = 0

            for src_name, tgt_name, obj_type in master_list:
                if obj_type != 'TABLE':
                    continue

                done_tables += 1
                if done_tables % 100 == 0:
                    log.info(f"  扩展校验 (索引/约束/触发器) 进度: {done_tables} / {total_tables} ...")

                try:
                    src_schema, src_table = src_name.split('.')
                    tgt_schema, tgt_table = tgt_name.split('.')
                except ValueError:
                    continue

                # 索引
                ok_idx, idx_mis = compare_indexes_for_table(
                    ora_conn, ob_cfg,
                    src_schema, src_table,
                    tgt_schema, tgt_table
                )
                if ok_idx:
                    extra_results["index_ok"].append(tgt_name)
                elif idx_mis:
                    extra_results["index_mismatched"].append(idx_mis)

                # 约束
                ok_cons, cons_mis = compare_constraints_for_table(
                    ora_conn, ob_cfg,
                    src_schema, src_table,
                    tgt_schema, tgt_table
                )
                if ok_cons:
                    extra_results["constraint_ok"].append(tgt_name)
                elif cons_mis:
                    extra_results["constraint_mismatched"].append(cons_mis)

                # 触发器
                ok_trg, trg_mis = compare_triggers_for_table(
                    ora_conn, ob_cfg,
                    src_schema, src_table,
                    tgt_schema, tgt_table
                )
                if ok_trg:
                    extra_results["trigger_ok"].append(tgt_name)
                elif trg_mis:
                    extra_results["trigger_mismatched"].append(trg_mis)

            # 2) 按 schema 做序列校验
            for src_schema in settings['source_schemas_list']:
                tgt_schema = schema_map.get(src_schema, src_schema)
                ok_seq, seq_mis = compare_sequences_for_schema(
                    ora_conn, ob_cfg,
                    src_schema, tgt_schema
                )
                if ok_seq:
                    extra_results["sequence_ok"].append(f"{src_schema}->{tgt_schema}")
                elif seq_mis:
                    extra_results["sequence_mismatched"].append(seq_mis)

    except oracledb.Error as e:
        log.error(f"严重错误: 扩展对象校验时无法维护 Oracle 连接: {e}")
        # 不直接退出，返回已有结果
        return extra_results

    return extra_results


# ====================== DDL 抽取 & 修补脚本生成 ======================

def setup_metadata_session(ora_conn):
    """
    配置 DBMS_METADATA 的 Transform，简化 DDL（去掉 storage/tablespace 等），
    以便更容易在 OceanBase 侧调整使用。
    """
    plsql = """
    BEGIN
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE',FALSE);
      -- 保留约束定义
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS',TRUE);
    END;
    """
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(plsql)
    except oracledb.Error as e:
        log.warning(f"[DDL] 设置 DBMS_METADATA transform 失败: {e}")


def oracle_get_ddl(ora_conn, obj_type: str, owner: str, name: str) -> Optional[str]:
    """
    调用 DBMS_METADATA.GET_DDL 获取对象 DDL。
    obj_type: 'TABLE'/'VIEW'/'INDEX'/'SEQUENCE'/'TRIGGER'/'CONSTRAINT'
    """
    sql = "SELECT DBMS_METADATA.GET_DDL(:1, :2, :3) FROM DUAL"
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(sql, [obj_type.upper(), name.upper(), owner.upper()])
            row = cursor.fetchone()
            if not row or row[0] is None:
                return None
            ddl = row[0]
            # oracledb 对 CLOB 通常给 str，保险起见转一下
            return str(ddl)
    except oracledb.Error as e:
        log.warning(f"[DDL] 获取 {obj_type} {owner}.{name} DDL 失败: {e}")
        return None


def adjust_ddl_for_object(
    ddl: str,
    src_schema: str,
    src_name: str,
    tgt_schema: str,
    tgt_name: str
) -> str:
    """
    简单的字符串替换，将 DDL 中的 "SRC_SCHEMA"."SRC_NAME" 替换为 "TGT_SCHEMA"."TGT_NAME"。
    注意：这是一个“尽量修正”的处理，不保证覆盖所有语法场景，建议人工审核。
    """
    src_schema = src_schema.upper()
    src_name = src_name.upper()
    tgt_schema = tgt_schema.upper()
    tgt_name = tgt_name.upper()

    pattern = f'"{src_schema}"."{src_name}"'
    replacement = f'"{tgt_schema}"."{tgt_name}"'
    ddl2 = ddl.replace(pattern, replacement)

    # 如果 schema 改名了，尝试把 CREATE 语句中第一处 "SRC_SCHEMA" 也改成 "TGT_SCHEMA"
    if src_schema != tgt_schema:
        ddl2 = ddl2.replace(f'"{src_schema}"', f'"{tgt_schema}"', 1)

    return ddl2


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def write_fixup_file(base_dir: Path, subdir: str, filename: str, content: str, header_comment: str):
    target_dir = base_dir / subdir
    ensure_dir(target_dir)
    file_path = target_dir / filename
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(f"-- {header_comment}\n")
        f.write("-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。\n\n")
        f.write(content.strip())
        if not content.strip().endswith(';'):
            f.write(';\n')
    log.info(f"[FIXUP] 生成修补脚本: {file_path}")


def generate_fixup_scripts(
    ora_cfg: OraConfig,
    settings: Dict,
    tv_results: ReportResults,
    extra_results: ExtraCheckResults,
    master_list: MasterCheckList
):
    """
    基于校验结果生成 fix_up DDL 脚本：
      - TABLE / VIEW: 针对缺失的对象生成 CREATE DDL。
      - INDEX / CONSTRAINT / TRIGGER: 针对“缺失”的对象生成 CREATE DDL。
      - SEQUENCE: 针对缺失的序列生成 CREATE DDL。
    所有脚本仅写入文件，不在 OB 上执行。
    """
    if not master_list:
        log.info("[FIXUP] master_list 为空，未生成修补脚本。")
        return

    base_dir = Path(settings.get('fixup_dir', 'fix_up'))
    ensure_dir(base_dir)
    log.info(f"[FIXUP] 修补脚本将生成到目录: {base_dir.resolve()}")

    # 构建目标表名 -> 源表名映射，辅助扩展对象定位
    table_map: Dict[str, str] = {
        tgt_name: src_name
        for (src_name, tgt_name, obj_type) in master_list
        if obj_type == 'TABLE'
    }

    # schema 映射，用于 SEQUENCE
    schema_map = build_schema_mapping(master_list)

    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as ora_conn:

            setup_metadata_session(ora_conn)

            # 1) TABLE / VIEW 缺失对象
            for (obj_type, tgt_name, src_name) in tv_results.get('missing', []):
                try:
                    src_schema, src_obj = src_name.split('.')
                    tgt_schema, tgt_obj = tgt_name.split('.')
                except ValueError:
                    continue

                ddl_type = obj_type  # 'TABLE' or 'VIEW'
                ddl = oracle_get_ddl(ora_conn, ddl_type, src_schema, src_obj)
                if not ddl:
                    continue

                ddl_adj = adjust_ddl_for_object(ddl, src_schema, src_obj, tgt_schema, tgt_obj)

                subdir = 'table' if obj_type == 'TABLE' else 'view'
                filename = f"{tgt_schema}.{tgt_obj}.sql"
                header = f"修补缺失的 {obj_type} {tgt_schema}.{tgt_obj} (源: {src_schema}.{src_obj})"
                write_fixup_file(base_dir, subdir, filename, ddl_adj, header)

            # 2) INDEX 缺失：extra_results['index_mismatched'] 中的 missing_indexes
            for item in extra_results.get('index_mismatched', []):
                table_str = item.table.split()[0]  # 去掉可能的 "(索引信息获取失败)"
                if '.' not in table_str:
                    continue
                tgt_schema, tgt_table = table_str.split('.', 1)
                src_name = table_map.get(table_str)
                if not src_name:
                    continue
                src_schema, src_table = src_name.split('.')

                for idx_name in item.missing_indexes:
                    ddl = oracle_get_ddl(ora_conn, 'INDEX', src_schema, idx_name)
                    if not ddl:
                        continue
                    # 尝试把 DDL 中的底层表名从 src_schema.src_table 改成 tgt_schema.tgt_table
                    ddl_adj = adjust_ddl_for_object(ddl, src_schema, src_table, tgt_schema, tgt_table)
                    filename = f"{tgt_schema}.{idx_name}.sql"
                    header = f"修补缺失的 INDEX {idx_name} (表: {tgt_schema}.{tgt_table}, 源表: {src_schema}.{src_table})"
                    write_fixup_file(base_dir, 'index', filename, ddl_adj, header)

            # 3) CONSTRAINT 缺失：extra_results['constraint_mismatched'] 中的 missing_constraints
            for item in extra_results.get('constraint_mismatched', []):
                table_str = item.table.split()[0]
                if '.' not in table_str:
                    continue
                tgt_schema, tgt_table = table_str.split('.', 1)
                src_name = table_map.get(table_str)
                if not src_name:
                    continue
                src_schema, src_table = src_name.split('.')

                for cons_name in item.missing_constraints:
                    ddl = oracle_get_ddl(ora_conn, 'CONSTRAINT', src_schema, cons_name)
                    if not ddl:
                        continue
                    ddl_adj = adjust_ddl_for_object(ddl, src_schema, src_table, tgt_schema, tgt_table)
                    filename = f"{tgt_schema}.{cons_name}.sql"
                    header = f"修补缺失的约束 {cons_name} (表: {tgt_schema}.{tgt_table}, 源表: {src_schema}.{src_table})"
                    write_fixup_file(base_dir, 'constraint', filename, ddl_adj, header)

            # 4) TRIGGER 缺失：extra_results['trigger_mismatched'] 中的 missing_triggers
            for item in extra_results.get('trigger_mismatched', []):
                table_str = item.table.split()[0]
                if '.' not in table_str:
                    continue
                tgt_schema, tgt_table = table_str.split('.', 1)
                src_name = table_map.get(table_str)
                if not src_name:
                    continue
                src_schema, src_table = src_name.split('.')

                for trg_name in item.missing_triggers:
                    ddl = oracle_get_ddl(ora_conn, 'TRIGGER', src_schema, trg_name)
                    if not ddl:
                        continue
                    ddl_adj = adjust_ddl_for_object(ddl, src_schema, src_table, tgt_schema, tgt_table)
                    filename = f"{tgt_schema}.{trg_name}.sql"
                    header = f"修补缺失的触发器 {trg_name} (表: {tgt_schema}.{tgt_table}, 源表: {src_schema}.{src_table})"
                    write_fixup_file(base_dir, 'trigger', filename, ddl_adj, header)

            # 5) SEQUENCE 缺失：extra_results['sequence_mismatched'] 中的 missing_sequences
            for seq_mis in extra_results.get('sequence_mismatched', []):
                # schema 字段类似 "SRC->TGT"
                schema_map_str = seq_mis.schema
                if '->' in schema_map_str:
                    src_schema, tgt_schema = schema_map_str.split('->', 1)
                else:
                    # fallback: 同名 schema
                    parts = schema_map_str.split(' ')
                    src_schema = parts[0]
                    tgt_schema = schema_map.get(src_schema, src_schema)

                src_schema = src_schema.upper()
                tgt_schema = tgt_schema.upper()

                for seq_name in seq_mis.missing_sequences:
                    ddl = oracle_get_ddl(ora_conn, 'SEQUENCE', src_schema, seq_name)
                    if not ddl:
                        continue
                    # 序列只改 schema
                    ddl_adj = adjust_ddl_for_object(ddl, src_schema, seq_name, tgt_schema, seq_name)
                    filename = f"{tgt_schema}.{seq_name}.sql"
                    header = f"修补缺失的 SEQUENCE {tgt_schema}.{seq_name} (源: {src_schema}.{seq_name})"
                    write_fixup_file(base_dir, 'sequence', filename, ddl_adj, header)

    except oracledb.Error as e:
        log.error(f"[FIXUP] 生成修补脚本时 Oracle 连接出错: {e}")
        return


# ====================== 报告输出 ======================

def print_final_report(
    tv_results: ReportResults,
    total_checked: int,
    extra_results: Optional[ExtraCheckResults] = None
):
    """
    输出最终报告：
      1~4 保持原有 TABLE/VIEW 报告结构；
      5~8 为新增的索引 / 约束 / 序列 / 触发器报告。
    """
    if extra_results is None:
        extra_results = {
            "index_ok": [],
            "index_mismatched": [],
            "constraint_ok": [],
            "constraint_mismatched": [],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }

    log.info("所有校验已完成。正在生成最终报告...")

    # 统计 TABLE/VIEW 数据
    ok_count = len(tv_results['ok'])
    missing_count = len(tv_results['missing'])
    mismatched_count = len(tv_results['mismatched'])
    extraneous_count = len(tv_results['extraneous'])

    # 打印到 stdout
    print("\n\n" + f"{Color.BOLD}{'='*80}{Color.ENDC}")
    print(f"           {Color.BOLD}数据库对象迁移校验报告 (V11){Color.ENDC}")
    print(f"{Color.BOLD}{'='*80}{Color.ENDC}")

    # --- 0. 综合概要 (保持原有) ---
    print(f"\n{Color.BOLD}[ 综合概要 - TABLE/VIEW ]{Color.ENDC}")
    print(f"  - 总计校验对象 (来自源库): {total_checked}")
    print(f"  - {Color.GREEN}一致:{Color.ENDC}       {ok_count}")
    print(f"  - {Color.RED}缺失:{Color.ENDC}       {missing_count}")
    print(f"  - {Color.YELLOW}不匹配:{Color.ENDC}   {mismatched_count}")
    print(f"  - {Color.YELLOW}无效规则:{Color.ENDC} {extraneous_count}")
    print("-" * 80)

    # --- 1. 缺失报告 ---
    print(f"\n{Color.BOLD}--- 1. [缺失的对象] (在 OceanBase 中未找到) --- (共 {missing_count} 个){Color.ENDC}")
    if tv_results['missing']:
        for (obj_type, tgt_name, src_name) in tv_results['missing']:
            print(f"    {Color.RED}[{obj_type}]{Color.ENDC} {tgt_name} {Color.YELLOW}(源: {src_name}){Color.ENDC}")
    else:
        print("    (无)")

    # --- 2. 列名不匹配报告 ---
    print(f"\n{Color.BOLD}--- 2. [列名不匹配的表] --- (共 {mismatched_count} 个){Color.ENDC}")
    if tv_results['mismatched']:
        for (obj_type, tgt_name, missing, extra) in tv_results['mismatched']:

            if "列检查失败" in tgt_name:
                print(f"\n  {Color.RED}[错误]{Color.ENDC} {tgt_name}")
            else:
                print(f"\n  {Color.YELLOW}[{obj_type}]{Color.ENDC} {tgt_name}")
                if missing:
                    print(f"      {Color.RED}- 缺失列 (源库 Oracle 中存在): {missing}{Color.ENDC}")
                if extra:
                    print(f"      {Color.YELLOW}+ 多余列 (源库 Oracle 中不存在): {extra}{Color.ENDC}")
    else:
        print("    (无)")

    # --- 3. 成功报告 ---
    print(f"\n{Color.BOLD}--- 3. [迁移成功且一致的对象] --- (共 {ok_count} 个){Color.ENDC}")
    if tv_results['ok']:
        for (obj_type, tgt_name) in tv_results['ok'][:50]:
            print(f"    {Color.GREEN}[{obj_type}]{Color.ENDC} {tgt_name}")
        if len(tv_results['ok']) > 50:
            print(f"    ... (及其他 {len(tv_results['ok']) - 50} 个对象)")
    else:
        print("    (无)")

    # --- 4. 无效规则报告 ---
    print(f"\n{Color.BOLD}--- 4. [无效的 Remap 规则] --- (共 {extraneous_count} 个){Color.ENDC}")
    if tv_results['extraneous']:
        print("    (下列对象在 remap_rules.txt 中定义, 但在源端 Oracle (db.ini 中配置的 schema) 中未找到)")
        for item in tv_results['extraneous']:
            print(f"    {Color.YELLOW}{item}{Color.ENDC}")
    else:
        print("    (无)")

    # ================= 新增扩展报告 =================

    # --- 5. 索引一致性报告 ---
    idx_ok_cnt = len(extra_results.get("index_ok", []))
    idx_mis_cnt = len(extra_results.get("index_mismatched", []))

    print(f"\n{Color.BOLD}--- 5. [索引一致性检查] ---{Color.ENDC}")
    print(f"  - {Color.GREEN}索引完全一致的表:{Color.ENDC} {idx_ok_cnt}")
    print(f"  - {Color.YELLOW}索引存在差异的表:{Color.ENDC} {idx_mis_cnt}")
    if idx_mis_cnt:
        for item in extra_results["index_mismatched"]:
            print(f"\n  {Color.YELLOW}[TABLE]{Color.ENDC} {item.table}")
            if item.missing_indexes:
                print(f"      {Color.RED}- 缺失索引:{Color.ENDC} {sorted(item.missing_indexes)}")
            if item.extra_indexes:
                print(f"      {Color.YELLOW}+ 多余索引:{Color.ENDC} {sorted(item.extra_indexes)}")
            for msg in item.detail_mismatch:
                print(f"      * {msg}")
    else:
        print("    (无差异或未执行索引检查)")

    # --- 6. 约束一致性报告 ---
    cons_ok_cnt = len(extra_results.get("constraint_ok", []))
    cons_mis_cnt = len(extra_results.get("constraint_mismatched", []))

    print(f"\n{Color.BOLD}--- 6. [约束 (PK/UK/FK) 一致性检查] ---{Color.ENDC}")
    print(f"  - {Color.GREEN}约束完全一致的表:{Color.ENDC} {cons_ok_cnt}")
    print(f"  - {Color.YELLOW}约束存在差异的表:{Color.ENDC} {cons_mis_cnt}")
    if cons_mis_cnt:
        for item in extra_results["constraint_mismatched"]:
            print(f"\n  {Color.YELLOW}[TABLE]{Color.ENDC} {item.table}")
            if item.missing_constraints:
                print(f"      {Color.RED}- 缺失约束:{Color.ENDC} {sorted(item.missing_constraints)}")
            if item.extra_constraints:
                print(f"      {Color.YELLOW}+ 多余约束:{Color.ENDC} {sorted(item.extra_constraints)}")
            for msg in item.detail_mismatch:
                print(f"      * {msg}")
    else:
        print("    (无差异或未执行约束检查)")

    # --- 7. 序列一致性报告 ---
    seq_ok_cnt = len(extra_results.get("sequence_ok", []))
    seq_mis_cnt = len(extra_results.get("sequence_mismatched", []))

    print(f"\n{Color.BOLD}--- 7. [序列 (SEQUENCE) 一致性检查] ---{Color.ENDC}")
    print(f"  - {Color.GREEN}序列集合完全一致的 schema 映射:{Color.ENDC} {seq_ok_cnt}")
    print(f"  - {Color.YELLOW}序列集合存在差异的 schema 映射:{Color.ENDC} {seq_mis_cnt}")
    if seq_mis_cnt:
        for item in extra_results["sequence_mismatched"]:
            print(f"\n  映射: {Color.BOLD}{item.schema}{Color.ENDC}")
            if item.missing_sequences:
                print(f"      {Color.RED}- 缺失序列 (源有 / 目标无):{Color.ENDC} {sorted(item.missing_sequences)}")
            if item.extra_sequences:
                print(f"      {Color.YELLOW}+ 多余序列 (目标有 / 源无):{Color.ENDC} {sorted(item.extra_sequences)}")
    else:
        print("    (无差异或未执行序列检查)")

    # --- 8. 触发器一致性报告 ---
    trg_ok_cnt = len(extra_results.get("trigger_ok", []))
    trg_mis_cnt = len(extra_results.get("trigger_mismatched", []))

    print(f"\n{Color.BOLD}--- 8. [触发器 (TRIGGER) 一致性检查] ---{Color.ENDC}")
    print(f"  - {Color.GREEN}触发器完全一致的表:{Color.ENDC} {trg_ok_cnt}")
    print(f"  - {Color.YELLOW}触发器存在差异的表:{Color.ENDC} {trg_mis_cnt}")
    if trg_mis_cnt:
        for item in extra_results["trigger_mismatched"]:
            print(f"\n  {Color.YELLOW}[TABLE]{Color.ENDC} {item.table}")
            if item.missing_triggers:
                print(f"      {Color.RED}- 缺失触发器:{Color.ENDC} {sorted(item.missing_triggers)}")
            if item.extra_triggers:
                print(f"      {Color.YELLOW}+ 多余触发器:{Color.ENDC} {sorted(item.extra_triggers)}")
            for msg in item.detail_mismatch:
                print(f"      * {msg}")
    else:
        print("    (无差异或未执行触发器检查)")

    print("\n" + f"{Color.BOLD}{'='*80}{Color.ENDC}")
    print("报告结束。")
    print("\n提示: 如果已启用修补脚本生成功能，请查看 fix_up/ 目录下各子目录的 .sql 文件，按需在 OceanBase 执行。")


# ====================== 主函数 ======================

def main():
    """主执行函数"""
    CONFIG_FILE = 'db.ini'

    # 步骤 1: 加载配置
    ora_cfg, ob_cfg, settings = load_config(CONFIG_FILE)

    # 步骤 2: 加载 Remap 规则
    remap_rules = load_remap_rules(settings['remap_file'])

    # 步骤 3: 加载源端全量对象 (TABLE/VIEW)
    source_objects = get_source_objects(ora_cfg, settings['source_schemas_list'])

    # 步骤 4: 验证 Remap 规则
    extraneous_rules = validate_remap_rules(remap_rules, source_objects)

    # 步骤 5: TABLE/VIEW 核心校验
    master_list, tv_results = check_tables_and_views(
        ora_cfg, ob_cfg, settings,
        remap_rules, source_objects, extraneous_rules
    )

    # 步骤 6: 扩展对象校验 (索引/约束/序列/触发器)
    extra_results = check_extra_objects(ora_cfg, ob_cfg, settings, master_list)

    # 步骤 7: 基于差异生成 fix_up 修补脚本（仅写文件，不执行）
    generate_fixup_scripts(ora_cfg, settings, tv_results, extra_results, master_list)

    # 步骤 8: 打印最终报告
    print_final_report(tv_results, len(master_list), extra_results)


if __name__ == "__main__":
    main()
