#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据库对象对比工具 (V9 - 带列名过滤器)
对比 Oracle (源) 和 OceanBase (目标) 的 TABLE 和 VIEW 对象。

功能:
1.  从 db.ini 读取配置。
2.  从 Oracle 源端加载所有 'TABLE' 和 'VIEW' 对象。
3.  从 remap_rules.txt 加载重映射规则。
4.  (V6) 验证 remap_rules 中的源对象是否在源库中真实存在。
5.  (V5) 生成 "主校验清单"，并检测 "多对一" 映射。
6.  严格使用 obclient CLI 校验 OceanBase 目标端：
    - VIEW: 检查是否存在。
    - TABLE: 检查是否存在，并对比列名集合。
7.  (V9) 对比列名时，自动忽略 (过滤掉) 目标端以 'OMS_' 开头的列。
8.  (V8) 输出带有颜色、无图标的、易于理解的最终报告。
"""

import configparser
import subprocess
import sys
import logging
from typing import Dict, Set, List, Tuple, Optional

# 尝试导入 oracledb，如果失败则提示安装
try:
    import oracledb
except ImportError:
    print("错误: 未找到 'oracledb' 库。", file=sys.stderr)
    print("请先安装: pip install oracledb", file=sys.stderr)
    sys.exit(1)

# --- (V7) ANSI 颜色定义 ---
class Color:
    """定义 ANSI 颜色代码，用于美化输出"""
    # 仅当输出到 TTY (终端) 时才启用颜色
    if sys.stdout.isatty():
        GREEN = '\033[92m'
        YELLOW = '\033[93m'
        RED = '\033[91m'
        BOLD = '\033[1m'
        ENDC = '\033[0m'
    else:
        # 如果是重定向到文件，则禁用所有颜色
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
MasterCheckList = List[Tuple[str, str, str]] # [(src_name, tgt_name, type)]
ReportResults = Dict[str, List]


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
    """(V6) 检查 remap 规则中的源对象是否存在于 Oracle source_objects 中。"""
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
    """(V5) 生成“最终校验清单”并检测 "多对一" 映射。"""
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
        master_list.append( (src_name, tgt_name, obj_type) )
        
    log.info(f"主校验清单生成完毕，共 {len(master_list)} 个待校验项。")
    return master_list


def obclient_run_sql(ob_cfg: ObConfig, sql_query: str) -> Tuple[bool, str, str]:
    """(核心) 运行 obclient CLI 命令并返回 (Success, stdout, stderr)"""
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


def print_final_report(results: ReportResults, total_checked: int):
    """
    (V8) 将最终报告打印到 stdout (简洁美化版，无图标)。
    """
    log.info("所有校验已完成。正在生成最终报告...")
    
    # 统计数据
    ok_count = len(results['ok'])
    missing_count = len(results['missing'])
    mismatched_count = len(results['mismatched'])
    extraneous_count = len(results['extraneous'])

    # 打印到 stdout
    print("\n\n" + f"{Color.BOLD}{'='*80}{Color.ENDC}")
    print(f"           {Color.BOLD}数据库对象迁移校验报告 (V9){Color.ENDC}")
    print(f"{Color.BOLD}{'='*80}{Color.ENDC}")

    # --- 0. (V8) 概要总结 ---
    print(f"\n{Color.BOLD}[ 综合概要 ]{Color.ENDC}")
    print(f"  - 总计校验对象 (来自源库): {total_checked}")
    print(f"  - {Color.GREEN}一致:{Color.ENDC}       {ok_count}")
    print(f"  - {Color.RED}缺失:{Color.ENDC}       {missing_count}")
    print(f"  - {Color.YELLOW}不匹配:{Color.ENDC}   {mismatched_count}")
    print(f"  - {Color.YELLOW}无效规则:{Color.ENDC} {extraneous_count}")
    print("-" * 80)

    # --- 1. 缺失报告 ---
    print(f"\n{Color.BOLD}--- 1. [缺失的对象] (在 OceanBase 中未找到) --- (共 {missing_count} 个){Color.ENDC}")
    if results['missing']:
        for (obj_type, tgt_name, src_name) in results['missing']:
            print(f"    {Color.RED}[{obj_type}]{Color.ENDC} {tgt_name} {Color.YELLOW}(源: {src_name}){Color.ENDC}")
    else:
        print("    (无)")

    # --- 2. 不匹配报告 ---
    print(f"\n{Color.BOLD}--- 2. [列名不匹配的表] --- (共 {mismatched_count} 个){Color.ENDC}")
    if results['mismatched']:
        for (obj_type, tgt_name, missing, extra) in results['mismatched']:
            
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
    if results['ok']:
        for (obj_type, tgt_name) in results['ok'][:50]:
            print(f"    {Color.GREEN}[{obj_type}]{Color.ENDC} {tgt_name}")
        if len(results['ok']) > 50:
            print(f"    ... (及其他 {len(results['ok']) - 50} 个对象)")
    else:
        print("    (无)")

    # --- 4. 无效规则报告 ---
    print(f"\n{Color.BOLD}--- 4. [无效的 Remap 规则] --- (共 {extraneous_count} 个){Color.ENDC}")
    if results['extraneous']:
        print("    (下列对象在 remap_rules.txt 中定义, 但在源端 Oracle (db.ini 中配置的 schema) 中未找到)")
        for item in results['extraneous']:
            print(f"    {Color.YELLOW}{item}{Color.ENDC}")
    else:
        print("    (无)")
        
    print("\n" + f"{Color.BOLD}{'='*80}{Color.ENDC}")
    print("报告结束。")


def main():
    """主执行函数"""
    CONFIG_FILE = 'db.ini'
    
    # 步骤 1: 加载配置
    ora_cfg, ob_cfg, settings = load_config(CONFIG_FILE)
    
    # 步骤 2: 加载 Remap 规则
    remap_rules = load_remap_rules(settings['remap_file'])
    
    # 步骤 3: 加载源端全量对象
    source_objects = get_source_objects(ora_cfg, settings['source_schemas_list'])
    
    # 步骤 4: (V6) 验证 Remap 规则
    extraneous_rules = validate_remap_rules(remap_rules, source_objects)
    
    # 步骤 5: 生成主校验清单 (包含 V5 检查)
    master_list = generate_master_list(source_objects, remap_rules)
    
    if not master_list:
        log.info("主校验清单为空，没有需要校验的对象。程序退出。")
        return

    # 步骤 6: 执行批量验证
    log.info("--- 开始执行批量验证 (可能需要几分钟...) ---")
    
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
                
                if (i+1) % 100 == 0:
                    log.info(f"  进度: {i+1} / {total} ...")
                
                try:
                    tgt_schema, tgt_obj_name = tgt_name.split('.')
                except ValueError:
                    log.warning(f"  [跳过] 目标名 '{tgt_name}' 格式不正确 (源: {src_name})")
                    continue
                
                # --- 分发器 ---
                if obj_type == 'VIEW':
                    exists = obclient_check_exists(ob_cfg, tgt_schema, tgt_obj_name, 'ALL_VIEWS', 'VIEW_NAME')
                    if not exists:
                        results['missing'].append( (obj_type, tgt_name, src_name) )
                    else:
                        results['ok'].append( (obj_type, tgt_name) )
                
                elif obj_type == 'TABLE':
                    exists = obclient_check_exists(ob_cfg, tgt_schema, tgt_obj_name, 'ALL_TABLES', 'TABLE_NAME')
                    
                    if not exists:
                        results['missing'].append( (obj_type, tgt_name, src_name) )
                        continue
                    
                    # --- (V9) 列名对比 (带过滤器) ---
                    src_schema, src_obj_name = src_name.split('.')
                    
                    # 1. 获取源列
                    src_cols = oracle_get_columns(ora_conn, src_schema, src_obj_name)
                    # 2. 获取目标原始列
                    tgt_cols_raw = obclient_get_columns(ob_cfg, tgt_schema, tgt_obj_name)
                    
                    if src_cols is None or tgt_cols_raw is None:
                        # 检查失败 (错误已在辅助函数中记录)
                        results['mismatched'].append( (obj_type, f"{tgt_name} (列检查失败)", set(), set()) )
                        continue
                    
                    # 3. (V9 新增) 过滤掉 'OMS_' 开头的列
                    tgt_cols = {
                        col for col in tgt_cols_raw 
                        if not col.upper().startswith('OMS_')
                    }
                    
                    # 4. 对比
                    if src_cols == tgt_cols:
                        results['ok'].append( (obj_type, tgt_name) )
                    else:
                        missing_in_tgt = src_cols - tgt_cols
                        extra_in_tgt = tgt_cols - src_cols
                        results['mismatched'].append( (obj_type, tgt_name, missing_in_tgt, extra_in_tgt) )

    except oracledb.Error as e:
        log.error(f"严重错误: 无法维护 Oracle 的持久连接: {e}")
        sys.exit(1)
    
    # 步骤 7: 打印最终报告
    print_final_report(results, len(master_list))


if __name__ == "__main__":
    main()

