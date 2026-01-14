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
Initialize users and roles in OceanBase using Oracle as the source of truth.

Reads config.ini ([ORACLE_SOURCE], [OCEANBASE_TARGET], [SETTINGS]) and:
1) Fetches non-system users/roles from Oracle (ORACLE_MAINTAINED='N').
2) Writes local DDL files for users/roles/grants.
3) Creates missing users/roles and applies role/system grants via obclient.
"""

from __future__ import annotations

import argparse
import configparser
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

try:
    import oracledb
except ImportError:
    oracledb = None

__version__ = "0.9.7"
__author__ = "Minor Li"
REPO_URL = "https://github.com/Minorli/ob_comparator"
REPO_ISSUES_URL = f"{REPO_URL}/issues"

CONFIG_DEFAULT_PATH = "config.ini"
DEFAULT_OUTPUT_SUBDIR = "init_users_roles"
DEFAULT_OBCLIENT_TIMEOUT = 60
DEFAULT_FIXUP_TIMEOUT = 3600

LOG_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FILE_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"

IDENT_SIMPLE_RE = re.compile(r"^[A-Z][A-Z0-9_$#]*$")


def init_console_logging(level: int) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    for handler in list(root_logger.handlers):
        if isinstance(handler, logging.FileHandler):
            continue
        root_logger.removeHandler(handler)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(LOG_FILE_FORMAT, datefmt=LOG_TIME_FORMAT))
    root_logger.addHandler(handler)


log = logging.getLogger(__name__)


def load_config(config_path: Path) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], Path, int, Optional[int]]:
    parser = configparser.ConfigParser()
    if not parser.read(config_path):
        raise ValueError(f"Config file not found or unreadable: {config_path}")

    if "ORACLE_SOURCE" not in parser or "OCEANBASE_TARGET" not in parser:
        raise ValueError("config.ini must include [ORACLE_SOURCE] and [OCEANBASE_TARGET].")

    ora_cfg = dict(parser["ORACLE_SOURCE"])
    ob_cfg = dict(parser["OCEANBASE_TARGET"])
    settings = dict(parser["SETTINGS"]) if parser.has_section("SETTINGS") else {}

    repo_root = config_path.parent.resolve()
    fixup_dir = (settings.get("fixup_dir") or "fixup_scripts").strip() or "fixup_scripts"
    output_dir = (repo_root / fixup_dir / DEFAULT_OUTPUT_SUBDIR).resolve()

    try:
        ob_timeout = int(settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT))
    except Exception:
        ob_timeout = DEFAULT_OBCLIENT_TIMEOUT

    try:
        fixup_timeout = int(settings.get("fixup_cli_timeout", DEFAULT_FIXUP_TIMEOUT))
        if fixup_timeout < 0:
            fixup_timeout = DEFAULT_FIXUP_TIMEOUT
    except Exception:
        fixup_timeout = DEFAULT_FIXUP_TIMEOUT

    ddl_timeout = None if fixup_timeout == 0 else fixup_timeout
    return ora_cfg, ob_cfg, settings, output_dir, ob_timeout, ddl_timeout


def init_oracle_client(settings: Dict[str, str]) -> None:
    client_dir = (settings.get("oracle_client_lib_dir") or "").strip()
    if not client_dir:
        return
    try:
        oracledb.init_oracle_client(lib_dir=str(Path(client_dir).expanduser()))
    except oracledb.Error as exc:
        raise RuntimeError(f"Failed to init Oracle Instant Client: {exc}") from exc


def build_obclient_command(ob_cfg: Dict[str, str]) -> List[str]:
    required = ["executable", "host", "port", "user_string", "password"]
    missing = [key for key in required if not (ob_cfg.get(key) or "").strip()]
    if missing:
        raise ValueError(f"[OCEANBASE_TARGET] missing keys: {', '.join(missing)}")

    ob_cfg["port"] = str(int(ob_cfg["port"]))
    return [
        ob_cfg["executable"],
        "-h", ob_cfg["host"],
        "-P", ob_cfg["port"],
        "-u", ob_cfg["user_string"],
        f"-p{ob_cfg['password']}",
        "--prompt", "init>",
        "--silent",
    ]


def run_sql(
    obclient_cmd: Sequence[str],
    sql_text: str,
    timeout: Optional[int],
) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(obclient_cmd),
        input=sql_text,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


def run_query_lines(
    obclient_cmd: Sequence[str],
    sql_text: str,
    timeout: Optional[int],
) -> Tuple[bool, List[str], str]:
    try:
        result = run_sql(obclient_cmd, sql_text, timeout)
    except subprocess.TimeoutExpired:
        return False, [], "TimeoutExpired"
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        return False, [], stderr or "execution failed"
    lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    return True, lines, ""


def query_single_column(
    obclient_cmd: Sequence[str],
    sql_text: str,
    timeout: Optional[int],
    column_name: str,
) -> Set[str]:
    ok, lines, err = run_query_lines(obclient_cmd, sql_text, timeout)
    if not ok:
        log.warning("OB query failed: %s", err)
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


def query_rows(
    obclient_cmd: Sequence[str],
    sql_text: str,
    timeout: Optional[int],
    columns: Sequence[str],
) -> List[Tuple[str, ...]]:
    ok, lines, err = run_query_lines(obclient_cmd, sql_text, timeout)
    if not ok:
        log.warning("OB query failed: %s", err)
        return []
    col_upper = [col.strip().upper() for col in columns]
    rows: List[Tuple[str, ...]] = []
    for line in lines:
        parts = [part.strip() for part in line.split("\t")]
        if len(parts) < len(col_upper):
            continue
        if [p.upper() for p in parts[:len(col_upper)]] == col_upper:
            continue
        rows.append(tuple(parts[:len(col_upper)]))
    return rows


def identifier_needs_quotes(name: str) -> bool:
    return not (name.isupper() and IDENT_SIMPLE_RE.match(name))


def format_identifier(name: str) -> str:
    if not name:
        return name
    if identifier_needs_quotes(name):
        escaped = name.replace('"', '""')
        return f"\"{escaped}\""
    return name.upper()


def format_password(password: str) -> str:
    escaped = password.replace('"', '""')
    return f"\"{escaped}\""


def normalize_admin_option(value: Optional[str]) -> str:
    if not value:
        return "NO"
    return "YES" if value.strip().upper().startswith("Y") else "NO"


def admin_option_clause(value: Optional[str]) -> str:
    return " WITH ADMIN OPTION" if normalize_admin_option(value) == "YES" else ""


def has_oracle_maintained_column(conn: "oracledb.Connection", table_name: str) -> bool:
    sql = """
        SELECT COUNT(1)
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = 'SYS'
          AND TABLE_NAME = :table_name
          AND COLUMN_NAME = 'ORACLE_MAINTAINED'
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, table_name=table_name.upper())
        row = cursor.fetchone()
        return bool(row and row[0])


def fetch_oracle_users(conn: "oracledb.Connection") -> List[str]:
    sql = """
        SELECT USERNAME
        FROM DBA_USERS
        WHERE ORACLE_MAINTAINED = 'N'
        ORDER BY USERNAME
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall() if row and row[0]]


def fetch_oracle_roles(conn: "oracledb.Connection") -> List[str]:
    sql = """
        SELECT ROLE
        FROM DBA_ROLES
        WHERE ORACLE_MAINTAINED = 'N'
        ORDER BY ROLE
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall() if row and row[0]]


def fetch_oracle_roles_fallback(conn: "oracledb.Connection") -> List[str]:
    sql = """
        SELECT ROLE
        FROM DBA_ROLES
        ORDER BY ROLE
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall() if row and row[0]]


def fetch_oracle_role_grants(
    conn: "oracledb.Connection",
    allowed_grantees: Set[str],
    allowed_roles: Set[str],
    use_maintained_filter: bool,
) -> List[Tuple[str, str, str]]:
    if use_maintained_filter:
        sql = """
            SELECT GRANTEE, GRANTED_ROLE, ADMIN_OPTION
            FROM DBA_ROLE_PRIVS
            WHERE (GRANTEE IN (SELECT USERNAME FROM DBA_USERS WHERE ORACLE_MAINTAINED = 'N')
                OR GRANTEE IN (SELECT ROLE FROM DBA_ROLES WHERE ORACLE_MAINTAINED = 'N'))
              AND GRANTED_ROLE IN (SELECT ROLE FROM DBA_ROLES WHERE ORACLE_MAINTAINED = 'N')
            ORDER BY GRANTEE, GRANTED_ROLE
        """
    else:
        sql = """
            SELECT GRANTEE, GRANTED_ROLE, ADMIN_OPTION
            FROM DBA_ROLE_PRIVS
            ORDER BY GRANTEE, GRANTED_ROLE
        """
    results: List[Tuple[str, str, str]] = []
    with conn.cursor() as cursor:
        cursor.execute(sql)
        for grantee, granted_role, admin_option in cursor.fetchall():
            if not grantee or not granted_role:
                continue
            grantee_u = grantee.upper()
            role_u = granted_role.upper()
            if grantee_u not in allowed_grantees or role_u not in allowed_roles:
                continue
            results.append((grantee_u, role_u, normalize_admin_option(admin_option)))
    return results


def fetch_oracle_sys_privs(
    conn: "oracledb.Connection",
    allowed_grantees: Set[str],
    use_maintained_filter: bool,
) -> List[Tuple[str, str, str]]:
    if use_maintained_filter:
        sql = """
            SELECT GRANTEE, PRIVILEGE, ADMIN_OPTION
            FROM DBA_SYS_PRIVS
            WHERE GRANTEE IN (SELECT USERNAME FROM DBA_USERS WHERE ORACLE_MAINTAINED = 'N')
               OR GRANTEE IN (SELECT ROLE FROM DBA_ROLES WHERE ORACLE_MAINTAINED = 'N')
            ORDER BY GRANTEE, PRIVILEGE
        """
    else:
        sql = """
            SELECT GRANTEE, PRIVILEGE, ADMIN_OPTION
            FROM DBA_SYS_PRIVS
            ORDER BY GRANTEE, PRIVILEGE
        """
    results: List[Tuple[str, str, str]] = []
    with conn.cursor() as cursor:
        cursor.execute(sql)
        for grantee, privilege, admin_option in cursor.fetchall():
            if not grantee or not privilege:
                continue
            grantee_u = grantee.upper()
            if grantee_u not in allowed_grantees:
                continue
            results.append((grantee_u, privilege.strip().upper(), normalize_admin_option(admin_option)))
    return results


def fetch_oracle_users_fallback(conn: "oracledb.Connection") -> List[str]:
    exact_blacklist = {
        "ANONYMOUS", "APPQOSSYS", "AUDSYS", "CTXSYS", "DBSNMP", "DIP", "DMSYS", "DVF",
        "DVSYS", "EXFSYS", "LBACSYS", "MGMT_VIEW", "OJVMSYS", "OLAPSYS", "ORACLE_OCM",
        "OUTLN", "OWBSYS", "SI_INFORMTN_SCHEMA", "SYS", "SYSMAN", "SYSTEM", "TSMSYS",
        "WMSYS", "XDB", "XS$NULL",
    }
    like_blacklist = [
        "APEX_%", "FLOWS_%", "GSM%", "MD%", "ORD%", "WK%",
    ]
    conditions = []
    if exact_blacklist:
        in_list = ", ".join(f"'{name}'" for name in sorted(exact_blacklist))
        conditions.append(f"USERNAME NOT IN ({in_list})")
    for pattern in like_blacklist:
        conditions.append(f"USERNAME NOT LIKE '{pattern}'")
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT USERNAME
        FROM DBA_USERS
        WHERE {where_clause}
        ORDER BY USERNAME
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall() if row and row[0]]


def write_sql_file(path: Path, statements: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue
            if not stmt.endswith(";"):
                stmt += ";"
            handle.write(stmt + "\n")


def is_already_exists_error(stderr: str) -> bool:
    msg = (stderr or "").upper()
    return "ORA-01920" in msg or "ORA-01921" in msg or "ALREADY EXISTS" in msg


def load_existing_role_grants(
    obclient_cmd: Sequence[str],
    timeout: Optional[int],
) -> Dict[Tuple[str, str], Set[str]]:
    sql = "SELECT GRANTEE, GRANTED_ROLE, ADMIN_OPTION FROM DBA_ROLE_PRIVS;"
    rows = query_rows(obclient_cmd, sql, timeout, ("GRANTEE", "GRANTED_ROLE", "ADMIN_OPTION"))
    grants: Dict[Tuple[str, str], Set[str]] = {}
    for grantee, role, admin_option in rows:
        key = (grantee.upper(), role.upper())
        grants.setdefault(key, set()).add(normalize_admin_option(admin_option))
    return grants


def load_existing_sys_privs(
    obclient_cmd: Sequence[str],
    timeout: Optional[int],
) -> Dict[Tuple[str, str], Set[str]]:
    sql = "SELECT GRANTEE, PRIVILEGE, ADMIN_OPTION FROM DBA_SYS_PRIVS;"
    rows = query_rows(obclient_cmd, sql, timeout, ("GRANTEE", "PRIVILEGE", "ADMIN_OPTION"))
    grants: Dict[Tuple[str, str], Set[str]] = {}
    for grantee, privilege, admin_option in rows:
        key = (grantee.upper(), privilege.upper())
        grants.setdefault(key, set()).add(normalize_admin_option(admin_option))
    return grants


def grant_satisfied(existing: Dict[Tuple[str, str], Set[str]], grantee: str, item: str, require_admin: bool) -> bool:
    options = existing.get((grantee.upper(), item.upper()))
    if not options:
        return False
    if require_admin:
        return "YES" in options
    return True


def execute_statements(
    obclient_cmd: Sequence[str],
    statements: Sequence[str],
    timeout: Optional[int],
    label: str,
) -> Tuple[int, int]:
    created = 0
    skipped = 0
    total = len(statements)
    for idx, statement in enumerate(statements, start=1):
        log.info("[%s] %d/%d executing: %s", label, idx, total, statement)
        try:
            result = run_sql(obclient_cmd, statement, timeout)
        except subprocess.TimeoutExpired:
            log.error("[%s] timeout: %s", label, statement)
            continue
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            if is_already_exists_error(stderr):
                log.info("[%s] already exists, skip: %s", label, statement)
                skipped += 1
                continue
            log.error("[%s] failed: %s", label, stderr or "unknown error")
            continue
        created += 1
    return created, skipped


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize users and roles in OceanBase.")
    parser.add_argument("config", nargs="?", default=CONFIG_DEFAULT_PATH, help="config.ini path")
    parser.add_argument("--output-dir", default="", help="override output directory")
    args = parser.parse_args()

    if oracledb is None:
        print("Error: missing 'oracledb' package.", file=sys.stderr)
        print("Install with: pip install oracledb", file=sys.stderr)
        return 1

    config_path = Path(args.config).expanduser()
    try:
        ora_cfg, ob_cfg, settings, output_dir, ob_timeout, ddl_timeout = load_config(config_path)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    log_level = (settings.get("log_level") or "INFO").strip().upper()
    init_console_logging(getattr(logging, log_level, logging.INFO))
    log.info("init_users_roles v%s", __version__)
    log.info("repo: %s", REPO_URL)

    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()

    try:
        init_oracle_client(settings)
    except RuntimeError as exc:
        log.error("%s", exc)
        return 1

    try:
        obclient_cmd = build_obclient_command(ob_cfg)
    except ValueError as exc:
        log.error("%s", exc)
        return 1

    ob_executable = Path(ob_cfg["executable"]).expanduser()
    if not ob_executable.exists():
        log.error("obclient executable not found: %s", ob_executable)
        return 1

    password_literal = format_password("Ob@sx2025")

    try:
        with oracledb.connect(
            user=ora_cfg.get("user"),
            password=ora_cfg.get("password"),
            dsn=ora_cfg.get("dsn"),
        ) as conn:
            has_maintained_users = has_oracle_maintained_column(conn, "DBA_USERS")
            has_maintained_roles = has_oracle_maintained_column(conn, "DBA_ROLES")
            use_maintained_filter = has_maintained_users and has_maintained_roles

            if has_maintained_users:
                users = fetch_oracle_users(conn)
            else:
                log.warning("DBA_USERS missing ORACLE_MAINTAINED, using blacklist fallback.")
                users = fetch_oracle_users_fallback(conn)

            if has_maintained_roles:
                roles = fetch_oracle_roles(conn)
            else:
                log.warning("DBA_ROLES missing ORACLE_MAINTAINED, roles are unfiltered.")
                roles = fetch_oracle_roles_fallback(conn)

            users_map = {name.upper(): name for name in users}
            roles_map = {name.upper(): name for name in roles}
            allowed_grantees = set(users_map) | set(roles_map)
            allowed_roles = set(roles_map)

            role_grants = fetch_oracle_role_grants(
                conn,
                allowed_grantees=allowed_grantees,
                allowed_roles=allowed_roles,
                use_maintained_filter=use_maintained_filter,
            )
            sys_privs = fetch_oracle_sys_privs(
                conn,
                allowed_grantees=allowed_grantees,
                use_maintained_filter=use_maintained_filter,
            )
    except oracledb.Error as exc:
        log.error("Oracle connection/query failed: %s", exc)
        return 1

    log.info("Oracle users: %d", len(users))
    log.info("Oracle roles: %d", len(roles))
    log.info("Oracle role grants: %d", len(role_grants))
    log.info("Oracle system privileges: %d", len(sys_privs))

    existing_users = query_single_column(
        obclient_cmd,
        "SELECT USERNAME FROM DBA_USERS;",
        ob_timeout,
        "USERNAME",
    )
    existing_roles = query_single_column(
        obclient_cmd,
        "SELECT ROLE FROM DBA_ROLES;",
        ob_timeout,
        "ROLE",
    )

    user_statements: List[str] = []
    role_statements: List[str] = []
    for role in roles:
        role_u = role.upper()
        if role_u in existing_roles:
            continue
        role_statements.append(f"CREATE ROLE {format_identifier(role)}")

    for user in users:
        user_u = user.upper()
        if user_u in existing_users:
            continue
        user_statements.append(
            f"CREATE USER {format_identifier(user)} IDENTIFIED BY {password_literal}"
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    roles_file = output_dir / "01_create_roles.sql"
    users_file = output_dir / "02_create_users.sql"
    role_grants_file = output_dir / "03_grant_roles.sql"
    sys_privs_file = output_dir / "04_grant_sys_privs.sql"
    write_sql_file(roles_file, role_statements)
    write_sql_file(users_file, user_statements)

    log.info("Roles to create: %d (written to %s)", len(role_statements), roles_file)
    log.info("Users to create: %d (written to %s)", len(user_statements), users_file)

    if role_statements:
        created_roles, skipped_roles = execute_statements(
            obclient_cmd, [stmt + ";" for stmt in role_statements], ddl_timeout, "ROLE"
        )
        log.info("ROLE done: success=%d, skipped=%d", created_roles, skipped_roles)
    if user_statements:
        created_users, skipped_users = execute_statements(
            obclient_cmd, [stmt + ";" for stmt in user_statements], ddl_timeout, "USER"
        )
        log.info("USER done: success=%d, skipped=%d", created_users, skipped_users)

    existing_users = query_single_column(
        obclient_cmd,
        "SELECT USERNAME FROM DBA_USERS;",
        ob_timeout,
        "USERNAME",
    )
    existing_roles = query_single_column(
        obclient_cmd,
        "SELECT ROLE FROM DBA_ROLES;",
        ob_timeout,
        "ROLE",
    )
    existing_principals = existing_users | existing_roles

    existing_role_grants = load_existing_role_grants(obclient_cmd, ob_timeout)
    existing_sys_privs = load_existing_sys_privs(obclient_cmd, ob_timeout)

    role_grant_entries: Set[Tuple[str, str, str]] = set()
    sys_priv_entries: Set[Tuple[str, str, str]] = set()
    skipped_missing_grantee = 0
    skipped_missing_role = 0

    for grantee_u, role_u, admin_option in role_grants:
        if grantee_u not in existing_principals:
            skipped_missing_grantee += 1
            continue
        if role_u not in existing_roles:
            skipped_missing_role += 1
            continue
        require_admin = admin_option == "YES"
        if grant_satisfied(existing_role_grants, grantee_u, role_u, require_admin):
            continue
        role_grant_entries.add((grantee_u, role_u, admin_option))

    for grantee_u, privilege, admin_option in sys_privs:
        if grantee_u not in existing_principals:
            skipped_missing_grantee += 1
            continue
        require_admin = admin_option == "YES"
        if grant_satisfied(existing_sys_privs, grantee_u, privilege, require_admin):
            continue
        sys_priv_entries.add((grantee_u, privilege, admin_option))

    if skipped_missing_grantee or skipped_missing_role:
        log.warning(
            "Skipped grants due to missing principals/roles: grantee=%d, role=%d",
            skipped_missing_grantee,
            skipped_missing_role,
        )

    role_grant_statements: List[str] = []
    for grantee_u, role_u, admin_option in sorted(role_grant_entries):
        grantee_name = users_map.get(grantee_u) or roles_map.get(grantee_u) or grantee_u
        role_name = roles_map.get(role_u) or role_u
        stmt = f"GRANT {format_identifier(role_name)} TO {format_identifier(grantee_name)}"
        stmt += admin_option_clause(admin_option)
        role_grant_statements.append(stmt)

    sys_priv_statements: List[str] = []
    for grantee_u, privilege, admin_option in sorted(sys_priv_entries):
        grantee_name = users_map.get(grantee_u) or roles_map.get(grantee_u) or grantee_u
        stmt = f"GRANT {privilege} TO {format_identifier(grantee_name)}"
        stmt += admin_option_clause(admin_option)
        sys_priv_statements.append(stmt)

    write_sql_file(role_grants_file, role_grant_statements)
    write_sql_file(sys_privs_file, sys_priv_statements)
    log.info("Role grants to apply: %d (written to %s)", len(role_grant_statements), role_grants_file)
    log.info("Sys privs to apply: %d (written to %s)", len(sys_priv_statements), sys_privs_file)

    if role_grant_statements:
        created_roles, skipped_roles = execute_statements(
            obclient_cmd, [stmt + ";" for stmt in role_grant_statements], ddl_timeout, "GRANT_ROLE"
        )
        log.info("GRANT_ROLE done: success=%d, skipped=%d", created_roles, skipped_roles)

    if sys_priv_statements:
        created_privs, skipped_privs = execute_statements(
            obclient_cmd, [stmt + ";" for stmt in sys_priv_statements], ddl_timeout, "GRANT_SYS"
        )
        log.info("GRANT_SYS done: success=%d, skipped=%d", created_privs, skipped_privs)

    return 0


if __name__ == "__main__":
    sys.exit(main())
