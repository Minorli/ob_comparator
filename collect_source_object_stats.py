#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Collect source object counts from Oracle based on config.ini.

Outputs:
- Total counts by object type
- Counts by schema (owner)
- Optional per-table stats for indexes/constraints/triggers

Usage:
  python3 collect_source_object_stats.py [config.ini]
  python3 collect_source_object_stats.py [config.ini] --table-stats --top-n 20
"""

from __future__ import annotations

import argparse
import configparser
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

try:
    import oracledb
except ImportError:
    print("ERROR: oracledb is not installed. Install with: pip install oracledb", file=sys.stderr)
    sys.exit(1)

ORACLE_IN_BATCH_SIZE = 900

OBJECT_TYPES = [
    "TABLE",
    "VIEW",
    "MATERIALIZED VIEW",
    "PROCEDURE",
    "FUNCTION",
    "PACKAGE",
    "PACKAGE BODY",
    "SYNONYM",
    "JOB",
    "SCHEDULE",
    "TYPE",
    "TYPE BODY",
    "TRIGGER",
    "SEQUENCE",
    "INDEX",
    "CONSTRAINT",
]


def chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def build_placeholders(count: int, offset: int = 0) -> str:
    return ",".join(f":{i + 1 + offset}" for i in range(count))


def load_config(path: Path) -> Tuple[Dict[str, str], Dict[str, str], List[str]]:
    parser = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=("#", ";"))
    if not parser.read(path):
        raise ValueError(f"Config file not found or unreadable: {path}")
    if "ORACLE_SOURCE" not in parser:
        raise ValueError("Missing [ORACLE_SOURCE] section in config.ini")

    ora_cfg = dict(parser["ORACLE_SOURCE"])
    settings = dict(parser["SETTINGS"]) if parser.has_section("SETTINGS") else {}

    schemas_raw = settings.get("source_schemas", "")
    schemas = [s.strip().upper() for s in schemas_raw.split(",") if s.strip()]
    if not schemas:
        raise ValueError("[SETTINGS].source_schemas is empty")

    return ora_cfg, settings, schemas


def init_oracle_client(settings: Dict[str, str]) -> None:
    client_dir = (settings.get("oracle_client_lib_dir") or "").strip()
    if not client_dir:
        return
    try:
        oracledb.init_oracle_client(lib_dir=str(Path(client_dir).expanduser()))
    except oracledb.Error as exc:
        raise RuntimeError(f"Failed to init Oracle Instant Client: {exc}") from exc


def fetch_object_counts(
    conn: "oracledb.Connection",
    owners: List[str],
    object_types: List[str],
) -> Dict[str, Dict[str, int]]:
    results: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    obj_clause = ",".join(f"'{t}'" for t in object_types)

    sql_tpl = f"""
        SELECT OWNER, OBJECT_TYPE, COUNT(*)
        FROM DBA_OBJECTS
        WHERE OWNER IN ({{owners}})
          AND OBJECT_TYPE IN ({obj_clause})
        GROUP BY OWNER, OBJECT_TYPE
    """

    with conn.cursor() as cursor:
        for chunk in chunk_list(owners, ORACLE_IN_BATCH_SIZE):
            owners_clause = build_placeholders(len(chunk))
            sql = sql_tpl.format(owners=owners_clause)
            cursor.execute(sql, chunk)
            for owner, obj_type, count in cursor.fetchall():
                owner_u = (owner or "").strip().upper()
                obj_type_u = (obj_type or "").strip().upper()
                if not owner_u or not obj_type_u:
                    continue
                results[owner_u][obj_type_u] += int(count or 0)

    return results


def fetch_table_group_counts(
    conn: "oracledb.Connection",
    owners: List[str],
    sql_tpl: str,
) -> Dict[str, int]:
    results: Dict[str, int] = defaultdict(int)
    with conn.cursor() as cursor:
        for chunk in chunk_list(owners, ORACLE_IN_BATCH_SIZE):
            owners_clause = build_placeholders(len(chunk))
            sql = sql_tpl.format(owners=owners_clause)
            cursor.execute(sql, chunk)
            for owner, table_name, count in cursor.fetchall():
                owner_u = (owner or "").strip().upper()
                table_u = (table_name or "").strip().upper()
                if not owner_u or not table_u:
                    continue
                results[f"{owner_u}.{table_u}"] += int(count or 0)
    return results


def fetch_public_synonym_count(
    conn: "oracledb.Connection",
    target_owners: List[str],
) -> int:
    if not target_owners:
        return 0
    total = 0
    sql_tpl = """
        SELECT COUNT(*)
        FROM DBA_SYNONYMS
        WHERE OWNER = 'PUBLIC'
          AND TABLE_OWNER IN ({owners})
          AND TABLE_NAME IS NOT NULL
    """
    with conn.cursor() as cursor:
        for chunk in chunk_list(target_owners, ORACLE_IN_BATCH_SIZE):
            owners_clause = build_placeholders(len(chunk))
            sql = sql_tpl.format(owners=owners_clause)
            cursor.execute(sql, chunk)
            row = cursor.fetchone()
            if row and row[0] is not None:
                total += int(row[0])
    return total


def summarize_table_stats(label: str, data: Dict[str, int], top_n: int) -> None:
    print(f"\n## {label}_TABLE_STATS")
    if not data:
        print("no data")
        return
    counts = list(data.values())
    counts.sort()
    total = sum(counts)
    table_count = len(counts)
    avg = total / table_count if table_count else 0
    p95 = counts[int(len(counts) * 0.95) - 1] if counts else 0
    p99 = counts[int(len(counts) * 0.99) - 1] if counts else 0
    print(f"tables_with_{label.lower()}: {table_count}")
    print(f"total_{label.lower()}: {total}")
    print(f"avg_per_table: {avg:.2f}")
    print(f"p95: {p95}")
    print(f"p99: {p99}")
    print(f"max: {counts[-1] if counts else 0}")

    if top_n > 0:
        print(f"\n# top {top_n} tables by {label.lower()} count")
        top_items = sorted(data.items(), key=lambda x: x[1], reverse=True)[:top_n]
        for full_name, cnt in top_items:
            print(f"{full_name}\t{cnt}")


def summarize_table_stats_brief(label: str, data: Dict[str, int], top_n: int) -> None:
    print(f"{label}_TABLE_STATS")
    if not data:
        print("  no data")
        return
    counts = list(data.values())
    counts.sort()
    total = sum(counts)
    table_count = len(counts)
    avg = total / table_count if table_count else 0
    p95 = counts[int(len(counts) * 0.95) - 1] if counts else 0
    p99 = counts[int(len(counts) * 0.99) - 1] if counts else 0
    print(
        f"  tables={table_count} total={total} avg={avg:.2f} "
        f"p95={p95} p99={p99} max={counts[-1] if counts else 0}"
    )
    if top_n > 0:
        top_items = sorted(data.items(), key=lambda x: x[1], reverse=True)[:top_n]
        for full_name, cnt in top_items:
            print(f"  {full_name}\t{cnt}")


def format_compact_type_counts(counts: Dict[str, int], max_width: int) -> List[str]:
    parts = [f"{name}={counts.get(name, 0)}" for name in OBJECT_TYPES]
    lines: List[str] = []
    line = ""
    for part in parts:
        candidate = part if not line else f"{line}  {part}"
        if len(candidate) > max_width and line:
            lines.append(line)
            line = part
        else:
            line = candidate
    if line:
        lines.append(line)
    return lines


def print_brief_report(
    config_path: Path,
    owners: List[str],
    counts_by_owner: Dict[str, Dict[str, int]],
    max_width: int,
    top_n: int,
    table_stats: bool,
    conn: "oracledb.Connection",
    public_synonym_count: int,
    public_synonym_note: str,
    public_synonym_error: str,
) -> None:
    total_by_type: Dict[str, int] = {}
    totals_by_schema: List[Tuple[str, int]] = []
    total_objects = 0
    for owner in owners:
        per_type = counts_by_owner.get(owner, {})
        schema_total = sum(per_type.get(t, 0) for t in OBJECT_TYPES)
        totals_by_schema.append((owner, schema_total))
        total_objects += schema_total
    for obj_type in OBJECT_TYPES:
        total_by_type[obj_type] = sum(
            counts_by_owner.get(owner, {}).get(obj_type, 0) for owner in owners
        )

    totals_by_schema.sort(key=lambda x: x[1], reverse=True)

    print("BRIEF_SUMMARY")
    print(f"config={config_path}")
    print(f"schemas={len(owners)} object_types={len(OBJECT_TYPES)} total_objects={total_objects}")
    if public_synonym_error:
        print(f"public_synonyms=ERROR {public_synonym_error}")
    else:
        print(f"public_synonyms={public_synonym_count} ({public_synonym_note})")
    print("TOTAL_BY_TYPE")
    for line in format_compact_type_counts(total_by_type, max_width=max_width):
        print(f"  {line}")

    print(f"TOP_SCHEMAS_BY_TOTAL (top {top_n})")
    for owner, total in totals_by_schema[:top_n]:
        print(f"  {owner}\t{total}")

    if table_stats:
        index_sql = """
            SELECT OWNER, TABLE_NAME, COUNT(*)
            FROM DBA_INDEXES
            WHERE OWNER IN ({owners})
              AND TABLE_NAME IS NOT NULL
            GROUP BY OWNER, TABLE_NAME
        """
        constraint_sql = """
            SELECT OWNER, TABLE_NAME, COUNT(*)
            FROM DBA_CONSTRAINTS
            WHERE OWNER IN ({owners})
              AND TABLE_NAME IS NOT NULL
              AND CONSTRAINT_TYPE IN ('P','U','R')
            GROUP BY OWNER, TABLE_NAME
        """
        trigger_sql = """
            SELECT TABLE_OWNER, TABLE_NAME, COUNT(*)
            FROM DBA_TRIGGERS
            WHERE TABLE_OWNER IN ({owners})
              AND TABLE_NAME IS NOT NULL
            GROUP BY TABLE_OWNER, TABLE_NAME
        """
        index_counts = fetch_table_group_counts(conn, owners, index_sql)
        constraint_counts = fetch_table_group_counts(conn, owners, constraint_sql)
        trigger_counts = fetch_table_group_counts(conn, owners, trigger_sql)
        summarize_table_stats_brief("INDEX", index_counts, top_n)
        summarize_table_stats_brief("CONSTRAINT", constraint_counts, top_n)
        summarize_table_stats_brief("TRIGGER", trigger_counts, top_n)


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect Oracle object counts for source schemas.")
    parser.add_argument("config", nargs="?", default="config.ini", help="config.ini path")
    parser.add_argument("--table-stats", action="store_true", help="include per-table stats for index/constraint/trigger")
    parser.add_argument("--top-n", type=int, default=20, help="top N tables to print for table stats")
    parser.add_argument("--brief", action="store_true", help="print a compact one-screen summary")
    parser.add_argument("--brief-width", type=int, default=110, help="max width for compact type summary")
    args = parser.parse_args()

    config_path = Path(args.config).expanduser().resolve()
    try:
        ora_cfg, settings, schemas = load_config(config_path)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    try:
        init_oracle_client(settings)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    try:
        conn = oracledb.connect(
            user=ora_cfg.get("user"),
            password=ora_cfg.get("password"),
            dsn=ora_cfg.get("dsn"),
        )
    except oracledb.Error as exc:
        print(f"ERROR: Oracle connection failed: {exc}", file=sys.stderr)
        return 1

    with conn:
        owner_list = sorted(set(schemas))
        counts_by_owner = fetch_object_counts(conn, owner_list, OBJECT_TYPES)
        public_synonym_count = 0
        public_synonym_error = ""
        public_synonym_note = "via source_schemas"
        public_owner = "PUBLIC"
        if public_owner not in owner_list:
            public_synonym_note = "forced"
            try:
                public_synonym_count = fetch_public_synonym_count(conn, owner_list)
                if public_synonym_count > 0:
                    counts_by_owner[public_owner]["SYNONYM"] += public_synonym_count
                    owner_list = sorted(set(owner_list + [public_owner]))
            except oracledb.Error as exc:
                public_synonym_error = str(exc)

        if args.brief:
            top_n = max(1, min(args.top_n, 20))
            print_brief_report(
                config_path=config_path,
                owners=owner_list,
                counts_by_owner=counts_by_owner,
                max_width=max(60, args.brief_width),
                top_n=top_n,
                table_stats=args.table_stats,
                conn=conn,
                public_synonym_count=public_synonym_count,
                public_synonym_note=public_synonym_note,
                public_synonym_error=public_synonym_error,
            )
            return 0

        print("# SOURCE OBJECT COUNTS")
        print(f"config: {config_path}")
        print(f"schemas: {len(owner_list)}")
        print(f"object_types: {', '.join(OBJECT_TYPES)}")
        if public_synonym_error:
            print(f"public_synonyms: ERROR {public_synonym_error}")
        else:
            print(f"public_synonyms: {public_synonym_count} ({public_synonym_note})")

        # totals by type
        total_by_type: Dict[str, int] = {}
        for obj_type in OBJECT_TYPES:
            total_by_type[obj_type] = sum(
                counts_by_owner.get(owner, {}).get(obj_type, 0) for owner in owner_list
            )

        print("\n## TOTAL_BY_TYPE")
        print("OBJECT_TYPE\tCOUNT")
        for obj_type in OBJECT_TYPES:
            print(f"{obj_type}\t{total_by_type.get(obj_type, 0)}")

        # totals by schema
        print("\n## TOTAL_BY_SCHEMA")
        header = ["SCHEMA", "TOTAL"] + OBJECT_TYPES
        print("\t".join(header))
        for owner in owner_list:
            row = [owner]
            per_type = counts_by_owner.get(owner, {})
            total = sum(per_type.get(t, 0) for t in OBJECT_TYPES)
            row.append(str(total))
            for obj_type in OBJECT_TYPES:
                row.append(str(per_type.get(obj_type, 0)))
            print("\t".join(row))

        if args.table_stats:
            index_sql = """
                SELECT OWNER, TABLE_NAME, COUNT(*)
                FROM DBA_INDEXES
                WHERE OWNER IN ({owners})
                  AND TABLE_NAME IS NOT NULL
                GROUP BY OWNER, TABLE_NAME
            """
            constraint_sql = """
                SELECT OWNER, TABLE_NAME, COUNT(*)
                FROM DBA_CONSTRAINTS
                WHERE OWNER IN ({owners})
                  AND TABLE_NAME IS NOT NULL
                  AND CONSTRAINT_TYPE IN ('P','U','R')
                GROUP BY OWNER, TABLE_NAME
            """
            trigger_sql = """
                SELECT TABLE_OWNER, TABLE_NAME, COUNT(*)
                FROM DBA_TRIGGERS
                WHERE TABLE_OWNER IN ({owners})
                  AND TABLE_NAME IS NOT NULL
                GROUP BY TABLE_OWNER, TABLE_NAME
            """

            index_counts = fetch_table_group_counts(conn, owner_list, index_sql)
            constraint_counts = fetch_table_group_counts(conn, owner_list, constraint_sql)
            trigger_counts = fetch_table_group_counts(conn, owner_list, trigger_sql)

            summarize_table_stats("INDEX", index_counts, args.top_n)
            summarize_table_stats("CONSTRAINT", constraint_counts, args.top_n)
            summarize_table_stats("TRIGGER", trigger_counts, args.top_n)

    return 0


if __name__ == "__main__":
    sys.exit(main())
