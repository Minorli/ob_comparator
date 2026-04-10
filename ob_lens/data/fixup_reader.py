"""Read fixup_scripts/ directory and execution state ledger."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

# Directories that run_fixup.py processes by default (not exhaustive, used for display)
DEFAULT_EXECUTE_DIRS = {
    "table_alter", "view", "procedure", "function", "package", "package_body",
    "type", "type_body", "index", "constraint", "sequence", "trigger",
    "synonym", "job", "schedule", "grants_miss", "compile", "status",
    "name_collision",
}

# Directories that require explicit opt-in
OPT_IN_DIRS = {"table", "grants_deferred", "grants_revoke", "grants_all",
               "cleanup_candidates", "unsupported", "constraint_validate_later"}


@dataclass
class FixupScript:
    rel_path: str           # "table/ZZ_APP.T_ORDER.sql"
    abs_path: Path
    dir_name: str           # "table"
    schema: str             # "ZZ_APP"
    obj_name: str           # "T_ORDER"
    completed: bool
    completed_at: str       # "2026-03-01 10:00:00" or ""
    note: str               # optional note from ledger
    requires_opt_in: bool   # True for table/ grants_deferred/ etc.


def load_ledger(fixup_dir: Path) -> Dict[str, dict]:
    """Load .fixup_state_ledger.json. Returns {} if not found or invalid."""
    ledger_path = fixup_dir / ".fixup_state_ledger.json"
    if not ledger_path.exists():
        return {}
    try:
        data = json.loads(ledger_path.read_text(encoding="utf-8"))
        return data.get("completed", {})
    except (json.JSONDecodeError, KeyError):
        return {}


def _parse_sql_name(filename: str):
    """'ZZ_APP.T_ORDER.sql' -> ('ZZ_APP', 'T_ORDER')"""
    stem = filename.removesuffix(".sql")
    if "." in stem:
        parts = stem.split(".", 1)
        return parts[0], parts[1]
    return "", stem


def list_fixup_scripts(fixup_dir: Path) -> List[FixupScript]:
    """List all *.sql files under fixup_dir with their execution status."""
    if not fixup_dir.exists():
        return []

    ledger = load_ledger(fixup_dir)
    scripts: List[FixupScript] = []

    for sql_file in sorted(fixup_dir.rglob("*.sql")):
        try:
            rel = sql_file.relative_to(fixup_dir)
        except ValueError:
            continue
        rel_str = str(rel).replace("\\", "/")
        dir_name = rel.parts[0] if len(rel.parts) > 1 else ""
        schema, obj_name = _parse_sql_name(sql_file.name)

        entry = ledger.get(rel_str, {})
        completed = bool(entry)
        # Validate fingerprint
        if completed:
            current_fp = hashlib.sha1(
                sql_file.read_bytes()
            ).hexdigest()
            if current_fp != entry.get("fingerprint", ""):
                completed = False  # file changed since execution

        scripts.append(FixupScript(
            rel_path=rel_str,
            abs_path=sql_file,
            dir_name=dir_name,
            schema=schema,
            obj_name=obj_name,
            completed=completed,
            completed_at=entry.get("updated_at", "") if completed else "",
            note=entry.get("note", ""),
            requires_opt_in=dir_name in OPT_IN_DIRS,
        ))

    return scripts
