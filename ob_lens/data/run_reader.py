"""Parse main_reports/ directory and report files."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple


@dataclass
class ObjectRecord:
    src_full: str
    obj_type: str
    tgt_full: str
    state: str           # SUPPORTED | UNSUPPORTED | BLOCKED
    reason_code: str
    reason: str
    dependency: str
    action: str
    detail: str
    section: str         # MISSING_SUPPORTED | UNSUPPORTED_OR_BLOCKED


@dataclass
class RunData:
    run_id: str
    run_dir: Path
    missing_supported: int
    unsupported_or_blocked: int
    objects: List[ObjectRecord] = field(default_factory=list)
    index_entries: List[dict] = field(default_factory=list)
    consistent: int = 0
    missing_total: int = 0
    extra: int = 0
    run_ts_display: str = ""


def find_runs(reports_dir: Path) -> List[Path]:
    """Return run directories sorted chronologically (ascending)."""
    runs = sorted(
        p for p in reports_dir.iterdir()
        if p.is_dir() and re.match(r"run_\d{8}_\d{6}", p.name)
    )
    return runs


def _ts_to_display(ts: str) -> str:
    """'20260301_093300' -> '2026-03-01 09:33'"""
    if len(ts) < 13:
        return ts
    return f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]} {ts[9:11]}:{ts[11:13]}"


def parse_migration_focus(
    path: Path,
) -> Tuple[List[ObjectRecord], int, int]:
    """Parse migration_focus_*.txt. Returns (objects, missing_supported, unsupported_or_blocked)."""
    objects: List[ObjectRecord] = []
    missing_supported = 0
    unsupported_or_blocked = 0
    current_section = ""

    if not path.exists():
        return objects, missing_supported, unsupported_or_blocked

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            m = re.search(r"missing_supported=(\d+)", line)
            if m:
                missing_supported = int(m.group(1))
            m = re.search(r"unsupported_or_blocked=(\d+)", line)
            if m:
                unsupported_or_blocked = int(m.group(1))
            m = re.search(r"section=(\w+)", line)
            if m:
                current_section = m.group(1)
            continue

        parts = line.split("|")

        if current_section == "MISSING_SUPPORTED":
            if len(parts) < 5 or parts[0] == "SRC_FULL":
                continue
            src_full, obj_type, tgt_full, action, detail = (
                parts[0], parts[1], parts[2], parts[3], parts[4]
            )
            objects.append(ObjectRecord(
                src_full=src_full, obj_type=obj_type, tgt_full=tgt_full,
                state="SUPPORTED", reason_code="", reason="",
                dependency="-", action=action, detail=detail,
                section="MISSING_SUPPORTED",
            ))

        elif current_section == "UNSUPPORTED_OR_BLOCKED":
            if len(parts) < 9 or parts[0] == "SRC_FULL":
                continue
            src_full, obj_type, tgt_full, state, reason_code, reason, dependency, action, detail = (
                parts[0], parts[1], parts[2], parts[3], parts[4],
                parts[5], parts[6], parts[7], parts[8],
            )
            objects.append(ObjectRecord(
                src_full=src_full, obj_type=obj_type, tgt_full=tgt_full,
                state=state, reason_code=reason_code, reason=reason,
                dependency=dependency, action=action, detail=detail,
                section="UNSUPPORTED_OR_BLOCKED",
            ))

    return objects, missing_supported, unsupported_or_blocked


def parse_report_index(path: Path) -> List[dict]:
    """Parse report_index_*.txt. Returns list of {category, path, rows, description}."""
    entries = []
    if not path.exists():
        return entries
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("|")
        if len(parts) < 4 or parts[0] == "CATEGORY":
            continue
        entries.append({
            "category": parts[0],
            "path": parts[1],
            "rows": parts[2],
            "description": parts[3],
        })
    return entries


def _extract_summary_counts(report_txt: Path) -> dict:
    """Extract key counts from main report text (best-effort, returns zeros on failure)."""
    counts = {"consistent": 0, "missing_total": 0, "extra": 0}
    if not report_txt.exists():
        return counts
    content = report_txt.read_text(encoding="utf-8", errors="ignore")
    # Look for lines like "一致: 9" or "缺失: 32" or "多余: 2" in the summary section
    for label, key in [("一致", "consistent"), ("缺失", "missing_total"), ("多余", "extra")]:
        m = re.search(rf"{label}.*?(\d+)", content)
        if m:
            counts[key] = int(m.group(1))
    return counts


def load_run(run_dir: Path, fixup_dir: Optional[Path] = None) -> RunData:
    """Load all data for a single run directory."""
    run_id = run_dir.name.removeprefix("run_")  # "20260301_093300"

    # Find migration_focus file
    focus_files = list(run_dir.glob("migration_focus_*.txt"))
    objects: List[ObjectRecord] = []
    missing_supported = 0
    unsupported_or_blocked = 0
    if focus_files:
        objects, missing_supported, unsupported_or_blocked = parse_migration_focus(
            focus_files[0]
        )

    # Find report_index file
    index_files = list(run_dir.glob("report_index_*.txt"))
    index_entries = parse_report_index(index_files[0]) if index_files else []

    # Best-effort summary counts from main report
    report_files = list(run_dir.glob("report_*.txt"))
    # Exclude report_index and report_sql files
    main_report = next(
        (f for f in report_files
         if not f.name.startswith("report_index_") and not f.name.startswith("report_sql_")),
        None,
    )
    counts = _extract_summary_counts(main_report) if main_report else {}

    return RunData(
        run_id=run_id,
        run_dir=run_dir,
        missing_supported=missing_supported,
        unsupported_or_blocked=unsupported_or_blocked,
        objects=objects,
        index_entries=index_entries,
        consistent=counts.get("consistent", 0),
        missing_total=missing_supported + unsupported_or_blocked,
        extra=counts.get("extra", 0),
        run_ts_display=_ts_to_display(run_id),
    )
