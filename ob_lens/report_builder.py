"""Assemble self-contained HTML report from a run directory."""
from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import List, Optional

from ob_lens.data.run_reader import find_runs, load_run, RunData
from ob_lens.data.fixup_reader import list_fixup_scripts


_STATIC_DIR = Path(__file__).parent / "static"


def _load_template() -> str:
    return (_STATIC_DIR / "template.html").read_text(encoding="utf-8")


def _load_css() -> str:
    return (_STATIC_DIR / "style.css").read_text(encoding="utf-8")


def _load_js() -> str:
    return (_STATIC_DIR / "app.js").read_text(encoding="utf-8")


def _serialize_run(run: RunData) -> dict:
    """Convert RunData to a JSON-serializable dict."""
    return {
        "run_id": run.run_id,
        "run_ts_display": run.run_ts_display,
        "missing_supported": run.missing_supported,
        "unsupported_or_blocked": run.unsupported_or_blocked,
        "consistent": run.consistent,
        "missing_total": run.missing_total,
        "extra": run.extra,
        "objects": [dataclasses.asdict(o) for o in run.objects],
        "index_entries": run.index_entries,
    }


def build_report(
    run_dir: Path,
    fixup_dir: Path,
    output_path: Optional[Path] = None,
) -> Path:
    """Generate a self-contained HTML report for the given run directory.

    Returns the path of the generated HTML file.
    """
    # Determine output path
    run_id = run_dir.name.removeprefix("run_")
    if output_path is None:
        output_path = run_dir / f"report_{run_id}.html"

    # Load current run
    current_run = load_run(run_dir, fixup_dir)

    # Load all runs for history (lightweight: only header counts, no objects)
    reports_dir = run_dir.parent
    all_run_dirs = find_runs(reports_dir)
    all_runs = []
    for rd in all_run_dirs:
        try:
            r = load_run(rd)  # No fixup_dir needed for history rows
            # Strip objects list for history — keep only summary counts
            all_runs.append({
                "run_id": r.run_id,
                "run_ts_display": r.run_ts_display,
                "missing_supported": r.missing_supported,
                "unsupported_or_blocked": r.unsupported_or_blocked,
                "consistent": r.consistent,
                "missing_total": r.missing_total,
                "extra": r.extra,
            })
        except Exception:
            pass  # Skip unreadable runs gracefully

    # Load fixup scripts (for future use; included in data for Phase 2 compatibility)
    fixup_scripts = [
        {
            "rel_path": s.rel_path,
            "dir_name": s.dir_name,
            "schema": s.schema,
            "obj_name": s.obj_name,
            "completed": s.completed,
            "completed_at": s.completed_at,
            "requires_opt_in": s.requires_opt_in,
        }
        for s in list_fixup_scripts(fixup_dir)
    ]

    # Assemble data payload
    data = {
        "current_run": _serialize_run(current_run),
        "runs": all_runs,
        "fixup_scripts": fixup_scripts,
        "generated_at": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    data_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    # Load and assemble template
    template = _load_template()
    css = _load_css()
    js = _load_js()

    html = (
        template
        .replace("/* STYLE_PLACEHOLDER */", css, 1)
        .replace("/* SCRIPT_PLACEHOLDER */", js, 1)
        .replace("/* DATA_PLACEHOLDER */", data_json, 1)
    )

    output_path.write_text(html, encoding="utf-8")
    return output_path
