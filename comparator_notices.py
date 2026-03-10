#!/usr/bin/env python3

import json
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Sequence, Tuple, Union

NOTICE_STATE_FILENAME = ".comparator_notice_state.json"
NOTICE_STATE_SCHEMA_VERSION = 1


class RuntimeNotice(NamedTuple):
    notice_id: str
    introduced_in: str
    title: str
    message: str


def resolve_notice_state_path(config_dir: Optional[Union[str, Path]]) -> Path:
    base_dir = Path(config_dir).expanduser() if config_dir else Path.cwd()
    try:
        base_dir = base_dir.resolve()
    except Exception:
        base_dir = Path.cwd().resolve()
    return base_dir / NOTICE_STATE_FILENAME


def load_notice_state(config_dir: Optional[Union[str, Path]]) -> Tuple[Path, Dict[str, object]]:
    state_path = resolve_notice_state_path(config_dir)
    state: Dict[str, object] = {
        "schema_version": NOTICE_STATE_SCHEMA_VERSION,
        "last_seen_tool_version": "",
        "seen_notices": {},
    }
    if not state_path.exists():
        return state_path, state
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            seen = payload.get("seen_notices")
            if not isinstance(seen, dict):
                seen = {}
            state["seen_notices"] = {
                str(key): str(value)
                for key, value in seen.items()
                if str(key).strip()
            }
            last_seen = payload.get("last_seen_tool_version")
            if isinstance(last_seen, str):
                state["last_seen_tool_version"] = last_seen
    except Exception:
        return state_path, state
    return state_path, state


def select_unseen_notices(
    state: Dict[str, object],
    notices: Sequence[RuntimeNotice]
) -> List[RuntimeNotice]:
    seen_notices = state.get("seen_notices")
    if not isinstance(seen_notices, dict):
        seen_notices = {}
    return [
        notice for notice in notices
        if notice.notice_id not in seen_notices
    ]


def persist_seen_notices(
    state_path: Path,
    state: Dict[str, object],
    current_version: str,
    notices: Sequence[RuntimeNotice],
) -> None:
    seen_notices = state.get("seen_notices")
    if not isinstance(seen_notices, dict):
        seen_notices = {}
    for notice in notices:
        seen_notices[notice.notice_id] = current_version
    payload = {
        "schema_version": NOTICE_STATE_SCHEMA_VERSION,
        "last_seen_tool_version": current_version,
        "seen_notices": seen_notices,
    }
    state_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
