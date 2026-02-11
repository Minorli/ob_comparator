# Change: Add expert swarm toolkit for comparator

## Why
- The comparator codebase is large and highly coupled across architecture, SQL compatibility, and execution safety concerns.
- The team needs a repeatable "expert swarm" workflow with explicit specialist roles instead of ad-hoc single-perspective reviews.

## What Changes
- Add `expert_swarm.py` to generate a deterministic swarm blueprint and task brief for this repository.
- Upgrade execute mode to a stronger pattern: parallel subteams run concurrently, then a dedicated arbiter resolves conflicts.
- Keep optional execution mode on OpenAI Agents SDK + Codex MCP, with lazy dependency loading.
- Write role reports, subteam summaries, and arbitration output under `audit/swarm/`.
- Add unit tests and operator documentation for the swarm workflow.

## Impact
- Affected specs: `expert-swarm` (new)
- Affected code: `expert_swarm.py`, `test_expert_swarm.py`, `docs/EXPERT_SWARM.md`, `README.md`
- New outputs:
  - `audit/swarm/swarm_blueprint.json`
  - `audit/swarm/swarm_task.md`
  - `audit/swarm/subteam_*_summary.md`
  - `audit/swarm/consolidated_report.md`
