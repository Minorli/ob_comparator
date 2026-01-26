# Design: run_fixup auto-grant defaults

## Goals
- Make auto-grant work out-of-the-box without extra CLI flags.
- Cover common dependent object types beyond VIEW.
- Avoid breaking existing manual workflows and preserve explicit CLI overrides.

## Inputs
- `fixup_scripts/grants_miss` and `fixup_scripts/grants_all` (primary grant sources).
- `main_reports/run_<ts>/dependency_chains_<ts>.txt` (TARGET - REMAPPED dependency graph for all objects).
- `main_reports/run_<ts>/VIEWs_chain_<ts>.txt` (view dependency chains when available).

## Behavior Overview
1. When `fixup_auto_grant=true`, the executor loads the latest dependency_chains report and builds a per-object grant requirement list.
2. Before executing a script for an object type in `fixup_auto_grant_types`, the executor applies matching GRANT statements from grants_miss/grants_all.
3. If execution fails with permission errors (ORA-01031/ORA-01720), retry once after attempting grants for that object.
4. If no matching grants exist, optionally generate a direct object GRANT (guarded by scope/type allowlist).
5. Track applied grants to avoid repeats and log a per-run summary.

## Scope & Safety
- Auto-grant is limited to configured object types and only grants to target schemas.
- System schemas (SYS/PUBLIC) remain excluded.
- Auto-grant is skipped when generate_grants=false or grants directories are missing (unless fallback generation is enabled).

## Compatibility
- `--smart-order`, `--iterative`, and `--view-chain-autofix` remain supported and can override defaults.
- Default behavior uses auto-grant even without CLI flags when enabled in config.
