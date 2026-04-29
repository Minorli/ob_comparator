# Production Reliability Roadmap

This document records implementation decisions for `add-production-reliability-and-diagnostics-roadmap`.

## Release Scope

0.9.x focuses on production reliability and operational diagnostics:

- release evidence and release gate
- runtime heartbeat and effective timeout summaries
- difference explanations and fixup safety tiers
- compatibility registry and recovery metadata
- standalone customer diagnostic bundles

The 1.0 release owns the core `schema_diff_reconciler.py` refactor. The 1.1 release owns UI work. This roadmap does not implement either.

## Resolved Decisions

- Diagnostic package entry point: standalone `diagnostic_bundle.py`, so evidence can be collected while the main process is hung. `schema_diff_reconciler.py` and `run_fixup.py` only write collectable evidence such as heartbeat, timeout, report, checkpoint, and fixup-plan artifacts.
- Release smoke: 0.9.x uses controlled real Oracle to OceanBase smoke evidence and local-only or ignored fixtures. Do not add tracked test schema initialization files under this change.
- SQL evidence in bundles: collect SQL file name, path, size, hash, object identity, and summary by default. Full SQL/DDL content requires explicit opt-in.
- report_db diagnostics: export count/status/consistency summaries from existing tables. Do not add dedicated diagnostic views in this change.
- Compatibility matrix: ship an external `compatibility_registry.json` and load it at runtime; do not hard-code the registry into the main program.

## Phase Order

1. Phase A: release gate, timeout table, heartbeat state.
2. Phase B: reason records, VARCHAR/VARCHAR2 compatibility guardrails, fixup safety tiers.
3. Phase C: compatibility registry, compatibility matrix export, recovery manifests.
4. Phase D: standalone diagnostic bundle CLI, first post-run then hang snapshot.

Phase D depends on Phase A heartbeat, Phase B fixup plan/explanations, and Phase C recovery/compatibility artifacts.

## Current Phase A Artifacts

- Main program heartbeat: `main_reports/run_<ts>/run_heartbeat_<ts>.json`
- Main program timeout table: `main_reports/run_<ts>/runtime_timeout_summary_<ts>.txt`
- run_fixup timeout table: `<report_dir>/run_fixup_timeout_summary_<ts>.txt`
- run_fixup execution heartbeat: `<fixup_dir>/run_fixup_heartbeat_<ts>.json` when a file or statement is executing
- Release gate: maintainer-local gate command against `release_evidence.json`

## Current Phase B Artifacts

- Fixup plan: `main_reports/run_<ts>/fixup_plan_<ts>.jsonl`
- Safety summary: `main_reports/run_<ts>/fixup_safety_summary_<ts>.txt`
- Difference explanations: `main_reports/run_<ts>/difference_explanations_<ts>.jsonl`
- Explanation summary: `main_reports/run_<ts>/difference_explanations_summary_<ts>.txt`
- Execution filter: `python3 run_fixup.py config.ini --safety-tiers safe,review`

`safe` is whitelist-only and currently limited to existing-object compile operations. `destructive` requires `--confirm-destructive`; `manual` requires `--confirm-manual`. These artifacts are support and execution evidence, not approval to run SQL without review.

## Current Phase C Artifacts

- Compatibility registry: `compatibility_registry.json`
- Per-run matrix: `main_reports/run_<ts>/compatibility_matrix_<ts>.json`
- Human summary: `main_reports/run_<ts>/compatibility_summary_<ts>.txt`
- Recovery manifest: `main_reports/run_<ts>/recovery_manifest_<ts>.json`

Recovery uses a decision/runtime hash split. Decision inputs such as source mode, schemas, remap, object type switches, compatibility registry, blacklist/exclude rules, and fixup behavior must match by default. Runtime/display settings such as report path, log level, heartbeat interval, and diagnostic settings may change and are recorded in the manifest. `--force-resume --resume-override-reason <reason>` is the explicit audited support bypass.

## Current Phase D Entry Point

Standalone post-run or hang collection:

```bash
python3 diagnostic_bundle.py --run-dir main_reports/run_<ts> --config config.ini
python3 diagnostic_bundle.py --run-dir main_reports/run_<ts> --config config.ini --pid <pid> --hang
```

The bundle contains `manifest.json`, `summary.txt`, `config_sanitized.ini`, `run_state.json`, and `artifacts/`. SQL file names, sizes, hashes, and summaries are included by default; full SQL content requires `--include-sql-content`. Identifier hashing is available with `--redact-identifiers`, and the local identifier map is intentionally excluded from the zip. Bundle collection enforces per-file and total uncompressed size caps via `--max-file-mb` / `--max-bundle-mb` or the equivalent config defaults.
