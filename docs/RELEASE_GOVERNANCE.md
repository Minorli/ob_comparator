# Release Governance

This project uses a hard release gate for public versions. A release is not publishable until the release evidence file says it is publishable and `scripts/release_gate.py` accepts it.

## Scope Freeze

Before validation starts, record:

- `version`, `branch`, `commit`, and intended `tag_candidate`
- included commits and intentionally excluded local artifacts
- docs touched
- compatibility-impacting changes
- residual risk and skipped validation, if any

The 0.9.x reliability work explicitly excludes the 1.0 core refactor and the 1.1 UI.

## Required Evidence

The evidence file is JSON. Minimum required fields:

```json
{
  "version": "0.9.9.6",
  "branch": "feature/example",
  "commit": "commit-sha",
  "tag_candidate": "v0.9.9.6",
  "publishable": true,
  "commands": [
    {
      "command": "python3 -m py_compile $(git ls-files '*.py')",
      "status": "passed",
      "notes": "compile check completed"
    }
  ],
  "git_diff_hygiene": {
    "status": "passed",
    "notes": "only intended files included"
  },
  "tracked_test_file_hygiene": {
    "status": "passed",
    "notes": "no tracked test fixtures or ad hoc probes added"
  },
  "real_db_smoke": {
    "source_mode": "oracle",
    "target_mode": "oceanbase",
    "status": "passed",
    "command": "PYTHONUNBUFFERED=1 .venv/bin/python schema_diff_reconciler.py config.ini",
    "duration_sec": 73,
    "report_path": "main_reports/run_YYYYMMDD_HHMMSS/report_YYYYMMDD_HHMMSS.txt",
    "oracle_version": "19c",
    "oceanbase_version": "4.x"
  },
  "runtime_artifacts": {
    "heartbeat": "main_reports/run_YYYYMMDD_HHMMSS/run_heartbeat_YYYYMMDD_HHMMSS.json",
    "timeout_summary": "main_reports/run_YYYYMMDD_HHMMSS/runtime_timeout_summary_YYYYMMDD_HHMMSS.txt",
    "compatibility_matrix": "main_reports/run_YYYYMMDD_HHMMSS/compatibility_matrix_YYYYMMDD_HHMMSS.json",
    "recovery_manifest": "main_reports/run_YYYYMMDD_HHMMSS/recovery_manifest_YYYYMMDD_HHMMSS.json",
    "difference_explanations": "main_reports/run_YYYYMMDD_HHMMSS/difference_explanations_YYYYMMDD_HHMMSS.jsonl",
    "diagnostic_bundle_command": "python3 diagnostic_bundle.py --run-dir main_reports/run_YYYYMMDD_HHMMSS --config config.ini"
  },
  "skipped_validation": [],
  "residual_risk": []
}
```

Run the gate:

```bash
python3 scripts/release_gate.py release_evidence_0.9.9.6.json
```

The gate fails closed when required evidence is missing, when `publishable` is not `true`, or when Oracle to OceanBase real database smoke has not passed.

## Smoke Policy

For 0.9.x, smoke evidence comes from a controlled real Oracle to OceanBase environment and local-only or ignored smoke fixtures. Do not add tracked test schema initialization files unless a future OpenSpec change explicitly changes the test-file policy.

If a release changes `source_db_mode=oceanbase`, record an OB-source smoke run or mark the release as blocked. If a release changes `run_fixup.py`, record a controlled non-mutating dry run, plan validation, or the reason execution was intentionally skipped.

## Rollback and Hotfix

If a public release has a production blocker, choose and record one action:

- retract or amend the GitHub release note
- publish a hotfix release
- give customer rollback guidance

A hotfix must include reproduction evidence, fix evidence, and a focused real database regression run for the blocker.
