# Release Checklist

Use this checklist before publishing a public release or customer hotfix.

## 1. Freeze Scope

- Confirm the release branch starts from the latest `main`.
- Record version, branch, commit, and tag candidate.
- Confirm only intended files are changed with `git status --short`.
- Confirm OpenSpec is present and validated for any new capability, switch, report, or compatibility rule.

## 2. Verify

Run the minimum local gate:

```bash
python3 -m py_compile $(git ls-files '*.py')
git diff --check
```

For changes touching `schema_diff_reconciler.py` or `run_fixup.py`, record:

- commands executed
- pass/fail result
- impact scope
- skipped validation and reason, if any

For summary/detail/report_db behavior, verify count consistency across:

- main summary
- split detail files
- report_db tables

For database semantics, run real Oracle and OceanBase verification. If a release changes `source_db_mode=oceanbase`, include OB-source verification.

## 3. Package

Prefer a complete toolkit zip over partial file replacement.

At minimum, a customer hotfix package must keep these files together:

- `schema_diff_reconciler.py`
- `run_fixup.py`
- `diagnostic_bundle.py`
- `comparator_reliability.py`
- `config.ini.template.txt`
- `readme_config.txt`
- `readme_lite.txt`
- `README.md`
- `blacklist_rules.json`
- `compatibility_registry.json`

Do not include:

- `config.ini`
- credentials or wallets
- generated `main_reports/`, `fixup_scripts/`, or local smoke output
- ad hoc test fixtures unless they are intentionally tracked

## 4. Publish

- Create or update the release tag.
- Upload the toolkit zip.
- Upload `SHA256SUMS`.
- Upload release evidence JSON when available.
- Include customer deployment notes in the GitHub release body.
- Do not silently move an existing public tag. Publish a new hotfix or clearly document any correction.

## 5. Post-release

- Merge release documentation back to `main`.
- Delete merged release branches when no longer needed.
- Confirm GitHub release assets are downloadable.
- Confirm the default branch shows the current docs and CI status.
- Record rollback guidance or residual risk if any validation was intentionally skipped.
