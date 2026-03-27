# Contributing

This repository can still move quickly, but it should no longer lose change history.

## Workflow

Use the lightest workflow that still preserves review context.

### Must use `issue + branch + PR`

Use a PR when the change includes any of the following:

- new capability
- new config switch
- new report or report-db behavior
- fixup execution strategy change
- remap, dependency, grant, blacklist, or compatibility policy change
- architecture shift or refactor with cross-cutting impact
- high-risk bugfix where regression risk is non-trivial

### Direct push may still be acceptable

Direct push is acceptable only for narrow maintainer-owned changes such as:

- typo or wording fixes
- isolated docs-only cleanup
- very small low-risk repair where the change boundary and verification are obvious

If a change starts to require explanation, review, or rollback discussion, it should be a PR.

## Issue linkage

Every substantial change should be linked to a GitHub issue.

- For new work, open or reuse a normal issue first.
- For follow-up work in an existing maintenance area, use the existing area issue and reference it with `Refs #N`.

Current area issues:

- `#1` synonym
- `#2` constraint
- `#3` view
- `#4` scope
- `#5` grant
- `#6` runtime / parser / metadata fallback
- `#7` blacklist re-entry
- `#9` reporting / report_db / operator guidance
- `#10` fixup safety / table presence
- `#11` trigger / view-chain
- `#12` cutoff scope
- `#13` hot reload
- `#14` name collision / SYS_C handling

The historical tracking entry is `#8`, and the maintenance backfill release is:

- `history-march-2026-maintenance`

## OpenSpec

Follow the OpenSpec rules already enforced in this repository:

- new capability, switch, report, or compatibility rule requires an OpenSpec change
- validate before implementation:
  - `openspec validate --changes --strict`

## Verification minimums

Before commit or PR:

```bash
python3 -m py_compile $(git ls-files '*.py')
.venv/bin/python -m unittest test_schema_diff_reconciler test_run_fixup
```

When the change affects database semantics, also include real Oracle + OceanBase verification evidence.

The minimum evidence expected in a PR body or maintainer note is:

- commands executed
- pass/fail result
- impact scope
- blocker, if validation could not be completed

## Commit guidance

Prefer small, reviewable commits with imperative subjects.

Examples:

- `Fix public synonym fixup filtering`
- `Refs #2 Preserve OB auto notnull semantics in table compare`

If the change is high-risk, the commit body should summarize:

- what changed
- why it changed
- how it was verified

## PR guidance

Use the PR template and fill the sections that apply.

A good PR should make it easy to answer:

- What changed?
- Why now?
- Which issue owns the history?
- What was verified?
- What remains risky or manual?

## Repository hygiene

Do not commit:

- `config.ini`
- secrets or tokens
- local temp files
- generated run artifacts under `/tmp`

Keep docs and config guidance in sync when user-facing workflow changes.
