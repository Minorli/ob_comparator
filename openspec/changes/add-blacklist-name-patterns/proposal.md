# Change: Add name-pattern blacklist inputs + per-rule enable tags

## Why
Customers want to blacklist tables by simple keywords (e.g. `_RENAME`) without editing JSON, and enable/disable blacklist rules individually. Current rules require JSON edits and produce “未知黑名单类型” for custom black_type values.

## What Changes
- Add user-friendly inputs for name-pattern blacklisting:
  - `blacklist_name_patterns` (comma-separated keywords)
  - `blacklist_name_patterns_file` (one keyword per line)
- Treat patterns as **literal substrings** and auto-escape `%`, `_`, and `!` using `LIKE ... ESCAPE '!'`.
- Update `blacklist_rules.json` to include:
  - a `NAME_PATTERN` rule with `{{name_pattern_clause}}` placeholder.
  - a built-in `RENAME_TABLES` rule (default enabled) to catch `_RENAME` patterns.
  - a per-rule `enabled` tag to allow rule-level enable/disable.
- Extend rule engine to:
  - respect `enabled=false` (skip rule with log note).
  - render `{{name_pattern_clause}}` when patterns exist; skip the rule with a clear warning when empty.
- Recognize `NAME_PATTERN` (and `RENAME`) as known blacklist types with reason code `BLACKLIST_NAME_PATTERN` so reports no longer say “未知黑名单类型”.

## Impact
- Affected specs: `configuration-control`, `compare-objects`
- Affected code: `schema_diff_reconciler.py`, `blacklist_rules.json`, `readme_config.txt`, `config.ini.template`
- Reports: `blacklist_tables.txt`, `DIFF_REPORT_BLACKLIST`, `DIFF_REPORT_DETAIL/DETAIL_ITEM`
