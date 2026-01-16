# Change: Add dynamic blacklist rule engine

## Why
The current blacklist flow depends on a pre-built `OMS_USER.TMP_BLACK_TABLE`, which requires manual setup before each run. This adds operational friction and makes it hard to evolve blacklist rules as OceanBase compatibility improves.

## What Changes
- Add a rule-driven blacklist engine that derives unsupported TABLEs from Oracle metadata at runtime without requiring a physical blacklist table.
- Introduce `blacklist_mode` to control how `TMP_BLACK_TABLE` and rules are used (auto, table_only, rules_only, disabled).
- Load rules from a configurable rules file so compatibility updates can be applied without code changes.
- Add version gating and per-rule enable/disable controls for future OceanBase upgrades.
- Extend logging/reporting to show blacklist sources and rule coverage.
- Add unit tests and Oracle 19c integration tests that create incompatible objects and validate blacklist output.

## Impact
- Affected specs: compare-objects, configuration-control, export-reports
- Affected code: schema_diff_reconciler.py (blacklist discovery), config.ini, readme_config.txt, README.md
