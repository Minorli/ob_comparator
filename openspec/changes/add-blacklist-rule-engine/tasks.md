## 1. Implementation
- [x] 1.1 Add config keys: `blacklist_mode`, `blacklist_rules_path`, `blacklist_rules_enable`, `blacklist_rules_disable`, `blacklist_lob_max_mb` with validation and defaults.
- [x] 1.2 Add a default blacklist rules file and loader with version gating and per-rule toggles.
- [x] 1.3 Implement rule evaluation queries with owner filtering and chunking; collect results into the existing `blacklist_tables` map.
- [x] 1.4 Merge rule results with `TMP_BLACK_TABLE` results per `blacklist_mode`, dedupe entries, and log counts by source.
- [x] 1.5 Extend blacklist report detail to include rule source info without changing existing columns.
- [x] 1.6 Update config.ini, config.ini.template, readme_config.txt, and README.md with new settings.

## 2. Tests
- [x] 2.1 Add unit tests for rules loading, version gating, enable/disable lists, and merge behavior.
- [x] 2.2 Oracle 19c integration test: create incompatible objects for each rule (UDT, unsupported types, LONG/LONG RAW, oversize LOB using a lowered threshold, temp tables, external tables, IOT), run comparison, and verify blacklist output.
- [x] 2.3 Validate fallback behavior when rules fail or required views are missing.
