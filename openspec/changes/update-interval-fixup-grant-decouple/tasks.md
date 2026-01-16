## 1. Implementation
- [x] 1.1 Add `interval_partition_cutoff_numeric` config default + validation + diagnostics.
- [x] 1.2 Extend interval parsing to support numeric `INTERVAL (n)` and numeric HIGH_VALUE extraction.
- [x] 1.3 Generate numeric interval ADD PARTITION DDL with safe partition naming and cutoff handling.
- [x] 1.4 Decouple generate_fixup from generate_grants so fixups run regardless of grant toggle.
- [x] 1.5 Update config templates and readme_config with numeric cutoff and grant/fixup behavior notes.

## 2. Tests
- [x] 2.1 Unit tests for numeric interval parsing, numeric HIGH_VALUE parsing, and boundary generation.
- [x] 2.2 Integration test in Oracle/OceanBase: numeric interval table + cutoff generates ADD PARTITION DDL; invalid cutoff skips.
