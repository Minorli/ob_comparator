## 1. Implementation
- [x] 1.1 Add config keys: `generate_interval_partition_fixup` and `interval_partition_cutoff` (YYYYMMDD), with validation and defaults.
- [x] 1.2 Extend Oracle metadata dump to load interval partition tables, partition keys, and last partition high values.
- [x] 1.3 Add CREATE TABLE DDL cleanup to remove INTERVAL clauses for OceanBase compatibility.
- [x] 1.4 Implement interval partition expansion logic and generate ADD PARTITION DDL up to cutoff date.
- [x] 1.5 Write fixup output under `fixup_scripts/table_alter/interval_add_<cutoff>/` per table.
- [x] 1.6 Update docs and config templates with the new settings and usage guidance.

## 2. Tests
- [x] 2.1 Unit tests for interval expression parsing and boundary iteration.
- [x] 2.2 Oracle 19c integration test: create interval partitioned table and verify generated DDL up to cutoff.
- [x] 2.3 Negative cases: invalid cutoff, unsupported interval expressions, missing HIGH_VALUE.
