## 1. Implementation
- [ ] 1.1 Extend Oracle metadata dump to load partition key columns for partitioned tables.
- [ ] 1.2 Extend OceanBase metadata dump to load partition key columns (for diagnostics and consistency checks).
- [ ] 1.3 Add partitioned-table classification helpers (partition key included vs not included in PK).
- [ ] 1.4 Update constraint comparison to downgrade non-inclusive PK expectations to UNIQUE.
- [ ] 1.5 Update constraint fixup generation to emit UNIQUE instead of PRIMARY KEY for non-inclusive PK cases.
- [ ] 1.6 Add unit tests covering inclusive/non-inclusive partition key scenarios.
- [ ] 1.7 Validate behavior with Oracle/OB sample tables once proposal is approved.
