## 1. Implementation
- [x] 1.1 Add Oracle role metadata load (DBA_ROLES) and track roles referenced by grants
- [x] 1.2 Add OB system privilege catalog load and compute unsupported privilege set
- [x] 1.3 Filter unsupported system/object privileges when building GRANT statements; log skipped counts and samples
- [x] 1.4 Generate CREATE ROLE DDL for user-defined roles before emitting role grants
- [x] 1.5 Add optional config overrides for object privilege allowlist / unsupported privilege list (if needed)
- [x] 1.6 Split OMS-ready missing exports into per-schema TABLE and VIEW files
- [x] 1.7 Export filtered/unsupported GRANT privileges to main_reports
- [x] 1.8 Update config.ini, config.ini.template, readme_config.txt, README.md, and docs
- [x] 1.9 Add/adjust tests or fixtures for grant filtering, role DDL, and report export split (if applicable)
