## 1. Implementation
- [x] 1.1 Harden `run_sql()` to catch FileNotFoundError/PermissionError/OSError and surface ConfigError.
- [x] 1.2 Implement safe move to `done/` to prevent overwrites.
- [x] 1.3 Fix iterative cumulative failure counting (unique script set).
- [x] 1.4 Validate port range (1-65535) and fail fast on invalid.
- [x] 1.5 Add `fixup_dir_allow_outside_repo` guard (default true).
- [x] 1.6 Improve auto-grant missing dependency warnings with explicit paths.
- [x] 1.7 Block view-chain execution on cycles (no SQL emitted).
- [x] 1.8 Add `fixup_max_sql_file_mb` and skip oversized SQL files with report.
- [x] 1.9 Expand SQL error classification codes.
- [x] 1.10 Add AutoGrant cache limit with eviction.
- [x] 1.11 Strengthen SQL splitter boundaries and add tests.

## 2. Tests
- [x] 2.1 Unit tests for error classification additions.
- [x] 2.2 Unit tests for oversized SQL skip.
- [x] 2.3 Unit tests for view-chain cycle behavior.
- [x] 2.4 Unit tests for new cache eviction behavior.
- [x] 2.5 Unit tests for splitter boundary cases.

## 3. Documentation
- [x] 3.1 Update `readme_config.txt` for new settings.
- [x] 3.2 Update `docs/TECHNICAL_SPECIFICATION.md` for fixup behavior changes.
