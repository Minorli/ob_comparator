## 1. Implementation
- [x] 1.1 Add report index export (report_index_<ts>.txt) with file path, type, description, row count when available.
- [x] 1.2 Add “执行结论” block and normalize section numbering + terminology without removing existing sections.
- [x] 1.3 Support log_level=auto and default to auto; choose INFO for TTY, WARNING for non‑TTY; keep file logs DEBUG.
- [x] 1.4 Update config template + readme_config documentation.

## 2. Tests
- [x] 2.1 Unit test: report index includes expected files for split vs summary mode.
- [x] 2.2 Unit test: log_level=auto resolves to INFO in TTY and WARNING in non‑TTY.
- [x] 2.3 Regression: report output retains all prior detail sections (no data loss).

## 3. Validation
- [x] 3.1 Run `openspec validate update-report-indexing --strict`.
