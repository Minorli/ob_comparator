## 1. Design
- [x] 1.1 Define column order comparison algorithm, noise filtering, and skip conditions.
- [x] 1.2 Decide report format for order mismatches and summary presentation.

## 2. Implementation
- [x] 2.1 Add `check_column_order` setting defaults, validation, wizard prompts, `config.ini.template`, and `readme_config.txt`.
- [x] 2.2 Load column order metadata when `check_column_order` is enabled (Oracle + OB).
- [x] 2.3 Implement column order comparison in `check_primary_objects` with noise filtering and skip rules.
- [x] 2.4 Add report summary + split detail export for column order mismatches and optional full report section.
- [x] 2.5 Ensure column order mismatches are report-only and excluded from fixup generation.

## 3. Tests & Verification
- [x] 3.1 Add unit tests for column order gating, noise filtering, and mismatch detection.
- [x] 3.2 Add regression coverage for 2026-01-25 audit findings (virtual/invisible metadata fallback, virtual expr normalization, auto-sequence suppression, auto-column comment filtering, metadata-gated identity/default-on-null checks).
- [x] 3.3 Run `python3 -m py_compile $(git ls-files '*.py')` and unit tests.
- [x] 3.4 Run `openspec validate add-column-order-check --strict`.
- [x] 3.5 Run a real Oracle + OceanBase test validation using config credentials and review the generated reports.
