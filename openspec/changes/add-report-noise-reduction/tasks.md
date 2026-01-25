## 1. Design
- [x] 1.1 Define noise-suppressed classification rules for system-generated artifacts (auto columns,
      SYS_NC hidden columns, OMS helper columns, OMS rowid indexes, OBNOTNULL constraints).

## 2. Implementation
- [x] 2.1 Apply the auto-generated column ignore filter in comment comparison.
- [x] 2.2 Add noise-suppressed reporting tiers and update summary counts.
- [x] 2.3 Exclude noise-suppressed mismatches from fixup generation while keeping high-signal fixups.
- [x] 2.4 Export noise-suppressed details to `main_reports/noise_suppressed_detail_<timestamp>.txt`
      with `|` delimiter and `# field` header when `report_detail_mode=split`.
- [x] 2.5 Add unit tests for comment filtering, noise-suppressed classification/reporting, and fixup exclusion.
- [x] 2.6 Skip comment comparison for tables missing on the target.

## 3. Validation
- [x] 3.1 Run `openspec validate add-report-noise-reduction --strict`.
