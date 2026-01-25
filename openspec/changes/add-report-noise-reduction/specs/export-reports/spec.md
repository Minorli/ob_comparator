## ADDED Requirements

### Requirement: Noise-reduced mismatch reporting
The system SHALL classify mismatches into high-signal and noise-suppressed tiers for reporting,
without altering comparison results or fixup selection.

#### Scenario: Suppressed count shown
- **WHEN** noise-suppressed items exist
- **THEN** the summary includes a noise-suppressed count and references the detail export when available

#### Scenario: Noise-suppressed detail export
- **WHEN** report_detail_mode is split and noise-suppressed items exist
- **THEN** the system writes main_reports/noise_suppressed_detail_<timestamp>.txt with `|` delimiter
  and `# field` header including object type, scope, reason, and key identifiers

#### Scenario: Mixed-signal table
- **WHEN** a table has both high-signal and noise-suppressed differences
- **THEN** the table remains mismatched in the main report while noise-suppressed items are listed
  in the suppressed detail output

#### Scenario: Deterministic system-generated classification
- **WHEN** a mismatch only involves system-generated artifacts (auto columns, SYS_NC hidden columns,
  OMS helper columns, OMS rowid indexes, OBNOTNULL constraints)
- **THEN** it is eligible for noise-suppressed reporting with the matching reason tag
