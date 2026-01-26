## ADDED Requirements

### Requirement: Column order mismatch reporting
The system SHALL report table column order mismatches when column order checking is enabled.

#### Scenario: Summary count
- **WHEN** column order mismatches exist
- **THEN** the report summary includes the mismatch count and references the detail export when available

#### Scenario: Split detail export
- **WHEN** `report_detail_mode` is split and column order mismatches exist
- **THEN** the system writes `main_reports/column_order_mismatch_detail_<timestamp>.txt` with `|` delimiter
  and a `# field` header containing `TABLE`, `SRC_ORDER`, and `TGT_ORDER`

#### Scenario: Full report detail
- **WHEN** `report_detail_mode` is full and column order mismatches exist
- **THEN** the main report includes a dedicated column order mismatch section
