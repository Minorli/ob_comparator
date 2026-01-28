## ADDED Requirements

### Requirement: View constraint cleanup reports
The system SHALL output cleanup reports for view constraints and include them in report_index.

#### Scenario: Cleaned report
- **WHEN** any VIEW constraint is cleaned
- **THEN** a file `view_constraint_cleaned_detail_<ts>.txt` is produced with pipe-delimited rows

#### Scenario: Uncleanable report
- **WHEN** any VIEW constraint is not cleanable
- **THEN** a file `view_constraint_uncleanable_detail_<ts>.txt` is produced with pipe-delimited rows

#### Scenario: Report indexing
- **WHEN** either file exists
- **THEN** report_index lists it with category and description
