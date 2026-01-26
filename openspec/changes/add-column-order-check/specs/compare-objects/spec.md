## ADDED Requirements

### Requirement: Column order comparison
The system SHALL optionally compare table column order when `check_column_order` is enabled.

#### Scenario: Column order check disabled
- **WHEN** `check_column_order` is false or unset
- **THEN** column order is not compared and no order mismatches are recorded

#### Scenario: Order comparison uses filtered columns
- **WHEN** `check_column_order` is enabled
- **THEN** column order comparison excludes OMS helper columns, auto-generated columns
  (`__PK_INCREMENT`), SYS_NC hidden columns, and hidden/invisible columns

#### Scenario: Order derived from column sequence
- **WHEN** column order comparison runs
- **THEN** the system compares the filtered column sequence and does not treat `COLUMN_ID` equality
  as a standalone mismatch criterion

#### Scenario: Column set mismatch skips order check
- **WHEN** the filtered source and target column sets differ
- **THEN** column order comparison for that table is skipped

#### Scenario: Order mismatch recorded
- **WHEN** the filtered source and target column sets match but their column sequences differ
- **THEN** the table is recorded as a column-order mismatch with source and target order details

#### Scenario: Order metadata missing
- **WHEN** column order metadata is unavailable on either side
- **THEN** the system skips column order comparison for that table and records a skip reason
