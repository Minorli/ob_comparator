## ADDED Requirements

### Requirement: Column order check toggle
The system SHALL provide a `check_column_order` setting to enable column order comparisons.

#### Scenario: Default off
- **WHEN** `check_column_order` is missing
- **THEN** the system defaults it to false

#### Scenario: Enabled
- **WHEN** `check_column_order` is true
- **THEN** column order comparison runs for TABLE objects
