## ADDED Requirements

### Requirement: Column visibility comparison
The system SHALL compare column visibility when INVISIBLE metadata is available and column_visibility_policy requires enforcement.

#### Scenario: Source invisible, target visible
- **WHEN** a source column is INVISIBLE and the target column is visible
- **THEN** the table comparison records a visibility mismatch for that column

#### Scenario: Visibility metadata unavailable
- **WHEN** visibility metadata cannot be loaded or column_visibility_policy is ignore
- **THEN** visibility comparison is skipped with a recorded reason

#### Scenario: System hidden columns ignored
- **WHEN** a column is system-hidden or internal-only
- **THEN** visibility comparison ignores that column
