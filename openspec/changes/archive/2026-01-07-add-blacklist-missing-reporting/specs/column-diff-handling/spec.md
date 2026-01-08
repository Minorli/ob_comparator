## ADDED Requirements
### Requirement: LONG type equivalence
The system SHALL treat `LONG -> CLOB` and `LONG RAW -> BLOB` as compatible column types during TABLE comparisons.

#### Scenario: LONG mapped to CLOB
- **WHEN** the source column type is `LONG` and the target column type is `CLOB`
- **THEN** the column is considered matching and no fixup is generated

#### Scenario: LONG RAW mapped to BLOB
- **WHEN** the source column type is `LONG RAW` and the target column type is `BLOB`
- **THEN** the column is considered matching and no fixup is generated

### Requirement: LONG column add fixup mapping
The system SHALL map missing `LONG` columns to `CLOB` and missing `LONG RAW` columns to `BLOB` when generating ALTER TABLE ADD statements.

#### Scenario: Missing LONG column
- **WHEN** a source TABLE has a `LONG` column that is missing in the target
- **THEN** the generated ADD COLUMN uses `CLOB`

#### Scenario: Missing LONG RAW column
- **WHEN** a source TABLE has a `LONG RAW` column that is missing in the target
- **THEN** the generated ADD COLUMN uses `BLOB`
