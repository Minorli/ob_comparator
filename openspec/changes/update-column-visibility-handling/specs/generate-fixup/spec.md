## ADDED Requirements

### Requirement: Column visibility fixup
The system SHALL generate DDL to align column visibility when column_visibility_policy permits enforcement.

#### Scenario: Missing invisible flag on target
- **WHEN** a target column exists but is visible and the source column is INVISIBLE
- **THEN** an ALTER TABLE MODIFY ... INVISIBLE statement is generated

#### Scenario: Create table preserves invisible
- **WHEN** a missing TABLE contains INVISIBLE columns in the source
- **THEN** the CREATE TABLE DDL preserves INVISIBLE columns or appends ALTER statements to apply invisibility

#### Scenario: Visibility enforcement disabled
- **WHEN** column_visibility_policy is ignore
- **THEN** no visibility-related fixup DDL is generated
