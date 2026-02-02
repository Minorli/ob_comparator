## ADDED Requirements

### Requirement: View DDL removes FORCE keyword
The system SHALL remove the FORCE keyword from CREATE OR REPLACE FORCE VIEW statements when generating view fixup DDL.

#### Scenario: FORCE view cleanup
- **WHEN** a VIEW DDL contains `CREATE OR REPLACE FORCE VIEW`
- **THEN** the generated fixup DDL uses `CREATE OR REPLACE VIEW`

### Requirement: View prereq grants
The system SHALL generate view prerequisite grants for missing views based on dependency pairs, and output them to a dedicated `view_prereq_grants` directory.

#### Scenario: View depends on cross-schema table
- **WHEN** a missing VIEW depends on a TABLE in another schema
- **THEN** a GRANT SELECT (and REFERENCES if needed) is emitted in `view_prereq_grants`

### Requirement: View post grants (sync source privileges)
The system SHALL output source-derived VIEW grants for missing views to a `view_post_grants` directory so they execute after view creation.

#### Scenario: Source grants on a missing view
- **WHEN** the source VIEW has object grants and the target VIEW is missing
- **THEN** the grants are emitted to `view_post_grants` and excluded from pre-grant execution
