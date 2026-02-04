## ADDED Requirements
### Requirement: Usability detail root-cause enrichment
The system SHALL enrich `usability_check_detail` with precise root-cause and actionable recommendations derived from dependency and permission context.

#### Scenario: View query error
- **WHEN** a VIEW usability check fails
- **THEN** the report uses the label “视图查询报错” and includes the first-level dependency object(s) when available

#### Scenario: Permission-related failure
- **WHEN** a usability failure is likely caused by missing privileges
- **THEN** the report indicates the missing object/privilege and suggests GRANT actions

#### Scenario: Unsupported or blocked object
- **WHEN** a VIEW/SYNONYM is marked unsupported/blocked
- **THEN** the report marks the usability result as SKIPPED and includes the specific blocking reason

#### Scenario: Dependency report missing
- **WHEN** dependency reports are missing
- **THEN** the report falls back to error classification and notes the missing dependency context
