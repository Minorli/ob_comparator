## ADDED Requirements

### Requirement: Skip invalid source objects during fixup
The system SHALL skip fixup DDL generation for source objects marked INVALID and record the skip reason in logs.

#### Scenario: Invalid VIEW skipped
- **WHEN** a missing VIEW is INVALID in the source
- **THEN** no VIEW DDL is generated for that object and the skip reason is recorded

#### Scenario: Invalid TRIGGER skipped
- **WHEN** a missing TRIGGER is INVALID in the source
- **THEN** no TRIGGER DDL is generated for that object and the skip reason is recorded

### Requirement: PACKAGE fixup dependency ordering
The system SHALL order PACKAGE and PACKAGE BODY fixup generation using dependency data and ensure PACKAGE BODY entries are emitted after their PACKAGE specs.

#### Scenario: Body after spec
- **WHEN** a PACKAGE and PACKAGE BODY are both missing
- **THEN** the PACKAGE DDL is emitted before the PACKAGE BODY DDL

#### Scenario: Package cycle detected
- **WHEN** PACKAGE dependencies form a cycle
- **THEN** the system logs a warning and falls back to a stable deterministic order
