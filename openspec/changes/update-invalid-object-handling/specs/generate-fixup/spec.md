## ADDED Requirements

### Requirement: Invalid source policy controls fixup generation
The system SHALL honor `invalid_source_policy` when deciding whether to generate fixup DDL for INVALID source objects, and record the decision.

#### Scenario: Default skip for invalid VIEW
- **WHEN** `invalid_source_policy` is not set and a missing VIEW is INVALID in the source
- **THEN** no VIEW DDL is generated for that object and the skip reason is recorded

#### Scenario: Policy block exports to unsupported
- **WHEN** `invalid_source_policy=block` and a missing TRIGGER is INVALID in the source
- **THEN** the DDL is emitted under an unsupported directory with reason SOURCE_INVALID and not included in supported fixups

#### Scenario: Policy fixup generates DDL
- **WHEN** `invalid_source_policy=fixup` and a missing PACKAGE is INVALID in the source
- **THEN** the PACKAGE DDL is generated in the normal fixup directory with an INVALID warning header

### Requirement: PACKAGE fixup dependency ordering
The system SHALL order PACKAGE and PACKAGE BODY fixup generation using dependency data and ensure PACKAGE BODY entries are emitted after their PACKAGE specs.

#### Scenario: Body after spec
- **WHEN** a PACKAGE and PACKAGE BODY are both missing
- **THEN** the PACKAGE DDL is emitted before the PACKAGE BODY DDL

#### Scenario: Package cycle detected
- **WHEN** PACKAGE dependencies form a cycle
- **THEN** the system logs a warning and falls back to a stable deterministic order
