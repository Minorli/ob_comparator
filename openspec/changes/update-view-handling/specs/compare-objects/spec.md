## ADDED Requirements

### Requirement: View compatibility for SYS/X$ references
The system SHALL classify views that reference SYS.OBJ$ or X$ system objects as unsupported, unless the X$ object exists as a user-defined object in managed source schemas.

#### Scenario: SYS.OBJ$ reference
- **WHEN** a VIEW definition contains a SYS.OBJ$ reference
- **THEN** the VIEW is marked unsupported with reason code VIEW_SYS_OBJ

#### Scenario: X$ reference
- **WHEN** a VIEW definition references an X$ object not found in managed source schemas
- **THEN** the VIEW is marked unsupported with reason code VIEW_X$

#### Scenario: User-defined X$ object
- **WHEN** a VIEW references an X$ object that exists in a managed source schema
- **THEN** the VIEW is NOT blocked for the X$ reason
