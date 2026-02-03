## MODIFIED Requirements

### Requirement: PUBLIC synonym preservation
The system SHALL preserve the PUBLIC owner for PUBLIC synonyms unless an explicit remap rule exists, and treat OceanBase __public owner as PUBLIC.

#### Scenario: PUBLIC synonym default behavior
- **WHEN** a synonym is owned by PUBLIC and no explicit remap is provided
- **THEN** the synonym remains in the PUBLIC schema

#### Scenario: OceanBase __public owner
- **WHEN** OceanBase reports a synonym owner as __public
- **THEN** the synonym is treated as PUBLIC for remap and reporting

## ADDED Requirements

### Requirement: View dependency remap avoids alias replacement
The system SHALL remap view dependencies by replacing only object references, not table aliases.

#### Scenario: Alias matches object name
- **WHEN** a view uses an alias that matches an existing object name
- **THEN** the remap process does not replace the alias token
