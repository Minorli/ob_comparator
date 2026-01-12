## ADDED Requirements

### Requirement: PL/SQL punctuation sanitization
The system SHALL sanitize full-width punctuation in PL/SQL fixup DDL to improve OceanBase compatibility while preserving string literals and quoted identifiers.

#### Scenario: Full-width punctuation outside strings
- **WHEN** a PL/SQL DDL contains full-width punctuation outside string literals, comments, or quoted identifiers
- **THEN** the system replaces those characters with ASCII equivalents before writing the fixup DDL

#### Scenario: Full-width punctuation inside string literals
- **WHEN** a PL/SQL DDL contains full-width punctuation inside string literals or quoted identifiers
- **THEN** the system preserves the original text without modification

#### Scenario: Sanitization disabled
- **WHEN** PL/SQL punctuation sanitization is disabled by configuration
- **THEN** the system writes the original DDL and logs that sanitization was skipped

### Requirement: PL/SQL sanitization report
The system SHALL write a report file listing PL/SQL objects whose fixup DDL was sanitized, including the replacement count.

#### Scenario: Sanitization report generated
- **WHEN** PL/SQL punctuation sanitization replaces full-width punctuation during fixup generation
- **THEN** the system writes a report file containing the object type, object name, and replacement count
