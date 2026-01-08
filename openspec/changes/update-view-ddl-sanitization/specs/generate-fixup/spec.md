## ADDED Requirements

### Requirement: View DDL sanitization
The system SHALL sanitize dbcat-generated VIEW DDL to prevent syntax errors caused by malformed tokens or inline comment collapse.

#### Scenario: Split identifier in select list
- **WHEN** a dbcat VIEW DDL line contains an identifier token split by stray whitespace
- **AND** the rejoined token matches a known column name in the view metadata
- **THEN** the sanitizer rejoins the token before emitting the DDL

#### Scenario: Inline comment collapse
- **WHEN** a dbcat VIEW DDL select list contains inline "--" comments followed by additional tokens on the same line
- **THEN** the sanitizer inserts a line break after the comment to preserve valid SQL

## MODIFIED Requirements

### Requirement: DDL cleanup for OceanBase
The system SHALL remove Oracle-only clauses from generated DDL to improve OceanBase compatibility, and SHALL preserve WITH CHECK OPTION only when the OceanBase version is 4.2.5.7 or higher.

#### Scenario: VIEW cleanup uses OceanBase version
- **WHEN** a VIEW DDL contains Oracle-only clauses such as EDITIONABLE or WITH CHECK OPTION
- **AND** the detected OceanBase version is less than 4.2.5.7
- **THEN** WITH CHECK OPTION is removed during cleanup

#### Scenario: VIEW cleanup preserves WITH CHECK OPTION
- **WHEN** a VIEW DDL contains WITH CHECK OPTION
- **AND** the detected OceanBase version is 4.2.5.7 or higher
- **THEN** WITH CHECK OPTION is preserved
