## ADDED Requirements

### Requirement: View dependency fallback for remap
The system SHALL use Oracle dependency metadata as a fallback to derive VIEW dependencies for remap when SQL extraction is incomplete.

#### Scenario: Subquery view dependency fallback
- **WHEN** a VIEW DDL contains a subquery and SQL extraction yields no dependencies
- **AND** Oracle dependency metadata is available for the VIEW
- **THEN** the system uses the dependency metadata to drive remap replacements

### Requirement: Public synonym resolution in VIEW DDL
The system SHALL resolve PUBLIC synonym references in VIEW DDL to their base objects before applying remap rules.

#### Scenario: View references PUBLIC synonym
- **WHEN** a VIEW DDL references a PUBLIC synonym that resolves to a base object
- **THEN** the rewritten VIEW DDL replaces the synonym name with the base object (after remap)
