## ADDED Requirements

### Requirement: Schema mapping terminology
The system SHALL use consistent mapping terms (1:1, N:1, 1:N) in schema mapping summaries and report when schema-level fallback is applied.

#### Scenario: One-to-one mapping summary
- **WHEN** a source schema maps directly to the same target schema
- **THEN** the mapping summary labels the relationship as 1:1

#### Scenario: One-to-many mapping fallback
- **WHEN** a source schema maps to multiple target schemas and schema-level resolution is ambiguous
- **THEN** the summary notes a 1:N mapping and states that per-object inference will be applied

#### Scenario: Many-to-one mapping summary
- **WHEN** multiple source schemas map to the same target schema
- **THEN** the summary labels the relationship as N:1
