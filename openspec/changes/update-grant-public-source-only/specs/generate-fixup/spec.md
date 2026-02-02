## MODIFIED Requirements
### Requirement: Dependency-derived grants
The system SHALL add cross-schema grants required by remapped dependency edges and deep view dependency chains, and SHALL NOT infer PUBLIC grants from dependencies unless PUBLIC grants exist explicitly in source privileges.

#### Scenario: Direct dependency edge
- **WHEN** a dependent object references a target object in a different schema
- **THEN** the system adds the required privilege for that referenced object

#### Scenario: View transitive dependency
- **WHEN** a VIEW or MATERIALIZED VIEW depends on a chain of objects across schemas
- **THEN** the system adds grants for referenced objects along the chain

#### Scenario: PUBLIC dependency does not infer grant
- **WHEN** a dependency resolves the grantee schema to PUBLIC
- **THEN** the system does not generate an inferred GRANT to PUBLIC, relying only on explicit source PUBLIC grants
