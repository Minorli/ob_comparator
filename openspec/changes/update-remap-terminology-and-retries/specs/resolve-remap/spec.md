## ADDED Requirements

### Requirement: Synonym remap policy
The system SHALL support a synonym_remap_policy configuration to control synonym inference behavior when no explicit rule exists.

#### Scenario: Synonym policy source_only
- **WHEN** synonym_remap_policy is source_only and no explicit remap exists
- **THEN** the synonym remains in the source schema

#### Scenario: Synonym policy infer
- **WHEN** synonym_remap_policy is infer and dependency inference yields a single target schema
- **THEN** the synonym maps to that inferred target schema

#### Scenario: Synonym policy auto in 1:1 mapping
- **WHEN** synonym_remap_policy is auto and the source schema maps 1:1 to itself
- **THEN** the synonym remains in the source schema

## MODIFIED Requirements

### Requirement: Synonym dependency inference
The system SHALL infer non-PUBLIC synonym targets based on direct dependencies when no explicit rule exists and synonym_remap_policy allows inference.

#### Scenario: Synonym depends on remapped table
- **WHEN** a non-PUBLIC synonym references a TABLE remapped to another schema and synonym_remap_policy allows inference
- **THEN** the synonym is inferred to that target schema

#### Scenario: Synonym in 1:1 schema mapping
- **WHEN** the source schema maps 1:1 to itself and no explicit remap exists
- **THEN** the synonym target schema remains the source schema

#### Scenario: Synonym policy source_only
- **WHEN** synonym_remap_policy is source_only and no explicit remap exists
- **THEN** the synonym target schema remains the source schema
