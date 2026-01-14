## MODIFIED Requirements

### Requirement: Sequence remap via dependencies
The system SHALL infer SEQUENCE targets based on dependent object remaps only when sequence_remap_policy is infer. When sequence_remap_policy is source_only, SEQUENCE targets SHALL remain in the source schema. When sequence_remap_policy is dominant_table, SEQUENCE targets SHALL follow the dominant TABLE schema mapping.

#### Scenario: sequence_remap_policy = infer
- **WHEN** a TRIGGER depends on a SEQUENCE and remap inference is enabled
- **THEN** the SEQUENCE target schema follows the inferred dependency schema

#### Scenario: sequence_remap_policy = source_only
- **WHEN** sequence_remap_policy is source_only
- **THEN** the SEQUENCE target schema remains the source schema regardless of dependencies

#### Scenario: sequence_remap_policy = dominant_table
- **WHEN** sequence_remap_policy is dominant_table and schema mapping can be inferred from TABLE remaps
- **THEN** the SEQUENCE target schema follows the dominant TABLE target schema
