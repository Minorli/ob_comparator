## ADDED Requirements

### Requirement: Sequence comparison respects remap policy
The system SHALL compare SEQUENCE existence using target schemas derived from sequence_remap_policy.

#### Scenario: Source-only policy
- **WHEN** sequence_remap_policy is source_only
- **THEN** the comparison checks SEQUENCE existence only in the source schema on the target side

#### Scenario: Infer policy
- **WHEN** sequence_remap_policy is infer
- **THEN** the comparison checks SEQUENCE existence in the inferred target schema
