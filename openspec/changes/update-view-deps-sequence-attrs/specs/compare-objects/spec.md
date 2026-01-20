## ADDED Requirements

### Requirement: Identity and default-on-null column detection
The system SHALL detect IDENTITY_COLUMN and DEFAULT_ON_NULL features in source column metadata and surface mismatches when the target lacks the same feature.

#### Scenario: Source identity column not present in target
- **WHEN** a source TABLE column is marked IDENTITY_COLUMN
- **AND** the target column metadata does not indicate identity
- **THEN** the table comparison records a type mismatch entry for that column

#### Scenario: Source default-on-null column not present in target
- **WHEN** a source TABLE column is marked DEFAULT_ON_NULL
- **AND** the target column metadata does not indicate default-on-null
- **THEN** the table comparison records a type mismatch entry for that column

### Requirement: Sequence attribute comparison
The system SHALL compare SEQUENCE attributes (increment/min/max/cycle/order/cache) when a sequence exists in both source and target schemas.

#### Scenario: Sequence attributes differ
- **WHEN** a sequence exists in both source and target schemas
- **AND** any of increment_by, min_value, max_value, cycle_flag, order_flag, or cache_size differs
- **THEN** the sequence comparison reports an attribute mismatch with the differing attributes
