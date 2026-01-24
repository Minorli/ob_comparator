## ADDED Requirements

### Requirement: NUMBER equivalence normalization
The system SHALL treat NUMBER/DECIMAL/NUMERIC equivalent forms as matching when their normalized NUMBER signatures are equal.

#### Scenario: DECIMAL vs NUMBER
- **WHEN** the source column is DECIMAL(10,2)
- **AND** the target column is NUMBER(10,2)
- **THEN** the column is treated as matching

#### Scenario: NUMBER(p) vs NUMBER(p,0)
- **WHEN** the source column is NUMBER(12)
- **AND** the target column is NUMBER(12,0)
- **THEN** the column is treated as matching

#### Scenario: NUMBER(*,s) vs NUMBER(38,s)
- **WHEN** the source column precision is NULL and scale is 2 (representing NUMBER(*,2))
- **AND** the target column precision is 38 and scale is 2
- **THEN** the column is treated as matching

#### Scenario: NUMBER(*) vs NUMBER(38,0)
- **WHEN** the source column precision is NULL and scale is 0 (representing NUMBER(*))
- **AND** the target column precision is 38 and scale is 0
- **THEN** the column is treated as matching
