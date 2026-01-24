## ADDED Requirements

### Requirement: DOUBLE PRECISION alias normalization
The system SHALL treat `DOUBLE PRECISION` as equivalent to `BINARY_DOUBLE` when comparing column types.

#### Scenario: Alias normalized to BINARY_DOUBLE
- **WHEN** a source column type is reported as `DOUBLE PRECISION`
- **AND** the target column type is `BINARY_DOUBLE`
- **THEN** the column is treated as matching for comparison purposes
