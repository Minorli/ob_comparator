## ADDED Requirements

### Requirement: NUMBER equivalence suppresses fixup
The system SHALL avoid generating number_precision fixup statements when NUMBER-equivalent forms match after normalization.

#### Scenario: NUMBER(*,2) vs NUMBER(38,2)
- **WHEN** the comparison treats NUMBER(*,2) and NUMBER(38,2) as matching
- **THEN** no MODIFY COLUMN fixup is generated for that column
