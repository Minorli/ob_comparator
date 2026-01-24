## ADDED Requirements

### Requirement: CHECK expression casefold
The system SHALL compare CHECK constraint expressions case-insensitively while preserving string literal contents.

#### Scenario: IS NOT NULL casing difference
- **WHEN** the source CHECK expression uses `IS NOT NULL`
- **AND** the target CHECK expression uses `is not null`
- **THEN** the CHECK constraint is treated as matching

### Requirement: Index expression casefold
The system SHALL compare function-based index expressions case-insensitively while preserving string literal contents.

#### Scenario: DECODE expression casing difference
- **WHEN** the source index expression uses `DECODE(...)`
- **AND** the target index expression uses `decode(...)`
- **THEN** the index definition is treated as matching
