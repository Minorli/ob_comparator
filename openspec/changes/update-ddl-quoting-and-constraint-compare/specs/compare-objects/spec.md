## MODIFIED Requirements
### Requirement: Constraint comparison
The system SHALL validate PK/UK/FK/CHECK constraints in the source against the target, comparing column sets for PK/UK/FK and normalized expressions for CHECK constraints. When a constraint name exists in the target, the system SHALL treat the constraint as present and only record expression mismatches without marking it missing.

#### Scenario: Constraint comparison
- **WHEN** a TABLE has PK/UK/FK/CHECK constraints in the source
- **THEN** the system compares PK/UK/FK column sets and CHECK normalized expressions with target constraints

#### Scenario: Constraint name exists
- **WHEN** a source constraint name exists in the target but the expression differs
- **THEN** the system records a mismatch detail and does not mark the constraint as missing

#### Scenario: Check expression case/whitespace differences
- **WHEN** CHECK expressions differ only by case, whitespace, or redundant parentheses
- **THEN** the system treats the expressions as equivalent for matching

#### Scenario: Constraint reference info unavailable
- **WHEN** target constraint reference fields cannot be loaded
- **THEN** the system falls back to basic constraint metadata for comparison
