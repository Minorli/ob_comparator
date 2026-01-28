## ADDED Requirements

### Requirement: View constraint cleanup mode
The system SHALL support `view_constraint_cleanup` with values `auto|force|off`.

#### Scenario: Default auto
- **WHEN** view_constraint_cleanup is missing
- **THEN** it defaults to `auto`

#### Scenario: Force
- **WHEN** view_constraint_cleanup is `force`
- **THEN** VIEW column-list constraints are always removed

#### Scenario: Off
- **WHEN** view_constraint_cleanup is `off`
- **THEN** VIEW column-list constraints are not removed
