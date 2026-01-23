## ADDED Requirements

### Requirement: Fixup cleanup safety override
The system SHALL honor fixup_force_clean to allow cleanup of fixup_dir even when the path is outside the working directory.

#### Scenario: Override enabled
- **WHEN** fixup_force_clean is true
- **THEN** fixup directory cleanup proceeds even for absolute paths outside the project

#### Scenario: Override disabled
- **WHEN** fixup_force_clean is false
- **THEN** cleanup is skipped for absolute paths outside the project and a warning is logged
