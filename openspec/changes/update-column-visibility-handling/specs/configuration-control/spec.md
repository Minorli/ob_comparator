## ADDED Requirements

### Requirement: Column visibility policy
The system SHALL honor column_visibility_policy to control visibility comparison and fixup generation.

#### Scenario: Auto policy
- **WHEN** column_visibility_policy is auto
- **THEN** visibility enforcement is enabled only when metadata and target support are available

#### Scenario: Unknown policy
- **WHEN** column_visibility_policy is not one of auto/enforce/ignore
- **THEN** the system logs a warning and defaults to auto
