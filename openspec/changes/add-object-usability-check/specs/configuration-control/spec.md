## ADDED Requirements

### Requirement: Object usability controls
The system SHALL provide configuration controls for object usability checks.

#### Scenario: Usability enabled
- **WHEN** check_object_usability is true
- **THEN** VIEW/SYNONYM usability checks are executed

#### Scenario: Source usability toggle
- **WHEN** check_source_usability is false
- **THEN** only target-side usability is checked

#### Scenario: Timeout control
- **WHEN** usability_check_timeout is configured
- **THEN** each usability query uses the configured timeout

#### Scenario: Concurrency control
- **WHEN** usability_check_workers is configured
- **THEN** the system runs usability checks with that worker count and falls back to a safe default if invalid

#### Scenario: Sampling control
- **WHEN** max_usability_objects is configured and the object count exceeds it
- **THEN** the system samples objects according to usability_sample_ratio
