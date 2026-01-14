## ADDED Requirements
### Requirement: Extra check tuning parameters
The system SHALL honor extra_check_workers, extra_check_chunk_size, and extra_check_progress_interval to control extra object checks.

#### Scenario: Invalid extra_check_workers
- **WHEN** extra_check_workers is missing, non-numeric, or <= 0
- **THEN** the system defaults to min(4, CPU) workers

#### Scenario: Invalid extra_check_chunk_size
- **WHEN** extra_check_chunk_size is missing or < 1
- **THEN** the system defaults to 200 tables per chunk

#### Scenario: Invalid extra_check_progress_interval
- **WHEN** extra_check_progress_interval is missing or < 1
- **THEN** the system defaults to 10 seconds and enforces a minimum of 1 second
