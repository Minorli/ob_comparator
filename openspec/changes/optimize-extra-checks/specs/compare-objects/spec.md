## ADDED Requirements
### Requirement: Extra object check performance safeguards
The system SHALL optimize extra object checks by reusing precomputed normalized metadata and preserving deterministic ordering.

#### Scenario: Reuse precomputed signatures
- **WHEN** INDEX/CONSTRAINT/TRIGGER checks are executed
- **THEN** the system reuses per-table normalized signatures computed once before extra checks

#### Scenario: Deterministic ordering with parallel execution
- **WHEN** extra object checks run with more than one worker
- **THEN** OK and mismatch results are ordered deterministically by target table name

#### Scenario: Time-based progress logging
- **WHEN** extra_check_progress_interval is configured
- **THEN** progress updates are logged at or above that interval during extra object checks
