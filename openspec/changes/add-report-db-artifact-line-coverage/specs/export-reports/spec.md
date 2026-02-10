## ADDED Requirements

### Requirement: Full-scope report DB text coverage
The system SHALL persist all run-directory txt report contents into report DB when `report_to_db=true` and `report_db_store_scope=full`, so operators can query report details without opening txt files.

#### Scenario: Full scope write
- **WHEN** a run completes with `report_db_store_scope=full`
- **THEN** every txt artifact under the run directory is persisted line-by-line with file identity and line number

#### Scenario: Preserve raw line order
- **WHEN** a txt file contains blank lines, comment lines, and data lines
- **THEN** line records preserve original line numbering and textual content order

### Requirement: Artifact coverage discoverability
The system SHALL expose artifact-level coverage status that reflects whether each txt artifact is queryable in DB for the current run.

#### Scenario: Full scope with line persistence
- **WHEN** txt lines are successfully persisted for a run
- **THEN** artifact catalog indicates DB coverage for those txt artifacts

#### Scenario: Scope limited
- **WHEN** `report_db_store_scope` is `summary` or `core`
- **THEN** txt artifacts may remain TXT-only and are marked accordingly in artifact catalog
