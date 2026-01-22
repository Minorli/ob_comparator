## ADDED Requirements

### Requirement: dbcat failure policy handling
The system SHALL apply dbcat_failure_policy when dbcat extraction fails for a chunk or schema.

#### Scenario: dbcat policy abort
- **WHEN** dbcat extraction fails and dbcat_failure_policy is abort
- **THEN** the run fails with an error after reporting the failure

#### Scenario: dbcat policy fallback
- **WHEN** dbcat extraction fails and dbcat_failure_policy is fallback
- **THEN** the system attempts DBMS_METADATA fallback per object where supported and reports any remaining failures

#### Scenario: dbcat policy continue
- **WHEN** dbcat extraction fails and dbcat_failure_policy is continue
- **THEN** the system skips the failed objects and reports them in the failure summary

### Requirement: DBMS_METADATA batch fallback
The system SHALL attempt per-object DBMS_METADATA fallback when batch DDL fetch fails for specific objects, honoring retry limits.

#### Scenario: Batch fetch partial failure
- **WHEN** oracle_get_ddl_batch fails for a subset of objects
- **THEN** the system attempts single-object DBMS_METADATA fetches for those objects and reports any final failures
