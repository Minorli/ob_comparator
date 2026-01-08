## MODIFIED Requirements
### Requirement: DDL extraction fallback
The system SHALL prefer dbcat DDL output and fall back to DBMS_METADATA for TABLE and VIEW DDL when dbcat output is missing or unsupported.

#### Scenario: dbcat returns unsupported table DDL
- **WHEN** dbcat output indicates unsupported TABLE DDL
- **THEN** the system attempts to fetch TABLE DDL via DBMS_METADATA

#### Scenario: dbcat provides view DDL
- **WHEN** dbcat returns VIEW DDL for a missing VIEW
- **THEN** the system uses the dbcat DDL for fixup generation

#### Scenario: dbcat missing view DDL
- **WHEN** dbcat does not return VIEW DDL for a missing VIEW
- **THEN** the system attempts to fetch VIEW DDL via DBMS_METADATA

#### Scenario: dbcat not configured
- **WHEN** generate_fixup is enabled but dbcat_bin is missing
- **THEN** the system logs a warning and continues with limited DDL sources
