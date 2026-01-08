## MODIFIED Requirements

### Requirement: DDL extraction fallback
The system SHALL prefer dbcat DDL output and fall back to DBMS_METADATA for TABLE DDL when dbcat output is missing or unsupported. The system SHALL use DBMS_METADATA as the primary source for VIEW DDL when generating missing VIEW fixups, and SHALL ignore dbcat view output.

#### Scenario: dbcat returns unsupported table DDL
- **WHEN** dbcat output indicates unsupported TABLE DDL
- **THEN** the system attempts to fetch TABLE DDL via DBMS_METADATA

#### Scenario: VIEW DDL uses DBMS_METADATA
- **WHEN** a VIEW is missing in the target
- **THEN** the system fetches VIEW DDL via DBMS_METADATA and does not use dbcat view output

#### Scenario: dbcat not configured
- **WHEN** generate_fixup is enabled but dbcat_bin is missing
- **THEN** the system logs a warning and continues with limited DDL sources

### Requirement: DDL cleanup for OceanBase
The system SHALL remove Oracle-only clauses from generated DDL to improve OceanBase compatibility, and SHALL preserve VIEW check options only when the detected OceanBase version supports them.

#### Scenario: VIEW cleanup uses OceanBase version
- **WHEN** a VIEW DDL contains WITH CHECK OPTION and OceanBase version < 4.2.5.7
- **THEN** the WITH CHECK OPTION clause is removed

#### Scenario: VIEW cleanup removes Oracle-only modifiers
- **WHEN** a VIEW DDL contains Oracle-only modifiers such as EDITIONABLE
- **THEN** those modifiers are removed while preserving FORCE/NO FORCE and WITH READ ONLY/WITH CHECK OPTION

#### Scenario: VIEW cleanup preserves CHECK OPTION on supported versions
- **WHEN** a VIEW DDL contains WITH CHECK OPTION and OceanBase version >= 4.2.5.7
- **THEN** the WITH CHECK OPTION clause is preserved
