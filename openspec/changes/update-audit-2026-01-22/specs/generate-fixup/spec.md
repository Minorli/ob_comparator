## ADDED Requirements

### Requirement: Fixup output cleanup
The system SHALL clean the fixup output directory before generating new scripts and log per-file deletion failures without aborting generation.

#### Scenario: No fixup targets
- **WHEN** generate_fixup is enabled but master_list is empty
- **THEN** the system still cleans the fixup directory and reports that no new scripts were generated

#### Scenario: Deletion failure
- **WHEN** a file in the fixup directory cannot be removed
- **THEN** the system logs a warning and continues cleaning remaining entries

#### Scenario: Absolute path safety
- **WHEN** fixup_dir is an absolute path outside the working directory
- **THEN** the system skips cleanup unless fixup_force_clean is enabled

### Requirement: PL/SQL collection range cleanup
The system SHALL fix single-dot range syntax after FIRST/LAST/COUNT in PL/SQL FOR loops by converting it to a double-dot range.

#### Scenario: FIRST/LAST single-dot range
- **WHEN** a PL/SQL DDL contains `collection.FIRST.collection.LAST`
- **THEN** the cleanup rewrites it as `collection.FIRST..collection.LAST`

#### Scenario: COUNT single-dot range
- **WHEN** a PL/SQL DDL contains `collection.COUNT.var_end`
- **THEN** the cleanup rewrites it as `collection.COUNT..var_end`

#### Scenario: Already-correct range
- **WHEN** a PL/SQL DDL already contains `collection.FIRST..collection.LAST`
- **THEN** the cleanup leaves it unchanged

### Requirement: Dependency-aware PL/SQL ordering
The system SHALL order fixup DDL for PACKAGE/PACKAGE BODY/TYPE/TYPE BODY/PROCEDURE/FUNCTION/TRIGGER using dependency pairs when available, ensuring specs precede bodies and TYPE precedes routines.

#### Scenario: Type before routine
- **WHEN** a FUNCTION depends on a TYPE
- **THEN** the TYPE fixup DDL is generated before the FUNCTION DDL

#### Scenario: Package body after package
- **WHEN** a PACKAGE BODY exists for a PACKAGE
- **THEN** the PACKAGE DDL is generated before the PACKAGE BODY DDL

#### Scenario: Dependency cycle
- **WHEN** dependency pairs form a cycle
- **THEN** the system falls back to a stable deterministic order and logs a warning
