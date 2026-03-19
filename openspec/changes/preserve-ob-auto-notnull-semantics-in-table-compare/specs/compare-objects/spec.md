## ADDED Requirements

### Requirement: Preserve OB auto NOT NULL checks for table semantic suppress

When the target OceanBase table already has an enabled equivalent single-column `IS NOT NULL` check represented by internal names such as `*_OBNOTNULL_*` or `*_OBCHECK_*`, the table compare MUST treat that semantic as already present for `NOT NULL ENABLE NOVALIDATE` suppression.

#### Scenario: Auto-generated OB check already exists

- **GIVEN** Oracle source exposes a system `CHECK (<col> IS NOT NULL)` backing `NOT NULL ENABLE NOVALIDATE`
- **AND** OceanBase target already has an enabled equivalent single-column `IS NOT NULL` check using an internal OB auto-generated constraint name
- **WHEN** the comparator performs table compare
- **THEN** it MUST NOT emit `nullability_novalidate_tighten`
- **AND** it MUST NOT generate redundant `table_alter` DDL for that column

### Requirement: Ordinary constraint diff noise behavior remains unchanged

OceanBase internal auto-generated NOT NULL style constraints MUST continue to be suppressed from ordinary constraint mismatch noise.

#### Scenario: Internal OB notnull constraints do not become extra-diff noise

- **GIVEN** OceanBase target contains `*_OBNOTNULL_*` or `*_OBCHECK_*` constraints
- **WHEN** ordinary constraint compare runs
- **THEN** those internal constraints MUST remain suppressed from extra/missing constraint noise
