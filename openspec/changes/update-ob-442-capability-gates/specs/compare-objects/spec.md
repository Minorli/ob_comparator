## MODIFIED Requirements
### Requirement: Primary object coverage
The system SHALL compare source and target objects for the primary object set and apply print-only handling based on effective feature gates.

#### Scenario: MATERIALIZED VIEW in legacy mode
- **WHEN** `effective_mview_enabled=false`
- **THEN** MATERIALIZED VIEW objects are recorded as print-only and skipped from OceanBase existence validation

#### Scenario: MATERIALIZED VIEW in enabled mode
- **WHEN** `effective_mview_enabled=true`
- **THEN** MATERIALIZED VIEW objects are included in normal primary-object comparison and missing/extra evaluation

