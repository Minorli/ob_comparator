## ADDED Requirements

### Requirement: Trigger qualification toggle
The system SHALL support trigger_qualify_schema to control whether trigger DDL names and body references are schema-qualified during fixup generation.

#### Scenario: Trigger qualification enabled
- **WHEN** trigger_qualify_schema is true
- **THEN** trigger DDL is rewritten to use schema-qualified object names

### Requirement: Sequence remap policy
The system SHALL support sequence_remap_policy with values infer, source_only, and dominant_table.

#### Scenario: Invalid policy
- **WHEN** sequence_remap_policy is not one of the supported values
- **THEN** the system logs a warning and falls back to infer

### Requirement: Report directory layout
The system SHALL support report_dir_layout with values flat and per_run to control report output structure.

#### Scenario: Per-run layout
- **WHEN** report_dir_layout is per_run
- **THEN** the system creates a timestamped report subdirectory for all report outputs

### Requirement: Fixup config conflict diagnostics
The system SHALL detect incompatible combinations of check_* and fixup_* settings and emit warnings before fixup generation.

#### Scenario: Fixup index without check
- **WHEN** fixup_types includes INDEX but check_extra_types excludes INDEX
- **THEN** the system logs a warning that index fixups will not be generated
