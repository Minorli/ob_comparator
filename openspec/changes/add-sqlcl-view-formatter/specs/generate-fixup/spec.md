## ADDED Requirements
### Requirement: Post-fixup DDL formatting
The system SHALL optionally format final DDL output **after** fixup generation, without altering fixup logic or object selection.

#### Scenario: Formatting enabled for a type
- **WHEN** ddl_format_enable is true and the object type is in ddl_format_types
- **THEN** the system formats the final DDL output and writes the formatted script

#### Scenario: Formatting disabled
- **WHEN** ddl_format_enable is false
- **THEN** the system writes the original DDL output without formatting

#### Scenario: Type not selected
- **WHEN** ddl_format_enable is true but the object type is not in ddl_format_types
- **THEN** the system writes the original DDL output without formatting

#### Scenario: Formatting failure fallback
- **WHEN** formatting fails or times out and ddl_format_fail_policy is fallback
- **THEN** the system writes the original DDL output for that object

#### Scenario: Large DDL skipped
- **WHEN** a DDL exceeds ddl_format_max_lines or ddl_format_max_bytes
- **THEN** the system skips formatting for that object and writes the original output

### Requirement: PL/SQL slash handling for formatting
The system SHALL remove trailing `/` delimiters before formatting PL/SQL DDL and restore them afterward.

#### Scenario: PL/SQL DDL with slash
- **WHEN** a PL/SQL DDL contains a trailing `/` delimiter
- **THEN** the formatter removes it before SQLcl invocation and re-appends it after formatting

### Requirement: Formatting batch execution
The system SHALL format DDL in batches when ddl_format_batch_size is configured.

#### Scenario: Batch formatting
- **WHEN** ddl_format_batch_size is set to N
- **THEN** the formatter invokes SQLcl with at most N DDL files per batch

### Requirement: Formatting does not affect fixup logic
The system SHALL NOT use formatted DDL for dependency analysis, remap inference, or fixup decisions.

#### Scenario: Formatting enabled
- **WHEN** formatting is enabled
- **THEN** the object selection, remap, and fixup generation behavior remains unchanged
