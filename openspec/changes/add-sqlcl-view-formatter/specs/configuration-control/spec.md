## ADDED Requirements
### Requirement: DDL formatting configuration
The system SHALL support optional DDL formatting via ddl_format_enable, ddl_format_types, and ddl_formatter.

#### Scenario: Formatting disabled
- **WHEN** ddl_format_enable is false
- **THEN** no external formatter is invoked and output DDL is written as generated

#### Scenario: Formatting enabled with SQLcl
- **WHEN** ddl_format_enable is true and ddl_formatter is sqlcl
- **THEN** the system invokes SQLcl to format DDL for the selected object types

#### Scenario: Empty type list
- **WHEN** ddl_format_enable is true and ddl_format_types is empty
- **THEN** the system defaults to formatting VIEW only

#### Scenario: Unknown type in list
- **WHEN** ddl_format_types contains an unknown type
- **THEN** the system logs a warning and ignores that entry

### Requirement: DDL formatting performance limits
The system SHALL honor ddl_format_max_lines and ddl_format_max_bytes to skip formatting of very large DDL.

#### Scenario: DDL exceeds line limit
- **WHEN** ddl_format_max_lines is set and a DDL exceeds that line count
- **THEN** the system skips formatting for that object and records the skip reason

#### Scenario: DDL exceeds byte limit
- **WHEN** ddl_format_max_bytes is set and a DDL exceeds that size
- **THEN** the system skips formatting for that object and records the skip reason

#### Scenario: Limits disabled
- **WHEN** ddl_format_max_lines or ddl_format_max_bytes is 0 or negative
- **THEN** the system does not enforce that limit

### Requirement: DDL formatting batch controls
The system SHALL support ddl_format_batch_size and ddl_format_timeout to bound SQLcl execution cost.

#### Scenario: Batch size set
- **WHEN** ddl_format_batch_size is configured
- **THEN** the system formats DDL in batches of at most that size per SQLcl invocation

#### Scenario: Batch timeout
- **WHEN** ddl_format_timeout is configured
- **THEN** each SQLcl batch invocation is terminated if it exceeds the timeout

### Requirement: SQLcl path resolution
The system SHALL accept sqlcl_bin as either a direct executable path or a SQLcl root directory.

#### Scenario: Formatter enabled with SQLcl executable
- **WHEN** ddl_formatter is sqlcl and sqlcl_bin points to an executable
- **THEN** the system invokes that executable

#### Scenario: Formatter enabled with SQLcl root path
- **WHEN** ddl_formatter is sqlcl and sqlcl_bin points to a SQLcl root directory
- **THEN** the system resolves bin/sql (or bin/sql.exe) under that directory and uses it if present

#### Scenario: SQLcl missing
- **WHEN** ddl_formatter is sqlcl and sqlcl_bin is missing or cannot be resolved to an executable
- **THEN** the system terminates with a configuration error

### Requirement: Formatter failure policy
The system SHALL honor ddl_format_fail_policy to decide fallback behavior when formatting fails or times out.

#### Scenario: Fallback policy
- **WHEN** ddl_format_fail_policy is fallback and formatting fails
- **THEN** the system logs a warning and writes the original DDL

#### Scenario: Error policy
- **WHEN** ddl_format_fail_policy is error and formatting fails
- **THEN** the system skips formatting for that object and records the failure in the formatter report
