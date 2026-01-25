## MODIFIED Requirements

### Requirement: Comment comparison
The system SHALL compare table and column comments when comment checking is enabled and metadata is
available, excluding auto-generated columns from comment mismatch detection and counts. Auto-
generated columns include `__PK_INCREMENT`, SYS_NC hidden columns matching `SYS_NC[0-9]+$` or
`SYS_NC_[A-Z_]+$`, and OMS ignore-list columns (`OMS_OBJECT_NUMBER`, `OMS_RELATIVE_FNO`,
`OMS_BLOCK_NUMBER`, `OMS_ROW_NUMBER`).

#### Scenario: Comments enabled
- **WHEN** check_comments is enabled and comment metadata is loaded
- **THEN** the system reports mismatched table or column comments

#### Scenario: Target table missing
- **WHEN** a TABLE is missing in the target
- **THEN** comment comparison for that table is skipped

#### Scenario: Comment whitespace normalization
- **WHEN** comments differ only by whitespace
- **THEN** the system treats the comments as equivalent

#### Scenario: Auto-generated columns ignored
- **WHEN** the target exposes auto-generated columns such as `__PK_INCREMENT`, SYS_NC hidden columns
  (`SYS_NC[0-9]+$`, `SYS_NC_[A-Z_]+$`), or OMS ignore-list columns
- **THEN** comment mismatches for those columns are not reported

#### Scenario: Comments metadata unavailable
- **WHEN** comment metadata cannot be loaded from either side
- **THEN** the comment comparison is skipped with a recorded reason
