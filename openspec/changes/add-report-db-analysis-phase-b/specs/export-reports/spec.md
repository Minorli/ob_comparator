## ADDED Requirements

### Requirement: Report DB write error tracking
The system SHALL record report-to-db write errors to a dedicated table when any insert fails, without aborting the main workflow unless report_db_fail_abort is enabled.

#### Scenario: Write error captured
- **WHEN** a report_db insert fails
- **THEN** the system writes an error row containing report_id, table, SQL snippet, and error message

#### Scenario: Write error table missing
- **WHEN** the write error table cannot be created
- **THEN** the system logs a warning and continues without error tracking

### Requirement: Resolution table for manual closure
The system SHALL provide a resolution table to allow users to mark objects as resolved or waived.

#### Scenario: User marks resolved
- **WHEN** a user inserts a resolution row
- **THEN** the pending-actions view excludes the resolved entry

### Requirement: Pending actions view
The system SHALL create a view that joins actions with resolution, showing only unresolved items.

#### Scenario: Pending actions visible
- **WHEN** report_db is enabled
- **THEN** the view lists unresolved action items for each report_id

### Requirement: Grant classification view
The system SHALL create a view that classifies GRANT differences by missing/extra and with_grant_option.

#### Scenario: Missing grants categorized
- **WHEN** GRANT rows exist
- **THEN** the view labels missing grants and whether grant option is required

### Requirement: Usability classification view
The system SHALL create a view that categorizes usability failures into common reason codes.

#### Scenario: Usability categorized
- **WHEN** usability rows exist
- **THEN** the view exposes a reason_code derived from the reason text
