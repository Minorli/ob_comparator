## MODIFIED Requirements

### Requirement: Result handling
The fixup executor SHALL move successfully executed scripts to fixup_scripts/done and keep failed scripts in place. For grant scripts, the executor SHALL remove successfully executed GRANT statements from the file, leaving only failed statements for retry.

#### Scenario: Successful execution
- **WHEN** a script executes without errors
- **THEN** it is moved into the done subdirectory

#### Scenario: Failed execution
- **WHEN** a script execution fails
- **THEN** it remains in place for retry

#### Scenario: Grant file partial success
- **WHEN** a grant script contains both successful and failed GRANT statements
- **THEN** the script is rewritten to include only failed GRANT statements
- **AND** the script remains in place for retry

## ADDED Requirements

### Requirement: Fixup error report
The fixup executor SHALL write a concise error report to fixup_scripts/errors/fixup_errors_<timestamp>.txt for failed statements.

#### Scenario: Error report generated
- **WHEN** one or more statements fail during execution
- **THEN** the executor writes an error report with file name, statement index, and error message

#### Scenario: Error report capped
- **WHEN** the number of failed statements exceeds the report limit
- **THEN** the report includes only the first 200 failures
