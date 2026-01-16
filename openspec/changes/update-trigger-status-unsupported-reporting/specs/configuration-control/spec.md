## ADDED Requirements

### Requirement: Report detail mode
The system SHALL allow configuring report_detail_mode to control summary vs detail report output.

#### Scenario: Split mode default
- **WHEN** report_detail_mode is split or missing
- **THEN** the system writes a concise main report and separate detail files

#### Scenario: Full mode
- **WHEN** report_detail_mode is full
- **THEN** the system emits full listings in the main report

### Requirement: View compatibility rules configuration
The system SHALL support optional view compatibility rule configuration to enable or disable specific incompatibility checks.

#### Scenario: Custom view rule file
- **WHEN** view_compat_rules_path is configured
- **THEN** the system loads compatibility patterns from the file

#### Scenario: Dblink policy override
- **WHEN** view_dblink_policy is allow
- **THEN** the system does not mark DBLINK views as unsupported
