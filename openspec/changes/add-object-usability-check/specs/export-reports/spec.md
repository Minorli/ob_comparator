## ADDED Requirements

### Requirement: Usability detail report
The system SHALL export a usability detail report when usability checks are enabled.

#### Scenario: Usability report generated
- **WHEN** check_object_usability is true
- **THEN** the system writes usability_check_detail_<timestamp>.txt with pipe-delimited fields

#### Scenario: Usability report suppressed
- **WHEN** check_object_usability is false
- **THEN** no usability report files are generated

### Requirement: Usability summary in main report
The system SHALL include a usability summary section in the main report when usability checks are enabled.

#### Scenario: Summary shown
- **WHEN** check_object_usability is true
- **THEN** the main report includes total checked, usable, unusable, expected_unusable, and timeout counts
