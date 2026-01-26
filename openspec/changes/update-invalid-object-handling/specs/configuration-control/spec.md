## ADDED Requirements

### Requirement: Invalid source policy switch
The system SHALL expose configuration switches to control how INVALID source objects are handled during check and fixup.

#### Scenario: Default policy applied
- **WHEN** `invalid_source_policy` is not provided
- **THEN** the system uses the default policy and logs it in the run summary

#### Scenario: Custom policy applied
- **WHEN** `invalid_source_policy` is set to `block`, `skip`, or `fixup`
- **THEN** the system applies the selected behavior consistently across supported object types

### Requirement: Invalid source type scope
The system SHALL allow configuring which object types participate in INVALID-source handling.

#### Scenario: Restrict invalid scope to packages
- **WHEN** `invalid_source_types` is set to `PACKAGE,PACKAGE BODY`
- **THEN** only invalid packages are subject to invalid-source handling and other types remain unaffected

