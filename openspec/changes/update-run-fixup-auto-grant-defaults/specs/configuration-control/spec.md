## ADDED Requirements

### Requirement: Fixup auto-grant toggle
The system SHALL support fixup_auto_grant to enable or disable automatic grant application during run_fixup.

#### Scenario: Auto-grant disabled
- **WHEN** fixup_auto_grant is false
- **THEN** run_fixup does not auto-apply grants or retry on permission errors

### Requirement: Fixup auto-grant type scope
The system SHALL support fixup_auto_grant_types to limit auto-grant to specific object types.

#### Scenario: Custom auto-grant types
- **WHEN** fixup_auto_grant_types is configured
- **THEN** run_fixup only applies auto-grant logic for those object types

### Requirement: Fixup auto-grant fallback
The system SHALL support fixup_auto_grant_fallback to control whether direct GRANT statements can be generated when no grant scripts exist.

#### Scenario: Fallback disabled
- **WHEN** fixup_auto_grant_fallback is false
- **AND** no matching GRANT statements exist in grants_miss/grants_all
- **THEN** run_fixup skips auto-grant generation and logs the missing grant
