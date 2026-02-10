## ADDED Requirements

### Requirement: Extra cleanup candidate toggle
The system SHALL support `generate_extra_cleanup` to control whether target-extra cleanup candidates are exported.

#### Scenario: Default disabled
- **WHEN** `generate_extra_cleanup` is missing from config
- **THEN** it defaults to `false`
- **AND** no cleanup candidate scripts are generated

#### Scenario: Explicit enabled
- **WHEN** `generate_extra_cleanup=true`
- **THEN** the run exports cleanup candidate scripts under `fixup_scripts/cleanup_candidates/`
- **AND** generated statements are commented candidates for manual review
