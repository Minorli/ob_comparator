# configuration-control

## ADDED Requirements

### Requirement: Name-pattern blacklist configuration
The system SHALL support configuring name-pattern blacklist keywords via settings.

#### Scenario: Inline keyword list
- **WHEN** `blacklist_name_patterns` is provided
- **THEN** the system parses a comma-separated keyword list and trims whitespace

#### Scenario: Keyword file list
- **WHEN** `blacklist_name_patterns_file` is provided
- **THEN** the system loads one keyword per line and ignores blank/comment lines

#### Scenario: Combined keywords
- **WHEN** both inline and file-based keywords are provided
- **THEN** the system merges them into a de-duplicated list for rule rendering
