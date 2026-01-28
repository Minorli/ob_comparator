## MODIFIED Requirements

### Requirement: Blacklist report export
The system SHALL export blacklisted tables to main_reports/blacklist_tables.txt grouped by schema with reasons and status details.

#### Scenario: Blacklisted tables listed
- **WHEN** tables match TMP_BLACK_TABLE entries
- **THEN** blacklist_tables.txt lists TABLE, BLACK_TYPE, DATA_TYPE, STATUS, DETAIL, and reason

#### Scenario: Black type normalization
- **WHEN** a blacklisted table has a lowercase or mixed-case BLACK_TYPE
- **THEN** the report normalizes the category and applies the mapped reason text

#### Scenario: Unknown blacklist category
- **WHEN** a blacklisted table has an unrecognized BLACK_TYPE
- **THEN** the report still lists the entry with an unknown reason

#### Scenario: LONG conversion status
- **WHEN** a blacklisted table is marked LONG/LONG RAW
- **THEN** the report includes conversion status details (VERIFIED/MISSING_TABLE/TYPE_MISMATCH/etc.)
- **AND** dependency blocking is determined by target table existence, not by conversion status
