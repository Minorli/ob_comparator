## MODIFIED Requirements
### Requirement: Blacklist report export
The system SHALL export blacklisted tables to main_reports/blacklist_tables.txt grouped by schema with reasons and status details.

#### Scenario: Blacklisted tables listed
- **WHEN** tables match TMP_BLACK_TABLE entries
- **THEN** blacklist_tables.txt lists TABLE, BLACK_TYPE, DATA_TYPE, STATUS, DETAIL, and reason

#### Scenario: Rule-derived blacklist entries
- **WHEN** tables match rule-derived blacklist entries
- **THEN** blacklist_tables.txt lists the entries and includes rule source information in DETAIL

#### Scenario: Black type normalization
- **WHEN** a blacklisted table has a lowercase or mixed-case BLACK_TYPE
- **THEN** the report normalizes the category and applies the mapped reason text

#### Scenario: Unknown blacklist category
- **WHEN** a blacklisted table has an unrecognized BLACK_TYPE
- **THEN** the report still lists the entry with an unknown reason

#### Scenario: LONG conversion status
- **WHEN** a blacklisted table is marked LONG/LONG RAW
- **THEN** the report includes conversion status such as VERIFIED, MISSING_TABLE, MISSING_COLUMN, or TYPE_MISMATCH
