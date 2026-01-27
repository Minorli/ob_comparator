## ADDED Requirements

### Requirement: Blocked detail exports for extra objects
The system SHALL export dedicated blocked detail files for INDEX/CONSTRAINT/TRIGGER when those objects are
blocked due to unsupported dependencies.

#### Scenario: Blocked index detail export
- **WHEN** unsupported_objects_detail contains INDEX rows with reason_code DEPENDENCY_UNSUPPORTED
- **AND** report_detail_mode is split
- **THEN** the system writes indexes_blocked_detail_<timestamp>.txt with table/index/dependency details

#### Scenario: Blocked constraint detail export
- **WHEN** unsupported_objects_detail contains CONSTRAINT rows with reason_code DEPENDENCY_UNSUPPORTED
- **AND** report_detail_mode is split
- **THEN** the system writes constraints_blocked_detail_<timestamp>.txt with table/constraint/dependency details

#### Scenario: Blocked trigger detail export
- **WHEN** unsupported_objects_detail contains TRIGGER rows with reason_code DEPENDENCY_UNSUPPORTED
- **AND** report_detail_mode is split
- **THEN** the system writes triggers_blocked_detail_<timestamp>.txt with table/trigger/dependency details

## MODIFIED Requirements

### Requirement: Split detail exports
The system SHALL export detailed mismatch lists to *_detail_<timestamp>.txt files when report_detail_mode is set to split,
including blocked extra-object details when applicable.

#### Scenario: Split mode enabled
- **WHEN** report_detail_mode is split
- **THEN** the system writes missing_objects_detail_<timestamp>.txt and unsupported_objects_detail_<timestamp>.txt
- **AND** writes indexes_blocked_detail_<timestamp>.txt / constraints_blocked_detail_<timestamp>.txt / triggers_blocked_detail_<timestamp>.txt when data exists

### Requirement: Rich report output
The system SHALL provide clear hints that unsupported summary counts may include dependency-blocked entries
and direct users to the correct detail files.

#### Scenario: Blocked entries exist
- **WHEN** blocked INDEX/CONSTRAINT/TRIGGER entries exist
- **THEN** the report includes a hint that dependency-blocked details are listed in blocked detail files

### Requirement: Report index export
The system SHALL clarify report index entries for unsupported vs blocked detail outputs.

#### Scenario: Report index entries generated
- **WHEN** report_index_<timestamp>.txt is generated
- **THEN** it labels indexes_unsupported_detail as syntax-unsupported only
- **AND** it labels indexes_blocked_detail as dependency-blocked only
