## ADDED Requirements

### Requirement: TABLE grant sections must distinguish object and column grants
The generated owner-level grant files SHALL keep table object grants and table column grants in separate readable subsections within the same `OBJECT_TYPE: TABLE` block.

#### Scenario: Owner file contains both table object grants and table column grants
- **WHEN** a generated owner grant file contains `TABLE` grants
- **AND** the file includes both ordinary table grants and column-level grants
- **THEN** the file SHALL render two subsections:
  - `TABLE_OBJECT_GRANTS`
  - `TABLE_COLUMN_GRANTS`
- **AND** the underlying statements SHALL remain unchanged and executable

#### Scenario: Non-table object types
- **WHEN** a generated owner grant file contains non-table object types
- **THEN** the renderer SHALL keep the existing object-type section format without adding table-specific subsection labels
