## ADDED Requirements

### Requirement: Synonym chain grant generation for views
The system SHALL resolve synonym chains used in view dependencies and generate object grants for the final targets.

#### Scenario: View references a public synonym
- **WHEN** a VIEW depends on an unqualified name that resolves to a PUBLIC synonym
- **THEN** the system resolves the synonym chain to the final target object
- **AND** emits GRANT statements for the required privilege on the target

#### Scenario: View granted to others via synonym dependency
- **WHEN** a VIEW is granted to non-owner grantees
- **AND** the VIEW depends on a synonym that resolves to a base object
- **THEN** the system emits a GRANT WITH GRANT OPTION for the view owner on the base object when required
