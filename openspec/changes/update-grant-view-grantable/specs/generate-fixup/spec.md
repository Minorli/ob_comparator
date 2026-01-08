## ADDED Requirements

### Requirement: Grantable privileges for view owners
The system SHALL ensure a view owner has grantable privileges on dependent objects when the view is granted to other users.

#### Scenario: View granted to non-owner
- **WHEN** a VIEW is granted to grantees other than its owner
- **AND** a dependency object requires SELECT or EXECUTE
- **AND** the view owner lacks the GRANTABLE privilege on that dependency
- **THEN** the fixup output includes a GRANT with GRANT OPTION to the view owner for the dependency
