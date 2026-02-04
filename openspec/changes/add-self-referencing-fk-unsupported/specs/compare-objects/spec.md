## MODIFIED Requirements

### Requirement: Constraint comparison
The system SHALL classify unsupported constraint patterns and report them in unsupported detail outputs.

#### Scenario: Self-referencing foreign key
- **WHEN** a source FK constraint references the same table as its owning table
- **THEN** the system marks the constraint as UNSUPPORTED with reason code `FK_SELF_REF`, includes it in `constraints_unsupported_detail` and `unsupported_objects_detail`, and excludes it from fixup generation
