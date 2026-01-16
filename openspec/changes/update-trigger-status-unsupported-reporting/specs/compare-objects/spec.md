## MODIFIED Requirements

### Requirement: Trigger comparison
The system SHALL validate TRIGGER existence and compare trigger event, enabled status, and validity status between source and target when TRIGGER checks are enabled.

#### Scenario: Trigger enabled/valid mismatch
- **WHEN** a trigger exists in both source and target but ENABLED/DISABLED or VALID/INVALID differs
- **THEN** the trigger comparison reports a mismatch with detailed status differences

#### Scenario: Trigger missing or extra
- **WHEN** a trigger exists only in the source or only in the target
- **THEN** the trigger comparison records it as missing or extra

## ADDED Requirements

### Requirement: Unsupported object classification
The system SHALL classify missing objects into supported, unsupported, or blocked based on blacklist tables, compatibility rules, and dependency relationships.

#### Scenario: Blacklisted table unsupported
- **WHEN** a missing TABLE is marked by blacklist rules
- **THEN** the TABLE is classified as unsupported and excluded from supported-missing counts

#### Scenario: Object blocked by unsupported dependency
- **WHEN** a VIEW/PROCEDURE/FUNCTION/PACKAGE/SYNONYM/TRIGGER depends on an unsupported object
- **THEN** the object is classified as blocked with a dependency reason
