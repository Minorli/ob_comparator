## ADDED Requirements

### Requirement: View fixup chain report
The system SHALL export a view fixup dependency chain report to main_reports/VIEWs_chain_<timestamp>.txt for views that require fixup, including synonym hops.

#### Scenario: View chain exported
- **WHEN** one or more views require fixup
- **THEN** VIEWs_chain_<timestamp>.txt includes dependency chains for those views

#### Scenario: Chain annotations
- **WHEN** a chain is written
- **THEN** each hop is annotated with object type, owner, existence (EXISTS/MISSING), and grant status (GRANT_OK/GRANT_MISSING)

#### Scenario: Synonym hop resolved
- **WHEN** a chain includes a SYNONYM and its target metadata is available
- **THEN** the chain expands the SYNONYM to its referenced target type and owner

#### Scenario: Cyclic dependencies
- **WHEN** a dependency cycle is detected
- **THEN** the report marks the cycle and stops traversal for that chain
