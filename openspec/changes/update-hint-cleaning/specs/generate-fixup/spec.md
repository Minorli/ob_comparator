## MODIFIED Requirements
### Requirement: DDL cleanup for OceanBase
The system SHALL remove Oracle-only clauses from generated DDL to improve OceanBase compatibility, preserve OceanBase-supported VIEW syntax, and filter Oracle hint comments according to ddl_hint_policy, ddl_hint_allowlist, and ddl_hint_denylist.

#### Scenario: VIEW cleanup removes Oracle-only modifiers
- **WHEN** a VIEW DDL contains Oracle-only modifiers such as EDITIONABLE
- **THEN** the modifiers are removed while preserving FORCE/NO FORCE and WITH READ ONLY/WITH CHECK OPTION

#### Scenario: VIEW cleanup uses OceanBase version
- **WHEN** a VIEW DDL contains WITH CHECK OPTION and OceanBase version < 4.2.5.7
- **THEN** the WITH CHECK OPTION clause is removed

#### Scenario: VIEW cleanup preserves CHECK OPTION on supported versions
- **WHEN** a VIEW DDL contains WITH CHECK OPTION and OceanBase version >= 4.2.5.7
- **THEN** the WITH CHECK OPTION clause is preserved

#### Scenario: Hint policy keep_supported
- **WHEN** ddl_hint_policy is keep_supported and a DDL statement contains a mix of supported and unsupported hints
- **THEN** supported hints are preserved and unsupported hints are removed

#### Scenario: Hint policy drop_all
- **WHEN** ddl_hint_policy is drop_all
- **THEN** all hint comment blocks are removed from generated DDL

#### Scenario: Hint policy keep_all
- **WHEN** ddl_hint_policy is keep_all and a hint is not supported by OceanBase
- **THEN** the hint is preserved unless it is in ddl_hint_denylist

#### Scenario: Empty hint block
- **WHEN** all hints in a comment are removed by filtering
- **THEN** the hint comment block itself is removed from the DDL
