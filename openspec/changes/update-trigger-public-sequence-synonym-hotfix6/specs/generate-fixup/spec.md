## MODIFIED Requirements

### Requirement: Trigger sequence references must be schema-qualified in generated DDL
The system SHALL rewrite unqualified trigger sequence references (`seq_name.NEXTVAL/CURRVAL`) into schema-qualified references in generated trigger DDL.

#### Scenario: Sequence mapping exists
- **WHEN** trigger SQL contains unqualified `seq_name.NEXTVAL/CURRVAL`
- **AND** source sequence has a `SEQUENCE` mapping target
- **THEN** generated trigger DDL uses mapped `schema.sequence.NEXTVAL/CURRVAL`

#### Scenario: Sequence mapping missing
- **WHEN** trigger SQL contains unqualified `seq_name.NEXTVAL/CURRVAL`
- **AND** source sequence has no `SEQUENCE` mapping
- **THEN** generated trigger DDL still schema-qualifies to `source_schema.sequence.NEXTVAL/CURRVAL`

#### Scenario: Explicit remap overrides identity fallback
- **WHEN** trigger SQL contains unqualified `seq_name.NEXTVAL/CURRVAL`
- **AND** remap rules explicitly map `SOURCE_SCHEMA.SEQ_NAME` to a target full name
- **THEN** generated trigger DDL uses the explicit remap target

#### Scenario: Sequence synonym terminal resolution
- **WHEN** trigger SQL contains unqualified `seq_name.NEXTVAL/CURRVAL`
- **AND** `seq_name` resolves via private/public synonym to a terminal sequence
- **THEN** generated trigger DDL resolves terminal sequence first, then applies mapping/remap/fallback rules

#### Scenario: Public synonym terminal outside managed schema
- **WHEN** trigger SQL contains `seq_syn.NEXTVAL/CURRVAL`
- **AND** `seq_syn` is a PUBLIC synonym to a local SEQUENCE outside the managed source schema list
- **AND** the terminal SEQUENCE has no mapping and no explicit remap rule
- **THEN** generated trigger DDL uses the real terminal `owner.sequence.NEXTVAL/CURRVAL`
- **AND** the system SHALL NOT treat the trigger owner plus synonym name as the sequence target

#### Scenario: OB source public owner alias
- **WHEN** OceanBase source synonym metadata stores PUBLIC synonyms with owner `__public`
- **AND** a trigger sequence reference resolves through that synonym to a local SEQUENCE
- **THEN** generated trigger DDL treats the synonym owner as PUBLIC and applies terminal sequence rewrite rules

#### Scenario: Trigger helper metadata does not widen compare scope
- **WHEN** trigger-only PUBLIC sequence synonym metadata is retained for DDL rewrite
- **THEN** the helper synonym and external terminal sequence are not added to the main compare/fixup object set solely because of that helper metadata

#### Scenario: Already qualified references are left unchanged
- **WHEN** trigger SQL contains `schema.seq_name.NEXTVAL/CURRVAL`
- **THEN** rewrite does not duplicate or alter that qualifier by unqualified sequence rewrite rule
