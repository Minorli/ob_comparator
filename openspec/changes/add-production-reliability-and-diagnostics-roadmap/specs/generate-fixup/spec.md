## ADDED Requirements

### Requirement: Fixup SQL explanation
The system SHALL include machine-readable and human-readable explanations for generated fixup SQL.

#### Scenario: ALTER generated
- **WHEN** an ALTER TABLE statement is generated
- **THEN** the script or sidecar manifest includes reason code, source evidence, target evidence, rule id, and safety tier

#### Scenario: CREATE object generated
- **WHEN** a CREATE script is generated for a missing object
- **THEN** the manifest records DDL source, compatibility decision, dependencies, grants needed, and manual review requirements

### Requirement: Fixup safety tier manifest
The system SHALL assign every fixup script and executable statement to a safety tier.

#### Scenario: Safe statement generated from whitelist
- **WHEN** a statement belongs to the explicit safe whitelist and has complete evidence
- **THEN** it may be marked `safe`

#### Scenario: Safe whitelist
- **WHEN** the implementation classifies safety tiers
- **THEN** `safe` is limited to existing-object compilation operations such as `ALTER ... COMPILE` for VIEW, TRIGGER, PACKAGE, PACKAGE BODY, TYPE, TYPE BODY, PROCEDURE, and FUNCTION, and any unlisted family defaults to `review` or stricter

#### Scenario: Table shape change generated
- **WHEN** a statement changes table shape, including `ALTER TABLE ADD COLUMN`, `ALTER TABLE MODIFY`, datatype change, length change, nullability change, or default change
- **THEN** it MUST be marked `review` or stricter even if the statement is expected to be correct, because it may lock, rewrite, or depend on target data state

#### Scenario: Review statement generated
- **WHEN** a statement is likely correct but depends on business or data assumptions
- **THEN** it MUST be marked `review`

#### Scenario: Review family examples
- **WHEN** generated SQL creates missing objects, creates or modifies indexes or constraints, writes comments, creates or replaces synonyms, grants privileges, restarts sequences, or replaces executable object text
- **THEN** it MUST be marked `review` unless another rule marks it `destructive` or `manual`

#### Scenario: Destructive statement generated
- **WHEN** a statement drops, truncates, disables, rewrites, or may remove user data/behavior
- **THEN** it MUST be marked `destructive`

#### Scenario: Destructive family examples
- **WHEN** generated SQL drops or truncates objects, drops columns, force-cleans target-only columns or constraints, disables constraints or triggers, revokes privileges, or performs cleanup of extra target objects
- **THEN** it MUST be marked `destructive`

#### Scenario: Manual-only object generated
- **WHEN** an object family is unsupported, degraded, or semi-auto only
- **THEN** generated artifacts MUST be marked `manual`

### Requirement: Fixup plan export
The system SHALL export a fixup plan that can drive reports, run_fixup selection, diagnostic bundles, and future UI.

#### Scenario: Fixup generated
- **WHEN** fixup generation completes
- **THEN** the system writes a machine-readable fixup plan containing file path, object identity, object type, operation, safety tier, reason code, dependencies, and execution contract

### Requirement: Fixup generation checkpoint
The system SHALL record checkpoint metadata during fixup generation.

#### Scenario: Object fixup generated
- **WHEN** fixup generation completes for an object
- **THEN** checkpoint metadata records object identity, generated artifacts, source evidence hash, and target mapping hash

#### Scenario: Object replay requested
- **WHEN** fixup generation is replayed for a specific object
- **THEN** only that object's eligible fixup artifacts are regenerated and the fixup plan is updated consistently
