## ADDED Requirements

### Requirement: Release validation gate
The system SHALL require a release validation gate before a public version tag or GitHub release is created.

#### Scenario: Release candidate validation
- **WHEN** a release candidate is prepared
- **THEN** the release evidence MUST include Python compile validation, unit test results, git diff hygiene, tracked-test-file hygiene, and at least one real Oracle->OceanBase main-program smoke run

#### Scenario: Real DB smoke source
- **WHEN** release smoke evidence is recorded for 0.9.x
- **THEN** it uses a controlled real Oracle->OceanBase environment and local-only or ignored smoke fixtures, without requiring tracked test schema initialization files in this change

#### Scenario: Source mode affected
- **WHEN** a release changes `source_db_mode=oceanbase` behavior
- **THEN** the release evidence MUST include an OceanBase-source smoke run or explicitly mark it as a blocker before release

#### Scenario: Fixup execution affected
- **WHEN** a release changes `run_fixup.py` execution behavior
- **THEN** the release evidence MUST include a controlled run_fixup smoke or a documented reason why execution was intentionally not performed

### Requirement: Release evidence artifact
The system SHALL produce a release evidence artifact for each release candidate.

#### Scenario: Evidence file generated
- **WHEN** release validation completes
- **THEN** a release evidence file records version, branch, commit, tag candidate, commands, pass/fail status, report paths, smoke scope, skipped validation, and residual risk

#### Scenario: Validation failure
- **WHEN** any required validation fails
- **THEN** the release evidence marks the release as not publishable and the release process MUST stop before tag creation

### Requirement: Release rollback and hotfix policy
The system SHALL define a rollback and hotfix policy for public releases.

#### Scenario: Released version has production blocker
- **WHEN** a public release is confirmed to contain a production blocker
- **THEN** the operator MUST choose one of retract release note, hotfix release, or rollback guidance and record that decision in the release evidence

#### Scenario: Hotfix release
- **WHEN** a hotfix is prepared for a production blocker
- **THEN** the hotfix release MUST include reproduction evidence, fix evidence, and a focused real-db regression run for the blocker

### Requirement: Release scope freeze
The system SHALL require a scope freeze before release documentation and tag creation.

#### Scenario: Scope freeze recorded
- **WHEN** a release candidate is prepared
- **THEN** included commits, excluded local artifacts, docs touched, and compatibility-impacting changes are recorded before validation starts
