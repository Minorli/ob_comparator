## ADDED Requirements

### Requirement: Default auto-grant during fixup execution
The fixup executor SHALL attempt to apply missing grants automatically when auto-grant is enabled, without requiring extra CLI flags.

#### Scenario: Auto-grant enabled by default
- **WHEN** fixup_auto_grant is true
- **AND** a dependent object requires cross-schema privileges
- **THEN** run_fixup applies the required GRANT statements before executing the object

### Requirement: Auto-grant scope by object type
The fixup executor SHALL apply auto-grant only to object types listed in fixup_auto_grant_types.

#### Scenario: Type outside auto-grant scope
- **WHEN** an object type is not included in fixup_auto_grant_types
- **THEN** run_fixup skips auto-grant planning for that object

### Requirement: Dependency-based grant planning
The fixup executor SHALL derive grant requirements from dependency_chains reports (TARGET - REMAPPED section), and VIEWs_chain for views when present.

#### Scenario: Dependency chain report available
- **WHEN** dependency_chains_<ts>.txt exists in the latest report directory
- **THEN** run_fixup uses it to plan required grants for each dependent object

### Requirement: Permission error retry
The fixup executor SHALL retry execution once after applying grants for permission-denied failures.

#### Scenario: Permission denied
- **WHEN** a script fails with ORA-01031 or ORA-01720
- **AND** auto-grant is enabled for that object type
- **THEN** run_fixup applies relevant grants and retries the script once

### Requirement: Grant source precedence
The fixup executor SHALL prefer grants_miss, then grants_all, and optionally auto-generate a grant when no matching statements exist.

#### Scenario: Missing grant statements
- **WHEN** no matching GRANT statements are found in grants_miss or grants_all
- **THEN** run_fixup may generate a direct object GRANT if fallback generation is enabled
