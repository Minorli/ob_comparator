## ADDED Requirements

### Requirement: View chain autofix skips existing views by default
The fixup executor SHALL skip view-chain auto-execution for views that already exist in OceanBase, while still emitting per-view plan and SQL artifacts marked as SKIPPED.

#### Scenario: View already exists
- **WHEN** --view-chain-autofix is enabled
- **AND** the root view exists in OceanBase
- **THEN** the executor skips auto-execution for that view
- **AND** writes plan/SQL files indicating SKIPPED

### Requirement: View chain autofix DDL fallback
The fixup executor SHALL search fixup_scripts/done for missing DDL when building per-view plans.

#### Scenario: DDL missing in fixup_scripts
- **WHEN** a dependency node requires DDL
- **AND** no matching SQL exists in the active fixup_scripts subdirectories
- **THEN** the executor searches fixup_scripts/done for a matching DDL script
- **AND** uses it if found

### Requirement: View chain autofix grant fallback
The fixup executor SHALL generate a targeted object GRANT when required privileges are missing and no matching grant statements exist in grants_miss or grants_all.

#### Scenario: Missing grant statements
- **WHEN** a dependency hop requires a privilege not present in OceanBase
- **AND** no matching GRANT statements are found in grants_miss or grants_all
- **THEN** the executor adds a generated object GRANT to the per-view SQL plan

### Requirement: View chain autofix execution summary
The fixup executor SHALL report per-view status and failure reasons after running view-chain autofix.

#### Scenario: Partial success
- **WHEN** some statements fail but the root view exists after execution
- **THEN** the executor reports the view status as PARTIAL
- **AND** includes the failure reasons in the summary output
