## ADDED Requirements
### Requirement: Sequence restart execution gating
The fixup executor SHALL exclude sequence_restart scripts by default and execute them only when explicitly requested.

#### Scenario: Default run
- **WHEN** run_fixup executes without sequence-restart options
- **THEN** scripts under fixup_scripts/sequence_restart are skipped

#### Scenario: Explicit include
- **WHEN** run_fixup is invoked with an explicit include (e.g., --include-sequence-restart or --only-dirs sequence_restart)
- **THEN** scripts under fixup_scripts/sequence_restart are executed
