## MODIFIED Requirements

### Requirement: Iterative retry mode
The fixup executor SHALL support iterative retry rounds when --iterative is enabled and report cumulative failure counts across all rounds.

#### Scenario: Iterative mode enabled
- **WHEN** --iterative is set
- **THEN** the executor repeats execution rounds until progress stops or max rounds is reached

#### Scenario: Cumulative failure summary
- **WHEN** iterative rounds execute with failures in multiple rounds
- **THEN** the final summary reports the total failures across all rounds (not only the last round)
