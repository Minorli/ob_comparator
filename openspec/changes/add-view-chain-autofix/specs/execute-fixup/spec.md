## ADDED Requirements

### Requirement: View chain guided autofix mode
The fixup executor SHALL support a view-chain-guided autofix mode that generates per-view repair plans and SQL files, then executes them using only fixup scripts and targeted GRANT statements.

#### Scenario: Mode enabled with chain file
- **WHEN** --view-chain-autofix is enabled
- **AND** report_dir contains VIEWs_chain_*.txt
- **THEN** the executor selects the latest chain file (by mtime)
- **AND** writes per-view plan files under fixup_scripts/view_chain_plans/
- **AND** writes per-view SQL files under fixup_scripts/view_chain_sql/
- **AND** executes statements in the generated SQL in dependency order

#### Scenario: Missing chain file
- **WHEN** --view-chain-autofix is enabled
- **AND** no VIEWs_chain_*.txt exists in report_dir
- **THEN** the executor exits with an error and does not execute scripts

#### Scenario: Grant lookup priority
- **WHEN** a dependency hop requires a privilege that is missing in OceanBase
- **THEN** the executor searches grants_miss for matching GRANT statements
- **AND** if not found, searches grants_all
- **AND** executes only the matched statements, not the full grant files

#### Scenario: Missing DDL or cyclic chain
- **WHEN** a dependency hop has no fixup DDL or the chain is cyclic
- **THEN** the executor marks the view as blocked and skips auto-execution for that view
- **AND** still writes the plan and SQL files for manual review
