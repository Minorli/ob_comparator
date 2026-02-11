## ADDED Requirements

### Requirement: Swarm blueprint generation
The toolkit SHALL generate a deterministic expert swarm blueprint for the comparator repository.

#### Scenario: Default blueprint includes critical experts
- **WHEN** a developer runs `python3 expert_swarm.py` in the project root
- **THEN** the tool writes `audit/swarm/swarm_blueprint.json`
- **AND** the blueprint includes architecture, code-review, programming, and database expert roles

### Requirement: Repository guardrails in swarm context
The toolkit SHALL embed comparator-specific guardrails in the generated swarm context.

#### Scenario: Guardrails are present
- **WHEN** the blueprint is generated
- **THEN** it contains configuration-discipline constraints
- **AND** it contains fixup-vs-check separation constraints
- **AND** it contains deterministic remap/dependency constraints

### Requirement: Structured output mapping
The toolkit SHALL define a deterministic output mapping for each expert and the final arbiter.

#### Scenario: Output mapping is explicit
- **WHEN** a blueprint is generated
- **THEN** each expert role includes an output path under `audit/swarm/`
- **AND** the arbiter output path is `audit/swarm/consolidated_report.md`

### Requirement: Parallel subteam topology
The toolkit SHALL encode deterministic subteams that can execute in parallel.

#### Scenario: Subteam definitions are present
- **WHEN** a blueprint is generated
- **THEN** it contains two or more subteam definitions
- **AND** each subteam references explicit member role IDs
- **AND** each subteam has its own summary output path under `audit/swarm/`

### Requirement: Final arbitration role
The toolkit SHALL define an explicit arbiter role to resolve cross-subteam conflicts.

#### Scenario: Arbiter mapping is present
- **WHEN** a blueprint is generated
- **THEN** it contains an arbiter definition
- **AND** the arbiter output path is `audit/swarm/consolidated_report.md`
- **AND** the arbiter instructions require conflict resolution and prioritized decisions

### Requirement: Optional executable swarm mode
The toolkit SHALL provide an optional execution mode for orchestrating expert agents via Agents SDK with Codex MCP using parallel subteams and final arbitration.

#### Scenario: Execute mode is requested
- **WHEN** the user runs `python3 expert_swarm.py --execute`
- **THEN** the tool validates runtime prerequisites
- **AND** the tool starts parallel subteam execution using the generated role definitions
- **AND** the tool runs arbiter synthesis after subteam results are available
