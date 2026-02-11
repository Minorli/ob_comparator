## Context
The comparator repository has broad concerns in one place: metadata extraction, cross-dialect normalization, report generation, and fixup execution. Manual review often misses cross-domain risks because architecture, coding, and database perspectives are not coordinated.

## Goals / Non-Goals
- Goals:
  - Define an explicit expert swarm with clear role boundaries.
  - Enable parallel subteam execution for faster end-to-end analysis.
  - Add a final arbiter role to resolve conflicts across subteam conclusions.
  - Make swarm setup deterministic and repository-aware.
  - Keep runtime execution optional and safe by default.
- Non-Goals:
  - No behavioral changes to `schema_diff_reconciler.py` or `run_fixup.py`.
  - No new core configuration keys in `config.ini.template`.

## Decisions
- Decision: Introduce a standalone `expert_swarm.py` entry point.
  - Rationale: Keeps swarm capability decoupled from core compare/fixup flow.
- Decision: Default action remains blueprint generation, not agent execution.
  - Rationale: Avoid accidental network/tool runtime dependency during local development.
- Decision: Execute mode uses two-level orchestration.
  - Rationale: Experts run inside parallel subteams, then arbiter performs final conflict resolution and prioritization.
- Decision: Use fixed role-to-artifact mapping in `audit/swarm/`.
  - Rationale: Enables repeatable review trails and diff-friendly output.
- Decision: Keep Agents SDK imports lazy.
  - Rationale: Blueprint and tests run without requiring optional SDK packages.

## Risks / Trade-offs
- Risk: Optional execute mode depends on external packages and API key.
  - Mitigation: Fail with explicit installation and environment guidance.
- Risk: Parallel execution can increase runtime resource pressure.
  - Mitigation: Keep subteam count bounded and use deterministic team definitions.
- Risk: Expert instructions may drift from repository constraints.
  - Mitigation: Encode comparator-specific guardrails in blueprint and tests.

## Migration Plan
1. Add OpenSpec change and deltas.
2. Add `expert_swarm.py` and tests.
3. Add usage docs and README entry.
4. Validate OpenSpec and run project checks.

## Open Questions
- Whether to later add CI automation that runs blueprint generation on every release tag.
