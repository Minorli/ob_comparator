## Context
These changes tighten correctness and maintainability without altering core comparison/fixup behavior. The work spans fixup execution reporting, configuration template hygiene, and a stats helper tool.

## Goals / Non-Goals
- Goals:
  - Accurate cumulative failure reporting across iterative fixup rounds.
  - Remove duplicated config template keys and prevent reintroduction.
  - Keep stats helper SQL definitions centralized to avoid drift.
  - Standardize exception handling to improve observability.
- Non-Goals:
  - No changes to comparison algorithms or fixup generation logic.
  - No functional changes to config semantics (template cleanup only).

## Decisions
- **Cumulative failures**: Track total failures across rounds and report the total in the final summary (retain per-round counts in logs).
- **Template duplication**: Remove the redundant `ddl_*` block and add a lightweight duplicate-key guard test.
- **Stats SQL templates**: Define INDEX/CONSTRAINT/TRIGGER SQL templates once at module level and reuse in both brief and full report paths.
- **Exception handling**: Replace silent `except` blocks with logging helpers that include context, while preserving existing control flow.

## Risks / Trade-offs
- More logging may increase verbosity; keep log level at WARN/DEBUG as appropriate.
- Strict duplicate-key checking could block unusual template edits; mitigate by targeting only `config.ini.template` in tests.

## Migration Plan
- No data migrations. Template cleanup is non-breaking.
- Rollback is simply restoring prior template content and cumulative failure logic.

## Open Questions
- Should the duplicate-key guard be a unit test or a lightweight pre-commit script?
- Which helper scripts should be included in the first pass for exception-handling normalization?
