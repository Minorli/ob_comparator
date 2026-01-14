## Context
The extra object check stage iterates every TABLE in master_list (~20k tables) and rebuilds per-table maps for indexes/constraints/triggers on each iteration. With ~26k indexes, ~13k constraints, and ~14k triggers, this produces long CPU-bound loops and sparse progress logs, which looks like a stall in production.

## Goals / Non-Goals
- Goals:
  - Reduce extra check runtime without changing mismatch semantics.
  - Improve progress visibility during long runs.
  - Allow controlled parallelism for large datasets.
- Non-Goals:
  - Changing object scope or comparison rules.
  - Altering report format (except deterministic ordering when parallelized).
  - Introducing new external dependencies.

## Decisions
- Add precomputed signatures:
  - IndexSignature: normalized column tuple -> {names, uniq}, plus SYS_NC normalization helper.
  - ConstraintSignature: PK/UK column sets, FK column sets with mapped references, and partition-aware PK downgrade flags.
  - TriggerSignature: mapped trigger full names with event/status pairs.
- Build signatures once per table after metadata load (or during load) and store in new cache fields.
- Compare via signatures first; if equal, mark OK and skip detailed diff. If not equal, run existing diff logic using cached structures.
- Add extra_check_workers and extra_check_chunk_size for optional parallel execution (default workers = min(4, CPU)). Use chunked work items (per-table bundles) to avoid pickling full metadata. Aggregate results and sort by target table for deterministic output.
- Add extra_check_progress_interval (seconds) to drive time-based progress logging. Log per-type elapsed time and throughput.

## Risks / Trade-offs
- Additional memory for signatures; mitigate by storing tuples/sets and reusing normalized values.
- Parallelization overhead; default to min(4, CPU) and tune chunk size for 100-500 tables.
- Deterministic ordering may differ from previous run order; document and sort consistently.

## Migration Plan
- Extend OracleMetadata/ObMetadata with signature caches (or build a separate ExtraCheckCache struct).
- Update extra check path to consume cached signatures and fast-path equality.
- Add config parsing, defaults, and validation for extra_check_*.
- Update logs and docs; validate with representative large schema runs.

## Open Questions
- Should signature equality rely on hashing to reduce memory, or full structure comparison for clarity?
