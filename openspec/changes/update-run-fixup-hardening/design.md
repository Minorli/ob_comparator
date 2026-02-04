## Context
`run_fixup.py` processes many SQL scripts and retries failures. Current behavior includes unsafe moves, limited error handling, and ambiguous view-chain output when cycles exist. Password handling is intentionally out of scope.

## Goals / Non-Goals
- Goals:
  - Fail fast and clearly on missing obclient or OS-level failures.
  - Avoid overwriting prior done/ artifacts.
  - Accurate iterative failure statistics.
  - Guard against extreme file sizes.
  - Clearer auto-grant and view-chain behavior.
  - Bounded caches for grant discovery.
- Non-Goals:
  - No change to password passing (keep CLI mode).

## Decisions
- Add a safe move strategy that preserves existing done/ files by renaming or timestamp suffix.
- Record cumulative failures using a set of unique scripts.
- Introduce optional `fixup_dir_allow_outside_repo` to permit current behavior while allowing stricter environments.
- Introduce `fixup_max_sql_file_mb` to skip oversized scripts gracefully.
- Improve view-chain cycle handling by blocking SQL generation when cycles exist.
- Expand SQL error classification to improve retry/summary grouping.
- Add AutoGrant cache limit (e.g., 10k) with simple eviction or LRU.

## Risks / Trade-offs
- Stricter path/size checks may skip scripts in edge cases; provide clear log output.
- Cache eviction could cause extra query calls; limit size conservatively.

## Migration Plan
- Add new settings with defaults aligned to current behavior (except new size limit).
- Update docs/readme_config for new switches and behaviors.

## Open Questions
- Exact cache eviction policy (simple FIFO vs LRU).
