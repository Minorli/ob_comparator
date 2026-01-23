## 1. Implementation
- [x] 1.1 Move fixup directory cleanup ahead of master_list checks, add per-file error handling, and honor fixup_force_clean for absolute paths.
- [x] 1.2 Add PL/SQL collection attribute range cleanup (FIRST/LAST/COUNT single-dot to double-dot) and wire into PLSQL cleanup rules.
- [x] 1.3 Extend PL/SQL fixup ordering to include TYPE/TYPE BODY and topo-sort PROCEDURE/FUNCTION/TRIGGER when dependency pairs are available.
- [x] 1.4 Update run_fixup dependency layers so TYPE precedes PROCEDURE/FUNCTION in smart order.
- [x] 1.5 Read DEFERRABLE/DEFERRED metadata from OB when available and treat missing target metadata as unknown.
- [x] 1.6 Extend dependency grant status evaluation to non-VIEW objects and clarify GRANT_UNKNOWN behavior for unmapped types.
- [x] 1.7 Remove redundant extra_results call and fix support_summary timing for trigger status filtering.
- [x] 1.8 Match CHECK constraints by expression first while still reporting same-name mismatches.
- [x] 1.9 Relax SYS_NC index normalization to match by column sets even when index names differ.

## 2. Tests
- [x] 2.1 Unit tests for clean_for_loop_collection_attr_range.
- [x] 2.2 Tests for fixup cleanup behavior with master_list empty and fixup_force_clean override.
- [x] 2.3 Tests for PL/SQL ordering (TYPE/TYPE BODY, PROC/FUNC/TRIGGER) using dependency pairs.
- [x] 2.4 Tests for OB deferrable metadata handling and UNKNOWN fallback.
- [x] 2.5 Tests for CHECK constraint expression-first matching with same-name mismatch reporting.
- [x] 2.6 Tests for SYS_NC index column-set matching without name equality.

## 3. Docs
- [x] 3.1 Update config.ini.template and readme_config.txt for fixup_force_clean and ordering/cleanup behavior.
- [ ] 3.2 Update AGENTS.md if any new workflow guardrails are required.
