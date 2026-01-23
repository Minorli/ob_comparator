## 1. Implementation
- [x] Extend constraint metadata loading to prefer SEARCH_CONDITION_VC and handle LONG search_condition safely.
- [x] Compare CHECK constraints by normalized expressions and deferrable flags.
- [x] Add compatibility classification for unsupported CHECK patterns (SYS_CONTEXT in CHECK, DEFERRABLE INITIALLY DEFERRED).
- [x] Exclude unsupported CHECK constraints from fixup generation and mark them as unsupported in summaries.
- [x] Export constraints_unsupported_detail_<timestamp>.txt in the per-run report directory with | delimiter and header.

## 2. Tests
- [x] Unit tests for CHECK constraint expression extraction and normalization.
- [x] Unit tests for unsupported CHECK constraint classification and report output.

## 3. Docs
- [x] Update readme_config.txt to document the new unsupported CHECK constraint report and how to interpret it.
