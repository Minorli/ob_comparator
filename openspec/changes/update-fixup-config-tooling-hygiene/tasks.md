## 1. Implementation
- [x] 1.1 Fix iterative `cumulative_failed` accounting in `run_fixup.py` and update the summary output to reflect total failures across rounds.
- [x] 1.2 Add/extend unit tests for cumulative failure counting and iterative summary output.
- [x] 1.3 Remove duplicate `ddl_punct_sanitize`/`ddl_hint_*` blocks from `config.ini.template` and keep key order stable.
- [x] 1.4 Add a lint/test that detects duplicate keys in `config.ini.template` to prevent regressions.
- [x] 1.5 Refactor `collect_source_object_stats.py` to centralize INDEX/CONSTRAINT/TRIGGER SQL templates and remove unused imports.
- [x] 1.6 Add tests verifying brief vs full stats outputs rely on shared templates (consistent counts).
- [x] 1.7 Standardize exception handling patterns in helper scripts (log context, avoid silent `except`) without changing runtime behavior.
- [x] 1.8 Add regression tests for key exception-handling paths (e.g., safe preview/first-line handling).

## 2. Validation
- [x] 2.1 Run `python3 -m py_compile $(git ls-files '*.py')`.
- [x] 2.2 Run targeted unit tests for run_fixup and stats helper.
