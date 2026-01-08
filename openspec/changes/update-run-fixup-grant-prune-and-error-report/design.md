## Context
run_fixup currently treats a GRANT file as all-or-nothing. A single failing GRANT causes the entire file to remain, leading to repeated long executions and large error logs.

## Goals / Non-Goals
- Goals:
  - Prune successful GRANT statements so each retry handles fewer statements.
  - Keep only failed GRANT statements for subsequent runs.
  - Generate a short, structured error report for production collection and OCR.
  - Default behavior: enabled with no configuration required.
- Non-Goals:
  - Do not change schema_diff_reconciler.py or fixup generation.
  - Do not alter non-GRANT scripts (tables/views/etc.).
  - Do not implement a full SQL parser.

## Decisions
- Decision: Only apply pruning to files under fixup_scripts/grants.
  - Rationale: Grants are safe to retry independently and benefit the most from pruning.
- Decision: Rewrite the original grant file to contain only failed statements.
  - Rationale: Reduces the work in subsequent runs without creating extra files.
- Decision: Emit a concise report file under fixup_scripts/errors capped to a fixed entry limit (default 200).
  - Rationale: Keeps logs short for OCR and avoids oversized files.

## Algorithm
1) Split each grants/*.sql into statements using the existing statement splitter.
2) Execute each GRANT statement independently and record success/failure.
3) If all statements succeed:
   - Move the original file to done/grants.
4) If some statements fail:
   - Rewrite the original file with only failed statements (preserve original order).
5) Collect failures across the run into a report file:
   - File path: fixup_scripts/errors/fixup_errors_<timestamp>.txt
   - Format per line: FILE | STMT_INDEX | ERROR_CODE | OBJECT | MESSAGE
   - Cap the number of lines to 200.

## Risks / Trade-offs
- Rewriting files changes content; ensure atomic write (write temp then replace) to avoid partial files.
- Error parsing may miss object names; fallback to '-' when unavailable.

## Open Questions
- Should we include the GRANT SQL text in the report when object parsing fails?
