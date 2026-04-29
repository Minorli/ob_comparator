## 1. Scope and Blocking Decisions

- [x] 1.1 Freeze this change scope to 0.9.x reliability and feature hardening, explicitly excluding 1.0 core refactor and 1.1 UI work.
- [x] 1.2 Record resolved implementation decisions in docs: standalone `diagnostic_bundle.py`, real-DB smoke without tracked schema fixtures, SQL filename/hash but no SQL content by default, report_db count/status summaries only, and external `compatibility_registry.json`.
- [x] 1.3 Define the release evidence file schema with version, commit, tag, commands, reports, real DB smoke result, skipped checks, and approver notes.
- [x] 1.4 Define local-only verification locations such as `test_scenarios/reliability/` or `/tmp/comparator_reliability_tests`, and document that these fixtures are not tracked.

## 2. Phase A: Release Gate, Heartbeat, and Timeout Policy

- [x] 2.1 Add a release gate script or documented command path that refuses release packaging when required evidence is missing.
- [x] 2.2 Document rollback and hotfix policy for failed production releases, including tag handling and customer-facing version guidance.
- [x] 2.3 Update release docs so every formal release requires Oracle->OceanBase real DB smoke evidence before tag/release publication.
- [x] 2.4 Design a shared Operation Tracker contract for phase, operation_id, object identity, current/total, elapsed time, last success, artifact path, and heartbeat state file.
- [x] 2.5 Implement main-program heartbeat output for metadata dump, compare, fixup generation, report export, and long object loops.
- [x] 2.6 Implement `run_fixup.py` heartbeat output for file execution, statement execution mode, retries, timeout waits, and last successful file or statement.
- [x] 2.7 Make file-mode `run_fixup.py` heartbeat explicitly file-level/process-level only when SQL is passed wholesale to obclient.
- [x] 2.8 Emit an effective timeout table at run start and in the summary report for `cli_timeout`, `obclient_timeout`, `fixup_cli_timeout`, and `ob_session_query_timeout_us`.
- [x] 2.9 Add validation warnings for contradictory timeout values that can make the process appear hung.
- [x] 2.10 Verify Phase A with local-only heartbeat fixtures, simulated slow phase logs, timeout summary snapshots, and one real Oracle->OceanBase smoke evidence entry.

## 3. Phase B: Difference Explanation and Fixup Safety Tiers

- [x] 3.1 Define stable reason codes, rule IDs, evidence fields, decision values, and action values for compare and fixup decisions.
- [x] 3.2 Keep responsibility boundaries explicit: compare builds in-memory reason records, generate-fixup enriches them with SQL/tier metadata, and export-reports only serializes them.
- [x] 3.3 Attach structured reason records to table column, type, length, char semantics, nullability, default, check, grant, view, trigger, and dependency differences.
- [x] 3.4 Add VARCHAR/VARCHAR2 acceptance cases for Oracle BYTE semantics, including Oracle `VARCHAR2(100 BYTE)` vs OB `VARCHAR2(150)` or `VARCHAR(150)` producing no mismatch and no fixup.
- [x] 3.5 Add CHAR semantics cases where `CHAR_USED='C'` requires exact character length handling and does not use the BYTE expansion window.
- [x] 3.6 Ensure type literal spelling differences (`VARCHAR` vs `VARCHAR2`) do not generate ALTER statements when semantic type and length rules match.
- [x] 3.7 Define the `safe`, `review`, `destructive`, and `manual` tier contract with a whitelist-only `safe` tier.
- [x] 3.8 Classify first-wave safe operations as existing-object compile operations only; classify table shape changes, grants, comments, synonyms, object create/replace, and sequence restart as `review` or stricter.
- [x] 3.9 Classify drop/truncate/drop-column/cleanup/disable/revoke paths as `destructive`, and unsupported/degraded/semi-auto-only families as `manual`.
- [x] 3.10 Write `fixup_plan_<timestamp>.jsonl` with SQL file, statement identity, object identity, reason record, compatibility decision, safety tier, and execution default.
- [x] 3.11 Teach `run_fixup.py` to select execution by tier and fail closed when destructive/manual SQL is requested without explicit confirmation.
- [x] 3.12 Verify Phase B with local-only fixtures for reason records, VARCHAR window acceptance, CHAR semantics exactness, tier classification, fixup plan export, and execution filtering.

## 4. Phase C: Compatibility Matrix and Recovery

- [x] 4.1 Create a shipped `compatibility_registry.json` for source mode, OB version, object family, operation, decision, rationale, and manual-action hint.
- [x] 4.2 Add config/default loading for the compatibility registry with an override path and clear validation errors for malformed registry files.
- [x] 4.3 Export `compatibility_matrix_<timestamp>.json` and a human-readable compatibility summary in each run report directory.
- [x] 4.4 Define checkpoint files with run_id, code version, `decision_config_hash`, `runtime_config_hash`, input artifact hash, phase state, object cursor, and output paths.
- [x] 4.5 Implement stage-level recovery for metadata dump, compare, fixup generation, and report export.
- [x] 4.6 Implement object-level replay for high-value TABLE, VIEW, GRANT, and fixup generation paths.
- [x] 4.7 Extend `run_fixup.py` ledger to support file-level and statement-level resume with clear skipped/succeeded/failed counts.
- [x] 4.8 Allow resume across harmless runtime/display config changes while recording changed runtime keys in the recovery manifest.
- [x] 4.9 Refuse resume by default when decision config, code version, or input artifact hash differs; support `--force-resume --resume-override-reason` with explicit audit output.
- [x] 4.10 Verify Phase C with local-only fixtures for registry decisions, matrix export, successful resume, harmless config resume, and unsafe resume rejection.

## 5. Phase D: Customer Diagnostic Package

- [x] 5.1 Implement standalone `diagnostic_bundle.py` CLI with `--run-dir`, `--config`, `--output`, `--pid`, `--hang`, `--include-sql-content`, `--redact-identifiers`, `--max-file-mb`, and `--max-bundle-mb`.
- [x] 5.2 Add report/log output that prints the recommended `diagnostic_bundle.py` command for completed, failed, and hang-triage situations.
- [x] 5.3 Finalize the diagnostic package data contract: `manifest.json`, `summary.txt`, `config_sanitized.ini`, `run_state.json`, and `artifacts/`.
- [x] 5.4 Implement post-run bundle generation from completed or failed `main_reports`, `fixup_scripts`, logs, compatibility matrix, fixup plan, recovery manifest, and report indexes.
- [x] 5.5 Implement strict redaction for passwords, DSNs with credentials, token/secret/private-key patterns, temporary client credential files, and environment-derived secrets.
- [x] 5.6 Include SQL file names, paths, sizes, hashes, object identities, and summaries by default, but exclude full DDL and fixup SQL unless `--include-sql-content` is explicitly set.
- [x] 5.7 Add optional identifier hashing with a local hash map when `diagnostic_redact_identifiers=true` or `--redact-identifiers` is used.
- [x] 5.8 Implement hang bundle collection from heartbeat state, process snapshot, log tail, current object/file, elapsed time, and timeout table without waiting for normal program exit.
- [x] 5.9 Add package self-checks that report missing evidence, stale heartbeat, redaction mode, file hashes, and next recommended support command.
- [x] 5.10 Add optional report_db diagnostic summaries that export count consistency and high-level status from existing tables, not full report_db table dumps or new diagnostic views.
- [x] 5.11 Verify Phase D with local-only fixtures for CLI help, missing run-dir failure, redaction, SQL-content opt-in, manifest hashes, missing-evidence warnings, and hang snapshot content.

## 6. Report and Documentation Updates

- [x] 6.1 Update main summary, split detail files, and report_db outputs to carry explanation, compatibility, safety tier, heartbeat, and recovery references consistently.
- [x] 6.2 Verify count consistency across main summary, split detail files, and report_db when new explanation or safety-tier fields are enabled.
- [x] 6.3 Update `config.ini.template.txt`, `readme_config.txt`, wizard prompts, and validation defaults for all new switches.
- [x] 6.4 Update README, lite docs, changelog template, and operator runbooks with release evidence, timeout interpretation, fixup tiers, recovery, and diagnostic package usage.
- [x] 6.5 Ensure diagnostic package and safety-tier artifacts are documented as support evidence, not as a replacement for reviewing SQL before execution.

## 7. Validation and Rollout

- [x] 7.1 Run `python3 -m py_compile $(git ls-files '*.py')` after implementation changes touching Python files.
- [x] 7.2 Run local-only tests or the approved ignored verification harness for explanation, tiering, recovery, timeout, heartbeat, and diagnostic package paths.
- [x] 7.3 Run Oracle->OceanBase real DB smoke for the main program and record report path, command, duration, DB versions, and outcome in release evidence.
- [x] 7.4 Run a non-mutating `run_fixup.py` dry-run or plan validation path against generated fixup output and record safety-tier filtering evidence.
- [x] 7.5 Validate that no tracked test fixtures, ad hoc scripts, credentials, or environment-specific artifacts are added to the release branch.
- [x] 7.6 Evaluate whether this roadmap must be split into implementation child changes; keep this delivery as one review packet because the user requested the full proposal before external review and the artifacts now cross-reference one run/diagnostic contract.
