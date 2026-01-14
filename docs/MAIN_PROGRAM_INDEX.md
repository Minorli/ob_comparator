# Main Program Index

This doc is a lightweight map of the primary entrypoints and the main execution
phases. It is meant to avoid re-reading large files during iteration.

## Entrypoints

- `schema_diff_reconciler.py`: main compare and fixup generation flow.
- `run_fixup.py`: executes generated fixup scripts (single, iterative, or view-chain modes).

## schema_diff_reconciler.py Index

### Core sections (top to bottom)

- Logging and console helpers: `strip_ansi_text`, `init_console_logging`, `log_section`.
- Run summary types: `RunSummary`, `RunSummaryContext`.
- Remap and dependency utilities: `load_remap_rules`, `build_dependency_graph`,
  `resolve_remap_target`, `build_full_object_mapping`, `generate_master_list`.
- Metadata dump: `dump_ob_metadata`, `dump_oracle_metadata`.
- Comparison logic: `check_primary_objects`, `compare_package_objects`,
  `check_extra_objects`, `check_comments`.
- Grant planning: `build_grant_plan`, `filter_missing_grant_entries`.
- DDL cleanup and fixup: `normalize_ddl_for_ob`, `generate_fixup_scripts`.
- Reporting: `export_*` helpers, `print_final_report`, `log_run_summary`.
- CLI and entrypoint: `parse_cli_args`, `main`.

### Main progress map (main)

1. Parse CLI args; optional `--wizard` config flow.
2. Load config, setup logging, validate paths, init Oracle client.
3. Load remap rules and source objects; validate remap rules.
4. Load dependencies and synonym metadata (if needed).
5. Build mapping and master list; infer schema mapping if enabled.
6. Compute target schemas and expected dependency pairs.
7. Dump OceanBase metadata; optionally load OB dependencies.
8. Dump Oracle metadata; build blacklist report rows.
9. Compare primary objects; compare packages; check comments.
10. Compare extra objects (index, constraint, sequence, trigger).
11. Process trigger list report (optional).
12. Check dependencies and export dependency chains (optional).
13. Generate fixup scripts and grant plan (optional).
14. Render final report and run summary.

### Short-circuit / gating conditions

- Empty master list short-circuits the heavy phases and produces a minimal report.
- `generate_fixup=false` skips fixup generation (and grant scripts).
- `check_dependencies=false` skips dependency analysis and chain export.
- `check_comments=false` skips comment comparison and related metadata.
- `check_extra_types` empty skips extra object checks.
- `trigger_list` file controls trigger filtering and reporting.
- `enable_schema_mapping_infer` controls dependency-driven schema inference.

## run_fixup.py Progress Map

### Main modes

- `run_single_fixup`: one pass execution with optional `--smart-order`.
- `run_iterative_fixup`: multi-round retries with progress tracking.
- `run_view_chain_autofix`: uses view chain plans and SQL output.

### Flow (main)

1. Parse args and map object types to directories.
2. Load OB config, fixup directory, report directory.
3. Select mode: view-chain, iterative, or single.
4. Collect SQL files and execute with status summary.
5. Optionally recompile invalid objects and produce error report.

## Files to jump to for debugging

- `schema_diff_reconciler.py`: `main`, `generate_fixup_scripts`, `print_final_report`.
- `run_fixup.py`: `main`, `run_single_fixup`, `run_iterative_fixup`.
