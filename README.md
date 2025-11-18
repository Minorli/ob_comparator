# OceanBase Comparator Toolkit

This repository contains a complete toolkit for validating Oracle→OceanBase migrations in Oracle-compatible tenants. The main comparator performs a one-time metadata dump from both sides, analyzes differences for every major database object type, and generates fix-up scripts that can be reviewed and executed later.

## Key Components

| Path | Description |
| --- | --- |
| `db_comparator_fixup_release.py` | Primary tool. Dumps metadata from Oracle/OB, compares TABLE/VIEW/INDEX/CONSTRAINT/SEQUENCE/TRIGGER/PROCEDURE/FUNCTION/PACKAGE/PACKAGE BODY/SYNONYM objects, and writes remediation SQL into `fix_up/`. |
| `final_fix.py` | Utility that reads OceanBase credentials from `db.ini` and executes every generated script under `fix_up/`, reporting successes/failures in a table. |
| `db.ini` | Sample configuration for Oracle and OceanBase connections plus comparator settings (source schemas, remap file, timeout, fix-up directory). |
| `remap_rules.txt` | Example object-level remap definitions. Each `SCHEMA.OBJECT = TARGET_SCHEMA.TARGET_OBJECT` line tells the comparator how the object moved during migration. |
| `fix_up/` | Output tree produced by the comparator. Includes subfolders for tables, indexes, constraints, packages, etc. Files can be edited manually before execution. |
| `test_case.txt` | Step-by-step instructions for constructing a simple validation environment. |
| `test_scenarios/spiderweb_case/` | Advanced stress scenario (Oracle & OceanBase DDL plus remap file) that exercises every comparator feature. |
| `requirements.txt` | Lists runtime dependencies (`oracledb`, `rich`). |
| `DESIGN.md` | Additional architecture/notes (if present). |

## Prerequisites

1. Python 3.7+ on Linux.
2. Oracle client connectivity (for the `oracledb` driver) and network access to both Oracle and OceanBase.
3. The OceanBase tenant must be running in Oracle compatibility mode with `obclient` installed.
4. Install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Configuration

1. **Edit `db.ini`**
   - `[ORACLE_SOURCE]`: Oracle username/password/DSN.
   - `[OCEANBASE_TARGET]`: `obclient` path, host, port, `-u` string, password.
   - `[SETTINGS]`: `source_schemas` (comma-separated, supports line breaks), `remap_file`, optional `cli_timeout`, and `fixup_dir`.
2. **Adjust remap rules**
   - Update `remap_rules.txt` (or point `remap_file` to another file) so every migrated object that changed schema or name is explicitly mapped.
3. **(Optional) Test Scenarios**
   - Use `test_case.txt` for a smaller example or `test_scenarios/spiderweb_case` for a comprehensive cross-schema stress test.

## Running the Comparator

```bash
python3 db_comparator_fixup_release.py
```

What it does:
1. Connects to Oracle and collects all TABLE/VIEW/PROCEDURE/FUNCTION/PACKAGE/PACKAGE BODY/SYNONYM metadata for the configured schemas.
2. Validates the remap rules to ensure each source object exists.
3. Performs a single metadata dump from OceanBase (ALL_OBJECTS, ALL_TAB_COLUMNS, indexes, constraints, sequences, triggers) and loads it into memory for comparison.
4. Generates:
   - Console report (using `rich`) summarizing missing/mismatched/OK objects across all categories, including detailed sections for indexes, constraints, sequences, and triggers.
   - `fix_up/` scripts:
        * CREATE statements for missing objects (tables, views, PL/SQL, synonyms, sequences, triggers, indexes, constraints).
        * ALTER scripts for column gaps, including commented DROP suggestions for extra columns.
        * Directory structure mirrors object types so you can review each script manually.

> Tip: re-run after applying fixes; previously generated files stay in `fix_up/` so clean up as needed between iterations.

## Applying Fix Scripts Automatically

Once the scripts look correct, you can push them to OceanBase with `final_fix.py`:

```bash
python3 final_fix.py [optional/path/to/db.ini]
```

Behavior:
1. Reads OceanBase settings and `fixup_dir` from `db.ini`.
2. Discovers every `*.sql` file under the first level of `fix_up/`.
3. Executes each file via `obclient`, continuing even if a script fails.
4. Prints a clean summary table listing every script and its result (success, skipped, or the error returned by `obclient`).

## Offline Package (optional workflow)

If you are working from the offline tarball referenced in older documentation:

```bash
tar -zxvf pa_comparator_offline_pkg.tar.gz
cd pa_comparator
python3.7 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install --no-index --find-links=wheelhouse oracledb cryptography
python3 db_comparator_fixup_release.py
```

## Support Files

- `remap_rules_old.txt`: legacy remap example (kept for reference).
- `db_comparator_fixup_*.py`: historical versions that may help troubleshoot prior releases.

## Getting Started Checklist

1. Clone or unpack the repository.
2. Set up the Python virtual environment and install `requirements.txt`.
3. Edit `db.ini` and `remap_rules.txt` to match your migration plan.
4. (Optional) Build test schemas per `test_scenarios/spiderweb_case`.
5. Run `python3 db_comparator_fixup_release.py` and inspect the console report and `fix_up/` outputs.
6. Apply scripts manually or via `python3 final_fix.py`.
7. Re-run the comparator to confirm all objects are synchronized.

This toolkit is intentionally modular—feel free to customize the remap rules, extend the test scenarios, or integrate the comparator into broader migration pipelines. For questions or design context, review `DESIGN.md` or the heavily commented sections in `db_comparator_fixup_release.py`.
