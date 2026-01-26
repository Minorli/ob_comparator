# Change: Improve report index + auto console log level

## Why
Report outputs are difficult to navigate for first‑time users and verbose console logs overwhelm non‑interactive runs. We need a simple navigation index and a safer default log level without removing any report content.

## What Changes
- Add a per‑run report index file that lists all report/detail outputs with path, type, short description, and row counts when available. Existing detail files remain unchanged.
- Add a concise “执行结论” block and enforce sequential section numbering with unified terminology, while preserving all existing report information.
- Update `log_level` default to `auto`, selecting INFO when running in a TTY and WARNING otherwise. Explicit values remain honored.

## Impact
- Affected specs: `export-reports`, `configuration-control`
- Affected code: `schema_diff_reconciler.py` (report rendering + index export + console level selection), `config.ini.template`, `readme_config.txt`
- No changes to comparison logic, fixup selection, or DDL generation
