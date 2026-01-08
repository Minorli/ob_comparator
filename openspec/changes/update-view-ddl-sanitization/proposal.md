# Change: Sanitize dbcat view DDL output

## Why
Some dbcat-generated VIEW DDL contains malformed tokens (e.g., identifiers split by stray spaces) or inline comments that collapse multiple select-list lines into one, causing syntax errors. We need deterministic cleanup rules that preserve valid SQL while fixing these cases.

## What Changes
- Normalize split identifiers when they can be safely rejoined using metadata.
- Restore line boundaries after inline "--" comments in SELECT lists to prevent syntax errors.
- Preserve WITH CHECK OPTION only when OceanBase version >= 4.2.5.7.
- Keep string literals and comment bodies unchanged.

## Impact
- Affected specs: generate-fixup
- Affected code: schema_diff_reconciler.py (view DDL cleanup)
- Docs: view DDL cleanup notes
