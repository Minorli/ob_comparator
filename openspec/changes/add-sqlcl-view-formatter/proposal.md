# Change: Add SQLcl-based DDL formatter (multi-type, optional)

## Why
DDL cleanup and fixup generation already resolves OceanBase incompatibilities, but output scripts are hard to read when line comments collapse, or when multi-line SQL becomes a single line. SQLcl embeds the SQL Developer formatter and can normalize DDL output across multiple object types. Formatting should be optional and must not change fixup logic (only post-process output for readability).

## What Changes
- Add a global DDL formatting switch and a list of object types to format.
- Use SQLcl (optional) to format final DDL output after all cleanup/remap steps.
- Ensure formatting failures never block fixup generation; fall back to original DDL.
- Add a formatter report that summarizes formatted counts and failures per object type.

## Impact
- Affected specs: configuration-control, generate-fixup, export-reports
- Affected code: schema_diff_reconciler.py (fixup output stage + formatter wrapper)
- External dependency: Oracle SQLcl (optional) + Java runtime
