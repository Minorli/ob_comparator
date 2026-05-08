# Change: Preserve external PUBLIC sequence synonym targets in trigger DDL

## Why
Customer trigger bodies can call sequences through synonyms, for example
`seq_syn.NEXTVAL` or `schema.seq_syn.NEXTVAL`.  The existing trigger DDL
rewrite correctly resolves mapped sequences, but one production edge case was
still unsafe: a PUBLIC synonym can point to a real sequence outside the managed
source schema set.  The PUBLIC synonym was pruned from synonym metadata before
trigger rewrite, or the resolved terminal sequence had no mapping, so the final
fallback used the trigger schema plus synonym name.  That can compile against a
wrong object if such an object exists in the target.

OceanBase source metadata also stores PUBLIC synonyms as `__public` on some
versions.  The same helper metadata path must treat that owner as PUBLIC.

## What Changes
1. Trigger DDL sequence rewrite keeps helper metadata for PUBLIC synonyms whose
   terminal object is a local SEQUENCE, even when the terminal owner is outside
   the managed source schema list.
2. If a synonym terminal sequence is resolved but has no object mapping and no
   explicit remap rule, trigger rewrite falls back to the real terminal sequence
   full name rather than the original synonym reference.
3. OceanBase source synonym metadata loading treats `OWNER='__public'` as
   PUBLIC when reading PUBLIC synonym rows.
4. The helper metadata is scoped to trigger DDL reference resolution only.  It
   does not expand source object compare/fixup scope or synonym fixup scope.

## Impact
- Affected specs:
  - `generate-fixup`
- Affected code:
  - `schema_diff_reconciler.py`
  - `test_schema_diff_reconciler.py` local regression coverage
- Affected docs/release:
  - `README.md`
  - `docs/CHANGELOG.md`
  - `readme_config.txt`
  - `readme_lite.txt`
  - version metadata in release-facing docs/scripts
