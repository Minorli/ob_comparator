# Change: Resolve synonym chains for view grants

## Why
Views that reference public or private synonyms (without schema qualification) often fail to create in OceanBase due to missing privileges on the underlying target objects. Granting on the synonym itself is insufficient; the base object needs proper privileges, sometimes with GRANT OPTION for the view owner.

## What Changes
- Resolve synonym chains used by view dependencies to their final target objects.
- Generate required object grants along the chain for view owners and downstream grantees.
- Reuse existing grant allowlist and remap rules when generating statements.

## Impact
- Affected specs: generate-fixup
- Affected code: schema_diff_reconciler.py (dependency resolution + grant generation)
- Docs: grant generation notes
