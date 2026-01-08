# Change: Ensure view owner has grantable privileges

## Why
Granting a VIEW to other users can fail with ORA/OB-01720 when the view owner does not have grantable privileges on the underlying objects. The fix should be automatic and targeted, not blanket WITH GRANT OPTION on all grants.

## What Changes
- Detect when a view is granted to other grantees.
- Ensure the view owner has the required object privileges WITH GRANT OPTION on the view's dependencies.
- Generate only the missing grantable statements.

## Impact
- Affected specs: generate-fixup
- Affected code: schema_diff_reconciler.py (grant generation)
- Docs: grant generation notes
