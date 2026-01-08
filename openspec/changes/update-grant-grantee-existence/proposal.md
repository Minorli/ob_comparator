# Change: Filter grant targets by existing users and roles

## Why
Grant scripts can fail when the target grantee (user or role) does not exist in OceanBase. These failures slow fixups and create noise. We need to filter grants to only existing users/roles and explicitly warn when grants are skipped due to missing principals.

## What Changes
- Load existing OceanBase usernames and roles during grant generation.
- Skip GRANT statements whose grantee is missing from users and roles (PUBLIC always allowed).
- Emit warning summaries listing missing grantees so users can create them before re-running.

## Impact
- Affected specs: generate-fixup
- Affected code: schema_diff_reconciler.py (grant generation)
- Docs: grant generation notes
