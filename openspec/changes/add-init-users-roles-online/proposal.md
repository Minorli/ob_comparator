# Change: Online initialization of users, roles, and non-object grants

## Why
Large-scale migrations need a repeatable, online initializer to create missing users/roles and apply role/system privileges before OMS and fixup stages.

## What Changes
- Add online initialization behavior to `init_users_roles.py` to create users/roles and apply role grants and system privileges from Oracle.
- Persist generated SQL files locally for audit and manual review.
- Use `config.ini` by default for Oracle/OB connections and timeouts.

## Impact
- Affected specs: init-users-roles (new)
- Affected code: init_users_roles.py, docs/MIGRATION_PROPOSAL_1567_SCHEMAS.md
