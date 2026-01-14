## 1. Implementation
- [x] 1.1 Update `init_users_roles.py` to fetch non-system users/roles, role grants, and system privileges from Oracle.
- [x] 1.2 Implement target-side existence checks for users, roles, role grants, and system privileges; skip already-applied items.
- [x] 1.3 Emit local SQL files for create/grant statements under `fixup_scripts/init_users_roles`.
- [x] 1.4 Add version/header metadata and logging consistent with the main program.

## 2. Documentation
- [x] 2.1 Update `docs/MIGRATION_PROPOSAL_1567_SCHEMAS.md` to describe role/sys-priv initialization.

## 3. Tests
- [x] 3.1 Add or update lightweight tests for SQL generation and filtering logic (if applicable).
