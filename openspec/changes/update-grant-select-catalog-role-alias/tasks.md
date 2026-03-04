## 1. Implementation
- [x] 1.1 Add built-in grant role alias mapping for SELECT_CATALOG_ROLE -> OB_CATALOG_ROLE
- [x] 1.2 Apply alias in role grants and grantee mapping paths
- [x] 1.3 Add unit test coverage for role grant + object grant alias behavior
- [x] 1.4 Update docs (`config.ini.template.txt`, `readme_config.txt`)

## 2. Verification
- [x] 2.1 `python3 -m py_compile $(git ls-files '*.py')`
- [x] 2.2 `python3 -m unittest -v test_schema_diff_reconciler.py`
- [x] 2.3 Real DB verification: query Oracle `DBA_ROLE_PRIVS`/`DBA_TAB_PRIVS` samples and confirm `build_grant_plan` outputs `OB_CATALOG_ROLE` as role/grantee alias
