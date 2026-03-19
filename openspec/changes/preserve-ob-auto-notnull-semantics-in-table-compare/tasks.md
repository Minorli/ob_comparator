## 1. Implementation

- [ ] 1.1 Preserve OB auto-generated NOT NULL checks for table semantic suppress.
- [ ] 1.2 Keep ordinary constraint diff noise suppression unchanged.
- [ ] 1.3 Add focused unit coverage for metadata-load + table-compare path.

## 2. Verification

- [ ] 2.1 Run `python3 -m py_compile $(git ls-files '*.py')`
- [ ] 2.2 Run relevant unit tests
- [ ] 2.3 Run Oracle + OceanBase real-DB replay proving no redundant `table_alter` DDL is generated when target already has equivalent OB auto check semantics.
