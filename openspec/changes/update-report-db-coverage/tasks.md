## 1. Spec & Design
- [x] 1.1 Update export-reports spec with DB coverage expansion requirements
- [x] 1.2 Update configuration-control spec for report_db_store_scope

## 2. Data Model
- [x] 2.1 Define new diff_ tables (DDL + indexes)
- [x] 2.2 Implement auto-create / safe alter logic

## 3. Write Pipeline
- [x] 3.1 Add artifact catalog generation
- [x] 3.2 Persist dependency edges and view chains
- [x] 3.3 Persist remap conflicts + object mapping
- [x] 3.4 Persist blacklist tables + LONG conversion status
- [x] 3.5 Persist fixup skip summary and OMS missing mapping

## 4. Docs & Queries
- [x] 4.1 Update HOW_TO_READ_REPORTS_IN_OB_67_sqls.txt with SQL playbook
- [x] 4.2 Update readme_config.txt and docs/ADVANCED_USAGE.md

## 5. Tests
- [x] 5.1 Add unit tests for DB payload generators
- [ ] 5.2 Add integration smoke test with report_to_db=true
