## 1. Spec & Design
- [x] 1.1 Update export-reports spec with detail_item requirements
- [x] 1.2 Update configuration-control spec with new switches

## 2. Data Model
- [x] 2.1 Add DIFF_REPORT_DETAIL_ITEM DDL + indexes

## 3. Write Pipeline
- [x] 3.1 Build detail_item rows from raw mismatch/support data
- [x] 3.2 Apply row cap and write to DB
- [x] 3.3 Ensure store_scope + enable switch gating

## 4. Docs & Queries
- [x] 4.1 Update HOW_TO_READ_REPORTS_IN_OB_70_sqls.txt with item queries
- [x] 4.2 Update readme_config.txt and docs/ADVANCED_USAGE.md

## 5. Tests
- [x] 5.1 Add unit tests for detail_item row builder
