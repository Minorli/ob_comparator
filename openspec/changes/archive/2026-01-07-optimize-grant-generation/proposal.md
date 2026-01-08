# Change: Optimize grant extraction and generation

## Why
Large environments can contain hundreds of thousands of object grants, causing slow grant planning and multi-day execution on OceanBase when every privilege is emitted as a standalone GRANT.

## What Changes
- Add configuration options to control grant extraction scope and GRANT statement merging.
- Cache privilege target resolution to reduce repeated remap inference.
- Merge GRANT statements by privileges and/or grantees to reduce SQL count.
- Emit progress logs during grant planning to avoid “hang” perception.

## Impact
- Affected specs: `generate-fixup`, `configuration-control`
- Affected code: `schema_diff_reconciler.py`, `config.ini.template`, `readme_config.txt`, `README.md`, `docs/ARCHITECTURE.md`, `docs/ADVANCED_USAGE.md`
