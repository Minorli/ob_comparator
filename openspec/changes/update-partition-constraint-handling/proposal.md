# Change: Partitioned table constraint handling

## Why
OceanBase requires the partition key to be included in a PRIMARY KEY. For partitioned tables where the source PK does not include the partition key, the current comparison and fixup logic reports missing PKs and generates PK DDL that will fail. OMS downgrades such PKs to UNIQUE and treats that result as correct.

## What Changes
- Capture partition key columns for partitioned tables during Oracle and OceanBase metadata dumps.
- Classify partitioned tables where the source PK does or does not include the partition key.
- Comparison: for non-inclusive PKs, treat a target UNIQUE constraint on the same columns as correct and do not report a missing PK; report a missing UNIQUE when neither PK nor UK exists.
- Fixup: generate UNIQUE DDL (not PRIMARY KEY) for non-inclusive PK cases; generate PRIMARY KEY for inclusive PK cases.

## Impact
- Affected specs: compare-objects, generate-fixup
- Affected code: schema_diff_reconciler.py (metadata dump, constraint comparison, constraint fixup generation)
- Tests: add unit tests for partitioned constraint handling; run integration checks when DB access is available
