## 1. Implementation
- [x] 1.1 Extend Oracle/OB metadata loaders to capture IDENTITY_COLUMN and DEFAULT_ON_NULL flags when available.
- [x] 1.2 Extend Oracle/OB metadata loaders to capture SEQUENCE attributes (increment/min/max/cycle/order/cache).
- [x] 1.3 Update view dependency extraction to handle subquery/CTE blocks and provide dependency fallback during VIEW rewrite.
- [x] 1.4 Update VIEW DDL rewrite to resolve PUBLIC synonym references to base objects before remap.
- [x] 1.5 Update sequence comparison to include attribute mismatches and surface details in reports.
- [x] 1.6 Update table comparison to record IDENTITY/DEFAULT ON NULL mismatches as type issues.

## 2. Tests
- [x] 2.1 Add unit tests for PUBLIC synonym view rewrite and subquery dependency extraction.
- [x] 2.2 Add unit tests for sequence attribute mismatch reporting.
- [x] 2.3 Add unit tests for IDENTITY/DEFAULT ON NULL mismatch detection.
