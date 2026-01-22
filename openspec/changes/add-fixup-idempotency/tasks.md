## 1. Implementation
- [x] 1.1 Add config parsing and validation for fixup_idempotent_mode and fixup_idempotent_types.
- [x] 1.2 Implement DDL wrapper utilities for replace/guard/drop_create modes.
- [x] 1.3 Integrate wrappers into fixup generation for each object type.
- [x] 1.4 Emit summary metrics for guarded/replaced statements.
- [x] 1.5 Update config templates and documentation.

## 2. Validation
- [x] 2.1 Add unit tests for guard/replace output formatting.
- [x] 2.2 Add tests to confirm statement splitting handles PL/SQL blocks.
- [x] 2.3 Run targeted fixup generation tests to verify idempotent output.
