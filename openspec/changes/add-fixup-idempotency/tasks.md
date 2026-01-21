## 1. Implementation
- [ ] 1.1 Add config parsing and validation for fixup_idempotent_mode and fixup_idempotent_types.
- [ ] 1.2 Implement DDL wrapper utilities for replace/guard/drop_create modes.
- [ ] 1.3 Integrate wrappers into fixup generation for each object type.
- [ ] 1.4 Emit summary metrics for guarded/replaced statements.
- [ ] 1.5 Update config templates and documentation.

## 2. Validation
- [ ] 2.1 Add unit tests for guard/replace output formatting.
- [ ] 2.2 Add tests to confirm statement splitting handles PL/SQL blocks.
- [ ] 2.3 Run targeted fixup generation tests to verify idempotent output.
