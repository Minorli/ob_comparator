## 1. Implementation
- [x] 1.1 Update remap resolution to keep TRIGGER/VIEW/MATERIALIZED VIEW in source schema unless explicitly remapped.
- [x] 1.2 Preserve existing parent-follow logic for other dependent objects; adjust schema mapping logs/docs accordingly.
- [x] 1.3 Adjust trigger fixup generation to use table target schema for ON clause rewrites and add required GRANTs to trigger scripts.
- [x] 1.4 Gate remap inference, dependency checks, and metadata loading by `check_primary_types`/`check_extra_types`.
- [x] 1.5 Implement print-only handling for MATERIALIZED VIEW and PACKAGE/PACKAGE BODY (no OB validation/fixup).
- [x] 1.6 Update or add unit tests for trigger/view remap behavior, type gating, and print-only handling.
- [x] 1.7 Document the behavior change in CHANGELOG or relevant docs (if required by project norms).
