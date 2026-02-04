## 1. Implementation
- [x] 1.1 Add self-referencing FK detection using Oracle constraint metadata (resolve referenced table).
- [x] 1.2 Classify detected constraints as unsupported with reason code `FK_SELF_REF`.
- [x] 1.3 Ensure unsupported constraints are excluded from fixup generation.
- [x] 1.4 Add unit tests covering FK self-reference detection and classification.

## 2. Reporting
- [x] 2.1 Include `FK_SELF_REF` in `constraints_unsupported_detail` rows with clear reason text.
- [x] 2.2 Ensure `unsupported_objects_detail` includes those constraints.

## 3. Documentation
- [x] 3.1 Update report description (if needed) to mention self-referencing FK unsupported rule.
