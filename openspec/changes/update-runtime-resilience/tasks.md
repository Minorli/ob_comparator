## 1. Implementation
- [ ] 1.1 Add config keys for retry/policy controls and update validation/wizard/template/docs.
- [ ] 1.2 Add obclient retry wrapper with transient error classification and failure tracking.
- [ ] 1.3 Implement dbcat failure policy handling with per-object fallback and reporting.
- [ ] 1.4 Add DBMS_METADATA batch fallback per object with retry limits.
- [ ] 1.5 Export failure summaries and add counts to the main report.

## 2. Validation
- [ ] 2.1 Unit tests for obclient retry/backoff logic.
- [ ] 2.2 Unit tests for dbcat failure policy routing and fallback selection.
- [ ] 2.3 Unit tests for oracle batch DDL fallback behavior.
- [ ] 2.4 Integration/E2E tests (opt-in) for end-to-end resilience paths.
