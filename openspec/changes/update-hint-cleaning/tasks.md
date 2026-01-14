## 1. Specification
- [x] 1.1 Update configuration-control with hint policy settings and defaults
- [x] 1.2 Update generate-fixup DDL cleanup requirement to include hint filtering behavior

## 2. Implementation
- [x] 2.1 Add hint policy parsing and allow/deny list loading
- [x] 2.2 Replace blanket hint removal with hint filtering logic
- [x] 2.3 Update config.ini.template with new settings
- [x] 2.4 Add tests for hint filtering scenarios

## 3. Validation
- [x] 3.1 Run `openspec validate update-hint-cleaning --strict`
- [x] 3.2 Run unit tests covering hint filtering behavior (or document manual checks if tests unavailable)
