## 1. Specification
- [x] 1.1 Update proposal with per-rule enable tags and RENAME rule
- [x] 1.2 Add spec deltas for compare-objects
- [x] 1.3 Add spec deltas for configuration-control

## 2. Implementation
- [x] 2.1 Extend blacklist rule loader to honor `enabled` tag (default true)
- [x] 2.2 Add name-pattern keyword settings and render `{{name_pattern_clause}}`
- [x] 2.3 Add built-in RENAME rule in `blacklist_rules.json`
- [x] 2.4 Map NAME_PATTERN/RENAME to a known blacklist reason
- [x] 2.5 Update docs (`readme_config.txt`, `config.ini.template`) with new settings

## 3. Tests
- [x] 3.1 Add unit tests for name-pattern clause rendering and enabled tag
- [ ] 3.2 Add integration test for `_RENAME` table blacklist

## 4. Validation
- [x] 4.1 Run `openspec validate add-blacklist-name-patterns --strict`
