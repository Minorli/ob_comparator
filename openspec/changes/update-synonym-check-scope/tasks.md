## 1. Implementation
- [x] 1.1 Add `synonym_check_scope` config parsing/normalization/default and wizard validation.
- [x] 1.2 Apply synonym scope filter to source synonym loading for check pipeline.
- [x] 1.3 Apply synonym scope filter to OB metadata synonym loading for check/summarization consistency.
- [x] 1.4 Update docs and config template with clear behavior description.

## 2. Verification
- [x] 2.1 Add unit tests for normalization and scope filtering behavior.
- [x] 2.2 Run `python3 -m py_compile $(git ls-files '*.py')`.
- [x] 2.3 Run relevant unit tests.
- [x] 2.4 Run `openspec validate update-synonym-check-scope --strict`.
