## 1. Implementation
- [x] 1.1 Add config keys: ddl_format_enable, ddl_format_types, ddl_formatter, sqlcl_bin, sqlcl_timeout, sqlcl_profile_path, ddl_format_fail_policy, ddl_format_batch_size, ddl_format_max_lines, ddl_format_max_bytes
- [x] 1.2 Resolve sqlcl_bin root path to bin/sql (or bin/sql.exe) and validate executable
- [x] 1.3 Implement SQLcl formatter wrapper (temp file IO, timeout, stderr capture, JAVA_TOOL_OPTIONS, batch mode)
- [x] 1.4 Add PL/SQL slash handling (strip trailing `/` before format, restore after)
- [x] 1.5 Add per-object-type formatting gate at fixup output stage (no effect on fixup logic)
- [x] 1.6 Emit formatter summary and failure detail report under main_reports

## 2. Tests
- [x] 2.1 Unit tests for formatter wrapper (success/failure/timeout)
- [x] 2.2 Unit tests for slash handling (procedure/package/trigger/type)
- [x] 2.3 Unit tests for type gating, size gating, and fallback behavior
- [x] 2.4 Unit tests for batch formatting selection and batch timeout handling
- [x] 2.5 Regression tests for VIEW comment-collapse scenarios (formatting on/off)

## 3. Documentation
- [x] 3.1 Update readme_config/config.ini template with new formatter switches
- [x] 3.2 Add formatter usage section to README/docs (types, risks, reports)
