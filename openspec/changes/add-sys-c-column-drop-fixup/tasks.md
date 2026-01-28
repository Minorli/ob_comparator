## 1. Implementation
- [ ] Add config key `fixup_drop_sys_c_columns` with default `false`
- [ ] Wire config validation and wizard prompts
- [ ] Detect SYS_C extra columns in table ALTER generation and emit `ALTER TABLE ... FORCE` when the switch is enabled
- [ ] Keep non-SYS_C extra columns commented as before
- [ ] Update report hint text to describe SYS_C FORCE behavior when enabled

## 2. Tests
- [ ] Unit test for SYS_C extra columns producing FORCE
- [ ] Unit test ensuring non-SYS_C extra columns remain commented
- [ ] Regression test covering default behavior (switch off)

## 3. Documentation
- [ ] Update config.ini.template and readme_config.txt with the new switch
