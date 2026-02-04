## 1. Implementation
- [ ] 1.1 Add config option `sequence_sync_mode` with validation (off|last_number)
- [ ] 1.2 Load `LAST_NUMBER` from Oracle `DBA_SEQUENCES` when sync mode enabled
- [ ] 1.3 Generate `fixup_scripts/sequence_restart/*.sql` for missing sequences when sync mode enabled
- [ ] 1.4 Ensure main sequence comparison remains existence-only (no new mismatches)
- [ ] 1.5 Update run_fixup to **exclude** sequence_restart by default and add explicit include option
- [ ] 1.6 Add report note indicating sequence restart scripts must run after data migration
- [ ] 1.7 Update config template and readme_config with new settings and usage guidance

## 2. Tests
- [ ] 2.1 Unit test: sequence_restart script generation uses LAST_NUMBER
- [ ] 2.2 Integration: Oracle/OB sequences (nocache/cache, inc=1, inc=-1) validate RESTART support
- [ ] 2.3 Run fixup smoke test: sequence_restart excluded by default; executed only with explicit flag

## 3. Validation
- [ ] 3.1 `python3 -m py_compile` passes
- [ ] 3.2 Update regression note in audit (if required)
