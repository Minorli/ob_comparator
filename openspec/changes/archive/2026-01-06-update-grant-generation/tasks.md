## 1. Design
- [x] Document the grant generation pipeline, data sources, remap mapping, and dependency traversal rules.

## 2. Spec Updates
- [x] Update configuration-control for generate_grants and privilege-source loading requirements.
- [x] Update generate-fixup to define grant DDL generation, dependency-based augmentation, and DDL injection rules.
- [x] Update export-reports to remove grant display requirements and note fixup-only grant output.

## 3. Implementation
- [x] Load Oracle privilege metadata (DBA_TAB_PRIVS/DBA_SYS_PRIVS/DBA_ROLE_PRIVS) and cache in OracleMetadata.
- [x] Build merged grant plan from source grants + dependency-derived grants, remapped to target.
- [x] Apply grants to fixup outputs: central grants scripts and per-object DDL injection.
- [x] Add generate_grants config toggle to config.ini, config.ini.template, readme_config.txt, and runtime parsing.
- [x] Remove grant details from report output while preserving other sections.

## 4. Validation
- [x] Run `openspec validate update-grant-generation --strict` and resolve any issues.
