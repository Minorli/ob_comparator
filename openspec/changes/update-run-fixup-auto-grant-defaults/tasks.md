## 1. Specification
- [ ] Add execute-fixup spec deltas for default auto-grant behavior and dependency-based planning
- [ ] Add configuration-control spec deltas for auto-grant switches and defaults

## 2. Implementation
- [ ] Read dependency_chains_*.txt (TARGET - REMAPPED section) and VIEWs_chain_*.txt to build grant requirements per object
- [ ] Implement auto-grant planning/execution for configured object types
- [ ] Add on-error retry path for permission-denied failures (ORA-01031/ORA-01720)
- [ ] Ensure auto-grant uses grants_miss/grants_all first, with optional fallback generation
- [ ] Add summary logging for auto-grant actions and blocked cases

## 3. Tests
- [ ] Unit tests: dependency_detail parsing and grant planning by type
- [ ] Unit tests: auto-grant scope filtering and retry behavior
- [ ] Integration test: cross-schema view/procedure fixup succeeds without extra CLI flags

## 4. Documentation
- [ ] Update readme_config.txt and config.ini.template for new switches
- [ ] Update docs/README.md (if needed) to reflect new defaults
