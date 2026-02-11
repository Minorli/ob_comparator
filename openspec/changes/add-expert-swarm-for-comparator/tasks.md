## 1. Implementation
- [x] 1.1 Add OpenSpec change artifacts for the expert swarm capability.
- [x] 1.2 Implement `expert_swarm.py` with deterministic expert-role blueprint generation.
- [x] 1.3 Upgrade execute mode to parallel subteams with a final arbiter.
- [x] 1.4 Add deterministic subteam/arbitration artifact mapping in blueprint and docs.

## 2. Tests
- [x] 2.1 Add unit tests for subteam structure and role coverage.
- [x] 2.2 Add unit tests for arbiter and subteam output-path mapping.
- [x] 2.3 Add unit tests for output path/file generation.

## 3. Verification
- [x] 3.1 Run `openspec validate add-expert-swarm-for-comparator --strict`.
- [x] 3.2 Run `python3 -m py_compile $(git ls-files '*.py')`.
- [x] 3.3 Run unit tests including `test_expert_swarm.py`.
