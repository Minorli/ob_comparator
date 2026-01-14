## 1. Implementation
- [x] 1.1 Add normalized signature caches for index/constraint/trigger data per table
- [x] 1.2 Add fast-path equality checks that skip detailed diff when signatures match
- [x] 1.3 Add extra_check_workers, extra_check_chunk_size, and extra_check_progress_interval config parsing with defaults and validation
- [x] 1.4 Add parallel execution when extra_check_workers > 1 and ensure deterministic result ordering
- [x] 1.5 Add extra check timing metrics and time-based progress logging

## 2. Tests
- [ ] 2.1 Unit tests for signature equality and mismatch detection (index/constraint/trigger)
- [ ] 2.2 Unit test for parallel run ordering determinism
- [ ] 2.3 Regression test to confirm extra check results unchanged vs baseline

## 3. Documentation
- [x] 3.1 Update config reference for extra_check_* settings
- [x] 3.2 Update performance guidance for large schema runs
