## ADDED Requirements

### Requirement: View grant execution order
The fixup executor SHALL run `view_prereq_grants` before view creation and `view_post_grants` after view creation when using dependency-aware ordering.

#### Scenario: Smart order execution
- **WHEN** run_fixup executes with smart-order
- **THEN** view_prereq_grants runs before `view/`
- **AND** view_post_grants runs after `view/`
