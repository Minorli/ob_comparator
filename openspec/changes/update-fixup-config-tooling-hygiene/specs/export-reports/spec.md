## ADDED Requirements

### Requirement: Source stats helper consistency
The project SHALL keep `collect_source_object_stats.py` brief and full outputs consistent by reusing shared SQL templates for index/constraint/trigger statistics.

#### Scenario: Brief vs full stats consistency
- **WHEN** the stats helper is run with and without --brief
- **THEN** both modes use the same underlying SQL templates and produce consistent counts for the same input
