## ADDED Requirements

### Requirement: Structured difference explanation
The compare stage SHALL attach a structured in-memory explanation to each reported mismatch, suppression, and review-first decision, while report export is responsible for serialization.

#### Scenario: VARCHAR length mismatch
- **WHEN** a VARCHAR/VARCHAR2 length difference is reported
- **THEN** the explanation includes source length basis, target length, length semantics, threshold, rule id, and decision

#### Scenario: VARCHAR byte expansion accepted
- **WHEN** source mode is Oracle, the source column is `VARCHAR2(100)` or equivalent BYTE semantics, and the target OceanBase column is `VARCHAR2(150)`, `VARCHAR(150)`, or another value inside the accepted BYTE expansion window
- **THEN** the compare result treats the column as compatible, does not emit a mismatch row, does not generate a fixup action, and does not export a per-column mismatch explanation

#### Scenario: VARCHAR literal spelling differs but semantics match
- **WHEN** source and target type literals differ only by `VARCHAR` versus `VARCHAR2` spelling and the length/semantics rule is compatible
- **THEN** the compare result treats the type spelling as compatible and does not generate an ALTER solely to change `VARCHAR` to `VARCHAR2` or `VARCHAR2` to `VARCHAR`

#### Scenario: Character semantics require exact source length
- **WHEN** source metadata has `CHAR_USED='C'`
- **THEN** the comparison uses exact character length semantics and MUST NOT apply the BYTE expansion window to decide fixup length

#### Scenario: Nullability mismatch
- **WHEN** a NULLABLE or NOT NULL semantic difference is reported
- **THEN** the explanation includes source nullable evidence, target nullable evidence, constraint/check evidence when available, and whether the decision is runnable or review-first

#### Scenario: Suppressed noise
- **WHEN** a difference is suppressed as noise
- **THEN** the explanation records the suppressing rule and the evidence that made the suppression valid

### Requirement: Compatibility matrix decision
The system SHALL derive compare support decisions from the loaded compatibility registry for source mode, target version, object type, and operation.

#### Scenario: Object family supported
- **WHEN** the matrix marks an object family and operation as supported
- **THEN** compare proceeds normally and records the matrix decision in structured output

#### Scenario: Object family degraded or manual
- **WHEN** the matrix marks an object family or operation as degraded or manual
- **THEN** compare records the degraded/manual reason and routes the object to report-only or manual action output as appropriate

### Requirement: Object-level evidence references
The system SHALL record evidence references for object-level compare decisions.

#### Scenario: Difference emitted
- **WHEN** a difference is emitted for an object
- **THEN** the corresponding detail row includes references to metadata fields, DDL source, query source, or report artifact paths sufficient to explain the decision
