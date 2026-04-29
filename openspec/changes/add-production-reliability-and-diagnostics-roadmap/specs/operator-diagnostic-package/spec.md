## ADDED Requirements

### Requirement: Diagnostic package generation
The system SHALL provide an independent `diagnostic_bundle.py` CLI that generates a customer diagnostic package for a comparator run.

#### Scenario: CLI help
- **WHEN** the operator runs `python3 diagnostic_bundle.py --help`
- **THEN** help text documents `--run-dir`, `--config`, `--output`, `--pid`, `--hang`, `--include-sql-content`, `--redact-identifiers`, `--max-file-mb`, and `--max-bundle-mb`

#### Scenario: Post-run package
- **WHEN** a completed or failed run directory is provided with `python3 diagnostic_bundle.py --run-dir <run_dir> --config <config.ini>`
- **THEN** the system writes a diagnostic bundle containing a manifest, sanitized configuration summary, run summary, report index, relevant detail files, log tail, warning/error summary, and fixup plan summary

#### Scenario: Main programs provide evidence only
- **WHEN** `schema_diff_reconciler.py` or `run_fixup.py` runs
- **THEN** they write heartbeat, checkpoint, log, report, and fixup-plan evidence for the diagnostic CLI to collect, but they do not implement bundle packaging themselves

#### Scenario: Missing run directory
- **WHEN** the requested run directory does not exist
- **THEN** the diagnostic command exits with a clear error and does not create a partial bundle

### Requirement: Diagnostic redaction
The diagnostic package SHALL redact secrets and sensitive connection material by default.

#### Scenario: Config contains credentials
- **WHEN** config.ini contains password, token, secret, wallet, key, or credential-like fields
- **THEN** the diagnostic package replaces those values with redaction markers

#### Scenario: Logs contain command-line credentials
- **WHEN** collected text contains Oracle connect strings such as `user/password@service`, URL-style credentials, `-p <password>`, or `--password <password>`
- **THEN** the diagnostic package redacts the credential portions before writing bundle artifacts

#### Scenario: SQL content collection
- **WHEN** diagnostic SQL content collection is not explicitly enabled
- **THEN** the diagnostic package includes SQL file names, paths, sizes, hashes, object identity when available, and summaries but not full SQL text

#### Scenario: SQL content opt-in
- **WHEN** the operator passes `--include-sql-content` or enables the equivalent explicit config switch
- **THEN** the diagnostic package records that opt-in in `manifest.json` and may include SQL content after secret redaction and file-size limits are applied

#### Scenario: Identifier redaction enabled
- **WHEN** identifier redaction is enabled
- **THEN** schema, object, and column identifiers are replaced with stable hashes and a local-only mapping file is excluded from the bundle by default

### Requirement: Hang diagnostic snapshot
The diagnostic package SHALL support collecting evidence from a run that appears hung.

#### Scenario: Running process detected
- **WHEN** `python3 diagnostic_bundle.py --pid <pid> --hang` is run for a comparator or run_fixup process
- **THEN** the diagnostic package includes heartbeat state, current phase, current object or SQL file when available, elapsed time, process id, command line summary, and latest log tail

#### Scenario: Unrelated process rejected
- **WHEN** `--pid` points to a process that does not match the heartbeat pid or an allowed comparator/obclient process identity
- **THEN** the diagnostic package records the rejection reason and does not include that process command line

#### Scenario: No heartbeat available
- **WHEN** a running process has no heartbeat state
- **THEN** the diagnostic package records that limitation and still includes process and log evidence

### Requirement: Diagnostic manifest
The diagnostic package SHALL include a machine-readable manifest.

#### Scenario: Bundle created
- **WHEN** a diagnostic bundle is created
- **THEN** `manifest.json` lists included files, bundle-content file hashes, redaction policy, collection time, tool version, code commit when available, size caps, and omitted artifact reasons

### Requirement: Diagnostic self-check
The diagnostic package SHALL report whether required support evidence is present.

#### Scenario: Evidence incomplete
- **WHEN** key artifacts such as run log, report index, or run summary are missing
- **THEN** the diagnostic summary lists missing evidence and suggests the next collection command or config switch
