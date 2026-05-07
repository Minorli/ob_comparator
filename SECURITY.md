# Security Policy

## Supported Versions

Security fixes are prioritized for the latest public 0.9.9.x hotfix line and the current `main` branch.

Older hotfixes may receive guidance or a forward-upgrade recommendation instead of a backport.

## Reporting a Vulnerability

Do not open a public issue for suspected vulnerabilities, exposed credentials, customer data, or SQL execution safety problems.

Use GitHub private vulnerability reporting when available. If it is not available, contact the maintainer privately and include:

- affected version or commit
- source and target database modes
- minimal reproduction steps
- sanitized error output or report snippets
- whether the issue can generate or execute unsafe fixup SQL

Do not include real passwords, full connection strings, Oracle wallet files, production `config.ini`, or customer data.

## Handling Expectations

The maintainer will triage severity, confirm reproducibility, and decide whether the fix should ship as a private patch, a public hotfix, or documented operational guidance.

Security-impacting releases must include verification evidence and explicit customer deployment notes.
