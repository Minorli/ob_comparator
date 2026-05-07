# Support

Support is focused on the latest public hotfix line and the current `main` branch.

## Before Opening an Issue

Check the latest release and package notes:

- https://github.com/Minorli/ob_comparator/releases/latest

For runtime or report issues, include sanitized details:

- tool version or commit
- source and target database modes
- relevant config switches
- report path and detail file name
- generated fixup file name, if any
- sanitized database error code and message

Do not share production credentials, full `config.ini`, Oracle wallet files, customer data, or private SQL text in public issues.

## Security Issues

Report vulnerabilities privately through GitHub Security Advisories:

- https://github.com/Minorli/ob_comparator/security/advisories/new

Do not open public issues for exposed secrets, unsafe SQL execution, or customer data leakage.

## Real Database Verification

Real Oracle and OceanBase validation is maintainer-controlled. Public CI verifies syntax, packaging, hygiene, dependency metadata, and static analysis only.
