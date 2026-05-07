# Roadmap

This roadmap is directional, not a public commitment to specific dates.

## Current Focus

- Keep Oracle to OceanBase and OceanBase to OceanBase definition compare behavior stable.
- Reduce customer hotfix packaging risk with deterministic release assets.
- Preserve fixup safety as the default operating model.

## Near-term

- Expand CI without requiring database credentials.
- Continue improving release evidence and package manifest quality.
- Add focused regression fixtures only when they can be safely tracked without customer data or environment coupling.

## Later

- Publish a structured documentation site from the existing `docs/` content.
- Add SBOM/provenance artifacts for release packages.
- Extract stable shared logic from large modules when risk and coverage justify it.

## Non-goals

- Public CI will not connect to maintainer Oracle or OceanBase instances.
- Releases will not include local `config.ini`, generated reports, wallets, credentials, or ad hoc smoke fixtures.
