# Maintainers

Primary maintainer:

- @Minorli

## Release Authority

Only maintainers should publish GitHub releases, move tags, or update release assets.

For customer hotfixes, maintainers must verify:

- intended files only
- package assets and checksums
- release evidence when database behavior changed
- customer deployment notes

## Branch Policy

The default branch is protected. Changes should go through pull requests with:

- passing `compile` status check
- review approval
- resolved conversations
- squash merge

Emergency maintainer bypass is reserved for production blockers and should be followed by a normal audit note or hotfix PR.
