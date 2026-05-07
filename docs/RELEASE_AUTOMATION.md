# Release Automation

This repository has two release layers:

- GitHub automation for deterministic package assembly.
- Maintainer-controlled real database evidence for releases that affect database semantics.

## Dry-run Package

Use the `Release Package` workflow from the Actions tab.

Recommended first run:

- `version`: next version without the leading `v`
- `commitish`: `main`, a release branch, or an exact commit
- `publish_release`: `false`

The workflow uploads one artifact containing:

- `ob_comparator-<version>-toolkit.zip`
- `ob_comparator-<version>-SHA256SUMS.txt`
- `ob_comparator-<version>-manifest.json`

The package is built from tracked git content only. Repository governance files such as `.github/`, `SECURITY.md`, and `CODE_OF_CONDUCT.md` are excluded from the customer toolkit zip.

The workflow always runs the packaging script from the default branch and packages the selected `commitish`. That means the release automation can package older release branches or tags without requiring the packaging script to exist in that older ref.

## Publish Release

Set `publish_release=true` only after:

- the release checklist is complete
- required local tests passed
- real database evidence is complete when database semantics changed
- customer deployment notes are ready

The workflow creates tag `v<version>` when the release does not exist and uploads the package assets. Existing releases are protected by default; set `update_existing_release=true` only when intentionally replacing assets and notes on an existing release.

Do not silently move a public tag. Publish a new hotfix or clearly document a correction.

## Real Database Evidence

Real Oracle and OceanBase verification is intentionally not executed in public CI. Those environments require private credentials, controlled schemas, and deliberate cleanup.

Attach real database evidence JSON to the release when the change affects:

- metadata extraction
- compare semantics
- fixup generation or execution
- report summary/detail/report_db counts
- source mode behavior

## Local Equivalent

The packaging step can be reproduced locally:

```bash
python tools/build_release_package.py --version 0.9.9.6-hotfix5 --commitish main --output-dir dist
cd dist
sha256sum -c ob_comparator-0.9.9.6-hotfix5-SHA256SUMS.txt
```
