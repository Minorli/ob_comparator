## 1. Implementation
- [x] 1.1 Preserve trigger-only helper metadata for PUBLIC synonyms that resolve to local SEQUENCE objects outside the managed terminal schema list.
- [x] 1.2 Keep the metadata expansion out of main object compare/fixup scope.
- [x] 1.3 Normalize OceanBase source PUBLIC synonym owner aliases, including `__public`.
- [x] 1.4 Preserve the real terminal sequence owner as fallback when synonym resolution succeeds but mapping/remap is absent.

## 2. Tests
- [x] 2.1 Add focused regression coverage for PUBLIC synonym -> external SEQUENCE trigger rewrite.
- [x] 2.2 Run focused trigger/synonym regression tests.
- [x] 2.3 Run pure-function regression suite.
- [x] 2.4 Run `python3 -m py_compile $(git ls-files '*.py')`.

## 3. Real DB verification
- [x] 3.1 Oracle source: create PUBLIC synonym to external SEQUENCE outside managed schema and verify generated trigger rewrite uses the real sequence owner.
- [x] 3.2 OceanBase source: create PUBLIC/`__public` synonym to external SEQUENCE and verify generated trigger rewrite uses the real sequence owner.
- [x] 3.3 Run Oracle -> OB smoke compare with TRIGGER/SEQUENCE enabled and verify the report is generated with version `V0.9.9.6-hotfix6`.

## 4. Release
- [x] 4.1 Update hotfix6 release documentation and version metadata.
- [x] 4.2 Build release package and verify SHA256 sums.
- [x] 4.3 Merge PR and publish GitHub release `v0.9.9.6-hotfix6`.
