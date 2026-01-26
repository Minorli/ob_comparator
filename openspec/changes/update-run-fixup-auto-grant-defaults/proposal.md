# Change: Enable default auto-grant in run_fixup and extend coverage

## Why
Users frequently forget to pass run_fixup flags that enable auto-grant and dependency-aware execution, which leads to avoidable ORA-01031 failures (especially for cross-schema VIEWs). Today the auto-grant fallback is limited to VIEWs under --iterative or --view-chain-autofix, leaving other dependent object types without similar recovery.

## What Changes
- Default auto-grant behavior for run_fixup (no extra CLI flags required).
- Extend auto-grant beyond VIEW to other dependency-heavy object types (SYNONYM, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TRIGGER, MATERIALIZED VIEW).
- Use dependency chain reports to plan required grants ahead of execution, with on-error retry as a fallback.
- Add configuration switches to control auto-grant enablement and scope.
- Preserve existing manual modes (smart-order/iterative/view-chain-autofix) for power users.

## Impact
- Affected specs: execute-fixup, configuration-control
- Affected code: run_fixup.py (primary), config.ini.template/readme_config.txt (new switches)
- Behavioral change: run_fixup will auto-apply missing grants by default when enabled
