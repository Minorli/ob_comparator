# Change: Use DBMS_METADATA for VIEW fixup DDL

## Why
DBCat view DDL output is frequently malformed (comments/columns merged and Oracle-only syntax). For the relatively small number of missing views, using DBMS_METADATA yields cleaner, authoritative definitions.

## What Changes
- Switch VIEW fixup DDL extraction to use DBMS_METADATA as the primary source (dbcat view output no longer used).
- Preserve existing view cleanup rules, including OB version gating for WITH CHECK OPTION.
- Strip Oracle-only VIEW modifiers that are unsupported by OceanBase (e.g., EDITIONABLE).

## Impact
- Affected specs: generate-fixup
- Affected code: DDL extraction and sanitization logic in the main program
