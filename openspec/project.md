# Project Context

## Purpose
数据库对象对比工具 (Database Object Comparator)
对比 Oracle (源) 与 OceanBase (目标) 的数据库对象结构，生成差异报告与修复 SQL。
核心目标是确保迁移后 OceanBase 与 Oracle 的结构一致性，特别是针对复杂的 PL/SQL、索引、约束和表结构。

## Tech Stack
- Language: Python 3
- CLI Tools: obclient, sqlplus (via subprocess)
- Libraries: oracledb, rich
- Architecture: "Dump-Once, Compare-Locally" (一次转储，本地对比)

## Project Conventions

### Code Style
- Pythonic, standard PEP8 compliant where possible.
- Extensive docstrings in Chinese.
- Single-file script dominance (`schema_diff_reconciler.py` is huge), likely needs refactoring eventually.

### Architecture Patterns
- **Extraction**: Extract metadata from Oracle and OceanBase.
- **Normalization**: Remap Oracle types/names to OceanBase equivalents.
- **Comparison**: In-memory comparison using Dictionaries/Sets.
- **Reporting**: Generate textual reports and `.sql` fix scripts.

## Domain Context
- **Oracle vs OceanBase**: Requires understanding of dialect differences (e.g., VARCHAR2 lengths, mapping rules).
- **Performance**: Avoid N+1 queries by dumping metadata first.

## Important Constraints
- Must handle large schemas efficiently.
- Must account for OceanBase specific limitations or differences.