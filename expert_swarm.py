#!/usr/bin/env python3
"""
Expert swarm toolkit for the comparator repository.

Default behavior:
- Generate a deterministic swarm blueprint JSON.
- Generate a task brief markdown.

Optional behavior:
- Execute a stronger swarm through parallel subteams + final arbiter.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

SWARM_OUTPUT_DIR = Path("audit/swarm")
DEFAULT_BLUEPRINT_PATH = SWARM_OUTPUT_DIR / "swarm_blueprint.json"
DEFAULT_TASK_PATH = SWARM_OUTPUT_DIR / "swarm_task.md"


@dataclass(frozen=True)
class ExpertRole:
    role_id: str
    name: str
    focus: str
    output_path: str
    instructions: str


@dataclass(frozen=True)
class SubTeam:
    team_id: str
    name: str
    focus: str
    output_path: str
    member_role_ids: Tuple[str, ...]
    coordinator_instructions: str


def build_guardrails() -> List[str]:
    return [
        "Do not modify production logic unless explicitly asked.",
        "Respect configuration discipline: any new switch must update schema_diff_reconciler.py defaults+validation+wizard, config.ini.template, and readme_config.txt.",
        "Keep fixup/formatting behavior output-only; never change check/remap determinism.",
        "Remap and dependency inference must remain deterministic regardless of formatting toggles.",
        "Treat Oracle/OceanBase compatibility decisions as high-risk and evidence-driven.",
        "When reporting findings, prioritize bugs/regressions and missing tests first.",
        "Statistics caliber changes require invariant validation: 应校验 = 命中 + 缺失 + 不可修补 + 排除.",
        "Real database verification is mandatory for SQL/DDL/remap/fixup logic changes (AGENTS.md §2).",
        "Zero regression policy: check/fixup logic must stay deterministic and decoupled (AGENTS.md §4).",
        "All output must be in Chinese (中文). Instructions are in English for compatibility.",
    ]


def build_expert_roles() -> List[ExpertRole]:
    return [
        # ── 1. Chief Architect ──────────────────────────────────────────
        ExpertRole(
            role_id="chief_architect",
            name="Chief Architect",
            focus="Module boundaries, decomposition roadmap, data flow integrity, dump-once architecture.",
            output_path="audit/swarm/architecture_report.md",
            instructions=(
                "You are the Chief Architect for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "ARCHITECTURE CONTEXT:\n"
                "- schema_diff_reconciler.py: ~33K lines, ~500 functions — single-file monolith\n"
                "- run_fixup.py: ~4.3K lines — fixup executor with 3 modes (single-round, iterative, VIEW chain autofix)\n"
                "- init_users_roles.py: ~800 lines — user/role initialization\n"
                "- Dump-once architecture: all metadata loaded into memory via OracleMetadata/ObMetadata NamedTuples\n\n"
                "DATA FLOW (9 stages — trace and validate each):\n"
                "Config → Remap rules (load_remap_rules) → Oracle metadata (dump_oracle_metadata) → "
                "OB metadata (dump_ob_metadata) → Object mapping (resolve_remap_target) → "
                "Comparison (check_primary_objects + check_extra_objects) → DDL extraction (dbcat/SQLcl) → "
                "Fixup generation (generate_fixup_scripts) → Report (print_final_report + save_report_to_db)\n\n"
                "KEY DATA STRUCTURES:\n"
                "- OracleMetadata / ObMetadata (NamedTuple): 20+ fields each\n"
                "- DependencyRecord: tracks object dependencies for remap inference\n"
                "- SupportClassificationResult: classifies missing objects by support status\n\n"
                "STATISTICS PIPELINE (known caliber bug — see audit/trigger_statistics_root_cause_analysis.md):\n"
                "compute_object_counts → reconcile_object_counts_summary → build_missing_breakdown_counts\n"
                "Problem: extra_blocked_counts uses pre-filter data, reconcile uses post-filter data.\n\n"
                "PROPOSED DECOMPOSITION: config/, metadata/, remap/, compare/, ddl/, fixup/, report/, blacklist/, grants/\n\n"
                "YOUR TASKS:\n"
                "1. Audit module boundaries and coupling hot-spots (with function names and line numbers)\n"
                "2. Propose phased decomposition roadmap (backward-compatible, no big-bang rewrite)\n"
                "3. Identify dependency direction violations\n"
                "4. Assess dump-once memory risk for large schemas (10K+ tables)\n"
                "5. Validate data flow integrity between pipeline stages\n\n"
                "OUTPUT: 所有输出使用中文。提供分阶段拆分路线图，耦合热点必须给出具体函数名和行号。"
            ),
        ),
        # ── 2. Principal Database Expert ────────────────────────────────
        ExpertRole(
            role_id="principal_database_expert",
            name="Principal Database Expert",
            focus="Oracle↔OceanBase compatibility, DDL semantics, remap correctness, migration safety.",
            output_path="audit/swarm/database_report.md",
            instructions=(
                "You are the Principal Database Expert for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "DOMAIN: Oracle-to-OceanBase (Oracle mode) schema migration correctness.\n\n"
                "CRITICAL CODE PATHS:\n"
                "1. Remap resolution: resolve_remap_target — 5-level priority chain "
                "(explicit → no-infer → dependent-follow → dependency-inference → schema-mapping)\n"
                "2. DDL cleaning pipeline: clean_view_ddl_for_oceanbase → SqlMasker → clean_storage_clauses\n"
                "   Known bug C-01: SqlMasker doesn't handle Oracle Q-quote literals\n"
                "   Known bug H-12: clean_storage_clauses doesn't protect strings/comments\n"
                "3. VIEW rewriting: Two functions handle unqualified references —\n"
                "   replace_unqualified_table_refs (safe, FROM/JOIN only) vs\n"
                "   replace_unqualified_identifier inside adjust_ddl_for_object (unsafe, flat token scan)\n"
                "   Known bug: VIEW column names matching table names get schema-prefixed "
                "(see audit/view_column_schema_prefix_bug.md)\n"
                "4. Trigger comparison: build_trigger_cache_for_table, compare_triggers_for_table\n"
                "   Known bug: trigger owner doesn't follow table remap; signature includes ENABLED/DISABLED status\n"
                "5. PRAGMA handling: clean_pragma_statements removes ALL PRAGMA including supported EXCEPTION_INIT (C-02)\n"
                "6. PL/SQL remap: remap_plsql_object_references has incomplete reserved word list (C-03)\n\n"
                "KNOWN ISSUES: Read audit/deep_code_review_2025.md (C-01~C-05, H-01~H-14)\n\n"
                "BLACKLIST: blacklist_rules.json — H-08: IOT_TABLES rule uses wrong black_type='DBLINK'\n\n"
                "OUTPUT: 所有输出使用中文。按严重度分级 CRITICAL/HIGH/MEDIUM/LOW。"
                "每个发现必须包含文件名、行号、影响范围、修复建议。"
            ),
        ),
        # ── 3. Principal Code Reviewer ──────────────────────────────────
        ExpertRole(
            role_id="principal_code_reviewer",
            name="Principal Code Reviewer",
            focus="Bug risk, behavioral regression, edge cases, missing error handling.",
            output_path="audit/swarm/code_review_report.md",
            instructions=(
                "You are the Principal Code Reviewer for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "REVIEW BASELINE: 51 known issues in audit/deep_code_review_2025.md "
                "(5 CRITICAL, 14 HIGH, 22 MEDIUM, 10 LOW).\n"
                "Your mission: find issues BEYOND the known 51.\n\n"
                "FOCUS AREAS:\n"
                "1. Race conditions in iterative fixup mode (run_fixup.py)\n"
                "2. Error handling gaps in subprocess calls (obclient, dbcat, SQLcl)\n"
                "3. Silent data corruption paths (errors caught but data wrong)\n"
                "4. State mutation side effects (dict/list passed by reference then modified)\n"
                "5. Unicode/encoding edge cases in DDL handling\n"
                "6. Boundary conditions in statistics reconciliation\n\n"
                "CRITICAL PATHS:\n"
                "- split_sql_statements (run_fixup.py): slash_block state machine (H-04)\n"
                "- recompile_invalid_objects (run_fixup.py): success detection uses returncode only (H-05)\n"
                "- VIEW chain autofix (run_fixup.py): exists_cache + planned_objects shared state (H-06/H-07)\n"
                "- save_report_to_db: 15+ table inserts, partial failure handling\n"
                "- build_missing_breakdown_counts: clamp logic fails when total=0\n\n"
                "SEPARATION RULE: check logic vs fixup/formatting must stay decoupled.\n\n"
                "OUTPUT: 所有输出使用中文。按严重度分级。"
                "明确标注'已知问题的新变体' vs '全新发现'。每个 CRITICAL/HIGH 给出最小复现场景。"
            ),
        ),
        # ── 4. Principal Performance Expert ─────────────────────────────
        ExpertRole(
            role_id="principal_performance_expert",
            name="Principal Performance & Reliability Expert",
            focus="Runtime efficiency, memory footprint, subprocess management, scaling behavior.",
            output_path="audit/swarm/performance_reliability_report.md",
            instructions=(
                "You are the Principal Performance & Reliability Expert for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "ARCHITECTURE: Dump-once — all metadata loaded into memory once. No streaming, no pagination.\n\n"
                "HOT PATHS TO PROFILE:\n"
                "1. Oracle metadata extraction: dump_oracle_metadata loads ALL objects into OracleMetadata NamedTuple. "
                "For 10K+ tables, estimate memory footprint.\n"
                "2. DDL extraction: fetch_dbcat_schema_objects (JDK subprocess) + oracle_get_ddl_batch (SQLcl). I/O bound.\n"
                "3. Object comparison: column-by-column per table. Trigger/index matching. Identify O(n²) patterns.\n"
                "4. DDL cleaning: SqlMasker regex operations on large DDL strings. Multiple regex passes.\n"
                "5. Report DB writes: save_report_to_db — 15+ sequential table inserts. No batching.\n"
                "6. Fixup execution: sequential obclient subprocess per SQL file. No connection reuse.\n\n"
                "KNOWN ISSUES:\n"
                "- flat_cache parallel loading: failed loads still removed from schema_requests (H-14)\n"
                "- exists_cache never invalidated during VIEW chain (H-06)\n"
                "- No memory monitoring for dump-once architecture\n"
                "- No file locking for concurrent execution (M-06)\n"
                "- active_failed_paths retains deleted files (H-10)\n\n"
                "OUTPUT: 所有输出使用中文。提供可量化的优化建议（预期提升百分比、内存节省量）。"
                "对每个热点路径给出 Big-O 分析。"
            ),
        ),
        # ── 5. Principal Programming Expert ─────────────────────────────
        ExpertRole(
            role_id="principal_programming_expert",
            name="Principal Programming Expert",
            focus="Implementation quality, refactoring strategy, code patterns, developer ergonomics.",
            output_path="audit/swarm/programming_report.md",
            instructions=(
                "You are the Principal Programming Expert for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "CODE PATTERNS TO EVALUATE:\n"
                "1. NamedTuple usage (40+ definitions): immutability good, but some very large (OracleMetadata 20+ fields)\n"
                "2. Regex patterns: SqlMasker, clean_*, replace_*, remap_* — check for catastrophic backtracking\n"
                "3. Function size: many >100 lines. Identify extraction candidates.\n"
                "4. Error handling: mix of try/except with logging vs raising. Inconsistent patterns.\n"
                "5. Type hints: partial coverage. Many functions lack return type annotations.\n\n"
                "REFACTORING PRIORITIES:\n"
                "1. Extract DDL cleaning into a pipeline class (currently 5+ standalone functions in sequence)\n"
                "2. Extract remap resolution into dedicated module (interleaved with comparison logic)\n"
                "3. Standardize subprocess invocation (obclient, dbcat, SQLcl all different patterns)\n"
                "4. Configuration validation: currently in main(), should be dedicated validator\n\n"
                "IMPLEMENTATION GUIDANCE:\n"
                "- Produce concrete patch slices with sequencing\n"
                "- Prefer minimal-risk, test-first changes\n"
                "- Each refactoring step must maintain backward compatibility\n\n"
                "OUTPUT: 所有输出使用中文。每个重构建议包含当前代码位置、目标结构、迁移步骤、风险评估。按 ROI 排序。"
            ),
        ),
        # ── 6. Principal Test Expert ────────────────────────────────────
        ExpertRole(
            role_id="principal_test_expert",
            name="Principal Test Strategy Expert",
            focus="Test architecture, coverage gaps, determinism, regression prevention.",
            output_path="audit/swarm/test_strategy_report.md",
            instructions=(
                "You are the Principal Test Strategy Expert for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "CURRENT STATE:\n"
                "- test_schema_diff_reconciler.py: ~227/494 functions tested (46%)\n"
                "- test_run_fixup.py: ~40/100 functions (40%)\n"
                "- test_init_users_roles.py: 4/30 functions (13%)\n"
                "- Total: ~271/624 functions tested (43%)\n"
                "- Zero assertRaises in main test file (7886 lines)\n\n"
                "CRITICAL GAPS:\n"
                "- CRITICAL: dump_oracle_metadata, dump_ob_metadata, main(), save_report_to_db\n"
                "- HIGH: fetch_dbcat_schema_objects, oracle_get_ddl_batch, auto grant functions\n"
                "- HIGH: init_users_roles.py 26/30 functions untested\n"
                "- MEDIUM: all exception handling paths (0 assertRaises)\n\n"
                "QUALITY ISSUES:\n"
                "1. Over-mocking: all DB queries mocked, SQL syntax errors invisible\n"
                "2. Weak assertions: some tests only check 'result is not None'\n"
                "3. No file I/O error testing (17 json.load/dump sites)\n"
                "4. No subprocess failure simulation\n\n"
                "AGENTS.MD REQUIREMENTS:\n"
                "- Syntax check before commit: python3 -m py_compile $(git ls-files '*.py')\n"
                "- Unit tests must cover changed paths\n"
                "- Integration run required for DB-affecting changes\n\n"
                "OUTPUT: 所有输出使用中文。测试矩阵格式: 函数名|当前覆盖|风险等级|建议测试类型|优先级。"
                "按'防止回归的价值'排序。"
            ),
        ),
        # ── 7. Principal Software Designer ──────────────────────────────
        ExpertRole(
            role_id="principal_software_designer",
            name="Principal Software Designer",
            focus="API design, abstraction layers, extensibility patterns, configuration architecture.",
            output_path="audit/swarm/software_design_report.md",
            instructions=(
                "You are the Principal Software Designer for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "DESIGN CONCERNS:\n"
                "1. Configuration: 100+ settings in flat config.ini. New switches require 4-place updates "
                "(schema_diff_reconciler.py defaults+validation+wizard, config.ini.template, readme_config.txt).\n"
                "2. Object type extensibility: adding new type requires touching 10+ functions. No plugin/registry.\n"
                "3. DDL cleaning pipeline: 5+ functions in sequence, no pipeline abstraction, no composability.\n"
                "4. Comparison interface: each type has different function signature. No common Comparator interface.\n"
                "5. Report generation: tightly coupled to comparison logic.\n"
                "6. Remap resolution: 5-level priority in one 200-line function. No strategy pattern.\n\n"
                "CONSTRAINTS:\n"
                "- Backward compatibility with existing config.ini files\n"
                "- Must not break dump-once architecture\n"
                "- check/fixup separation (AGENTS.md §4.3)\n"
                "- Remap resolution must stay deterministic\n\n"
                "OUTPUT: 所有输出使用中文。提供接口设计（Python Protocol/ABC）。"
                "每个设计建议标注当前痛点、目标设计、迁移路径、兼容性影响。"
            ),
        ),
        # ── 8. Principal Product Manager ────────────────────────────────
        ExpertRole(
            role_id="principal_product_manager",
            name="Principal Product Manager",
            focus="User experience, report clarity, error messaging, migration workflow completeness.",
            output_path="audit/swarm/product_report.md",
            instructions=(
                "You are the Principal Product Manager for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "USER PERSONAS:\n"
                "1. DBA performing Oracle→OceanBase migration (primary)\n"
                "2. Migration project manager reviewing reports\n"
                "3. Developer debugging migration failures\n\n"
                "KNOWN UX ISSUES:\n"
                "1. Statistics panel confusion (audit/trigger_statistics_root_cause_analysis.md):\n"
                "   '应校验 50, 命中 20, 缺失 0, 不支持 30' — math doesn't add up.\n"
                "   Users ask: '到底缺不缺？30 个触发器是什么状态？'\n"
                "2. Trigger report: '差异表 30' misread as '30 个触发器缺失'\n"
                "3. VIEW fixup: column names get schema-prefixed (audit/view_column_schema_prefix_bug.md)\n"
                "4. Error messaging: mix of English/Chinese, no actionable guidance\n"
                "5. Report traceability: report DB has empty connection info fields (C-05)\n\n"
                "WORKFLOW GAPS:\n"
                "- No pre-flight check (validate config + connectivity before full run)\n"
                "- No incremental comparison\n"
                "- No migration progress dashboard\n"
                "- No rollback guidance when fixup fails\n\n"
                "OUTPUT: 所有输出使用中文。按用户旅程（配置→比对→修复→验证）组织发现。"
                "每个 UX 问题包含: 用户看到什么、用户期望什么、修复建议。"
            ),
        ),
        # ── 9. Principal Security Expert ────────────────────────────────
        ExpertRole(
            role_id="principal_security_expert",
            name="Principal Security Expert",
            focus="Credential handling, command injection, file safety, supply chain.",
            output_path="audit/swarm/security_report.md",
            instructions=(
                "You are the Principal Security Expert for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "KNOWN VULNERABILITIES (from audit/deep_code_review_2025.md):\n"
                "1. H-01: Passwords in CLI args — obclient -p{password} visible via ps aux\n"
                "   Files: run_fixup.py, init_users_roles.py, init_test.py, schema_diff_reconciler.py\n"
                "2. H-03: obclient -e command injection — malicious SQL files can execute system commands\n"
                "3. M-05: escape_sql_literal incomplete (single quote only, no null byte)\n"
                "4. M-13: SQL injection in collect_source_object_stats.py (string interpolation)\n"
                "5. M-06: No file locking — concurrent execution corrupts files\n\n"
                "CREDENTIAL FLOW:\n"
                "config.ini (plaintext) → oracledb.connect() (safe) → obclient -p (UNSAFE) → dbcat/SQLcl CLI (UNSAFE)\n\n"
                "SUBPROCESS SURFACES:\n"
                "obclient, dbcat (JDK), SQLcl — all use subprocess.run(shell=False) but credentials in args\n\n"
                "OUTPUT: 所有输出使用中文。按攻击面分类: 凭据泄露、命令注入、文件操作、供应链。"
                "每个漏洞给出利用条件和 secure-by-default 修复方案。"
            ),
        ),
        # ── 10. Principal Migration Specialist ──────────────────────────
        ExpertRole(
            role_id="principal_migration_specialist",
            name="Principal Migration Specialist",
            focus="End-to-end Oracle→OceanBase migration patterns, compatibility matrix, unsupported feature handling.",
            output_path="audit/swarm/migration_report.md",
            instructions=(
                "You are the Principal Migration Specialist for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "SUPPORTED OBJECT TYPES:\n"
                "TABLE, VIEW, INDEX, CONSTRAINT, TRIGGER, SEQUENCE, PROCEDURE, FUNCTION, "
                "PACKAGE, PACKAGE BODY, TYPE, TYPE BODY, SYNONYM\n\n"
                "BLACKLIST CATEGORIES (blacklist_rules.json):\n"
                "IOT tables, temporary tables, external tables, DB links, materialized views (partial)\n"
                "Known bug H-08: IOT_TABLES rule uses wrong black_type='DBLINK'\n\n"
                "REMAP PATTERNS:\n"
                "- Schema remap (source schema → target schema)\n"
                "- Object rename (source name → target name)\n"
                "- Cross-schema dependency resolution\n"
                "- NO_INFER_SCHEMA_TYPES: VIEW, TRIGGER, PACKAGE keep source schema\n\n"
                "GRANT MIGRATION:\n"
                "- init_users_roles.py for users/roles\n"
                "- Auto-grant in run_fixup.py for object privileges\n\n"
                "DDL COMPATIBILITY:\n"
                "Oracle-specific syntax removed: STORAGE, TABLESPACE, hints, PRAGMA AUTONOMOUS_TRANSACTION\n"
                "Known issues: Q-quote not handled (C-01), EXCEPTION_INIT wrongly removed (C-02)\n\n"
                "OUTPUT: 所有输出使用中文。提供兼容性矩阵和替代方案。对不支持特性给出迁移 workaround。"
            ),
        ),
        # ── 11. Principal Report Analyst ────────────────────────────────
        ExpertRole(
            role_id="principal_report_analyst",
            name="Principal Report Analyst",
            focus="Report DB structure, statistics caliber, query templates, analytic views.",
            output_path="audit/swarm/report_analysis_report.md",
            instructions=(
                "You are the Principal Report Analyst for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "REPORT DB STRUCTURE (save_report_to_db):\n"
                "15+ tables: DIFF_REPORT_SUMMARY, DIFF_REPORT_DETAIL, DIFF_REPORT_COUNTS, "
                "DIFF_REPORT_GRANT, DIFF_REPORT_USABILITY, DIFF_REPORT_VIEW_CHAIN, "
                "DIFF_REPORT_DEPENDENCY, DIFF_REPORT_ARTIFACT, DIFF_REPORT_ARTIFACT_LINE, "
                "DIFF_REPORT_OBJECT_MAPPING, DIFF_REPORT_BLACKLIST, DIFF_REPORT_FIXUP_SKIP, etc.\n\n"
                "ANALYTIC VIEWS (6):\n"
                "DIFF_REPORT_ACTIONS_V, DIFF_REPORT_OBJECT_PROFILE_V, DIFF_REPORT_TRENDS_V, "
                "DIFF_REPORT_PENDING_ACTIONS_V, DIFF_REPORT_GRANT_CLASS_V, DIFF_REPORT_USABILITY_CLASS_V\n\n"
                "STATISTICS CALIBER (known bug — audit/trigger_statistics_root_cause_analysis.md):\n"
                "compute_object_counts → reconcile_object_counts_summary → build_missing_breakdown_counts\n"
                "Problem: extra_blocked_counts uses pre-filter data, reconcile uses post-filter data.\n"
                "Required invariant: 应校验 = 命中 + 缺失 + 不可修补 + 排除\n\n"
                "QUERY TEMPLATES: 77 SQL queries in HOW_TO_READ_REPORTS_IN_OB playbook\n"
                "Scope levels: summary, core, full\n\n"
                "OUTPUT: 所有输出使用中文。审查统计口径一致性。验证恒等式 A=B+C+D+E。提供查询优化建议。"
            ),
        ),
        # ── 12. Principal DevOps Engineer ───────────────────────────────
        ExpertRole(
            role_id="principal_devops_engineer",
            name="Principal DevOps Engineer",
            focus="Deployment, external dependency management, CI/CD, environment configuration.",
            output_path="audit/swarm/devops_report.md",
            instructions=(
                "You are the Principal DevOps Engineer for OceanBase Comparator Toolkit v0.9.8.4.\n\n"
                "EXTERNAL DEPENDENCIES:\n"
                "- Python 3.8+ with oracledb, rich\n"
                "- Oracle Instant Client 19c+ (oracledb Thick Mode)\n"
                "- obclient (OceanBase CLI)\n"
                "- JDK + dbcat (DDL batch extraction)\n"
                "- SQLcl (optional, DDL formatting)\n\n"
                "SUBPROCESS PATTERNS:\n"
                "- obclient: run_fixup.py, init_users_roles.py, init_test.py\n"
                "- dbcat: schema_diff_reconciler.py (fetch_dbcat_schema_objects)\n"
                "- SQLcl: schema_diff_reconciler.py (oracle_get_ddl_batch)\n\n"
                "CONFIGURATION:\n"
                "- config.ini: plaintext credentials, 100+ settings\n"
                "- config.ini.template: shipped template\n"
                "- blacklist_rules.json: shipped with tool\n"
                "- exclude_objects.txt: user-defined exclusions\n\n"
                "CURRENT GAPS:\n"
                "- No CI/CD pipeline\n"
                "- No Docker/container support\n"
                "- No health check or connectivity validation\n"
                "- No version management for external tools\n"
                "- Credentials in plaintext config and CLI args (H-01)\n\n"
                "OUTPUT: 所有输出使用中文。提供 CI/CD 管道设计、容器化方案、外部依赖风险评估。"
            ),
        ),
    ]


def build_subteams() -> List[SubTeam]:
    return [
        SubTeam(
            team_id="delivery_architecture_team",
            name="Delivery & Architecture Team",
            focus="Architecture decomposition, implementation strategy, scalability, deployment, and design patterns.",
            output_path="audit/swarm/subteam_delivery_architecture_summary.md",
            member_role_ids=(
                "chief_architect",
                "principal_programming_expert",
                "principal_software_designer",
                "principal_performance_expert",
                "principal_devops_engineer",
            ),
            coordinator_instructions=(
                "Unify architecture/programming/design/performance/devops findings into one actionable plan. "
                "Prioritize implementation slices by ROI. Resolve conflicts between decomposition roadmap "
                "and backward compatibility. Produce a phased execution plan with rollback boundaries. "
                "所有输出使用中文。"
            ),
        ),
        SubTeam(
            team_id="risk_control_team",
            name="Risk Control Team",
            focus="Correctness, migration safety, security posture, and testing confidence.",
            output_path="audit/swarm/subteam_risk_control_summary.md",
            member_role_ids=(
                "principal_code_reviewer",
                "principal_database_expert",
                "principal_security_expert",
                "principal_test_expert",
            ),
            coordinator_instructions=(
                "Unify correctness/database/security/testing findings into a risk register. "
                "Classify blockers vs non-blockers and define release gates. "
                "Cross-reference known 51 issues (audit/deep_code_review_2025.md) with new findings. "
                "所有输出使用中文。"
            ),
        ),
        SubTeam(
            team_id="user_experience_team",
            name="User Experience Team",
            focus="User-facing quality: report clarity, migration workflow, statistics accuracy, and product completeness.",
            output_path="audit/swarm/subteam_user_experience_summary.md",
            member_role_ids=(
                "principal_product_manager",
                "principal_migration_specialist",
                "principal_report_analyst",
            ),
            coordinator_instructions=(
                "Unify product/migration/report findings into a user experience improvement plan. "
                "Prioritize by user impact: statistics caliber bugs > report confusion > workflow gaps. "
                "Cross-reference audit/trigger_statistics_root_cause_analysis.md for known caliber issues. "
                "Propose redesigned report panel with mathematical invariant A=B+C+D+E. "
                "所有输出使用中文。"
            ),
        ),
    ]


def build_arbiter() -> Dict[str, str]:
    return {
        "role_id": "swarm_arbiter",
        "name": "Swarm Arbiter",
        "output_path": "audit/swarm/consolidated_report.md",
        "instructions": (
            "You are the final arbiter for the OceanBase Comparator Toolkit expert swarm.\n\n"
            "You receive summaries from 3 subteams:\n"
            "1. Delivery & Architecture Team (architect, programmer, designer, performance, devops)\n"
            "2. Risk Control Team (code reviewer, database expert, security, test)\n"
            "3. User Experience Team (product manager, migration specialist, report analyst)\n\n"
            "YOUR TASKS:\n"
            "1. Resolve conflicts between subteam conclusions\n"
            "2. Rank top risks by severity (CRITICAL > HIGH > MEDIUM > LOW)\n"
            "3. Cross-reference findings across teams (e.g., architecture debt causing UX issues)\n"
            "4. Define go/no-go criteria for next release\n"
            "5. Produce a staged execution plan with owners, dependencies, and rollback points\n\n"
            "KNOWN BASELINES:\n"
            "- 51 issues from audit/deep_code_review_2025.md\n"
            "- Statistics caliber bugs from audit/trigger_statistics_root_cause_analysis.md\n"
            "- VIEW column prefix bug from audit/view_column_schema_prefix_bug.md\n\n"
            "OUTPUT: 所有输出使用中文。按优先级 P0/P1/P2/P3 组织执行计划。"
            "每个行动项包含: 负责团队、预估工作量、依赖关系、风险等级。"
        ),
    }


def build_default_task_brief() -> str:
    return (
        "# Comparator Expert Swarm Task\n\n"
        "## Objective\n"
        "Run a cross-discipline quality, migration-risk, and user-experience review for this repository.\n\n"
        "## Scope\n"
        "- schema_diff_reconciler.py (~33K lines, core orchestrator)\n"
        "- run_fixup.py (~4.3K lines, fixup executor)\n"
        "- init_users_roles.py (~800 lines, user/role init)\n"
        "- config.ini.template, readme_config.txt\n"
        "- blacklist_rules.json\n"
        "- test suite (test_schema_diff_reconciler.py, test_run_fixup.py, test_init_users_roles.py)\n"
        "- audit/ reports (deep_code_review_2025.md, trigger_statistics_root_cause_analysis.md, "
        "view_column_schema_prefix_bug.md)\n"
        "- openspec/ specs and changes\n\n"
        "## Known Issue Baselines\n"
        "- 51 issues in audit/deep_code_review_2025.md (5 CRITICAL, 14 HIGH, 22 MEDIUM, 10 LOW)\n"
        "- Statistics caliber inconsistency in audit/trigger_statistics_root_cause_analysis.md\n"
        "- VIEW column schema prefix bug in audit/view_column_schema_prefix_bug.md\n\n"
        "## Required Outcomes\n"
        "1. High-severity correctness and migration risks (beyond known 51).\n"
        "2. Architectural debt map with phased remediation steps.\n"
        "3. Database compatibility and remap-risk checklist.\n"
        "4. Test gap matrix tied to proposed code changes.\n"
        "5. Software design improvement proposals (extensibility, configuration).\n"
        "6. User experience audit (report clarity, statistics caliber, error messaging).\n"
        "7. Migration workflow completeness assessment.\n"
        "8. Report DB and statistics caliber verification.\n"
        "9. Security posture assessment.\n"
        "10. DevOps and deployment readiness evaluation.\n"
        "11. Consolidated execution plan with owners, priorities, and dependencies.\n\n"
        "## Output Language\n"
        "All reports must be written in Chinese (中文).\n"
    )


def build_swarm_blueprint(project_root: Optional[Path] = None) -> Dict[str, Any]:
    root = (project_root or Path.cwd()).resolve()
    roles = [asdict(role) for role in build_expert_roles()]
    subteams = []
    for team in build_subteams():
        payload = asdict(team)
        payload["member_role_ids"] = list(team.member_role_ids)
        subteams.append(payload)
    role_ids = [role["role_id"] for role in roles]
    return {
        "project": "comparator",
        "project_root": str(root),
        "output_dir": str((root / SWARM_OUTPUT_DIR).resolve()),
        "execution_pattern": "parallel_subteams_with_arbiter",
        "primary_paths": [
            "schema_diff_reconciler.py",
            "run_fixup.py",
            "init_users_roles.py",
            "config.ini.template",
            "readme_config.txt",
            "blacklist_rules.json",
            "test_schema_diff_reconciler.py",
            "test_run_fixup.py",
            "test_init_users_roles.py",
            "audit/deep_code_review_2025.md",
            "audit/trigger_statistics_root_cause_analysis.md",
            "audit/view_column_schema_prefix_bug.md",
        ],
        "guardrails": build_guardrails(),
        "roles": roles,
        "subteams": subteams,
        "arbiter": build_arbiter(),
        "role_ids": role_ids,
        "handoff_order": [team["team_id"] for team in subteams] + ["swarm_arbiter"],
    }


def write_swarm_blueprint(output_path: Path, blueprint: Dict[str, Any]) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(blueprint, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return output_path


def write_task_brief(output_path: Path, task_text: str) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(task_text.strip() + "\n", encoding="utf-8")
    return output_path


def _load_task_text(task: Optional[str], task_file: Optional[str]) -> str:
    if task and task_file:
        raise ValueError("--task and --task-file cannot be used together.")
    if task:
        return task.strip()
    if task_file:
        return Path(task_file).read_text(encoding="utf-8").strip()
    return build_default_task_brief().strip()


def _load_claude_sdk() -> Any:
    try:
        from claude_agent_sdk import query
    except ImportError as exc:
        raise RuntimeError(
            "Missing optional dependencies for --execute. "
            "Install with: .venv/bin/python -m pip install --upgrade claude-agent-sdk"
        ) from exc
    return query


def _validate_subteam_memberships(blueprint: Dict[str, Any]) -> None:
    role_ids = [role["role_id"] for role in blueprint["roles"]]
    declared = []
    for team in blueprint["subteams"]:
        declared.extend(team["member_role_ids"])

    unknown = sorted(set(declared) - set(role_ids))
    if unknown:
        raise ValueError(f"Subteam references unknown role IDs: {unknown}")

    missing = sorted(set(role_ids) - set(declared))
    if missing:
        raise ValueError(f"Roles missing from subteams: {missing}")

    counts = Counter(declared)
    duplicates = sorted(role_id for role_id, n in counts.items() if n > 1)
    if duplicates:
        raise ValueError(f"Roles assigned to multiple subteams: {duplicates}")


def _format_guardrails(guardrails: Sequence[str]) -> str:
    return "Guardrails:\n- " + "\n- ".join(guardrails)


def _join_stream_messages(messages: Sequence[str]) -> str:
    return "\n\n".join(str(message) for message in messages if str(message).strip())


def _exception_summary(error: Exception) -> str:
    return str(error) or error.__class__.__name__


async def _run_role_agent(
    *,
    role: Dict[str, Any],
    subteam_name: str,
    task_text: str,
    blueprint: Dict[str, Any],
    model: str,
    max_turns: int,
    query_fn: Any,
) -> Dict[str, str]:
    system_prompt = (
        "You are a specialist in your assigned domain. "
        f"{role['instructions']} "
        f"You must write your final report into `{role['output_path']}`. "
        "Use the Write tool to create files. Do not edit unrelated files."
    )
    role_prompt = (
        f"Project root: {blueprint['project_root']}\n"
        f"Subteam: {subteam_name}\n"
        f"Role: {role['name']}\n"
        f"Focus: {role['focus']}\n"
        f"Task:\n{task_text}\n\n"
        f"{_format_guardrails(blueprint['guardrails'])}"
    )

    outputs: List[str] = []
    async for message in query_fn(
        prompt=role_prompt,
        system_prompt=system_prompt,
        model=model,
        max_turns=max_turns,
    ):
        outputs.append(str(message))

    return {
        "role_id": role["role_id"],
        "name": role["name"],
        "output_path": role["output_path"],
        "summary": _join_stream_messages(outputs),
    }


async def _run_subteam(
    *,
    subteam: Dict[str, Any],
    role_map: Dict[str, Dict[str, Any]],
    task_text: str,
    blueprint: Dict[str, Any],
    model: str,
    max_turns: int,
    query_fn: Any,
) -> Dict[str, Any]:
    member_roles = [role_map[role_id] for role_id in subteam["member_role_ids"]]
    member_tasks = [
        _run_role_agent(
            role=role,
            subteam_name=subteam["name"],
            task_text=task_text,
            blueprint=blueprint,
            model=model,
            max_turns=max_turns,
            query_fn=query_fn,
        )
        for role in member_roles
    ]
    raw_member_results = await asyncio.gather(*member_tasks, return_exceptions=True)
    member_summaries: List[Dict[str, Any]] = []
    for role, result in zip(member_roles, raw_member_results):
        if isinstance(result, Exception):
            member_summaries.append(
                {
                    "role_id": role["role_id"],
                    "name": role["name"],
                    "output_path": role["output_path"],
                    "summary": "",
                    "error": _exception_summary(result),
                }
            )
        else:
            member_summaries.append(result)

    system_prompt = (
        "You are the subteam lead. "
        f"{subteam['coordinator_instructions']} "
        f"You must write the subteam summary to `{subteam['output_path']}`. "
        "Use the Write tool to create files."
    )
    subteam_prompt = (
        f"Project root: {blueprint['project_root']}\n"
        f"Subteam: {subteam['name']} ({subteam['team_id']})\n"
        f"Subteam focus: {subteam['focus']}\n"
        f"Task:\n{task_text}\n\n"
        "Member summaries:\n"
        f"{json.dumps(member_summaries, indent=2, ensure_ascii=False)}\n\n"
        f"{_format_guardrails(blueprint['guardrails'])}"
    )

    outputs: List[str] = []
    async for message in query_fn(
        prompt=subteam_prompt,
        system_prompt=system_prompt,
        model=model,
        max_turns=max_turns,
    ):
        outputs.append(str(message))

    return {
        "team_id": subteam["team_id"],
        "name": subteam["name"],
        "focus": subteam["focus"],
        "output_path": subteam["output_path"],
        "member_summaries": member_summaries,
        "summary": _join_stream_messages(outputs),
    }


async def _run_arbiter(
    *,
    arbiter: Dict[str, Any],
    subteam_summaries: List[Dict[str, Any]],
    task_text: str,
    blueprint: Dict[str, Any],
    model: str,
    max_turns: int,
    query_fn: Any,
) -> str:
    system_prompt = (
        "You are the final arbiter. "
        f"{arbiter['instructions']} "
        f"You must write the final arbitration report to `{arbiter['output_path']}`. "
        "Use the Write tool to create files."
    )
    arbiter_prompt = (
        f"Project root: {blueprint['project_root']}\n"
        "Task:\n"
        f"{task_text}\n\n"
        "Subteam summaries:\n"
        f"{json.dumps(subteam_summaries, indent=2, ensure_ascii=False)}\n\n"
        f"{_format_guardrails(blueprint['guardrails'])}"
    )

    outputs: List[str] = []
    async for message in query_fn(
        prompt=arbiter_prompt,
        system_prompt=system_prompt,
        model=model,
        max_turns=max_turns,
    ):
        outputs.append(str(message))

    return _join_stream_messages(outputs)


async def execute_swarm(
    blueprint: Dict[str, Any],
    task_text: str,
    model: str,
    max_turns: int,
) -> str:
    query_fn = _load_claude_sdk()
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is required for --execute mode.")
    _validate_subteam_memberships(blueprint)

    role_map = {role["role_id"]: role for role in blueprint["roles"]}
    subteam_tasks = [
        _run_subteam(
            subteam=subteam,
            role_map=role_map,
            task_text=task_text,
            blueprint=blueprint,
            model=model,
            max_turns=max_turns,
            query_fn=query_fn,
        )
        for subteam in blueprint["subteams"]
    ]
    raw_subteam_results = await asyncio.gather(*subteam_tasks, return_exceptions=True)
    subteam_summaries: List[Dict[str, Any]] = []
    for subteam, result in zip(blueprint["subteams"], raw_subteam_results):
        if isinstance(result, Exception):
            subteam_summaries.append(
                {
                    "team_id": subteam["team_id"],
                    "name": subteam["name"],
                    "focus": subteam["focus"],
                    "output_path": subteam["output_path"],
                    "summary": "",
                    "error": _exception_summary(result),
                }
            )
        else:
            subteam_summaries.append(result)

    return await _run_arbiter(
        arbiter=blueprint["arbiter"],
        subteam_summaries=subteam_summaries,
        task_text=task_text,
        blueprint=blueprint,
        model=model,
        max_turns=max_turns,
        query_fn=query_fn,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate or execute comparator expert swarm.")
    parser.add_argument(
        "--output-blueprint",
        default=str(DEFAULT_BLUEPRINT_PATH),
        help="Path for generated swarm blueprint JSON.",
    )
    parser.add_argument(
        "--output-task",
        default=str(DEFAULT_TASK_PATH),
        help="Path for generated swarm task markdown.",
    )
    parser.add_argument(
        "--task",
        default=None,
        help="Inline task text; overrides default task brief.",
    )
    parser.add_argument(
        "--task-file",
        default=None,
        help="Path to task markdown/text file; mutually exclusive with --task.",
    )
    parser.add_argument(
        "--print-blueprint",
        action="store_true",
        help="Print blueprint JSON to stdout.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute parallel subteams + final arbiter via Claude Agent SDK.",
    )
    parser.add_argument(
        "--model",
        default="claude-opus-4-6",
        help="Model for execute mode.",
    )
    parser.add_argument(
        "--max-turns",
        type=int,
        default=20,
        help="Max turns per role in execute mode.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    task_text = _load_task_text(args.task, args.task_file)
    blueprint = build_swarm_blueprint(project_root=Path.cwd())

    blueprint_path = write_swarm_blueprint(Path(args.output_blueprint), blueprint)
    task_path = write_task_brief(Path(args.output_task), task_text)

    print(f"Swarm blueprint written: {blueprint_path}")
    print(f"Swarm task written: {task_path}")

    if args.print_blueprint:
        print(json.dumps(blueprint, indent=2, ensure_ascii=False))

    if args.execute:
        final_summary = asyncio.run(
            execute_swarm(
                blueprint=blueprint,
                task_text=task_text,
                model=args.model,
                max_turns=args.max_turns,
            )
        )
        print("Swarm execution completed.")
        if final_summary:
            print(final_summary)
    else:
        print("Run with --execute to start parallel subteams + arbiter.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
