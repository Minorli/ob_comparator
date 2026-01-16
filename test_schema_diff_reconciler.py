import unittest
import sys
import types
import tempfile
import json
from pathlib import Path

# schema_diff_reconciler 在 import 时强依赖 oracledb；
# 单元测试只覆盖纯函数，因此用 dummy 模块兜底，避免环境未安装时退出。
try:  # pragma: no cover
    import oracledb  # noqa: F401
except ImportError:  # pragma: no cover
    sys.modules["oracledb"] = types.ModuleType("oracledb")

import schema_diff_reconciler as sdr


class TestSchemaDiffReconcilerPureFunctions(unittest.TestCase):
    def test_infer_dominant_schema_tables_only(self):
        remap_rules = {
            "A.T1": "X.T1",
            "A.V1": "Y.V1",
        }
        source_objects = {
            "A.T1": {"TABLE"},
            "A.V1": {"VIEW"},
        }
        dominant = sdr.infer_dominant_schema_from_rules(remap_rules, "A", source_objects)
        self.assertEqual(dominant, "X")

    def test_infer_dominant_schema_counts_implicit_tables(self):
        remap_rules = {
            "A.T1": "B.T1",
        }
        source_objects = {
            "A.T1": {"TABLE"},
            "A.T2": {"TABLE"},
            "A.T3": {"TABLE"},
        }
        dominant = sdr.infer_dominant_schema_from_rules(remap_rules, "A", source_objects)
        self.assertEqual(dominant, "A")

    def test_resolve_remap_target_parent_table_uses_schema_mapping(self):
        remap_rules = {}
        schema_mapping = {"A": "B"}
        object_parent_map = {"A.IDX1": "A.T1"}
        target = sdr.resolve_remap_target(
            "A.IDX1",
            "INDEX",
            remap_rules,
            schema_mapping=schema_mapping,
            object_parent_map=object_parent_map,
        )
        self.assertEqual(target, "B.IDX1")

    def test_resolve_remap_target_type_body_explicit_rule(self):
        remap_rules = {"A.TY BODY": "B.TY BODY"}
        target = sdr.resolve_remap_target("A.TY", "TYPE BODY", remap_rules)
        self.assertEqual(target, "B.TY")

    def test_sequence_follows_remapped_dependents(self):
        remap_rules = {"A.T1": "B.T1"}
        source_objects = {
            "A.SEQ1": {"SEQUENCE"},
            "A.T1": {"TABLE"},
            "A.TRG1": {"TRIGGER"},
        }
        deps = {
            ("A", "TRG1", "TRIGGER", "A", "SEQ1", "SEQUENCE"),
        }
        graph = sdr.build_dependency_graph(deps)
        object_parent_map = {"A.TRG1": "A.T1"}
        target = sdr.resolve_remap_target(
            "A.SEQ1",
            "SEQUENCE",
            remap_rules,
            source_objects=source_objects,
            schema_mapping={"A": "B"},
            object_parent_map=object_parent_map,
            dependency_graph=graph,
            source_dependencies=deps,
            sequence_remap_policy="infer",
        )
        self.assertEqual(target, "B.SEQ1")

    def test_trigger_keeps_source_schema_without_explicit_remap(self):
        remap_rules = {"A.T1": "B.T1"}
        object_parent_map = {"A.TRG1": "A.T1"}
        target = sdr.resolve_remap_target(
            "A.TRG1",
            "TRIGGER",
            remap_rules,
            schema_mapping={"A": "B"},
            object_parent_map=object_parent_map,
        )
        self.assertEqual(target, "A.TRG1")

    def test_view_keeps_source_schema_without_explicit_remap(self):
        remap_rules = {"A.T1": "B.T1"}
        deps = {
            ("A", "V1", "VIEW", "A", "T1", "TABLE"),
        }
        target = sdr.resolve_remap_target(
            "A.V1",
            "VIEW",
            remap_rules,
            source_dependencies=deps,
            schema_mapping={"A": "B"},
        )
        self.assertEqual(target, "A.V1")

    def test_build_full_object_mapping_respects_enabled_types(self):
        source_objects = {
            "A.T1": {"TABLE"},
            "A.P1": {"PACKAGE"},
        }
        remap_rules = {"A.T1": "B.T1", "A.P1": "B.P1"}
        mapping = sdr.build_full_object_mapping(
            source_objects,
            remap_rules,
            enabled_types={"TABLE"}
        )
        self.assertIn("A.T1", mapping)
        self.assertNotIn("A.P1", mapping)

    def test_check_primary_objects_print_only_types(self):
        master_list = [
            ("A.MV1", "A.MV1", "MATERIALIZED VIEW"),
            ("A.P1", "A.P1", "PACKAGE"),
        ]
        ob_meta = sdr.ObMetadata(
            objects_by_type={
                "MATERIALIZED VIEW": set(),
                "PACKAGE": set()
            },
            tab_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}
        )
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[],
            sys_privileges=[],
            role_privileges=[],
            role_metadata={},
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"MATERIALIZED VIEW", "PACKAGE"},
            print_only_types={"MATERIALIZED VIEW"}
        )
        self.assertEqual(len(results["skipped"]), 1)
        self.assertEqual(results["missing"], [])

    def test_supplement_missing_views_from_mapping(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        full_mapping = {
            "A.V1": {"VIEW": "A.V1"},
            "A.V2": {"VIEW": "A.V2"},
        }
        ob_meta = sdr.ObMetadata(
            objects_by_type={"VIEW": {"A.V1"}},
            tab_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}
        )
        added = sdr.supplement_missing_views_from_mapping(
            tv_results,
            full_mapping,
            ob_meta,
            enabled_primary_types={"VIEW"}
        )
        self.assertEqual(added, 1)
        self.assertIn(("VIEW", "A.V2", "A.V2"), tv_results["missing"])

    def test_clean_end_schema_prefix_single_line(self):
        ddl = "BEGIN NULL; END SCHEMA.PKG_TEST;"
        cleaned = sdr.clean_end_schema_prefix(ddl)
        self.assertIn("END PKG_TEST;", cleaned)
        self.assertNotIn("END SCHEMA.PKG_TEST", cleaned)

    def test_clean_for_loop_single_dot_range(self):
        ddl = "BEGIN FOR i IN 1.v_tablen LOOP NULL; END LOOP; END;"
        cleaned = sdr.clean_for_loop_single_dot_range(ddl)
        self.assertIn("IN 1..v_tablen", cleaned)
        ddl = "BEGIN FOR i IN 0.v_tablen LOOP NULL; END LOOP; END;"
        cleaned = sdr.clean_for_loop_single_dot_range(ddl)
        self.assertIn("IN 0..v_tablen", cleaned)
        ddl = "BEGIN FOR i IN 7.v_tablen LOOP NULL; END LOOP; END;"
        cleaned = sdr.clean_for_loop_single_dot_range(ddl)
        self.assertIn("IN 7..v_tablen", cleaned)

    def test_normalize_synonym_fixup_scope(self):
        self.assertEqual(sdr.normalize_synonym_fixup_scope(None), "all")
        self.assertEqual(sdr.normalize_synonym_fixup_scope("all"), "all")
        self.assertEqual(sdr.normalize_synonym_fixup_scope("public"), "public_only")
        self.assertEqual(sdr.normalize_synonym_fixup_scope("PUBLIC_ONLY"), "public_only")

    def test_normalize_sequence_remap_policy(self):
        self.assertEqual(sdr.normalize_sequence_remap_policy(None), "source_only")
        self.assertEqual(sdr.normalize_sequence_remap_policy("infer"), "infer")
        self.assertEqual(sdr.normalize_sequence_remap_policy("SOURCE"), "source_only")
        self.assertEqual(sdr.normalize_sequence_remap_policy("dominant"), "dominant_table")

    def test_normalize_report_dir_layout(self):
        self.assertEqual(sdr.normalize_report_dir_layout(None), "per_run")
        self.assertEqual(sdr.normalize_report_dir_layout("flat"), "flat")
        self.assertEqual(sdr.normalize_report_dir_layout("per-run"), "per_run")

    def test_remap_trigger_object_references(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg1 BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  INSERT INTO t2(col) VALUES (1);\n"
            "  INSERT INTO src.t2(col) VALUES (2);\n"
            "  :new.id := seq1.NEXTVAL;\n"
            "  :new.id2 := src.seq1 . NEXTVAL;\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "SRC.T2": {"TABLE": "TGT.T2"},
            "SRC.SEQ1": {"SEQUENCE": "TGT.SEQ1"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG1",
            on_target=("TGT", "T1"),
            qualify_schema=True
        )
        self.assertIn("CREATE OR REPLACE TRIGGER TGT.TRG1", remapped)
        self.assertIn("ON TGT.T1", remapped)
        self.assertIn("INSERT INTO TGT.T2", remapped)
        self.assertIn("TGT.SEQ1.NEXTVAL", remapped)
        self.assertNotIn("TGT.TGT", remapped)

    def test_compare_package_objects_source_invalid_and_target_invalid(self):
        master_list = [
            ("A.P1", "A.P1", "PACKAGE"),
            ("A.P2", "A.P2", "PACKAGE BODY"),
        ]
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[],
            sys_privileges=[],
            role_privileges=[],
            role_metadata={},
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={
                ("A", "P1", "PACKAGE"): "INVALID",
                ("A", "P2", "PACKAGE BODY"): "VALID",
            },
            package_errors={
                ("A", "P1", "PACKAGE"): sdr.PackageErrorInfo(2, "L1:1 bad"),
            },
            package_errors_complete=True, partition_key_columns={}, interval_partitions={}
        )
        ob_meta = sdr.ObMetadata(
            objects_by_type={
                "PACKAGE": set(),
                "PACKAGE BODY": {"A.P2"}
            },
            tab_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={
                ("A", "P2", "PACKAGE BODY"): "INVALID"
            },
            package_errors={
                ("A", "P2", "PACKAGE BODY"): sdr.PackageErrorInfo(1, "L2:5 bad"),
            },
            package_errors_complete=True, partition_key_columns={}
        )
        results = sdr.compare_package_objects(master_list, oracle_meta, ob_meta, enabled_primary_types={"PACKAGE", "PACKAGE BODY"})
        summary = results["summary"]
        self.assertEqual(summary.get("SOURCE_INVALID"), 1)
        self.assertEqual(summary.get("TARGET_INVALID"), 1)
        self.assertEqual(len(results["diff_rows"]), 2)

    def test_export_package_compare_report(self):
        rows = [
            sdr.PackageCompareRow(
                src_full="A.P1",
                obj_type="PACKAGE",
                src_status="VALID",
                tgt_full="A.P1",
                tgt_status="INVALID",
                result="TARGET_INVALID",
                error_count=1,
                first_error="L1:1 bad"
            )
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "package_compare_test.txt"
            result = sdr.export_package_compare_report(rows, output_path)
            self.assertEqual(result, output_path)
            content = output_path.read_text(encoding="utf-8")
            self.assertIn("PACKAGE/PACKAGE BODY", content)
            self.assertIn("A.P1", content)

    def test_export_package_compare_report_groups_by_owner_and_name(self):
        rows = [
            sdr.PackageCompareRow(
                src_full="B.P1",
                obj_type="PACKAGE",
                src_status="VALID",
                tgt_full="B.P1",
                tgt_status="VALID",
                result="OK",
                error_count=0,
                first_error=""
            ),
            sdr.PackageCompareRow(
                src_full="A.P1",
                obj_type="PACKAGE BODY",
                src_status="VALID",
                tgt_full="A.P1",
                tgt_status="VALID",
                result="OK",
                error_count=0,
                first_error=""
            ),
            sdr.PackageCompareRow(
                src_full="A.P1",
                obj_type="PACKAGE",
                src_status="VALID",
                tgt_full="A.P1",
                tgt_status="VALID",
                result="OK",
                error_count=0,
                first_error=""
            ),
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "package_compare_test.txt"
            sdr.export_package_compare_report(rows, output_path)
            lines = [
                line for line in output_path.read_text(encoding="utf-8").splitlines()
                if line and not line.startswith("#") and not line.startswith("SRC_FULL") and set(line.strip("-"))
            ]
            idx_pkg = idx_body = idx_b = None
            for idx, line in enumerate(lines):
                if "A.P1" in line and "PACKAGE BODY" in line:
                    idx_body = idx
                elif "A.P1" in line and "PACKAGE" in line:
                    idx_pkg = idx
                elif "B.P1" in line and "PACKAGE" in line:
                    idx_b = idx
            self.assertIsNotNone(idx_pkg)
            self.assertIsNotNone(idx_body)
            self.assertIsNotNone(idx_b)
            self.assertLess(idx_pkg, idx_body)
            self.assertLess(idx_body, idx_b)

    def test_print_final_report_includes_package_section(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        extra_results = {
            "index_ok": [],
            "index_mismatched": [],
            "constraint_ok": [],
            "constraint_mismatched": [],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }
        comment_results = {
            "ok": [],
            "mismatched": [],
            "skipped_reason": "skip"
        }
        dependency_report = {
            "missing": [],
            "unexpected": [],
            "skipped": []
        }
        package_rows = [
            sdr.PackageCompareRow(
                src_full="A.P1",
                obj_type="PACKAGE",
                src_status="VALID",
                tgt_full="A.P1",
                tgt_status="INVALID",
                result="TARGET_INVALID",
                error_count=1,
                first_error="L1:1 bad"
            )
        ]
        package_results = {
            "rows": package_rows,
            "diff_rows": package_rows,
            "summary": {
                "TARGET_INVALID": 1
            }
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            report_path = Path(tmp_dir) / "report_20240101.txt"
            sdr.print_final_report(
                tv_results,
                total_checked=0,
                extra_results=extra_results,
                comment_results=comment_results,
                dependency_report=dependency_report,
                report_file=report_path,
                object_counts_summary=None,
                endpoint_info=None,
                schema_summary=None,
                settings={"report_width": 120, "report_detail_mode": "full"},
                blacklisted_missing_tables={},
                blacklist_report_rows=[],
                trigger_list_summary=None,
                trigger_list_rows=None,
                package_results=package_results,
                run_summary_ctx=None,
                filtered_grants=None
            )
            content = report_path.read_text(encoding="utf-8")
            self.assertIn("PACKAGE/PKG BODY", content)
            self.assertIn("目标无效", content)
            package_report_path = Path(tmp_dir) / "package_compare_20240101.txt"
            self.assertTrue(package_report_path.exists())

    def test_generate_fixup_scripts_outputs_package(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        extra_results = {
            "index_ok": [],
            "index_mismatched": [],
            "constraint_ok": [],
            "constraint_mismatched": [],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }
        master_list = [("A.P1", "A.P1", "PACKAGE")]
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[],
            sys_privileges=[],
            role_privileges=[],
            role_metadata={},
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        ob_meta = sdr.ObMetadata(
            objects_by_type={"PACKAGE": set(), "PACKAGE BODY": set()},
            tab_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}
        )
        full_mapping = {"A.P1": {"PACKAGE": "A.P1"}}
        package_results = {
            "rows": [
                sdr.PackageCompareRow(
                    src_full="A.P1",
                    obj_type="PACKAGE",
                    src_status="VALID",
                    tgt_full="A.P1",
                    tgt_status="MISSING",
                    result="MISSING_TARGET",
                    error_count=0,
                    first_error=""
                )
            ],
            "diff_rows": [],
            "summary": {"MISSING_TARGET": 1}
        }
        dbcat_data = {"A": {"PACKAGE": {"P1": "CREATE OR REPLACE PACKAGE A.P1 AS BEGIN NULL; END;"}}}
        dbcat_meta = {("A", "PACKAGE", "P1"): ("cache", 0.01)}
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings = {
                "fixup_dir": tmp_dir,
                "fixup_workers": 1,
                "progress_log_interval": 999,
                "fixup_type_set": {"PACKAGE"},
                "fixup_schema_list": set(),
            }
            ora_cfg = {"user": "u", "password": "p", "dsn": "d"}
            ob_cfg = {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"}
            original_fetch = sdr.fetch_dbcat_schema_objects
            original_ver = sdr.get_oceanbase_version
            try:
                sdr.fetch_dbcat_schema_objects = lambda *_args, **_kwargs: (dbcat_data, dbcat_meta)
                sdr.get_oceanbase_version = lambda *_args, **_kwargs: None
                sdr.generate_fixup_scripts(
                    ora_cfg,
                    ob_cfg,
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    full_mapping,
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=package_results,
                    report_dir=None,
                    report_timestamp=None
                )
            finally:
                sdr.fetch_dbcat_schema_objects = original_fetch
                sdr.get_oceanbase_version = original_ver
            package_path = Path(tmp_dir) / "package" / "A.P1.sql"
            self.assertTrue(package_path.exists())

    def test_generate_fixup_scripts_respects_fixup_type_filter(self):
        tv_results = {"missing": [], "mismatched": [], "ok": [], "skipped": [], "extraneous": [], "extra_targets": []}
        extra_results = {
            "index_ok": [], "index_mismatched": [], "constraint_ok": [],
            "constraint_mismatched": [], "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [("A.P1", "A.P1", "PACKAGE")]
        oracle_meta = sdr.OracleMetadata(
            table_columns={}, indexes={}, constraints={}, triggers={}, sequences={},
            table_comments={}, column_comments={}, comments_complete=True,
            blacklist_tables={}, object_privileges=[], sys_privileges=[],
            role_privileges=[], role_metadata={}, system_privilege_map=set(),
            table_privilege_map=set(), object_statuses={}, package_errors={}, package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        ob_meta = sdr.ObMetadata(
            objects_by_type={"PACKAGE": set(), "PACKAGE BODY": set()},
            tab_columns={}, indexes={}, constraints={}, triggers={}, sequences={},
            roles=set(), table_comments={}, column_comments={}, comments_complete=True,
            object_statuses={}, package_errors={}, package_errors_complete=False, partition_key_columns={}
        )
        full_mapping = {"A.P1": {"PACKAGE": "A.P1"}}
        package_results = {
            "rows": [
                sdr.PackageCompareRow(
                    src_full="A.P1",
                    obj_type="PACKAGE",
                    src_status="VALID",
                    tgt_full="A.P1",
                    tgt_status="MISSING",
                    result="MISSING_TARGET",
                    error_count=0,
                    first_error=""
                )
            ],
            "diff_rows": [],
            "summary": {"MISSING_TARGET": 1}
        }
        dbcat_data = {"A": {"PACKAGE": {"P1": "CREATE OR REPLACE PACKAGE A.P1 AS BEGIN NULL; END;"}}}
        dbcat_meta = {("A", "PACKAGE", "P1"): ("cache", 0.01)}
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings = {
                "fixup_dir": tmp_dir,
                "fixup_workers": 1,
                "progress_log_interval": 999,
                "fixup_type_set": {"TABLE"},
                "fixup_schema_list": set(),
            }
            ora_cfg = {"user": "u", "password": "p", "dsn": "d"}
            ob_cfg = {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"}
            original_fetch = sdr.fetch_dbcat_schema_objects
            original_ver = sdr.get_oceanbase_version
            try:
                sdr.fetch_dbcat_schema_objects = lambda *_args, **_kwargs: (dbcat_data, dbcat_meta)
                sdr.get_oceanbase_version = lambda *_args, **_kwargs: None
                sdr.generate_fixup_scripts(
                    ora_cfg,
                    ob_cfg,
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    full_mapping,
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=package_results,
                    report_dir=None,
                    report_timestamp=None
                )
            finally:
                sdr.fetch_dbcat_schema_objects = original_fetch
                sdr.get_oceanbase_version = original_ver
            package_path = Path(tmp_dir) / "package" / "A.P1.sql"
            self.assertFalse(package_path.exists())

    def test_direct_dependency_inference_follows_remapped_reference(self):
        remap_rules = {"A.V1": "B.V1"}
        deps = {
            ("A", "P1", "PROCEDURE", "A", "V1", "VIEW"),
        }
        target = sdr.resolve_remap_target(
            "A.P1",
            "PROCEDURE",
            remap_rules,
            source_dependencies=deps,
        )
        self.assertEqual(target, "B.P1")

    def test_schema_synonym_follows_remapped_reference(self):
        remap_rules = {"A.V1": "B.V1"}
        deps = {
            ("A", "SYN1", "SYNONYM", "A", "V1", "VIEW"),
        }
        target = sdr.resolve_remap_target(
            "A.SYN1",
            "SYNONYM",
            remap_rules,
            source_dependencies=deps,
        )
        self.assertEqual(target, "B.SYN1")

    def test_public_synonym_stays_public(self):
        remap_rules = {"A.T1": "B.T1"}
        deps = {
            ("PUBLIC", "SYN1", "SYNONYM", "A", "T1", "TABLE"),
        }
        target = sdr.resolve_remap_target(
            "PUBLIC.SYN1",
            "SYNONYM",
            remap_rules,
            source_dependencies=deps,
        )
        self.assertEqual(target, "PUBLIC.SYN1")

    def test_find_mapped_target_any_type_deterministic_fallback(self):
        full_mapping = {
            "A.O1": {"VIEW": "Z.O1", "TABLE": "Y.O1"},
        }
        # preferred types miss -> deterministic smallest value
        mapped = sdr.find_mapped_target_any_type(full_mapping, "A.O1", preferred_types=("PACKAGE",))
        self.assertEqual(mapped, "Y.O1")

    def test_split_ddl_statements_ignores_semicolon_in_string(self):
        ddl = "CREATE TABLE T (C VARCHAR2(10) DEFAULT ';');\nALTER TABLE T ADD (D NUMBER);"
        stmts = sdr.split_ddl_statements(ddl)
        self.assertEqual(len(stmts), 2)
        self.assertTrue(stmts[0].upper().startswith("CREATE TABLE"))
        self.assertTrue(stmts[1].upper().startswith("ALTER TABLE"))

    def test_split_ddl_statements_does_not_split_inside_begin_end(self):
        ddl = "BEGIN\n  EXECUTE IMMEDIATE 'ALTER TABLE X ADD Y NUMBER;';\n  NULL;\nEND;"
        stmts = sdr.split_ddl_statements(ddl)
        self.assertEqual(len(stmts), 1)
        self.assertIn("EXECUTE IMMEDIATE", stmts[0].upper())

    def test_parse_trigger_list_file(self):
        content = "\n".join([
            "# comment",
            "A.TR1",
            "A.TR1",
            "B.TR2 # inline comment",
            "INVALID",
            "C.",
            ""
        ])
        with tempfile.NamedTemporaryFile("w+", delete=False) as fp:
            fp.write(content)
            fp.flush()
            path = fp.name
        try:
            entries, invalids, duplicates, total_lines, error = sdr.parse_trigger_list_file(path)
        finally:
            try:
                Path(path).unlink()
            except OSError:
                pass
        self.assertIsNone(error)
        self.assertEqual(entries, {"A.TR1", "B.TR2"})
        self.assertEqual(len(duplicates), 1)
        self.assertEqual(len(invalids), 2)
        self.assertEqual(total_lines, 5)

    def test_filter_missing_grant_entries_grantable(self):
        object_grants = {
            "A": {
                sdr.ObjectGrantEntry("SELECT", "X.T1", False),
                sdr.ObjectGrantEntry("SELECT", "X.T2", True),
            }
        }
        sys_privs = {
            "A": {sdr.SystemGrantEntry("CREATE SESSION", False)}
        }
        role_privs = {
            "A": {sdr.RoleGrantEntry("R1", False)}
        }
        ob_catalog = sdr.ObGrantCatalog(
            object_privs={("A", "SELECT", "X.T1"), ("A", "SELECT", "X.T2")},
            object_privs_grantable=set(),
            sys_privs=set(),
            sys_privs_admin=set(),
            role_privs=set(),
            role_privs_admin=set(),
        )
        miss_obj, miss_sys, miss_role = sdr.filter_missing_grant_entries(
            object_grants,
            sys_privs,
            role_privs,
            ob_catalog
        )
        self.assertEqual(len(miss_obj.get("A", set())), 1)
        self.assertIn(sdr.ObjectGrantEntry("SELECT", "X.T2", True), miss_obj.get("A", set()))
        self.assertEqual(len(miss_sys.get("A", set())), 1)
        self.assertEqual(len(miss_role.get("A", set())), 1)

    def test_build_view_fixup_chains_with_synonym(self):
        dependency_pairs = {
            ("A.V1", "VIEW", "B.SYN1", "SYNONYM"),
        }
        full_mapping = {
            "A.V1": {"VIEW": "A.V1"},
            "B.SYN1": {"SYNONYM": "B.SYN1"},
            "C.T1": {"TABLE": "C.T1"},
        }
        synonym_meta = {
            ("B", "SYN1"): sdr.SynonymMeta("B", "SYN1", "C", "T1", None),
        }
        ob_meta = sdr.ObMetadata(
            objects_by_type={
                "VIEW": {"A.V1"},
                "SYNONYM": {"B.SYN1"},
                "TABLE": {"C.T1"},
            },
            tab_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}
        )
        ob_grant_catalog = sdr.ObGrantCatalog(
            object_privs={("A", "SELECT", "C.T1")},
            object_privs_grantable=set(),
            sys_privs=set(),
            sys_privs_admin=set(),
            role_privs=set(),
            role_privs_admin=set(),
        )
        chains, cycles = sdr.build_view_fixup_chains(
            ["A.V1"],
            dependency_pairs,
            full_mapping,
            {},
            synonym_meta=synonym_meta,
            ob_meta=ob_meta,
            ob_grant_catalog=ob_grant_catalog
        )
        self.assertFalse(cycles)
        self.assertTrue(chains)
        self.assertIn("A.V1[VIEW|EXISTS|GRANT_NA]", chains[0])
        self.assertIn("B.SYN1[SYNONYM|EXISTS|GRANT_OK]", chains[0])
        self.assertIn("C.T1[TABLE|EXISTS|GRANT_NA]", chains[0])

    def test_build_view_fixup_chains_cycle_detection(self):
        dependency_pairs = {
            ("A.V1", "VIEW", "A.V2", "VIEW"),
            ("A.V2", "VIEW", "A.V1", "VIEW"),
        }
        chains, cycles = sdr.build_view_fixup_chains(
            ["A.V1"],
            dependency_pairs,
            {},
            {},
        )
        self.assertTrue(cycles)

    def test_extract_view_dependencies_with_default_schema(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM T1 t\n"
            "JOIN B.T2 b ON t.ID=b.ID\n"
        )
        deps = sdr.extract_view_dependencies(ddl, default_schema="A")
        self.assertEqual(deps, {"A.T1", "B.T2"})

    def test_remap_view_dependencies_rewrites_qualified_and_unqualified(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM T1 t\n"
            "JOIN B.T2 b ON t.ID=b.ID\n"
        )
        remap_rules = {}
        full_mapping = {
            "A.T1": {"TABLE": "X.T1"},
            "B.T2": {"TABLE": "Y.T2"},
        }
        rewritten = sdr.remap_view_dependencies(ddl, "A", remap_rules, full_mapping)
        self.assertIn("X.T1", rewritten.upper())
        self.assertIn("Y.T2", rewritten.upper())

    def test_clean_view_ddl_preserves_check_option_on_new_version(self):
        ddl = "CREATE OR REPLACE VIEW A.V AS SELECT 1 FROM DUAL WITH CHECK OPTION"
        cleaned = sdr.clean_view_ddl_for_oceanbase(ddl, ob_version="4.2.5.7")
        self.assertIn("WITH CHECK OPTION", cleaned.upper())

    def test_clean_view_ddl_removes_check_option_on_old_version(self):
        ddl = "CREATE OR REPLACE VIEW A.V AS SELECT 1 FROM DUAL WITH CHECK OPTION"
        cleaned = sdr.clean_view_ddl_for_oceanbase(ddl, ob_version="4.2.5.6")
        self.assertNotIn("WITH CHECK OPTION", cleaned.upper())

    def test_clean_view_ddl_removes_check_option_when_version_missing(self):
        ddl = "CREATE OR REPLACE VIEW A.V AS SELECT 1 FROM DUAL WITH CHECK OPTION"
        cleaned = sdr.clean_view_ddl_for_oceanbase(ddl, ob_version=None)
        self.assertNotIn("WITH CHECK OPTION", cleaned.upper())

    def test_clean_view_ddl_removes_check_option_constraint_name(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS SELECT 1 FROM DUAL "
            "WITH CHECK OPTION CONSTRAINT \"C1\""
        )
        cleaned = sdr.clean_view_ddl_for_oceanbase(ddl, ob_version="4.2.5.7")
        self.assertIn("WITH CHECK OPTION", cleaned.upper())
        self.assertNotIn("CONSTRAINT", cleaned.upper())

    def test_clean_view_ddl_strips_trailing_constraint(self):
        ddl = "CREATE OR REPLACE VIEW A.V AS SELECT 1 FROM DUAL CONSTRAINT C1;"
        cleaned = sdr.clean_view_ddl_for_oceanbase(ddl, ob_version="4.2.5.7")
        self.assertNotIn("CONSTRAINT", cleaned.upper())

    def test_clean_view_ddl_preserves_force_and_removes_editionable(self):
        ddl = "CREATE OR REPLACE FORCE EDITIONABLE VIEW A.V AS SELECT 1 FROM DUAL"
        cleaned = sdr.clean_view_ddl_for_oceanbase(ddl, ob_version="4.2.5.7")
        self.assertIn("FORCE VIEW", cleaned.upper())
        self.assertNotIn("EDITIONABLE", cleaned.upper())

    def test_sanitize_view_ddl_inline_comment_breaks_line(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS select a.COL1,--注释1 a.COL2,--注释2 from T a"
        )
        cleaned = sdr.sanitize_view_ddl(ddl, set())
        self.assertIn("--注释1\n", cleaned)
        self.assertIn("\n a.COL2", cleaned)

    def test_sanitize_view_ddl_repairs_split_identifier(self):
        ddl = "CREATE OR REPLACE VIEW A.V AS SELECT TOT_P ERM FROM T"
        cleaned = sdr.sanitize_view_ddl(ddl, {"TOT_PERM"})
        self.assertIn("TOT_PERM", cleaned)
        self.assertNotIn("TOT_P ERM", cleaned)

    def test_sanitize_plsql_punctuation_replaces_fullwidth(self):
        ddl = (
            "CREATE OR REPLACE PACKAGE P AS\n"
            "PROCEDURE P1（A NUMBER，B VARCHAR2）；\n"
            "END;"
        )
        cleaned, count, _samples = sdr.sanitize_plsql_punctuation(ddl, "PACKAGE")
        self.assertNotIn("（", cleaned)
        self.assertNotIn("）", cleaned)
        self.assertNotIn("，", cleaned)
        self.assertNotIn("；", cleaned)
        self.assertIn("P1(ANUMBER,BVARCHAR2);", cleaned.replace(" ", ""))
        self.assertGreater(count, 0)

    def test_sanitize_plsql_punctuation_preserves_strings_comments_and_qids(self):
        ddl = (
            "CREATE OR REPLACE PROCEDURE \"标识（符）\" AS\n"
            "  v VARCHAR2(10) := '中文（，）';\n"
            "  -- 注释（；）\n"
            "BEGIN\n"
            "  NULL；\n"
            "END;"
        )
        cleaned, _count, _samples = sdr.sanitize_plsql_punctuation(ddl, "PROCEDURE")
        self.assertIn("\"标识（符）\"", cleaned)
        self.assertIn("'中文（，）'", cleaned)
        self.assertIn("-- 注释（；）", cleaned)
        self.assertIn("NULL;", cleaned)

    def test_write_fixup_file_includes_clean_notes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            sdr.write_fixup_file(
                base_dir=base,
                subdir="package",
                filename="TEST.SQL",
                content="CREATE OR REPLACE PACKAGE P AS BEGIN NULL; END;",
                header_comment="test",
                extra_comments=["DDL_CLEAN: 全角标点清洗 2 处。示例: （->("]
            )
            output = (base / "package" / "TEST.SQL").read_text(encoding="utf-8")
            self.assertIn("DDL_CLEAN: 全角标点清洗 2 处。示例: （->(", output)

    def test_export_ddl_clean_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [
                sdr.DdlCleanReportRow(
                    obj_type="PACKAGE",
                    obj_full="A.PKG",
                    replaced=3,
                    samples=[("（", "("), ("，", ",")]
                )
            ]
            path = sdr.export_ddl_clean_report(rows, Path(tmpdir), "20240101")
            self.assertIsNotNone(path)
            output = Path(path).read_text(encoding="utf-8")
            self.assertIn("total_objects=1", output)
            self.assertIn("A.PKG", output)
            self.assertIn("3", output)
            self.assertIn("（->(", output)

    def test_filter_oracle_hints_keep_supported(self):
        ddl = "SELECT /*+ INDEX(t idx) FOO */ * FROM t"
        result = sdr.filter_oracle_hints(
            ddl,
            sdr.DDL_HINT_POLICY_KEEP_SUPPORTED,
            {"INDEX"},
            set()
        )
        self.assertIn("INDEX(t idx)", result.ddl)
        self.assertNotIn("FOO", result.ddl)
        self.assertEqual(result.kept, 1)
        self.assertEqual(result.removed, 1)
        self.assertEqual(result.unknown, 1)

    def test_filter_oracle_hints_drop_all(self):
        ddl = "SELECT /*+ INDEX(t idx) */ * FROM t"
        result = sdr.filter_oracle_hints(
            ddl,
            sdr.DDL_HINT_POLICY_DROP_ALL,
            {"INDEX"},
            set()
        )
        self.assertNotIn("/*+", result.ddl)
        self.assertEqual(result.removed, 1)

    def test_filter_oracle_hints_keep_all_with_denylist(self):
        ddl = "SELECT /*+ INDEX(t idx) NO_USE_HASH */ * FROM t"
        result = sdr.filter_oracle_hints(
            ddl,
            sdr.DDL_HINT_POLICY_KEEP_ALL,
            {"INDEX"},
            {"NO_USE_HASH"}
        )
        self.assertIn("INDEX(t idx)", result.ddl)
        self.assertNotIn("NO_USE_HASH", result.ddl)
        self.assertEqual(result.removed, 1)
        self.assertEqual(result.unknown, 1)

    def test_filter_oracle_hints_report_only_keeps_unknown(self):
        ddl = "SELECT /*+ FOO */ * FROM t"
        result = sdr.filter_oracle_hints(
            ddl,
            sdr.DDL_HINT_POLICY_REPORT_ONLY,
            {"INDEX"},
            set()
        )
        self.assertIn("FOO", result.ddl)
        self.assertEqual(result.removed, 0)
        self.assertEqual(result.unknown, 1)

    def test_filter_oracle_hints_ignores_string_literals(self):
        ddl = "SELECT '/*+ INDEX */' AS txt FROM dual /*+ INDEX(t idx) */"
        result = sdr.filter_oracle_hints(
            ddl,
            sdr.DDL_HINT_POLICY_DROP_ALL,
            {"INDEX"},
            set()
        )
        self.assertIn("'/*+ INDEX */'", result.ddl)
        self.assertNotIn("/*+", result.ddl.split("dual")[-1])

    def test_export_ddl_hint_clean_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [
                sdr.DdlHintCleanReportRow(
                    obj_type="VIEW",
                    obj_full="A.V1",
                    policy="keep_supported",
                    total=3,
                    kept=2,
                    removed=1,
                    unknown=1,
                    kept_samples=["INDEX", "LEADING"],
                    removed_samples=["FOO"],
                    unknown_samples=["FOO"]
                )
            ]
            path = sdr.export_ddl_hint_clean_report(rows, Path(tmpdir), "20240101")
            self.assertIsNotNone(path)
            output = Path(path).read_text(encoding="utf-8")
            self.assertIn("total_objects=1", output)
            self.assertIn("A.V1", output)
            self.assertIn("keep_supported", output)
            self.assertIn("FOO", output)

    def test_dependency_grants_add_grantable_for_view_owner(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[
                sdr.OracleObjectPrivilege(
                    grantee="U1",
                    owner="VOWNER",
                    object_name="V1",
                    object_type="VIEW",
                    privilege="SELECT",
                    grantable=False
                )
            ],
            sys_privileges=[],
            role_privileges=[],
            role_metadata={},
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        deps = [
            sdr.DependencyRecord(
                owner="VOWNER",
                name="V1",
                object_type="VIEW",
                referenced_owner="TOWNER",
                referenced_name="T1",
                referenced_type="TABLE"
            )
        ]
        source_objects = {
            "VOWNER.V1": {"VIEW"},
            "TOWNER.T1": {"TABLE"}
        }
        full_mapping = {
            "VOWNER.V1": {"VIEW": "VOWNER.V1"},
            "TOWNER.T1": {"TABLE": "TOWNER.T1"}
        }
        grant_plan = sdr.build_grant_plan(
            oracle_meta=oracle_meta,
            full_mapping=full_mapping,
            remap_rules={},
            source_objects=source_objects,
            schema_mapping={},
            object_parent_map=None,
            dependency_graph=None,
            transitive_table_cache=None,
            source_dependencies=None,
            source_schema_set={"VOWNER", "TOWNER"},
            remap_conflicts=None,
            synonym_meta={},
            dependencies=deps
        )
        entries = grant_plan.object_grants.get("VOWNER", set())
        self.assertIn(
            sdr.ObjectGrantEntry("SELECT", "TOWNER.T1", True),
            entries
        )

    def test_build_grant_plan_filters_missing_grantees(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[
                sdr.OracleObjectPrivilege(
                    grantee="U1",
                    owner="S1",
                    object_name="T1",
                    object_type="TABLE",
                    privilege="SELECT",
                    grantable=False
                ),
                sdr.OracleObjectPrivilege(
                    grantee="R1",
                    owner="S1",
                    object_name="T1",
                    object_type="TABLE",
                    privilege="SELECT",
                    grantable=False
                ),
                sdr.OracleObjectPrivilege(
                    grantee="MISSING",
                    owner="S1",
                    object_name="T1",
                    object_type="TABLE",
                    privilege="SELECT",
                    grantable=False
                ),
            ],
            sys_privileges=[
                sdr.OracleSysPrivilege(grantee="U1", privilege="CREATE SESSION", admin_option=False),
                sdr.OracleSysPrivilege(grantee="MISSING", privilege="CREATE SESSION", admin_option=False),
            ],
            role_privileges=[
                sdr.OracleRolePrivilege(grantee="U1", role="R1", admin_option=False),
                sdr.OracleRolePrivilege(grantee="MISSING", role="R1", admin_option=False),
            ],
            role_metadata={},
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        source_objects = {"S1.T1": {"TABLE"}}
        full_mapping = {"S1.T1": {"TABLE": "S1.T1"}}
        grant_plan = sdr.build_grant_plan(
            oracle_meta=oracle_meta,
            full_mapping=full_mapping,
            remap_rules={},
            source_objects=source_objects,
            schema_mapping={},
            object_parent_map=None,
            dependency_graph=None,
            transitive_table_cache=None,
            source_dependencies=None,
            source_schema_set={"S1", "U1", "MISSING"},
            remap_conflicts=None,
            synonym_meta={},
            ob_roles={"R1"},
            ob_users={"U1"}
        )
        self.assertIn(
            sdr.ObjectGrantEntry("SELECT", "S1.T1", False),
            grant_plan.object_grants.get("U1", set())
        )
        self.assertIn(
            sdr.ObjectGrantEntry("SELECT", "S1.T1", False),
            grant_plan.object_grants.get("R1", set())
        )
        self.assertNotIn("MISSING", grant_plan.object_grants)
        self.assertIn(
            sdr.SystemGrantEntry("CREATE SESSION", False),
            grant_plan.sys_privs.get("U1", set())
        )
        self.assertNotIn("MISSING", grant_plan.sys_privs)
        self.assertIn(
            sdr.RoleGrantEntry("R1", False),
            grant_plan.role_privs.get("U1", set())
        )
        self.assertNotIn("MISSING", grant_plan.role_privs)

    def test_dependency_grants_resolve_synonym_chain(self):
        deps = [
            sdr.DependencyRecord(
                owner="A",
                name="V1",
                object_type="VIEW",
                referenced_owner="PUBLIC",
                referenced_name="S1",
                referenced_type="SYNONYM"
            )
        ]
        synonym_meta = {
            ("PUBLIC", "S1"): sdr.SynonymMeta("PUBLIC", "S1", "SCOTT", "S2", None),
            ("SCOTT", "S2"): sdr.SynonymMeta("SCOTT", "S2", "HR", "T1", None),
        }
        full_mapping = {
            "A.V1": {"VIEW": "A.V1"},
            "HR.T1": {"TABLE": "HR.T1"}
        }
        source_objects = {
            "A.V1": {"VIEW"},
            "HR.T1": {"TABLE"}
        }
        expected = sdr.build_dependency_pairs_for_grants(
            dependencies=deps,
            full_mapping=full_mapping,
            remap_rules={},
            source_objects=source_objects,
            schema_mapping={},
            object_parent_map=None,
            dependency_graph=None,
            transitive_table_cache=None,
            source_dependencies=None,
            source_schema_set={"A", "HR"},
            remap_conflicts=None,
            synonym_meta=synonym_meta,
            progress_interval=999.0
        )
        self.assertIn(("A.V1", "VIEW", "HR.T1", "TABLE"), expected)

    def test_compare_constraints_for_table_fk_reference_check(self):
        oracle_constraints = {
            ("A", "T1"): {
                "FK1": {
                    "type": "R",
                    "columns": ["C1"],
                    "ref_table_owner": "A",
                    "ref_table_name": "RT1",
                }
            },
            ("A", "RT1"): {
                "PK_RT1": {"type": "P", "columns": ["ID"]},
            },
        }
        ob_constraints = {
            ("X", "T1"): {
                "FK1_OB": {
                    "type": "R",
                    "columns": ["C1"],
                    "ref_table_owner": "Y",
                    "ref_table_name": "RT1",
                }
            }
        }
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            indexes={},
            constraints=oracle_constraints,
            triggers={},
            sequences={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[],
            sys_privileges=[],
            role_privileges=[],
            role_metadata={},
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        ob_meta = sdr.ObMetadata(
            objects_by_type={},
            tab_columns={},
            indexes={},
            constraints=ob_constraints,
            triggers={},
            sequences={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}
        )
        full_mapping = {
            "A.RT1": {"TABLE": "Y.RT1"},
        }
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "X",
            "T1",
            full_mapping,
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_collect_blacklisted_missing_tables(self):
        tv_results = {
            "missing": [
                ("TABLE", "TGT.T1", "SRC.T1"),
                ("VIEW", "TGT.V1", "SRC.V1"),
                ("TABLE", "TGT.T2", "SRC.T2"),
            ]
        }
        blacklist = {
            ("SRC", "T1"): {("SPE", "XMLTYPE"): sdr.BlacklistEntry("SPE", "XMLTYPE", "RULE=R1")},
            ("SRC", "T3"): {("DIY", "UDT"): sdr.BlacklistEntry("DIY", "UDT", "")},
        }
        filtered = sdr.collect_blacklisted_missing_tables(tv_results, blacklist)
        self.assertIn(("SRC", "T1"), filtered)
        self.assertNotIn(("SRC", "T2"), filtered)
        self.assertNotIn(("SRC", "T3"), filtered)

    def test_add_blacklist_entry_merges_sources(self):
        blacklist = {}
        sdr.add_blacklist_entry(blacklist, "A", "T1", "SPE", "XMLTYPE", "TABLE")
        sdr.add_blacklist_entry(blacklist, "A", "T1", "SPE", "XMLTYPE", "RULE=R1")
        entry = blacklist[("A", "T1")][("SPE", "XMLTYPE")]
        self.assertIn("RULE=R1", entry.source)
        self.assertIn("TABLE", entry.source)
        self.assertEqual(sdr.format_blacklist_source(entry.source), "RULE=R1")

    def test_blacklist_mode_normalization(self):
        self.assertEqual(sdr.normalize_blacklist_mode("rules_only"), "rules_only")
        self.assertEqual(sdr.normalize_blacklist_mode("UNKNOWN"), "auto")

    def test_load_blacklist_rules_from_file(self):
        payload = {
            "rules": [
                {
                    "id": "SAMPLE",
                    "black_type": "SPE",
                    "sql": "SELECT OWNER, TABLE_NAME, DATA_TYPE, 'SPE' FROM DBA_TAB_COLUMNS WHERE OWNER IN ({{owners_clause}})"
                }
            ]
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            handle.write(json.dumps(payload))
            path = handle.name
        try:
            rules = sdr.load_blacklist_rules(path)
        finally:
            Path(path).unlink(missing_ok=True)
        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0].rule_id, "SAMPLE")

    def test_blacklist_rule_enable_disable_and_version(self):
        rule = sdr.BlacklistRule(
            rule_id="R1",
            black_type="SPE",
            sql="SELECT 1 FROM DUAL",
            min_ob_version="4.2.5.7",
            max_ob_version=None,
            enabled=True,
        )
        enabled, reason = sdr.is_blacklist_rule_enabled(rule, set(), set(), "4.2.5.7")
        self.assertTrue(enabled)
        self.assertEqual(reason, "")

        enabled, reason = sdr.is_blacklist_rule_enabled(rule, set(), set(), "4.2.5.6")
        self.assertFalse(enabled)
        self.assertEqual(reason, "below_min_version")

        enabled, reason = sdr.is_blacklist_rule_enabled(rule, set(), {"R1"}, "4.2.5.7")
        self.assertFalse(enabled)
        self.assertEqual(reason, "in_disable_set")

        enabled, reason = sdr.is_blacklist_rule_enabled(rule, {"R2"}, set(), "4.2.5.7")
        self.assertFalse(enabled)
        self.assertEqual(reason, "not_in_enable_set")

    def test_clean_interval_partition_clause(self):
        ddl = (
            "CREATE TABLE T1 (ID NUMBER) PARTITION BY RANGE (DT) "
            "INTERVAL (NUMTOYMINTERVAL(1,'MONTH')) "
            "(PARTITION P1 VALUES LESS THAN (TO_DATE('2024-01-01','YYYY-MM-DD')))"
        )
        cleaned = sdr.clean_interval_partition_clause(ddl)
        self.assertNotIn("INTERVAL", cleaned.upper())

    def test_parse_interval_expression(self):
        spec = sdr.parse_interval_expression("NUMTOYMINTERVAL(1,'MONTH')")
        self.assertIsNotNone(spec)
        self.assertEqual(spec.value, 1)
        self.assertEqual(spec.unit, "MONTH")

        spec = sdr.parse_interval_expression("NUMTODSINTERVAL(7,'DAY')")
        self.assertIsNotNone(spec)
        self.assertEqual(spec.value, 7)
        self.assertEqual(spec.unit, "DAY")

    def test_parse_numeric_interval_expression(self):
        spec = sdr.parse_numeric_interval_expression("INTERVAL (1)")
        self.assertIsNotNone(spec)
        self.assertEqual(str(spec.value), "1")

        spec = sdr.parse_numeric_interval_expression("2.5")
        self.assertIsNotNone(spec)
        self.assertEqual(str(spec.value), "2.5")

    def test_parse_partition_high_value(self):
        expr = "TO_DATE('2024-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"
        parsed = sdr.parse_partition_high_value(expr)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.strftime("%Y-%m-%d"), "2024-01-01")

    def test_parse_partition_high_value_numeric(self):
        parsed = sdr.parse_partition_high_value_numeric("TO_NUMBER('10')")
        self.assertIsNotNone(parsed)
        self.assertEqual(str(parsed), "10")

        parsed = sdr.parse_partition_high_value_numeric(" 42 ")
        self.assertIsNotNone(parsed)
        self.assertEqual(str(parsed), "42")

    def test_generate_interval_partition_statements(self):
        info = sdr.IntervalPartitionInfo(
            interval_expr="NUMTOYMINTERVAL(1,'MONTH')",
            partitioning_type="RANGE",
            subpartitioning_type="",
            last_partition_name="P20240101",
            last_high_value="TO_DATE('2024-01-01','YYYY-MM-DD')",
            last_partition_position=1,
            partition_key_columns=["DT"],
            existing_partition_names=set()
        )
        cutoff = sdr.datetime.strptime("20240401", "%Y%m%d")
        stmts, warnings, kind = sdr.generate_interval_partition_statements(
            info,
            cutoff,
            None,
            "DATE",
            "SCHEMA.T1"
        )
        self.assertFalse(warnings)
        self.assertEqual(len(stmts), 3)
        self.assertEqual(kind, "date")

    def test_generate_numeric_interval_partition_statements(self):
        info = sdr.IntervalPartitionInfo(
            interval_expr="1",
            partitioning_type="RANGE",
            subpartitioning_type="",
            last_partition_name="P0",
            last_high_value="0",
            last_partition_position=1,
            partition_key_columns=["ID"],
            existing_partition_names=set()
        )
        stmts, warnings, kind = sdr.generate_interval_partition_statements(
            info,
            None,
            sdr.Decimal("3"),
            "NUMBER",
            "SCHEMA.T1"
        )
        self.assertFalse(warnings)
        self.assertEqual(kind, "numeric")
        self.assertEqual(len(stmts), 3)

    def test_format_oracle_column_type_maps_long(self):
        info_long = {"data_type": "LONG"}
        info_long_raw = {"data_type": "LONG RAW"}
        self.assertEqual(sdr.format_oracle_column_type(info_long), "CLOB")
        self.assertEqual(sdr.format_oracle_column_type(info_long_raw), "BLOB")

    def test_recursive_infer_target_schema_uses_indirect_tables(self):
        deps = {
            ("A", "V2", "VIEW", "A", "V1", "VIEW"),
            ("A", "V1", "VIEW", "A", "T1", "TABLE"),
        }
        graph = sdr.build_dependency_graph(deps)
        remap_rules = {"A.T1": "B.T1"}
        inferred, conflict = sdr.infer_target_schema_from_dependencies(
            "A.V2",
            "VIEW",
            remap_rules,
            graph,
        )
        self.assertEqual(inferred, "B.V2")
        self.assertFalse(conflict)

    def test_recursive_infer_target_schema_counts_unremapped_tables(self):
        deps = {
            ("A", "V1", "VIEW", "A", "T1", "TABLE"),
            ("A", "V1", "VIEW", "A", "T2", "TABLE"),
            ("A", "V1", "VIEW", "A", "T3", "TABLE"),
        }
        graph = sdr.build_dependency_graph(deps)
        remap_rules = {"A.T1": "B.T1"}  # T2/T3 未显式 remap，视为 A
        inferred, conflict = sdr.infer_target_schema_from_dependencies(
            "A.V1",
            "VIEW",
            remap_rules,
            graph,
        )
        self.assertEqual(inferred, "A.V1")
        self.assertFalse(conflict)

    def test_adjust_ddl_qualifies_unremapped_dependency_when_main_moved(self):
        ddl = "CREATE OR REPLACE VIEW A.V AS SELECT * FROM T2;"
        adjusted = sdr.adjust_ddl_for_object(
            ddl,
            "A",
            "V",
            "B",
            "V",
            extra_identifiers=[(("A", "T2"), ("A", "T2"))],
            obj_type="VIEW",
        )
        self.assertIn("A.T2", adjusted.upper())

    def test_precompute_transitive_table_cache_handles_cycle(self):
        deps = {
            ("A", "P1", "PROCEDURE", "A", "P2", "PROCEDURE"),
            ("A", "P2", "PROCEDURE", "A", "P1", "PROCEDURE"),
            ("A", "P1", "PROCEDURE", "A", "T1", "TABLE"),
        }
        graph = sdr.build_dependency_graph(deps)
        cache = sdr.precompute_transitive_table_cache(graph)
        self.assertEqual(cache.get(("A.P1", "PROCEDURE")), {"A.T1"})
        self.assertEqual(cache.get(("A.P2", "PROCEDURE")), {"A.T1"})
        inferred, conflict = sdr.infer_target_schema_from_dependencies(
            "A.P2",
            "PROCEDURE",
            {"A.T1": "B.T1"},
            graph,
            transitive_table_cache=cache,
        )
        self.assertEqual(inferred, "B.P2")
        self.assertFalse(conflict)

    def test_package_body_keeps_source_schema_without_explicit_remap(self):
        source_objects = {"A.PKG": {"PACKAGE", "PACKAGE BODY"}}
        deps = {("A", "PKG", "PACKAGE BODY", "A", "T1", "TABLE")}
        graph = sdr.build_dependency_graph(deps)
        cache = sdr.precompute_transitive_table_cache(graph)
        remap_rules = {"A.T1": "B.T1"}
        mapping = sdr.build_full_object_mapping(
            source_objects,
            remap_rules,
            schema_mapping=None,
            object_parent_map=None,
            transitive_table_cache=cache,
            source_dependencies=None,
            dependency_graph=graph,
        )
        self.assertEqual(mapping["A.PKG"]["PACKAGE"], "A.PKG")
        self.assertEqual(mapping["A.PKG"]["PACKAGE BODY"], "A.PKG")


if __name__ == "__main__":
    unittest.main()
