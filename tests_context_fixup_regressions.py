import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

try:  # pragma: no cover
    import oracledb  # noqa: F401
except ImportError:  # pragma: no cover
    import sys

    dummy_oracledb = types.ModuleType("oracledb")

    class _DummyConnection:  # pragma: no cover
        pass

    def _dummy_connect(*_args, **_kwargs):  # pragma: no cover
        raise RuntimeError("dummy oracledb.connect called")

    dummy_oracledb.Connection = _DummyConnection
    dummy_oracledb.connect = _dummy_connect
    dummy_oracledb.Error = Exception
    sys.modules["oracledb"] = dummy_oracledb

import run_fixup as rf
import schema_diff_reconciler as sdr


class TestRunFixupRegressionScenarios(unittest.TestCase):
    def test_non_smart_order_places_synonym_and_refresh_before_view(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixup_dir = Path(tmpdir) / "fixup"
            (fixup_dir / "view").mkdir(parents=True)
            (fixup_dir / "view_refresh").mkdir(parents=True)
            (fixup_dir / "synonym").mkdir(parents=True)
            (fixup_dir / "materialized_view").mkdir(parents=True)

            for rel in (
                "view/A.V1.sql",
                "view_refresh/A.V1.sql",
                "synonym/A.S1.sql",
                "materialized_view/A.MV1.sql",
            ):
                path = fixup_dir / rel
                path.write_text("SELECT 1 FROM DUAL;", encoding="utf-8")

            files = rf.collect_sql_files_by_layer(fixup_dir, smart_order=False)
            order = [str(path.relative_to(fixup_dir)) for _, path in files]

            self.assertLess(order.index("synonym/A.S1.sql"), order.index("view_refresh/A.V1.sql"))
            self.assertLess(order.index("view_refresh/A.V1.sql"), order.index("view/A.V1.sql"))
            self.assertLess(
                order.index("view_refresh/A.V1.sql"), order.index("materialized_view/A.MV1.sql")
            )


class TestContextRegressionScenarios(unittest.TestCase):
    def test_context_compare_reports_extra_target_inventory(self):
        result = sdr.build_context_compare_results(
            source_contexts={
                "APP_CTX": sdr.ContextMetadata(
                    namespace="APP_CTX",
                    schema="SRC",
                    package="CTX_PKG",
                    type="ACCESSED LOCALLY",
                ),
            },
            target_contexts={
                "APP_CTX": sdr.ContextMetadata(
                    namespace="APP_CTX",
                    schema="TGT",
                    package="CTX_PKG",
                    type="ACCESSED LOCALLY",
                ),
                "EXTRA_CTX": sdr.ContextMetadata(
                    namespace="EXTRA_CTX",
                    schema="TGT",
                    package="CTX_EXTRA",
                    type="ACCESSED GLOBALLY",
                ),
            },
            reference_rows=[],
            full_object_mapping={
                "SRC.CTX_PKG": {"PACKAGE": "TGT.CTX_PKG", "PACKAGE BODY": "TGT.CTX_PKG"},
            },
            enabled_primary_types={"CONTEXT", "PACKAGE", "PACKAGE BODY"},
            context_fixup_mode="manual",
            target_package_objects={"TGT.CTX_PKG"},
            planned_package_targets=set(),
        )

        detail_rows = {row.namespace: row for row in result.detail_rows}

        self.assertEqual(detail_rows["APP_CTX"].status, "OK")
        self.assertEqual(detail_rows["EXTRA_CTX"].status, "EXTRA_TARGET")
        self.assertEqual(detail_rows["EXTRA_CTX"].reason_code, "TARGET_CONTEXT_EXTRA")
        self.assertEqual(result.summary["extra_target"], 1)

    def test_context_compare_normalizes_local_alias_before_mode_compare(self):
        result = sdr.build_context_compare_results(
            source_contexts={
                "APP_CTX": sdr.ContextMetadata(
                    namespace="APP_CTX",
                    schema="SRC",
                    package="CTX_PKG",
                    type="ACCESSED LOCALLY",
                ),
            },
            target_contexts={
                "APP_CTX": sdr.ContextMetadata(
                    namespace="APP_CTX",
                    schema="TGT",
                    package="CTX_PKG",
                    type="LOCAL",
                ),
            },
            reference_rows=[],
            full_object_mapping={
                "SRC.CTX_PKG": {"PACKAGE": "TGT.CTX_PKG", "PACKAGE BODY": "TGT.CTX_PKG"},
            },
            enabled_primary_types={"CONTEXT", "PACKAGE", "PACKAGE BODY"},
            context_fixup_mode="manual",
            target_package_objects={"TGT.CTX_PKG"},
            planned_package_targets=set(),
        )

        detail_rows = {row.namespace: row for row in result.detail_rows}

        self.assertEqual(detail_rows["APP_CTX"].status, "OK")
        self.assertEqual(result.summary["mismatch"], 0)

    def test_oracle_context_inventory_logs_fallback_reason(self):
        error_cls = getattr(sdr.oracledb, "Error", Exception)

        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, sql, params=None):
                if "TYPE, TRACKING, ORIGIN_CON_ID" in sql.upper():
                    raise error_cls("TRACKING not available")
                self._rows = [("APP_CTX", "SRC", "CTX_PKG", "LOCAL", "", None)]

            def __iter__(self):
                return iter(self._rows)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        orig_connect = getattr(sdr.oracledb, "connect", None)
        try:
            sdr.oracledb.connect = lambda **_kwargs: FakeConnection()
            with mock.patch.object(sdr.log, "debug") as debug_mock:
                contexts, err, health = sdr.load_oracle_context_inventory(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"SRC"},
                )
        finally:
            if orig_connect is None:
                delattr(sdr.oracledb, "connect")
            else:
                sdr.oracledb.connect = orig_connect

        self.assertIsNotNone(err)
        self.assertIn("fallback_to_simplified_dba_context", err)
        self.assertEqual(health, sdr.CONTEXT_INVENTORY_HEALTH_DEGRADED)
        self.assertEqual(contexts["APP_CTX"].type, sdr.CONTEXT_MODE_LOCAL)
        debug_mock.assert_called()
        self.assertIn("load_oracle_context_inventory 降级", debug_mock.call_args[0][0])


class TestConstraintValidatedUnknownRegressionScenarios(unittest.TestCase):
    @staticmethod
    def _make_oracle_meta(table_columns, constraints=None):
        return sdr.OracleMetadata(
            table_columns=table_columns,
            invisible_column_supported=False,
            identity_column_supported=False,
            default_on_null_supported=False,
            indexes={},
            constraints=constraints or {},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=False,
            blacklist_tables={},
            object_privileges=[],
            column_privileges=[],
            sys_privileges=[],
            role_privileges=[],
            role_metadata={},
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            interval_partitions={},
        )

    @staticmethod
    def _make_ob_meta(objects_by_type, tab_columns, constraints=None):
        return sdr.ObMetadata(
            objects_by_type=objects_by_type,
            tab_columns=tab_columns,
            invisible_column_supported=False,
            identity_column_supported=False,
            default_on_null_supported=False,
            indexes={},
            constraints=constraints or {},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=False,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
        )

    def test_check_primary_objects_reports_unknown_source_validated_state(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta(
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "char_length": None,
                        "char_used": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                    }
                }
            },
            constraints={
                ("A", "T1"): {
                    "CK_NN_C1": {
                        "type": "C",
                        "status": "ENABLED",
                        "validated": None,
                        "search_condition": '"C1" IS NOT NULL',
                        "columns": ["C1"],
                    }
                }
            },
        )
        ob_meta = self._make_ob_meta(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "char_length": None,
                        "char_used": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                    }
                }
            },
            constraints={("A", "T1"): {}},
        )

        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )

        self.assertEqual(len(results["mismatched"]), 1)
        type_mismatches = results["mismatched"][0][5]
        self.assertEqual(len(type_mismatches), 1)
        issue = type_mismatches[0]
        self.assertEqual(issue.issue, "nullability_unknown_source_validated")
        self.assertEqual(issue.src_type, "NOT NULL (SOURCE VALIDATED UNKNOWN)")
        self.assertEqual(issue.tgt_type, "NULLABLE")
        self.assertEqual(issue.expected_type, "REVIEW SOURCE VALIDATED STATE")

    def test_generate_alter_for_table_columns_keeps_unknown_source_validated_review_only(self):
        oracle_meta = self._make_oracle_meta(
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "char_length": None,
                        "char_used": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                    }
                }
            },
            constraints={("A", "T1"): {}},
        )

        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "A",
            "T1",
            "A",
            "T1",
            missing_cols=set(),
            extra_cols=set(),
            length_mismatches=[],
            type_mismatches=[
                sdr.ColumnTypeIssue(
                    "C1",
                    "NOT NULL (SOURCE VALIDATED UNKNOWN)",
                    "NULLABLE",
                    "REVIEW SOURCE VALIDATED STATE",
                    "nullability_unknown_source_validated",
                )
            ],
        )

        self.assertIsNotNone(sql)
        self.assertIn("REVIEW-FIRST", sql)
        self.assertIn("VALIDATED 状态未知", sql)
        self.assertIn("UNKNOWN 默认视为匹配", sql)

    def test_export_column_nullability_detail_includes_unknown_source_validated_review(self):
        mismatched_items = [
            (
                "TABLE",
                "A.T1",
                set(),
                set(),
                [],
                [
                    sdr.ColumnTypeIssue(
                        "C1",
                        "NOT NULL (SOURCE VALIDATED UNKNOWN)",
                        "NULLABLE",
                        "REVIEW SOURCE VALIDATED STATE",
                        "nullability_unknown_source_validated",
                    )
                ],
            )
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = sdr.export_column_nullability_detail(
                mismatched_items,
                Path(tmp_dir),
                "20260417_120000",
            )
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")

        self.assertIn("列空值语义差异明细", content)
        self.assertIn(
            "A.T1|C1|NOT NULL (SOURCE VALIDATED UNKNOWN)|NULLABLE|REVIEW SOURCE VALIDATED STATE|REVIEW_SOURCE_VALIDATED_UNKNOWN",
            content,
        )
