import unittest
import logging
from unittest import mock
import sys
import types
import tempfile
import json
import inspect
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Set, List, Tuple

# schema_diff_reconciler 在 import 时强依赖 oracledb；
# 单元测试只覆盖纯函数，因此用 dummy 模块兜底，避免环境未安装时退出。
try:  # pragma: no cover
    import oracledb  # noqa: F401
except ImportError:  # pragma: no cover
    dummy_oracledb = types.ModuleType("oracledb")

    class _DummyConnection:  # pragma: no cover
        pass

    def _dummy_connect(*_args, **_kwargs):  # pragma: no cover
        raise RuntimeError("dummy oracledb.connect called")

    dummy_oracledb.Connection = _DummyConnection
    dummy_oracledb.connect = _dummy_connect
    dummy_oracledb.Error = Exception
    sys.modules["oracledb"] = dummy_oracledb

import schema_diff_reconciler as sdr


class TestSchemaDiffReconcilerPureFunctions(unittest.TestCase):
    def _make_oracle_meta(
        self,
        *,
        sequences: Dict[str, Set[str]] = None,
        indexes: Dict = None,
        constraints: Dict = None,
        triggers: Dict = None,
        invisible_supported: bool = False,
        identity_supported: bool = True,
        default_on_null_supported: bool = True,
        default_on_null_columns: Dict[Tuple[str, str], Tuple[str, ...]] = None,
        identity_modes: Dict[Tuple[str, str], Dict[str, str]] = None,
        identity_options: Dict[Tuple[str, str], Dict[str, Dict[str, str]]] = None,
        temporary_tables: Set[Tuple[str, str]] = None,
        loaded_schemas: Set[str] = None,
    ) -> sdr.OracleMetadata:
        return sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=invisible_supported,
            identity_column_supported=identity_supported,
            default_on_null_supported=default_on_null_supported,
            indexes=indexes or {},
            constraints=constraints or {},
            triggers=triggers or {},
            sequences=sequences or {},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
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
            loaded_schemas=frozenset((loaded_schemas or set())),
            temporary_tables=temporary_tables or set(),
            identity_modes=identity_modes or {},
            default_on_null_columns=default_on_null_columns or {},
            identity_options=identity_options or {},
        )

    def _make_ob_meta(
        self,
        *,
        sequences: Dict[str, Set[str]] = None,
        indexes: Dict = None,
        constraints: Dict = None,
        triggers: Dict = None,
        invisible_supported: bool = False,
        identity_supported: bool = True,
        default_on_null_supported: bool = True,
        default_on_null_columns: Dict[Tuple[str, str], Tuple[str, ...]] = None,
        identity_modes: Dict[Tuple[str, str], Dict[str, str]] = None,
        identity_options: Dict[Tuple[str, str], Dict[str, Dict[str, str]]] = None,
        constraint_deferrable_supported: bool = False
    ) -> sdr.ObMetadata:
        return sdr.ObMetadata(
            objects_by_type={},
            tab_columns={},
            invisible_column_supported=invisible_supported,
            identity_column_supported=identity_supported,
            default_on_null_supported=default_on_null_supported,
            indexes=indexes or {},
            constraints=constraints or {},
            triggers=triggers or {},
            sequences=sequences or {},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            constraint_deferrable_supported=constraint_deferrable_supported,
            temporary_tables=set(),
            identity_modes=identity_modes or {},
            default_on_null_columns=default_on_null_columns or {},
            identity_options=identity_options or {},
        )

    def _make_oracle_meta_with_columns(
        self,
        table_columns: Dict,
        *,
        invisible_supported: bool = False,
        identity_supported: bool = True,
        default_on_null_supported: bool = True,
        default_on_null_columns: Dict[Tuple[str, str], Tuple[str, ...]] = None,
        identity_modes: Dict[Tuple[str, str], Dict[str, str]] = None,
        identity_options: Dict[Tuple[str, str], Dict[str, Dict[str, str]]] = None,
    ) -> sdr.OracleMetadata:
        return sdr.OracleMetadata(
            table_columns=table_columns,
            invisible_column_supported=invisible_supported,
            identity_column_supported=identity_supported,
            default_on_null_supported=default_on_null_supported,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
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
            identity_modes=identity_modes or {},
            default_on_null_columns=default_on_null_columns or {},
            identity_options=identity_options or {},
        )

    def _make_ob_meta_with_columns(
        self,
        objects_by_type: Dict,
        tab_columns: Dict,
        *,
        invisible_supported: bool = False,
        identity_supported: bool = True,
        default_on_null_supported: bool = True,
        default_on_null_columns: Dict[Tuple[str, str], Tuple[str, ...]] = None,
        identity_modes: Dict[Tuple[str, str], Dict[str, str]] = None,
        identity_options: Dict[Tuple[str, str], Dict[str, Dict[str, str]]] = None,
        constraint_deferrable_supported: bool = False
    ) -> sdr.ObMetadata:
        return sdr.ObMetadata(
            objects_by_type=objects_by_type,
            tab_columns=tab_columns,
            invisible_column_supported=invisible_supported,
            identity_column_supported=identity_supported,
            default_on_null_supported=default_on_null_supported,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            constraint_deferrable_supported=constraint_deferrable_supported,
            temporary_tables=set(),
            identity_modes=identity_modes or {},
            default_on_null_columns=default_on_null_columns or {},
            identity_options=identity_options or {},
        )

    def test_derive_managed_target_scope_tracks_target_only_schema(self):
        full_mapping = {
            "LIFEBASE.EM_PREM_TBL": {"TABLE": "BASEDATA.EM_PREM_TBL"},
            "LIFEBASE.TR_U_POL_AGT": {"TRIGGER": "LIFEDATA.TR_U_POL_AGT"},
            "LIFEBASE.V_POL_AGT": {"VIEW": "LIFEDATA.V_POL_AGT"},
        }

        scope = sdr.derive_managed_target_scope(
            ["LIFEBASE", "BASEDATA"],
            full_mapping
        )

        self.assertEqual(scope.target_schemas, frozenset(["BASEDATA", "LIFEDATA"]))
        self.assertEqual(scope.target_only_schemas, frozenset(["LIFEDATA"]))
        self.assertEqual(scope.target_to_source_schemas["LIFEDATA"], ("LIFEBASE",))
        self.assertEqual(scope.target_object_counts["LIFEDATA"], 2)

    def test_should_load_constraint_metadata_for_table_semantics(self):
        self.assertTrue(sdr.should_load_constraint_metadata({"TABLE"}, set()))
        self.assertTrue(sdr.should_load_constraint_metadata(set(), {"CONSTRAINT"}))
        self.assertTrue(sdr.should_load_constraint_metadata({"TABLE"}, {"INDEX"}))
        self.assertFalse(sdr.should_load_constraint_metadata({"VIEW"}, {"INDEX", "TRIGGER"}))

    def test_export_managed_target_scope_detail_marks_remapped_only_schema(self):
        scope = sdr.derive_managed_target_scope(
            ["LIFEBASE", "BASEDATA"],
            {
                "LIFEBASE.EM_PREM_TBL": {"TABLE": "BASEDATA.EM_PREM_TBL"},
                "LIFEBASE.TR_U_POL_AGT": {"TRIGGER": "LIFEDATA.TR_U_POL_AGT"},
                "LIFEBASE.V_POL_AGT": {"VIEW": "LIFEDATA.V_POL_AGT"},
            }
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = sdr.export_managed_target_scope_detail(
                scope,
                Path(tmp_dir),
                "20260319_000000"
            )
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
            self.assertIn("LIFEDATA|N|LIFEBASE|2|REMAPPED_ONLY", content)
            self.assertIn("BASEDATA|Y|LIFEBASE|1|REMAPPED_IN_CONFIG", content)

    def test_derive_managed_target_scope_after_filtering_drops_excluded_self_schema(self):
        full_mapping = {
            "SRC.T_REMAPPED": {"TABLE": "TGT.T_REMAPPED"},
            "SRC.T_SELF": {"TABLE": "SRC.T_SELF"},
        }
        filtered_mapping = sdr.filter_full_object_mapping_by_nodes(
            full_mapping,
            {("SRC.T_SELF", "TABLE")},
        )
        scope = sdr.derive_managed_target_scope(["SRC"], filtered_mapping)
        self.assertEqual(scope.target_schemas, frozenset(["TGT"]))
        self.assertEqual(scope.target_only_schemas, frozenset(["TGT"]))

    def test_report_artifact_type_includes_managed_target_scope_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260319/managed_target_scope_detail_20260319.txt"
        )
        self.assertEqual(artifact_type, "MANAGED_TARGET_SCOPE_DETAIL")
        status, note = sdr._infer_artifact_status(
            artifact_type,
            "core",
            set(),
            False
        )
        self.assertEqual((status, note), ("IN_DB", ""))

    def test_dump_oracle_metadata_fallback_keeps_multiple_tables(self):
        master_list = [
            ("OMS_USER.T_A", "OMS_USER.T_A", "TABLE"),
            ("OMS_USER.T_B", "OMS_USER.T_B", "TABLE"),
        ]
        settings = {
            "source_schemas_list": ["OMS_USER"],
            "grant_tab_privs_scope": "owner",
            "blacklist_mode": "disabled",
            "enable_column_order_check": False,
        }
        fail_once = {"done": False}

        class CursorMock:
            def __init__(self):
                self.rows = []
                self.index = 0

            def execute(self, sql, params=None, **kwargs):
                sql_u = str(sql).upper()
                self.index = 0
                if "FROM DBA_TAB_COLUMNS" in sql_u and "OWNER = 'SYS'" in sql_u:
                    view_name = (kwargs.get("view_name") or "").upper()
                    col_name = (kwargs.get("col_name") or "").upper()
                    if view_name == "DBA_TAB_COLUMNS" and col_name in {"HIDDEN_COLUMN", "VIRTUAL_COLUMN"}:
                        self.rows = [(0,)]
                    else:
                        self.rows = [(1,)]
                    return
                if ("FROM DBA_TAB_COLS" in sql_u or "FROM DBA_TAB_COLUMNS" in sql_u) and "OWNER IN (" in sql_u:
                    if "FROM DBA_TAB_COLS" in sql_u and not fail_once["done"]:
                        fail_once["done"] = True
                        raise RuntimeError("forced fallback")
                    self.rows = [
                        ("OMS_USER", "T_A", "COL1", "NUMBER", 22, 10, 0, "Y", None, "B", 0),
                        ("OMS_USER", "T_A", "COL2", "VARCHAR2", 30, None, None, "Y", None, "B", 30),
                        ("OMS_USER", "T_B", "COL1", "NUMBER", 22, 10, 0, "Y", None, "B", 0),
                        ("OMS_USER", "T_B", "COL2", "DATE", 7, None, None, "Y", None, "B", 0),
                    ]
                    return
                self.rows = []

            def fetchone(self):
                if self.index >= len(self.rows):
                    return None
                row = self.rows[self.index]
                self.index += 1
                return row

            def __iter__(self):
                return iter(self.rows)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class ConnectionMock:
            def cursor(self):
                return CursorMock()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with mock.patch.object(sdr.oracledb, "connect", return_value=ConnectionMock()), \
             mock.patch.object(sdr.oracledb, "Error", RuntimeError):
            meta = sdr.dump_oracle_metadata(
                {"user": "u", "password": "p", "dsn": "d"},
                master_list,
                settings,
                include_indexes=False,
                include_constraints=False,
                include_triggers=False,
                include_sequences=False,
                include_comments=False,
                include_blacklist=False,
                include_privileges=False,
            )

        self.assertEqual(
            sorted(meta.table_columns.keys()),
            [("OMS_USER", "T_A"), ("OMS_USER", "T_B")],
        )
        self.assertEqual(set(meta.table_columns[("OMS_USER", "T_A")].keys()), {"COL1", "COL2"})
        self.assertEqual(set(meta.table_columns[("OMS_USER", "T_B")].keys()), {"COL1", "COL2"})

    def test_select_tab_columns_view_prefers_secondary(self):
        primary_support = {
            "HIDDEN_COLUMN": False,
            "VIRTUAL_COLUMN": False,
            "IDENTITY_COLUMN": True
        }
        secondary_support = {
            "HIDDEN_COLUMN": True,
            "VIRTUAL_COLUMN": True,
            "IDENTITY_COLUMN": True
        }
        view_name, support_map, missing_cols = sdr.select_tab_columns_view(
            "DBA_TAB_COLUMNS",
            primary_support,
            "DBA_TAB_COLS",
            secondary_support
        )
        self.assertEqual(view_name, "DBA_TAB_COLS")
        self.assertEqual(support_map, secondary_support)
        self.assertEqual(set(missing_cols), {"HIDDEN_COLUMN", "VIRTUAL_COLUMN"})

    def test_select_tab_columns_view_keeps_primary(self):
        primary_support = {
            "HIDDEN_COLUMN": True,
            "VIRTUAL_COLUMN": True
        }
        secondary_support = {
            "HIDDEN_COLUMN": True,
            "VIRTUAL_COLUMN": True
        }
        view_name, support_map, missing_cols = sdr.select_tab_columns_view(
            "DBA_TAB_COLUMNS",
            primary_support,
            "DBA_TAB_COLS",
            secondary_support
        )
        self.assertEqual(view_name, "DBA_TAB_COLUMNS")
        self.assertEqual(support_map, primary_support)
        self.assertFalse(missing_cols)

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

    def test_normalize_name_collision_mode_defaults_to_fixup(self):
        self.assertEqual(sdr.normalize_name_collision_mode(None), "fixup")
        self.assertEqual(sdr.normalize_name_collision_mode(""), "fixup")
        self.assertEqual(sdr.normalize_name_collision_mode("FIXUP"), "fixup")
        self.assertEqual(sdr.normalize_name_collision_mode("bad_value"), "fixup")

    def test_build_name_collision_plan_rewrites_planned_index_name_on_existing_conflict(self):
        oracle_meta = self._make_oracle_meta(
            indexes={
                ("SRC", "T1"): {
                    "C1": {
                        "columns": ["COL1"],
                        "expressions": {},
                    }
                }
            },
            constraints={}
        )
        ob_meta = self._make_ob_meta(
            indexes={},
            constraints={
                ("TGT", "T1"): {
                    "C1": {
                        "type": "P",
                        "columns": ["ID"],
                        "search_condition": "",
                    }
                }
            }
        )
        index_tasks = [
            (
                sdr.IndexMismatch(
                    table="TGT.T1",
                    missing_indexes={"C1"},
                    extra_indexes=set(),
                    detail_mismatch=[],
                ),
                "SRC",
                "T1",
                "TGT",
                "T1",
            )
        ]

        planned_map, rename_actions, detail_rows = sdr.build_name_collision_plan(
            oracle_meta,
            ob_meta,
            index_tasks,
            [],
            mode="fixup",
            rename_existing=False,
        )

        mapped_name = planned_map.get(("TGT", "INDEX", "T1", "C1"))
        self.assertTrue(mapped_name)
        self.assertNotEqual(mapped_name, "C1")
        self.assertTrue(mapped_name.startswith("IX_"))
        self.assertEqual(rename_actions, [])
        self.assertTrue(any("PLANNED_VS_EXISTING" in row.conflict_type for row in detail_rows))

    def test_build_name_collision_plan_keeps_planned_name_without_conflict(self):
        oracle_meta = self._make_oracle_meta(
            indexes={
                ("SRC", "T1"): {
                    "IX_SRC_T1_COL1": {
                        "columns": ["COL1"],
                        "expressions": {},
                    }
                }
            },
            constraints={}
        )
        ob_meta = self._make_ob_meta(indexes={}, constraints={})
        index_tasks = [
            (
                sdr.IndexMismatch(
                    table="TGT.T1",
                    missing_indexes={"IX_SRC_T1_COL1"},
                    extra_indexes=set(),
                    detail_mismatch=[],
                ),
                "SRC",
                "T1",
                "TGT",
                "T1",
            )
        ]

        planned_map, rename_actions, detail_rows = sdr.build_name_collision_plan(
            oracle_meta,
            ob_meta,
            index_tasks,
            [],
            mode="fixup",
            rename_existing=False,
        )

        self.assertEqual(
            planned_map.get(("TGT", "INDEX", "T1", "IX_SRC_T1_COL1")),
            "IX_SRC_T1_COL1",
        )
        self.assertEqual(rename_actions, [])
        self.assertEqual(detail_rows, [])

    def test_build_name_collision_plan_keeps_planned_name_when_existing_is_renamed_away(self):
        oracle_meta = self._make_oracle_meta(
            indexes={
                ("SRC", "T1"): {
                    "C1": {
                        "columns": ["COL1"],
                        "expressions": {},
                    }
                }
            },
            constraints={}
        )
        ob_meta = self._make_ob_meta(
            indexes={},
            constraints={
                ("TGT", "T1"): {
                    "C1": {
                        "type": "P",
                        "columns": ["ID"],
                        "search_condition": "",
                    }
                }
            }
        )
        index_tasks = [
            (
                sdr.IndexMismatch(
                    table="TGT.T1",
                    missing_indexes={"C1"},
                    extra_indexes=set(),
                    detail_mismatch=[],
                ),
                "SRC",
                "T1",
                "TGT",
                "T1",
            )
        ]

        planned_map, rename_actions, _detail_rows = sdr.build_name_collision_plan(
            oracle_meta,
            ob_meta,
            index_tasks,
            [],
            mode="fixup",
            rename_existing=True,
        )

        self.assertEqual(planned_map.get(("TGT", "INDEX", "T1", "C1")), "C1")
        self.assertTrue(rename_actions)
        self.assertEqual(rename_actions[0]["old_name"], "C1")
        self.assertNotEqual(rename_actions[0]["new_name"], "C1")

    def test_generate_name_collision_fixup_scripts_two_phase(self):
        actions = [
            {
                "schema": "TGT",
                "object_type": "CONSTRAINT",
                "table": "T1",
                "old_name": "C_OLD",
                "new_name": "PK_T1",
            },
            {
                "schema": "TGT",
                "object_type": "INDEX",
                "table": "T1",
                "old_name": "I_OLD",
                "new_name": "IX_T1_COL1",
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            written = sdr.generate_name_collision_fixup_scripts(base_dir, actions)
            self.assertEqual(written, 2)
            phase1 = (base_dir / "name_collision" / "TGT.phase1_temp_rename.sql").read_text(encoding="utf-8")
            phase2 = (base_dir / "name_collision" / "TGT.phase2_final_rename.sql").read_text(encoding="utf-8")
            self.assertIn("RENAME CONSTRAINT", phase1)
            self.assertIn("ALTER INDEX", phase1)
            self.assertIn('"PK_T1"', phase2)
            self.assertIn('"IX_T1_COL1"', phase2)

    def test_is_mview_log_table_name(self):
        self.assertTrue(sdr.is_mview_log_table_name("MLOG$_PERST_TRAIL"))
        self.assertTrue(sdr.is_mview_log_table_name('"mlog$_x1"'))
        self.assertFalse(sdr.is_mview_log_table_name("MLOG_TABLE"))
        self.assertFalse(sdr.is_mview_log_table_name("APP_MLOG$_T1"))

    def test_is_oracle_derived_artifact_table_name(self):
        self.assertTrue(sdr.is_oracle_derived_artifact_table_name("MLOG$_PERST_TRAIL"))
        self.assertTrue(sdr.is_oracle_derived_artifact_table_name("RUPD$_PERST_TRAIL"))
        self.assertTrue(sdr.is_oracle_derived_artifact_table_name('"snap$_trail"'))
        self.assertFalse(sdr.is_oracle_derived_artifact_table_name("RUPD_TABLE"))
        self.assertFalse(sdr.is_oracle_derived_artifact_table_name("APP_RUPD$_T1"))

    def test_collect_mview_log_artifact_table_keys_and_filter_master_list(self):
        source_objects = {
            "SRC.MLOG$_T1": {"TABLE"},
            "SRC.RUPD$_T2": {"TABLE"},
            "SRC.SNAP$_T3": {"TABLE"},
            "SRC.BIZ_T1": {"TABLE"},
            "SRC.MLOG$_V1": {"VIEW"},
        }
        keys = sdr.collect_mview_log_artifact_table_keys(source_objects)
        self.assertEqual(keys, {("SRC", "MLOG$_T1"), ("SRC", "RUPD$_T2"), ("SRC", "SNAP$_T3")})

        excluded_nodes = {(f"{schema}.{name}", "TABLE") for schema, name in keys}
        master_list = [
            ("SRC.MLOG$_T1", "TGT.MLOG$_T1", "TABLE"),
            ("SRC.RUPD$_T2", "TGT.RUPD$_T2", "TABLE"),
            ("SRC.SNAP$_T3", "TGT.SNAP$_T3", "TABLE"),
            ("SRC.BIZ_T1", "TGT.BIZ_T1", "TABLE"),
            ("SRC.V1", "TGT.V1", "VIEW"),
        ]
        filtered = sdr.filter_master_list_by_nodes(master_list, excluded_nodes)
        self.assertEqual(
            filtered,
            [
                ("SRC.BIZ_T1", "TGT.BIZ_T1", "TABLE"),
                ("SRC.V1", "TGT.V1", "VIEW"),
            ]
        )

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
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            constraint_deferrable_supported=False
        )
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
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

    def test_check_primary_objects_identity_default_on_null_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "ID": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "data_precision": 10,
                    "data_scale": 0,
                    "nullable": "N",
                    "data_default": None,
                    "char_used": None,
                    "char_length": None,
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": True,
                    "default_on_null": True,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "ID": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "N",
                        "data_default": None,
                        "char_used": None,
                        "char_length": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                        "default_on_null": False,
                    }
                }
            }
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
        issues = {issue.issue for issue in type_mismatches}
        self.assertIn("identity_missing", issues)
        self.assertIn("default_on_null_missing", issues)

    def test_check_primary_objects_detects_default_on_null_unexpected_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "data_precision": 10,
                    "data_scale": 0,
                    "nullable": "Y",
                    "data_default": "10",
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": False,
                    "default_on_null": False,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": "10",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                        "default_on_null": True,
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(len(results["mismatched"]), 1)
        issues = {issue.issue for issue in results["mismatched"][0][5]}
        self.assertIn("default_on_null_unexpected", issues)

    def test_check_primary_objects_detects_default_on_null_missing_via_ddl_fallback(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns(
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": "10",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                        "default_on_null": None,
                    }
                }
            },
            default_on_null_supported=False,
            default_on_null_columns={("A", "T1"): ("C1",)},
        )
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": "10",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                        "default_on_null": None,
                    }
                }
            },
            default_on_null_supported=False,
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(len(results["mismatched"]), 1)
        issues = {issue.issue for issue in results["mismatched"][0][5]}
        self.assertIn("default_on_null_missing", issues)

    def test_check_primary_objects_detects_nullability_tighten_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "VARCHAR2",
                    "data_length": 10,
                    "char_length": 10,
                    "char_used": "B",
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "N",
                    "data_default": None,
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "B",
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                    }
                }
            }
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
        self.assertEqual(type_mismatches[0].issue, "nullability_tighten")
        self.assertEqual(type_mismatches[0].src_type, "NOT NULL")
        self.assertEqual(type_mismatches[0].tgt_type, "NULLABLE")
        self.assertEqual(type_mismatches[0].expected_type, "NOT NULL")

    def test_check_primary_objects_detects_identity_mode_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "ID": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "char_length": None,
                    "char_used": None,
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "N",
                    "data_default": "SEQUENCE.NEXTVAL",
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": True,
                }
            }
        })._replace(identity_modes={("A", "T1"): {"ID": "ALWAYS"}})
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "ID": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "char_length": None,
                        "char_used": None,
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "N",
                        "data_default": "SEQUENCE.NEXTVAL",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": None,
                    }
                }
            }
        )._replace(identity_modes={("A", "T1"): {"ID": "BY DEFAULT"}})
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
        self.assertEqual(type_mismatches[0].issue, "identity_mode_mismatch")
        self.assertEqual(type_mismatches[0].src_type, "IDENTITY ALWAYS")
        self.assertEqual(type_mismatches[0].tgt_type, "IDENTITY BY DEFAULT")
        self.assertEqual(type_mismatches[0].expected_type, "IDENTITY ALWAYS")

    def test_check_primary_objects_detects_identity_option_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns(
            {
                ("A", "T1"): {
                    "ID": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "char_length": None,
                        "char_used": None,
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "N",
                        "data_default": "SEQ.NEXTVAL",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": True,
                    }
                }
            },
            identity_modes={("A", "T1"): {"ID": "BY DEFAULT"}},
            identity_options={("A", "T1"): {"ID": {"START WITH": "11", "INCREMENT BY": "2", "CACHE": "25"}}},
        )
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "ID": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "char_length": None,
                        "char_used": None,
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "N",
                        "data_default": "SEQ.NEXTVAL",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": True,
                    }
                }
            },
            identity_modes={("A", "T1"): {"ID": "BY DEFAULT"}},
            identity_options={("A", "T1"): {"ID": {"START WITH": "11", "INCREMENT BY": "7", "CACHE": "30"}}},
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
        self.assertEqual(type_mismatches[0].issue, "identity_option_mismatch")
        self.assertEqual(type_mismatches[0].src_type, "START WITH=11, INCREMENT BY=2, CACHE=25")
        self.assertEqual(type_mismatches[0].tgt_type, "START WITH=11, INCREMENT BY=7, CACHE=30")
        self.assertEqual(type_mismatches[0].expected_type, "START WITH=11, INCREMENT BY=2, CACHE=25")

    def test_check_primary_objects_identity_mode_mismatch_suppresses_option_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns(
            {
                ("A", "T1"): {
                    "ID": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "char_length": None,
                        "char_used": None,
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "N",
                        "data_default": "SEQ.NEXTVAL",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": True,
                    }
                }
            },
            identity_modes={("A", "T1"): {"ID": "ALWAYS"}},
            identity_options={("A", "T1"): {"ID": {"START WITH": "11", "INCREMENT BY": "2"}}},
        )
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "ID": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "char_length": None,
                        "char_used": None,
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "N",
                        "data_default": "SEQ.NEXTVAL",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": True,
                    }
                }
            },
            identity_modes={("A", "T1"): {"ID": "BY DEFAULT"}},
            identity_options={("A", "T1"): {"ID": {"START WITH": "11", "INCREMENT BY": "7"}}},
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        issues = [issue.issue for issue in results["mismatched"][0][5]]
        self.assertEqual(issues, ["identity_mode_mismatch"])

    def test_check_primary_objects_detects_identity_unexpected(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "ID": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "char_length": None,
                    "char_used": None,
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "N",
                    "data_default": None,
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": False,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "ID": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "char_length": None,
                        "char_used": None,
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "N",
                        "data_default": "SEQUENCE.NEXTVAL",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": None,
                    }
                }
            }
        )._replace(identity_modes={("A", "T1"): {"ID": "BY DEFAULT ON NULL"}})
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
        self.assertEqual(type_mismatches[0].issue, "identity_unexpected")
        self.assertEqual(type_mismatches[0].src_type, "NO IDENTITY")
        self.assertEqual(type_mismatches[0].tgt_type, "IDENTITY BY DEFAULT ON NULL")

    def test_ob_metadata_defaults_are_immutable(self):
        meta1 = sdr.ObMetadata(
            objects_by_type={},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
        )
        self.assertIsInstance(meta1.temporary_tables, frozenset)
        meta2 = sdr.ObMetadata(
            objects_by_type={},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
        )
        self.assertIsInstance(meta1.temporary_tables, frozenset)
        self.assertEqual(meta1.temporary_tables, frozenset())
        self.assertEqual(meta2.temporary_tables, frozenset())
        self.assertEqual(dict(meta1.identity_modes), {})
        self.assertEqual(dict(meta2.identity_modes), {})
        with self.assertRaises(AttributeError):
            getattr(meta1.temporary_tables, "add")

    def test_oracle_metadata_defaults_are_immutable(self):
        meta1 = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
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
        meta2 = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
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
        self.assertIsInstance(meta1.temporary_tables, frozenset)
        self.assertEqual(meta1.temporary_tables, frozenset())
        self.assertEqual(meta2.temporary_tables, frozenset())
        self.assertEqual(dict(meta1.identity_modes), {})
        self.assertEqual(dict(meta2.identity_modes), {})
        with self.assertRaises(AttributeError):
            getattr(meta1.temporary_tables, "add")

    def test_compare_sequences_for_schema_uses_loaded_schemas_to_report_extra_sequences(self):
        oracle_meta = self._make_oracle_meta(
            sequences={},
            loaded_schemas={"SCOTT"},
        )
        ob_meta = self._make_ob_meta(
            sequences={"TARGET": {"SEQ1"}},
        )

        ok, mismatch = sdr.compare_sequences_for_schema(
            oracle_meta,
            ob_meta,
            "SCOTT",
            "TARGET",
        )

        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.extra_sequences, {"SEQ1"})
        self.assertIn("目标端存在序列", mismatch.note)

    def test_compare_sequences_for_schema_skips_when_schema_not_loaded(self):
        oracle_meta = self._make_oracle_meta(
            sequences={},
            loaded_schemas={"OTHER"},
        )
        ob_meta = self._make_ob_meta(
            sequences={"TARGET": {"SEQ1"}},
        )

        ok, mismatch = sdr.compare_sequences_for_schema(
            oracle_meta,
            ob_meta,
            "SCOTT",
            "TARGET",
        )

        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_check_primary_objects_detects_nullability_relax_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
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
        })
        ob_meta = self._make_ob_meta_with_columns(
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
                        "nullable": "N",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                    }
                }
            }
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
        self.assertEqual(type_mismatches[0].issue, "nullability_relax")
        self.assertEqual(type_mismatches[0].src_type, "NULLABLE")
        self.assertEqual(type_mismatches[0].tgt_type, "NOT NULL")
        self.assertEqual(type_mismatches[0].expected_type, "NULLABLE")

    def test_check_primary_objects_detects_system_notnull_novalidate_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
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
        })._replace(constraints={
            ("A", "T1"): {
                "SYS_C001234": {
                    "type": "C",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                    "search_condition": '"C1" IS NOT NULL',
                    "columns": ["C1"],
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
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
            }
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
        self.assertEqual(type_mismatches[0].issue, "nullability_novalidate_tighten")
        self.assertEqual(type_mismatches[0].src_type, "NOT NULL ENABLE NOVALIDATE")
        self.assertEqual(type_mismatches[0].tgt_type, "NULLABLE")
        self.assertEqual(type_mismatches[0].expected_type, "NOT NULL ENABLE NOVALIDATE")

    def test_check_primary_objects_detects_system_notnull_novalidate_mismatch_with_hash_column(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "PK_SERIAL#": {
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
        })._replace(constraints={
            ("A", "T1"): {
                "SYS_C001234": {
                    "type": "C",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                    "search_condition": '"PK_SERIAL#" IS NOT NULL',
                    "columns": ["PK_SERIAL#"],
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "PK_SERIAL#": {
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
            }
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
        self.assertEqual(type_mismatches[0].issue, "nullability_novalidate_tighten")

    def test_check_primary_objects_skips_system_notnull_novalidate_when_target_has_equivalent_check(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
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
        })._replace(constraints={
            ("A", "T1"): {
                "SYS_C001234": {
                    "type": "C",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                    "search_condition": '"C1" IS NOT NULL',
                    "columns": ["C1"],
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
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
            }
        )._replace(constraints={
            ("A", "T1"): {
                "T1_OBCHECK_1": {
                    "type": "C",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                    "search_condition": '"C1" IS NOT NULL',
                    "columns": ["C1"],
                }
            }
        })
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(results["mismatched"], [])

    def test_check_primary_objects_uses_precomputed_enabled_notnull_check_columns(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
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
        })._replace(constraints={
            ("A", "T1"): {
                "SYS_C001234": {
                    "type": "C",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                    "search_condition": '"C1" IS NOT NULL',
                    "columns": ["C1"],
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
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
            }
        )._replace(
            constraints={("A", "T1"): {}},
            enabled_notnull_check_columns={
                ("A", "T1"): {
                    "C1": {
                        "constraint_name": "T1_OBCHECK_1",
                        "search_condition": '"C1" IS NOT NULL',
                        "status": "ENABLED",
                        "validated": "NOT VALIDATED",
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(results["mismatched"], [])

    def test_check_primary_objects_inline_novalidate_notnull_with_equivalent_obnotnull_is_clean(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "ID_COL": {
                    "data_type": "VARCHAR2",
                    "data_length": 32,
                    "char_length": 32,
                    "char_used": "B",
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "N",
                    "data_default": "TO_CHAR(RAWTOHEX(SYS_GUID()))",
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                }
            }
        })._replace(constraints={
            ("A", "T1"): {
                "SYS_C001234": {
                    "type": "C",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                    "search_condition": '"ID_COL" IS NOT NULL',
                    "columns": ["ID_COL"],
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "ID_COL": {
                        "data_type": "VARCHAR2",
                        "data_length": 48,
                        "char_length": 48,
                        "char_used": "B",
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "Y",
                        "data_default": "TO_CHAR(RAWTOHEX(SYS_GUID()))",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                    }
                }
            }
        )._replace(
            constraints={("A", "T1"): {}},
            enabled_notnull_check_columns={
                ("A", "T1"): {
                    "ID_COL": {
                        "constraint_name": "T1_OBNOTNULL_1",
                        "search_condition": '"ID_COL" IS NOT NULL',
                        "status": "ENABLED",
                        "validated": "NOT VALIDATED",
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(results["mismatched"], [])

    def test_check_primary_objects_inline_novalidate_notnull_missing_equivalent_check_uses_novalidate_issue(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "ID_COL": {
                    "data_type": "VARCHAR2",
                    "data_length": 32,
                    "char_length": 32,
                    "char_used": "B",
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "N",
                    "data_default": "TO_CHAR(RAWTOHEX(SYS_GUID()))",
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                }
            }
        })._replace(constraints={
            ("A", "T1"): {
                "SYS_C001234": {
                    "type": "C",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                    "search_condition": '"ID_COL" IS NOT NULL',
                    "columns": ["ID_COL"],
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "ID_COL": {
                        "data_type": "VARCHAR2",
                        "data_length": 48,
                        "char_length": 48,
                        "char_used": "B",
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "Y",
                        "data_default": "TO_CHAR(RAWTOHEX(SYS_GUID()))",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                    }
                }
            }
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
        self.assertEqual(type_mismatches[0].issue, "nullability_novalidate_tighten")
        self.assertEqual(type_mismatches[0].expected_type, "NOT NULL ENABLE NOVALIDATE")

    def test_check_primary_objects_detects_default_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "VARCHAR2",
                    "data_length": 10,
                    "char_length": 10,
                    "char_used": "B",
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "Y",
                    "data_default": "'SRC'",
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": False,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "B",
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "Y",
                        "data_default": "'TGT'",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                    }
                }
            }
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
        self.assertEqual(type_mismatches[0].issue, "default_mismatch")
        self.assertEqual(type_mismatches[0].src_type, "'SRC'")
        self.assertEqual(type_mismatches[0].tgt_type, "'TGT'")
        self.assertEqual(type_mismatches[0].expected_type, "'SRC'")

    def test_check_primary_objects_ignores_default_function_case_only_difference(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "RAW",
                    "data_length": 16,
                    "char_length": None,
                    "char_used": None,
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "Y",
                    "data_default": "rowtohex(sys_guid())",
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": False,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "RAW",
                        "data_length": 16,
                        "char_length": None,
                        "char_used": None,
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "Y",
                        "data_default": "ROWTOHEX(SYS_GUID())",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(results["mismatched"], [])

    def test_normalize_column_default_expression_semantic_literals(self):
        self.assertEqual(
            sdr.normalize_column_default_expression("0.98"),
            sdr.normalize_column_default_expression(".98"),
        )
        self.assertEqual(
            sdr.normalize_column_default_expression("0.0"),
            sdr.normalize_column_default_expression("0"),
        )
        self.assertEqual(
            sdr.normalize_column_default_expression("0.00"),
            sdr.normalize_column_default_expression("0"),
        )
        self.assertEqual(
            sdr.normalize_column_default_expression("0.001"),
            sdr.normalize_column_default_expression(".001"),
        )
        self.assertEqual(
            sdr.normalize_column_default_expression("0.0000"),
            sdr.normalize_column_default_expression("0"),
        )
        self.assertEqual(
            sdr.normalize_column_default_expression("DATE '1990-1-1'"),
            sdr.normalize_column_default_expression("DATE '1990-01-01'"),
        )
        self.assertEqual(
            sdr.normalize_column_default_expression("-1"),
            sdr.normalize_column_default_expression("-(1)"),
        )
        self.assertEqual(
            sdr.normalize_column_default_expression("USER--更新人"),
            sdr.normalize_column_default_expression("user"),
        )

    def test_describe_column_default_expression_strips_trailing_comment_noise(self):
        self.assertEqual(
            sdr.describe_column_default_expression("USER--更新人"),
            "USER",
        )

    def test_check_primary_objects_ignores_semantically_equivalent_default_forms(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 2, "nullable": "Y", "data_default": "0.98", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                "C2": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 1, "nullable": "Y", "data_default": "0.0", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                "C3": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 2, "nullable": "Y", "data_default": "0.00", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                "C4": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 3, "nullable": "Y", "data_default": "0.001", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                "C5": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 4, "nullable": "Y", "data_default": "0.0000", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                "C6": {"data_type": "DATE", "data_length": None, "char_length": None, "char_used": None, "data_precision": None, "data_scale": None, "nullable": "Y", "data_default": "DATE '1990-1-1'", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                "C7": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 0, "nullable": "Y", "data_default": "-1", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                "C8": {"data_type": "VARCHAR2", "data_length": 128, "char_length": 128, "char_used": "B", "data_precision": None, "data_scale": None, "nullable": "Y", "data_default": "USER--更新人", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 2, "nullable": "Y", "data_default": ".98", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                    "C2": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 1, "nullable": "Y", "data_default": "0", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                    "C3": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 2, "nullable": "Y", "data_default": "0", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                    "C4": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 3, "nullable": "Y", "data_default": ".001", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                    "C5": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 4, "nullable": "Y", "data_default": "0", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                    "C6": {"data_type": "DATE", "data_length": None, "char_length": None, "char_used": None, "data_precision": None, "data_scale": None, "nullable": "Y", "data_default": "DATE '1990-01-01'", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                    "C7": {"data_type": "NUMBER", "data_length": None, "char_length": None, "char_used": None, "data_precision": 10, "data_scale": 0, "nullable": "Y", "data_default": "-(1)", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                    "C8": {"data_type": "VARCHAR2", "data_length": 192, "char_length": 192, "char_used": "B", "data_precision": None, "data_scale": None, "nullable": "Y", "data_default": "user", "hidden": False, "virtual": False, "virtual_expr": None, "identity": False},
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(results["mismatched"], [])

    def test_check_primary_objects_treats_null_default_as_no_default(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
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
                    "identity": False,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
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
                        "data_default": "NULL",
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(results["mismatched"], [])

    def test_check_primary_objects_ignores_hidden_source_columns_in_target(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "VARCHAR2",
                    "data_length": 10,
                    "char_length": 10,
                    "char_used": "C",
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "Y",
                    "data_default": None,
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                },
                "HIDDEN_COL": {
                    "data_type": "VARCHAR2",
                    "data_length": 10,
                    "char_length": 10,
                    "char_used": "B",
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "Y",
                    "data_default": None,
                    "hidden": True,
                    "virtual": False,
                    "virtual_expr": None,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "C",
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                    },
                    "HIDDEN_COL": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "B",
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                    },
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(results["mismatched"], [])
        self.assertEqual(results["missing"], [])
        self.assertIn(("TABLE", "A.T1"), results["ok"])

    def test_check_primary_objects_column_order_disabled(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "data_precision": 10,
                    "data_scale": 0,
                    "nullable": "Y",
                    "data_default": None,
                    "char_used": None,
                    "char_length": None,
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": None,
                    "default_on_null": None,
                    "invisible": None,
                    "column_id": 1,
                },
                "C2": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "data_precision": 10,
                    "data_scale": 0,
                    "nullable": "Y",
                    "data_default": None,
                    "char_used": None,
                    "char_length": None,
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": None,
                    "default_on_null": None,
                    "invisible": None,
                    "column_id": 2,
                },
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": None,
                        "char_used": None,
                        "char_length": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": None,
                        "default_on_null": None,
                        "invisible": None,
                        "column_id": 2,
                    },
                    "C2": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": None,
                        "char_used": None,
                        "char_length": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": None,
                        "default_on_null": None,
                        "invisible": None,
                        "column_id": 1,
                    },
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
            settings={"enable_column_order_check": False}
        )
        self.assertEqual(results["column_order_mismatched"], [])
        self.assertEqual(results["column_order_skipped"], [])

    def test_check_primary_objects_column_order_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "data_precision": 10,
                    "data_scale": 0,
                    "nullable": "Y",
                    "data_default": None,
                    "char_used": None,
                    "char_length": None,
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": None,
                    "default_on_null": None,
                    "invisible": None,
                    "column_id": 1,
                },
                "C2": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "data_precision": 10,
                    "data_scale": 0,
                    "nullable": "Y",
                    "data_default": None,
                    "char_used": None,
                    "char_length": None,
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": None,
                    "default_on_null": None,
                    "invisible": None,
                    "column_id": 2,
                },
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": None,
                        "char_used": None,
                        "char_length": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": None,
                        "default_on_null": None,
                        "invisible": None,
                        "column_id": 2,
                    },
                    "C2": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": None,
                        "char_used": None,
                        "char_length": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": None,
                        "default_on_null": None,
                        "invisible": None,
                        "column_id": 1,
                    },
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
            settings={"enable_column_order_check": True}
        )
        self.assertEqual(results["mismatched"], [])
        self.assertEqual(len(results["column_order_mismatched"]), 1)
        mismatch = results["column_order_mismatched"][0]
        self.assertEqual(mismatch.table, "A.T1")
        self.assertEqual(mismatch.src_order, ("C1", "C2"))
        self.assertEqual(mismatch.tgt_order, ("C2", "C1"))

    def test_check_primary_objects_column_order_filters_noise(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "data_precision": 10,
                    "data_scale": 0,
                    "nullable": "Y",
                    "data_default": None,
                    "char_used": None,
                    "char_length": None,
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": None,
                    "default_on_null": None,
                    "invisible": None,
                    "column_id": 1,
                },
                "C2": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "data_precision": 10,
                    "data_scale": 0,
                    "nullable": "Y",
                    "data_default": None,
                    "char_used": None,
                    "char_length": None,
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                    "identity": None,
                    "default_on_null": None,
                    "invisible": None,
                    "column_id": 2,
                },
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": None,
                        "char_used": None,
                        "char_length": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": None,
                        "default_on_null": None,
                        "invisible": None,
                        "column_id": 1,
                    },
                    "__PK_INCREMENT": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": None,
                        "char_used": None,
                        "char_length": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": None,
                        "default_on_null": None,
                        "invisible": None,
                        "column_id": 2,
                    },
                    "C2": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "Y",
                        "data_default": None,
                        "char_used": None,
                        "char_length": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": None,
                        "default_on_null": None,
                        "invisible": None,
                        "column_id": 3,
                    },
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
            settings={"enable_column_order_check": True}
        )
        self.assertEqual(results["column_order_mismatched"], [])
        self.assertEqual(results["column_order_skipped"], [])

    def test_check_primary_objects_skips_virtual_length_rule(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "V1": {
                    "data_type": "VARCHAR2",
                    "data_length": 30,
                    "char_length": 30,
                    "char_used": "B",
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "Y",
                    "data_default": None,
                    "hidden": False,
                    "virtual": True,
                    "virtual_expr": "BASE_COL + 1",
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "V1": {
                        "data_type": "VARCHAR2",
                        "data_length": 30,
                        "char_length": 30,
                        "char_used": "B",
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": True,
                        "virtual_expr": "BASE_COL + 1",
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(results["mismatched"], [])
        self.assertIn(("TABLE", "A.T1"), results["ok"])

    def test_check_primary_objects_skips_identity_default_on_null_when_unsupported(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns(
            {
                ("A", "T1"): {
                    "ID": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "N",
                        "data_default": None,
                        "char_used": None,
                        "char_length": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": True,
                        "default_on_null": True,
                    }
                }
            },
            identity_supported=True,
            default_on_null_supported=True,
        )
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "ID": {
                        "data_type": "NUMBER",
                        "data_length": None,
                        "data_precision": 10,
                        "data_scale": 0,
                        "nullable": "N",
                        "data_default": None,
                        "char_used": None,
                        "char_length": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                        "default_on_null": False,
                    }
                }
            },
            identity_supported=False,
            default_on_null_supported=False,
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(results["mismatched"], [])
        self.assertIn(("TABLE", "A.T1"), results["ok"])

    def test_check_primary_objects_number_precision_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "data_precision": 10,
                    "data_scale": 2,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_precision": 8,
                        "data_scale": 2,
                    }
                }
            }
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
        self.assertEqual(type_mismatches[0].issue, "number_precision")
        self.assertEqual(type_mismatches[0].expected_type, "NUMBER(10,2)")

    def test_check_primary_objects_number_star_zero_equivalence(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "data_precision": None,
                    "data_scale": 0,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_precision": 38,
                        "data_scale": 0,
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(len(results["mismatched"]), 0)
        self.assertEqual(len(results["ok"]), 1)

    def test_check_primary_objects_number_star_scale_equivalence(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "data_precision": None,
                    "data_scale": 2,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_precision": 38,
                        "data_scale": 2,
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(len(results["mismatched"]), 0)
        self.assertEqual(len(results["ok"]), 1)

    def test_check_primary_objects_number_star_scale_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "data_precision": None,
                    "data_scale": 2,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_precision": 37,
                        "data_scale": 2,
                    }
                }
            }
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
        self.assertEqual(type_mismatches[0].issue, "number_precision")
        self.assertEqual(type_mismatches[0].expected_type, "NUMBER(38,2)")

    def test_check_primary_objects_number_decimal_equivalence(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "DECIMAL",
                    "data_precision": 10,
                    "data_scale": 0,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_precision": 10,
                        "data_scale": 0,
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(len(results["mismatched"]), 0)
        self.assertEqual(len(results["ok"]), 1)

    def test_check_primary_objects_number_scale_default_equivalence(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "data_precision": 12,
                    "data_scale": None,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_precision": 12,
                        "data_scale": 0,
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(len(results["mismatched"]), 0)
        self.assertEqual(len(results["ok"]), 1)

    def test_check_primary_objects_number_star_zero_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "data_precision": None,
                    "data_scale": 0,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "data_precision": 37,
                        "data_scale": 0,
                    }
                }
            }
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
        self.assertEqual(type_mismatches[0].issue, "number_precision")

    def test_check_primary_objects_char_semantics_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "VARCHAR2",
                    "char_length": 10,
                    "data_length": 10,
                    "char_used": "C",
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "char_length": 10,
                        "data_length": 10,
                        "char_used": "B",
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(len(results["mismatched"]), 1)
        length_mismatches = results["mismatched"][0][4]
        self.assertEqual(len(length_mismatches), 1)
        self.assertEqual(length_mismatches[0].issue, "char_mismatch")
        self.assertEqual(length_mismatches[0].limit_length, 10)

    def test_check_primary_objects_virtual_column_mismatch(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "virtual": True,
                    "virtual_expr": "COL0 + 1",
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "virtual": False,
                    }
                }
            }
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
        self.assertEqual(type_mismatches[0].issue, "virtual_missing")

    def test_check_primary_objects_virtual_expr_whitespace_equivalent(self):
        master_list = [("A.T1", "A.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("A", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "virtual": True,
                    "virtual_expr": "BASE_COL+1",
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"A.T1"}},
            {
                ("A", "T1"): {
                    "C1": {
                        "data_type": "NUMBER",
                        "virtual": True,
                        "virtual_expr": "BASE_COL + 1",
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
        )
        self.assertEqual(len(results["mismatched"]), 0)
        self.assertEqual(len(results["ok"]), 1)

    def test_check_primary_objects_visibility_mismatch(self):
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns(
            {
                ("SRC", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "C",
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                        "default_on_null": False,
                        "invisible": True,
                    }
                }
            },
            invisible_supported=True
        )
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"TGT.T1"}},
            {
                ("TGT", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "C",
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                        "default_on_null": False,
                        "invisible": False,
                    }
                }
            },
            invisible_supported=True
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
            settings={"column_visibility_policy": "auto"}
        )
        self.assertEqual(len(results["mismatched"]), 1)
        type_mismatches = results["mismatched"][0][5]
        self.assertEqual(len(type_mismatches), 1)
        self.assertEqual(type_mismatches[0].issue, "visibility_mismatch")
        self.assertEqual(type_mismatches[0].expected_type, "INVISIBLE")

    def test_check_primary_objects_visibility_ignored(self):
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns(
            {
                ("SRC", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "C",
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                        "default_on_null": False,
                        "invisible": True,
                    }
                }
            },
            invisible_supported=True
        )
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"TGT.T1"}},
            {
                ("TGT", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "C",
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                        "default_on_null": False,
                        "invisible": False,
                    }
                }
            },
            invisible_supported=True
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
            settings={"column_visibility_policy": "ignore"}
        )
        self.assertEqual(results["mismatched"], [])
        self.assertEqual(len(results["ok"]), 1)

    def test_check_primary_objects_records_visibility_skip_when_auto_metadata_incomplete(self):
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns(
            {
                ("SRC", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "C",
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                        "default_on_null": False,
                        "invisible": True,
                    }
                }
            },
            invisible_supported=True
        )
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"TGT.T1"}},
            {
                ("TGT", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "C",
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                        "identity": False,
                        "default_on_null": False,
                        "invisible": None,
                    }
                }
            },
            invisible_supported=False
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
            settings={"column_visibility_policy": "auto"}
        )
        self.assertEqual(results["mismatched"], [])
        self.assertEqual(len(results["ok"]), 1)
        self.assertEqual(len(results["visibility_skipped"]), 1)
        skip_row = results["visibility_skipped"][0]
        self.assertEqual(skip_row.target_table, "TGT.T1")
        self.assertEqual(skip_row.source_metadata, "READY")
        self.assertEqual(skip_row.target_metadata, "MISSING")
        self.assertIn("INVISIBLE compare/fixup", skip_row.reason)

    def test_build_invisible_column_alter_sql(self):
        oracle_meta = self._make_oracle_meta_with_columns(
            {
                ("SRC", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "B",
                        "hidden": False,
                        "virtual": False,
                        "invisible": True,
                    }
                }
            },
            invisible_supported=True
        )
        sql = sdr.build_invisible_column_alter_sql(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            True
        )
        self.assertIsNotNone(sql)
        self.assertIn('ALTER TABLE "TGT"."T1" MODIFY C1 INVISIBLE;', sql)

    def test_generate_alter_for_table_columns_visibility_mismatch(self):
        oracle_meta = self._make_oracle_meta_with_columns(
            {
                ("SRC", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "B",
                        "nullable": "Y",
                        "hidden": False,
                        "virtual": False,
                        "identity": False,
                        "default_on_null": False,
                    }
                }
            }
        )
        type_mismatches = [
            sdr.ColumnTypeIssue(
                "C1",
                "VISIBLE",
                "INVISIBLE",
                "INVISIBLE",
                "visibility_mismatch"
            )
        ]
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols=set(),
            length_mismatches=[],
            type_mismatches=type_mismatches
        )
        self.assertIsNotNone(sql)
        self.assertIn("MODIFY C1 INVISIBLE", sql)

    def test_generate_alter_for_table_columns_sys_c_drop_enabled(self):
        oracle_meta = self._make_oracle_meta_with_columns({("SRC", "T1"): {}})
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols={"SYS_C00025_2025121711:29:07$", "SYS_C_00019$", "EXTRA1"},
            length_mismatches=[],
            type_mismatches=[],
            drop_sys_c_columns=True
        )
        self.assertIsNotNone(sql)
        self.assertIn('ALTER TABLE "TGT"."T1" FORCE;', sql)
        self.assertIn('-- ALTER TABLE "TGT"."T1" DROP COLUMN EXTRA1;', sql)
        self.assertNotIn('DROP COLUMN SYS_C00025', sql)
        self.assertNotIn('DROP COLUMN SYS_C_00019', sql)

    def test_generate_alter_for_table_columns_sys_c_drop_disabled(self):
        oracle_meta = self._make_oracle_meta_with_columns({("SRC", "T1"): {}})
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols={"SYS_C000123"},
            length_mismatches=[],
            type_mismatches=[],
            drop_sys_c_columns=False
        )
        self.assertIsNone(sql)

    def test_generate_alter_for_table_columns_sys_c_drop_disabled_keeps_non_sys_extra(self):
        oracle_meta = self._make_oracle_meta_with_columns({("SRC", "T1"): {}})
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols={"SYS_C000123", "EXTRA_A"},
            length_mismatches=[],
            type_mismatches=[],
            drop_sys_c_columns=False
        )
        self.assertIsNotNone(sql)
        self.assertIn('-- ALTER TABLE "TGT"."T1" DROP COLUMN EXTRA_A;', sql)
        self.assertNotIn('DROP COLUMN SYS_C000123', sql)
        self.assertNotIn('\nALTER TABLE "TGT"."T1" FORCE;', "\n" + sql)

    def test_generate_alter_for_table_columns_nullability_review_only(self):
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "C1": {
                    "data_type": "VARCHAR2",
                    "data_length": 10,
                    "char_length": 10,
                    "char_used": "B",
                    "nullable": "N",
                    "data_default": None,
                    "hidden": False,
                    "virtual": False,
                    "identity": False,
                    "default_on_null": False,
                }
            }
        })
        type_mismatches = [
            sdr.ColumnTypeIssue(
                "C1",
                "NOT NULL",
                "NULLABLE",
                "NOT NULL",
                "nullability_tighten"
            )
        ]
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols=set(),
            length_mismatches=[],
            type_mismatches=type_mismatches
        )
        self.assertIsNotNone(sql)
        self.assertIn("REVIEW-FIRST: C1 源端为 NOT NULL，目标端仍为可空", sql)
        self.assertIn('-- ALTER TABLE "TGT"."T1" MODIFY (C1 VARCHAR(10) NOT NULL);', sql)

    def test_generate_alter_for_table_columns_nullability_runnable_if_no_nulls(self):
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "C1": {
                    "data_type": "VARCHAR2",
                    "data_length": 10,
                    "char_length": 10,
                    "char_used": "B",
                    "nullable": "N",
                    "data_default": None,
                    "hidden": False,
                    "virtual": False,
                    "identity": False,
                    "default_on_null": False,
                }
            }
        })
        type_mismatches = [
            sdr.ColumnTypeIssue(
                "C1",
                "NOT NULL",
                "NULLABLE",
                "NOT NULL",
                "nullability_tighten"
            )
        ]
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols=set(),
            length_mismatches=[],
            type_mismatches=type_mismatches,
            plain_not_null_fixup_mode="runnable_if_no_nulls",
            plain_not_null_probe_results={"C1": (False, "")},
        )
        self.assertIsNotNone(sql)
        self.assertIn("AUTO-GUARD: C1 目标端未探测到 NULL", sql)
        self.assertIn('ALTER TABLE "TGT"."T1" MODIFY (C1 VARCHAR(10) NOT NULL);', sql)
        self.assertNotIn('-- ALTER TABLE "TGT"."T1" MODIFY (C1 VARCHAR(10) NOT NULL);', sql)

    def test_generate_alter_for_table_columns_nullability_guarded_when_nulls_found(self):
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "C1": {
                    "data_type": "VARCHAR2",
                    "data_length": 10,
                    "char_length": 10,
                    "char_used": "B",
                    "nullable": "N",
                    "data_default": None,
                    "hidden": False,
                    "virtual": False,
                    "identity": False,
                    "default_on_null": False,
                }
            }
        })
        type_mismatches = [
            sdr.ColumnTypeIssue(
                "C1",
                "NOT NULL",
                "NULLABLE",
                "NOT NULL",
                "nullability_tighten"
            )
        ]
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols=set(),
            length_mismatches=[],
            type_mismatches=type_mismatches,
            plain_not_null_fixup_mode="runnable_if_no_nulls",
            plain_not_null_probe_results={"C1": (True, "")},
        )
        self.assertIsNotNone(sql)
        self.assertIn("AUTO-GUARD: C1 目标端已探测到 NULL 数据", sql)
        self.assertIn('-- ALTER TABLE "TGT"."T1" MODIFY (C1 VARCHAR(10) NOT NULL);', sql)

    def test_generate_alter_for_table_columns_novalidate_review_only(self):
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "C1": {
                    "data_type": "VARCHAR2",
                    "data_length": 10,
                    "char_length": 10,
                    "char_used": "B",
                    "nullable": "Y",
                    "data_default": None,
                    "hidden": False,
                    "virtual": False,
                    "identity": False,
                    "default_on_null": False,
                }
            }
        })._replace(constraints={
            ("SRC", "T1"): {
                "SYS_C001234": {
                    "type": "C",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                    "search_condition": '"C1" IS NOT NULL',
                    "columns": ["C1"],
                }
            }
        })
        type_mismatches = [
            sdr.ColumnTypeIssue(
                "C1",
                "NOT NULL ENABLE NOVALIDATE",
                "NULLABLE",
                "NOT NULL ENABLE NOVALIDATE",
                "nullability_novalidate_tighten"
            )
        ]
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols=set(),
            length_mismatches=[],
            type_mismatches=type_mismatches
        )
        self.assertIsNotNone(sql)
        self.assertIn("REVIEW-FIRST: C1 源端为 NOT NULL ENABLE NOVALIDATE，目标端仍为可空", sql)
        self.assertIn('SYS_C001234 CHECK (C1 IS NOT NULL) ENABLE NOVALIDATE', sql)
        self.assertIn('ALTER TABLE "TGT"."T1" ADD CONSTRAINT "NN_T1_C1" CHECK (C1 IS NOT NULL) ENABLE NOVALIDATE;', sql)
        self.assertIn('-- ALTER TABLE "TGT"."T1" MODIFY (C1 VARCHAR(10) NOT NULL);', sql)

    def test_generate_alter_for_table_columns_default_review_only(self):
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "C1": {
                    "data_type": "VARCHAR2",
                    "data_length": 10,
                    "char_length": 10,
                    "char_used": "B",
                    "nullable": "Y",
                    "data_default": "'SRC'",
                    "hidden": False,
                    "virtual": False,
                    "identity": False,
                    "default_on_null": False,
                }
            }
        })
        type_mismatches = [
            sdr.ColumnTypeIssue(
                "C1",
                "'SRC'",
                "NO DEFAULT",
                "'SRC'",
                "default_missing"
            )
        ]
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols=set(),
            length_mismatches=[],
            type_mismatches=type_mismatches
        )
        self.assertIsNotNone(sql)
        self.assertIn("REVIEW-FIRST: C1 默认值与源端不一致", sql)
        self.assertIn('-- ALTER TABLE "TGT"."T1" MODIFY (C1 DEFAULT \'SRC\');', sql)

    def test_generate_alter_for_table_columns_default_on_null_review_only(self):
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "C1": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "data_precision": 10,
                    "data_scale": 0,
                    "nullable": "Y",
                    "data_default": "10",
                    "hidden": False,
                    "virtual": False,
                    "identity": False,
                    "default_on_null": True,
                },
                "C2": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "data_precision": 10,
                    "data_scale": 0,
                    "nullable": "Y",
                    "data_default": "10",
                    "hidden": False,
                    "virtual": False,
                    "identity": False,
                    "default_on_null": False,
                },
            }
        })
        type_mismatches = [
            sdr.ColumnTypeIssue(
                "C1",
                "DEFAULT ON NULL",
                "NO DEFAULT ON NULL",
                "DEFAULT ON NULL",
                "default_on_null_missing"
            ),
            sdr.ColumnTypeIssue(
                "C2",
                "NO DEFAULT ON NULL",
                "DEFAULT ON NULL",
                "NO DEFAULT ON NULL",
                "default_on_null_unexpected"
            ),
        ]
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols=set(),
            length_mismatches=[],
            type_mismatches=type_mismatches
        )
        self.assertIsNotNone(sql)
        self.assertIn("REVIEW-FIRST: C1 源端含 DEFAULT ON NULL 语义，目标端缺失。", sql)
        self.assertIn("REVIEW-FIRST: C2 目标端额外存在 DEFAULT ON NULL 语义，源端无该语义。", sql)

    def test_generate_alter_for_table_columns_identity_option_review_only(self):
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "ID": {
                    "data_type": "NUMBER",
                    "data_length": None,
                    "data_precision": 10,
                    "data_scale": 0,
                    "nullable": "N",
                    "data_default": "SEQ.NEXTVAL",
                    "hidden": False,
                    "virtual": False,
                    "identity": True,
                    "default_on_null": False,
                }
            }
        })
        type_mismatches = [
            sdr.ColumnTypeIssue(
                "ID",
                "START WITH=11, INCREMENT BY=2, CACHE=25",
                "START WITH=11, INCREMENT BY=7, CACHE=30",
                "START WITH=11, INCREMENT BY=2, CACHE=25",
                "identity_option_mismatch"
            )
        ]
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols=set(),
            length_mismatches=[],
            type_mismatches=type_mismatches
        )
        self.assertIsNotNone(sql)
        self.assertIn("REVIEW-FIRST: ID identity 细项与源端不一致", sql)
        self.assertIn("当前仅覆盖 START WITH / INCREMENT BY / CACHE", sql)

    def test_export_column_nullability_detail(self):
        mismatched_items = [
            (
                "TABLE",
                "A.T1",
                set(),
                set(),
                [],
                [
                    sdr.ColumnTypeIssue("C1", "NOT NULL", "NULLABLE", "NOT NULL", "nullability_tighten"),
                    sdr.ColumnTypeIssue(
                        "C3",
                        "NOT NULL ENABLE NOVALIDATE",
                        "NULLABLE",
                        "NOT NULL ENABLE NOVALIDATE",
                        "nullability_novalidate_tighten"
                    ),
                    sdr.ColumnTypeIssue("C2", "NULLABLE", "NOT NULL", "NULLABLE", "nullability_relax"),
                ],
            )
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = sdr.export_column_nullability_detail(
                mismatched_items,
                Path(tmp_dir),
                "20260316_120000"
            )
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
        self.assertIn("列空值语义差异明细", content)
        self.assertIn("A.T1|C1|NOT NULL|NULLABLE|NOT NULL|REVIEW_NOT_NULL", content)
        self.assertIn(
            "A.T1|C3|NOT NULL ENABLE NOVALIDATE|NULLABLE|NOT NULL ENABLE NOVALIDATE|REVIEW_NOT_NULL_NOVALIDATE",
            content
        )
        self.assertIn("A.T1|C2|NULLABLE|NOT NULL|NULLABLE|REVIEW_NULLABLE", content)

    def test_export_column_default_detail(self):
        mismatched_items = [
            (
                "TABLE",
                "A.T1",
                set(),
                set(),
                [],
                [
                    sdr.ColumnTypeIssue("C1", "'SRC'", "NO DEFAULT", "'SRC'", "default_missing"),
                    sdr.ColumnTypeIssue("C2", "NO DEFAULT", "'TGT'", "NO DEFAULT", "default_extra"),
                ],
            )
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = sdr.export_column_default_detail(
                mismatched_items,
                Path(tmp_dir),
                "20260316_120000"
            )
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
        self.assertIn("列默认值差异明细", content)
        self.assertIn("A.T1|C1|'SRC'|NO DEFAULT|'SRC'|REVIEW_SET_DEFAULT", content)
        self.assertIn("A.T1|C2|NO DEFAULT|'TGT'|NO DEFAULT|REVIEW_DROP_DEFAULT", content)

    def test_export_column_default_detail_preserves_source_function_case(self):
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
                        "rowtohex(sys_guid())",
                        "NO DEFAULT",
                        "rowtohex(sys_guid())",
                        "default_missing"
                    ),
                ],
            )
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = sdr.export_column_default_detail(
                mismatched_items,
                Path(tmp_dir),
                "20260325_120000"
            )
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
        self.assertIn("A.T1|C1|rowtohex(sys_guid())|NO DEFAULT|rowtohex(sys_guid())|REVIEW_SET_DEFAULT", content)

    def test_generate_alter_for_table_columns_default_preserves_source_function_case(self):
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "C1": {
                    "data_type": "VARCHAR2",
                    "data_length": 32,
                    "char_length": 32,
                    "char_used": "B",
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "Y",
                    "data_default": "rowtohex(sys_guid())",
                    "hidden": False,
                    "virtual": False,
                    "identity": False,
                    "default_on_null": False,
                }
            }
        })
        type_mismatches = [
            sdr.ColumnTypeIssue(
                "C1",
                "rowtohex(sys_guid())",
                "NO DEFAULT",
                "rowtohex(sys_guid())",
                "default_missing"
            )
        ]
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            missing_cols=set(),
            extra_cols=set(),
            length_mismatches=[],
            type_mismatches=type_mismatches
        )
        self.assertIsNotNone(sql)
        self.assertIn('-- ALTER TABLE "TGT"."T1" MODIFY (C1 DEFAULT rowtohex(sys_guid()));', sql)

    def test_export_column_default_on_null_detail(self):
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
                        "DEFAULT ON NULL",
                        "NO DEFAULT ON NULL",
                        "DEFAULT ON NULL",
                        "default_on_null_missing",
                    ),
                    sdr.ColumnTypeIssue(
                        "C2",
                        "NO DEFAULT ON NULL",
                        "DEFAULT ON NULL",
                        "NO DEFAULT ON NULL",
                        "default_on_null_unexpected",
                    ),
                ],
            )
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = sdr.export_column_default_on_null_detail(
                mismatched_items,
                Path(tmp_dir),
                "20260318_150000"
            )
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
        self.assertIn("列 DEFAULT ON NULL 差异明细", content)
        self.assertIn(
            "A.T1|C1|DEFAULT ON NULL|NO DEFAULT ON NULL|DEFAULT ON NULL|REVIEW_ADD_DEFAULT_ON_NULL",
            content,
        )
        self.assertIn(
            "A.T1|C2|NO DEFAULT ON NULL|DEFAULT ON NULL|NO DEFAULT ON NULL|REVIEW_DROP_DEFAULT_ON_NULL",
            content,
        )

    def test_export_column_visibility_skipped_detail(self):
        skipped_items = [
            sdr.ColumnVisibilitySkippedRow(
                source_table="SRC.T1",
                target_table="TGT.T1",
                policy="auto",
                source_metadata="READY",
                target_metadata="MISSING",
                reason="INVISIBLE compare/fixup skipped",
                action="rerun later",
            )
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = sdr.export_column_visibility_skipped_detail(
                skipped_items,
                Path(tmp_dir),
                "20260318_160000"
            )
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
        self.assertIn("列可见性(INVISIBLE)跳过明细", content)
        self.assertIn(
            "SRC.T1|TGT.T1|auto|READY|MISSING|INVISIBLE compare/fixup skipped|rerun later",
            content,
        )

    def test_extract_default_on_null_columns_from_table_ddl(self):
        ddl = """
        CREATE TABLE "A"."T1"
        (
          "C1" NUMBER DEFAULT ON NULL 10,
          "C2" NUMBER DEFAULT 20
        )
        """
        self.assertEqual(
            sdr.extract_default_on_null_columns_from_table_ddl(ddl, {"C1", "C2"}),
            ("C1",),
        )

    def test_extract_identity_options_from_table_ddl(self):
        ddl = """
        CREATE TABLE "A"."T1"
        (
          "ID1" NUMBER GENERATED BY DEFAULT AS IDENTITY START WITH 11 INCREMENT BY 2 CACHE 25,
          "ID2" NUMBER GENERATED ALWAYS AS IDENTITY ( START WITH 7 INCREMENT BY 5 NOCACHE ),
          "C1"  NUMBER
        )
        """
        self.assertEqual(
            sdr.extract_identity_options_from_table_ddl(ddl, {"ID1", "ID2", "C1"}),
            {
                "ID1": {"START WITH": "11", "INCREMENT BY": "2", "CACHE": "25"},
                "ID2": {"START WITH": "7", "INCREMENT BY": "5", "CACHE": "NOCACHE"},
            },
        )

    def test_extract_notnull_column_preserves_hash(self):
        self.assertEqual(
            sdr._extract_notnull_column('"PK_SERIAL#" IS NOT NULL'),
            "PK_SERIAL#"
        )

    def test_export_column_identity_detail(self):
        mismatched_items = [
            (
                "TABLE",
                "A.T1",
                set(),
                set(),
                [],
                [
                    sdr.ColumnTypeIssue("ID1", "NUMBER", "NUMBER", "IDENTITY", "identity_missing"),
                    sdr.ColumnTypeIssue(
                        "ID2",
                        "IDENTITY ALWAYS",
                        "IDENTITY BY DEFAULT",
                        "IDENTITY ALWAYS",
                        "identity_mode_mismatch"
                    ),
                    sdr.ColumnTypeIssue(
                        "ID3",
                        "NO IDENTITY",
                        "IDENTITY BY DEFAULT ON NULL",
                        "NO IDENTITY",
                        "identity_unexpected"
                    ),
                ],
            )
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = sdr.export_column_identity_detail(
                mismatched_items,
                Path(tmp_dir),
                "20260317_120000"
            )
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
        self.assertIn("列 identity 差异明细", content)
        self.assertIn("A.T1|ID1|NUMBER|NUMBER|IDENTITY|REVIEW_ADD_IDENTITY", content)
        self.assertIn("A.T1|ID2|IDENTITY ALWAYS|IDENTITY BY DEFAULT|IDENTITY ALWAYS|REVIEW_IDENTITY_MODE", content)
        self.assertIn("A.T1|ID3|NO IDENTITY|IDENTITY BY DEFAULT ON NULL|NO IDENTITY|REVIEW_REMOVE_IDENTITY", content)

    def test_export_column_identity_option_detail(self):
        mismatched_items = [
            (
                "TABLE",
                "A.T1",
                set(),
                set(),
                [],
                [
                    sdr.ColumnTypeIssue(
                        "ID1",
                        "START WITH=11, INCREMENT BY=2, CACHE=25",
                        "START WITH=11, INCREMENT BY=7, CACHE=30",
                        "START WITH=11, INCREMENT BY=2, CACHE=25",
                        "identity_option_mismatch",
                    )
                ],
            )
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = sdr.export_column_identity_option_detail(
                mismatched_items,
                Path(tmp_dir),
                "20260318_170000"
            )
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
        self.assertIn("列 identity 细项差异明细", content)
        self.assertIn(
            "A.T1|ID1|START WITH=11, INCREMENT BY=2, CACHE=25|START WITH=11, INCREMENT BY=7, CACHE=30|START WITH=11, INCREMENT BY=2, CACHE=25|REVIEW_IDENTITY_OPTIONS",
            content,
        )

    def test_export_fatal_error_matrix(self):
        rows = [
            sdr.FatalErrorMatrixRow(
                category="SCOPED_TRIGGER_LIST",
                trigger_condition="remap_root_closure + trigger_list 非法",
                default_behavior="FATAL_ABORT",
                currently_relevant="YES",
                remediation="修正 trigger_list",
            )
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = sdr.export_fatal_error_matrix(rows, Path(tmp_dir), "20260330_180000")
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
        self.assertIn("Fatal error 场景矩阵", content)
        self.assertIn(
            "SCOPED_TRIGGER_LIST|remap_root_closure + trigger_list 非法|FATAL_ABORT|YES|修正 trigger_list",
            content,
        )

    def test_locate_target_identity_sequences_object_id_match(self):
        ob_meta = self._make_ob_meta()._replace(
            objects_by_type={"TABLE": {"TGT.T1"}},
            identity_modes={("TGT", "T1"): {"ID": "BY DEFAULT ON NULL"}},
            identity_options={("TGT", "T1"): {"ID": {"INCREMENT BY": "1", "CACHE": "20"}}},
        )

        def fake_run(_cfg, sql, **_kwargs):
            sql_u = " ".join(str(sql).upper().split())
            if "FROM DBA_OBJECTS" in sql_u:
                return True, "TGT\tT1\tTABLE\t2026-03-30 12:00:00\t123\nTGT\tISEQ$$_123_16\tSEQUENCE\t2026-03-30 12:00:00\t125", ""
            if "FROM DBA_SEQUENCES" in sql_u:
                return True, "TGT\tISEQ$$_123_16\t1\t999\t1\t20\tN\tN", ""
            return False, "", "unexpected_sql"

        with mock.patch.object(sdr, "obclient_run_sql", side_effect=fake_run):
            rows = sdr.locate_target_identity_sequences(
                {"executable": "obclient"},
                ob_meta,
                {("TGT", "T1")},
            )
        row = rows[("TGT", "T1")]
        self.assertEqual(row.status, sdr.IDENTITY_SEQUENCE_STATUS_PRESENT)
        self.assertEqual(row.reason_code, sdr.IDENTITY_SEQUENCE_REASON_OBJECT_ID_MATCH)
        self.assertEqual(row.target_sequence, "TGT.ISEQ$$_123_16")

    def test_locate_target_identity_sequences_created_fallback_match(self):
        ob_meta = self._make_ob_meta()._replace(
            objects_by_type={"TABLE": {"TGT.T1"}},
            identity_modes={("TGT", "T1"): {"ID": "BY DEFAULT ON NULL"}},
            identity_options={("TGT", "T1"): {"ID": {"INCREMENT BY": "1", "CACHE": "20"}}},
        )

        def fake_run(_cfg, sql, **_kwargs):
            sql_u = " ".join(str(sql).upper().split())
            if "FROM DBA_OBJECTS" in sql_u:
                return True, "TGT\tT1\tTABLE\t2026-03-30 12:00:00\t123\nTGT\tISEQ$$_1\tSEQUENCE\t2026-03-30 12:00:00\t125", ""
            if "FROM DBA_SEQUENCES" in sql_u:
                return True, "TGT\tISEQ$$_1\t1\t999\t1\t20\tN\tN", ""
            return False, "", "unexpected_sql"

        with mock.patch.object(sdr, "obclient_run_sql", side_effect=fake_run):
            rows = sdr.locate_target_identity_sequences(
                {"executable": "obclient"},
                ob_meta,
                {("TGT", "T1")},
            )
        row = rows[("TGT", "T1")]
        self.assertEqual(row.status, sdr.IDENTITY_SEQUENCE_STATUS_PRESENT)
        self.assertEqual(row.reason_code, sdr.IDENTITY_SEQUENCE_REASON_CREATED_MATCH)
        self.assertEqual(row.target_sequence, "TGT.ISEQ$$_1")

    def test_locate_target_identity_sequences_ambiguous(self):
        ob_meta = self._make_ob_meta()._replace(
            objects_by_type={"TABLE": {"TGT.T1"}},
            identity_modes={("TGT", "T1"): {"ID": "BY DEFAULT ON NULL"}},
            identity_options={("TGT", "T1"): {"ID": {"INCREMENT BY": "1", "CACHE": "20"}}},
        )

        def fake_run(_cfg, sql, **_kwargs):
            sql_u = " ".join(str(sql).upper().split())
            if "FROM DBA_OBJECTS" in sql_u:
                return True, (
                    "TGT\tT1\tTABLE\t2026-03-30 12:00:00\n"
                    "TGT\tISEQ$$_1\tSEQUENCE\t2026-03-30 12:00:00\n"
                    "TGT\tISEQ$$_2\tSEQUENCE\t2026-03-30 12:00:00"
                ), ""
            if "FROM DBA_SEQUENCES" in sql_u:
                return True, (
                    "TGT\tISEQ$$_1\t1\t999\t1\t20\tN\tN\n"
                    "TGT\tISEQ$$_2\t1\t999\t1\t20\tN\tN"
                ), ""
            return False, "", "unexpected_sql"

        with mock.patch.object(sdr, "obclient_run_sql", side_effect=fake_run):
            rows = sdr.locate_target_identity_sequences(
                {"executable": "obclient"},
                ob_meta,
                {("TGT", "T1")},
            )
        row = rows[("TGT", "T1")]
        self.assertEqual(row.status, sdr.IDENTITY_SEQUENCE_STATUS_UNRESOLVED)
        self.assertEqual(row.reason_code, sdr.IDENTITY_SEQUENCE_REASON_AMBIGUOUS_CREATED_MATCH)
        self.assertIn("ISEQ$$_1", row.detail)
        self.assertIn("ISEQ$$_2", row.detail)

    def test_augment_grant_plan_with_identity_sequence_grants(self):
        oracle_meta = self._make_oracle_meta()._replace(
            identity_modes={("SRC", "T1"): {"ID": "BY DEFAULT ON NULL"}},
            identity_options={("SRC", "T1"): {"ID": {"INCREMENT BY": "1", "CACHE": "20"}}},
        )
        ob_meta = self._make_ob_meta()._replace(
            objects_by_type={"TABLE": {"TGT.T1"}},
            identity_modes={("TGT", "T1"): {"ID": "BY DEFAULT ON NULL"}},
            identity_options={("TGT", "T1"): {"ID": {"INCREMENT BY": "1", "CACHE": "20"}}},
        )
        plan = sdr.GrantPlan(
            object_grants={"APP": {sdr.ObjectGrantEntry("INSERT", "TGT.T1", False)}},
            column_grants={},
            sys_privs={},
            role_privs={},
            role_ddls=[],
            filtered_grants=[],
            view_grant_targets=set(),
            object_target_types={"TGT.T1": "TABLE"},
        )

        def fake_run(_cfg, sql, **_kwargs):
            sql_u = " ".join(str(sql).upper().split())
            if "FROM DBA_OBJECTS" in sql_u:
                return True, "TGT\tT1\tTABLE\t2026-03-30 12:00:00\nTGT\tISEQ$$_1\tSEQUENCE\t2026-03-30 12:00:00", ""
            if "FROM DBA_SEQUENCES" in sql_u:
                return True, "TGT\tISEQ$$_1\t1\t999\t1\t20\tN\tN", ""
            return False, "", "unexpected_sql"

        with mock.patch.object(sdr, "obclient_run_sql", side_effect=fake_run):
            updated_plan, rows = sdr.augment_grant_plan_with_identity_sequence_grants(
                plan,
                oracle_meta,
                ob_meta,
                {"executable": "obclient"},
                {"SRC.T1": {"TABLE": "TGT.T1"}},
            )
        self.assertIn(sdr.ObjectGrantEntry("SELECT", "TGT.ISEQ$$_1", False), updated_plan.object_grants["APP"])
        self.assertEqual(updated_plan.object_target_types["TGT.ISEQ$$_1"], "SEQUENCE")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].target_sequence, "TGT.ISEQ$$_1")

    def test_finalize_identity_sequence_grant_rows_marks_missing(self):
        expectations = [
            sdr.IdentitySequenceGrantExpectationRow(
                grantee="APP",
                src_table_full="SRC.T1",
                tgt_table_full="TGT.T1",
                identity_columns=("ID",),
                target_sequence="TGT.ISEQ$$_1",
                status=sdr.IDENTITY_SEQUENCE_STATUS_PRESENT,
                reason_code=sdr.IDENTITY_SEQUENCE_REASON_CREATED_MATCH,
                detail="created=2026-03-30 12:00:00",
            )
        ]
        rows = sdr.finalize_identity_sequence_grant_rows(
            expectations,
            {"APP": {sdr.ObjectGrantEntry("SELECT", "TGT.ISEQ$$_1", False)}},
        )
        self.assertEqual(rows[0].status, sdr.IDENTITY_SEQUENCE_STATUS_MISSING_GRANT)
        self.assertEqual(rows[0].action, "FIXUP_GRANT")

    def test_build_sequence_restart_detail_rows_generates_for_target_behind(self):
        oracle_meta = self._make_oracle_meta(
            sequences={"SRC": {"SEQ1"}},
        )._replace(
            sequence_attrs={"SRC": {"SEQ1": {"last_number": 21}}}
        )
        ob_meta = self._make_ob_meta(
            sequences={"TGT": {"SEQ1"}},
        )._replace(
            sequence_attrs={"TGT": {"SEQ1": {"last_number": 1}}}
        )
        rows = sdr.build_sequence_restart_detail_rows(
            oracle_meta,
            ob_meta,
            {"SRC.SEQ1": {"SEQUENCE": "TGT.SEQ1"}},
            planned_sequence_creates=set(),
            sequence_sync_mode="last_number",
            allow_fixup_predicate=lambda _tgt, _src: True,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].status, sdr.SEQUENCE_RESTART_STATUS_GENERATE)
        self.assertEqual(rows[0].reason_code, sdr.SEQUENCE_RESTART_REASON_TARGET_LAST_NUMBER_BEHIND)
        self.assertEqual(rows[0].src_last_number, "21")
        self.assertEqual(rows[0].tgt_last_number, "1")

    def test_build_sequence_restart_detail_rows_skips_when_target_caught_up(self):
        oracle_meta = self._make_oracle_meta(
            sequences={"SRC": {"SEQ1"}},
        )._replace(
            sequence_attrs={"SRC": {"SEQ1": {"last_number": 21}}}
        )
        ob_meta = self._make_ob_meta(
            sequences={"TGT": {"SEQ1"}},
        )._replace(
            sequence_attrs={"TGT": {"SEQ1": {"last_number": 25}}}
        )
        rows = sdr.build_sequence_restart_detail_rows(
            oracle_meta,
            ob_meta,
            {"SRC.SEQ1": {"SEQUENCE": "TGT.SEQ1"}},
            planned_sequence_creates=set(),
            sequence_sync_mode="last_number",
            allow_fixup_predicate=lambda _tgt, _src: True,
        )
        self.assertEqual(rows[0].status, sdr.SEQUENCE_RESTART_STATUS_NO_ACTION)
        self.assertEqual(rows[0].reason_code, sdr.SEQUENCE_RESTART_REASON_TARGET_ALREADY_CAUGHT_UP)

    def test_build_sequence_restart_detail_rows_generates_for_missing_planned_create(self):
        oracle_meta = self._make_oracle_meta(
            sequences={"SRC": {"SEQ1"}},
        )._replace(
            sequence_attrs={"SRC": {"SEQ1": {"last_number": 21}}}
        )
        ob_meta = self._make_ob_meta(sequences={"TGT": set()})._replace(
            sequence_attrs={"TGT": {}}
        )
        rows = sdr.build_sequence_restart_detail_rows(
            oracle_meta,
            ob_meta,
            {"SRC.SEQ1": {"SEQUENCE": "TGT.SEQ1"}},
            planned_sequence_creates={("TGT", "SEQ1")},
            sequence_sync_mode="last_number",
            allow_fixup_predicate=lambda _tgt, _src: True,
        )
        self.assertEqual(rows[0].status, sdr.SEQUENCE_RESTART_STATUS_GENERATE)
        self.assertEqual(rows[0].reason_code, sdr.SEQUENCE_RESTART_REASON_TARGET_MISSING_PLANNED_CREATE)

    def test_build_sequence_restart_detail_rows_marks_unresolved_without_last_number(self):
        oracle_meta = self._make_oracle_meta(
            sequences={"SRC": {"SEQ1"}},
        )._replace(
            sequence_attrs={"SRC": {"SEQ1": {}}}
        )
        ob_meta = self._make_ob_meta(
            sequences={"TGT": {"SEQ1"}},
        )._replace(
            sequence_attrs={"TGT": {"SEQ1": {"last_number": 1}}}
        )
        rows = sdr.build_sequence_restart_detail_rows(
            oracle_meta,
            ob_meta,
            {"SRC.SEQ1": {"SEQUENCE": "TGT.SEQ1"}},
            planned_sequence_creates=set(),
            sequence_sync_mode="last_number",
            allow_fixup_predicate=lambda _tgt, _src: True,
        )
        self.assertEqual(rows[0].status, sdr.SEQUENCE_RESTART_STATUS_UNRESOLVED)
        self.assertEqual(rows[0].reason_code, sdr.SEQUENCE_RESTART_REASON_SOURCE_LAST_NUMBER_UNAVAILABLE)

    def test_build_sequence_restart_detail_rows_skips_auto_sequence(self):
        oracle_meta = self._make_oracle_meta(
            sequences={"SRC": {"ISEQ$$_1"}},
        )._replace(
            sequence_attrs={"SRC": {"ISEQ$$_1": {"last_number": 21}}}
        )
        ob_meta = self._make_ob_meta(
            sequences={"TGT": {"ISEQ$$_1"}},
        )._replace(
            sequence_attrs={"TGT": {"ISEQ$$_1": {"last_number": 1}}}
        )
        rows = sdr.build_sequence_restart_detail_rows(
            oracle_meta,
            ob_meta,
            {"SRC.ISEQ$$_1": {"SEQUENCE": "TGT.ISEQ$$_1"}},
            planned_sequence_creates=set(),
            sequence_sync_mode="last_number",
            allow_fixup_predicate=lambda _tgt, _src: True,
        )
        self.assertEqual(rows[0].status, sdr.SEQUENCE_RESTART_STATUS_SKIPPED)
        self.assertEqual(rows[0].reason_code, sdr.SEQUENCE_RESTART_REASON_AUTO_SEQUENCE_SKIPPED)

    def test_export_sequence_restart_detail_writes_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [
                sdr.SequenceRestartDetailRow(
                    src_sequence_full="SRC.SEQ1",
                    tgt_sequence_full="TGT.SEQ1",
                    src_last_number="21",
                    tgt_last_number="1",
                    status=sdr.SEQUENCE_RESTART_STATUS_GENERATE,
                    reason_code=sdr.SEQUENCE_RESTART_REASON_TARGET_LAST_NUMBER_BEHIND,
                    detail="target last_number is behind oracle last_number",
                    action="GENERATE_RESTART",
                )
            ]
            output = sdr.export_sequence_restart_detail(rows, Path(tmpdir), "123")
            self.assertIsNotNone(output)
            content = Path(output).read_text(encoding="utf-8")
            self.assertIn("sequence restart 规划明细", content)
            self.assertIn("SRC.SEQ1|TGT.SEQ1|21|1|GENERATE", content)

    def test_inflate_table_varchar_lengths_handles_byte_inside_parentheses(self):
        oracle_meta = self._make_oracle_meta_with_columns(
            {
                ("SRC", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "B",
                    }
                }
            }
        )
        ddl = 'CREATE TABLE "SRC"."T1" ("C1" VARCHAR2(10 BYTE));'
        adjusted = sdr.inflate_table_varchar_lengths(ddl, "SRC", "T1", oracle_meta)
        self.assertIn('VARCHAR2(15 BYTE)', adjusted)

    def test_inflate_table_varchar_lengths_keeps_char_semantics(self):
        oracle_meta = self._make_oracle_meta_with_columns(
            {
                ("SRC", "T1"): {
                    "C1": {
                        "data_type": "VARCHAR2",
                        "data_length": 10,
                        "char_length": 10,
                        "char_used": "C",
                    }
                }
            }
        )
        ddl = 'CREATE TABLE "SRC"."T1" ("C1" VARCHAR2(10 CHAR));'
        adjusted = sdr.inflate_table_varchar_lengths(ddl, "SRC", "T1", oracle_meta)
        self.assertEqual(adjusted, ddl)

    def test_filter_trigger_results_for_unsupported_tables(self):
        extra_results = {
            "index_ok": [],
            "index_mismatched": [],
            "constraint_ok": [],
            "constraint_mismatched": [],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": ["TGT.T1", "TGT.T2"],
            "trigger_mismatched": [
                sdr.TriggerMismatch(
                    table="TGT.T1",
                    missing_triggers={"TR1"},
                    extra_triggers=set(),
                    detail_mismatch=["missing"],
                    missing_mappings=None
                )
            ],
        }
        unsupported_table_keys = {("SRC", "T1")}
        table_target_map = {
            ("SRC", "T1"): ("TGT", "T1"),
            ("SRC", "T2"): ("TGT", "T2"),
        }
        filtered = sdr.filter_trigger_results_for_unsupported_tables(
            extra_results,
            unsupported_table_keys,
            table_target_map
        )
        self.assertEqual(filtered["trigger_mismatched"], [])
        self.assertEqual(filtered["trigger_ok"], ["TGT.T2"])

    def test_filter_trigger_results_for_unsupported_tables_filters_all_extra_types(self):
        extra_results = {
            "index_ok": ["TGT.T1", "TGT.T2"],
            "index_mismatched": [
                sdr.IndexMismatch("TGT.T1", {"IX1"}, set(), []),
                sdr.IndexMismatch("TGT.T2", {"IX2"}, set(), []),
            ],
            "constraint_ok": ["TGT.T1", "TGT.T2"],
            "constraint_mismatched": [
                sdr.ConstraintMismatch("TGT.T1", {"CK1"}, set(), [], set()),
                sdr.ConstraintMismatch("TGT.T2", {"CK2"}, set(), [], set()),
            ],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": ["TGT.T1", "TGT.T2"],
            "trigger_mismatched": [
                sdr.TriggerMismatch("TGT.T1", {"TR1"}, set(), [], None),
                sdr.TriggerMismatch("TGT.T2", {"TR2"}, set(), [], None),
            ],
        }
        table_target_map = {
            ("SRC", "T1"): ("TGT", "T1"),
            ("SRC", "T2"): ("TGT", "T2"),
        }
        support_state_map = {
            ("TABLE", "SRC.T2"): sdr.ObjectSupportReportRow(
                obj_type="TABLE",
                src_full="SRC.T2",
                tgt_full="TGT.T2",
                support_state=sdr.SUPPORT_STATE_SUPPORTED,
                reason_code="-",
                reason="-",
                dependency="-",
                action="FIXUP",
                detail="-",
            )
        }
        filtered = sdr.filter_trigger_results_for_unsupported_tables(
            extra_results,
            {("SRC", "T1")},
            table_target_map,
            support_state_map,
        )
        self.assertEqual(filtered["index_mismatched"], [])
        self.assertEqual(filtered["constraint_mismatched"], [])
        self.assertEqual(filtered["trigger_mismatched"], [])
        self.assertEqual(filtered["index_ok"], [])
        self.assertEqual(filtered["constraint_ok"], [])
        self.assertEqual(filtered["trigger_ok"], [])

    def test_collect_trigger_status_rows_skips_unsupported_tables(self):
        oracle_meta = self._make_oracle_meta(triggers={
            ("SRC", "T1"): {
                "TR1": {"event": "INSERT", "status": "ENABLED", "owner": "SRC"}
            }
        })
        ob_meta = self._make_ob_meta(triggers={
            ("TGT", "T1"): {
                "TR1": {"event": "UPDATE", "status": "ENABLED", "owner": "TGT"}
            }
        })
        rows = sdr.collect_trigger_status_rows(
            oracle_meta,
            ob_meta,
            {"SRC.TR1": {"TRIGGER": "TGT.TR1"}},
            unsupported_table_keys={("SRC", "T1")}
        )
        self.assertEqual(rows, [])

    def test_compare_triggers_cross_owner_same_name_matches(self):
        oracle_meta = self._make_oracle_meta(triggers={
            ("A", "T1"): {
                "A.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "A", "name": "TR_X"},
                "B.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "B", "name": "TR_X"},
            }
        })
        ob_meta = self._make_ob_meta(triggers={
            ("A", "T1"): {
                "A.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "A", "name": "TR_X"},
                "B.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "B", "name": "TR_X"},
            }
        })
        ok, mismatch = sdr.compare_triggers_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {},
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_triggers_owner_drift_not_counted_as_missing(self):
        oracle_meta = self._make_oracle_meta(triggers={
            ("A", "T1"): {
                "A.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "A", "name": "TR_X"},
            }
        })
        ob_meta = self._make_ob_meta(triggers={
            ("A", "T1"): {
                "B.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "B", "name": "TR_X"},
            }
        })
        ok, mismatch = sdr.compare_triggers_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {},
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.missing_triggers, set())
        self.assertEqual(mismatch.extra_triggers, set())
        self.assertTrue(any("OWNER 不一致" in line for line in mismatch.detail_mismatch))

    def test_compare_triggers_cross_schema_derived_copy_not_counted_missing(self):
        oracle_meta = self._make_oracle_meta(triggers={
            ("A", "T1"): {
                "A.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "A", "name": "TR_X"},
                "B.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "B", "name": "TR_X"},
            }
        })
        ob_meta = self._make_ob_meta(triggers={
            ("A", "T1"): {
                "A.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "A", "name": "TR_X"},
            }
        })
        ok, mismatch = sdr.compare_triggers_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {},
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.missing_triggers, set())
        self.assertEqual(mismatch.extra_triggers, set())
        self.assertTrue(any("跨 schema 派生触发器副本按同名同事件语义匹配" in line for line in mismatch.detail_mismatch))

    def test_compare_triggers_cross_schema_derived_copy_still_missing_without_semantic_match(self):
        oracle_meta = self._make_oracle_meta(triggers={
            ("A", "T1"): {
                "A.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "A", "name": "TR_X"},
                "B.TR_Y": {"event": "INSERT", "status": "ENABLED", "owner": "B", "name": "TR_Y"},
            }
        })
        ob_meta = self._make_ob_meta(triggers={
            ("A", "T1"): {
                "A.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "A", "name": "TR_X"},
                "A.TR_Z": {"event": "UPDATE", "status": "ENABLED", "owner": "A", "name": "TR_Z"},
            }
        })
        ok, mismatch = sdr.compare_triggers_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {},
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.missing_triggers, {"B.TR_Y"})

    def test_compare_triggers_for_view_detects_missing_instead_of_trigger(self):
        oracle_meta = self._make_oracle_meta(triggers={
            ("SRC", "V1"): {
                "SRC.TRG_V1_IOI": {"event": "INSERT", "status": "ENABLED", "owner": "SRC", "name": "TRG_V1_IOI"},
            }
        })
        ob_meta = self._make_ob_meta(triggers={})
        ok, mismatch = sdr.compare_triggers_for_table(
            oracle_meta,
            ob_meta,
            "SRC",
            "V1",
            "TGT",
            "V1",
            {"SRC.TRG_V1_IOI": {"TRIGGER": "TGT.TRG_V1_IOI"}},
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.table, "TGT.V1")
        self.assertEqual(mismatch.missing_triggers, {"TGT.TRG_V1_IOI"})

    def test_check_extra_objects_detects_missing_view_trigger(self):
        master_list = [("SRC.V1", "TGT.V1", "VIEW")]
        oracle_meta = self._make_oracle_meta(triggers={
            ("SRC", "V1"): {
                "SRC.TRG_V1_IOI": {"event": "INSERT", "status": "ENABLED", "owner": "SRC", "name": "TRG_V1_IOI"},
            }
        })
        ob_meta = self._make_ob_meta(triggers={})._replace(
            objects_by_type={"VIEW": {"TGT.V1"}}
        )
        full_mapping = {
            "SRC.V1": {"VIEW": "TGT.V1"},
            "SRC.TRG_V1_IOI": {"TRIGGER": "TGT.TRG_V1_IOI"},
        }
        extra = sdr.check_extra_objects(
            {"extra_check_workers": 1, "extra_check_progress_interval": 999},
            master_list,
            ob_meta,
            oracle_meta,
            full_mapping,
            enabled_extra_types={"TRIGGER"},
        )
        self.assertEqual(extra["trigger_ok"], [])
        self.assertEqual(len(extra["trigger_mismatched"]), 1)
        self.assertEqual(extra["trigger_mismatched"][0].table, "TGT.V1")
        self.assertEqual(extra["trigger_mismatched"][0].missing_triggers, {"TGT.TRG_V1_IOI"})

    def test_build_trigger_full_set_handles_full_key(self):
        trigger_meta = {
            ("A", "T1"): {
                "A.TR_X": {"event": "INSERT", "status": "ENABLED", "owner": "A", "name": "TR_X"},
                "B.TR_X": {"event": "UPDATE", "status": "ENABLED", "owner": "B", "name": "TR_X"},
            }
        }
        self.assertEqual(sdr.build_trigger_full_set(trigger_meta), {"A.TR_X", "B.TR_X"})

    def test_classify_missing_objects_marks_invalid_and_blocks_synonym(self):
        tv_results = {
            "missing": [
                ("VIEW", "TGT.V1", "SRC.V1"),
                ("SYNONYM", "TGT.S1", "SRC.S1"),
            ],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        oracle_meta = self._make_oracle_meta()
        oracle_meta = oracle_meta._replace(
            object_statuses={
                ("SRC", "V1", "VIEW"): "INVALID"
            }
        )
        ob_meta = self._make_ob_meta()
        full_mapping = {
            "SRC.V1": {"VIEW": "TGT.V1"},
            "SRC.S1": {"SYNONYM": "TGT.S1"},
        }
        source_objects = {
            "SRC.V1": {"VIEW"},
            "SRC.S1": {"SYNONYM"},
        }
        table_target_map = {}
        synonym_meta = {
            ("SRC", "S1"): sdr.SynonymMeta("SRC", "S1", "SRC", "V1", None)
        }
        settings = {"view_compat_rules": {}, "view_dblink_policy": "block"}
        ora_cfg = {"user": "u", "password": "p", "dsn": "d"}
        with mock.patch.object(sdr, "oracle_get_views_ddl_batch", return_value={("SRC", "V1"): "CREATE VIEW V1 AS SELECT 1 FROM DUAL"}):
            summary = sdr.classify_missing_objects(
                ora_cfg,
                settings,
                tv_results,
                {
                    "index_ok": [], "index_mismatched": [],
                    "constraint_ok": [], "constraint_mismatched": [],
                    "sequence_ok": [], "sequence_mismatched": [],
                    "trigger_ok": [], "trigger_mismatched": [],
                },
                oracle_meta,
                ob_meta,
                full_mapping,
                source_objects,
                dependency_graph=None,
                object_parent_map=None,
                table_target_map=table_target_map,
                synonym_meta_map=synonym_meta
            )
        view_row = next(row for row in summary.missing_detail_rows if row.src_full == "SRC.V1")
        self.assertEqual(view_row.support_state, sdr.SUPPORT_STATE_BLOCKED)
        self.assertEqual(view_row.reason_code, "SOURCE_INVALID")
        syn_row = next(row for row in summary.missing_detail_rows if row.src_full == "SRC.S1")
        self.assertEqual(syn_row.support_state, sdr.SUPPORT_STATE_BLOCKED)
        self.assertEqual(syn_row.reason_code, "DEPENDENCY_INVALID")

    def test_classify_missing_objects_dependency_cycle_does_not_recurse_forever(self):
        tv_results = {
            "missing": [
                ("PROCEDURE", "TGT.A", "SRC.A"),
                ("PROCEDURE", "TGT.B", "SRC.B"),
            ],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        oracle_meta = self._make_oracle_meta()._replace(
            object_statuses={("SRC", "C", "PROCEDURE"): "INVALID"}
        )
        ob_meta = self._make_ob_meta()
        full_mapping = {
            "SRC.A": {"PROCEDURE": "TGT.A"},
            "SRC.B": {"PROCEDURE": "TGT.B"},
            "SRC.C": {"PROCEDURE": "TGT.C"},
        }
        source_objects = {
            "SRC.A": {"PROCEDURE"},
            "SRC.B": {"PROCEDURE"},
            "SRC.C": {"PROCEDURE"},
        }
        dependency_graph = {
            ("SRC.A", "PROCEDURE"): {("SRC.B", "PROCEDURE")},
            ("SRC.B", "PROCEDURE"): {("SRC.A", "PROCEDURE"), ("SRC.C", "PROCEDURE")},
        }
        summary = sdr.classify_missing_objects(
            {"user": "u", "password": "p", "dsn": "d"},
            {"view_compat_rules": {}, "view_dblink_policy": "block"},
            tv_results,
            {
                "index_ok": [], "index_mismatched": [],
                "constraint_ok": [], "constraint_mismatched": [],
                "sequence_ok": [], "sequence_mismatched": [],
                "trigger_ok": [], "trigger_mismatched": [],
            },
            oracle_meta,
            ob_meta,
            full_mapping,
            source_objects,
            dependency_graph=dependency_graph,
            object_parent_map=None,
            table_target_map={},
            synonym_meta_map={},
        )
        rows = {row.src_full: row for row in summary.missing_detail_rows}
        self.assertEqual(rows["SRC.A"].support_state, sdr.SUPPORT_STATE_BLOCKED)
        self.assertEqual(rows["SRC.B"].support_state, sdr.SUPPORT_STATE_BLOCKED)
        self.assertIn("SOURCE_INVALID", rows["SRC.A"].root_cause)
        self.assertIn("SOURCE_INVALID", rows["SRC.B"].root_cause)

    def test_classify_missing_objects_blocks_synonym_target_out_of_scope(self):
        tv_results = {
            "missing": [
                ("SYNONYM", "TGT.S1", "SRC.S1"),
            ],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        full_mapping = {
            "SRC.S1": {"SYNONYM": "TGT.S1"},
        }
        source_objects = {
            "SRC.S1": {"SYNONYM"},
        }
        summary = sdr.classify_missing_objects(
            {"user": "u", "password": "p", "dsn": "d"},
            {"view_compat_rules": {}, "view_dblink_policy": "block"},
            tv_results,
            {
                "index_ok": [], "index_mismatched": [],
                "constraint_ok": [], "constraint_mismatched": [],
                "sequence_ok": [], "sequence_mismatched": [],
                "trigger_ok": [], "trigger_mismatched": [],
            },
            oracle_meta,
            ob_meta,
            full_mapping,
            source_objects,
            dependency_graph=None,
            object_parent_map=None,
            table_target_map={},
            synonym_meta_map={
                ("SRC", "S1"): sdr.SynonymMeta("SRC", "S1", "SRC", "S2", None)
            },
            remap_rules={},
        )
        syn_row = next(row for row in summary.missing_detail_rows if row.src_full == "SRC.S1")
        self.assertEqual(syn_row.support_state, sdr.SUPPORT_STATE_BLOCKED)
        self.assertEqual(syn_row.reason_code, sdr.SYNONYM_TARGET_OUT_OF_SCOPE_REASON_CODE)
        self.assertEqual(syn_row.dependency, "SRC.S2")

    def test_classify_missing_objects_keeps_explicit_remap_synonym_in_scope(self):
        tv_results = {
            "missing": [
                ("SYNONYM", "TGT.S1", "SRC.S1"),
            ],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        summary = sdr.classify_missing_objects(
            {"user": "u", "password": "p", "dsn": "d"},
            {"view_compat_rules": {}, "view_dblink_policy": "block"},
            tv_results,
            {
                "index_ok": [], "index_mismatched": [],
                "constraint_ok": [], "constraint_mismatched": [],
                "sequence_ok": [], "sequence_mismatched": [],
                "trigger_ok": [], "trigger_mismatched": [],
            },
            self._make_oracle_meta(),
            self._make_ob_meta(),
            {"SRC.S1": {"SYNONYM": "TGT.S1"}},
            {"SRC.S1": {"SYNONYM"}},
            dependency_graph=None,
            object_parent_map=None,
            table_target_map={},
            synonym_meta_map={
                ("SRC", "S1"): sdr.SynonymMeta("SRC", "S1", "OTHER", "T1", None)
            },
            remap_rules={"SRC.S1": "TGT.S1"},
        )
        syn_row = next(row for row in summary.missing_detail_rows if row.src_full == "SRC.S1")
        self.assertEqual(syn_row.support_state, sdr.SUPPORT_STATE_SUPPORTED)
        self.assertEqual(syn_row.reason_code, "-")
        self.assertEqual(syn_row.action, "FIXUP")

    def test_resolve_synonym_scope_status_public_chain_stays_in_scope(self):
        synonym_meta = {
            ("SRC", "TOP"): sdr.SynonymMeta("SRC", "TOP", "SRC", "PUB_A", None),
            ("PUBLIC", "PUB_A"): sdr.SynonymMeta("PUBLIC", "PUB_A", "SRC", "PUB_B", None),
            ("PUBLIC", "PUB_B"): sdr.SynonymMeta("PUBLIC", "PUB_B", "SRC", "T1", None),
        }
        terminal, state, detail = sdr.resolve_synonym_scope_status(
            "SRC",
            "TOP",
            synonym_meta,
            {"SRC.T1": {"TABLE"}},
            remap_rules={},
        )
        self.assertEqual(terminal, "SRC.T1")
        self.assertEqual(state, "in_scope")
        self.assertEqual(detail, "TABLE")

    def test_resolve_synonym_scope_status_explicit_remap_keeps_in_scope(self):
        synonym_meta = {
            ("SRC", "S1"): sdr.SynonymMeta("SRC", "S1", "SRC", "T1", None),
        }
        terminal, state, detail = sdr.resolve_synonym_scope_status(
            "SRC",
            "S1",
            synonym_meta,
            {},
            remap_rules={"SRC.S1": "TGT.S1"},
        )
        self.assertEqual(terminal, "SRC.T1")
        self.assertEqual(state, "in_scope")
        self.assertIn("synonym_explicit_remap", detail)

    def test_resolve_synonym_terminal_source_prefers_public_chain_only_when_exact_object_missing(self):
        synonym_meta = {
            ("SRC", "TOP"): sdr.SynonymMeta("SRC", "TOP", "SRC", "PUB_A", None),
            ("PUBLIC", "PUB_A"): sdr.SynonymMeta("PUBLIC", "PUB_A", "SRC", "PUB_B", None),
            ("PUBLIC", "PUB_B"): sdr.SynonymMeta("PUBLIC", "PUB_B", "SRC", "T1", None),
        }
        terminal = sdr.resolve_synonym_terminal_source(
            "SRC",
            "TOP",
            synonym_meta,
            {"SRC.T1": {"TABLE"}, "PUBLIC.PUB_A": {"SYNONYM"}, "PUBLIC.PUB_B": {"SYNONYM"}},
        )
        self.assertEqual(terminal, "SRC.T1")

    def test_resolve_synonym_terminal_source_does_not_override_real_object_with_public_synonym(self):
        synonym_meta = {
            ("SRC", "TOP"): sdr.SynonymMeta("SRC", "TOP", "SRC", "REAL_T", None),
            ("PUBLIC", "REAL_T"): sdr.SynonymMeta("PUBLIC", "REAL_T", "OTHER", "X1", None),
        }
        terminal = sdr.resolve_synonym_terminal_source(
            "SRC",
            "TOP",
            synonym_meta,
            {"SRC.REAL_T": {"TABLE"}, "PUBLIC.REAL_T": {"SYNONYM"}},
        )
        self.assertEqual(terminal, "SRC.REAL_T")

    def test_resolve_synonym_fixup_target_flattens_public_chain_to_mapped_terminal(self):
        synonym_meta = {
            ("SRC", "TOP"): sdr.SynonymMeta("SRC", "TOP", "SRC", "PUB_A", None),
            ("PUBLIC", "PUB_A"): sdr.SynonymMeta("PUBLIC", "PUB_A", "SRC", "PUB_B", None),
            ("PUBLIC", "PUB_B"): sdr.SynonymMeta("PUBLIC", "PUB_B", "SRC", "T1", None),
        }
        full_object_mapping = {
            "SRC.TOP": {"SYNONYM": "TGT.TOP"},
            "PUBLIC.PUB_A": {"SYNONYM": "PUBLIC.PUB_A"},
            "PUBLIC.PUB_B": {"SYNONYM": "PUBLIC.PUB_B"},
            "SRC.T1": {"TABLE": "TGT.T1"},
        }
        target = sdr.resolve_synonym_fixup_target(
            "SRC",
            "TOP",
            synonym_meta,
            full_object_mapping,
            remap_rules={},
        )
        self.assertEqual(target, "TGT.T1")

    def test_resolve_synonym_fixup_target_preserves_dblink_target(self):
        synonym_meta = {
            ("SRC", "S1"): sdr.SynonymMeta("SRC", "S1", "OTHER", "T1", "DBL1"),
        }
        target = sdr.resolve_synonym_fixup_target(
            "SRC",
            "S1",
            synonym_meta,
            {},
            remap_rules={},
        )
        self.assertEqual(target, "OTHER.T1@DBL1")

    def test_classify_missing_objects_marks_job_schedule_manual_only(self):
        tv_results = {
            "missing": [
                ("JOB", "TGT.JOB_A", "SRC.JOB_A"),
                ("SCHEDULE", "TGT.SCH_A", "SRC.SCH_A"),
            ],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        summary = sdr.classify_missing_objects(
            {"user": "u", "password": "p", "dsn": "d"},
            {"view_compat_rules": {}, "view_dblink_policy": "block"},
            tv_results,
            {
                "index_ok": [], "index_mismatched": [],
                "constraint_ok": [], "constraint_mismatched": [],
                "sequence_ok": [], "sequence_mismatched": [],
                "trigger_ok": [], "trigger_mismatched": [],
            },
            self._make_oracle_meta(),
            self._make_ob_meta(),
            {
                "SRC.JOB_A": {"JOB": "TGT.JOB_A"},
                "SRC.SCH_A": {"SCHEDULE": "TGT.SCH_A"},
            },
            {"SRC.JOB_A": {"JOB"}, "SRC.SCH_A": {"SCHEDULE"}},
            dependency_graph=None,
            object_parent_map=None,
            table_target_map={},
            synonym_meta_map={}
        )
        by_type = {row.obj_type: row for row in summary.missing_detail_rows}
        self.assertEqual(by_type["JOB"].support_state, sdr.SUPPORT_STATE_UNSUPPORTED)
        self.assertEqual(by_type["SCHEDULE"].support_state, sdr.SUPPORT_STATE_UNSUPPORTED)
        self.assertEqual(by_type["JOB"].reason_code, sdr.MANUAL_FIXUP_REASON_CODE)
        self.assertEqual(by_type["SCHEDULE"].reason_code, sdr.MANUAL_FIXUP_REASON_CODE)
        self.assertEqual(by_type["JOB"].action, sdr.MANUAL_FIXUP_ACTION_TEXT)
        self.assertEqual(by_type["SCHEDULE"].action, sdr.MANUAL_FIXUP_ACTION_TEXT)

    def test_classify_missing_objects_marks_job_schedule_semi_auto_action(self):
        tv_results = {
            "missing": [
                ("JOB", "TGT.JOB_A", "SRC.JOB_A"),
                ("SCHEDULE", "TGT.SCH_A", "SRC.SCH_A"),
            ],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        summary = sdr.classify_missing_objects(
            {"user": "u", "password": "p", "dsn": "d"},
            {
                "view_compat_rules": {},
                "view_dblink_policy": "block",
                "job_schedule_fixup_mode": "semi_auto",
            },
            tv_results,
            {
                "index_ok": [], "index_mismatched": [],
                "constraint_ok": [], "constraint_mismatched": [],
                "sequence_ok": [], "sequence_mismatched": [],
                "trigger_ok": [], "trigger_mismatched": [],
            },
            self._make_oracle_meta(),
            self._make_ob_meta(),
            {
                "SRC.JOB_A": {"JOB": "TGT.JOB_A"},
                "SRC.SCH_A": {"SCHEDULE": "TGT.SCH_A"},
            },
            {"SRC.JOB_A": {"JOB"}, "SRC.SCH_A": {"SCHEDULE"}},
            dependency_graph=None,
            object_parent_map=None,
            table_target_map={},
            synonym_meta_map={}
        )
        by_type = {row.obj_type: row for row in summary.missing_detail_rows}
        self.assertEqual(by_type["JOB"].support_state, sdr.SUPPORT_STATE_UNSUPPORTED)
        self.assertEqual(by_type["SCHEDULE"].support_state, sdr.SUPPORT_STATE_UNSUPPORTED)
        self.assertEqual(by_type["JOB"].action, sdr.MANUAL_FIXUP_SEMI_AUTO_ACTION_TEXT)
        self.assertEqual(by_type["SCHEDULE"].action, sdr.MANUAL_FIXUP_SEMI_AUTO_ACTION_TEXT)
        self.assertEqual(by_type["JOB"].detail, sdr.MANUAL_FIXUP_SEMI_AUTO_DETAIL_TEXT)
        self.assertEqual(by_type["SCHEDULE"].detail, sdr.MANUAL_FIXUP_SEMI_AUTO_DETAIL_TEXT)

    def test_classify_missing_objects_allows_verified_long_dependency(self):
        tv_results = {
            "missing": [
                ("VIEW", "TGT.V1", "SRC.V1"),
            ],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        oracle_meta = self._make_oracle_meta()
        oracle_meta = oracle_meta._replace(
            blacklist_tables={
                ("SRC", "T1"): {
                    ("LONG", "LONG"): sdr.BlacklistEntry("LONG", "LONG", "RULE=LONG")
                }
            },
            table_columns={
                ("SRC", "T1"): {
                    "C1": {"data_type": "LONG"}
                }
            }
        )
        ob_meta = self._make_ob_meta()
        ob_meta = ob_meta._replace(
            objects_by_type={"TABLE": {"TGT.T1"}},
            tab_columns={
                ("TGT", "T1"): {"C1": {"data_type": "CLOB"}}
            }
        )
        full_mapping = {
            "SRC.V1": {"VIEW": "TGT.V1"},
            "SRC.T1": {"TABLE": "TGT.T1"},
        }
        source_objects = {
            "SRC.V1": {"VIEW"},
            "SRC.T1": {"TABLE"},
        }
        deps = {("SRC", "V1", "VIEW", "SRC", "T1", "TABLE")}
        dependency_graph = sdr.build_dependency_graph(deps)
        table_target_map = {("SRC", "T1"): ("TGT", "T1")}
        settings = {"view_compat_rules": {}, "view_dblink_policy": "block"}
        ora_cfg = {"user": "u", "password": "p", "dsn": "d"}

        with mock.patch.object(
            sdr,
            "oracle_get_views_ddl_batch",
            return_value={("SRC", "V1"): "CREATE VIEW V1 AS SELECT * FROM T1"}
        ):
            summary = sdr.classify_missing_objects(
                ora_cfg,
                settings,
                tv_results,
                {
                    "index_ok": [], "index_mismatched": [],
                    "constraint_ok": [], "constraint_mismatched": [],
                    "sequence_ok": [], "sequence_mismatched": [],
                    "trigger_ok": [], "trigger_mismatched": [],
                },
                oracle_meta,
                ob_meta,
                full_mapping,
                source_objects,
                dependency_graph=dependency_graph,
                object_parent_map=None,
                table_target_map=table_target_map,
                synonym_meta_map={}
            )

        view_row = next(row for row in summary.missing_detail_rows if row.src_full == "SRC.V1")
        self.assertEqual(view_row.support_state, sdr.SUPPORT_STATE_SUPPORTED)
        self.assertNotIn(("SRC", "T1"), summary.unsupported_table_keys)

    def test_classify_missing_objects_marks_lob_oversize_as_risky_without_blocking_dependency(self):
        tv_results = {
            "missing": [
                ("TABLE", "TGT.T1", "SRC.T1"),
                ("VIEW", "TGT.V1", "SRC.V1"),
            ],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        oracle_meta = self._make_oracle_meta()._replace(
            blacklist_tables={
                ("SRC", "T1"): {
                    ("LOB_OVERSIZE", "CLOB"): sdr.BlacklistEntry("LOB_OVERSIZE", "CLOB", "RULE=LOB")
                }
            }
        )
        ob_meta = self._make_ob_meta()
        full_mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "SRC.V1": {"VIEW": "TGT.V1"},
        }
        source_objects = {
            "SRC.T1": {"TABLE"},
            "SRC.V1": {"VIEW"},
        }
        deps = {("SRC", "V1", "VIEW", "SRC", "T1", "TABLE")}
        dependency_graph = sdr.build_dependency_graph(deps)
        table_target_map = {("SRC", "T1"): ("TGT", "T1")}
        settings = {"view_compat_rules": {}, "view_dblink_policy": "block"}
        ora_cfg = {"user": "u", "password": "p", "dsn": "d"}
        with mock.patch.object(
            sdr,
            "oracle_get_views_ddl_batch",
            return_value={("SRC", "V1"): "CREATE VIEW V1 AS SELECT * FROM T1"}
        ):
            summary = sdr.classify_missing_objects(
                ora_cfg,
                settings,
                tv_results,
                {
                    "index_ok": [], "index_mismatched": [],
                    "constraint_ok": [], "constraint_mismatched": [],
                    "sequence_ok": [], "sequence_mismatched": [],
                    "trigger_ok": [], "trigger_mismatched": [],
                },
                oracle_meta,
                ob_meta,
                full_mapping,
                source_objects,
                dependency_graph=dependency_graph,
                object_parent_map=None,
                table_target_map=table_target_map,
                synonym_meta_map={}
            )
        table_row = next(row for row in summary.missing_detail_rows if row.obj_type == "TABLE")
        self.assertEqual(table_row.support_state, sdr.SUPPORT_STATE_RISKY)
        self.assertEqual(table_row.reason_code, "BLACKLIST_LOB_OVERSIZE")
        self.assertEqual(table_row.root_cause, "SRC.T1(BLACKLIST_LOB_OVERSIZE)")
        view_row = next(row for row in summary.missing_detail_rows if row.obj_type == "VIEW")
        self.assertEqual(view_row.support_state, sdr.SUPPORT_STATE_SUPPORTED)
        self.assertNotIn(("SRC", "T1"), summary.unsupported_table_keys)

    def test_classify_missing_objects_blocks_extra_when_target_table_missing(self):
        tv_results = {
            "missing": [("TABLE", "TGT.T1", "SRC.T1")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        extra_results = {
            "index_ok": [],
            "index_mismatched": [sdr.IndexMismatch("TGT.T1", {"IX_T1"}, set(), [])],
            "constraint_ok": [],
            "constraint_mismatched": [sdr.ConstraintMismatch("TGT.T1", {"CK_T1"}, set(), [], set())],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [sdr.TriggerMismatch("TGT.T1", {"TR_T1"}, set(), [], None)],
        }
        summary = sdr.classify_missing_objects(
            {"user": "u", "password": "p", "dsn": "d"},
            {"view_compat_rules": {}, "view_dblink_policy": "block"},
            tv_results,
            extra_results,
            self._make_oracle_meta(),
            self._make_ob_meta(),
            {"SRC.T1": {"TABLE": "TGT.T1"}},
            {"SRC.T1": {"TABLE"}},
            dependency_graph=None,
            object_parent_map=None,
            table_target_map={("SRC", "T1"): ("TGT", "T1")},
            synonym_meta_map={}
        )
        blocked_rows = [
            row for row in summary.unsupported_rows
            if row.obj_type in {"INDEX", "CONSTRAINT", "TRIGGER"}
        ]
        self.assertEqual(len(blocked_rows), 3)
        self.assertTrue(all(row.reason_code == "DEPENDENCY_TARGET_TABLE_MISSING" for row in blocked_rows))
        self.assertEqual(summary.extra_blocked_counts.get("INDEX"), 1)
        self.assertEqual(summary.extra_blocked_counts.get("CONSTRAINT"), 1)
        self.assertEqual(summary.extra_blocked_counts.get("TRIGGER"), 1)
        self.assertFalse(any(row.obj_type in {"INDEX", "CONSTRAINT", "TRIGGER"} for row in summary.extra_missing_rows))

    def test_classify_missing_objects_marks_trigger_on_temp_table_as_unsupported(self):
        tv_results = {
            "missing": [("TABLE", "TGT.T_TEMP", "SRC.T_TEMP")],
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
            "trigger_mismatched": [sdr.TriggerMismatch("TGT.T_TEMP", {"TRG_T_TEMP_BI"}, set(), [], None)],
        }
        oracle_meta = self._make_oracle_meta()._replace(
            blacklist_tables={
                ("SRC", "T_TEMP"): {
                    ("TEMP_TABLE", ""): sdr.BlacklistEntry("TEMP_TABLE", "", "RULE=TEMP")
                }
            }
        )
        summary = sdr.classify_missing_objects(
            {"user": "u", "password": "p", "dsn": "d"},
            {"view_compat_rules": {}, "view_dblink_policy": "block"},
            tv_results,
            extra_results,
            oracle_meta,
            self._make_ob_meta(),
            {"SRC.T_TEMP": {"TABLE": "TGT.T_TEMP"}},
            {"SRC.T_TEMP": {"TABLE"}},
            dependency_graph=None,
            object_parent_map=None,
            table_target_map={("SRC", "T_TEMP"): ("TGT", "T_TEMP")},
            synonym_meta_map={}
        )
        trigger_rows = [row for row in summary.unsupported_rows if row.obj_type == "TRIGGER"]
        self.assertEqual(len(trigger_rows), 1)
        self.assertEqual(trigger_rows[0].support_state, sdr.SUPPORT_STATE_UNSUPPORTED)
        self.assertEqual(
            trigger_rows[0].reason_code,
            sdr.TRIGGER_TEMP_TABLE_UNSUPPORTED_REASON_CODE
        )
        self.assertIn("ORA-00600/-4007", trigger_rows[0].reason)
        self.assertEqual(summary.extra_blocked_counts.get("TRIGGER"), 1)

    def test_classify_missing_objects_marks_temp_table_trigger_unsupported_when_blacklist_disabled(self):
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
            "trigger_mismatched": [sdr.TriggerMismatch("TGT.T_TEMP", {"TRG_T_TEMP_BI"}, set(), [], None)],
        }
        oracle_meta = self._make_oracle_meta(
            temporary_tables={("SRC", "T_TEMP")}
        )
        summary = sdr.classify_missing_objects(
            {"user": "u", "password": "p", "dsn": "d"},
            {"view_compat_rules": {}, "view_dblink_policy": "block", "blacklist_mode": "disabled"},
            tv_results,
            extra_results,
            oracle_meta,
            self._make_ob_meta(),
            {"SRC.T_TEMP": {"TABLE": "TGT.T_TEMP"}},
            {"SRC.T_TEMP": {"TABLE"}},
            dependency_graph=None,
            object_parent_map=None,
            table_target_map={("SRC", "T_TEMP"): ("TGT", "T_TEMP")},
            synonym_meta_map={}
        )
        trigger_rows = [row for row in summary.unsupported_rows if row.obj_type == "TRIGGER"]
        self.assertEqual(len(trigger_rows), 1)
        self.assertEqual(trigger_rows[0].support_state, sdr.SUPPORT_STATE_UNSUPPORTED)
        self.assertEqual(trigger_rows[0].reason_code, sdr.TRIGGER_TEMP_TABLE_UNSUPPORTED_REASON_CODE)
        self.assertEqual(trigger_rows[0].action, "改造/不迁移")
        self.assertEqual(summary.extra_blocked_counts.get("TRIGGER"), 1)
        self.assertFalse(any(row.obj_type == "TRIGGER" for row in summary.extra_missing_rows))

    def test_generate_fixup_skips_invalid_view_and_trigger(self):
        tv_results = {
            "missing": [("VIEW", "TGT.V1", "SRC.V1")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [
                sdr.TriggerMismatch(
                    table="TGT.T1",
                    missing_triggers={"TR1"},
                    extra_triggers=set(),
                    detail_mismatch=[],
                    missing_mappings=None
                )
            ],
        }
        master_list = [
            ("SRC.T1", "TGT.T1", "TABLE"),
            ("SRC.V1", "TGT.V1", "VIEW"),
        ]
        oracle_meta = self._make_oracle_meta()
        oracle_meta = oracle_meta._replace(
            object_statuses={
                ("SRC", "V1", "VIEW"): "INVALID",
                ("SRC", "TR1", "TRIGGER"): "INVALID",
            }
        )
        ob_meta = self._make_ob_meta()._replace(
            objects_by_type={"VIEW": set(), "TRIGGER": set(), "TABLE": {"TGT.T1"}}
        )
        full_mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "SRC.V1": {"VIEW": "TGT.V1"},
            "SRC.TR1": {"TRIGGER": "TGT.TR1"},
        }
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"VIEW", "TRIGGER"},
            "fixup_schema_list": set(),
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
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
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            self.assertFalse((Path(tmp_dir) / "view" / "TGT.V1.sql").exists())
            self.assertFalse((Path(tmp_dir) / "trigger" / "TGT.TR1.sql").exists())

    def test_generate_fixup_routes_temp_table_trigger_to_unsupported_dir(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [
                sdr.TriggerMismatch(
                    table="TGT.T_TEMP",
                    missing_triggers={"TRG_T_TEMP_BI"},
                    extra_triggers=set(),
                    detail_mismatch=[],
                    missing_mappings=None
                )
            ],
        }
        master_list = [("SRC.T_TEMP", "TGT.T_TEMP", "TABLE")]
        oracle_meta = self._make_oracle_meta(
            temporary_tables={("SRC", "T_TEMP")}
        )
        ob_meta = self._make_ob_meta()._replace(objects_by_type={"TABLE": {"TGT.T_TEMP"}, "TRIGGER": set()})
        full_mapping = {
            "SRC.T_TEMP": {"TABLE": "TGT.T_TEMP"},
            "SRC.TRG_T_TEMP_BI": {"TRIGGER": "TGT.TRG_T_TEMP_BI"},
        }
        support_row = sdr.ObjectSupportReportRow(
            obj_type="TRIGGER",
            src_full="SRC.TRG_T_TEMP_BI",
            tgt_full="TGT.TRG_T_TEMP_BI",
            support_state=sdr.SUPPORT_STATE_UNSUPPORTED,
            reason_code=sdr.TRIGGER_TEMP_TABLE_UNSUPPORTED_REASON_CODE,
            reason=sdr.TRIGGER_TEMP_TABLE_UNSUPPORTED_REASON,
            dependency="TGT.T_TEMP",
            action="改造/不迁移",
            detail="TRIGGER",
            root_cause="SRC.T_TEMP(TEMP_TABLE)",
        )
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"TRIGGER"},
            "fixup_schema_list": set(),
            "source_schemas_list": ["SRC"],
            "trigger_qualify_schema": True,
        }
        dbcat_data = {
            "SRC": {
                "TRIGGER": {
                    "TRG_T_TEMP_BI": (
                        'CREATE OR REPLACE TRIGGER "SRC"."TRG_T_TEMP_BI"\n'
                        'BEFORE INSERT ON "SRC"."T_TEMP"\n'
                        'FOR EACH ROW\n'
                        'BEGIN\n'
                        '  NULL;\n'
                        'END;\n'
                        '/'
                    )
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=(dbcat_data, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
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
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={("TRIGGER", "SRC.TRG_T_TEMP_BI"): support_row},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            unsupported_path = Path(tmp_dir) / "unsupported" / "trigger" / "TGT.TRG_T_TEMP_BI.sql"
            normal_path = Path(tmp_dir) / "trigger" / "TGT.TRG_T_TEMP_BI.sql"
            self.assertTrue(unsupported_path.exists())
            self.assertFalse(normal_path.exists())
            content = unsupported_path.read_text(encoding="utf-8")
            self.assertIn(sdr.TRIGGER_TEMP_TABLE_UNSUPPORTED_REASON_CODE, content)
            self.assertIn("action: 改造/不迁移", content)

    def test_generate_fixup_writes_missing_view_trigger(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [
                sdr.TriggerMismatch(
                    table="TGT.V1",
                    missing_triggers={"TGT.TRG_V1_IOI"},
                    extra_triggers=set(),
                    detail_mismatch=[],
                    missing_mappings=[("SRC.TRG_V1_IOI", "TGT.TRG_V1_IOI")]
                )
            ],
        }
        master_list = [("SRC.V1", "TGT.V1", "VIEW")]
        oracle_meta = self._make_oracle_meta(
            triggers={
                ("SRC", "V1"): {
                    "SRC.TRG_V1_IOI": {
                        "event": "INSERT",
                        "status": "ENABLED",
                        "owner": "SRC",
                        "name": "TRG_V1_IOI",
                    }
                }
            }
        )._replace(
            object_statuses={
                ("SRC", "TRG_V1_IOI", "TRIGGER"): "VALID",
                ("SRC", "V1", "VIEW"): "VALID",
            }
        )
        ob_meta = self._make_ob_meta()._replace(objects_by_type={"VIEW": {"TGT.V1"}, "TRIGGER": set()})
        full_mapping = {
            "SRC.V1": {"VIEW": "TGT.V1"},
            "SRC.TRG_V1_IOI": {"TRIGGER": "TGT.TRG_V1_IOI"},
        }
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"TRIGGER"},
            "fixup_schema_list": set(),
            "source_schemas_list": ["SRC"],
            "trigger_qualify_schema": True,
        }
        ddl_text = (
            'CREATE OR REPLACE TRIGGER "SRC"."TRG_V1_IOI"\n'
            'INSTEAD OF INSERT ON "SRC"."V1"\n'
            'BEGIN\n'
            '  NULL;\n'
            'END;\n'
            '/'
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            dbcat_data = {"SRC": {"TRIGGER": {"TRG_V1_IOI": ddl_text}}}
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=(dbcat_data, {})), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value="4.2.5.7"):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
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
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            normal_path = Path(tmp_dir) / "trigger" / "TGT.TRG_V1_IOI.sql"
            self.assertTrue(normal_path.exists())
            content = normal_path.read_text(encoding="utf-8").upper()
            self.assertIn("INSTEAD OF INSERT ON", content)
            self.assertIn('"TGT"."V1"', content)

    def test_generate_fixup_skips_synonym_with_out_of_scope_terminal_target(self):
        tv_results = {
            "missing": [("SYNONYM", "TGT.S1", "SRC.S1")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [("SRC.S1", "TGT.S1", "SYNONYM")]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()._replace(objects_by_type={"SYNONYM": set()})
        full_mapping = {"SRC.S1": {"SYNONYM": "TGT.S1"}}
        support_row = sdr.ObjectSupportReportRow(
            obj_type="SYNONYM",
            src_full="SRC.S1",
            tgt_full="TGT.S1",
            support_state=sdr.SUPPORT_STATE_BLOCKED,
            reason_code=sdr.SYNONYM_TARGET_OUT_OF_SCOPE_REASON_CODE,
            reason=sdr.SYNONYM_TARGET_OUT_OF_SCOPE_REASON_TEXT,
            dependency="SRC.S2",
            action=sdr.SYNONYM_TARGET_OUT_OF_SCOPE_ACTION_TEXT,
            detail="terminal_target_not_in_source_scope",
            root_cause="SRC.S2(SYNONYM_TARGET_OUT_OF_SCOPE)",
        )
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"SYNONYM"},
            "fixup_schema_list": set(),
            "source_schemas_list": ["SRC"],
            "synonym_fixup_scope": "all",
        }
        dbcat_data = {
            "SRC": {
                "SYNONYM": {
                    "S1": 'CREATE OR REPLACE SYNONYM "SRC"."S1" FOR "SRC"."S2";'
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=(dbcat_data, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
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
                    synonym_metadata={("SRC", "S1"): sdr.SynonymMeta("SRC", "S1", "SRC", "S2", None)},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={("SYNONYM", "SRC.S1"): support_row},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            self.assertFalse((Path(tmp_dir) / "synonym" / "TGT.S1.sql").exists())
            self.assertFalse((Path(tmp_dir) / "unsupported" / "synonym" / "TGT.S1.sql").exists())

    def test_generate_fixup_allows_public_synonym_without_public_in_fixup_schemas(self):
        tv_results = {
            "missing": [("SYNONYM", "PUBLIC.S1", "PUBLIC.S1")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [("PUBLIC.S1", "PUBLIC.S1", "SYNONYM")]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()._replace(objects_by_type={"SYNONYM": set()})
        full_mapping = {"PUBLIC.S1": {"SYNONYM": "PUBLIC.S1"}}
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"SYNONYM"},
            "fixup_schema_list": {"SRC"},
            "source_schemas_list": ["SRC"],
            "synonym_fixup_scope": "public_only",
        }
        fixup_skip_summary: Dict[str, Dict[str, object]] = {}
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
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
                    synonym_metadata={("PUBLIC", "S1"): sdr.SynonymMeta("PUBLIC", "S1", "SRC", "T1", None)},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    fixup_skip_summary=fixup_skip_summary,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            synonym_path = Path(tmp_dir) / "synonym" / "PUBLIC.S1.sql"
            self.assertTrue(synonym_path.exists())
            content = synonym_path.read_text(encoding="utf-8")
            self.assertIn("CREATE OR REPLACE PUBLIC SYNONYM", content)
            self.assertEqual(fixup_skip_summary.get("SYNONYM", {}).get("missing_total"), 1)
            self.assertEqual(fixup_skip_summary.get("SYNONYM", {}).get("task_total"), 1)
            self.assertEqual(fixup_skip_summary.get("SYNONYM", {}).get("generated"), 1)

    def test_generate_fixup_public_synonym_without_meta_uses_fallback_ddl(self):
        tv_results = {
            "missing": [("SYNONYM", "PUBLIC.S1", "PUBLIC.S1")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [("PUBLIC.S1", "PUBLIC.S1", "SYNONYM")]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()._replace(objects_by_type={"SYNONYM": set()})
        full_mapping = {"PUBLIC.S1": {"SYNONYM": "PUBLIC.S1"}}
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"SYNONYM"},
            "fixup_schema_list": {"SRC"},
            "source_schemas_list": ["SRC"],
            "synonym_fixup_scope": "public_only",
        }
        fixup_skip_summary: Dict[str, Dict[str, object]] = {}
        fallback_map = {
            ("PUBLIC", "SYNONYM", "S1"): 'CREATE OR REPLACE PUBLIC SYNONYM "S1" FOR "SRC"."T1";'
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value=fallback_map), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
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
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    fixup_skip_summary=fixup_skip_summary,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            synonym_path = Path(tmp_dir) / "synonym" / "PUBLIC.S1.sql"
            self.assertTrue(synonym_path.exists())
            self.assertEqual(fixup_skip_summary.get("SYNONYM", {}).get("missing_total"), 1)
            self.assertEqual(fixup_skip_summary.get("SYNONYM", {}).get("task_total"), 1)
            self.assertEqual(fixup_skip_summary.get("SYNONYM", {}).get("generated"), 1)

    def test_generate_fixup_public_synonym_without_meta_records_ddl_missing(self):
        tv_results = {
            "missing": [("SYNONYM", "PUBLIC.S1", "PUBLIC.S1")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [("PUBLIC.S1", "PUBLIC.S1", "SYNONYM")]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()._replace(objects_by_type={"SYNONYM": set()})
        full_mapping = {"PUBLIC.S1": {"SYNONYM": "PUBLIC.S1"}}
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"SYNONYM"},
            "fixup_schema_list": {"SRC"},
            "source_schemas_list": ["SRC"],
            "synonym_fixup_scope": "public_only",
        }
        fixup_skip_summary: Dict[str, Dict[str, object]] = {}
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "oracle_get_ddl", return_value=None), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
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
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    fixup_skip_summary=fixup_skip_summary,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            self.assertFalse((Path(tmp_dir) / "synonym" / "PUBLIC.S1.sql").exists())
            self.assertEqual(fixup_skip_summary.get("SYNONYM", {}).get("missing_total"), 1)
            self.assertEqual(fixup_skip_summary.get("SYNONYM", {}).get("task_total"), 1)
            self.assertEqual(fixup_skip_summary.get("SYNONYM", {}).get("generated"), 0)
            self.assertEqual(fixup_skip_summary.get("SYNONYM", {}).get("skipped", {}).get("ddl_missing"), 1)

    def test_generate_fixup_skips_extra_when_parent_table_missing(self):
        tv_results = {
            "missing": [("TABLE", "TGT.T1", "SRC.T1")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [],
            "index_mismatched": [
                sdr.IndexMismatch(
                    table="TGT.T1",
                    missing_indexes={"IX1"},
                    extra_indexes=set(),
                    detail_mismatch=[],
                )
            ],
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="TGT.T1",
                    missing_constraints={"CK1"},
                    extra_constraints=set(),
                    detail_mismatch=[],
                    downgraded_pk_constraints=set(),
                )
            ],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [
                sdr.TriggerMismatch(
                    table="TGT.T1",
                    missing_triggers={"TR1"},
                    extra_triggers=set(),
                    detail_mismatch=[],
                    missing_mappings=None
                )
            ],
        }
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()._replace(objects_by_type={"TABLE": set()})
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"TABLE", "INDEX", "CONSTRAINT", "TRIGGER"},
            "fixup_schema_list": set(),
            "source_schemas_list": ["SRC"],
        }
        fixup_skip_summary: Dict[str, Dict[str, object]] = {}
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    {"SRC.T1": {"TABLE": "TGT.T1"}},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    fixup_skip_summary=fixup_skip_summary,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={},
                )
            self.assertFalse((Path(tmp_dir) / "index").exists())
            self.assertFalse((Path(tmp_dir) / "constraint").exists())
            self.assertFalse((Path(tmp_dir) / "trigger").exists())
            self.assertEqual(
                fixup_skip_summary.get("INDEX", {}).get("skipped", {}).get("table_missing_target"),
                1,
            )

    def test_generate_fixup_emits_job_semi_auto_draft_when_ddl_missing(self):
        tv_results = {
            "missing": [("JOB", "TGT.JOB_A", "SRC.JOB_A")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
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
        master_list = [("SRC.JOB_A", "TGT.JOB_A", "JOB")]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()._replace(objects_by_type={"JOB": set()})
        support_state_map = {
            ("JOB", "SRC.JOB_A"): sdr.ObjectSupportReportRow(
                obj_type="JOB",
                src_full="SRC.JOB_A",
                tgt_full="TGT.JOB_A",
                support_state=sdr.SUPPORT_STATE_UNSUPPORTED,
                reason_code=sdr.MANUAL_FIXUP_REASON_CODE,
                reason=sdr.MANUAL_FIXUP_REASON_TEXT,
                dependency="-",
                action=sdr.MANUAL_FIXUP_SEMI_AUTO_ACTION_TEXT,
                detail=sdr.MANUAL_FIXUP_SEMI_AUTO_DETAIL_TEXT,
                root_cause=f"SRC.JOB_A({sdr.MANUAL_FIXUP_REASON_CODE})",
            )
        }
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"JOB"},
            "fixup_schema_list": set(),
            "job_schedule_fixup_mode": "semi_auto",
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    {"SRC.JOB_A": {"JOB": "TGT.JOB_A"}},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map=support_state_map,
                    unsupported_table_keys=set(),
                    view_compat_map={},
                )
            draft_path = Path(tmp_dir) / "unsupported" / "job" / "TGT.JOB_A.sql"
            self.assertTrue(draft_path.exists())
            content = draft_path.read_text(encoding="utf-8")
            self.assertIn("JOB 半自动草案模板", content)
            self.assertIn("DBA_SCHEDULER_JOBS", content)

    def test_generate_fixup_skips_view_compile(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = []
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        ob_meta = ob_meta._replace(objects_by_type={
            "VIEW": {"TGT.V1"},
            "TYPE BODY": {"TGT.TB1"},
        })
        full_mapping = {}
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"VIEW"},
            "fixup_schema_list": set(),
            "source_schemas_list": ["SRC"],
        }
        dep_report = {
            "missing": [
                sdr.DependencyIssue(
                    dependent="TGT.V1",
                    dependent_type="VIEW",
                    referenced="TGT.T1",
                    referenced_type="TABLE",
                    reason="MISSING_DEP"
                ),
                sdr.DependencyIssue(
                    dependent="TGT.TB1",
                    dependent_type="TYPE BODY",
                    referenced="TGT.T1",
                    referenced_type="TABLE",
                    reason="MISSING_DEP"
                )
            ],
            "unexpected": [],
            "skipped": []
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    full_mapping,
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report=dep_report,
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            self.assertFalse((Path(tmp_dir) / "compile").exists())

    def test_generate_fixup_writes_sequence_restart_when_target_behind(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = []
        oracle_meta = self._make_oracle_meta(
            sequences={"SRC": {"SEQ1"}},
        )._replace(
            sequence_attrs={"SRC": {"SEQ1": {"last_number": 21}}}
        )
        ob_meta = self._make_ob_meta(
            sequences={"TGT": {"SEQ1"}},
        )._replace(
            sequence_attrs={"TGT": {"SEQ1": {"last_number": 1}}}
        )
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"SEQUENCE"},
            "fixup_schema_list": set(),
            "source_schemas_list": ["SRC"],
            "sequence_sync_mode": "last_number",
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    {"SRC.SEQ1": {"SEQUENCE": "TGT.SEQ1"}},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            restart_path = Path(tmp_dir) / "sequence_restart" / "TGT.SEQ1.sql"
            self.assertTrue(restart_path.exists())
            content = restart_path.read_text(encoding="utf-8")
            self.assertIn('ALTER SEQUENCE "TGT"."SEQ1" RESTART START WITH 21;', content)
            self.assertIn("建议在数据装载完成后显式执行", content)

    def test_generate_fixup_skips_sequence_restart_when_target_caught_up(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = []
        oracle_meta = self._make_oracle_meta(
            sequences={"SRC": {"SEQ1"}},
        )._replace(
            sequence_attrs={"SRC": {"SEQ1": {"last_number": 21}}}
        )
        ob_meta = self._make_ob_meta(
            sequences={"TGT": {"SEQ1"}},
        )._replace(
            sequence_attrs={"TGT": {"SEQ1": {"last_number": 25}}}
        )
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"SEQUENCE"},
            "fixup_schema_list": set(),
            "source_schemas_list": ["SRC"],
            "sequence_sync_mode": "last_number",
        }
        fixup_skip_summary = {}
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    {"SRC.SEQ1": {"SEQUENCE": "TGT.SEQ1"}},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    fixup_skip_summary=fixup_skip_summary,
                    view_compat_map={}
                )
            self.assertFalse((Path(tmp_dir) / "sequence_restart" / "TGT.SEQ1.sql").exists())
            self.assertEqual(fixup_skip_summary.get("SEQUENCE_RESTART", {}).get("generated"), 0)

    def test_generate_fixup_skips_missing_sequence_restart_when_create_already_synced(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [],
            "sequence_mismatched": [
                sdr.SequenceMismatch(
                    src_schema="SRC",
                    tgt_schema="TGT",
                    missing_sequences={"SEQ1"},
                    extra_sequences=set(),
                    note=None,
                    missing_mappings=[("SRC.SEQ1", "TGT.SEQ1")],
                    detail_mismatch=None,
                )
            ],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = []
        oracle_meta = self._make_oracle_meta(
            sequences={"SRC": {"SEQ1"}},
        )._replace(
            sequence_attrs={"SRC": {"SEQ1": {"last_number": 21}}}
        )
        ob_meta = self._make_ob_meta(
            sequences={"TGT": set()},
        )._replace(sequence_attrs={"TGT": {}})
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"SEQUENCE"},
            "fixup_schema_list": set(),
            "source_schemas_list": ["SRC"],
            "sequence_sync_mode": "last_number",
        }
        fixup_skip_summary = {}
        raw_seq_ddl = 'CREATE SEQUENCE "SRC"."SEQ1" START WITH 21 INCREMENT BY 1 NOCACHE;'
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={("SRC", "SEQUENCE", "SEQ1"): raw_seq_ddl}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    {"SRC.SEQ1": {"SEQUENCE": "TGT.SEQ1"}},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    fixup_skip_summary=fixup_skip_summary,
                    view_compat_map={}
                )
            self.assertTrue((Path(tmp_dir) / "sequence" / "TGT.SEQ1.sql").exists())
            self.assertFalse((Path(tmp_dir) / "sequence_restart" / "TGT.SEQ1.sql").exists())
            self.assertEqual(fixup_skip_summary.get("SEQUENCE_RESTART", {}).get("generated"), 0)
            self.assertEqual(
                fixup_skip_summary.get("SEQUENCE_RESTART", {}).get("skipped", {}).get("TARGET_CREATE_ALREADY_SYNCED"),
                1,
            )

    def test_generate_fixup_view_avoids_implicit_dependency_rename(self):
        tv_results = {
            "missing": [("VIEW", "TGT.PERST_TRAIL_BVW", "SRC.PERST_TRAIL_BVW")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [
            ("SRC.PERST_TRAIL_BVW", "TGT.PERST_TRAIL_BVW", "VIEW"),
        ]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        full_mapping = {
            "SRC.PERST_TRAIL_BVW": {"VIEW": "TGT.PERST_TRAIL_BVW"},
            # 模拟异常映射：依赖对象被映射到了不同对象名
            "SRC.PERST_TRAIL": {"VIEW": "TGT.PERST_TRAIL_VW"},
        }
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"VIEW"},
            "fixup_schema_list": set(),
            "source_schemas_list": ["SRC"],
        }
        raw_view_ddl = (
            "CREATE OR REPLACE VIEW SRC.PERST_TRAIL_BVW AS\n"
            "SELECT * FROM SRC.PERST_TRAIL A;"
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(
                     sdr,
                     "oracle_get_ddl_batch",
                     return_value={("SRC", "VIEW", "PERST_TRAIL_BVW"): raw_view_ddl}
                 ), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value="4.2.5.7"):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
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
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            sql_path = Path(tmp_dir) / "view" / "TGT.PERST_TRAIL_BVW.sql"
            self.assertTrue(sql_path.exists())
            content = sql_path.read_text(encoding="utf-8").upper()
            self.assertIn("FROM TGT.PERST_TRAIL A", content)
            self.assertNotIn("PERST_TRAIL_VW", content)

    def test_generate_fixup_filters_grant_owner_scope(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [("APP.T1", "APP.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        full_mapping = {"APP.T1": {"TABLE": "APP.T1"}}
        grant_plan = sdr.GrantPlan(
            object_grants={
                "APP": {
                    sdr.ObjectGrantEntry("SELECT", "APP.T1", False),
                    sdr.ObjectGrantEntry("SELECT", "SYS.DUAL", False),
                    sdr.ObjectGrantEntry("SELECT", "PUBLIC.DUAL", False),
                }
            },
            column_grants={},
            sys_privs={},
            role_privs={},
            role_ddls=[],
            filtered_grants=[],
            view_grant_targets=set(),
            object_target_types={"APP.T1": "TABLE"},
        )
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": set(),
            "fixup_schema_list": set(),
            "source_schemas_list": ["APP"],
            "grant_merge_privileges": "true",
            "grant_merge_grantees": "true",
        }
        dep_report = {"missing": [], "unexpected": [], "skipped": []}
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None), \
                 mock.patch.object(sdr, "load_ob_grant_catalog", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    full_mapping,
                    {},
                    grant_plan=grant_plan,
                    enable_grant_generation=True,
                    dependency_report=dep_report,
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            app_grants = Path(tmp_dir) / "grants_all" / "APP.grants.sql"
            sys_grants = Path(tmp_dir) / "grants_all" / "SYS.grants.sql"
            public_grants = Path(tmp_dir) / "grants_all" / "PUBLIC.grants.sql"
            self.assertTrue(app_grants.exists())
            self.assertFalse(sys_grants.exists())
            self.assertFalse(public_grants.exists())
            content = app_grants.read_text(encoding="utf-8")
            self.assertIn("-- OBJECT_TYPE: TABLE (1)", content)
            self.assertIn("-- TABLE_OBJECT_GRANTS (1)", content)
            self.assertIn("GRANT SELECT ON APP.T1", content)
            self.assertNotIn("ALTER SESSION SET CURRENT_SCHEMA", content)

    def test_generate_fixup_groups_object_grants_by_object_type(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [("APP.T1", "APP.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        ob_meta = ob_meta._replace(
            objects_by_type={
                **dict(ob_meta.objects_by_type),
                "TABLE": {"APP.T1"},
                "TYPE": {"APP.TYP1"},
                "PACKAGE": {"APP.PKG1"},
            }
        )
        full_mapping = {"APP.T1": {"TABLE": "APP.T1"}}
        grant_plan = sdr.GrantPlan(
            object_grants={
                "APP": {
                    sdr.ObjectGrantEntry("SELECT", "APP.T1", False),
                    sdr.ObjectGrantEntry("EXECUTE", "APP.TYP1", False),
                    sdr.ObjectGrantEntry("EXECUTE", "APP.PKG1", False),
                }
            },
            column_grants={},
            sys_privs={},
            role_privs={},
            role_ddls=[],
            filtered_grants=[],
            view_grant_targets=set(),
            object_target_types={
                "APP.T1": "TABLE",
                "APP.TYP1": "TYPE",
                "APP.PKG1": "PACKAGE",
            },
        )
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": set(),
            "fixup_schema_list": set(),
            "source_schemas_list": ["APP"],
            "grant_merge_privileges": "true",
            "grant_merge_grantees": "true",
        }
        dep_report = {"missing": [], "unexpected": [], "skipped": []}
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None), \
                 mock.patch.object(sdr, "load_ob_grant_catalog", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    full_mapping,
                    {},
                    grant_plan=grant_plan,
                    enable_grant_generation=True,
                    dependency_report=dep_report,
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            app_grants = Path(tmp_dir) / "grants_all" / "APP.grants.sql"
            content = app_grants.read_text(encoding="utf-8")
            self.assertIn("-- OBJECT_TYPE: TABLE (1)", content)
            self.assertIn("-- TABLE_OBJECT_GRANTS (1)", content)
            self.assertIn("-- OBJECT_TYPE: TYPE (1)", content)
            self.assertIn("-- OBJECT_TYPE: PACKAGE (1)", content)
            self.assertIn("GRANT SELECT ON APP.T1 TO APP;", content)
            self.assertIn("GRANT EXECUTE ON APP.TYP1 TO APP;", content)
            self.assertIn("GRANT EXECUTE ON APP.PKG1 TO APP;", content)

    def test_generate_fixup_subgroups_table_object_and_column_grants(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [("APP.T1", "APP.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        ob_meta = ob_meta._replace(
            objects_by_type={
                **dict(ob_meta.objects_by_type),
                "TABLE": {"APP.T1"},
            }
        )
        full_mapping = {"APP.T1": {"TABLE": "APP.T1"}}
        grant_plan = sdr.GrantPlan(
            object_grants={
                "APP": {
                    sdr.ObjectGrantEntry("SELECT", "APP.T1", False),
                }
            },
            column_grants={
                "APP": {
                    sdr.ColumnGrantEntry("UPDATE", "APP.T1", "C1", False),
                }
            },
            sys_privs={},
            role_privs={},
            role_ddls=[],
            filtered_grants=[],
            view_grant_targets=set(),
            object_target_types={
                "APP.T1": "TABLE",
            },
        )
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": set(),
            "fixup_schema_list": set(),
            "source_schemas_list": ["APP"],
            "grant_merge_privileges": "true",
            "grant_merge_grantees": "true",
        }
        dep_report = {"missing": [], "unexpected": [], "skipped": []}
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None), \
                 mock.patch.object(sdr, "load_ob_grant_catalog", return_value=None):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    full_mapping,
                    {},
                    grant_plan=grant_plan,
                    enable_grant_generation=True,
                    dependency_report=dep_report,
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            app_grants = Path(tmp_dir) / "grants_all" / "APP.grants.sql"
            content = app_grants.read_text(encoding="utf-8")
            self.assertIn("-- OBJECT_TYPE: TABLE (2)", content)
            self.assertIn("-- TABLE_OBJECT_GRANTS (1)", content)
            self.assertIn("-- TABLE_COLUMN_GRANTS (1)", content)
            self.assertLess(
                content.index("-- TABLE_OBJECT_GRANTS (1)"),
                content.index("-- TABLE_COLUMN_GRANTS (1)")
            )
            self.assertIn("GRANT SELECT ON APP.T1 TO APP;", content)
            self.assertIn("GRANT UPDATE (C1) ON APP.T1 TO APP;", content)

    def test_generate_fixup_outputs_grants_revoke_for_extra_public_grants(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [("APP.T1", "APP.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        full_mapping = {"APP.T1": {"TABLE": "APP.T1"}}
        grant_plan = sdr.GrantPlan(
            object_grants={
                "APP": {sdr.ObjectGrantEntry("SELECT", "APP.T1", False)}
            },
            column_grants={},
            sys_privs={},
            role_privs={},
            role_ddls=[],
            filtered_grants=[],
            view_grant_targets=set()
        )
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": set(),
            "fixup_schema_list": set(),
            "source_schemas_list": ["APP"],
            "grant_merge_privileges": "true",
            "grant_merge_grantees": "true",
        }
        dep_report = {"missing": [], "unexpected": [], "skipped": []}
        target_grants = {
            ("APP", "SELECT", "APP.T1", False),
            ("PUBLIC", "SELECT", "APP.T1", False),
            ("APP_AUDITOR", "SELECT", "APP.T1", False),
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value=None), \
                 mock.patch.object(sdr, "load_ob_grant_catalog", return_value=None), \
                 mock.patch.object(sdr, "load_ob_object_privileges_by_owners", return_value=target_grants):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    full_mapping,
                    {},
                    grant_plan=grant_plan,
                    enable_grant_generation=True,
                    dependency_report=dep_report,
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            revoke_sql = Path(tmp_dir) / "grants_revoke" / "revoke_public_object_grants.sql"
            review_txt = Path(tmp_dir) / "grants_revoke" / "review_extra_object_grants.txt"
            self.assertTrue(revoke_sql.exists())
            self.assertTrue(review_txt.exists())
            self.assertIn('REVOKE SELECT ON "APP"."T1" FROM PUBLIC;', revoke_sql.read_text(encoding="utf-8"))
            review_content = review_txt.read_text(encoding="utf-8")
            self.assertIn("APP_AUDITOR", review_content)
            self.assertIn("MANUAL_REVIEW", review_content)

    def test_generate_fixup_orders_packages_by_dependency(self):
        tv_results = {
            "missing": [
                ("PACKAGE BODY", "TGT.PKG_A", "SRC.PKG_A"),
                ("PACKAGE", "TGT.PKG_A", "SRC.PKG_A"),
                ("PACKAGE BODY", "TGT.PKG_B", "SRC.PKG_B"),
                ("PACKAGE", "TGT.PKG_B", "SRC.PKG_B"),
            ],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [
            ("SRC.PKG_A", "TGT.PKG_A", "PACKAGE"),
            ("SRC.PKG_A", "TGT.PKG_A", "PACKAGE BODY"),
            ("SRC.PKG_B", "TGT.PKG_B", "PACKAGE"),
            ("SRC.PKG_B", "TGT.PKG_B", "PACKAGE BODY"),
        ]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()._replace(
            objects_by_type={"PACKAGE": set(), "PACKAGE BODY": set()}
        )
        full_mapping = {
            "SRC.PKG_A": {"PACKAGE": "TGT.PKG_A", "PACKAGE BODY": "TGT.PKG_A"},
            "SRC.PKG_B": {"PACKAGE": "TGT.PKG_B", "PACKAGE BODY": "TGT.PKG_B"},
        }
        expected_pairs = {
            ("TGT.PKG_A", "PACKAGE", "TGT.PKG_B", "PACKAGE")
        }
        dbcat_data = {
            "SRC": {
                "PACKAGE": {
                    "PKG_A": "CREATE OR REPLACE PACKAGE SRC.PKG_A AS END;",
                    "PKG_B": "CREATE OR REPLACE PACKAGE SRC.PKG_B AS END;",
                },
                "PACKAGE BODY": {
                    "PKG_A": "CREATE OR REPLACE PACKAGE BODY SRC.PKG_A AS END;",
                    "PKG_B": "CREATE OR REPLACE PACKAGE BODY SRC.PKG_B AS END;",
                }
            }
        }
        dbcat_meta = {
            ("SRC", "PACKAGE", "PKG_A"): ("cache", 0.01),
            ("SRC", "PACKAGE", "PKG_B"): ("cache", 0.01),
            ("SRC", "PACKAGE BODY", "PKG_A"): ("cache", 0.01),
            ("SRC", "PACKAGE BODY", "PKG_B"): ("cache", 0.01),
        }
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"PACKAGE", "PACKAGE BODY"},
            "fixup_schema_list": set(),
        }
        recorded: List[Tuple[str, str]] = []
        def fake_write_fixup_file(_base, subdir, filename, *_args, **_kwargs):
            if subdir in ("package", "package_body"):
                recorded.append((subdir, filename))
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            orig_write = sdr.write_fixup_file
            orig_fetch = sdr.fetch_dbcat_schema_objects
            orig_ver = sdr.get_oceanbase_version
            try:
                sdr.write_fixup_file = fake_write_fixup_file
                sdr.fetch_dbcat_schema_objects = lambda *_a, **_k: (dbcat_data, dbcat_meta)
                sdr.get_oceanbase_version = lambda *_a, **_k: None
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
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
                    expected_dependency_pairs=expected_pairs,
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            finally:
                sdr.write_fixup_file = orig_write
                sdr.fetch_dbcat_schema_objects = orig_fetch
                sdr.get_oceanbase_version = orig_ver
        order_index = {item: idx for idx, item in enumerate(recorded)}
        self.assertLess(order_index.get(("package", "TGT.PKG_B.sql")), order_index.get(("package", "TGT.PKG_A.sql")))
        self.assertLess(order_index.get(("package", "TGT.PKG_A.sql")), order_index.get(("package_body", "TGT.PKG_A.sql")))
        self.assertLess(order_index.get(("package", "TGT.PKG_B.sql")), order_index.get(("package_body", "TGT.PKG_B.sql")))

    def test_generate_fixup_orders_types_before_routines(self):
        tv_results = {
            "missing": [
                ("TYPE BODY", "TGT.TYP1", "SRC.TYP1"),
                ("TYPE", "TGT.TYP1", "SRC.TYP1"),
                ("FUNCTION", "TGT.F1", "SRC.F1"),
                ("PROCEDURE", "TGT.P1", "SRC.P1"),
            ],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [
            ("SRC.TYP1", "TGT.TYP1", "TYPE"),
            ("SRC.TYP1", "TGT.TYP1", "TYPE BODY"),
            ("SRC.F1", "TGT.F1", "FUNCTION"),
            ("SRC.P1", "TGT.P1", "PROCEDURE"),
        ]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()._replace(
            objects_by_type={"TYPE": set(), "TYPE BODY": set(), "FUNCTION": set(), "PROCEDURE": set()}
        )
        full_mapping = {
            "SRC.TYP1": {"TYPE": "TGT.TYP1", "TYPE BODY": "TGT.TYP1"},
            "SRC.F1": {"FUNCTION": "TGT.F1"},
            "SRC.P1": {"PROCEDURE": "TGT.P1"},
        }
        expected_pairs = {
            ("TGT.F1", "FUNCTION", "TGT.TYP1", "TYPE"),
            ("TGT.P1", "PROCEDURE", "TGT.TYP1", "TYPE"),
        }
        dbcat_data = {
            "SRC": {
                "TYPE": {"TYP1": "CREATE OR REPLACE TYPE SRC.TYP1 AS OBJECT (C1 NUMBER);"},
                "TYPE BODY": {"TYP1": "CREATE OR REPLACE TYPE BODY SRC.TYP1 AS END;"},
                "FUNCTION": {"F1": "CREATE OR REPLACE FUNCTION SRC.F1 RETURN NUMBER AS BEGIN RETURN 1; END;"},
                "PROCEDURE": {"P1": "CREATE OR REPLACE PROCEDURE SRC.P1 AS BEGIN NULL; END;"},
            }
        }
        dbcat_meta = {
            ("SRC", "TYPE", "TYP1"): ("cache", 0.01),
            ("SRC", "TYPE BODY", "TYP1"): ("cache", 0.01),
            ("SRC", "FUNCTION", "F1"): ("cache", 0.01),
            ("SRC", "PROCEDURE", "P1"): ("cache", 0.01),
        }
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"TYPE", "TYPE BODY", "FUNCTION", "PROCEDURE"},
            "fixup_schema_list": set(),
        }
        recorded: List[Tuple[str, str]] = []

        def fake_write_fixup_file(_base, subdir, filename, *_args, **_kwargs):
            if subdir in ("type", "type_body", "function", "procedure"):
                recorded.append((subdir, filename))

        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            orig_write = sdr.write_fixup_file
            orig_fetch = sdr.fetch_dbcat_schema_objects
            orig_ver = sdr.get_oceanbase_version
            try:
                sdr.write_fixup_file = fake_write_fixup_file
                sdr.fetch_dbcat_schema_objects = lambda *_a, **_k: (dbcat_data, dbcat_meta)
                sdr.get_oceanbase_version = lambda *_a, **_k: None
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
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
                    expected_dependency_pairs=expected_pairs,
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )
            finally:
                sdr.write_fixup_file = orig_write
                sdr.fetch_dbcat_schema_objects = orig_fetch
                sdr.get_oceanbase_version = orig_ver
        order_index = {item: idx for idx, item in enumerate(recorded)}
        self.assertLess(order_index.get(("type", "TGT.TYP1.sql")), order_index.get(("function", "TGT.F1.sql")))
        self.assertLess(order_index.get(("type", "TGT.TYP1.sql")), order_index.get(("procedure", "TGT.P1.sql")))
        self.assertLess(order_index.get(("type", "TGT.TYP1.sql")), order_index.get(("type_body", "TGT.TYP1.sql")))

    def test_generate_fixup_cleans_dir_when_master_list_empty(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        settings = {
            "fixup_dir": "",
            "fixup_force_clean": "true",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": set(),
            "fixup_schema_list": set(),
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            fixup_dir = Path(tmp_dir)
            dummy_path = fixup_dir / "old.sql"
            dummy_path.write_text("SELECT 1;\n", encoding="utf-8")
            settings["fixup_dir"] = str(fixup_dir)
            settings["config_dir"] = str(fixup_dir)
            sdr.generate_fixup_scripts(
                {"user": "u", "password": "p", "dsn": "d"},
                {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                settings,
                tv_results,
                extra_results,
                [],
                oracle_meta,
                {},
                {},
                grant_plan=None,
                enable_grant_generation=False,
                dependency_report={"missing": [], "unexpected": [], "skipped": []},
                ob_meta=ob_meta,
                expected_dependency_pairs=set(),
                synonym_metadata={},
                trigger_filter_entries=None,
                trigger_filter_enabled=False,
                package_results=None,
                report_dir=None,
                report_timestamp=None,
                support_state_map={},
                unsupported_table_keys=set(),
                view_compat_map={}
            )
            self.assertFalse(dummy_path.exists())

    def test_generate_fixup_blocks_outside_dir_when_disallowed(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        with tempfile.TemporaryDirectory() as cfg_tmp, tempfile.TemporaryDirectory() as out_tmp:
            config_dir = Path(cfg_tmp)
            outside_dir = Path(out_tmp)
            settings = {
                "config_dir": str(config_dir),
                "fixup_dir": str(outside_dir),
                "fixup_dir_allow_outside_repo": "false",
                "fixup_force_clean": "true",
                "fixup_clean_outside_repo": "true",
                "fixup_workers": 1,
                "progress_log_interval": 999,
                "fixup_type_set": set(),
                "fixup_schema_list": set(),
            }
            with self.assertRaises(sdr.FatalError):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    [],
                    oracle_meta,
                    {},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={}
                )

    def test_generate_fixup_outside_dir_not_cleaned_without_dual_gate(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": []
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        with tempfile.TemporaryDirectory() as cfg_tmp, tempfile.TemporaryDirectory() as out_tmp:
            config_dir = Path(cfg_tmp)
            outside_dir = Path(out_tmp)
            sentinel = outside_dir / "old.sql"
            sentinel.write_text("SELECT 1;\n", encoding="utf-8")
            settings = {
                "config_dir": str(config_dir),
                "fixup_dir": str(outside_dir),
                "fixup_dir_allow_outside_repo": "true",
                "fixup_force_clean": "true",
                "fixup_clean_outside_repo": "false",
                "fixup_workers": 1,
                "progress_log_interval": 999,
                "fixup_type_set": set(),
                "fixup_schema_list": set(),
            }
            sdr.generate_fixup_scripts(
                {"user": "u", "password": "p", "dsn": "d"},
                {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                settings,
                tv_results,
                extra_results,
                [],
                oracle_meta,
                {},
                {},
                grant_plan=None,
                enable_grant_generation=False,
                dependency_report={"missing": [], "unexpected": [], "skipped": []},
                ob_meta=ob_meta,
                expected_dependency_pairs=set(),
                synonym_metadata={},
                trigger_filter_entries=None,
                trigger_filter_enabled=False,
                package_results=None,
                report_dir=None,
                report_timestamp=None,
                support_state_map={},
                unsupported_table_keys=set(),
                view_compat_map={}
            )
            self.assertTrue(sentinel.exists())

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
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
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

    def test_clean_plsql_ending_supports_quoted_end_name(self):
        ddl = 'CREATE OR REPLACE PACKAGE BODY P AS\nBEGIN\nNULL;\nEND "P";\n;\n/\n'
        cleaned = sdr.clean_plsql_ending(ddl)
        self.assertIn('END "P";', cleaned)
        self.assertNotIn('\n;\n/', cleaned)

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
        ddl = "BEGIN FOR i IN 1.E10 LOOP NULL; END LOOP; END;"
        cleaned = sdr.clean_for_loop_single_dot_range(ddl)
        self.assertIn("IN 1.E10", cleaned)

    def test_clean_for_loop_collection_attr_range(self):
        ddl = "BEGIN FOR i IN v_list.FIRST.v_list.LAST LOOP NULL; END LOOP; END;"
        cleaned = sdr.clean_for_loop_collection_attr_range(ddl)
        self.assertIn("v_list.FIRST..v_list.LAST", cleaned)
        ddl = "BEGIN FOR i IN arr.COUNT.10 LOOP NULL; END LOOP; END;"
        cleaned = sdr.clean_for_loop_collection_attr_range(ddl)
        self.assertIn("arr.COUNT..10", cleaned)
        ddl = "BEGIN FOR i IN v_list.FIRST..v_list.LAST LOOP NULL; END LOOP; END;"
        cleaned = sdr.clean_for_loop_collection_attr_range(ddl)
        self.assertIn("v_list.FIRST..v_list.LAST", cleaned)

    def test_clean_extra_dots_preserves_range_operator(self):
        ddl = "BEGIN FOR i IN v_list.FIRST..v_list.LAST LOOP NULL; END LOOP; END;"
        cleaned = sdr.clean_extra_dots(ddl)
        self.assertIn("v_list.FIRST..v_list.LAST", cleaned)

    def test_clean_extra_dots_collapses_three_or_more(self):
        ddl = "SELECT * FROM SCHEMA...TABLE_NAME"
        cleaned = sdr.clean_extra_dots(ddl)
        self.assertIn("SCHEMA.TABLE_NAME", cleaned)
        self.assertNotIn("...", cleaned)

    def test_clean_extra_semicolons_preserves_string_literal(self):
        ddl = "BEGIN\n  v_sql := 'cmd1;;cmd2';\nEND;"
        cleaned = sdr.clean_extra_semicolons(ddl)
        self.assertIn("'cmd1;;cmd2'", cleaned)

    def test_is_index_expression_token_detects_case_keyword(self):
        self.assertTrue(sdr.is_index_expression_token("CASE"))

    def test_compare_version_with_suffix(self):
        self.assertEqual(sdr.compare_version("4.2.5.7-bp1", "4.2.5.7"), 1)
        self.assertEqual(sdr.compare_version("4.2.5.7", "4.2.5.7-bp1"), -1)

    def test_add_custom_cleanup_rule_applies(self):
        rule_name = "ut_replace_abc"
        try:
            sdr.add_custom_cleanup_rule(rule_name, ["PROCEDURE"], lambda d: d.replace("ABC_TOKEN", "XYZ_TOKEN"))
            ddl = "CREATE OR REPLACE PROCEDURE P AS\nBEGIN\n  ABC_TOKEN;\nEND;\n/"
            cleaned = sdr.apply_ddl_cleanup_rules(ddl, "PROCEDURE")
            self.assertIn("XYZ_TOKEN", cleaned)
        finally:
            sdr.DDL_CLEANUP_RULES.pop(f"CUSTOM_{rule_name.upper()}", None)

    def test_apply_ddl_cleanup_rules_keeps_previous_on_rule_exception(self):
        rule_name = "ut_raise_after_replace"
        try:
            sdr.add_custom_cleanup_rule(rule_name, ["PROCEDURE"], lambda d: d.replace("ABC_TOKEN", "XYZ_TOKEN"))
            def _raise_rule(_ddl):
                raise RuntimeError("boom")
            sdr.add_custom_cleanup_rule(rule_name, ["PROCEDURE"], _raise_rule)
            ddl = "CREATE OR REPLACE PROCEDURE P AS\nBEGIN\n  ABC_TOKEN;\nEND;\n/"
            cleaned = sdr.apply_ddl_cleanup_rules(ddl, "PROCEDURE")
            self.assertIn("XYZ_TOKEN", cleaned)
        finally:
            sdr.DDL_CLEANUP_RULES.pop(f"CUSTOM_{rule_name.upper()}", None)

    def test_clean_long_types_in_table_ddl(self):
        ddl = "CREATE TABLE T_LONG (A LONG, B LONG RAW, C VARCHAR2(10));"
        cleaned = sdr.clean_long_types_in_table_ddl(ddl)
        self.assertIn("A CLOB", cleaned)
        self.assertIn("B BLOB", cleaned)
        self.assertNotIn("LONG RAW", cleaned.upper())
        self.assertNotIn(" LONG,", cleaned.upper())

    def test_clean_long_types_in_table_ddl_ignores_comments_and_literals(self):
        ddl = (
            "CREATE TABLE T_LONG (\n"
            "  A LONG,\n"
            "  B VARCHAR2(50) DEFAULT 'LONG RAW',\n"
            "  C VARCHAR2(50) -- LONG RAW should stay in comment\n"
            ");"
        )
        cleaned = sdr.clean_long_types_in_table_ddl(ddl)
        self.assertIn("A CLOB", cleaned)
        self.assertIn("'LONG RAW'", cleaned)
        self.assertIn("-- LONG RAW should stay in comment", cleaned)

    def test_clean_sequence_unsupported_options_ignores_comments_and_literals(self):
        ddl = (
            "CREATE SEQUENCE S1 START WITH 1 INCREMENT BY 1 NOKEEP NOSCALE GLOBAL;\n"
            "-- NOKEEP should stay in comment\n"
            "SELECT 'NOKEEP' FROM DUAL;"
        )
        cleaned = sdr.clean_sequence_unsupported_options(ddl)
        first_line = cleaned.splitlines()[0].upper()
        self.assertNotIn(" NOKEEP ", first_line)
        self.assertNotIn(" NOSCALE ", first_line)
        self.assertNotIn(" GLOBAL ", first_line)
        self.assertIn("-- NOKEEP should stay in comment", cleaned)
        self.assertIn("'NOKEEP'", cleaned)

    def test_apply_ddl_cleanup_rules_plsql_pipeline_integration(self):
        ddl = (
            "CREATE OR REPLACE PACKAGE BODY PKG_DEMO AS\n"
            "BEGIN\n"
            "  FOR i IN v_list.FIRST.v_list.LAST LOOP\n"
            "    NULL;\n"
            "  END LOOP;\n"
            "END DEMO_SCHEMA.PKG_DEMO;\n"
            "/\n"
        )
        cleaned = sdr.apply_ddl_cleanup_rules(ddl, "PACKAGE BODY")
        self.assertIn("v_list.FIRST..v_list.LAST", cleaned)
        self.assertIn("END PKG_DEMO;", cleaned)
        self.assertNotIn("END DEMO_SCHEMA.PKG_DEMO", cleaned)

    def test_apply_ddl_cleanup_rules_table_pipeline_integration(self):
        ddl = (
            "CREATE TABLE T_INTEG (\n"
            "  C1 LONG,\n"
            "  C2 LONG RAW,\n"
            "  C3 VARCHAR2(30) DEFAULT 'LONG',\n"
            "  C4 VARCHAR2(30) -- LONG in comment\n"
            ");"
        )
        cleaned = sdr.apply_ddl_cleanup_rules(ddl, "TABLE")
        self.assertIn("C1 CLOB", cleaned)
        self.assertIn("C2 BLOB", cleaned)
        self.assertIn("'LONG'", cleaned)
        self.assertIn("-- LONG in comment", cleaned)

    def test_apply_ddl_cleanup_rules_with_audit_marks_semantic_rewrite(self):
        ddl = "CREATE TABLE T_LONG (A LONG, B LONG RAW, C BFILE);"
        cleaned, actions = sdr.apply_ddl_cleanup_rules_with_audit(ddl, "TABLE")
        self.assertIn("A CLOB", cleaned)
        self.assertIn("B BLOB", cleaned)
        self.assertIn("C BLOB", cleaned)
        self.assertTrue(actions)
        rewrite_actions = [a for a in actions if a.rule_name == "rewrite_unsupported_table_oracle_types"]
        self.assertEqual(len(rewrite_actions), 1)
        self.assertEqual(rewrite_actions[0].category, sdr.DDL_CLEAN_CATEGORY_SEMANTIC_REWRITE)
        self.assertEqual(rewrite_actions[0].evidence_level, sdr.DDL_CLEAN_EVIDENCE_VERIFIED_UNSUPPORTED)
        self.assertGreaterEqual(rewrite_actions[0].change_count, 3)
        self.assertTrue(any("LONG -> CLOB" in sample for sample in rewrite_actions[0].samples))

    def test_view_cleanup_pipeline_integration(self):
        raw = (
            "CREATE OR REPLACE FORCE EDITIONABLE VIEW V_DEMO AS\n"
            "SELECT 1 AS C FROM DUAL\n"
            "WITH CHECK OPTION CONSTRAINT CK_V_DEMO;"
        )
        view_clean = sdr.clean_view_ddl_for_oceanbase(raw, ob_version="4.2.5.7")
        final = sdr.apply_ddl_cleanup_rules(view_clean, "VIEW")
        self.assertTrue(final.upper().startswith("CREATE OR REPLACE VIEW"))
        self.assertIn("WITH CHECK OPTION", final.upper())

    def test_normalize_synonym_fixup_scope(self):
        self.assertEqual(sdr.normalize_synonym_fixup_scope(None), "public_only")
        self.assertEqual(sdr.normalize_synonym_fixup_scope("all"), "all")
        self.assertEqual(sdr.normalize_synonym_fixup_scope("public"), "public_only")
        self.assertEqual(sdr.normalize_synonym_fixup_scope("PUBLIC_ONLY"), "public_only")

    def test_normalize_synonym_check_scope(self):
        self.assertEqual(sdr.normalize_synonym_check_scope(None), "public_only")
        self.assertEqual(sdr.normalize_synonym_check_scope("all"), "all")
        self.assertEqual(sdr.normalize_synonym_check_scope("public"), "public_only")
        self.assertEqual(sdr.normalize_synonym_check_scope("PUBLIC_ONLY"), "public_only")

    def test_normalize_sequence_remap_policy(self):
        self.assertEqual(sdr.normalize_sequence_remap_policy(None), "source_only")
        self.assertEqual(sdr.normalize_sequence_remap_policy("infer"), "infer")
        self.assertEqual(sdr.normalize_sequence_remap_policy("SOURCE"), "source_only")
        self.assertEqual(sdr.normalize_sequence_remap_policy("dominant"), "dominant_table")

    def test_normalize_fixup_exec_mode(self):
        self.assertEqual(sdr.normalize_fixup_exec_mode(None), "auto")
        self.assertEqual(sdr.normalize_fixup_exec_mode("FILE"), "file")
        self.assertEqual(sdr.normalize_fixup_exec_mode("legacy"), "statement")
        self.assertEqual(sdr.normalize_fixup_exec_mode("bad_mode"), "auto")

    def test_normalize_object_created_missing_policy(self):
        self.assertEqual(sdr.normalize_object_created_missing_policy(None), "strict")
        self.assertEqual(sdr.normalize_object_created_missing_policy("include"), "include_missing")
        self.assertEqual(sdr.normalize_object_created_missing_policy("exclude"), "exclude_missing")
        self.assertEqual(sdr.normalize_object_created_missing_policy("bad_mode"), "strict")

    def test_normalize_config_hot_reload_mode(self):
        self.assertEqual(sdr.normalize_config_hot_reload_mode(None), "off")
        self.assertEqual(sdr.normalize_config_hot_reload_mode("PHASE"), "phase")
        self.assertEqual(sdr.normalize_config_hot_reload_mode("round"), "round")
        self.assertEqual(sdr.normalize_config_hot_reload_mode("bad_mode"), "off")

    def test_normalize_config_hot_reload_fail_policy(self):
        self.assertEqual(sdr.normalize_config_hot_reload_fail_policy(None), "keep_last_good")
        self.assertEqual(sdr.normalize_config_hot_reload_fail_policy("abort"), "abort")
        self.assertEqual(sdr.normalize_config_hot_reload_fail_policy("keep"), "keep_last_good")
        self.assertEqual(sdr.normalize_config_hot_reload_fail_policy("bad"), "keep_last_good")

    def test_load_config_allows_percent_in_password(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.ini"
            path.write_text(
                "\n".join([
                    "[ORACLE_SOURCE]",
                    "user = scott",
                    "password = ab%cd",
                    "dsn = 127.0.0.1:1521/ORCL",
                    "[OCEANBASE_TARGET]",
                    "executable = /bin/obclient",
                    "host = 127.0.0.1",
                    "port = 2881",
                    "user_string = root@sys",
                    "password = p%w",
                    "[SETTINGS]",
                    "source_schemas = A",
                ]) + "\n",
                encoding="utf-8"
            )
            ora_cfg, ob_cfg, _settings = sdr.load_config(str(path))
            self.assertEqual(ora_cfg["password"], "ab%cd")
            self.assertEqual(ob_cfg["password"], "p%w")

    def test_load_config_hot_reload_defaults(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.ini"
            path.write_text(
                "\n".join([
                    "[ORACLE_SOURCE]",
                    "user = scott",
                    "password = tiger",
                    "dsn = 127.0.0.1:1521/ORCL",
                    "[OCEANBASE_TARGET]",
                    "executable = /bin/obclient",
                    "host = 127.0.0.1",
                    "port = 2881",
                    "user_string = root@sys",
                    "password = p",
                    "[SETTINGS]",
                    "source_schemas = A",
                ]) + "\n",
                encoding="utf-8"
            )
            _ora_cfg, _ob_cfg, settings = sdr.load_config(str(path))
            self.assertEqual(settings["config_hot_reload_mode"], "off")
            self.assertEqual(settings["config_hot_reload_interval_sec"], "5")
            self.assertEqual(settings["config_hot_reload_fail_policy"], "keep_last_good")
            self.assertEqual(settings["fixup_exec_mode"], "auto")
            self.assertTrue(settings["fixup_exec_file_fallback"])
            self.assertEqual(settings["plain_not_null_fixup_mode"], "runnable_if_no_nulls")
            self.assertTrue(settings["generate_extra_cleanup"])
            self.assertEqual(settings["extra_constraint_cleanup_mode"], "safe_only")
            self.assertEqual(settings["source_object_scope_mode"], "full_source")
            self.assertEqual(settings["report_retention_days"], 30)
            self.assertEqual(settings["object_created_before_missing_created_policy"], "strict")
            self.assertFalse(settings["fixup_drop_sys_c_columns"])

    def test_probe_target_plain_not_null_columns(self):
        calls = []

        def fake_run(_cfg, sql_query, timeout=None, quiet_error=False):
            calls.append((sql_query, timeout, quiet_error))
            return True, "C1\tN\nPK_SERIAL#\tY", ""

        with mock.patch.object(sdr, "obclient_run_sql", side_effect=fake_run):
            results = sdr.probe_target_plain_not_null_columns(
                {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                "TGT",
                "T1",
                ["C1", "PK_SERIAL#"],
            )
        self.assertEqual(results["C1"], (False, ""))
        self.assertEqual(results["PK_SERIAL#"], (True, ""))
        self.assertTrue(calls)
        self.assertIn('"PK_SERIAL#"', calls[0][0])

    def test_load_config_object_created_before_valid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.ini"
            path.write_text(
                "\n".join([
                    "[ORACLE_SOURCE]",
                    "user = scott",
                    "password = tiger",
                    "dsn = 127.0.0.1:1521/ORCL",
                    "[OCEANBASE_TARGET]",
                    "executable = /bin/obclient",
                    "host = 127.0.0.1",
                    "port = 2881",
                    "user_string = root@sys",
                    "password = p",
                    "[SETTINGS]",
                    "source_schemas = A",
                    "object_created_before = 20260303 150000",
                    "object_created_before_missing_created_policy = include",
                ]) + "\n",
                encoding="utf-8"
            )
            _ora_cfg, _ob_cfg, settings = sdr.load_config(str(path))
            self.assertTrue(settings["object_created_before_enabled"])
            self.assertEqual(settings["object_created_before"], "20260303 150000")
            self.assertEqual(settings["object_created_before_dt"], datetime(2026, 3, 3, 15, 0, 0))
            self.assertEqual(settings["object_created_before_missing_created_policy"], "include_missing")

    def test_load_config_object_created_before_invalid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.ini"
            path.write_text(
                "\n".join([
                    "[ORACLE_SOURCE]",
                    "user = scott",
                    "password = tiger",
                    "dsn = 127.0.0.1:1521/ORCL",
                    "[OCEANBASE_TARGET]",
                    "executable = /bin/obclient",
                    "host = 127.0.0.1",
                    "port = 2881",
                    "user_string = root@sys",
                    "password = p",
                    "[SETTINGS]",
                    "source_schemas = A",
                    "object_created_before = 2026/03/03 15:00:00",
                ]) + "\n",
                encoding="utf-8"
            )
            with self.assertRaises(sdr.FatalError):
                sdr.load_config(str(path))

    def test_load_config_rejects_invalid_report_db_schema(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.ini"
            path.write_text(
                "\n".join([
                    "[ORACLE_SOURCE]",
                    "user = scott",
                    "password = tiger",
                    "dsn = 127.0.0.1:1521/ORCL",
                    "[OCEANBASE_TARGET]",
                    "executable = /bin/obclient",
                    "host = 127.0.0.1",
                    "port = 2881",
                    "user_string = root@sys",
                    "password = p",
                    "[SETTINGS]",
                    "source_schemas = A",
                    "report_db_schema = DIFF.REPORT",
                ]) + "\n",
                encoding="utf-8"
            )
            with self.assertRaises(sdr.FatalError):
                sdr.load_config(str(path))

    def test_load_config_rejects_grant_role_alias_collision(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.ini"
            path.write_text(
                "\n".join([
                    "[ORACLE_SOURCE]",
                    "user = scott",
                    "password = tiger",
                    "dsn = 127.0.0.1:1521/ORCL",
                    "[OCEANBASE_TARGET]",
                    "executable = /bin/obclient",
                    "host = 127.0.0.1",
                    "port = 2881",
                    "user_string = root@sys",
                    "password = p",
                    "[SETTINGS]",
                    "source_schemas = A",
                ]) + "\n",
                encoding="utf-8"
            )
            bad_alias_map = {
                "ROLE_A": "OB_ROLE_X",
                "ROLE_B": "OB_ROLE_X",
            }
            with mock.patch.dict(sdr.GRANT_ROLE_ALIAS_MAP, bad_alias_map, clear=True):
                with self.assertRaises(sdr.FatalError):
                    sdr.load_config(str(path))

    def test_build_validated_grant_role_alias_reverse_map_rejects_source_collision(self):
        with self.assertRaises(ValueError):
            sdr.build_validated_grant_role_alias_reverse_map(
                {
                    "role_a": "OB_ROLE_A",
                    "ROLE_A": "OB_ROLE_B",
                }
            )

    def test_build_validated_grant_role_alias_reverse_map_accepts_one_to_one(self):
        reverse_map = sdr.build_validated_grant_role_alias_reverse_map(
            {
                "SELECT_CATALOG_ROLE": "OB_CATALOG_ROLE",
                "RESOURCE": "OB_RESOURCE",
            }
        )
        self.assertEqual(
            reverse_map,
            {
                "OB_CATALOG_ROLE": "SELECT_CATALOG_ROLE",
                "OB_RESOURCE": "RESOURCE",
            }
        )

    def test_normalize_report_db_schema(self):
        self.assertEqual(sdr.normalize_report_db_schema(""), "")
        self.assertEqual(sdr.normalize_report_db_schema("diff_report"), "DIFF_REPORT")
        with self.assertRaises(ValueError):
            sdr.normalize_report_db_schema("DIFF.REPORT")

    def test_apply_config_hot_reload_at_phase_applies_runtime_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "config.ini"
            cfg_path.write_text("A", encoding="utf-8")
            runtime = sdr.ConfigHotReloadRuntime(
                config_path=cfg_path,
                mode="phase",
                interval_sec=1,
                fail_policy="keep_last_good",
                watch_paths=[cfg_path],
                snapshot=sdr.build_hot_reload_snapshot([cfg_path]),
            )
            settings = {
                "log_level": "AUTO",
                "config_hot_reload_mode": "phase",
                "config_hot_reload_interval_sec": "1",
                "config_hot_reload_fail_policy": "keep_last_good",
                "source_schemas": "A",
                "remap_file": "",
                "fixup_dir": "fixup_scripts",
                "report_dir": "main_reports",
                "check_primary_types": "",
                "check_extra_types": "",
                "check_dependencies": "true",
                "generate_fixup": "true",
                "generate_grants": "true",
            }
            cfg_path.write_text("B", encoding="utf-8")
            candidate_settings = dict(settings)
            candidate_settings.update({
                "log_level": "DEBUG",
                "config_hot_reload_interval_sec": "8",
                "config_hot_reload_fail_policy": "abort",
            })
            with mock.patch.object(sdr, "load_config", return_value=({}, {}, candidate_settings)), \
                 mock.patch.object(sdr, "set_console_log_level") as m_set_level:
                sdr.apply_config_hot_reload_at_phase(runtime, "对象映射准备", settings)
            self.assertTrue(m_set_level.called)
            self.assertEqual(settings["log_level"], "DEBUG")
            self.assertEqual(settings["config_hot_reload_interval_sec"], "8")
            self.assertEqual(settings["config_hot_reload_fail_policy"], "abort")
            self.assertTrue(runtime.events)
            self.assertEqual(runtime.events[-1]["status"], "APPLIED")

    def test_apply_config_hot_reload_at_phase_abort_on_invalid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "config.ini"
            cfg_path.write_text("A", encoding="utf-8")
            runtime = sdr.ConfigHotReloadRuntime(
                config_path=cfg_path,
                mode="phase",
                interval_sec=1,
                fail_policy="abort",
                watch_paths=[cfg_path],
                snapshot=sdr.build_hot_reload_snapshot([cfg_path]),
            )
            settings = {
                "log_level": "AUTO",
                "config_hot_reload_mode": "phase",
                "config_hot_reload_interval_sec": "1",
                "config_hot_reload_fail_policy": "abort",
            }
            cfg_path.write_text("B", encoding="utf-8")
            with mock.patch.object(sdr, "load_config", side_effect=Exception("invalid")):
                with self.assertRaises(sdr.FatalError):
                    sdr.apply_config_hot_reload_at_phase(runtime, "对象映射准备", settings)

    def test_apply_config_hot_reload_restores_obc_timeout_side_effect(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "config.ini"
            cfg_path.write_text("A", encoding="utf-8")
            runtime = sdr.ConfigHotReloadRuntime(
                config_path=cfg_path,
                mode="phase",
                interval_sec=1,
                fail_policy="keep_last_good",
                watch_paths=[cfg_path],
                snapshot=sdr.build_hot_reload_snapshot([cfg_path]),
            )
            settings = {
                "log_level": "AUTO",
                "config_hot_reload_mode": "phase",
                "config_hot_reload_interval_sec": "1",
                "config_hot_reload_fail_policy": "keep_last_good",
            }
            cfg_path.write_text("B", encoding="utf-8")
            old_timeout = sdr.OBC_TIMEOUT

            def _fake_load(_path):
                sdr.OBC_TIMEOUT = 999
                return {}, {}, dict(settings)

            with mock.patch.object(sdr, "load_config", side_effect=_fake_load):
                sdr.apply_config_hot_reload_at_phase(runtime, "对象映射准备", settings)

            self.assertEqual(sdr.OBC_TIMEOUT, old_timeout)

    def test_obclient_query_by_owner_chunks_splits(self):
        calls = []
        def fake_run(_cfg, sql):
            calls.append(sql)
            return True, sql, ""
        orig = sdr.obclient_run_sql
        sdr.obclient_run_sql = fake_run
        try:
            ok, lines, err = sdr.obclient_query_by_owner_chunks(
                {},
                "SELECT * FROM T WHERE OWNER IN ({owners_in})",
                ["A", "B", "C"],
                chunk_size=2
            )
            self.assertTrue(ok)
            self.assertEqual(err, "")
            self.assertEqual(len(lines), 2)
            self.assertIn("OWNER IN ('A','B')", lines[0])
            self.assertIn("OWNER IN ('C')", lines[1])
        finally:
            sdr.obclient_run_sql = orig

    def test_obclient_query_by_owner_chunks_escapes_quote(self):
        calls = []
        def fake_run(_cfg, sql):
            calls.append(sql)
            return True, sql, ""
        orig = sdr.obclient_run_sql
        sdr.obclient_run_sql = fake_run
        try:
            ok, _lines, err = sdr.obclient_query_by_owner_chunks(
                {},
                "SELECT * FROM T WHERE OWNER IN ({owners_in})",
                ["A", "B'X"],
                chunk_size=10
            )
            self.assertTrue(ok)
            self.assertEqual(err, "")
            self.assertEqual(len(calls), 1)
            self.assertIn("OWNER IN ('A','B''X')", calls[0])
        finally:
            sdr.obclient_run_sql = orig

    def test_obclient_query_by_owner_pairs_splits(self):
        calls = []
        def fake_run(_cfg, sql):
            calls.append(sql)
            return True, sql, ""
        orig = sdr.obclient_run_sql
        sdr.obclient_run_sql = fake_run
        try:
            ok, lines, err = sdr.obclient_query_by_owner_pairs(
                {},
                "SELECT * FROM D WHERE OWNER IN ({owners_in}) AND REFERENCED_OWNER IN ({ref_owners_in})",
                ["A", "B", "C"],
                ["X", "Y"],
                chunk_size=2
            )
            self.assertTrue(ok)
            self.assertEqual(err, "")
            self.assertEqual(len(lines), 2)
            self.assertIn("OWNER IN ('A','B')", lines[0])
            self.assertIn("REFERENCED_OWNER IN ('X','Y')", lines[0])
            self.assertIn("OWNER IN ('C')", lines[1])
            self.assertIn("REFERENCED_OWNER IN ('X','Y')", lines[1])
        finally:
            sdr.obclient_run_sql = orig

    def test_obclient_query_by_owner_pairs_escapes_quote(self):
        calls = []
        def fake_run(_cfg, sql):
            calls.append(sql)
            return True, sql, ""
        orig = sdr.obclient_run_sql
        sdr.obclient_run_sql = fake_run
        try:
            ok, _lines, err = sdr.obclient_query_by_owner_pairs(
                {},
                "SELECT * FROM D WHERE OWNER IN ({owners_in}) AND REFERENCED_OWNER IN ({ref_owners_in})",
                ["A'1"],
                ["B'2"],
                chunk_size=10
            )
            self.assertTrue(ok)
            self.assertEqual(err, "")
            self.assertEqual(len(calls), 1)
            self.assertIn("OWNER IN ('A''1')", calls[0])
            self.assertIn("REFERENCED_OWNER IN ('B''2')", calls[0])
        finally:
            sdr.obclient_run_sql = orig

    def test_obclient_run_sql_hides_password_from_args(self):
        class Dummy:
            def __init__(self):
                self.returncode = 0
                self.stdout = "OK"
                self.stderr = ""

        captured = {}

        def fake_run(*args, **kwargs):
            captured["cmd"] = list(args[0]) if args else []
            return Dummy()

        orig_run = sdr.subprocess.run
        try:
            sdr.subprocess.run = fake_run
            ok, out, err = sdr.obclient_run_sql(
                {
                    "executable": "obclient",
                    "host": "127.0.0.1",
                    "port": "2881",
                    "user_string": "root@sys",
                    "password": "PAssw0rd01##"
                },
                "select 1 from dual"
            )
            self.assertTrue(ok)
            self.assertEqual(out, "OK")
            self.assertEqual(err, "")
            cmd = captured.get("cmd", [])
            self.assertTrue(any(part.startswith(f"{sdr.OBCLIENT_SECURE_OPT}=") for part in cmd))
            self.assertNotIn("PAssw0rd01##", " ".join(cmd))
        finally:
            sdr.subprocess.run = orig_run
            sdr._cleanup_obclient_secure_files()

    def test_obclient_run_sql_warning_does_not_fail(self):
        class Dummy:
            def __init__(self):
                self.returncode = 0
                self.stdout = "OK"
                self.stderr = "Warning: ignored"
        def fake_run(*_args, **_kwargs):
            return Dummy()
        orig_run = sdr.subprocess.run
        try:
            sdr.subprocess.run = fake_run
            ok, out, err = sdr.obclient_run_sql(
                {
                    "executable": "obclient",
                    "host": "127.0.0.1",
                    "port": "2881",
                    "user_string": "root@sys",
                    "password": "p"
                },
                "select 1 from dual"
            )
            self.assertTrue(ok)
            self.assertEqual(out, "OK")
            self.assertEqual(err, "")
        finally:
            sdr.subprocess.run = orig_run

    def test_obclient_run_sql_nonzero_extracts_stdout_error_line(self):
        class Dummy:
            def __init__(self):
                self.returncode = 1
                self.stdout = "ORA-00942: table or view does not exist\n"
                self.stderr = ""
        def fake_run(*_args, **_kwargs):
            return Dummy()
        orig_run = sdr.subprocess.run
        try:
            sdr.subprocess.run = fake_run
            ok, out, err = sdr.obclient_run_sql(
                {
                    "executable": "obclient",
                    "host": "127.0.0.1",
                    "port": "2881",
                    "user_string": "root@sys",
                    "password": "p"
                },
                "select 1 from dual"
            )
            self.assertFalse(ok)
            self.assertIn("ORA-00942", err)
            self.assertIn("ORA-00942", out)
        finally:
            sdr.subprocess.run = orig_run

    def test_classify_usability_status(self):
        self.assertEqual(
            sdr.classify_usability_status(True, True, True, False),
            sdr.USABILITY_STATUS_OK
        )
        self.assertEqual(
            sdr.classify_usability_status(True, True, False, False),
            sdr.USABILITY_STATUS_UNUSABLE
        )
        self.assertEqual(
            sdr.classify_usability_status(True, False, False, False),
            sdr.USABILITY_STATUS_EXPECTED_UNUSABLE
        )
        self.assertEqual(
            sdr.classify_usability_status(True, False, True, False),
            sdr.USABILITY_STATUS_UNEXPECTED_USABLE
        )
        self.assertEqual(
            sdr.classify_usability_status(True, True, True, True),
            sdr.USABILITY_STATUS_TIMEOUT
        )

    def test_analyze_usability_error_mapping(self):
        root, rec = sdr.analyze_usability_error("ORA-00942: table or view does not exist")
        self.assertIn("依赖对象不存在", root)
        root, rec = sdr.analyze_usability_error("ORA-01031: insufficient privileges")
        self.assertIn("权限不足", root)

    def test_compute_usability_sample(self):
        sample_cnt, skipped, sampled = sdr._compute_usability_sample(2000, 1000, 0.1)
        self.assertTrue(sampled)
        self.assertEqual(sample_cnt, 200)
        self.assertEqual(skipped, 1800)
        sample_cnt, skipped, sampled = sdr._compute_usability_sample(2000, 0, 0.1)
        self.assertFalse(sampled)
        self.assertEqual(sample_cnt, 2000)

    def test_build_usability_query_public_synonym(self):
        sql_public = sdr._build_usability_query("PUBLIC.SYN_TEST", "SYNONYM")
        self.assertEqual(sql_public, 'SELECT * FROM "SYN_TEST" WHERE 1=2')
        sql_private = sdr._build_usability_query("OMS_USER.SYN_TEST", "SYNONYM")
        self.assertEqual(sql_private, 'SELECT * FROM "OMS_USER"."SYN_TEST" WHERE 1=2')
        sql_view = sdr._build_usability_query("OMS_USER.V1", "VIEW")
        self.assertEqual(sql_view, 'SELECT * FROM "OMS_USER"."V1" WHERE 1=2')

    def test_check_object_usability_oracle_cursor_context_manager(self):
        class DummyCursor:
            def __init__(self):
                self.call_timeout = None
                self.enter_called = False
                self.exit_called = False

            def __enter__(self):
                self.enter_called = True
                return self

            def __exit__(self, exc_type, exc, tb):
                self.exit_called = True
                return False

            def execute(self, _sql):
                raise RuntimeError("boom")

        class DummyConn:
            def __init__(self, cursor_obj):
                self._cursor = cursor_obj
                self.closed = False

            def cursor(self):
                return self._cursor

            def close(self):
                self.closed = True

        cursor = DummyCursor()
        conn = DummyConn(cursor)
        settings = {
            "check_object_usability": "true",
            "check_source_usability": "true",
            "usability_check_timeout": "5",
            "usability_check_workers": "1",
            "max_usability_objects": "0",
            "usability_sample_ratio": "0",
        }
        master_list = [("SRC.V1", "TGT.V1", "VIEW")]
        tv_results = {"missing": []}
        ob_meta = sdr.ObMetadata(
            objects_by_type={"VIEW": {"TGT.V1"}, "SYNONYM": set()},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            constraint_deferrable_supported=False
        )
        ora_cfg = {"user": "u", "password": "p", "dsn": "d"}
        ob_cfg = {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"}

        with mock.patch.object(sdr.oracledb, "connect", return_value=conn, create=True), \
             mock.patch.object(sdr, "obclient_run_sql", return_value=(True, "", "")):
            summary = sdr.check_object_usability(
                settings,
                master_list,
                tv_results,
                ob_cfg,
                ora_cfg,
                ob_meta,
                enabled_primary_types={"VIEW"},
            )
        self.assertIsNotNone(summary)
        self.assertTrue(cursor.enter_called)
        self.assertTrue(cursor.exit_called)
        self.assertTrue(conn.closed)

    def test_pick_dependency_issue_missing_target(self):
        deps = [("SRC.T1", "TABLE")]
        full_mapping = {"SRC.T1": {"TABLE": "TGT.T1"}}
        missing_targets = {("TABLE", "TGT.T1")}
        issue = sdr._pick_dependency_issue(deps, full_mapping, {}, missing_targets)
        self.assertIsNotNone(issue)
        self.assertIn("依赖对象缺失", issue[0])

    def test_dump_ob_metadata_infers_char_used_from_lengths(self):
        def fake_run(_cfg, sql):
            sql_u = " ".join(str(sql).upper().split())
            if "NLS_LENGTH_SEMANTICS" in sql_u:
                return True, "BYTE", ""
            if "FROM DBA_TAB_COLUMNS" in sql_u and "OWNER='SYS'" in sql_u:
                return True, "0", ""
            if "FROM DBA_TAB_COLUMNS" in sql_u and "OWNER IN ('A')" in sql_u:
                return True, "A\tT1\tC1\tVARCHAR2\t10\t20\t\t\t\tY\t", ""
            if "FROM DBA_TAB_COLS" in sql_u and "OWNER IN ('A')" in sql_u:
                return True, "", ""
            return True, "", ""

        def fake_query(_cfg, sql_tpl, _owners, **_kwargs):
            sql = sql_tpl.upper()
            if "DBA_OBJECTS" in sql:
                return True, [], ""
            return True, [], ""

        orig_run = sdr.obclient_run_sql
        orig_query = sdr.obclient_query_by_owner_chunks
        try:
            sdr.obclient_run_sql = fake_run
            sdr.obclient_query_by_owner_chunks = fake_query
            ob_meta = sdr.dump_ob_metadata(
                {"executable": "/bin/obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                {"A"},
                tracked_object_types={"TABLE"},
                include_tab_columns=True,
                include_column_order=False,
                include_indexes=False,
                include_constraints=False,
                include_triggers=False,
                include_sequences=False,
                include_comments=False,
                include_roles=False,
            )
            self.assertEqual(ob_meta.tab_columns[("A", "T1")]["C1"]["char_used"], "C")
        finally:
            sdr.obclient_run_sql = orig_run
            sdr.obclient_query_by_owner_chunks = orig_query

    def test_dump_ob_metadata_defaults_char_used_to_nls(self):
        def fake_run(_cfg, sql):
            sql_u = " ".join(str(sql).upper().split())
            if "NLS_LENGTH_SEMANTICS" in sql_u:
                return True, "BYTE", ""
            if "FROM DBA_TAB_COLUMNS" in sql_u and "OWNER='SYS'" in sql_u:
                if "COLUMN_NAME='HIDDEN_COLUMN'" in sql_u or "COLUMN_NAME='VIRTUAL_COLUMN'" in sql_u:
                    return True, "1", ""
                return True, "0", ""
            if "FROM DBA_TAB_COLUMNS" in sql_u and "OWNER IN ('A')" in sql_u:
                if "CHAR_USED" in sql_u:
                    return False, "", "no column"
                if "DATA_LENGTH" in sql_u and "DATA_PRECISION" in sql_u:
                    return False, "", "no column"
                return True, "A\tT1\tC1\tVARCHAR2\t10\tY\t", ""
            if "FROM DBA_TAB_COLS" in sql_u and "OWNER IN ('A')" in sql_u:
                return False, "", "no column"
            return True, "", ""

        def fake_query(_cfg, sql_tpl, _owners, **_kwargs):
            sql = sql_tpl.upper()
            if "DBA_OBJECTS" in sql:
                return True, [], ""
            return True, [], ""

        orig_run = sdr.obclient_run_sql
        orig_query = sdr.obclient_query_by_owner_chunks
        try:
            sdr.obclient_run_sql = fake_run
            sdr.obclient_query_by_owner_chunks = fake_query
            ob_meta = sdr.dump_ob_metadata(
                {"executable": "/bin/obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                {"A"},
                tracked_object_types={"TABLE"},
                include_tab_columns=True,
                include_column_order=False,
                include_indexes=False,
                include_constraints=False,
                include_triggers=False,
                include_sequences=False,
                include_comments=False,
                include_roles=False,
            )
            self.assertEqual(ob_meta.tab_columns[("A", "T1")]["C1"]["char_used"], "B")
        finally:
            sdr.obclient_run_sql = orig_run
            sdr.obclient_query_by_owner_chunks = orig_query

    def test_dump_ob_metadata_keeps_disabled_constraints(self):
        def fake_run(_cfg, sql):
            sql_u = " ".join(str(sql).upper().split())
            if "FROM DBA_CONSTRAINTS" in sql_u and "OWNER IN ('A')" in sql_u:
                lines = [
                    "A\tT1\tFK1\tR\tDISABLED\tA\tPK_RT1\tNO ACTION",
                    "A\tRT1\tPK_RT1\tP\tENABLED\t\t\t",
                ]
                return True, "\n".join(lines), ""
            return True, "", ""

        def fake_query(_cfg, sql_tpl, _owners, **_kwargs):
            sql = sql_tpl.upper()
            if "DBA_OBJECTS" in sql:
                return True, [], ""
            if "DBA_CONS_COLUMNS" in sql:
                lines = [
                    "A\tT1\tFK1\tC1\t1",
                    "A\tRT1\tPK_RT1\tID\t1",
                ]
                return True, lines, ""
            return True, [], ""

        orig_run = sdr.obclient_run_sql
        orig_query = sdr.obclient_query_by_owner_chunks
        orig_has_col = sdr.ob_has_dba_column
        try:
            sdr.obclient_run_sql = fake_run
            sdr.obclient_query_by_owner_chunks = fake_query
            sdr.ob_has_dba_column = lambda *_a, **_k: False
            ob_meta = sdr.dump_ob_metadata(
                {"executable": "/bin/obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                {"A"},
                tracked_object_types={"TABLE"},
                include_tab_columns=False,
                include_column_order=False,
                include_indexes=False,
                include_constraints=True,
                include_triggers=False,
                include_sequences=False,
                include_comments=False,
                include_roles=False,
            )
            fk = ob_meta.constraints[("A", "T1")]["FK1"]
            self.assertEqual(fk["type"], "R")
            self.assertEqual(fk.get("status"), "DISABLED")
            self.assertEqual(fk["columns"], ["C1"])
            self.assertEqual(fk.get("ref_table_owner"), "A")
            self.assertEqual(fk.get("ref_table_name"), "RT1")
        finally:
            sdr.obclient_run_sql = orig_run
            sdr.obclient_query_by_owner_chunks = orig_query
            sdr.ob_has_dba_column = orig_has_col

    def test_dump_ob_metadata_backfills_check_search_condition_after_chunk_degrade(self):
        def fake_run(_cfg, sql):
            sql_u = " ".join(str(sql).upper().split())
            if "FROM DBA_CONSTRAINTS" in sql_u and "SEARCH_CONDITION" in sql_u and "OWNER IN ('A','B')" in sql_u:
                return False, "", "forced chunk degrade"
            if "FROM DBA_CONSTRAINTS" in sql_u and "SEARCH_CONDITION" in sql_u and "OWNER IN ('A')" in sql_u:
                return True, 'A\tT1\tCHK_A\tC\tENABLED\tNOT VALIDATED\t\t\t\t"C1" IS NOT NULL', ""
            if "FROM DBA_CONSTRAINTS" in sql_u and "SEARCH_CONDITION" in sql_u and "OWNER IN ('B')" in sql_u:
                return False, "", "forced single-owner degrade"
            if "FROM DBA_CONSTRAINTS" in sql_u and "OWNER IN ('B')" in sql_u and "SEARCH_CONDITION" not in sql_u:
                return True, "B\tT1\tCHK_B\tC\tENABLED\tNOT VALIDATED\t\t\t", ""
            if "FROM DBA_CONSTRAINTS" in sql_u and "OWNER='B'" in sql_u and "TABLE_NAME='T1'" in sql_u and "SEARCH_CONDITION" in sql_u:
                return True, 'B\tT1\tCHK_B\t"PK_SERIAL#" IS NOT NULL', ""
            return True, "", ""

        def fake_query(_cfg, sql_tpl, _owners, **_kwargs):
            sql = sql_tpl.upper()
            if "FROM DBA_OBJECTS" in sql:
                return True, [], ""
            if "FROM DBA_CONS_COLUMNS" in sql:
                return True, [
                    "A\tT1\tCHK_A\tC1\t1",
                    "B\tT1\tCHK_B\tPK_SERIAL#\t1",
                ], ""
            if "FROM DBA_TABLES" in sql and "TEMPORARY = 'Y'" in sql:
                return True, [], ""
            return True, [], ""

        orig_run = sdr.obclient_run_sql
        orig_query = sdr.obclient_query_by_owner_chunks
        orig_has_col = sdr.ob_has_dba_column
        try:
            sdr.obclient_run_sql = fake_run
            sdr.obclient_query_by_owner_chunks = fake_query
            sdr.ob_has_dba_column = lambda _cfg, table_name, column_name, owner="SYS": (
                table_name == "DBA_CONSTRAINTS" and column_name in {"SEARCH_CONDITION", "VALIDATED"}
            )
            ob_meta = sdr.dump_ob_metadata(
                {"executable": "/bin/obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                {"A", "B"},
                tracked_object_types={"TABLE"},
                include_tab_columns=False,
                include_column_order=False,
                include_indexes=False,
                include_constraints=True,
                include_triggers=False,
                include_sequences=False,
                include_comments=False,
                include_roles=False,
            )
            self.assertEqual(
                ob_meta.constraints[("A", "T1")]["CHK_A"]["search_condition"],
                '"C1" IS NOT NULL'
            )
            self.assertEqual(
                ob_meta.constraints[("B", "T1")]["CHK_B"]["search_condition"],
                '"PK_SERIAL#" IS NOT NULL'
            )
        finally:
            sdr.obclient_run_sql = orig_run
            sdr.obclient_query_by_owner_chunks = orig_query
            sdr.ob_has_dba_column = orig_has_col

    def test_dump_ob_metadata_preserves_obcheck_semantics_for_table_compare(self):
        def fake_run(_cfg, sql):
            sql_u = " ".join(str(sql).upper().split())
            if "FROM DBA_CONSTRAINTS" in sql_u and "OWNER IN ('A')" in sql_u and "SEARCH_CONDITION" in sql_u:
                return True, 'A\tT1\tT1_OBCHECK_1\tC\tENABLED\tNOT VALIDATED\t\t\t\t("C1" is not null)', ""
            return True, "", ""

        def fake_query(_cfg, sql_tpl, _owners, **_kwargs):
            sql = sql_tpl.upper()
            if "FROM DBA_OBJECTS" in sql:
                return True, [], ""
            if "FROM DBA_CONS_COLUMNS" in sql:
                return True, ["A\tT1\tT1_OBCHECK_1\tC1\t1"], ""
            if "FROM DBA_TABLES" in sql and "TEMPORARY = 'Y'" in sql:
                return True, [], ""
            return True, [], ""

        orig_run = sdr.obclient_run_sql
        orig_query = sdr.obclient_query_by_owner_chunks
        orig_has_col = sdr.ob_has_dba_column
        try:
            sdr.obclient_run_sql = fake_run
            sdr.obclient_query_by_owner_chunks = fake_query
            sdr.ob_has_dba_column = lambda _cfg, table_name, column_name, owner="SYS": (
                table_name == "DBA_CONSTRAINTS" and column_name in {"SEARCH_CONDITION", "VALIDATED"}
            )
            ob_meta = sdr.dump_ob_metadata(
                {"executable": "/bin/obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                {"A"},
                tracked_object_types={"TABLE"},
                include_tab_columns=False,
                include_column_order=False,
                include_indexes=False,
                include_constraints=True,
                include_triggers=False,
                include_sequences=False,
                include_comments=False,
                include_roles=False,
            )
            self.assertNotIn("T1_OBCHECK_1", ob_meta.constraints.get(("A", "T1"), {}))
            self.assertIn("C1", ob_meta.enabled_notnull_check_columns.get(("A", "T1"), {}))
            self.assertEqual(
                ob_meta.enabled_notnull_check_columns[("A", "T1")]["C1"]["constraint_name"],
                "T1_OBCHECK_1"
            )
        finally:
            sdr.obclient_run_sql = orig_run
            sdr.obclient_query_by_owner_chunks = orig_query
            sdr.ob_has_dba_column = orig_has_col

    def test_dump_ob_metadata_backfills_degraded_fk_reference_metadata(self):
        def fake_run(_cfg, sql):
            sql_u = " ".join(str(sql).upper().split())
            if (
                "FROM DBA_CONSTRAINTS" in sql_u
                and "OWNER='A'" in sql_u
                and "TABLE_NAME='CHILD'" in sql_u
                and "CONSTRAINT_TYPE='R'" in sql_u
            ):
                return True, "A\tCHILD\tFK_CHILD_PARENT\tA\tPK_PARENT", ""
            return True, "", ""

        def fake_best_effort(_cfg, _mode_sqls, _owners, **_kwargs):
            return True, [
                ("basic", "A\tPARENT\tPK_PARENT\tP\tENABLED"),
                ("basic", "A\tCHILD\tFK_CHILD_PARENT\tR\tENABLED"),
            ], ""

        def fake_query(_cfg, sql_tpl, _owners, **_kwargs):
            sql = sql_tpl.upper()
            if "FROM DBA_OBJECTS" in sql:
                return True, [], ""
            if "FROM DBA_CONS_COLUMNS" in sql:
                return True, [
                    "A\tPARENT\tPK_PARENT\tID\t1",
                    "A\tCHILD\tFK_CHILD_PARENT\tPARENT_ID\t1",
                ], ""
            if "FROM DBA_TABLES" in sql and "TEMPORARY = 'Y'" in sql:
                return True, [], ""
            return True, [], ""

        orig_run = sdr.obclient_run_sql
        orig_best_effort = sdr.obclient_query_by_owner_chunks_best_effort
        orig_query = sdr.obclient_query_by_owner_chunks
        orig_has_col = sdr.ob_has_dba_column
        try:
            sdr.obclient_run_sql = fake_run
            sdr.obclient_query_by_owner_chunks_best_effort = fake_best_effort
            sdr.obclient_query_by_owner_chunks = fake_query
            sdr.ob_has_dba_column = lambda *_args, **_kwargs: False
            ob_meta = sdr.dump_ob_metadata(
                {"executable": "/bin/obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                {"A"},
                tracked_object_types={"TABLE"},
                include_tab_columns=False,
                include_column_order=False,
                include_indexes=False,
                include_constraints=True,
                include_triggers=False,
                include_sequences=False,
                include_comments=False,
                include_roles=False,
            )
            fk_meta = ob_meta.constraints[("A", "CHILD")]["FK_CHILD_PARENT"]
            self.assertEqual(fk_meta.get("r_owner"), "A")
            self.assertEqual(fk_meta.get("r_constraint"), "PK_PARENT")
            self.assertEqual(fk_meta.get("ref_table_owner"), "A")
            self.assertEqual(fk_meta.get("ref_table_name"), "PARENT")
            self.assertTrue(fk_meta.get("ref_metadata_complete"))
        finally:
            sdr.obclient_run_sql = orig_run
            sdr.obclient_query_by_owner_chunks_best_effort = orig_best_effort
            sdr.obclient_query_by_owner_chunks = orig_query
            sdr.ob_has_dba_column = orig_has_col

    def test_obclient_query_by_owner_chunks_best_effort_preserves_partial_success(self):
        def fake_run(_cfg, sql, **_kwargs):
            sql_u = " ".join(str(sql).upper().split())
            if "OWNER IN ('A','B')" in sql_u:
                return False, "", "chunk_failed"
            if "OWNER IN ('A')" in sql_u:
                return True, "A\tROW1", ""
            if "OWNER IN ('B')" in sql_u:
                return False, "", "owner_b_failed"
            return False, "", "unexpected_sql"

        orig_run = sdr.obclient_run_sql
        try:
            sdr.obclient_run_sql = fake_run
            ok, lines, err = sdr.obclient_query_by_owner_chunks_best_effort(
                {"executable": "/bin/obclient"},
                [("basic", "SELECT * FROM T WHERE OWNER IN ({owners_in})")],
                ["A", "B"],
                chunk_size=2,
            )
        finally:
            sdr.obclient_run_sql = orig_run

        self.assertTrue(ok)
        self.assertEqual(lines, [("basic", "A\tROW1")])
        self.assertIn("OWNERS=B", err.upper())

    def test_dump_ob_metadata_merges_dba_tab_columns_and_tab_cols(self):
        def fake_run(_cfg, sql, **_kwargs):
            sql_u = " ".join(str(sql).upper().split())
            if "FROM DBA_TAB_COLUMNS" in sql_u and "OWNER='SYS'" in sql_u:
                view_name = "DBA_TAB_COLS" if "TABLE_NAME='DBA_TAB_COLS'" in sql_u else "DBA_TAB_COLUMNS"
                if view_name == "DBA_TAB_COLUMNS":
                    if "COLUMN_NAME='COLUMN_ID'" in sql_u or "COLUMN_NAME='IDENTITY_COLUMN'" in sql_u or "COLUMN_NAME='DEFAULT_ON_NULL'" in sql_u or "COLUMN_NAME='INVISIBLE_COLUMN'" in sql_u:
                        return True, "0", ""
                    return True, "1", ""
                if "COLUMN_NAME='HIDDEN_COLUMN'" in sql_u or "COLUMN_NAME='VIRTUAL_COLUMN'" in sql_u:
                    return True, "0", ""
                if "COLUMN_NAME='COLUMN_ID'" in sql_u or "COLUMN_NAME='IDENTITY_COLUMN'" in sql_u or "COLUMN_NAME='DEFAULT_ON_NULL'" in sql_u or "COLUMN_NAME='INVISIBLE_COLUMN'" in sql_u:
                    return True, "1", ""
                return True, "0", ""
            if "FROM DBA_TAB_COLUMNS" in sql_u and "OWNER IN ('A')" in sql_u:
                return True, "A\tT1\tC1\tVARCHAR2\t10\t10\t\t\tB\tY\t", ""
            if "FROM DBA_TAB_COLS" in sql_u and "OWNER IN ('A')" in sql_u:
                return True, "A\tT1\tC1\tVARCHAR2\t10\t10\t\t\tB\tY\t\t3\tYES\tNO\tYES", ""
            if "FROM DBA_TABLES" in sql_u and "TEMPORARY = 'Y'" in sql_u:
                return True, "", ""
            return True, "", ""

        def fake_query(_cfg, sql_tpl, _owners, **_kwargs):
            sql = sql_tpl.upper()
            if "FROM DBA_OBJECTS" in sql:
                return True, [], ""
            return True, [], ""

        orig_run = sdr.obclient_run_sql
        orig_query = sdr.obclient_query_by_owner_chunks
        try:
            sdr.obclient_run_sql = fake_run
            sdr.obclient_query_by_owner_chunks = fake_query
            ob_meta = sdr.dump_ob_metadata(
                {"executable": "/bin/obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                {"A"},
                tracked_object_types={"TABLE"},
                include_tab_columns=True,
                include_column_order=True,
                include_indexes=False,
                include_constraints=False,
                include_triggers=False,
                include_sequences=False,
                include_comments=False,
                include_roles=False,
            )
            col = ob_meta.tab_columns[("A", "T1")]["C1"]
            self.assertEqual(col["data_type"], "VARCHAR2")
            self.assertEqual(col["column_id"], 3)
            self.assertIs(col["identity"], True)
            self.assertIs(col["default_on_null"], False)
            self.assertIs(col["invisible"], True)
        finally:
            sdr.obclient_run_sql = orig_run
            sdr.obclient_query_by_owner_chunks = orig_query

    def test_dump_ob_metadata_filters_private_synonyms_when_public_only(self):
        def fake_run(_cfg, _sql):
            return True, "", ""

        def fake_query(_cfg, sql_tpl, _owners, **_kwargs):
            sql = sql_tpl.upper()
            if "DBA_OBJECTS" in sql:
                lines = [
                    "A\tS_PRIV\tSYNONYM\tVALID",
                    "__PUBLIC\tS_PUB\tSYNONYM\tVALID",
                    "A\tT1\tTABLE\tVALID",
                ]
                return True, lines, ""
            return True, [], ""

        orig_run = sdr.obclient_run_sql
        orig_query = sdr.obclient_query_by_owner_chunks
        try:
            sdr.obclient_run_sql = fake_run
            sdr.obclient_query_by_owner_chunks = fake_query
            ob_meta = sdr.dump_ob_metadata(
                {"executable": "/bin/obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                {"A", "PUBLIC"},
                tracked_object_types={"TABLE", "SYNONYM"},
                synonym_check_scope="public_only",
                include_tab_columns=False,
                include_column_order=False,
                include_indexes=False,
                include_constraints=False,
                include_triggers=False,
                include_sequences=False,
                include_comments=False,
                include_roles=False,
            )
            self.assertEqual(ob_meta.objects_by_type.get("SYNONYM", set()), {"PUBLIC.S_PUB"})
            self.assertNotIn(("A", "S_PRIV", "SYNONYM"), ob_meta.object_statuses)
            self.assertIn(("PUBLIC", "S_PUB", "SYNONYM"), ob_meta.object_statuses)
        finally:
            sdr.obclient_run_sql = orig_run
            sdr.obclient_query_by_owner_chunks = orig_query

    def test_get_source_objects_respects_synonym_check_scope(self):
        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, sql, params=None):
                sql_u = sql.upper()
                binds = list(params or [])
                if "FROM DBA_OBJECTS" in sql_u:
                    self._rows = []
                    return
                if "FROM DBA_SYNONYMS" in sql_u and "OWNER = 'PUBLIC'" not in sql_u:
                    owner_binds = {str(x).upper() for x in binds}
                    all_rows = [
                        ("A", "S_PRIV", "A", "T1"),
                    ]
                    self._rows = [
                        row for row in all_rows
                        if row[0].upper() in owner_binds
                    ]
                    return
                if "FROM DBA_SYNONYMS" in sql_u and "OWNER = 'PUBLIC'" in sql_u:
                    target_binds = {str(x).upper() for x in binds}
                    all_rows = [
                        ("PUBLIC", "S_PUB", "A", "T1"),
                    ]
                    self._rows = [
                        row for row in all_rows
                        if row[2].upper() in target_binds
                    ]
                    return
                if "FROM DBA_MVIEWS" in sql_u or "FROM DBA_TABLES" in sql_u:
                    self._rows = []
                    return
                self._rows = []

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
            ora_cfg = {"user": "u", "password": "p", "dsn": "d"}
            objs_public, findings_public = sdr.get_source_objects(
                ora_cfg,
                ["A"],
                object_types={"SYNONYM"},
                synonym_check_scope="public_only",
                case_sensitive_mode="warn",
            )
            objs_all, findings_all = sdr.get_source_objects(
                ora_cfg,
                ["A"],
                object_types={"SYNONYM"},
                synonym_check_scope="all",
                case_sensitive_mode="warn",
            )
            self.assertIn("PUBLIC.S_PUB", objs_public)
            self.assertNotIn("A.S_PRIV", objs_public)
            self.assertIn("PUBLIC.S_PUB", objs_all)
            self.assertIn("A.S_PRIV", objs_all)
            self.assertEqual(findings_public, ())
            self.assertEqual(findings_all, ())
        finally:
            if orig_connect is None:
                delattr(sdr.oracledb, "connect")
            else:
                sdr.oracledb.connect = orig_connect

    def test_get_source_objects_keeps_private_out_of_scope_synonym(self):
        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, sql, params=None):
                sql_u = sql.upper()
                binds = list(params or [])
                if "FROM DBA_OBJECTS" in sql_u:
                    self._rows = []
                    return
                if "FROM DBA_SYNONYMS" in sql_u and "OWNER = 'PUBLIC'" not in sql_u:
                    owner_binds = {str(x).upper() for x in binds}
                    all_rows = [
                        ("A", "S_OUT", "B", "T2"),
                    ]
                    self._rows = [row for row in all_rows if row[0].upper() in owner_binds]
                    return
                if "FROM DBA_SYNONYMS" in sql_u and "OWNER = 'PUBLIC'" in sql_u:
                    target_binds = {str(x).upper() for x in binds}
                    all_rows = [
                        ("PUBLIC", "S_PUB_OUT", "B", "T2"),
                    ]
                    self._rows = [row for row in all_rows if row[2].upper() in target_binds]
                    return
                if "FROM DBA_MVIEWS" in sql_u or "FROM DBA_TABLES" in sql_u:
                    self._rows = []
                    return
                self._rows = []

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
            ora_cfg = {"user": "u", "password": "p", "dsn": "d"}
            objs_all, _ = sdr.get_source_objects(
                ora_cfg,
                ["A"],
                object_types={"SYNONYM"},
                synonym_check_scope="all",
                case_sensitive_mode="warn",
            )
            self.assertIn("A.S_OUT", objs_all)
            self.assertNotIn("PUBLIC.S_PUB_OUT", objs_all)
        finally:
            if orig_connect is None:
                delattr(sdr.oracledb, "connect")
            else:
                sdr.oracledb.connect = orig_connect

    def test_get_source_objects_filters_unmanaged_public_synonym_chain(self):
        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, sql, params=None):
                sql_u = sql.upper()
                binds = [str(item).upper() for item in (params or [])]
                if "FROM DBA_OBJECTS" in sql_u:
                    self._rows = []
                    return
                if "FROM DBA_SYNONYMS" in sql_u and "OWNER = 'PUBLIC'" not in sql_u:
                    self._rows = []
                    return
                if "FROM DBA_SYNONYMS" in sql_u and "OWNER = 'PUBLIC'" in sql_u:
                    target_binds = set(binds)
                    all_rows = [
                        ("PUBLIC", "ORACLE", "PUBLIC", "JAVA_X"),
                        ("PUBLIC", "JAVA_X", "PUBLIC", "COM_X"),
                    ]
                    self._rows = [
                        row for row in all_rows
                        if row[2].upper() == "PUBLIC" or row[2].upper() in target_binds
                    ]
                    return
                if "FROM DBA_MVIEWS" in sql_u or "FROM DBA_TABLES" in sql_u:
                    self._rows = []
                    return
                self._rows = []

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
            ora_cfg = {"user": "u", "password": "p", "dsn": "d"}
            objs_public, findings_public = sdr.get_source_objects(
                ora_cfg,
                ["A"],
                object_types={"SYNONYM"},
                synonym_check_scope="public_only",
                case_sensitive_mode="warn",
            )
            self.assertNotIn("PUBLIC.ORACLE", objs_public)
            self.assertNotIn("PUBLIC.JAVA_X", objs_public)
            self.assertEqual(findings_public, ())
        finally:
            if orig_connect is None:
                delattr(sdr.oracledb, "connect")
            else:
                sdr.oracledb.connect = orig_connect

    def test_get_source_objects_keeps_public_chain_to_managed_terminal(self):
        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, sql, params=None):
                sql_u = sql.upper()
                binds = [str(item).upper() for item in (params or [])]
                if "FROM DBA_OBJECTS" in sql_u:
                    self._rows = []
                    return
                if "FROM DBA_SYNONYMS" in sql_u and "OWNER = 'PUBLIC'" not in sql_u:
                    self._rows = []
                    return
                if "FROM DBA_SYNONYMS" in sql_u and "OWNER = 'PUBLIC'" in sql_u:
                    target_binds = set(binds)
                    all_rows = [
                        ("PUBLIC", "PUB_A", "PUBLIC", "PUB_B"),
                        ("PUBLIC", "PUB_B", "A", "T1"),
                    ]
                    self._rows = [
                        row for row in all_rows
                        if row[2].upper() == "PUBLIC" or row[2].upper() in target_binds
                    ]
                    return
                if "FROM DBA_MVIEWS" in sql_u or "FROM DBA_TABLES" in sql_u:
                    self._rows = []
                    return
                self._rows = []

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
            ora_cfg = {"user": "u", "password": "p", "dsn": "d"}
            objs_public, findings_public = sdr.get_source_objects(
                ora_cfg,
                ["A"],
                object_types={"SYNONYM"},
                synonym_check_scope="public_only",
                case_sensitive_mode="warn",
            )
            self.assertIn("PUBLIC.PUB_A", objs_public)
            self.assertIn("PUBLIC.PUB_B", objs_public)
            self.assertEqual(findings_public, ())
        finally:
            if orig_connect is None:
                delattr(sdr.oracledb, "connect")
            else:
                sdr.oracledb.connect = orig_connect

    def test_get_source_objects_public_only_filters_public_to_private_out_of_scope_chain(self):
        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, sql, params=None):
                sql_u = sql.upper()
                binds = [str(item).upper() for item in (params or [])]
                if "FROM DBA_OBJECTS" in sql_u:
                    self._rows = []
                    return
                if "FROM DBA_SYNONYMS" in sql_u and "OWNER = 'PUBLIC'" not in sql_u:
                    owner_binds = set(binds)
                    all_rows = [
                        ("A", "S_PRIV", "B", "T2"),
                    ]
                    self._rows = [row for row in all_rows if row[0].upper() in owner_binds]
                    return
                if "FROM DBA_SYNONYMS" in sql_u and "OWNER = 'PUBLIC'" in sql_u:
                    target_binds = set(binds)
                    all_rows = [
                        ("PUBLIC", "S_PUB", "A", "S_PRIV"),
                    ]
                    self._rows = [
                        row for row in all_rows
                        if row[2].upper() == "PUBLIC" or row[2].upper() in target_binds
                    ]
                    return
                if "FROM DBA_MVIEWS" in sql_u or "FROM DBA_TABLES" in sql_u:
                    self._rows = []
                    return
                self._rows = []

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
            ora_cfg = {"user": "u", "password": "p", "dsn": "d"}
            objs_public, findings_public = sdr.get_source_objects(
                ora_cfg,
                ["A"],
                object_types={"SYNONYM"},
                synonym_check_scope="public_only",
                case_sensitive_mode="warn",
            )
            self.assertNotIn("A.S_PRIV", objs_public)
            self.assertNotIn("PUBLIC.S_PUB", objs_public)
            self.assertEqual(findings_public, ())
        finally:
            if orig_connect is None:
                delattr(sdr.oracledb, "connect")
            else:
                sdr.oracledb.connect = orig_connect

    def test_load_synonym_metadata_prunes_irrelevant_public_chain(self):
        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, sql, params=None):
                sql_u = sql.upper()
                binds = [str(item).upper() for item in (params or [])]
                if "FROM DBA_SYNONYMS" not in sql_u:
                    self._rows = []
                    return
                if "OWNER = 'PUBLIC'" not in sql_u:
                    owner_binds = set(binds)
                    all_rows = [
                        ("A", "S_PRIV", "A", "T1", None),
                    ]
                    self._rows = [row for row in all_rows if row[0].upper() in owner_binds]
                    return
                target_binds = set(binds)
                all_rows = [
                    ("PUBLIC", "PUB_A", "PUBLIC", "PUB_B", None),
                    ("PUBLIC", "PUB_B", "A", "T1", None),
                    ("PUBLIC", "ORACLE", "PUBLIC", "JAVA_X", None),
                    ("PUBLIC", "JAVA_X", "PUBLIC", "COM_X", None),
                ]
                self._rows = [
                    row for row in all_rows
                    if not target_binds or row[2].upper() == "PUBLIC" or row[2].upper() in target_binds
                ]

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
            ora_cfg = {"user": "u", "password": "p", "dsn": "d"}
            meta = sdr.load_synonym_metadata(
                ora_cfg,
                ["A"],
                allowed_terminal_source_schemas=["A"],
            )
            self.assertIn(("A", "S_PRIV"), meta)
            self.assertIn(("PUBLIC", "PUB_A"), meta)
            self.assertIn(("PUBLIC", "PUB_B"), meta)
            self.assertNotIn(("PUBLIC", "ORACLE"), meta)
            self.assertNotIn(("PUBLIC", "JAVA_X"), meta)
        finally:
            if orig_connect is None:
                delattr(sdr.oracledb, "connect")
            else:
                sdr.oracledb.connect = orig_connect

    def test_check_extra_objects_sequence_without_master_list(self):
        oracle_meta = self._make_oracle_meta(sequences={"A": {"SEQ1", "SEQ2"}})
        ob_meta = self._make_ob_meta(sequences={"A": {"SEQ2"}})
        settings = {
            "extra_check_workers": 1,
            "extra_check_chunk_size": 50,
            "extra_check_progress_interval": 1,
        }
        extra_results = sdr.check_extra_objects(
            settings,
            [],
            ob_meta,
            oracle_meta,
            {},
            enabled_extra_types={"SEQUENCE"}
        )
        self.assertEqual(len(extra_results["sequence_mismatched"]), 1)
        mismatch = extra_results["sequence_mismatched"][0]
        self.assertIn("SEQ1", mismatch.missing_sequences)

    def test_check_extra_objects_sequence_attribute_mismatch(self):
        oracle_meta = self._make_oracle_meta(sequences={"A": {"SEQ1"}})
        oracle_meta = oracle_meta._replace(sequence_attrs={
            "A": {
                "SEQ1": {
                    "increment_by": 1,
                    "min_value": 1,
                    "max_value": 9999,
                    "cycle_flag": "N",
                    "order_flag": "N",
                    "cache_size": 20,
                }
            }
        })
        ob_meta = self._make_ob_meta(sequences={"X": {"SEQ1"}})
        ob_meta = ob_meta._replace(sequence_attrs={
            "X": {
                "SEQ1": {
                    "increment_by": 10,
                    "min_value": 1,
                    "max_value": 9999,
                    "cycle_flag": "N",
                    "order_flag": "N",
                    "cache_size": 20,
                }
            }
        })
        settings = {
            "extra_check_workers": 1,
            "extra_check_chunk_size": 50,
            "extra_check_progress_interval": 1,
        }
        extra_results = sdr.check_extra_objects(
            settings,
            [],
            ob_meta,
            oracle_meta,
            {"A.SEQ1": {"SEQUENCE": "X.SEQ1"}},
            enabled_extra_types={"SEQUENCE"}
        )
        self.assertEqual(len(extra_results["sequence_mismatched"]), 0)

    def test_check_extra_objects_large_table_uses_threadpool(self):
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        master_list = [
            ("S.T1", "T.T1", "TABLE"),
            ("S.T2", "T.T2", "TABLE"),
            ("S.T3", "T.T3", "TABLE"),
        ]
        settings = {
            "extra_check_workers": 2,
            "extra_check_chunk_size": 1,
            "extra_check_progress_interval": 1,
        }
        orig_pp = sdr.ProcessPoolExecutor
        orig_run = sdr.run_extra_check_for_table
        orig_max = sdr.EXTRA_CHECK_PROCESS_MAX_TABLES
        try:
            def raising_pp(*_args, **_kwargs):
                raise AssertionError("ProcessPoolExecutor should not be used")
            def stub_run(entry, _ora, _ob, _map, _types):
                return sdr.ExtraTableResult(
                    tgt_name=f"{entry[2]}.{entry[3]}",
                    index_ok=True,
                    index_mismatch=None,
                    constraint_ok=None,
                    constraint_mismatch=None,
                    trigger_ok=None,
                    trigger_mismatch=None,
                    index_time=0.0,
                    constraint_time=0.0,
                    trigger_time=0.0
                )
            sdr.ProcessPoolExecutor = raising_pp
            sdr.run_extra_check_for_table = stub_run
            sdr.EXTRA_CHECK_PROCESS_MAX_TABLES = 2
            extra_results = sdr.check_extra_objects(
                settings,
                master_list,
                ob_meta,
                oracle_meta,
                {},
                enabled_extra_types={"INDEX"}
            )
            self.assertEqual(len(extra_results["index_ok"]), 3)
        finally:
            sdr.ProcessPoolExecutor = orig_pp
            sdr.run_extra_check_for_table = orig_run
            sdr.EXTRA_CHECK_PROCESS_MAX_TABLES = orig_max

    def test_check_extra_objects_threadpool_fallback_on_worker_exception(self):
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        master_list = [
            ("S.T1", "T.T1", "TABLE"),
            ("S.T2", "T.T2", "TABLE"),
        ]
        settings = {
            "extra_check_workers": 2,
            "extra_check_chunk_size": 1,
            "extra_check_progress_interval": 1,
        }
        orig_max = sdr.EXTRA_CHECK_PROCESS_MAX_TABLES
        orig_run = sdr.run_extra_check_for_table
        calls: Dict[str, int] = {}
        try:
            sdr.EXTRA_CHECK_PROCESS_MAX_TABLES = 1

            def flaky_run(entry, _ora, _ob, _map, _types):
                key = f"{entry[2]}.{entry[3]}"
                calls[key] = calls.get(key, 0) + 1
                if key == "T.T1" and calls[key] == 1:
                    raise RuntimeError("simulated worker failure")
                return sdr.ExtraTableResult(
                    tgt_name=key,
                    index_ok=True,
                    index_mismatch=None,
                    constraint_ok=None,
                    constraint_mismatch=None,
                    trigger_ok=None,
                    trigger_mismatch=None,
                    index_time=0.0,
                    constraint_time=0.0,
                    trigger_time=0.0
                )

            sdr.run_extra_check_for_table = flaky_run
            extra_results = sdr.check_extra_objects(
                settings,
                master_list,
                ob_meta,
                oracle_meta,
                {},
                enabled_extra_types={"INDEX"}
            )
            self.assertEqual(len(extra_results["index_ok"]), 2)
            self.assertGreaterEqual(calls.get("T.T1", 0), 2)
        finally:
            sdr.EXTRA_CHECK_PROCESS_MAX_TABLES = orig_max
            sdr.run_extra_check_for_table = orig_run

    def test_normalize_report_dir_layout(self):
        self.assertEqual(sdr.normalize_report_dir_layout(None), "per_run")
        self.assertEqual(sdr.normalize_report_dir_layout("flat"), "flat")
        self.assertEqual(sdr.normalize_report_dir_layout("per-run"), "per_run")

    def test_ensure_trigger_mappings_for_extra_checks_keeps_source_schema_by_default(self):
        oracle_meta = self._make_oracle_meta(
            triggers={
                ("SRC", "T1"): {
                    "TRG_BI": {"owner": "SRC", "event": "INSERT", "status": "ENABLED"}
                }
            }
        )
        mapping = {}
        sdr.ensure_trigger_mappings_for_extra_checks(
            [("SRC.T1", "TGT.T1", "TABLE")],
            oracle_meta,
            mapping
        )
        self.assertEqual(
            sdr.get_mapped_target(mapping, "SRC.TRG_BI", "TRIGGER"),
            "SRC.TRG_BI"
        )

    def test_ensure_trigger_mappings_for_extra_checks_respects_existing_mapping(self):
        oracle_meta = self._make_oracle_meta(
            triggers={
                ("SRC", "T1"): {
                    "TRG_BI": {"owner": "SRC", "event": "INSERT", "status": "ENABLED"}
                }
            }
        )
        mapping = {
            "SRC.TRG_BI": {
                "TRIGGER": "KEEP.TRG_BI"
            }
        }
        sdr.ensure_trigger_mappings_for_extra_checks(
            [("SRC.T1", "TGT.T1", "TABLE")],
            oracle_meta,
            mapping
        )
        self.assertEqual(
            sdr.get_mapped_target(mapping, "SRC.TRG_BI", "TRIGGER"),
            "KEEP.TRG_BI"
        )

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
        self.assertIn('CREATE OR REPLACE TRIGGER "TGT"."TRG1"', remapped)
        self.assertIn('ON "TGT"."T1"', remapped)
        self.assertIn('INSERT INTO "TGT"."T2"', remapped)
        self.assertIn('"TGT"."SEQ1".NEXTVAL', remapped)
        self.assertNotIn("TGT.TGT", remapped)

    def test_remap_trigger_object_references_sequence_fallback_qualifies_source_schema(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_seq BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  :new.id := seq_local.NEXTVAL;\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_SEQ",
            on_target=("TGT", "T1"),
            qualify_schema=True,
        )
        self.assertIn('"SRC"."SEQ_LOCAL".NEXTVAL', remapped)

    def test_remap_trigger_object_references_sequence_uses_explicit_remap_rule(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_seq_exp BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  :new.id := seq_local.CURRVAL;\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_SEQ_EXP",
            on_target=("TGT", "T1"),
            qualify_schema=True,
            remap_rules={"SRC.SEQ_LOCAL": "TGT.SEQ_LOCAL"},
        )
        self.assertIn('"TGT"."SEQ_LOCAL".CURRVAL', remapped)

    def test_remap_trigger_object_references_resolve_private_synonym_to_terminal(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_syn BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  INSERT INTO syn_t(col) VALUES (1);\n"
            "  INSERT INTO src.syn_t(col) VALUES (2);\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "SRC.SYN_T": {"SYNONYM": "TGT.SYN_T"},
            "SRC.BASE_T": {"TABLE": "TGT.BASE_T"},
        }
        synonym_meta = {
            ("SRC", "SYN_T"): sdr.SynonymMeta(
                owner="SRC",
                name="SYN_T",
                table_owner="SRC",
                table_name="BASE_T",
                db_link=None,
            )
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "SRC",
            "TRG_SYN",
            on_target=("TGT", "T1"),
            qualify_schema=True,
            synonym_meta=synonym_meta,
        )
        self.assertIn('INSERT INTO "TGT"."BASE_T"', remapped)
        self.assertNotIn('"TGT"."SYN_T"', remapped)

    def test_remap_trigger_object_references_public_synonym_fallback(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_pub BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  INSERT INTO pub_syn(col) VALUES (1);\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "SRC.BASE_T": {"TABLE": "TGT.BASE_T"},
        }
        synonym_meta = {
            ("PUBLIC", "PUB_SYN"): sdr.SynonymMeta(
                owner="PUBLIC",
                name="PUB_SYN",
                table_owner="SRC",
                table_name="BASE_T",
                db_link=None,
            )
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "SRC",
            "TRG_PUB",
            on_target=("TGT", "T1"),
            qualify_schema=True,
            synonym_meta=synonym_meta,
        )
        self.assertIn('INSERT INTO "TGT"."BASE_T"', remapped)

    def test_remap_trigger_object_references_local_table_beats_public_synonym(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_local_pub BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  INSERT INTO t_local(col) VALUES (1);\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "SRC.T_LOCAL": {"TABLE": "TGT.T_LOCAL"},
            "PUB.T_LOCAL": {"TABLE": "PUB.T_LOCAL"},
        }
        synonym_meta = {
            ("PUBLIC", "T_LOCAL"): sdr.SynonymMeta(
                owner="PUBLIC",
                name="T_LOCAL",
                table_owner="PUB",
                table_name="T_LOCAL",
                db_link=None,
            ),
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_LOCAL_PUB",
            on_target=("TGT", "T1"),
            qualify_schema=True,
            synonym_meta=synonym_meta,
        )
        self.assertIn('INSERT INTO "TGT"."T_LOCAL"', remapped)
        self.assertNotIn('"PUB"."T_LOCAL"', remapped)

    def test_remap_trigger_object_references_explicit_rule_beats_same_name_synonym(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_syn_conf BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  INSERT INTO perst_trail(col) VALUES (1);\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "SRC.PERST_TRAIL": {"TABLE": "TGT.PERST_TRAIL"},
            "SRC.PERST_TRAIL_VW": {"VIEW": "TGT.PERST_TRAIL_VW"},
        }
        synonym_meta = {
            ("SRC", "PERST_TRAIL"): sdr.SynonymMeta(
                owner="SRC",
                name="PERST_TRAIL",
                table_owner="SRC",
                table_name="PERST_TRAIL_VW",
                db_link=None,
            ),
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_SYN_CONF",
            on_target=("TGT", "T1"),
            qualify_schema=True,
            remap_rules={"SRC.PERST_TRAIL": "TGT.PERST_TRAIL"},
            synonym_meta=synonym_meta,
        )
        self.assertIn('INSERT INTO "TGT"."PERST_TRAIL"', remapped)
        self.assertNotIn('"TGT"."PERST_TRAIL_VW"', remapped)

    def test_remap_trigger_object_references_view_reference_keeps_view_semantics(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_v BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  INSERT INTO v_bus(col) VALUES (1);\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            # 即便 remap 结果指向 TABLE 名，也应保留 VIEW 语义（仅补 schema）。
            "SRC.V_BUS": {"VIEW": "TGT.T_BUS"},
        }
        view_rows: List[sdr.TriggerViewReferenceRow] = []
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_V",
            on_target=("TGT", "T1"),
            qualify_schema=True,
            trigger_full="TGT.TRG_V",
            trigger_view_reference_rows=view_rows,
        )
        self.assertIn('INSERT INTO "TGT"."V_BUS"', remapped)
        self.assertNotIn('"TGT"."T_BUS"', remapped)
        self.assertEqual(len(view_rows), 1)
        self.assertEqual(view_rows[0].trigger_full, "TGT.TRG_V")
        self.assertTrue(view_rows[0].location.startswith("DML:"))

    def test_remap_trigger_object_references_synonym_terminal_view_keeps_view_semantics(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_syn_v BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  INSERT INTO syn_v(col) VALUES (1);\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "SRC.SYN_V": {"SYNONYM": "TGT.SYN_V"},
            # 终点为 VIEW，但 remap 值故意给到 TABLE 名，验证“保留视图语义”。
            "SRC.V_BUS": {"VIEW": "TGT.T_BUS"},
        }
        synonym_meta = {
            ("SRC", "SYN_V"): sdr.SynonymMeta(
                owner="SRC",
                name="SYN_V",
                table_owner="SRC",
                table_name="V_BUS",
                db_link=None,
            )
        }
        view_rows: List[sdr.TriggerViewReferenceRow] = []
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_SYN_V",
            on_target=("TGT", "T1"),
            qualify_schema=True,
            synonym_meta=synonym_meta,
            trigger_full="TGT.TRG_SYN_V",
            trigger_view_reference_rows=view_rows,
        )
        self.assertIn('INSERT INTO "TGT"."V_BUS"', remapped)
        self.assertNotIn('"TGT"."T_BUS"', remapped)
        self.assertEqual(len(view_rows), 1)
        self.assertIn("SYN_V", view_rows[0].source_reference)
        self.assertEqual(view_rows[0].resolved_reference, "TGT.V_BUS")

    def test_remap_trigger_object_references_view_semantics_with_source_view_keys_fallback(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_v_fallback BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  INSERT INTO src.v_bus(col) VALUES (1);\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            # 模拟 check_primary_types 未包含 VIEW：映射缺少 SRC.V_BUS 的 VIEW 类型。
        }
        view_rows: List[sdr.TriggerViewReferenceRow] = []
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_V_FALLBACK",
            on_target=("TGT", "T1"),
            qualify_schema=True,
            remap_rules={"SRC.V_BUS": "TGT.T_BUS"},
            trigger_full="TGT.TRG_V_FALLBACK",
            trigger_view_reference_rows=view_rows,
            source_view_keys={"SRC.V_BUS"},
        )
        self.assertIn('INSERT INTO "TGT"."V_BUS"', remapped)
        self.assertNotIn('"TGT"."T_BUS"', remapped)
        self.assertEqual(len(view_rows), 1)
        self.assertEqual(view_rows[0].reference_type, "VIEW")

    def test_remap_trigger_object_references_unqualified_table_gets_schema(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_local BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  INSERT INTO local_tab(col) VALUES (1);\n"
            "  UPDATE local_tab SET col = col + 1;\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "SRC",
            "TRG_LOCAL",
            on_target=("TGT", "T1"),
            qualify_schema=True,
        )
        self.assertIn('INSERT INTO "SRC"."LOCAL_TAB"', remapped)
        self.assertIn('UPDATE "SRC"."LOCAL_TAB"', remapped)

    def test_remap_trigger_object_references_update_event_header_keeps_on_keyword(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_upd BEFORE UPDATE ON t1\n"
            "BEGIN\n"
            "  UPDATE t2 SET c = 1;\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_UPD",
            on_target=("TGT", "T1"),
            qualify_schema=True,
        )
        self.assertIn('BEFORE UPDATE ON "TGT"."T1"', remapped)
        self.assertNotIn('"SRC"."ON"', remapped)
        self.assertIn('UPDATE "SRC"."T2" SET', remapped)

    def test_remap_trigger_object_references_legacy_on_keyword_regression_case(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_pol_agt BEFORE UPDATE ON pol_agt\n"
            "BEGIN\n"
            "  NULL;\n"
            "END;\n"
        )
        mapping = {
            "LIFEDATA.POL_AGT": {"TABLE": "UWSDATA.POL_AGT"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "LIFEDATA",
            "UWSDATA",
            "TRG_POL_AGT",
            on_target=("UWSDATA", "POL_AGT"),
            qualify_schema=True,
        )
        self.assertIn('CREATE OR REPLACE TRIGGER "UWSDATA"."TRG_POL_AGT"', remapped)
        self.assertIn('BEFORE UPDATE ON "UWSDATA"."POL_AGT"', remapped)
        self.assertNotIn('"LIFEDATA"."ON"', remapped)
        self.assertNotIn('BEFORE UPDATE "LIFEDATA"."ON"', remapped)

    def test_remap_trigger_object_references_editionable_update_header_keeps_on_keyword(self):
        ddl = (
            "CREATE OR REPLACE EDITIONABLE TRIGGER trg_pol_agt BEFORE UPDATE ON pol_agt\n"
            "BEGIN\n"
            "  NULL;\n"
            "END;\n"
        )
        mapping = {
            "LIFEDATA.POL_AGT": {"TABLE": "UWSDATA.POL_AGT"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "LIFEDATA",
            "LIFELOG",
            "TR_U_POL_AGT",
            on_target=("UWSDATA", "POL_AGT"),
            qualify_schema=True,
        )
        self.assertIn('CREATE OR REPLACE EDITIONABLE TRIGGER "LIFELOG"."TR_U_POL_AGT"', remapped)
        self.assertIn('BEFORE UPDATE ON "UWSDATA"."POL_AGT"', remapped)
        self.assertNotIn('"LIFEDATA"."ON"', remapped)
        self.assertNotIn('BEFORE UPDATE "LIFEDATA"."ON"', remapped)

    def test_remap_trigger_object_references_noneditionable_update_header_keeps_on_keyword(self):
        ddl = (
            "CREATE OR REPLACE NONEDITIONABLE TRIGGER trg_pol_agt BEFORE UPDATE ON pol_agt\n"
            "BEGIN\n"
            "  NULL;\n"
            "END;\n"
        )
        mapping = {
            "LIFEDATA.POL_AGT": {"TABLE": "UWSDATA.POL_AGT"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "LIFEDATA",
            "LIFELOG",
            "TR_U_POL_AGT",
            on_target=("UWSDATA", "POL_AGT"),
            qualify_schema=True,
        )
        self.assertIn('CREATE OR REPLACE NONEDITIONABLE TRIGGER "LIFELOG"."TR_U_POL_AGT"', remapped)
        self.assertIn('BEFORE UPDATE ON "UWSDATA"."POL_AGT"', remapped)
        self.assertNotIn('"LIFEDATA"."ON"', remapped)
        self.assertNotIn('BEFORE UPDATE "LIFEDATA"."ON"', remapped)

    def test_trigger_legacy_nonqualify_mode_editionable_header_keeps_minimal_remap(self):
        ddl = (
            "CREATE OR REPLACE EDITIONABLE TRIGGER trg_pol_agt BEFORE UPDATE ON pol_agt\n"
            "BEGIN\n"
            "  NULL;\n"
            "END;\n"
        )
        adjusted = sdr.adjust_ddl_for_object(
            ddl,
            "LIFEDATA",
            "TRG_POL_AGT",
            "UWSDATA",
            "TRG_POL_AGT",
            extra_identifiers=[(("LIFEDATA", "POL_AGT"), ("UWSDATA", "POL_AGT"))],
            obj_type="TRIGGER",
        )
        remapped = sdr.remap_plsql_object_references(
            adjusted,
            "TRIGGER",
            {"LIFEDATA.POL_AGT": {"TABLE": "UWSDATA.POL_AGT"}},
            source_schema="LIFEDATA",
            trigger_qualify_schema=False,
            trigger_on_target=("UWSDATA", "POL_AGT"),
            trigger_tgt_schema="UWSDATA",
            trigger_tgt_name="TRG_POL_AGT",
        )
        name_pattern = sdr.re.compile(
            rf'({sdr.TRIGGER_CREATE_PREFIX_RE}\s+)"?LIFEDATA"?\s*\.\s*"?TRG_POL_AGT"?',
            sdr.re.IGNORECASE,
        )
        remapped = name_pattern.sub(
            rf'\1{sdr.quote_qualified_parts("UWSDATA", "TRG_POL_AGT")}',
            remapped,
            count=1,
        )
        on_pattern = sdr.re.compile(r'(\bON\s+)("?\s*LIFEDATA\s*"?\s*\.\s*)?"?POL_AGT"?', sdr.re.IGNORECASE)
        remapped = on_pattern.sub(
            rf'\1{sdr.quote_qualified_parts("UWSDATA", "POL_AGT")}',
            remapped,
            count=1,
        )
        self.assertIn('CREATE OR REPLACE EDITIONABLE TRIGGER UWSDATA.TRG_POL_AGT', adjusted.upper())
        self.assertIn('BEFORE UPDATE ON "UWSDATA"."POL_AGT"', remapped)
        self.assertNotIn('"LIFEDATA"."ON"', remapped)

    def test_trigger_legacy_nonqualify_mode_noneditionable_header_keeps_minimal_remap(self):
        ddl = (
            "CREATE OR REPLACE NONEDITIONABLE TRIGGER trg_pol_agt BEFORE UPDATE ON pol_agt\n"
            "BEGIN\n"
            "  NULL;\n"
            "END;\n"
        )
        adjusted = sdr.adjust_ddl_for_object(
            ddl,
            "LIFEDATA",
            "TRG_POL_AGT",
            "UWSDATA",
            "TRG_POL_AGT",
            extra_identifiers=[(("LIFEDATA", "POL_AGT"), ("UWSDATA", "POL_AGT"))],
            obj_type="TRIGGER",
        )
        remapped = sdr.remap_plsql_object_references(
            adjusted,
            "TRIGGER",
            {"LIFEDATA.POL_AGT": {"TABLE": "UWSDATA.POL_AGT"}},
            source_schema="LIFEDATA",
            trigger_qualify_schema=False,
            trigger_on_target=("UWSDATA", "POL_AGT"),
            trigger_tgt_schema="UWSDATA",
            trigger_tgt_name="TRG_POL_AGT",
        )
        name_pattern = sdr.re.compile(
            rf'({sdr.TRIGGER_CREATE_PREFIX_RE}\s+)"?LIFEDATA"?\s*\.\s*"?TRG_POL_AGT"?',
            sdr.re.IGNORECASE,
        )
        remapped = name_pattern.sub(
            rf'\1{sdr.quote_qualified_parts("UWSDATA", "TRG_POL_AGT")}',
            remapped,
            count=1,
        )
        on_pattern = sdr.re.compile(r'(\bON\s+)("?\s*LIFEDATA\s*"?\s*\.\s*)?"?POL_AGT"?', sdr.re.IGNORECASE)
        remapped = on_pattern.sub(
            rf'\1{sdr.quote_qualified_parts("UWSDATA", "POL_AGT")}',
            remapped,
            count=1,
        )
        self.assertIn('CREATE OR REPLACE NONEDITIONABLE TRIGGER UWSDATA.TRG_POL_AGT', adjusted.upper())
        self.assertIn('BEFORE UPDATE ON "UWSDATA"."POL_AGT"', remapped)
        self.assertNotIn('"LIFEDATA"."ON"', remapped)

    def test_remap_trigger_object_references_multi_event_header_keeps_or_keyword(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_ui BEFORE UPDATE OR INSERT ON t1\n"
            "BEGIN\n"
            "  UPDATE t2 SET c = 1;\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_UI",
            on_target=("TGT", "T1"),
            qualify_schema=True,
        )
        self.assertIn('BEFORE UPDATE OR INSERT ON "TGT"."T1"', remapped)
        self.assertNotIn('"SRC"."OR"', remapped)
        self.assertIn('UPDATE "SRC"."T2" SET', remapped)

    def test_remap_trigger_object_references_event_header_ignores_comment_on_token(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_ui BEFORE UPDATE OR INSERT\n"
            "-- ON SRC.FAKE_TAB should not affect event header match\n"
            "ON t1\n"
            "BEGIN\n"
            "  UPDATE t2 SET c = 1;\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_UI",
            on_target=("TGT", "T1"),
            qualify_schema=True,
        )
        self.assertIn("-- ON SRC.FAKE_TAB should not affect event header match", remapped)
        self.assertIn('BEFORE UPDATE OR INSERT\n-- ON SRC.FAKE_TAB should not affect event header match\nON "TGT"."T1"', remapped)
        self.assertNotIn('"SRC"."ON"', remapped)
        self.assertNotIn('"SRC"."OR"', remapped)
        self.assertIn('UPDATE "SRC"."T2" SET', remapped)

    def test_remap_trigger_object_references_ignores_comments_and_literals(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_syn BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  -- INSERT INTO syn_t should stay untouched\n"
            "  v_sql := 'UPDATE syn_t SET id = 1';\n"
            "  INSERT INTO syn_t(id) VALUES (1);\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "SRC.BASE_T": {"TABLE": "TGT.BASE_T"},
        }
        synonym_meta = {
            ("SRC", "SYN_T"): sdr.SynonymMeta(
                owner="SRC",
                name="SYN_T",
                table_owner="SRC",
                table_name="BASE_T",
                db_link=None,
            )
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "SRC",
            "TRG_SYN",
            on_target=("TGT", "T1"),
            qualify_schema=True,
            synonym_meta=synonym_meta,
        )
        self.assertIn("-- INSERT INTO syn_t should stay untouched", remapped)
        self.assertIn("'UPDATE syn_t SET id = 1'", remapped)
        self.assertIn('INSERT INTO "TGT"."BASE_T"', remapped)

    def test_remap_trigger_object_references_remaps_exact_single_quoted_object_literal(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_cfg BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  SELECT 1 INTO :new.id FROM dual WHERE EXISTS (\n"
            "    SELECT 1 FROM cfg WHERE table_name = 'LIFEBASE.BSE_LCS_CONFIG_DATA_TYPE'\n"
            "  );\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "LIFEBASE.BSE_LCS_CONFIG_DATA_TYPE": {"TABLE": "BASEDATA.BSE_LCS_CONFIG_DATA_TYPE"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_CFG",
            on_target=("TGT", "T1"),
            qualify_schema=True,
        )
        self.assertIn("'BASEDATA.BSE_LCS_CONFIG_DATA_TYPE'", remapped)
        self.assertNotIn("'LIFEBASE.BSE_LCS_CONFIG_DATA_TYPE'", remapped)

    def test_remap_trigger_object_references_keeps_partial_string_literal_untouched(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_log BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  v_msg := 'before trace BASEDATA.BANK_FLAG_TBL.BANK_FLAG';\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "LIFEBASE.BANK_FLAG_TBL": {"TABLE": "BASEDATA.BANK_FLAG_TBL"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_LOG",
            on_target=("TGT", "T1"),
            qualify_schema=True,
        )
        self.assertIn("'before trace BASEDATA.BANK_FLAG_TBL.BANK_FLAG'", remapped)

    def test_remap_trigger_object_references_reports_three_part_literal_without_rewrite(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_path BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  v_msg := 'LIFEBASE.BSE_LCS_CONFIG_DATA_TYPE.COL_A';\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "LIFEBASE.BSE_LCS_CONFIG_DATA_TYPE": {"TABLE": "BASEDATA.BSE_LCS_CONFIG_DATA_TYPE"},
        }
        literal_rows = []
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_PATH",
            on_target=("TGT", "T1"),
            qualify_schema=True,
            trigger_full="TGT.TRG_PATH",
            trigger_literal_alert_rows=literal_rows,
        )
        self.assertIn("'LIFEBASE.BSE_LCS_CONFIG_DATA_TYPE.COL_A'", remapped)
        self.assertEqual(len(literal_rows), 1)
        self.assertEqual(literal_rows[0].matched_reference, "LIFEBASE.BSE_LCS_CONFIG_DATA_TYPE")
        self.assertEqual(literal_rows[0].suggested_reference, "BASEDATA.BSE_LCS_CONFIG_DATA_TYPE")

    def test_remap_trigger_object_references_rewrites_schema_table_prefix_in_code_reference(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER trg_col BEFORE INSERT ON t1\n"
            "BEGIN\n"
            "  SELECT lifebase.bse_lcs_config_data_type.col_a INTO :new.id FROM dual;\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "LIFEBASE.BSE_LCS_CONFIG_DATA_TYPE": {"TABLE": "BASEDATA.BSE_LCS_CONFIG_DATA_TYPE"},
        }
        remapped = sdr.remap_trigger_object_references(
            ddl,
            mapping,
            "SRC",
            "TGT",
            "TRG_COL",
            on_target=("TGT", "T1"),
            qualify_schema=True,
        )
        self.assertIn('"BASEDATA"."BSE_LCS_CONFIG_DATA_TYPE".col_a', remapped)

    def test_adjust_ddl_for_object_trigger_keeps_string_literals_for_later_trigger_logic(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER SRC.TRG_A BEFORE INSERT ON SRC.T_EVT\n"
            "BEGIN\n"
            "  v_note := 'SRC.T_MAP';\n"
            "  v_path := 'SRC.T_MAP.COL_A';\n"
            "  SELECT SRC.T_MAP.COL_A INTO :new.id FROM dual;\n"
            "END;\n"
        )
        adjusted = sdr.adjust_ddl_for_object(
            ddl,
            "SRC",
            "TRG_A",
            "TGT",
            "TRG_A",
            extra_identifiers=[
                (("SRC", "T_EVT"), ("TGT", "T_EVT")),
                (("SRC", "T_MAP"), ("TGT", "T_MAP")),
            ],
            obj_type="TRIGGER",
        )
        self.assertIn("'SRC.T_MAP'", adjusted)
        self.assertIn("'SRC.T_MAP.COL_A'", adjusted)
        self.assertIn("SELECT TGT.T_MAP.COL_A INTO", adjusted)

    def test_extract_trigger_table_references_masks_comment_and_literal(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER SRC.TRG_A BEFORE INSERT ON SRC.T1\n"
            "BEGIN\n"
            "  -- FROM SRC.TX in comment\n"
            "  v_sql := 'DELETE FROM SRC.TY';\n"
            "  INSERT INTO SRC.T2(col) VALUES (1);\n"
            "END;\n"
        )
        refs = sdr.extract_trigger_table_references(ddl)
        self.assertIn("SRC.T1", refs)
        self.assertIn("SRC.T2", refs)
        self.assertNotIn("SRC.TX", refs)
        self.assertNotIn("SRC.TY", refs)

    def test_remap_trigger_table_references_ignores_comments_and_literals(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER SRC.TRG_A BEFORE INSERT ON SRC.T1\n"
            "BEGIN\n"
            "  -- INSERT INTO SRC.T2 should not be replaced in comment\n"
            "  v_sql := 'UPDATE SRC.T2 SET C=1';\n"
            "  INSERT INTO SRC.T2(col) VALUES (1);\n"
            "END;\n"
        )
        mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "SRC.T2": {"TABLE": "TGT.T2"},
        }
        remapped = sdr.remap_trigger_table_references(ddl, mapping)
        self.assertIn('INSERT INTO "TGT"."T2"', remapped)
        self.assertIn("-- INSERT INTO SRC.T2 should not be replaced in comment", remapped)
        self.assertIn("'UPDATE SRC.T2 SET C=1'", remapped)

    def test_remap_trigger_object_references_preserves_quoted(self):
        ddl = (
            'CREATE OR REPLACE TRIGGER "SRC"."TRG1" BEFORE INSERT ON "SRC"."T1"\n'
            "BEGIN\n"
            '  INSERT INTO "SRC"."T2"(col) VALUES (1);\n'
            "  :new.id := \"SRC\".\"SEQ1\".NEXTVAL;\n"
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
        self.assertIn('CREATE OR REPLACE TRIGGER "TGT"."TRG1"', remapped)
        self.assertIn('ON "TGT"."T1"', remapped)
        self.assertNotIn('""TGT"', remapped)

    def test_compare_package_objects_source_invalid_and_target_invalid(self):
        master_list = [
            ("A.P1", "A.P1", "PACKAGE"),
            ("A.P2", "A.P2", "PACKAGE BODY"),
        ]
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[],
            column_privileges=[],
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
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
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

    def test_check_primary_objects_does_not_mark_package_as_extra_target(self):
        master_list = [
            ("A.PKG1", "B.PKG1", "PACKAGE"),
            ("A.PKG1", "B.PKG1", "PACKAGE BODY"),
        ]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()._replace(objects_by_type={
            "PACKAGE": {"B.PKG1"},
            "PACKAGE BODY": {"B.PKG1"},
        })

        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"PACKAGE", "PACKAGE BODY"},
            print_only_types=set(),
            settings={},
        )

        self.assertEqual(results.get("extra_targets"), [])

    def test_check_primary_objects_marks_print_only_mview_extra_target(self):
        master_list = [
            ("A.MV1", "B.MV1", "MATERIALIZED VIEW"),
        ]
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()._replace(objects_by_type={
            "MATERIALIZED VIEW": {"B.MV1", "B.MV_EXTRA"},
        })

        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"MATERIALIZED VIEW"},
            print_only_types={"MATERIALIZED VIEW"},
            settings={},
        )

        self.assertEqual(len(results.get("skipped", [])), 1)
        self.assertIn(("MATERIALIZED VIEW", "B.MV_EXTRA"), results.get("extra_targets", []))
        self.assertNotIn(("MATERIALIZED VIEW", "B.MV1"), results.get("extra_targets", []))

    def test_check_primary_objects_scoped_mode_suppresses_unmanaged_target_extras(self):
        master_list = [
            ("SRC.T1", "TGT.T1", "TABLE"),
        ]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
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
                    "identity": False,
                }
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"TGT.T1", "TGT.UNRELATED"}},
            {
                ("TGT", "T1"): {
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
                        "identity": False,
                    }
                },
                ("TGT", "UNRELATED"): {
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
                        "identity": False,
                    }
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
            settings={},
            suppress_unmanaged_target_extras=True,
            managed_target_objects={"TGT.T1"},
        )
        self.assertEqual(results["extra_targets"], [])

    def test_collect_and_export_extra_cleanup_candidates(self):
        tv_results = {
            "extra_targets": [
                ("MATERIALIZED VIEW", "B.MV_EXTRA"),
                ("PACKAGE", "B.PKG_EXTRA"),
                ("SYNONYM", "PUBLIC.SYN_PUB"),
            ]
        }
        extra_results = {
            "index_mismatched": [
                sdr.IndexMismatch(
                    table="B.T1",
                    missing_indexes=set(),
                    extra_indexes={"IX_T1_X"},
                    detail_mismatch=[]
                )
            ],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="B.T1",
                    missing_constraints=set(),
                    extra_constraints={"CK_T1_X"},
                    detail_mismatch=[],
                    downgraded_pk_constraints=set()
                )
            ],
            "sequence_mismatched": [
                sdr.SequenceMismatch(
                    src_schema="A",
                    tgt_schema="B",
                    missing_sequences=set(),
                    extra_sequences={"SEQ_EXTRA"},
                    note=None,
                    missing_mappings=[]
                )
            ],
            "trigger_mismatched": [
                sdr.TriggerMismatch(
                    table="B.T1",
                    missing_triggers=set(),
                    extra_triggers={"B.TRG_T1_X"},
                    detail_mismatch=[],
                    missing_mappings=[]
                )
            ],
        }

        candidates = sdr.collect_extra_cleanup_candidates(tv_results, extra_results)
        target_map = {target: sql for _obj_type, target, _source, sql in candidates}

        self.assertIn("B.MV_EXTRA", target_map)
        self.assertIn("PUBLIC.SYN_PUB", target_map)
        self.assertNotIn("B.PKG_EXTRA", target_map)
        self.assertEqual(target_map["PUBLIC.SYN_PUB"], "DROP PUBLIC SYNONYM SYN_PUB;")

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = sdr.export_extra_cleanup_candidates(Path(tmpdir), candidates)
            self.assertIsNotNone(output_path)
            self.assertTrue(output_path.exists())
            text = output_path.read_text(encoding="utf-8")
            self.assertIn("GENERAL_CANDIDATES", text)
            self.assertIn("CANDIDATE_SQL_COMMENTS", text)
            self.assertIn("-- DROP INDEX", text)

    def test_collect_semantic_extra_constraint_cleanup_candidates_only_fk_and_check(self):
        extra_results = {
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="B.T1",
                    missing_constraints=set(),
                    extra_constraints={"CK_T1_X", "FK_T1_P", "PK_T1", "UK_T1", "NN_T1_C1"},
                    detail_mismatch=[],
                    downgraded_pk_constraints=set(),
                    duplicate_notnull_extra_constraints=frozenset({"NN_T1_C1"}),
                )
            ]
        }
        ob_meta = self._make_ob_meta(constraints={
            ("B", "T1"): {
                "CK_T1_X": {"type": "C", "search_condition": "C1 > 0"},
                "FK_T1_P": {"type": "R", "columns": ["P_ID"], "ref_table_owner": "B", "ref_table_name": "PARENT"},
                "PK_T1": {"type": "P", "columns": ["ID"]},
                "UK_T1": {"type": "U", "columns": ["CODE"]},
                "NN_T1_C1": {"type": "C", "search_condition": '"C1" IS NOT NULL'},
            }
        })
        candidates = sdr.collect_semantic_extra_constraint_cleanup_candidates(
            extra_results,
            ob_meta,
            "semantic_fk_check",
        )
        self.assertEqual(
            {target for _obj_type, target, _source, _sql in candidates},
            {"B.CK_T1_X", "B.FK_T1_P"}
        )
        self.assertEqual(
            sdr.collect_semantic_extra_constraint_cleanup_candidates(extra_results, ob_meta, "safe_only"),
            []
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = sdr.export_extra_cleanup_candidates(
                Path(tmpdir),
                [],
                semantic_constraint_candidates=candidates,
            )
            self.assertIsNotNone(output_path)
            text = output_path.read_text(encoding="utf-8")
            self.assertIn("SEMANTIC_EXTRA_CONSTRAINT_DROP_SQL", text)
            self.assertIn('ALTER TABLE "B"."T1" DROP CONSTRAINT CK_T1_X;', text)
            self.assertIn('ALTER TABLE "B"."T1" DROP CONSTRAINT FK_T1_P;', text)

    def test_export_semantic_extra_constraint_cleanup_fixup_scripts(self):
        candidates = [
            ("CONSTRAINT", "B.CK_T1_X", "SEMANTIC_EXTRA_CHECK", 'ALTER TABLE "B"."T1" DROP CONSTRAINT CK_T1_X;'),
            ("CONSTRAINT", "B.FK_T1_P", "SEMANTIC_EXTRA_FK", 'ALTER TABLE "B"."T1" DROP CONSTRAINT FK_T1_P;'),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = sdr.export_semantic_extra_constraint_cleanup_fixup_scripts(Path(tmpdir), candidates)
            rels = sorted(str(path.relative_to(tmpdir)) for path in paths)
            self.assertEqual(
                rels,
                [
                    "cleanup_semantic/constraint/B.CK_T1_X.drop.sql",
                    "cleanup_semantic/constraint/B.FK_T1_P.drop.sql",
                ]
            )
            text = (Path(tmpdir) / "cleanup_semantic" / "constraint" / "B.CK_T1_X.drop.sql").read_text(encoding="utf-8")
            self.assertIn('ALTER SESSION SET CURRENT_SCHEMA = B;', text)
            self.assertIn('ALTER TABLE "B"."T1" DROP CONSTRAINT CK_T1_X;', text)

    def test_generate_fixup_skips_executable_extra_constraint_cleanup_when_mode_off(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": [],
        }
        extra_results = {
            "index_ok": [],
            "index_mismatched": [],
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="B.T1",
                    missing_constraints=set(),
                    extra_constraints={"NN_T1_C1"},
                    detail_mismatch=["CHECK_DUPLICATE_NOTNULL: 列 C1 源端同语义数=1，目标端同语义数=2；保留=T1_OBCHECK_1；额外=NN_T1_C1。"],
                    downgraded_pk_constraints=set(),
                    duplicate_notnull_extra_constraints=frozenset({"NN_T1_C1"}),
                )
            ],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": set(),
            "fixup_schema_list": set(),
            "name_collision_mode": "off",
            "generate_extra_cleanup": True,
            "extra_constraint_cleanup_mode": "off",
        }
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta(constraints={
            ("B", "T1"): {
                "NN_T1_C1": {"type": "C", "search_condition": '"C1" IS NOT NULL'},
            }
        })
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value="4.2.5.7"):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    [],
                    oracle_meta,
                    {},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=Path(tmp_dir),
                    report_timestamp="20260325_000000",
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={},
                )
            self.assertFalse((Path(tmp_dir) / "cleanup_safe").exists())
            self.assertFalse((Path(tmp_dir) / "cleanup_semantic").exists())

    def test_compute_object_counts_ignores_source_invalid_package_on_both_sides(self):
        full_mapping = {
            "A.P1": {"PACKAGE": "B.P1"},
            "A.P2": {"PACKAGE": "B.P2"},
        }
        oracle_meta = self._make_oracle_meta()
        oracle_meta = oracle_meta._replace(object_statuses={
            ("A", "P1", "PACKAGE"): "INVALID",
            ("A", "P2", "PACKAGE"): "VALID",
        })
        ob_meta = self._make_ob_meta()
        ob_meta = ob_meta._replace(objects_by_type={
            "PACKAGE": {"B.P1", "B.P2"},
        })

        summary = sdr.compute_object_counts(
            full_mapping,
            ob_meta,
            oracle_meta,
            monitored_types=("PACKAGE",),
        )

        self.assertEqual(summary["oracle"]["PACKAGE"], 1)
        self.assertEqual(summary["oceanbase"]["PACKAGE"], 1)
        self.assertEqual(summary["missing"]["PACKAGE"], 0)
        self.assertEqual(summary["extra"]["PACKAGE"], 0)

    def test_reconcile_object_counts_summary_prefers_final_results(self):
        base_summary = {
            "oracle": {
                "TABLE": 3,
                "INDEX": 10,
                "CONSTRAINT": 8,
                "SEQUENCE": 4,
                "TRIGGER": 5,
                "PACKAGE": 2,
                "PACKAGE BODY": 2,
            },
            "oceanbase": {
                "TABLE": 3,
                "INDEX": 2,
                "CONSTRAINT": 1,
                "SEQUENCE": 2,
                "TRIGGER": 1,
                "PACKAGE": 1,
                "PACKAGE BODY": 1,
            },
            "missing": {
                "TABLE": 0,
                "INDEX": 8,
                "CONSTRAINT": 7,
                "SEQUENCE": 2,
                "TRIGGER": 4,
                "PACKAGE": 1,
                "PACKAGE BODY": 1,
            },
            "extra": {
                "TABLE": 0,
                "INDEX": 0,
                "CONSTRAINT": 0,
                "SEQUENCE": 0,
                "TRIGGER": 0,
                "PACKAGE": 5,
                "PACKAGE BODY": 6,
            },
        }
        tv_results = {
            "missing": [("TABLE", "B.T2", "A.T2")],
            "extra_targets": [("TABLE", "B.TX")],
        }
        extra_results = {
            "index_mismatched": [
                sdr.IndexMismatch(table="B.T1", missing_indexes={"I1", "I2"}, extra_indexes={"IX"}, detail_mismatch=[])
            ],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="B.T1",
                    missing_constraints={"C1"},
                    extra_constraints={"CX", "CY"},
                    detail_mismatch=[],
                    downgraded_pk_constraints=set()
                )
            ],
            "sequence_mismatched": [
                sdr.SequenceMismatch(
                    src_schema="A",
                    tgt_schema="B",
                    missing_sequences={"S1"},
                    extra_sequences={"SX"},
                    missing_mappings=[]
                )
            ],
            "trigger_mismatched": [
                sdr.TriggerMismatch(
                    table="B.T1",
                    missing_triggers={"B.TRG1"},
                    extra_triggers={"B.TRGX"},
                    detail_mismatch=[],
                    missing_mappings=[]
                )
            ],
        }
        package_results = {
            "rows": [
                sdr.PackageCompareRow(
                    src_full="A.P1",
                    obj_type="PACKAGE",
                    src_status="VALID",
                    tgt_full="B.P1",
                    tgt_status="MISSING",
                    result="MISSING_TARGET",
                    error_count=0,
                    first_error=""
                ),
                sdr.PackageCompareRow(
                    src_full="A.P1",
                    obj_type="PACKAGE BODY",
                    src_status="VALID",
                    tgt_full="B.P1",
                    tgt_status="VALID",
                    result="OK",
                    error_count=0,
                    first_error=""
                ),
            ]
        }

        out = sdr.reconcile_object_counts_summary(base_summary, tv_results, extra_results, package_results)
        self.assertEqual(out["missing"]["TABLE"], 1)
        self.assertEqual(out["extra"]["TABLE"], 1)
        self.assertEqual(out["missing"]["INDEX"], 2)
        self.assertEqual(out["extra"]["INDEX"], 1)
        self.assertEqual(out["missing"]["CONSTRAINT"], 1)
        self.assertEqual(out["extra"]["CONSTRAINT"], 2)
        self.assertEqual(out["missing"]["SEQUENCE"], 1)
        self.assertEqual(out["extra"]["SEQUENCE"], 1)
        self.assertEqual(out["missing"]["TRIGGER"], 1)
        self.assertEqual(out["extra"]["TRIGGER"], 1)
        self.assertEqual(out["missing"]["PACKAGE"], 1)
        self.assertEqual(out["extra"]["PACKAGE"], 5)  # package extra 仍沿用原口径
        self.assertEqual(out["missing"]["PACKAGE BODY"], 0)
        self.assertEqual(out["extra"]["PACKAGE BODY"], 6)

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

    def test_print_final_report_suggests_view_chain_autofix_when_view_missing(self):
        tv_results = {
            "missing": [("VIEW", "A.V1", "A.V1")],
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
        with tempfile.TemporaryDirectory() as tmp_dir:
            report_path = Path(tmp_dir) / "report_20240101.txt"
            sdr.print_final_report(
                tv_results,
                total_checked=1,
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
                package_results=None,
                run_summary_ctx=None,
                filtered_grants=None
            )
            content = report_path.read_text(encoding="utf-8")
            self.assertIn("view-chain-autofix", content)

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
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
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
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        ob_meta = sdr.ObMetadata(
            objects_by_type={"PACKAGE": set(), "PACKAGE BODY": set()},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
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
            table_columns={}, invisible_column_supported=False, identity_column_supported=True, default_on_null_supported=True,
            indexes={}, constraints={}, triggers={}, sequences={}, sequence_attrs={},
            table_comments={}, column_comments={}, comments_complete=True,
            blacklist_tables={}, object_privileges=[], column_privileges=[], sys_privileges=[],
            role_privileges=[], role_metadata={}, system_privilege_map=set(),
            table_privilege_map=set(), object_statuses={}, package_errors={}, package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        ob_meta = sdr.ObMetadata(
            objects_by_type={"PACKAGE": set(), "PACKAGE BODY": set()},
            tab_columns={}, invisible_column_supported=False, identity_column_supported=True, default_on_null_supported=True,
            indexes={}, constraints={}, triggers={}, sequences={}, sequence_attrs={},
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

    def test_normalize_ob_public_owner(self):
        meta = self._make_ob_meta()
        meta = meta._replace(
            objects_by_type={"SYNONYM": {"__PUBLIC.S1"}},
            object_statuses={("__PUBLIC", "S1", "SYNONYM"): "VALID"},
            sequences={"__PUBLIC": {"SEQ1"}},
        )
        normalized = sdr.normalize_ob_metadata_public_owner(meta)
        self.assertIn("PUBLIC.S1", normalized.objects_by_type.get("SYNONYM", set()))
        self.assertIn(("PUBLIC", "S1", "SYNONYM"), normalized.object_statuses)
        self.assertIn("PUBLIC", normalized.sequences)

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

    def test_split_ddl_statements_handles_q_quote_with_semicolon(self):
        ddl = (
            "CREATE VIEW V1 AS SELECT q'!A''B;C!' AS C FROM DUAL;\n"
            "CREATE VIEW V2 AS SELECT 1 FROM DUAL;"
        )
        stmts = sdr.split_ddl_statements(ddl)
        self.assertEqual(len(stmts), 2)
        self.assertTrue(stmts[0].upper().startswith("CREATE VIEW V1"))
        self.assertTrue(stmts[1].upper().startswith("CREATE VIEW V2"))

    def test_purge_report_db_retention_cleans_children_and_summary(self):
        calls = []

        def _fake_commit(_ob_cfg, sql_query, timeout=None):
            calls.append(sql_query)
            return True, "", ""

        with mock.patch.object(sdr, "obclient_run_sql_commit", side_effect=_fake_commit):
            sdr.purge_report_db_retention({"executable": "/usr/bin/obclient"}, "RPT.", 90)

        child_count = len(sdr.REPORT_DB_TABLES) - 1
        self.assertEqual(len(calls), child_count + 1)
        self.assertTrue(any("DELETE FROM RPT.DIFF_REPORT_DETAIL" in sql for sql in calls))
        self.assertTrue(any("DELETE FROM RPT.DIFF_REPORT_SUMMARY" in sql for sql in calls))

    def test_load_from_flat_cache_keeps_schema_request_on_read_failure(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            file_path = sdr.get_flat_cache_path(base, "A", "VIEW", "V1")
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text("CREATE VIEW A.V1 AS SELECT 1 FROM DUAL;", encoding="utf-8")

            schema_requests = {"A": {"VIEW": {"V1"}}}
            accumulator = {}
            source_meta = {}
            original_read_text = Path.read_text

            def _fake_read_text(self, *args, **kwargs):
                if self == file_path:
                    raise OSError("mock read failure")
                return original_read_text(self, *args, **kwargs)

            with mock.patch.object(Path, "read_text", new=_fake_read_text):
                loaded = sdr.load_from_flat_cache(base, schema_requests, accumulator, source_meta, parallel_workers=1)

            self.assertEqual(loaded, 0)
            self.assertIn("A", schema_requests)
            self.assertIn("VIEW", schema_requests["A"])
            self.assertIn("V1", schema_requests["A"]["VIEW"])

    def test_load_from_flat_cache_uses_file_when_index_misses_object(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            file_path = sdr.get_flat_cache_path(base, "A", "TRIGGER", "TR1")
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text("CREATE OR REPLACE TRIGGER A.TR1 BEFORE INSERT ON A.T1 BEGIN NULL; END;/", encoding="utf-8")
            index_path = sdr.get_flat_cache_index_path(base)
            index_path.parent.mkdir(parents=True, exist_ok=True)
            index_path.write_text(
                json.dumps({"version": 1, "objects": {"A": {"TRIGGER": ["OTHER_TRG"]}}}),
                encoding="utf-8",
            )

            schema_requests = {"A": {"TRIGGER": {"TR1"}}}
            accumulator = {}
            source_meta = {}

            loaded = sdr.load_from_flat_cache(base, schema_requests, accumulator, source_meta, parallel_workers=1)

            self.assertEqual(loaded, 1)
            self.assertEqual(accumulator["A"]["TRIGGER"]["TR1"], "CREATE OR REPLACE TRIGGER A.TR1 BEFORE INSERT ON A.T1 BEGIN NULL; END;/")
            self.assertEqual(source_meta[("A", "TRIGGER", "TR1")][0], "cache")
            self.assertNotIn("A", schema_requests)

    def test_load_from_flat_cache_repairs_index_when_file_exists(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            file_path = sdr.get_flat_cache_path(base, "A", "TRIGGER", "TR1")
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text("CREATE OR REPLACE TRIGGER A.TR1 BEFORE INSERT ON A.T1 BEGIN NULL; END;/", encoding="utf-8")
            index_path = sdr.get_flat_cache_index_path(base)
            index_path.parent.mkdir(parents=True, exist_ok=True)
            index_path.write_text(
                json.dumps({"version": 1, "objects": {"A": {"TRIGGER": []}}}),
                encoding="utf-8",
            )

            schema_requests = {"A": {"TRIGGER": {"TR1"}}}
            accumulator = {}
            source_meta = {}

            loaded = sdr.load_from_flat_cache(base, schema_requests, accumulator, source_meta, parallel_workers=1)

            self.assertEqual(loaded, 1)
            repaired_index = sdr.load_flat_cache_index(base)
            self.assertIn("TR1", repaired_index["A"]["TRIGGER"])

    def test_insert_report_artifact_line_rows_counts_only_successful_batches(self):
        rows = [
            {"artifact_type": "A", "file_path": "f1", "line_no": 1, "line_text": "x"},
            {"artifact_type": "A", "file_path": "f2", "line_no": 2, "line_text": "y"},
        ]
        call_seq = [
            (False, "", "ORA-00900"),
            (True, "", ""),
        ]
        with mock.patch.object(sdr, "obclient_run_sql_commit", side_effect=call_seq), \
             mock.patch.object(sdr, "_record_report_db_write_error", return_value=None):
            ok, inserted = sdr._insert_report_artifact_line_rows(
                {"executable": "/usr/bin/obclient"},
                "RPT.",
                "RID",
                iter(rows),
                batch_size=1
            )

        self.assertFalse(ok)
        self.assertEqual(inserted, 1)

    def test_build_report_sql_template_uses_embedded_template(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            report_dir = Path(tmp_dir)
            output = sdr.build_report_sql_template_file(report_dir, "20260311_090000", "RID123")
            self.assertIsNotNone(output)
            content = Path(output).read_text(encoding="utf-8")
            self.assertIn("# report_id=RID123", content)
            self.assertIn("# report_sql_template_version=20260313_12", content)
            self.assertIn("本文件仅提供 report_id 与 HOW TO 入口", content)
            self.assertIn("HOW_TO_READ_REPORTS_IN_OB_latest.txt", content)
            self.assertIn("HOW_TO_READ_REPORTS_IN_OB_20260313_12_sqls.txt", content)

    def test_apply_fixup_idempotency_replace(self):
        ddl = "CREATE VIEW V1 AS SELECT 1 FROM DUAL;"
        settings = {
            "fixup_idempotent_mode": "replace",
            "fixup_idempotent_types_set": {"VIEW"},
        }
        out = sdr.apply_fixup_idempotency(ddl, "VIEW", "SCHEMA", "V1", settings)
        self.assertIn("CREATE OR REPLACE VIEW", out.upper())

    def test_apply_fixup_idempotency_guard_with_remainder(self):
        ddl = "CREATE TABLE T1 (ID NUMBER);\nCOMMENT ON TABLE T1 IS 'X';"
        settings = {
            "fixup_idempotent_mode": "guard",
            "fixup_idempotent_types_set": {"TABLE"},
        }
        out = sdr.apply_fixup_idempotency(ddl, "TABLE", "SCHEMA", "T1", settings)
        self.assertIn("DECLARE", out)
        self.assertIn("EXECUTE IMMEDIATE", out)
        self.assertIn("COMMENT ON TABLE", out.upper())

    def test_apply_fixup_idempotency_drop_create_constraint(self):
        ddl = "ALTER TABLE S.T1 ADD CONSTRAINT C1 PRIMARY KEY (ID);"
        settings = {
            "fixup_idempotent_mode": "drop_create",
            "fixup_idempotent_types_set": {"CONSTRAINT"},
        }
        out = sdr.apply_fixup_idempotency(
            ddl,
            "CONSTRAINT",
            "S",
            "C1",
            settings,
            parent_table="T1"
        )
        self.assertIn("DROP CONSTRAINT C1", out.upper())
        self.assertIn("ALTER TABLE S.T1 ADD CONSTRAINT C1", out.upper())

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
            column_privs=set(),
            column_privs_grantable=set(),
            sys_privs=set(),
            sys_privs_admin=set(),
            role_privs=set(),
            role_privs_admin=set(),
        )
        miss_obj, miss_col, miss_sys, miss_role = sdr.filter_missing_grant_entries(
            object_grants,
            {},
            sys_privs,
            role_privs,
            ob_catalog
        )
        self.assertEqual(len(miss_obj.get("A", set())), 1)
        self.assertEqual(miss_col, {})
        self.assertIn(sdr.ObjectGrantEntry("SELECT", "X.T2", True), miss_obj.get("A", set()))
        self.assertEqual(len(miss_sys.get("A", set())), 1)
        self.assertEqual(len(miss_role.get("A", set())), 1)

    def test_filter_missing_grant_entries_normalizes_debug_alias(self):
        object_grants = {
            "PUBLIC": {
                sdr.ObjectGrantEntry("DEBUG", "A.P1", False),
            }
        }
        ob_catalog = sdr.ObGrantCatalog(
            object_privs={("PUBLIC", "OTHERS", "A.P1")},
            object_privs_grantable=set(),
            column_privs=set(),
            column_privs_grantable=set(),
            sys_privs=set(),
            sys_privs_admin=set(),
            role_privs=set(),
            role_privs_admin=set(),
        )
        capability = sdr.GrantCapabilityLibrary(
            object_alias_to_logical={("PROCEDURE", "OTHERS"): "DEBUG"},
            known_logical_object_privileges={"DEBUG"},
        )
        miss_obj, miss_col, miss_sys, miss_role = sdr.filter_missing_grant_entries(
            object_grants,
            {},
            {},
            {},
            ob_catalog,
            capability_library=capability,
            object_target_types={"A.P1": "PROCEDURE"},
        )
        self.assertEqual(miss_obj, {})
        self.assertEqual(miss_col, {})
        self.assertEqual(miss_sys, {})
        self.assertEqual(miss_role, {})

    def test_filter_missing_grant_entries_accepts_role_inherited_object_privilege(self):
        object_grants = {
            "U1": {
                sdr.ObjectGrantEntry("EXECUTE", "A.P1", False),
            }
        }
        ob_catalog = sdr.ObGrantCatalog(
            object_privs={("R_EXEC", "EXECUTE", "A.P1")},
            object_privs_grantable=set(),
            column_privs=set(),
            column_privs_grantable=set(),
            sys_privs=set(),
            sys_privs_admin=set(),
            role_privs={("U1", "R_EXEC")},
            role_privs_admin=set(),
        )
        miss_obj, miss_col, miss_sys, miss_role = sdr.filter_missing_grant_entries(
            object_grants,
            {},
            {},
            {},
            ob_catalog,
        )
        self.assertEqual(miss_obj, {})
        self.assertEqual(miss_col, {})
        self.assertEqual(miss_sys, {})
        self.assertEqual(miss_role, {})

    def test_filter_missing_grant_entries_accepts_sys_privilege_coverage(self):
        object_grants = {
            "U1": {
                sdr.ObjectGrantEntry("EXECUTE", "A.P1", False),
            }
        }
        ob_catalog = sdr.ObGrantCatalog(
            object_privs=set(),
            object_privs_grantable=set(),
            column_privs=set(),
            column_privs_grantable=set(),
            sys_privs={("U1", "EXECUTE ANY PROCEDURE")},
            sys_privs_admin=set(),
            role_privs=set(),
            role_privs_admin=set(),
        )
        miss_obj, miss_col, miss_sys, miss_role = sdr.filter_missing_grant_entries(
            object_grants,
            {},
            {},
            {},
            ob_catalog,
        )
        self.assertEqual(miss_obj, {})
        self.assertEqual(miss_col, {})
        self.assertEqual(miss_sys, {})
        self.assertEqual(miss_role, {})

    def test_filter_missing_grant_entries_accepts_insert_any_table(self):
        object_grants = {
            "U1": {
                sdr.ObjectGrantEntry("INSERT", "A.T1", False),
            }
        }
        ob_catalog = sdr.ObGrantCatalog(
            object_privs=set(),
            object_privs_grantable=set(),
            column_privs=set(),
            column_privs_grantable=set(),
            sys_privs={("U1", "INSERT ANY TABLE")},
            sys_privs_admin=set(),
            role_privs=set(),
            role_privs_admin=set(),
        )
        miss_obj, miss_col, miss_sys, miss_role = sdr.filter_missing_grant_entries(
            object_grants,
            {},
            {},
            {},
            ob_catalog,
        )
        self.assertEqual(miss_obj, {})
        self.assertEqual(miss_col, {})
        self.assertEqual(miss_sys, {})
        self.assertEqual(miss_role, {})

    def test_filter_missing_grant_entries_accepts_update_any_table_for_column_privilege(self):
        column_grants = {
            "U1": {
                sdr.ColumnGrantEntry("UPDATE", "A.T1", "C1", False),
            }
        }
        ob_catalog = sdr.ObGrantCatalog(
            object_privs=set(),
            object_privs_grantable=set(),
            column_privs=set(),
            column_privs_grantable=set(),
            sys_privs={("U1", "UPDATE ANY TABLE")},
            sys_privs_admin=set(),
            role_privs=set(),
            role_privs_admin=set(),
        )
        miss_obj, miss_col, miss_sys, miss_role = sdr.filter_missing_grant_entries(
            {},
            column_grants,
            {},
            {},
            ob_catalog,
        )
        self.assertEqual(miss_obj, {})
        self.assertEqual(miss_col, {})
        self.assertEqual(miss_sys, {})
        self.assertEqual(miss_role, {})

    def test_build_invalid_target_object_set_uses_object_type_map(self):
        ob_meta = self._make_ob_meta()._replace(
            object_statuses={
                ("APP", "V1", "VIEW"): "INVALID",
                ("APP", "V1", "TABLE"): "VALID",
                ("APP", "P1", "PROCEDURE"): "INVALID",
            }
        )
        invalid = sdr.build_invalid_target_object_set(
            ob_meta,
            object_target_types={
                "APP.V1": "VIEW",
                "APP.P1": "PROCEDURE",
            },
        )
        self.assertEqual(invalid, {"APP.V1", "APP.P1"})

    def test_defer_invalid_target_grants_moves_invalid_objects_out_of_runnable(self):
        grants = {
            "U1": {
                sdr.ObjectGrantEntry("SELECT", "APP.V1", False),
                sdr.ObjectGrantEntry("SELECT", "APP.T1", False),
            }
        }
        kept, deferred, filtered = sdr.defer_invalid_target_grants(
            grants,
            {"APP.V1"},
        )
        self.assertEqual(kept, {
            "U1": {sdr.ObjectGrantEntry("SELECT", "APP.T1", False)}
        })
        self.assertEqual(deferred, {
            "U1": {sdr.ObjectGrantEntry("SELECT", "APP.V1", False)}
        })
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].reason, sdr.DEFERRED_GRANT_REASON_TARGET_INVALID)

    def test_defer_invalid_target_column_grants_moves_invalid_objects_out_of_runnable(self):
        grants = {
            "U1": {
                sdr.ColumnGrantEntry("UPDATE", "APP.V1", "C1", False),
                sdr.ColumnGrantEntry("UPDATE", "APP.T1", "C1", False),
            }
        }
        kept, deferred, filtered = sdr.defer_invalid_target_column_grants(
            grants,
            {"APP.V1"},
        )
        self.assertEqual(kept, {
            "U1": {sdr.ColumnGrantEntry("UPDATE", "APP.T1", "C1", False)}
        })
        self.assertEqual(deferred, {
            "U1": {sdr.ColumnGrantEntry("UPDATE", "APP.V1", "C1", False)}
        })
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].reason, sdr.DEFERRED_GRANT_REASON_TARGET_INVALID)

    def test_build_grant_runnability_detail_rows_classifies_paths(self):
        rows = sdr.build_grant_runnability_detail_rows(
            object_grants_by_grantee={
                "APP": {
                    sdr.ObjectGrantEntry("SELECT", "A.T1", False),
                    sdr.ObjectGrantEntry("UPDATE", "A.V1", True),
                    sdr.ObjectGrantEntry("SELECT", "A.BADV", False),
                }
            },
            column_grants_by_grantee={},
            sys_privs_by_grantee={},
            role_privs_by_grantee={},
            object_grants_missing_by_grantee={
                "APP": {sdr.ObjectGrantEntry("SELECT", "A.T1", False)}
            },
            column_grants_missing_by_grantee={},
            sys_privs_missing_by_grantee={},
            role_privs_missing_by_grantee={},
            view_prereq_grants_by_grantee={},
            view_post_grants_by_grantee={
                "APP": {sdr.ObjectGrantEntry("UPDATE", "A.V1", True)}
            },
            deferred_object_grants_by_grantee={
                "APP": {sdr.ObjectGrantEntry("SELECT", "A.BADV", False)}
            },
            deferred_column_grants_by_grantee={},
            filtered_grants=[],
            deferred_filtered_grants=[
                sdr.FilteredGrantEntry("OBJECT", "APP", "SELECT", "A.BADV", sdr.DEFERRED_GRANT_REASON_TARGET_INVALID)
            ],
            ob_meta=self._make_ob_meta()._replace(
                object_statuses={("A", "BADV", "VIEW"): "INVALID"}
            ),
            object_target_types={
                "A.T1": "TABLE",
                "A.V1": "VIEW",
                "A.BADV": "VIEW",
            },
        )
        by_key = {(row.object_full, row.privilege): row for row in rows if row.category == "OBJECT"}
        self.assertEqual(by_key[("A.T1", "SELECT")].execution_class, "RUNNABLE_NOW")
        self.assertEqual(by_key[("A.T1", "SELECT")].artifact_dir, "grants_miss")
        self.assertEqual(by_key[("A.V1", "UPDATE WITH GRANT OPTION")].execution_class, "RUNNABLE_NOW")
        self.assertEqual(by_key[("A.V1", "UPDATE WITH GRANT OPTION")].artifact_dir, "view_post_grants")
        self.assertEqual(by_key[("A.BADV", "SELECT")].execution_class, "DEFERRED")
        self.assertEqual(by_key[("A.BADV", "SELECT")].target_status, "INVALID")

    def test_export_grant_runnability_detail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [
                sdr.GrantRunnabilityDetailRow(
                    category="OBJECT",
                    grantee="APP",
                    privilege="SELECT",
                    object_full="A.T1",
                    object_type="TABLE",
                    target_status="VALID",
                    execution_class="RUNNABLE_NOW",
                    artifact_dir="grants_miss",
                    reason_code="TARGET_GRANT_MISSING",
                    reason="目标端缺少该授权，可进入本轮授权闭环。",
                    action="执行 grants_miss/。",
                )
            ]
            path = sdr.export_grant_runnability_detail(rows, Path(tmpdir), "20240101")
            self.assertIsNotNone(path)
            output = Path(path).read_text(encoding="utf-8")
            self.assertIn("授权可执行性明细", output)
            self.assertIn("RUNNABLE_NOW", output)
            self.assertIn("grants_miss", output)

    def test_build_target_extra_column_grant_rows_marks_public_revoke(self):
        rows = sdr.build_target_extra_column_grant_rows(
            {},
            {("PUBLIC", "UPDATE", "APP.T1", "C1", False)},
            object_target_types={"APP.T1": "TABLE"},
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].category, "COLUMN")
        self.assertEqual(rows[0].column_name, "C1")
        self.assertEqual(rows[0].action, "REVOKE_PUBLIC")

    def test_build_target_extra_column_grant_rows_skips_unmanaged_target_objects(self):
        rows = sdr.build_target_extra_column_grant_rows(
            {},
            {
                ("PUBLIC", "UPDATE", "OMS_USER.LEGACY_T1", "C1", False),
                ("PUBLIC", "UPDATE", "ORA_APP.T1", "C1", False),
            },
            object_target_types={"ORA_APP.T1": "TABLE", "OMS_USER.LEGACY_T1": "TABLE"},
            managed_target_objects={"ORA_APP.T1"},
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].object_full, "ORA_APP.T1")
        self.assertEqual(rows[0].action, "REVOKE_PUBLIC")

    def test_build_oracle_privilege_family_detail_rows_skips_core_only(self):
        oracle_meta = self._make_oracle_meta()._replace(
            privilege_family_counts=(
                sdr.OraclePrivilegeFamilyCount("OBJECT_PRIV", "DBA_TAB_PRIVS", 3, "RUNNABLE"),
                sdr.OraclePrivilegeFamilyCount("SYSTEM_PRIV", "DBA_SYS_PRIVS", 2, "RUNNABLE"),
            )
        )
        rows = sdr.build_oracle_privilege_family_detail_rows(oracle_meta, object())
        self.assertEqual(rows, [])

    def test_build_oracle_privilege_family_detail_rows_includes_column_family(self):
        oracle_meta = self._make_oracle_meta()._replace(
            privilege_family_counts=(
                sdr.OraclePrivilegeFamilyCount("OBJECT_PRIV", "DBA_TAB_PRIVS", 3, "RUNNABLE"),
                sdr.OraclePrivilegeFamilyCount("COLUMN_PRIV", "DBA_COL_PRIVS", 2, "RUNNABLE"),
                sdr.OraclePrivilegeFamilyCount("NETWORK_ACL_PRIV", "DBA_NETWORK_ACL_PRIVILEGES", 1, "MANUAL_ONLY"),
            )
        )
        with mock.patch.object(sdr, "load_ob_dictionary_view_presence", return_value={
            "DBA_TAB_PRIVS": True,
            "DBA_COL_PRIVS": True,
            "DBA_NETWORK_ACL_PRIVILEGES": False,
        }):
            rows = sdr.build_oracle_privilege_family_detail_rows(oracle_meta, object())
        self.assertEqual(len(rows), 3)
        by_family = {row.family_id: row for row in rows}
        self.assertEqual(by_family["COLUMN_PRIV"].target_capability_state, "TARGET_VIEW_PRESENT")
        self.assertEqual(by_family["NETWORK_ACL_PRIV"].migration_mode, "MANUAL_ONLY")

    def test_build_grant_plan_includes_column_grants(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=False,
            default_on_null_supported=False,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[],
            column_privileges=[
                sdr.OracleColumnPrivilege(
                    grantee="U1",
                    owner="APP",
                    object_name="T1",
                    column_name="C1",
                    privilege="UPDATE",
                    grantable=False,
                )
            ],
            sys_privileges=[],
            role_privileges=[],
            role_metadata={},
            system_privilege_map=set(),
            table_privilege_map={"UPDATE"},
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            interval_partitions={},
            privilege_family_counts=(),
            non_table_triggers=(),
        )
        capability = sdr.GrantCapabilityLibrary(
            column_decisions={
                ("UPDATE", "TABLE"): sdr.GrantCapabilityDecision(
                    support_status=sdr.GRANT_CAPABILITY_SUPPORT_SUPPORTED,
                    decision=sdr.GRANT_CAPABILITY_DECISION_ALLOW,
                )
            },
            known_logical_column_privileges={"UPDATE"},
        )
        plan = sdr.build_grant_plan(
            oracle_meta,
            {"APP.T1": {"TABLE": "APP.T1"}},
            {},
            {"APP.T1": {"TABLE"}},
            {},
            {},
            {},
            {},
            set(),
            {"APP"},
            {},
            {},
            capability_library=capability,
            ob_users={"U1"},
        )
        self.assertIn(
            sdr.ColumnGrantEntry("UPDATE", "APP.T1", "C1", False),
            plan.column_grants.get("U1", set())
        )

    def test_ensure_object_probe_fixture_registers_cleanup_before_multistatement_failure(self):
        names = {}
        created_types = set()
        cleanup_sqls = []

        def fake_run(_ob_cfg, stmt, timeout=None):
            if "PACKAGE BODY" in stmt:
                return False, "", "body failed"
            return True, "", ""

        with mock.patch.object(sdr, "_run_ob_probe_sql", side_effect=fake_run):
            ok, err = sdr._ensure_object_probe_fixture(
                object(),
                "OMS_USER",
                names,
                created_types,
                cleanup_sqls,
                "PACKAGE",
            )
        self.assertFalse(ok)
        self.assertIn("body failed", err)
        self.assertTrue(any(sql.startswith("DROP PACKAGE ") for sql in cleanup_sqls))

    def test_build_non_table_trigger_detail_rows_for_database_event_trigger(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=False,
            default_on_null_supported=False,
            indexes={},
            constraints={},
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
            non_table_triggers=(
                sdr.NonTableTriggerInfo(
                    owner="OMS_USER",
                    trigger_name="TRG_DB_DROP",
                    trigger_type="BEFORE EVENT",
                    triggering_event="DROP",
                    base_object_type="DATABASE",
                    table_owner="SYS",
                    table_name="-",
                    status="ENABLED",
                ),
            ),
        )
        rows = sdr.build_non_table_trigger_detail_rows(oracle_meta, {})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].reason_code, "DATABASE_EVENT_TRIGGER_UNSUPPORTED")
        self.assertIn("DROP", rows[0].reason)

    def test_build_trigger_list_report_marks_non_table_source_trigger(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=False,
            default_on_null_supported=False,
            indexes={},
            constraints={},
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
            non_table_triggers=(
                sdr.NonTableTriggerInfo(
                    owner="OMS_USER",
                    trigger_name="TRG_DB_DROP",
                    trigger_type="BEFORE EVENT",
                    triggering_event="DROP",
                    base_object_type="DATABASE",
                    table_owner="SYS",
                    table_name="-",
                    status="ENABLED",
                ),
            ),
        )
        ob_meta = sdr.ObMetadata(
            objects_by_type={},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=False,
            default_on_null_supported=False,
            indexes={},
            constraints={},
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
        rows, summary = sdr.build_trigger_list_report(
            "new_trigger_list.txt",
            {"OMS_USER.TRG_DB_DROP"},
            [],
            [],
            1,
            None,
            {"trigger_mismatched": []},
            oracle_meta,
            ob_meta,
            {},
            True,
        )
        self.assertEqual(rows[0].status, "NON_TABLE_SOURCE_TRIGGER")
        self.assertEqual(summary["non_table"], 1)

    def test_build_trigger_list_report_accepts_target_trigger_name(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=False,
            default_on_null_supported=False,
            indexes={},
            constraints={},
            triggers={
                ("SRC", "T1"): {
                    "TRG_T1": {"event": "INSERT", "status": "ENABLED"},
                }
            },
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
            non_table_triggers=(),
        )
        ob_meta = sdr.ObMetadata(
            objects_by_type={},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=False,
            default_on_null_supported=False,
            indexes={},
            constraints={},
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
        extra_results = {
            "trigger_mismatched": [
                sdr.TriggerMismatch(
                    table="TGT.T1",
                    missing_triggers={"TGT.TRG_T1"},
                    extra_triggers=set(),
                    detail_mismatch=[],
                    missing_mappings=[("SRC.TRG_T1", "TGT.TRG_T1")],
                )
            ]
        }
        rows, summary = sdr.build_trigger_list_report(
            "new_trigger_list.txt",
            {"TGT.TRG_T1"},
            [],
            [],
            1,
            None,
            extra_results,
            oracle_meta,
            ob_meta,
            {"SRC.TRG_T1": {"TRIGGER": "TGT.TRG_T1"}},
            True,
        )
        self.assertEqual(summary["selected_missing"], 1)
        self.assertEqual(rows[0].status, "SELECTED_MISSING")
        self.assertIn("SRC.TRG_T1", rows[0].detail)

    def test_dump_oracle_metadata_keeps_instead_of_view_trigger_in_scope(self):
        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, sql, binds=None, **kwargs):
                sql_u = " ".join(str(sql).upper().split())
                if "FROM DBA_TAB_COLUMNS" in sql_u and "OWNER = 'SYS'" in sql_u:
                    self._rows = [(0,)]
                elif "FROM DBA_TRIGGERS" in sql_u:
                    self._rows = [
                        ("SRC", "SRC", "V1", "TRG_V1_IOI", "INSTEAD OF", "INSERT", "VIEW", "ENABLED"),
                        ("SRC", "SYS", "-", "TRG_DB_DROP", "BEFORE EVENT", "DROP", "DATABASE", "ENABLED"),
                    ]
                else:
                    self._rows = []

            def fetchone(self):
                return None

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

        master_list = [("SRC.V1", "TGT.V1", "VIEW")]
        settings = {"source_schemas_list": ["SRC"]}
        with mock.patch.object(sdr.oracledb, "connect", return_value=FakeConnection()):
            meta = sdr.dump_oracle_metadata(
                {"user": "u", "password": "p", "dsn": "d"},
                master_list,
                settings,
                include_indexes=False,
                include_constraints=False,
                include_triggers=True,
                include_sequences=False,
                include_comments=False,
                include_blacklist=False,
                include_privileges=False,
            )
        self.assertIn(("SRC", "V1"), meta.triggers)
        self.assertIn("SRC.TRG_V1_IOI", meta.triggers[("SRC", "V1")])
        self.assertEqual(len(meta.non_table_triggers), 1)
        self.assertEqual(meta.non_table_triggers[0].trigger_name, "TRG_DB_DROP")

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
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
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
            column_privs=set(),
            column_privs_grantable=set(),
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

    def test_build_view_fixup_chains_uses_view_dependency_map_fallback(self):
        full_mapping = {
            "A.V1": {"VIEW": "A.V1"},
            "B.SYN1": {"SYNONYM": "B.SYN1"},
            "C.T1": {"TABLE": "C.T1"},
        }
        synonym_meta = {
            ("B", "SYN1"): sdr.SynonymMeta("B", "SYN1", "C", "T1", None),
        }
        chains, cycles = sdr.build_view_fixup_chains(
            ["A.V1"],
            set(),
            full_mapping,
            {},
            synonym_meta=synonym_meta,
            view_dependency_map={("A", "V1"): {"B.SYN1"}},
        )
        self.assertFalse(cycles)
        self.assertTrue(chains)
        self.assertIn("A.V1[VIEW|UNKNOWN|GRANT_NA]", chains[0])
        self.assertIn("B.SYN1[SYNONYM|UNKNOWN|GRANT_UNKNOWN]", chains[0])
        self.assertIn("C.T1[TABLE|UNKNOWN|GRANT_UNKNOWN]", chains[0])

    def test_build_view_fixup_chains_fallback_prefers_explicit_remap(self):
        full_mapping = {
            "A.V1": {"VIEW": "A.V1"},
            "SRC.POL_INFO": {"VIEW": "UWSDATA.POL_INFO_VW"},
        }
        remap_rules = {
            "SRC.POL_INFO": "UWSDATA.POL_INFO",
        }
        chains, cycles = sdr.build_view_fixup_chains(
            ["A.V1"],
            set(),
            full_mapping,
            remap_rules,
            view_dependency_map={("A", "V1"): {"SRC.POL_INFO"}},
        )
        self.assertFalse(cycles)
        self.assertTrue(chains)
        upper = chains[0].upper()
        self.assertIn("UWSDATA.POL_INFO[VIEW", upper)
        self.assertNotIn("UWSDATA.POL_INFO_VW", upper)

    def test_resolve_synonym_chain_target_prefers_explicit_remap(self):
        synonym_meta = {
            ("SRC", "SYN1"): sdr.SynonymMeta("SRC", "SYN1", "SRC", "POL_INFO", None),
        }
        full_mapping = {
            "SRC.POL_INFO": {"VIEW": "UWSDATA.POL_INFO_VW"},
        }
        remap_rules = {
            "SRC.POL_INFO": "UWSDATA.POL_INFO",
        }
        target_to_source = {
            ("TGT.SYN1", "SYNONYM"): "SRC.SYN1",
        }
        target_full, target_type = sdr.resolve_synonym_chain_target(
            "TGT.SYN1",
            "SYNONYM",
            target_to_source,
            synonym_meta,
            full_mapping,
            remap_rules,
        )
        self.assertEqual(target_full, "UWSDATA.POL_INFO")
        self.assertEqual(target_type, "VIEW")

    def test_extract_view_dependencies_with_default_schema(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM T1 t\n"
            "JOIN B.T2 b ON t.ID=b.ID\n"
        )
        deps = sdr.extract_view_dependencies(ddl, default_schema="A")
        self.assertEqual(deps, {"A.T1", "B.T2"})

    def test_build_remap_root_seed_nodes_only_table_and_view(self):
        source_objects = {
            "SRC.T1": {"TABLE"},
            "SRC.V1": {"VIEW"},
            "SRC.P1": {"PROCEDURE"},
        }
        remap_rules = {
            "SRC.T1": "TGT.T1",
            "SRC.V1": "TGT.V1",
            "SRC.P1": "TGT.P1",
            "SRC.MISSING": "TGT.MISSING",
        }
        roots, skipped, explicit_keys = sdr.build_remap_root_seed_nodes(source_objects, remap_rules)
        self.assertEqual(
            roots,
            {("SRC.T1", "TABLE"), ("SRC.V1", "VIEW")}
        )
        self.assertIn(("SRC.P1", "PROCEDURE", "NON_ROOT_OBJECT_TYPE"), skipped)
        self.assertIn(("SRC.MISSING", "-", "SOURCE_OBJECT_NOT_FOUND"), skipped)
        self.assertEqual(explicit_keys, ["SRC.T1", "SRC.V1"])

    def test_build_scoped_trigger_keep_nodes_adds_parent(self):
        source_objects = {
            "SRC.TRG_T1": {"TRIGGER"},
            "SRC.T1": {"TABLE"},
        }
        object_parent_map = {
            "SRC.TRG_T1": "SRC.T1",
        }
        keep_nodes, detail_rows, missing_entries = sdr.build_scoped_trigger_keep_nodes(
            {"SRC.TRG_T1"},
            source_objects,
            object_parent_map,
        )
        self.assertEqual(
            keep_nodes,
            {("SRC.TRG_T1", "TRIGGER"), ("SRC.T1", "TABLE")}
        )
        self.assertEqual(missing_entries, [])
        self.assertTrue(any(row[0] == "TRIGGER_KEEP" for row in detail_rows))
        self.assertTrue(any(row[0] == "TRIGGER_PARENT" for row in detail_rows))

    def test_build_scoped_trigger_keep_nodes_resolves_target_name(self):
        source_objects = {
            "SRC.TRG_T1": {"TRIGGER"},
            "SRC.T1": {"TABLE"},
        }
        object_parent_map = {
            "SRC.TRG_T1": "SRC.T1",
        }
        full_object_mapping = {
            "SRC.TRG_T1": {"TRIGGER": "TGT.TRG_T1"},
        }
        keep_nodes, detail_rows, missing_entries = sdr.build_scoped_trigger_keep_nodes(
            {"TGT.TRG_T1"},
            source_objects,
            object_parent_map,
            full_object_mapping,
        )
        self.assertEqual(
            keep_nodes,
            {("SRC.TRG_T1", "TRIGGER"), ("SRC.T1", "TABLE")}
        )
        self.assertEqual(missing_entries, [])
        self.assertTrue(any("TARGET_NAME" in row[3] for row in detail_rows if row[0] == "TRIGGER_KEEP"))

    def test_build_source_scope_closure_includes_dependencies_attached_and_paired(self):
        source_objects = {
            "SRC.V1": {"VIEW"},
            "SRC.T1": {"TABLE"},
            "SRC.IDX_T1": {"INDEX"},
            "SRC.CK_T1": {"CONSTRAINT"},
            "SRC.TRG_T1": {"TRIGGER"},
            "SRC.PKG1": {"PACKAGE", "PACKAGE BODY"},
            "SRC.SEQ1": {"SEQUENCE"},
            "SRC.UNRELATED": {"TABLE"},
        }
        dependency_graph = {
            ("SRC.V1", "VIEW"): {("SRC.T1", "TABLE"), ("SRC.PKG1", "PACKAGE")},
            ("SRC.PKG1", "PACKAGE"): {("SRC.SEQ1", "SEQUENCE")},
        }
        object_parent_map = {
            "SRC.IDX_T1": "SRC.T1",
            "SRC.CK_T1": "SRC.T1",
            "SRC.TRG_T1": "SRC.T1",
        }
        result = sdr.build_source_scope_closure(
            source_objects,
            dependency_graph,
            object_parent_map,
            {("SRC.V1", "VIEW")},
            mode="remap_root_closure",
        )
        self.assertIn(("SRC.V1", "VIEW"), result.included_nodes)
        self.assertIn(("SRC.T1", "TABLE"), result.included_nodes)
        self.assertIn(("SRC.IDX_T1", "INDEX"), result.included_nodes)
        self.assertIn(("SRC.CK_T1", "CONSTRAINT"), result.included_nodes)
        self.assertIn(("SRC.TRG_T1", "TRIGGER"), result.included_nodes)
        self.assertIn(("SRC.PKG1", "PACKAGE"), result.included_nodes)
        self.assertIn(("SRC.PKG1", "PACKAGE BODY"), result.included_nodes)
        self.assertIn(("SRC.SEQ1", "SEQUENCE"), result.included_nodes)
        self.assertIn(("SRC.UNRELATED", "TABLE"), result.excluded_nodes)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = sdr.export_source_scope_detail(result, Path(tmpdir), "20260325_120000")
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
            self.assertIn("ROOT_REMAP|VIEW|SRC.V1|EXPLICIT_REMAP_ROOT", content)
            self.assertIn("FILTERED_OUT|TABLE|SRC.UNRELATED|OUTSIDE_REMAP_ROOT_CLOSURE", content)

    def test_build_source_scope_closure_with_explicit_trigger_keep(self):
        source_objects = {
            "SRC.TRG_T2": {"TRIGGER"},
            "SRC.T2": {"TABLE"},
            "SRC.IDX_T2": {"INDEX"},
        }
        object_parent_map = {
            "SRC.TRG_T2": "SRC.T2",
            "SRC.IDX_T2": "SRC.T2",
        }
        keep_nodes, detail_rows, missing = sdr.build_scoped_trigger_keep_nodes(
            {"SRC.TRG_T2"},
            source_objects,
            object_parent_map,
        )
        self.assertEqual(missing, [])
        result = sdr.build_source_scope_closure(
            source_objects,
            {},
            object_parent_map,
            set(),
            explicit_trigger_keep_nodes=keep_nodes,
            explicit_trigger_detail_rows=detail_rows,
            mode="remap_root_closure",
        )
        self.assertIn(("SRC.TRG_T2", "TRIGGER"), result.included_nodes)
        self.assertIn(("SRC.T2", "TABLE"), result.included_nodes)
        self.assertIn(("SRC.IDX_T2", "INDEX"), result.included_nodes)
        self.assertFalse(result.excluded_nodes)

    def test_build_source_scope_closure_full_source_keeps_all(self):
        source_objects = {
            "SRC.T1": {"TABLE"},
            "SRC.V1": {"VIEW"},
        }
        result = sdr.build_source_scope_closure(
            source_objects,
            {},
            {},
            set(),
            mode="full_source",
        )
        self.assertEqual(result.included_nodes, frozenset())
        self.assertEqual(result.excluded_nodes, frozenset())
        self.assertEqual(result.detail_rows, ())
        self.assertEqual(sdr.build_source_scope_diagnostics(result), [])

    def test_build_source_scope_diagnostics(self):
        result = sdr.ScopedSourceScopeResult(
            mode="remap_root_closure",
            root_seed_nodes=frozenset({("SRC.T1", "TABLE")}),
            explicit_trigger_nodes=frozenset({("SRC.TRG1", "TRIGGER")}),
            included_nodes=frozenset({("SRC.T1", "TABLE"), ("SRC.TRG1", "TRIGGER")}),
            excluded_nodes=frozenset({("SRC.T2", "TABLE")}),
            detail_rows=(),
        )
        diagnostics = sdr.build_source_scope_diagnostics(result)
        self.assertTrue(any("source_object_scope_mode=remap_root_closure" in item for item in diagnostics))
        self.assertTrue(any("roots=1" in item and "filtered_out=1" in item for item in diagnostics))

    def test_build_non_table_trigger_detail_rows_respects_scoped_source_objects(self):
        oracle_meta = self._make_oracle_meta()._replace(non_table_triggers=(
            sdr.NonTableTriggerInfo(
                owner="SRC",
                trigger_name="TRG_KEEP",
                trigger_type="BEFORE EVENT",
                triggering_event="DROP",
                base_object_type="DATABASE",
                table_owner="",
                table_name="",
                status="ENABLED",
            ),
            sdr.NonTableTriggerInfo(
                owner="SRC",
                trigger_name="TRG_DROP",
                trigger_type="BEFORE EVENT",
                triggering_event="DROP",
                base_object_type="DATABASE",
                table_owner="",
                table_name="",
                status="ENABLED",
            ),
        ))
        rows = sdr.build_non_table_trigger_detail_rows(
            oracle_meta,
            {"SRC.TRG_KEEP": {"TRIGGER": "TGT.TRG_KEEP"}},
            source_objects={"SRC.TRG_KEEP": {"TRIGGER"}},
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].source_trigger, "SRC.TRG_KEEP")

    def test_check_dependencies_against_ob_scoped_filters_unmanaged_unexpected(self):
        ob_meta = self._make_ob_meta()._replace(objects_by_type={
            "VIEW": {"TGT.V1", "TGT.UNRELATED_V"},
            "TABLE": {"TGT.T1", "TGT.UNRELATED_T"},
        })
        expected_pairs = {
            ("TGT.V1", "VIEW", "TGT.T1", "TABLE"),
        }
        actual_pairs = {
            ("TGT.V1", "VIEW", "TGT.T1", "TABLE"),
            ("TGT.UNRELATED_V", "VIEW", "TGT.UNRELATED_T", "TABLE"),
        }
        report = sdr.check_dependencies_against_ob(
            expected_pairs,
            actual_pairs,
            [],
            ob_meta,
            managed_target_objects={"TGT.V1", "TGT.T1"},
        )
        self.assertEqual(report["missing"], [])
        self.assertEqual(report["unexpected"], [])

    def test_extract_view_dependencies_with_subquery(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM (SELECT * FROM T1) t\n"
            "JOIN B.T2 b ON t.ID=b.ID\n"
        )
        deps = sdr.extract_view_dependencies(ddl, default_schema="A")
        self.assertIn("A.T1", deps)
        self.assertIn("B.T2", deps)

    def test_extract_view_dependencies_with_deep_nested_subquery(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM (\n"
            "  SELECT * FROM (\n"
            "    SELECT * FROM T1\n"
            "  ) x\n"
            ") t\n"
            "JOIN B.T2 b ON t.ID = b.ID\n"
        )
        deps = sdr.extract_view_dependencies(ddl, default_schema="A")
        self.assertIn("A.T1", deps)
        self.assertIn("B.T2", deps)

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
        rewritten = sdr.remap_view_dependencies(ddl, "A", "V", remap_rules, full_mapping)
        self.assertIn("X.T1", rewritten.upper())
        self.assertIn("Y.T2", rewritten.upper())

    def test_should_skip_system_notnull_from_constraint_compare_only_when_enabled(self):
        self.assertTrue(
            sdr.should_skip_system_notnull_from_constraint_compare(
                "SYS_C1",
                '"C1" IS NOT NULL',
                "ENABLED",
            )
        )
        self.assertFalse(
            sdr.should_skip_system_notnull_from_constraint_compare(
                "SYS_C1",
                '"C1" IS NOT NULL',
                "DISABLED",
            )
        )
        self.assertFalse(
            sdr.should_skip_system_notnull_from_constraint_compare(
                "SYS_C1",
                '"C1" IS NOT NULL',
                None,
            )
        )

    def test_remap_view_dependencies_rewrites_quoted_qualified(self):
        ddl = (
            'CREATE OR REPLACE VIEW A.V AS\n'
            'SELECT * FROM "A"."T1" t\n'
            'JOIN "B"."T2" b ON t.ID=b.ID\n'
        )
        full_mapping = {
            "A.T1": {"TABLE": "X.T1"},
            "B.T2": {"TABLE": "Y.T2"},
        }
        rewritten = sdr.remap_view_dependencies(ddl, "A", "V", {}, full_mapping)
        self.assertIn('"X"."T1"', rewritten.upper())
        self.assertIn('"Y"."T2"', rewritten.upper())

    def test_remap_view_dependencies_does_not_double_rewrite_qualified_targets(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM A.X a\n"
            "JOIN B.X b ON a.ID=b.ID\n"
        )
        full_mapping = {
            "A.X": {"TABLE": "B.X"},
            "B.X": {"TABLE": "C.X"},
        }
        rewritten = sdr.remap_view_dependencies(ddl, "A", "V", {}, full_mapping)
        upper = rewritten.upper()
        self.assertIn("FROM B.X A", upper)
        self.assertIn("JOIN C.X B", upper)
        self.assertNotIn("FROM C.X A", upper)

    def test_remap_view_dependencies_resolves_public_synonym(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM SYN1\n"
        )
        full_mapping = {"SRC.T1": {"TABLE": "TGT.T1"}}
        synonym_meta = {
            ("PUBLIC", "SYN1"): sdr.SynonymMeta("PUBLIC", "SYN1", "SRC", "T1", None)
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "A",
            "V",
            {},
            full_mapping,
            synonym_meta=synonym_meta
        )
        self.assertIn("TGT.T1", rewritten.upper())

    def test_remap_view_dependencies_fallback_to_managed_public_synonym_object(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM SYN1\n"
        )
        full_mapping = {"PUBLIC.SYN1": {"SYNONYM": "TGT.SYN1"}}
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "A",
            "V",
            {},
            full_mapping,
            synonym_meta={},
        )
        self.assertIn("TGT.SYN1", rewritten.upper())

    def test_remap_view_dependencies_logs_unresolved_dependency_when_no_safe_mapping(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM SYN1\n"
        )
        with mock.patch.object(sdr.log, "warning") as m_warning:
            rewritten = sdr.remap_view_dependencies(
                ddl,
                "A",
                "V",
                {},
                {},
                synonym_meta={},
            )
        self.assertIn("FROM SYN1", rewritten.upper())
        self.assertTrue(m_warning.called)

    def test_remap_view_dependencies_resolves_private_synonym(self):
        ddl = (
            "CREATE OR REPLACE VIEW LIFELOGTMP.V1 AS\n"
            "SELECT * FROM LIFELOGTMP.BENEFICIARY_INFO_DELETE\n"
        )
        synonym_meta = {
            ("LIFELOGTMP", "BENEFICIARY_INFO_DELETE"): sdr.SynonymMeta(
                "LIFELOGTMP",
                "BENEFICIARY_INFO_DELETE",
                "LIFEDATA",
                "BENEFICIARY_INFO_DELETE",
                None,
            )
        }
        remap_rules = {
            "LIFEDATA.BENEFICIARY_INFO_DELETE": "PASDATA.BENEFICIARY_INFO_DELETE"
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "LIFELOGTMP",
            "V1",
            remap_rules,
            {},
            synonym_meta=synonym_meta
        )
        self.assertIn("PASDATA.BENEFICIARY_INFO_DELETE", rewritten.upper())
        self.assertNotIn("FROM LIFELOGTMP.BENEFICIARY_INFO_DELETE", rewritten.upper())

    def test_remap_view_dependencies_prefers_synonym_terminal_target(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM A.SYN1\n"
        )
        full_mapping = {
            "A.SYN1": {"SYNONYM": "B.SYN1"},
            "A.T1": {"TABLE": "TGT.T1"},
        }
        synonym_meta = {
            ("A", "SYN1"): sdr.SynonymMeta("A", "SYN1", "A", "T1", None),
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "A",
            "V",
            {},
            full_mapping,
            synonym_meta=synonym_meta,
        )
        self.assertIn("TGT.T1", rewritten.upper())
        self.assertNotIn("B.SYN1", rewritten.upper())

    def test_remap_view_dependencies_synonym_to_view_keeps_view_target(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM A.SYN_VIEW\n"
        )
        full_mapping = {
            "A.V_BASE": {"VIEW": "TGT.V_BASE"},
        }
        synonym_meta = {
            ("A", "SYN_VIEW"): sdr.SynonymMeta("A", "SYN_VIEW", "A", "V_BASE", None),
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "A",
            "V",
            {},
            full_mapping,
            synonym_meta=synonym_meta,
        )
        self.assertIn("TGT.V_BASE", rewritten.upper())

    def test_remap_view_dependencies_prefers_explicit_rule_over_identity_mapping(self):
        ddl = (
            "CREATE OR REPLACE VIEW LIFELOGTMP.V2 AS\n"
            "SELECT * FROM LIFELOGTMP.BENEFICIARY_INFO_DELETE\n"
        )
        # 模拟 full_object_mapping 因冲突回退为 1:1，但 remap_rules 里存在显式映射
        full_mapping = {
            "LIFELOGTMP.BENEFICIARY_INFO_DELETE": {
                "TABLE": "LIFELOGTMP.BENEFICIARY_INFO_DELETE"
            }
        }
        remap_rules = {
            "LIFELOGTMP.BENEFICIARY_INFO_DELETE": "PASDATA.BENEFICIARY_INFO_DELETE"
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "LIFELOGTMP",
            "V2",
            remap_rules,
            full_mapping
        )
        self.assertIn("PASDATA.BENEFICIARY_INFO_DELETE", rewritten.upper())
        self.assertNotIn("FROM LIFELOGTMP.BENEFICIARY_INFO_DELETE", rewritten.upper())

    def test_remap_synonym_target_prefers_explicit_rule_over_non_identity_mapping(self):
        ddl = 'CREATE OR REPLACE SYNONYM "A"."S1" FOR SRC.POL_INFO;'
        full_mapping = {
            "SRC.POL_INFO": {"VIEW": "UWSDATA.POL_INFO_VW"},
        }
        remap_rules = {
            "SRC.POL_INFO": "UWSDATA.POL_INFO",
        }
        rewritten = sdr.remap_synonym_target(ddl, remap_rules, full_mapping)
        upper = rewritten.upper()
        self.assertIn("FOR UWSDATA.POL_INFO;", upper)
        self.assertNotIn("POL_INFO_VW", upper)

    def test_remap_view_dependencies_avoids_implicit_object_rename(self):
        ddl = (
            "CREATE OR REPLACE VIEW SRC.V AS\n"
            "SELECT * FROM SRC.T1 A\n"
        )
        # 模拟 full_object_mapping 出现了“同名依赖被映射到不同对象名”的情况，
        # 未显式 remap 时，依赖重写只允许改 schema，不改对象名。
        full_mapping = {
            "SRC.T1": {"VIEW": "TGT.T1_VW"},
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "SRC",
            "V",
            {},
            full_mapping,
        )
        upper = rewritten.upper()
        self.assertIn("FROM TGT.T1 A", upper)
        self.assertNotIn("TGT.T1_VW", upper)

    def test_remap_view_dependencies_allows_explicit_object_rename(self):
        ddl = (
            "CREATE OR REPLACE VIEW SRC.V AS\n"
            "SELECT * FROM SRC.T1 A\n"
        )
        full_mapping = {
            "SRC.T1": {"VIEW": "TGT.T1_VW"},
        }
        remap_rules = {
            "SRC.T1": "TGT.T1_VW",
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "SRC",
            "V",
            remap_rules,
            full_mapping,
        )
        self.assertIn("TGT.T1_VW", rewritten.upper())

    def test_remap_view_dependencies_explicit_rule_overrides_non_identity_mapping(self):
        ddl = (
            "CREATE OR REPLACE VIEW LIFEDATA.POL_INFO_VW AS\n"
            "SELECT * FROM LIFEDATA.POL_INFO A\n"
        )
        # 自动映射命中了 VIEW 目标名（*_VW），但显式 remap 要求使用 POL_INFO。
        full_mapping = {
            "LIFEDATA.POL_INFO": {"VIEW": "UWSDATA.POL_INFO_VW"},
        }
        remap_rules = {
            "LIFEDATA.POL_INFO": "UWSDATA.POL_INFO",
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "LIFEDATA",
            "POL_INFO_VW",
            remap_rules,
            full_mapping,
        )
        upper = rewritten.upper()
        self.assertIn("FROM UWSDATA.POL_INFO A", upper)
        self.assertNotIn("UWSDATA.POL_INFO_VW A", upper)

    def test_remap_view_dependencies_explicit_rule_beats_same_name_synonym(self):
        ddl = (
            "CREATE OR REPLACE VIEW LIFEDATA.PERST_TRAIL_BVW AS\n"
            "SELECT * FROM LIFEDATA.PERST_TRAIL A\n"
        )
        # 依赖对象存在“同名同义词 -> *_VW”链路时，显式 remap 必须优先，
        # 否则会把 TABLE 误改写成 VIEW 名称。
        full_mapping = {
            "LIFEDATA.PERST_TRAIL": {"VIEW": "LCSDATA.PERST_TRAIL_VW"},
            "LIFEDATA.PERST_TRAIL_VW": {"VIEW": "LCSDATA.PERST_TRAIL_VW"},
        }
        remap_rules = {
            "LIFEDATA.PERST_TRAIL": "LCSDATA.PERST_TRAIL",
        }
        synonym_meta = {
            ("LIFEDATA", "PERST_TRAIL"): sdr.SynonymMeta(
                "LIFEDATA",
                "PERST_TRAIL",
                "LIFEDATA",
                "PERST_TRAIL_VW",
                None,
            ),
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "LIFEDATA",
            "PERST_TRAIL_BVW",
            remap_rules,
            full_mapping,
            synonym_meta=synonym_meta,
        )
        upper = rewritten.upper()
        self.assertIn("FROM LCSDATA.PERST_TRAIL A", upper)
        self.assertNotIn("PERST_TRAIL_VW A", upper)

    def test_remap_view_dependencies_local_object_beats_public_synonym(self):
        ddl = (
            "CREATE OR REPLACE VIEW SRC.V_LOCAL AS\n"
            "SELECT * FROM T_LOCAL A\n"
        )
        full_mapping = {
            "SRC.T_LOCAL": {"TABLE": "TGT.T_LOCAL"},
            "PUB.T_LOCAL": {"TABLE": "PUB.T_LOCAL"},
        }
        synonym_meta = {
            ("PUBLIC", "T_LOCAL"): sdr.SynonymMeta(
                owner="PUBLIC",
                name="T_LOCAL",
                table_owner="PUB",
                table_name="T_LOCAL",
                db_link=None,
            ),
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "SRC",
            "V_LOCAL",
            {},
            full_mapping,
            synonym_meta=synonym_meta,
        )
        upper = rewritten.upper()
        self.assertIn("FROM TGT.T_LOCAL A", upper)
        self.assertNotIn("FROM PUB.T_LOCAL A", upper)

    def test_remap_view_dependencies_fallback_uses_dependency_map(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM (SELECT * FROM (SELECT * FROM T1) t1) t2\n"
        )
        full_mapping = {"A.T1": {"TABLE": "X.T1"}}
        view_dep_map = {("A", "V"): {"A.T1"}}
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "A",
            "V",
            {},
            full_mapping,
            view_dependency_map=view_dep_map
        )
        self.assertIn("X.T1", rewritten.upper())

    def test_remap_view_dependencies_skips_alias_rewrite(self):
        ddl = (
            "CREATE OR REPLACE VIEW SRC.V AS\n"
            "SELECT t.FCD, t.FCU FROM SRC.POL_INFO T\n"
        )
        full_mapping = {
            "SRC.POL_INFO": {"TABLE": "TGT.POL_INFO"},
            "SRC.T": {"TABLE": "TGT.T"}
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "SRC",
            "V",
            {},
            full_mapping
        )
        self.assertIn("TGT.POL_INFO T", rewritten.upper())
        self.assertNotIn("TGT.T", rewritten.upper())

    def test_remap_view_dependencies_skips_derived_alias(self):
        ddl = (
            "CREATE OR REPLACE VIEW SRC.V AS\n"
            "SELECT t.FCD FROM (SELECT * FROM SRC.POL_INFO) T\n"
        )
        full_mapping = {
            "SRC.POL_INFO": {"TABLE": "TGT.POL_INFO"},
            "SRC.T": {"TABLE": "TGT.T"}
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "SRC",
            "V",
            {},
            full_mapping
        )
        self.assertIn("TGT.POL_INFO", rewritten.upper())
        self.assertNotIn("TGT.T", rewritten.upper())

    def test_remap_view_dependencies_realistic_alias_case(self):
        ddl = (
            "CREATE OR REPLACE VIEW LIFEDATA.V_POL AS\n"
            "SELECT t.FCD, t.FCU\n"
            "FROM UWSDATA.POL_INFO T, LCSDATA.CHILD_REGION_CODE_SYNCH r1, "
            "LCSDATA.REGION_CODE_TBL b1\n"
            "WHERE t.lcd >= SYSDATE - 7\n"
            "AND business_src IN ('D', 'W')"
        )
        full_mapping = {
            "UWSDATA.POL_INFO": {"TABLE": "LIFEDATA.POL_INFO"},
            "LCSDATA.CHILD_REGION_CODE_SYNCH": {"TABLE": "LIFEDATA.CHILD_REGION_CODE_SYNCH"},
            "LCSDATA.REGION_CODE_TBL": {"TABLE": "LIFEDATA.REGION_CODE_TBL"},
            "LIFEDATA.T": {"TABLE": "LIFEDATA.T"}
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "LIFEDATA",
            "V_POL",
            {},
            full_mapping
        )
        upper = rewritten.upper()
        self.assertIn("LIFEDATA.POL_INFO T", upper)
        self.assertIn("LIFEDATA.CHILD_REGION_CODE_SYNCH R1", upper)
        self.assertIn("LIFEDATA.REGION_CODE_TBL B1", upper)
        self.assertNotIn("LIFEDATA.T", upper)

    def test_remap_view_dependencies_unqualified_from_only(self):
        ddl = (
            "CREATE OR REPLACE VIEW SRC.V AS\n"
            "SELECT t.C1 FROM T1 t WHERE t.C1 > 0\n"
        )
        full_mapping = {"SRC.T1": {"TABLE": "TGT.T1"}}
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "SRC",
            "V",
            {},
            full_mapping
        )
        upper = rewritten.upper()
        self.assertIn("TGT.T1 T", upper)
        self.assertIn("T.C1", upper)

    def test_remap_view_dependencies_subquery_alias_kept(self):
        ddl = (
            "CREATE OR REPLACE VIEW SRC.V AS\n"
            "SELECT x.C1 FROM (SELECT * FROM T1) x JOIN T2 y ON x.ID = y.ID"
        )
        full_mapping = {
            "SRC.T1": {"TABLE": "TGT.T1"},
            "SRC.T2": {"TABLE": "TGT.T2"},
            "SRC.X": {"TABLE": "TGT.X"}
        }
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "SRC",
            "V",
            {},
            full_mapping
        )
        upper = rewritten.upper()
        self.assertIn("FROM (SELECT * FROM TGT.T1) X", upper)
        self.assertIn("JOIN TGT.T2 Y", upper)
        self.assertNotIn("TGT.X", upper)

    def test_remap_view_dependencies_keeps_select_column_when_name_equals_table(self):
        ddl = (
            "CREATE OR REPLACE VIEW SRC.V AS\n"
            "SELECT (SELECT MAX(ID) FROM DUAL), T1, A.ID\n"
            "FROM SRC.T1 A\n"
        )
        full_mapping = {"SRC.T1": {"TABLE": "TGT.T1"}}
        rewritten = sdr.remap_view_dependencies(
            ddl,
            "SRC",
            "V",
            {},
            full_mapping
        )
        upper = rewritten.upper()
        self.assertIn("FROM TGT.T1 A", upper)
        self.assertIn("), T1, A.ID", upper)
        self.assertNotIn("), TGT.T1, A.ID", upper)

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

    def test_clean_view_ddl_for_oceanbase_with_audit_tracks_named_check_option_cleanup(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS SELECT 1 FROM DUAL "
            "WITH CHECK OPTION CONSTRAINT \"C1\""
        )
        cleaned, actions = sdr.clean_view_ddl_for_oceanbase_with_audit(ddl, ob_version="4.2.5.7")
        self.assertIn("WITH CHECK OPTION", cleaned.upper())
        self.assertNotIn("CONSTRAINT", cleaned.upper())
        names = {action.rule_name for action in actions}
        self.assertIn("clean_view_check_option_constraint_name", names)

    def test_oracle_get_view_text_without_check_option_column(self):
        class DummyCursor:
            def __init__(self):
                self.sql = ""
                self.params = None
            def execute(self, sql, params=None):
                self.sql = sql
                self.params = params
            def fetchone(self):
                return ("SELECT 1 FROM DUAL", "Y", "")
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False

        class DummyConn:
            def __init__(self, cur):
                self._cur = cur
            def cursor(self):
                return self._cur

        cur = DummyCursor()
        conn = DummyConn(cur)
        with mock.patch.object(sdr, "_oracle_dba_views_has_check_option", return_value=False):
            row = sdr.oracle_get_view_text(conn, "A", "V")
        self.assertEqual(row, ("SELECT 1 FROM DUAL", "Y", ""))
        self.assertIn("'' AS CHECK_OPTION", cur.sql.upper())

    def test_oracle_get_view_text_with_check_option_column(self):
        class DummyCursor:
            def __init__(self):
                self.sql = ""
                self.params = None
            def execute(self, sql, params=None):
                self.sql = sql
                self.params = params
            def fetchone(self):
                return ("SELECT 1 FROM DUAL", "N", "NONE")
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False

        class DummyConn:
            def __init__(self, cur):
                self._cur = cur
            def cursor(self):
                return self._cur

        cur = DummyCursor()
        conn = DummyConn(cur)
        with mock.patch.object(sdr, "_oracle_dba_views_has_check_option", return_value=True):
            row = sdr.oracle_get_view_text(conn, "A", "V")
        self.assertEqual(row, ("SELECT 1 FROM DUAL", "N", "NONE"))
        self.assertIn("CHECK_OPTION", cur.sql.upper())

    def test_clean_view_ddl_strips_trailing_constraint(self):
        ddl = "CREATE OR REPLACE VIEW A.V AS SELECT 1 FROM DUAL CONSTRAINT C1;"
        cleaned = sdr.clean_view_ddl_for_oceanbase(ddl, ob_version="4.2.5.7")
        self.assertNotIn("CONSTRAINT", cleaned.upper())

    def test_view_constraint_cleanup_auto_cleanable(self):
        ddl = (
            "CREATE OR REPLACE FORCE VIEW A.V\n"
            "(\n"
            "  C1,\n"
            "  C2,\n"
            "  CONSTRAINT PK_V PRIMARY KEY (C1) RELY DISABLE\n"
            ") AS SELECT 1 C1, 2 C2 FROM DUAL"
        )
        result = sdr.apply_view_constraint_cleanup(ddl, "auto")
        self.assertEqual(result.action, sdr.VIEW_CONSTRAINT_ACTION_CLEANED)
        self.assertNotIn("CONSTRAINT", result.cleaned_ddl.upper())
        self.assertIn("C1", result.cleaned_ddl.upper())
        self.assertIn("C2", result.cleaned_ddl.upper())

    def test_view_constraint_cleanup_auto_uncleanable(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V\n"
            "(\n"
            "  C1,\n"
            "  CONSTRAINT PK_V PRIMARY KEY (C1) ENABLE\n"
            ") AS SELECT 1 C1 FROM DUAL"
        )
        result = sdr.apply_view_constraint_cleanup(ddl, "auto")
        self.assertEqual(result.action, sdr.VIEW_CONSTRAINT_ACTION_UNCLEANABLE)
        self.assertIn("CONSTRAINT", result.cleaned_ddl.upper())

    def test_view_constraint_cleanup_force(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V\n"
            "(\n"
            "  C1,\n"
            "  CONSTRAINT PK_V PRIMARY KEY (C1) ENABLE\n"
            ") AS SELECT 1 C1 FROM DUAL"
        )
        result = sdr.apply_view_constraint_cleanup(ddl, "force")
        self.assertEqual(result.action, sdr.VIEW_CONSTRAINT_ACTION_CLEANED)
        self.assertNotIn("CONSTRAINT", result.cleaned_ddl.upper())

    def test_view_constraint_cleanup_off(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V\n"
            "(\n"
            "  C1,\n"
            "  CONSTRAINT PK_V PRIMARY KEY (C1) RELY DISABLE\n"
            ") AS SELECT 1 C1 FROM DUAL"
        )
        result = sdr.apply_view_constraint_cleanup(ddl, "off")
        self.assertEqual(result.action, sdr.VIEW_CONSTRAINT_ACTION_UNCLEANABLE)
        self.assertIn("CONSTRAINT", result.cleaned_ddl.upper())

    def test_normalize_ddl_for_ob_removes_using_index_name(self):
        ddl = (
            'ALTER TABLE "A"."T1" ADD CONSTRAINT "C1" UNIQUE ("C1") '
            'USING INDEX "IDX_C1" ENABLE;'
        )
        cleaned = sdr.normalize_ddl_for_ob(ddl)
        self.assertNotIn("USING INDEX", cleaned.upper())
        self.assertNotIn("IDX_C1", cleaned.upper())

    def test_clean_view_ddl_removes_force_and_editionable(self):
        ddl = "CREATE OR REPLACE FORCE EDITIONABLE VIEW A.V AS SELECT 1 FROM DUAL"
        cleaned = sdr.clean_view_ddl_for_oceanbase(ddl, ob_version="4.2.5.7")
        self.assertNotIn("FORCE VIEW", cleaned.upper())
        self.assertNotIn("EDITIONABLE", cleaned.upper())

    def test_clean_trigger_ddl_removes_editionable(self):
        ddl = (
            "CREATE OR REPLACE EDITIONABLE TRIGGER \"SRC\".\"TR1\"\n"
            "BEFORE INSERT ON SRC.T1\n"
            "FOR EACH ROW\n"
            "BEGIN\n"
            "  :new.id := 1;\n"
            "END;\n"
        )
        cleaned = sdr.apply_ddl_cleanup_rules(ddl, 'TRIGGER')
        self.assertIn("CREATE OR REPLACE TRIGGER", cleaned.upper())
        self.assertNotIn("EDITIONABLE", cleaned.upper())

    def test_is_ob_notnull_constraint_handles_tuple(self):
        self.assertTrue(sdr.is_ob_notnull_constraint(("ZZ_OBNOTNULL_1", "extra")))
        self.assertFalse(sdr.is_ob_notnull_constraint(("ZZ_NORMAL", "extra")))
        self.assertTrue(
            sdr.is_ob_notnull_constraint(
                "T1_OBCHECK_1761134849332186",
                '("C1" is not null)'
            )
        )
        self.assertFalse(
            sdr.is_ob_notnull_constraint(
                "T1_OBCHECK_1761134849332186",
                '("C1" > 0)'
            )
        )
        self.assertTrue(
            sdr.is_ob_notnull_constraint(
                "T1_OBCHECK_1761134849332186",
                '(("C1" is not null))'
            )
        )

    def test_normalize_extra_results_names_handles_tuple(self):
        extra_results = {
            "index_mismatched": [
                sdr.IndexMismatch(
                    table="A.T1",
                    missing_indexes={("IDX1", ("C1",))},
                    extra_indexes=set(),
                    detail_mismatch=[]
                )
            ],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="A.T1",
                    missing_constraints={("CK1", "X")},
                    extra_constraints=set(),
                    detail_mismatch=[],
                    downgraded_pk_constraints={("PK1", ("C1",))}
                )
            ],
            "sequence_mismatched": [
                sdr.SequenceMismatch(
                    src_schema="A",
                    tgt_schema="A",
                    missing_sequences={("SEQ1", "X")},
                    extra_sequences=set(),
                    note=None,
                    missing_mappings=[],
                    detail_mismatch=[]
                )
            ],
            "trigger_mismatched": [
                sdr.TriggerMismatch(
                    table="A.T1",
                    missing_triggers={("TR1", "X")},
                    extra_triggers=set(),
                    detail_mismatch=[]
                )
            ]
        }
        normalized = sdr.normalize_extra_results_names(extra_results)
        self.assertEqual(normalized["index_mismatched"][0].missing_indexes, {"IDX1"})
        self.assertEqual(normalized["constraint_mismatched"][0].missing_constraints, {"CK1"})
        self.assertEqual(normalized["constraint_mismatched"][0].downgraded_pk_constraints, {"PK1"})
        self.assertEqual(normalized["sequence_mismatched"][0].missing_sequences, {"SEQ1"})
        self.assertEqual(normalized["trigger_mismatched"][0].missing_triggers, {"TR1"})

    def test_resolve_console_log_level_auto(self):
        self.assertEqual(sdr.resolve_console_log_level("AUTO", is_tty=True), logging.INFO)
        self.assertEqual(sdr.resolve_console_log_level("AUTO", is_tty=False), logging.WARNING)
        self.assertEqual(sdr.resolve_console_log_level("INFO", is_tty=False), logging.INFO)

    def test_export_report_index_summary_note(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir)
            entries = [sdr.ReportIndexEntry("REPORT", "report_123.txt", "-", "主报告")]
            output = sdr.export_report_index(entries, report_dir, "123", "summary")
            self.assertIsNotNone(output)
            content = Path(output).read_text(encoding="utf-8")
            self.assertIn("report_123.txt", content)
            self.assertIn("report_detail_mode=summary", content)

    def test_export_report_index_split_no_note(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir)
            entries = [sdr.ReportIndexEntry("REPORT", "report_123.txt", "-", "主报告")]
            output = sdr.export_report_index(entries, report_dir, "123", "split")
            self.assertIsNotNone(output)
            content = Path(output).read_text(encoding="utf-8")
            self.assertIn("report_123.txt", content)
            self.assertNotIn("report_detail_mode=summary", content)

    def test_export_report_index_includes_guide_note(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir)
            entries = [
                sdr.ReportIndexEntry("GUIDE", "report_123.txt", "-", "先看主报告"),
                sdr.ReportIndexEntry("REPORT", "report_123.txt", "-", "主报告"),
            ]
            output = sdr.export_report_index(entries, report_dir, "123", "split")
            self.assertIsNotNone(output)
            content = Path(output).read_text(encoding="utf-8")
            self.assertIn("GUIDE|report_123.txt|-|先看主报告", content)
            self.assertIn("先看主报告", content)

    def test_write_fixup_root_readme_highlights_sensitive_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "fixup_scripts"
            (base_dir / "table").mkdir(parents=True)
            (base_dir / "grants_revoke").mkdir(parents=True)
            (base_dir / "table_alter").mkdir(parents=True)
            report_dir = Path(tmpdir) / "reports"
            report_dir.mkdir(parents=True)
            output = sdr.write_fixup_root_readme(base_dir, report_dir, "123")
            self.assertIsNotNone(output)
            content = Path(output).read_text(encoding="utf-8")
            self.assertIn("README_FIRST", str(output))
            self.assertIn("table/ 是缺表 CREATE 脚本", content)
            self.assertIn("grants_revoke/", content)
            self.assertIn("run_fixup.py config.ini --smart-order --recompile", content)

    def test_notice_state_persists_seen_ids(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path, state = sdr.load_notice_state(tmpdir)
            notices = [sdr.RuntimeNotice("n1", "0.9.8.7", "标题", "内容")]
            unseen = sdr.select_unseen_notices(state, notices)
            self.assertEqual(len(unseen), 1)
            sdr.persist_seen_notices(state_path, state, "0.9.8.7", unseen)
            _state_path2, state2 = sdr.load_notice_state(tmpdir)
            unseen2 = sdr.select_unseen_notices(state2, notices)
            self.assertEqual(unseen2, [])

    def test_load_notice_state_backs_up_corrupted_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / ".comparator_notice_state.json"
            state_path.write_text("{bad-json", encoding="utf-8")
            resolved, state = sdr.load_notice_state(tmpdir)
            self.assertEqual(resolved, state_path)
            self.assertEqual(state["seen_notices"], {})
            backups = list(Path(tmpdir).glob(".comparator_notice_state.json.corrupted.*"))
            self.assertTrue(backups)

    def test_build_runtime_change_notices_by_run_context(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixup_dir = Path(tmpdir) / "fixup_scripts"
            fixup_dir.mkdir(parents=True)
            readme_path = fixup_dir / "README_FIRST.txt"
            readme_path.write_text("x", encoding="utf-8")
            report_path = Path(tmpdir) / "main_reports" / "run_1" / "report_123.txt"
            report_path.parent.mkdir(parents=True)
            report_path.write_text("x", encoding="utf-8")
            summary = sdr.RunSummary(
                start_time=sdr.datetime.now(),
                end_time=sdr.datetime.now(),
                total_seconds=1.0,
                phases=[],
                actions_done=[],
                actions_skipped=[],
                findings=[],
                attention=[],
                manual_actions=[],
                change_notices=[],
                next_steps=[
                    "如视图依赖复杂，可使用: python3 run_fixup.py config.ini --view-chain-autofix",
                    f"若存在 PUBLIC 扩权，先审 target_extra_grants_detail_*.txt，再决定是否执行 {fixup_dir / 'grants_revoke'}。",
                ],
            )
            ctx = sdr.RunSummaryContext(
                start_time=sdr.datetime.now(),
                start_perf=0.0,
                phase_durations={},
                phase_skip_reasons={},
                enabled_primary_types=set(),
                enabled_extra_types=set(),
                print_only_types=set(),
                total_checked=0,
                enable_dependencies_check=False,
                enable_comment_check=False,
                enable_grant_generation=True,
                enable_schema_mapping_infer=False,
                fixup_enabled=True,
                fixup_dir=str(fixup_dir),
                dependency_chain_file=None,
                view_chain_file=None,
                trigger_list_summary=None,
                report_start_perf=0.0,
            )
            notices = sdr.build_runtime_change_notices(
                settings={
                    "object_created_before": "20260303 150000",
                    "_fixup_root_readme_path": str(readme_path),
                    "_generated_fixup_dirs": ["table"],
                },
                summary=summary,
                ctx=ctx,
                report_file=report_path,
            )
            ids = {item.notice_id for item in notices}
            self.assertIn("embedded_report_guidance", ids)
            self.assertIn("fixup_root_readme", ids)
            self.assertIn("fixup_table_safe_gate", ids)
            self.assertIn("view_chain_autofix", ids)
            self.assertIn("public_grants_revoke_audit", ids)
            self.assertIn("object_created_before_scope", ids)

    def test_write_fixup_root_readme_uses_current_generated_dirs_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "fixup_scripts"
            (base_dir / "table").mkdir(parents=True)
            (base_dir / "view").mkdir(parents=True)
            report_dir = Path(tmpdir) / "reports"
            report_dir.mkdir(parents=True)

            output = sdr.write_fixup_root_readme(
                base_dir,
                report_dir,
                "123",
                generated_dirs=["view"],
            )

            self.assertIsNotNone(output)
            content = Path(output).read_text(encoding="utf-8")
            self.assertIn("- view/", content)
            self.assertNotIn("- table/", content)

    def test_write_fixup_root_readme_describes_sequence_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "fixup_scripts"
            (base_dir / "sequence_restart").mkdir(parents=True)
            report_dir = Path(tmpdir) / "reports"
            report_dir.mkdir(parents=True)

            output = sdr.write_fixup_root_readme(
                base_dir,
                report_dir,
                "123",
                generated_dirs=["sequence_restart"],
            )

            content = Path(output).read_text(encoding="utf-8")
            self.assertIn("sequence_restart/", content)
            self.assertIn("RESTART START WITH", content)

    def test_build_report_index_guide_entries_uses_explicit_readme_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "reports" / "run_1" / "report_123.txt"
            report_path.parent.mkdir(parents=True)
            report_path.write_text("x", encoding="utf-8")
            fixup_root = Path(tmpdir) / "nested" / "fixup_scripts"
            fixup_root.mkdir(parents=True)
            readme_path = fixup_root / "README_FIRST.txt"
            readme_path.write_text("x", encoding="utf-8")

            entries = sdr.build_report_index_guide_entries(
                report_path=report_path,
                manual_actions_path=None,
                fixup_root_readme_path=readme_path,
                migration_focus_path=None,
                unsupported_detail_path=None,
                table_presence_detail_path=None,
                target_extra_grant_detail_path=None,
                trigger_literal_alert_path=None,
                blocked_total=0,
                table_presence_risk_cnt=0,
                extra_public_grant_count=0,
                trigger_literal_alert_count=0,
            )

            self.assertTrue(any(entry.path.endswith("README_FIRST.txt") for entry in entries))

    def test_build_operator_action_rows_groups_manual_items(self):
        rows = sdr.build_operator_action_rows(
            report_dir=Path("/tmp/run_1"),
            report_timestamp="123",
            fixup_dir="fixup_scripts",
            blocked_total=3,
            table_presence_risk_cnt=2,
            unsupported_grant_count=5,
            deferred_grant_count=4,
            extra_public_grant_count=1,
            trigger_literal_alert_count=2,
            sequence_restart_generated_count=2,
            sequence_restart_unresolved_count=1,
            ddl_cleanup_semantic_rows=1,
            generated_fixup_dirs=["job", "schedule"],
            job_missing_count=2,
            schedule_missing_count=1,
        )
        categories = {row.category for row in rows}
        self.assertIn("DATA_RISK", categories)
        self.assertIn("UNSUPPORTED_OBJECT", categories)
        self.assertIn("UNSUPPORTED_GRANT", categories)
        self.assertIn("DEFERRED_GRANT", categories)
        self.assertIn("PUBLIC_REVOKE_REVIEW", categories)
        self.assertIn("TRIGGER_LITERAL_REVIEW", categories)
        self.assertIn("SEQUENCE_RESTART_REVIEW", categories)
        self.assertIn("SEQUENCE_RESTART_UNRESOLVED", categories)
        self.assertIn("DDL_SEMANTIC_REWRITE", categories)
        self.assertIn("JOB_SCHEDULE_MANUAL", categories)

    def test_export_manual_actions_required_writes_grouped_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [
                sdr.OperatorActionRow(
                    priority="BLOCKER",
                    stage="BEFORE_FIXUP",
                    category="DATA_RISK",
                    count=2,
                    default_behavior="REPORT_ONLY",
                    primary_artifact="table_data_presence_detail_123.txt",
                    related_fixup_dir="",
                    why="risk",
                    recommended_action="fix",
                )
            ]
            output = sdr.export_manual_actions_required(rows, Path(tmpdir), "123")
            self.assertIsNotNone(output)
            content = Path(output).read_text(encoding="utf-8")
            self.assertIn("# report_kind=MANUAL_ACTION_REQUIRED", content)
            self.assertIn("# schema_version=1", content)
            self.assertIn("PRIORITY|STAGE|CATEGORY|COUNT|DEFAULT_BEHAVIOR", content)
            self.assertIn("BLOCKER|BEFORE_FIXUP|DATA_RISK|2|REPORT_ONLY", content)

    def test_export_constraint_validate_deferred_detail_includes_scope_note(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [
                sdr.ConstraintValidateDeferredRow(
                    schema_name="OMS_USER",
                    table_name="T1",
                    constraint_name="FK_T1_PARENT",
                    constraint_type="R",
                    src_validated="VALIDATED",
                    applied_mode="safe_novalidate",
                    reason="safe_novalidate",
                    validate_sql='ALTER TABLE "OMS_USER"."T1" ENABLE VALIDATE CONSTRAINT "FK_T1_PARENT";',
                )
            ]
            output = sdr.export_constraint_validate_deferred_detail(rows, Path(tmpdir), "123")
            self.assertIsNotNone(output)
            content = Path(output).read_text(encoding="utf-8")
            self.assertIn("仅覆盖：缺失约束当前以 ENABLE NOVALIDATE 落地", content)
            self.assertIn("不会进入 constraint_validate_later/", content)
            self.assertIn("fixup/status/constraint/", content)

    def test_write_fixup_root_readme_points_to_manual_actions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "fixup_scripts"
            (base_dir / "grants_miss").mkdir(parents=True)
            (base_dir / "constraint_validate_later").mkdir(parents=True)
            (base_dir / "status").mkdir(parents=True)
            report_dir = Path(tmpdir) / "reports"
            report_dir.mkdir(parents=True)
            manual_path = report_dir / "manual_actions_required_123.txt"
            manual_path.write_text("x", encoding="utf-8")
            output = sdr.write_fixup_root_readme(
                base_dir,
                report_dir,
                "123",
                generated_dirs=["grants_miss", "constraint_validate_later", "status"],
                manual_actions_path=manual_path,
                unsupported_grant_count=3,
                deferred_grant_count=0,
            )
            content = Path(output).read_text(encoding="utf-8")
            self.assertIn(str(manual_path), content)
            self.assertIn("grants_miss/ 不是完整授权闭环", content)
            self.assertIn("先 NOVALIDATE 落地、最终需 VALIDATED", content)
            self.assertIn("恢复 ENABLE NOVALIDATE 也在这里", content)

    def test_build_report_index_guide_entries_prioritizes_manual_actions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "reports" / "run_1" / "report_123.txt"
            report_path.parent.mkdir(parents=True)
            report_path.write_text("x", encoding="utf-8")
            manual_path = report_path.parent / "manual_actions_required_123.txt"
            manual_path.write_text("x", encoding="utf-8")
            entries = sdr.build_report_index_guide_entries(
                report_path=report_path,
                manual_actions_path=manual_path,
                fixup_root_readme_path=None,
                migration_focus_path=None,
                unsupported_detail_path=None,
                table_presence_detail_path=None,
                target_extra_grant_detail_path=None,
                trigger_literal_alert_path=None,
                blocked_total=0,
                table_presence_risk_cnt=0,
                extra_public_grant_count=0,
                trigger_literal_alert_count=0,
            )
            self.assertEqual(entries[0].path, "manual_actions_required_123.txt")

    def test_export_indexes_blocked_detail_filters_dependency_unsupported(self):
        rows = [
            sdr.ObjectSupportReportRow(
                obj_type="INDEX",
                src_full="SRC.IDX1",
                tgt_full="TGT.IDX1",
                support_state="BLOCKED",
                reason_code="DEPENDENCY_UNSUPPORTED",
                reason="依赖不支持表",
                dependency="TGT.T1",
                action="先改造依赖表",
                detail="INDEX"
            ),
            sdr.ObjectSupportReportRow(
                obj_type="INDEX",
                src_full="SRC.IDX2",
                tgt_full="TGT.IDX2",
                support_state="BLOCKED",
                reason_code="OTHER",
                reason="其他原因",
                dependency="TGT.T2",
                action="先改造依赖表",
                detail="INDEX"
            ),
            sdr.ObjectSupportReportRow(
                obj_type="CONSTRAINT",
                src_full="SRC.CK1",
                tgt_full="TGT.CK1",
                support_state="BLOCKED",
                reason_code="DEPENDENCY_UNSUPPORTED",
                reason="依赖不支持表",
                dependency="TGT.T3",
                action="先改造依赖表",
                detail="CONSTRAINT"
            )
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = sdr.export_indexes_blocked_detail(rows, Path(tmpdir), "123")
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
            self.assertIn("IDX1", content)
            self.assertIn("TGT.T1", content)
            self.assertNotIn("IDX2", content)
            self.assertNotIn("TGT.T2", content)
            self.assertNotIn("CK1", content)

    def test_export_migration_focus_report_sections(self):
        missing_rows = [
            sdr.ObjectSupportReportRow(
                obj_type="VIEW",
                src_full="SRC.V1",
                tgt_full="TGT.V1",
                support_state=sdr.SUPPORT_STATE_SUPPORTED,
                reason_code="-",
                reason="-",
                dependency="-",
                action="FIXUP",
                detail="-"
            )
        ]
        unsupported_rows = [
            sdr.ObjectSupportReportRow(
                obj_type="INDEX",
                src_full="SRC.IDX1",
                tgt_full="TGT.IDX1",
                support_state=sdr.SUPPORT_STATE_BLOCKED,
                reason_code="DEPENDENCY_UNSUPPORTED",
                reason="依赖不支持表",
                dependency="TGT.T1",
                action="先改造依赖表",
                detail="INDEX"
            )
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = sdr.export_migration_focus_report(
                missing_rows,
                unsupported_rows,
                Path(tmpdir),
                "123"
            )
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
            self.assertIn("section=MISSING_SUPPORTED", content)
            self.assertIn("section=UNSUPPORTED_OR_BLOCKED", content)
            self.assertIn("SRC.V1", content)
            self.assertIn("SRC.IDX1", content)

    def test_export_missing_by_type_filters_supported(self):
        rows = [
            sdr.ObjectSupportReportRow(
                obj_type="VIEW",
                src_full="SRC.V1",
                tgt_full="TGT.V1",
                support_state=sdr.SUPPORT_STATE_SUPPORTED,
                reason_code="-",
                reason="-",
                dependency="-",
                action="FIXUP",
                detail="-"
            ),
            sdr.ObjectSupportReportRow(
                obj_type="VIEW",
                src_full="SRC.V2",
                tgt_full="TGT.V2",
                support_state=sdr.SUPPORT_STATE_BLOCKED,
                reason_code="DEPENDENCY_UNSUPPORTED",
                reason="依赖不支持表",
                dependency="TGT.T1",
                action="先改造依赖表",
                detail="VIEW"
            )
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = sdr.export_missing_by_type(rows, Path(tmpdir), "123")
            self.assertIn("VIEW", paths)
            content = Path(paths["VIEW"]).read_text(encoding="utf-8")
            self.assertIn("SRC.V1", content)
            self.assertNotIn("SRC.V2", content)

    def test_export_unsupported_by_type_includes_root_cause(self):
        rows = [
            sdr.ObjectSupportReportRow(
                obj_type="VIEW",
                src_full="SRC.V1",
                tgt_full="TGT.V1",
                support_state=sdr.SUPPORT_STATE_BLOCKED,
                reason_code="DEPENDENCY_UNSUPPORTED",
                reason="依赖不支持表",
                dependency="SRC.T1",
                action="先改造依赖表",
                detail="VIEW",
                root_cause="SRC.T1(SPE)"
            )
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = sdr.export_unsupported_by_type(rows, Path(tmpdir), "123")
            self.assertIn("VIEW", paths)
            content = Path(paths["VIEW"]).read_text(encoding="utf-8")
            self.assertIn("ROOT_CAUSE", content)
            self.assertIn("SRC.T1(SPE)", content)

    def test_export_triggers_temp_table_unsupported_detail_filters_reason(self):
        rows = [
            sdr.ObjectSupportReportRow(
                obj_type="TRIGGER",
                src_full="SRC.TRG_TEMP_BI",
                tgt_full="TGT.TRG_TEMP_BI",
                support_state=sdr.SUPPORT_STATE_UNSUPPORTED,
                reason_code=sdr.TRIGGER_TEMP_TABLE_UNSUPPORTED_REASON_CODE,
                reason=sdr.TRIGGER_TEMP_TABLE_UNSUPPORTED_REASON,
                dependency="TGT.T_TEMP",
                action="改造/不迁移",
                detail="TRIGGER",
                root_cause="SRC.T_TEMP(TEMP_TABLE)"
            ),
            sdr.ObjectSupportReportRow(
                obj_type="TRIGGER",
                src_full="SRC.TRG_OTHER",
                tgt_full="TGT.TRG_OTHER",
                support_state=sdr.SUPPORT_STATE_BLOCKED,
                reason_code="DEPENDENCY_UNSUPPORTED",
                reason="依赖不支持表",
                dependency="TGT.T_OTHER",
                action="先改造依赖表",
                detail="TRIGGER",
                root_cause="SRC.T_OTHER(SPE)"
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = sdr.export_triggers_temp_table_unsupported_detail(rows, Path(tmpdir), "123")
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
            self.assertIn("TRG_TEMP_BI", content)
            self.assertIn(sdr.TRIGGER_TEMP_TABLE_UNSUPPORTED_REASON_CODE, content)
            self.assertNotIn("TRG_OTHER", content)

    def test_export_sys_c_force_candidates_detail(self):
        rows = [
            sdr.SysCForceCandidateRow(
                source_schema="SRC",
                source_table="T1",
                target_schema="TGT",
                target_table="T1",
                sys_c_columns=("SYS_C0001$", "SYS_C0002$")
            )
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = sdr.export_sys_c_force_candidates_detail(
                rows,
                force_enabled=False,
                report_dir=Path(tmpdir),
                report_timestamp="123"
            )
            self.assertIsNotNone(path)
            content = Path(path).read_text(encoding="utf-8")
            self.assertIn("SYS_C FORCE 候选表明细", content)
            self.assertIn("FORCE_DISABLED_REPORT_ONLY", content)
            self.assertIn("SYS_C0001$", content)

    def test_enforce_schema_for_ddl_skips_duplicate(self):
        ddl = (
            "ALTER SESSION SET CURRENT_SCHEMA = A;\n"
            "CREATE OR REPLACE TRIGGER A.T1\n"
            "BEFORE INSERT ON A.T1\n"
            "BEGIN\n"
            "  :new.id := 1;\n"
            "END;\n"
        )
        enforced = sdr.enforce_schema_for_ddl(ddl, "A", "TRIGGER")
        self.assertEqual(
            enforced.upper().count("ALTER SESSION SET CURRENT_SCHEMA = A"),
            1
        )

    def test_sanitize_view_ddl_inline_comment_breaks_line(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS select a.COL1,--注释1 a.COL2,--注释2 from T a"
        )
        cleaned = sdr.sanitize_view_ddl(ddl, set())
        self.assertIn("--注释1\n", cleaned)
        self.assertIn("\n a.COL2", cleaned)

    def test_sanitize_view_ddl_inline_comment_breaks_line_with_parens(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS SELECT a.C1,--注释 (a.C2-a.C1) AS diff,"
            " --注释2 decode(a.flag,'Y',1,0) AS x FROM T a"
        )
        cleaned = sdr.sanitize_view_ddl(ddl, set())
        self.assertIn("--注释\n", cleaned)
        self.assertIn("\n (a.C2", cleaned)
        self.assertIn("\n decode", cleaned)

    def test_build_view_ddl_from_text_quotes_owner(self):
        ddl = sdr.build_view_ddl_from_text(
            "scott",
            "v1",
            "select 1 from dual",
            "",
            ""
        )
        self.assertIn('CREATE OR REPLACE VIEW "SCOTT"."V1" AS', ddl)

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

    def test_sanitize_plsql_punctuation_preserves_q_quote_with_inner_single_quote(self):
        ddl = (
            "CREATE OR REPLACE PROCEDURE P AS\n"
            "BEGIN\n"
            "  DBMS_OUTPUT.PUT_LINE(q'[John's，Book]');\n"
            "END;\n"
        )
        cleaned, count, _samples = sdr.sanitize_plsql_punctuation(ddl, "PROCEDURE")
        self.assertIn("q'[John's，Book]'", cleaned)
        self.assertEqual(count, 0)

    def test_clean_storage_clauses_keeps_following_clause_after_tablespace(self):
        ddl = 'CREATE TABLE T1 (C1 NUMBER) TABLESPACE "My TS" LOGGING PCTFREE 10;'
        cleaned = sdr.clean_storage_clauses(ddl)
        self.assertNotIn("TABLESPACE", cleaned.upper())
        self.assertIn("LOGGING", cleaned.upper())
        self.assertIn("PCTFREE 10", cleaned.upper())

    def test_detect_view_constraint_state_ignores_comment_keywords(self):
        item = 'CONSTRAINT CK1 CHECK (C1 > 0) DISABLE -- ENABLE for rollback'
        ok, state = sdr._detect_view_constraint_state(item)
        self.assertTrue(ok)
        self.assertEqual(state, "DISABLE")

    def test_mask_sql_for_scan_masks_double_quoted_identifiers_and_preserves_length(self):
        sql = 'CREATE VIEW V1 ("Col Name", "Another Col") AS SELECT \'x\' FROM T1'
        masked = sdr.mask_sql_for_scan(sql)
        self.assertEqual(len(masked), len(sql))
        self.assertNotIn('"Col Name"', masked)
        self.assertNotIn('"Another Col"', masked)
        self.assertNotIn("'x'", masked)

    def test_mask_sql_for_scan_masks_q_quote_with_internal_single_quote_and_preserves_length(self):
        sql = "SELECT q'[a'b,c]' AS txt, func(col) FROM dual"
        masked = sdr.mask_sql_for_scan(sql)
        self.assertEqual(len(masked), len(sql))
        self.assertNotIn("q'[a'b,c]'", masked)
        self.assertIn("func(col)", masked)

    def test_split_sql_list_items_keeps_q_quote_item_together(self):
        segment = "c1, CHECK (msg = q'[a,b,c]'), c3"
        items = sdr.split_sql_list_items(segment)
        self.assertEqual(items, ["c1", "CHECK (msg = q'[a,b,c]')", "c3"])

    def test_split_sql_list_items_keeps_q_quote_with_internal_single_quote_together(self):
        segment = "c1, CHECK (msg = q'[a'b,c]'), c3"
        items = sdr.split_sql_list_items(segment)
        self.assertEqual(items, ["c1", "CHECK (msg = q'[a'b,c]')", "c3"])

    def test_build_exist_check_sql_rejects_invalid_identifier(self):
        sql = sdr._build_exist_check_sql("TABLE", "SCOTT", "T1;DROP")
        self.assertIsNone(sql)

    def test_build_exist_check_sql_accepts_valid_identifier(self):
        sql = sdr._build_exist_check_sql("TABLE", "SCOTT", "T1")
        self.assertIn("FROM ALL_TABLES", sql)
        self.assertIn("OWNER='SCOTT'", sql)
        self.assertIn("TABLE_NAME='T1'", sql)

    def test_is_case_sensitive_identifier(self):
        self.assertFalse(sdr.is_case_sensitive_identifier("ABC_DEF"))
        self.assertTrue(sdr.is_case_sensitive_identifier("Abc_Def"))
        self.assertTrue(sdr.is_case_sensitive_identifier("mixedCase"))

    def test_should_ignore_case_sensitive_finding_public_synonym_owner_alias(self):
        self.assertTrue(
            sdr.should_ignore_case_sensitive_finding("__public", "SYN_PUB", "SYNONYM")
        )
        self.assertFalse(
            sdr.should_ignore_case_sensitive_finding("__public", "SynMixed", "SYNONYM")
        )

    def test_handle_case_sensitive_identifiers_warn_returns_findings(self):
        findings = sdr.handle_case_sensitive_identifiers(
            {("Abc", "Obj1", "TABLE")},
            "Oracle.DBA_OBJECTS",
            "warn",
        )
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].side, "SOURCE")
        self.assertEqual(findings[0].owner, "Abc")
        self.assertEqual(findings[0].object_name, "Obj1")
        self.assertEqual(findings[0].object_type, "TABLE")
        self.assertEqual(findings[0].mode, "warn")

    def test_handle_case_sensitive_identifiers_ignores_public_owner_alias_noise(self):
        findings = sdr.handle_case_sensitive_identifiers(
            {("__public", "SYN_PUB", "SYNONYM")},
            "OceanBase.DBA_OBJECTS",
            "warn",
        )
        self.assertEqual(findings, ())

    def test_handle_case_sensitive_identifiers_abort_raises(self):
        with self.assertRaises(sdr.FatalError):
            sdr.handle_case_sensitive_identifiers(
                {("Abc", "Obj1", "TABLE")},
                "OceanBase.DBA_OBJECTS",
                "abort",
            )

    def test_log_metadata_volume_warning_emits_warning(self):
        with mock.patch.object(sdr.log, "warning") as warn_mock:
            sdr.log_metadata_volume_warning(
                "ORACLE",
                table_count=sdr.METADATA_VOLUME_WARN_TABLES + 1,
                column_count=10,
                index_count=20,
                constraint_count=30,
                trigger_count=40,
            )
        self.assertTrue(warn_mock.called)

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
                    action_status=sdr.DDL_CLEAN_ACTION_APPLIED,
                    rule_name="rewrite_unsupported_table_oracle_types",
                    category=sdr.DDL_CLEAN_CATEGORY_SEMANTIC_REWRITE,
                    evidence_level=sdr.DDL_CLEAN_EVIDENCE_VERIFIED_UNSUPPORTED,
                    change_count=3,
                    note="Oracle 专有列类型已改写为 OB 兼容类型，属于语义改写。",
                    samples=["LONG -> CLOB", "LONG RAW -> BLOB"]
                )
            ]
            path = sdr.export_ddl_clean_report(rows, Path(tmpdir), "20240101")
            self.assertIsNotNone(path)
            output = Path(path).read_text(encoding="utf-8")
            self.assertIn("DDL 清理/改写明细", output)
            self.assertIn("A.PKG", output)
            self.assertIn("3", output)
            self.assertIn("rewrite_unsupported_table_oracle_types", output)
            self.assertIn("LONG -> CLOB", output)

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

    def test_clean_oracle_hints_removes_hint_with_star(self):
        ddl = "SELECT /*+ PARALLEL(T, 4*2) USE_HASH(*) */ * FROM T"
        cleaned = sdr.clean_oracle_hints(ddl)
        self.assertNotIn("PARALLEL(T, 4*2)", cleaned)
        self.assertNotIn("USE_HASH(*)", cleaned)
        self.assertIn("SELECT", cleaned)
        self.assertIn("FROM T", cleaned)

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

    def test_clean_xmltype_xmlschema_clause_preserves_comment_text(self):
        ddl = (
            "CREATE TABLE T_XML (\n"
            "  DOC XMLTYPE\n"
            ");\n"
            "-- XMLTYPE COLUMN DOC XMLSCHEMA 'http://example'\n"
        )
        cleaned = sdr.clean_xmltype_xmlschema_clause(ddl)
        self.assertIn("-- XMLTYPE COLUMN DOC XMLSCHEMA 'http://example'", cleaned)

    def test_clean_xmltype_xmlschema_clause_preserves_string_literal(self):
        ddl = (
            "BEGIN\n"
            "  v_sql := 'XMLTYPE COLUMN DOC XMLSCHEMA abc';\n"
            "END;"
        )
        cleaned = sdr.clean_xmltype_xmlschema_clause(ddl)
        self.assertIn("'XMLTYPE COLUMN DOC XMLSCHEMA abc'", cleaned)

    def test_clean_xmltype_xmlschema_clause_removes_real_clause(self):
        ddl = (
            "CREATE TABLE T_XML OF XMLTYPE\n"
            "XMLTYPE COLUMN DOC XMLSCHEMA \"http://example.com/schema.xsd\";\n"
        )
        cleaned = sdr.clean_xmltype_xmlschema_clause(ddl)
        self.assertNotIn("XMLTYPE COLUMN DOC XMLSCHEMA", cleaned.upper())

    def test_dependency_grants_add_grantable_for_view_owner(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
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
            column_privileges=[],
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
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
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
            column_privileges=[],
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

    def test_build_grant_plan_keeps_same_schema_view_dml_grant(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[
                sdr.OracleObjectPrivilege(
                    grantee="APP",
                    owner="APP",
                    object_name="V1",
                    object_type="VIEW",
                    privilege="UPDATE",
                    grantable=False
                ),
                sdr.OracleObjectPrivilege(
                    grantee="U3",
                    owner="APP",
                    object_name="V1",
                    object_type="VIEW",
                    privilege="UPDATE",
                    grantable=True
                ),
            ],
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
            interval_partitions={}
        )
        source_objects = {"APP.V1": {"VIEW"}, "APP.T1": {"TABLE"}}
        full_mapping = {
            "APP.V1": {"VIEW": "APP.V1"},
            "APP.T1": {"TABLE": "APP.T1"},
        }
        deps = [
            sdr.DependencyRecord(
                owner="APP",
                name="V1",
                object_type="VIEW",
                referenced_owner="APP",
                referenced_name="T1",
                referenced_type="TABLE"
            )
        ]
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
            source_schema_set={"APP"},
            remap_conflicts=None,
            synonym_meta={},
            dependencies=deps
        )
        self.assertIn(
            sdr.ObjectGrantEntry("UPDATE", "APP.V1", True),
            grant_plan.object_grants.get("U3", set())
        )
        self.assertFalse(any(
            row.reason == sdr.VIEW_DML_GRANT_PREREQ_UNVERIFIED
            for row in grant_plan.filtered_grants
        ))

    def test_build_grant_plan_filters_cross_schema_view_dml_without_provable_prereq(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[
                sdr.OracleObjectPrivilege(
                    grantee="VOWNER",
                    owner="TOWNER",
                    object_name="T1",
                    object_type="TABLE",
                    privilege="SELECT",
                    grantable=True
                ),
                sdr.OracleObjectPrivilege(
                    grantee="U3",
                    owner="VOWNER",
                    object_name="V1",
                    object_type="VIEW",
                    privilege="UPDATE",
                    grantable=True
                ),
            ],
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
            interval_partitions={}
        )
        source_objects = {"VOWNER.V1": {"VIEW"}, "TOWNER.T1": {"TABLE"}}
        full_mapping = {
            "VOWNER.V1": {"VIEW": "VOWNER.V1"},
            "TOWNER.T1": {"TABLE": "TOWNER.T1"},
        }
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
        self.assertNotIn("U3", grant_plan.object_grants)
        self.assertTrue(any(
            row.grantee == "U3"
            and row.privilege == "UPDATE"
            and row.object_full == "VOWNER.V1"
            and row.reason == sdr.VIEW_DML_GRANT_PREREQ_UNVERIFIED
            for row in grant_plan.filtered_grants
        ))

    def test_build_grant_plan_allows_cross_schema_view_dml_with_grantable_prereq(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[
                sdr.OracleObjectPrivilege(
                    grantee="VOWNER",
                    owner="TOWNER",
                    object_name="T1",
                    object_type="TABLE",
                    privilege="UPDATE",
                    grantable=True
                ),
                sdr.OracleObjectPrivilege(
                    grantee="U3",
                    owner="VOWNER",
                    object_name="V1",
                    object_type="VIEW",
                    privilege="UPDATE",
                    grantable=True
                ),
            ],
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
            interval_partitions={}
        )
        source_objects = {"VOWNER.V1": {"VIEW"}, "TOWNER.T1": {"TABLE"}}
        full_mapping = {
            "VOWNER.V1": {"VIEW": "VOWNER.V1"},
            "TOWNER.T1": {"TABLE": "TOWNER.T1"},
        }
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
        self.assertIn(
            sdr.ObjectGrantEntry("UPDATE", "VOWNER.V1", True),
            grant_plan.object_grants.get("U3", set())
        )
        self.assertFalse(any(
            row.reason == sdr.VIEW_DML_GRANT_PREREQ_UNVERIFIED
            for row in grant_plan.filtered_grants
        ))

    def test_build_grant_plan_stops_view_dml_prereq_at_cross_schema_view_boundary(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[
                sdr.OracleObjectPrivilege(
                    grantee="VOWNER",
                    owner="XOWNER",
                    object_name="VEXT",
                    object_type="VIEW",
                    privilege="UPDATE",
                    grantable=True
                ),
                sdr.OracleObjectPrivilege(
                    grantee="U3",
                    owner="VOWNER",
                    object_name="V1",
                    object_type="VIEW",
                    privilege="UPDATE",
                    grantable=True
                ),
            ],
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
            interval_partitions={}
        )
        source_objects = {
            "VOWNER.V1": {"VIEW"},
            "VOWNER.VLOCAL": {"VIEW"},
            "XOWNER.VEXT": {"VIEW"},
            "XOWNER.TBASE": {"TABLE"},
        }
        full_mapping = {
            "VOWNER.V1": {"VIEW": "VOWNER.V1"},
            "VOWNER.VLOCAL": {"VIEW": "VOWNER.VLOCAL"},
            "XOWNER.VEXT": {"VIEW": "XOWNER.VEXT"},
            "XOWNER.TBASE": {"TABLE": "XOWNER.TBASE"},
        }
        deps = [
            sdr.DependencyRecord(
                owner="VOWNER",
                name="V1",
                object_type="VIEW",
                referenced_owner="VOWNER",
                referenced_name="VLOCAL",
                referenced_type="VIEW"
            ),
            sdr.DependencyRecord(
                owner="VOWNER",
                name="VLOCAL",
                object_type="VIEW",
                referenced_owner="XOWNER",
                referenced_name="VEXT",
                referenced_type="VIEW"
            ),
            sdr.DependencyRecord(
                owner="XOWNER",
                name="VEXT",
                object_type="VIEW",
                referenced_owner="XOWNER",
                referenced_name="TBASE",
                referenced_type="TABLE"
            ),
        ]
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
            source_schema_set={"VOWNER", "XOWNER"},
            remap_conflicts=None,
            synonym_meta={},
            dependencies=deps
        )
        self.assertIn(
            sdr.ObjectGrantEntry("UPDATE", "VOWNER.V1", True),
            grant_plan.object_grants.get("U3", set())
        )
        self.assertFalse(any(
            row.reason == sdr.VIEW_DML_GRANT_PREREQ_UNVERIFIED
            for row in grant_plan.filtered_grants
        ))

    def test_build_grant_plan_allows_debug_with_capability_alias(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[
                sdr.OracleObjectPrivilege(
                    grantee="PUBLIC",
                    owner="S1",
                    object_name="P1",
                    object_type="PROCEDURE",
                    privilege="DEBUG",
                    grantable=False
                )
            ],
            column_privileges=[],
            sys_privileges=[],
            role_privileges=[],
            role_metadata={},
            system_privilege_map=set(),
            table_privilege_map={"DEBUG"},
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        source_objects = {"S1.P1": {"PROCEDURE"}}
        full_mapping = {"S1.P1": {"PROCEDURE": "S1.P1"}}
        capability = sdr.GrantCapabilityLibrary(
            object_decisions={
                ("DEBUG", "PROCEDURE"): sdr.GrantCapabilityDecision(
                    support_status=sdr.GRANT_CAPABILITY_SUPPORT_SUPPORTED_ALIAS,
                    decision=sdr.GRANT_CAPABILITY_DECISION_ALLOW,
                    target_catalog_privilege="OTHERS",
                )
            },
            object_alias_to_logical={("PROCEDURE", "OTHERS"): "DEBUG"},
            object_logical_to_catalog={("DEBUG", "PROCEDURE"): "OTHERS"},
            known_logical_object_privileges={"DEBUG"},
        )
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
            source_schema_set={"S1"},
            remap_conflicts=None,
            synonym_meta={},
            capability_library=capability,
        )
        self.assertIn(
            sdr.ObjectGrantEntry("DEBUG", "S1.P1", False),
            grant_plan.object_grants.get("PUBLIC", set())
        )
        self.assertEqual(grant_plan.object_target_types.get("S1.P1"), "PROCEDURE")

    def test_build_grant_plan_maps_select_catalog_role_to_ob_catalog_role(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[
                sdr.OracleObjectPrivilege(
                    grantee="SELECT_CATALOG_ROLE",
                    owner="S1",
                    object_name="T1",
                    object_type="TABLE",
                    privilege="SELECT",
                    grantable=False
                )
            ],
            column_privileges=[],
            sys_privileges=[],
            role_privileges=[
                sdr.OracleRolePrivilege(grantee="U1", role="SELECT_CATALOG_ROLE", admin_option=False),
            ],
            role_metadata={
                "SELECT_CATALOG_ROLE": sdr.OracleRoleInfo(
                    role="SELECT_CATALOG_ROLE",
                    authentication_type="NONE",
                    password_required=False,
                    oracle_maintained=True,
                )
            },
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
            source_schema_set={"S1", "U1"},
            remap_conflicts=None,
            synonym_meta={},
            ob_roles={"OB_CATALOG_ROLE"},
            ob_users={"U1"},
            include_oracle_maintained_roles=False
        )

        self.assertIn(
            sdr.RoleGrantEntry("OB_CATALOG_ROLE", False),
            grant_plan.role_privs.get("U1", set())
        )
        self.assertNotIn(
            sdr.RoleGrantEntry("SELECT_CATALOG_ROLE", False),
            grant_plan.role_privs.get("U1", set())
        )
        self.assertIn(
            sdr.ObjectGrantEntry("SELECT", "S1.T1", False),
            grant_plan.object_grants.get("OB_CATALOG_ROLE", set())
        )
        self.assertNotIn("SELECT_CATALOG_ROLE", grant_plan.object_grants)

    def test_build_grant_plan_filters_missing_oracle_maintained_role_grant(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[],
            column_privileges=[],
            sys_privileges=[],
            role_privileges=[
                sdr.OracleRolePrivilege(grantee="DBA", role="EXP_FULL_DATABASE", admin_option=False),
            ],
            role_metadata={
                "EXP_FULL_DATABASE": sdr.OracleRoleInfo(
                    role="EXP_FULL_DATABASE",
                    authentication_type="NONE",
                    password_required=False,
                    oracle_maintained=True,
                )
            },
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        grant_plan = sdr.build_grant_plan(
            oracle_meta=oracle_meta,
            full_mapping={},
            remap_rules={},
            source_objects={},
            schema_mapping={},
            object_parent_map=None,
            dependency_graph=None,
            transitive_table_cache=None,
            source_dependencies=None,
            source_schema_set={"DBA"},
            remap_conflicts=None,
            synonym_meta={},
            ob_roles={"DBA"},
            ob_users={"DBA"},
            include_oracle_maintained_roles=False,
        )
        self.assertNotIn("DBA", grant_plan.role_privs)
        self.assertTrue(any(
            row.category == "ROLE"
            and row.grantee == "DBA"
            and row.privilege == "EXP_FULL_DATABASE"
            and row.reason == sdr.ROLE_GRANT_ORACLE_MAINTAINED_TARGET_MISSING
            for row in grant_plan.filtered_grants
        ))

    def test_build_grant_plan_filters_alias_role_when_target_role_missing(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[],
            column_privileges=[],
            sys_privileges=[],
            role_privileges=[
                sdr.OracleRolePrivilege(grantee="U1", role="SELECT_CATALOG_ROLE", admin_option=False),
            ],
            role_metadata={
                "SELECT_CATALOG_ROLE": sdr.OracleRoleInfo(
                    role="SELECT_CATALOG_ROLE",
                    authentication_type="NONE",
                    password_required=False,
                    oracle_maintained=True,
                )
            },
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        grant_plan = sdr.build_grant_plan(
            oracle_meta=oracle_meta,
            full_mapping={},
            remap_rules={},
            source_objects={},
            schema_mapping={},
            object_parent_map=None,
            dependency_graph=None,
            transitive_table_cache=None,
            source_dependencies=None,
            source_schema_set={"U1"},
            remap_conflicts=None,
            synonym_meta={},
            ob_roles={"U1"},
            ob_users={"U1"},
            include_oracle_maintained_roles=False,
        )
        self.assertNotIn("U1", grant_plan.role_privs)
        self.assertTrue(any(
            row.category == "ROLE"
            and row.grantee == "U1"
            and row.privilege == "OB_CATALOG_ROLE"
            and row.reason == sdr.ROLE_GRANT_ORACLE_MAINTAINED_TARGET_MISSING
            for row in grant_plan.filtered_grants
        ))

    def test_build_grant_plan_filters_oracle_maintained_role_when_ob_roles_unavailable(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[],
            column_privileges=[],
            sys_privileges=[],
            role_privileges=[
                sdr.OracleRolePrivilege(grantee="DBA", role="EXP_FULL_DATABASE", admin_option=False),
            ],
            role_metadata={
                "EXP_FULL_DATABASE": sdr.OracleRoleInfo(
                    role="EXP_FULL_DATABASE",
                    authentication_type="NONE",
                    password_required=False,
                    oracle_maintained=True,
                )
            },
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        grant_plan = sdr.build_grant_plan(
            oracle_meta=oracle_meta,
            full_mapping={},
            remap_rules={},
            source_objects={},
            schema_mapping={},
            object_parent_map=None,
            dependency_graph=None,
            transitive_table_cache=None,
            source_dependencies=None,
            source_schema_set={"DBA"},
            remap_conflicts=None,
            synonym_meta={},
            ob_roles=None,
            ob_users={"DBA"},
            include_oracle_maintained_roles=False,
        )
        self.assertNotIn("DBA", grant_plan.role_privs)
        self.assertTrue(any(
            row.category == "ROLE"
            and row.grantee == "DBA"
            and row.privilege == "EXP_FULL_DATABASE"
            and row.reason == sdr.ROLE_GRANT_TARGET_ROLE_UNVERIFIED
            for row in grant_plan.filtered_grants
        ))

    def test_build_grant_plan_keeps_existing_oracle_maintained_role_grant(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
            blacklist_tables={},
            object_privileges=[],
            column_privileges=[],
            sys_privileges=[],
            role_privileges=[
                sdr.OracleRolePrivilege(grantee="DBA", role="EXP_FULL_DATABASE", admin_option=False),
            ],
            role_metadata={
                "EXP_FULL_DATABASE": sdr.OracleRoleInfo(
                    role="EXP_FULL_DATABASE",
                    authentication_type="NONE",
                    password_required=False,
                    oracle_maintained=True,
                )
            },
            system_privilege_map=set(),
            table_privilege_map=set(),
            object_statuses={},
            package_errors={},
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        grant_plan = sdr.build_grant_plan(
            oracle_meta=oracle_meta,
            full_mapping={},
            remap_rules={},
            source_objects={},
            schema_mapping={},
            object_parent_map=None,
            dependency_graph=None,
            transitive_table_cache=None,
            source_dependencies=None,
            source_schema_set={"DBA"},
            remap_conflicts=None,
            synonym_meta={},
            ob_roles={"DBA", "EXP_FULL_DATABASE"},
            ob_users={"DBA"},
            include_oracle_maintained_roles=False,
        )
        self.assertIn(
            sdr.RoleGrantEntry("EXP_FULL_DATABASE", False),
            grant_plan.role_privs.get("DBA", set())
        )

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

    def test_dependency_grants_skip_public_grantee(self):
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
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
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        deps = [
            sdr.DependencyRecord(
                owner="PUBLIC",
                name="S1",
                object_type="SYNONYM",
                referenced_owner="HR",
                referenced_name="T1",
                referenced_type="TABLE"
            )
        ]
        source_objects = {
            "PUBLIC.S1": {"SYNONYM"},
            "HR.T1": {"TABLE"}
        }
        full_mapping = {
            "PUBLIC.S1": {"SYNONYM": "PUBLIC.S1"},
            "HR.T1": {"TABLE": "HR.T1"}
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
            source_schema_set={"PUBLIC", "HR"},
            remap_conflicts=None,
            synonym_meta={},
            dependencies=deps
        )
        self.assertNotIn("PUBLIC", grant_plan.object_grants)

    def test_remap_grantee_schema_non_public_cannot_map_to_public(self):
        mapped = sdr.remap_grantee_schema(
            grantee="APP_OWNER",
            schema_mapping={"APP_OWNER": "PUBLIC"},
            role_names=set()
        )
        self.assertEqual(mapped, "APP_OWNER")
        self.assertEqual(
            sdr.remap_grantee_schema("PUBLIC", {"PUBLIC": "PUBLIC"}, set()),
            "PUBLIC"
        )

    def test_build_target_extra_object_grant_rows(self):
        expected = {
            "APP": {
                sdr.ObjectGrantEntry("SELECT", "APP.T1", False),
            }
        }
        target = {
            ("APP", "SELECT", "APP.T1", False),           # expected
            ("PUBLIC", "SELECT", "APP.T1", False),        # extra public grant
            ("APP", "SELECT", "APP.T1", True),            # extra grant option
            ("APP_AUDITOR", "SELECT", "APP.T1", False),   # extra non-public grant
        }
        rows = sdr.build_target_extra_object_grant_rows(expected, target)
        self.assertEqual(len(rows), 3)
        by_key = {
            (r.grantee, r.privilege, r.object_full, r.reason_code): r
            for r in rows
        }
        self.assertIn(
            ("PUBLIC", "SELECT", "APP.T1", "TARGET_ONLY_PUBLIC_OBJECT_GRANT"),
            by_key
        )
        self.assertEqual(
            by_key[("PUBLIC", "SELECT", "APP.T1", "TARGET_ONLY_PUBLIC_OBJECT_GRANT")].action,
            "REVOKE_PUBLIC"
        )
        self.assertIn(
            ("APP", "SELECT", "APP.T1", "TARGET_ONLY_OBJECT_GRANT_OPTION"),
            by_key
        )
        self.assertIn(
            ("APP_AUDITOR", "SELECT", "APP.T1", "TARGET_ONLY_OBJECT_GRANT"),
            by_key
        )

    def test_build_target_extra_object_grant_rows_ignores_source_declared_filtered_public_privilege(self):
        expected = {
            "PUBLIC": {
                sdr.ObjectGrantEntry("INSERT", "APP.T1", False),
            }
        }
        target = {
            ("PUBLIC", "ALTER", "APP.T1", False),
            ("PUBLIC", "INSERT", "APP.T1", False),
        }
        filtered = [
            sdr.FilteredGrantEntry(
                category="OBJECT",
                grantee="PUBLIC",
                privilege="ALTER",
                object_full="APP.T1",
                reason="UNSUPPORTED_OBJECT_PRIV_IN_OB",
            )
        ]
        rows = sdr.build_target_extra_object_grant_rows(
            expected,
            target,
            declared_filtered_grants=filtered,
        )
        self.assertEqual(rows, [])

    def test_build_target_extra_object_grant_rows_ignores_source_declared_filtered_non_public_privilege(self):
        expected = {}
        target = {
            ("APP_USER", "ALTER", "APP.T1", False),
        }
        filtered = [
            sdr.FilteredGrantEntry(
                category="OBJECT",
                grantee="APP_USER",
                privilege="ALTER",
                object_full="APP.T1",
                reason="UNSUPPORTED_OBJECT_PRIV_IN_OB",
            )
        ]
        rows = sdr.build_target_extra_object_grant_rows(
            expected,
            target,
            declared_filtered_grants=filtered,
        )
        self.assertEqual(rows, [])

    def test_build_target_extra_object_grant_rows_normalizes_debug_alias(self):
        expected = {
            "PUBLIC": {
                sdr.ObjectGrantEntry("DEBUG", "A.P1", False),
            }
        }
        target = {
            ("PUBLIC", "OTHERS", "A.P1", False),
        }
        capability = sdr.GrantCapabilityLibrary(
            object_alias_to_logical={("PROCEDURE", "OTHERS"): "DEBUG"},
            known_logical_object_privileges={"DEBUG"},
        )
        rows = sdr.build_target_extra_object_grant_rows(
            expected,
            target,
            capability_library=capability,
            object_target_types={"A.P1": "PROCEDURE"},
        )
        self.assertEqual(rows, [])

    def test_build_target_extra_object_grant_rows_marks_unknown_target_privilege_manual(self):
        expected = {}
        target = {
            ("PUBLIC", "OTHERS", "A.P1", False),
        }
        capability = sdr.GrantCapabilityLibrary(
            known_logical_object_privileges={"DEBUG"},
        )
        rows = sdr.build_target_extra_object_grant_rows(
            expected,
            target,
            capability_library=capability,
            object_target_types={"A.P1": "PROCEDURE"},
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].reason_code, sdr.GRANT_CAPABILITY_REASON_TARGET_UNKNOWN)
        self.assertEqual(rows[0].action, "MANUAL_REVIEW")
        self.assertEqual(rows[0].target_catalog_privilege, "OTHERS")

    def test_build_target_extra_object_grant_rows_target_scope_can_exclude_old_source_schema(self):
        expected = {}
        target = {
            ("PUBLIC", "SELECT", "OMS_USER.LEGACY_T1", False),
            ("PUBLIC", "SELECT", "ORA_APP.T1", False),
        }
        rows = sdr.build_target_extra_object_grant_rows(
            expected,
            target,
            managed_target_objects={"ORA_APP.T1"},
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].object_full, "ORA_APP.T1")

    def test_build_managed_target_object_set_collects_all_targets(self):
        managed = sdr.build_managed_target_object_set({
            "SRC.T1": {"TABLE": "ORA_APP.T1", "INDEX": "ORA_APP.IDX_T1"},
            "SRC.P1": {"PROCEDURE": "OMS_USER.P1"},
        })
        self.assertEqual(
            managed,
            {"ORA_APP.T1", "ORA_APP.IDX_T1", "OMS_USER.P1"}
        )

    def test_export_grant_capability_detail_includes_probe_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [
                sdr.GrantCapabilityDetailRow(
                    category="OBJECT",
                    source_privilege="DEBUG",
                    object_type="PROCEDURE",
                    target_catalog_privilege="OTHERS",
                    support_status=sdr.GRANT_CAPABILITY_SUPPORT_SUPPORTED_ALIAS,
                    decision=sdr.GRANT_CAPABILITY_DECISION_ALLOW,
                    reason_code="",
                    error_code="",
                    error_message="",
                    sample_sql="GRANT DEBUG ON APP.P1 TO PUBLIC",
                )
            ]
            path = sdr.export_grant_capability_detail(
                rows,
                Path(tmpdir),
                "20240101",
                probe_complete=False,
                summary={"allow_rows": 1, "filter_rows": 0, "manual_rows": 0, "supported_alias_rows": 1},
            )
            self.assertIsNotNone(path)
            output = Path(path).read_text(encoding="utf-8")
            self.assertIn("# probe_complete=false", output)
            self.assertIn("# summary: allow=1 filter=0 manual=0 alias=1", output)
            self.assertIn("ALLOW/FILTER 中已标定结果仍有效", output)
            self.assertIn("GRANT DEBUG ON APP.P1 TO PUBLIC", output)

    def test_filter_grant_plan_unsupported_targets(self):
        grant_plan = sdr.GrantPlan(
            object_grants={
                "U1": {
                    sdr.ObjectGrantEntry("SELECT", "A.T_UNSUPPORTED", False),
                    sdr.ObjectGrantEntry("SELECT", "A.T_OK", False),
                }
            },
            column_grants={},
            sys_privs={},
            role_privs={},
            role_ddls=[],
            filtered_grants=[],
            view_grant_targets=set(),
        )
        support_row = sdr.ObjectSupportReportRow(
            obj_type="TABLE",
            src_full="SRC.T_UNSUPPORTED",
            tgt_full="A.T_UNSUPPORTED",
            support_state=sdr.SUPPORT_STATE_UNSUPPORTED,
            reason_code="BLACKLIST_SPE",
            reason="表字段存在不支持的类型",
            dependency="-",
            action="改造/不迁移",
            detail="SPE:ANYDATA",
            root_cause="A.T_UNSUPPORTED(SPE)",
        )
        support_summary = sdr.SupportClassificationResult(
            support_state_map={("TABLE", "SRC.T_UNSUPPORTED"): support_row},
            missing_detail_rows=[],
            unsupported_rows=[support_row],
            extra_missing_rows=[],
            missing_support_counts={},
            extra_blocked_counts={},
            unsupported_table_keys={("SRC", "T_UNSUPPORTED")},
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[],
        )
        filtered_plan, skipped = sdr.filter_grant_plan_unsupported_targets(
            grant_plan,
            support_summary
        )
        self.assertEqual(skipped, 1)
        self.assertIsNotNone(filtered_plan)
        kept_entries = filtered_plan.object_grants.get("U1", set())
        self.assertIn(sdr.ObjectGrantEntry("SELECT", "A.T_OK", False), kept_entries)
        self.assertNotIn(sdr.ObjectGrantEntry("SELECT", "A.T_UNSUPPORTED", False), kept_entries)
        reasons = {x.reason for x in filtered_plan.filtered_grants}
        self.assertIn("UNSUPPORTED_TARGET_BLACKLIST_SPE", reasons)

    def test_build_unsupported_grant_detail_rows(self):
        support_row = sdr.ObjectSupportReportRow(
            obj_type="TABLE",
            src_full="SRC.T_UNSUPPORTED",
            tgt_full="A.T_UNSUPPORTED",
            support_state=sdr.SUPPORT_STATE_BLOCKED,
            reason_code="DEPENDENCY_UNSUPPORTED",
            reason="依赖不支持对象",
            dependency="A.T_BLACK",
            action="先改造依赖对象",
            detail="TABLE",
            root_cause="A.T_BLACK(SPE)",
        )
        support_summary = sdr.SupportClassificationResult(
            support_state_map={("TABLE", "SRC.T_UNSUPPORTED"): support_row},
            missing_detail_rows=[],
            unsupported_rows=[support_row],
            extra_missing_rows=[],
            missing_support_counts={},
            extra_blocked_counts={},
            unsupported_table_keys={("SRC", "T_UNSUPPORTED")},
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[],
        )
        rows = sdr.build_unsupported_grant_detail_rows(
            [
                sdr.FilteredGrantEntry(
                    category="OBJECT",
                    grantee="U1",
                    privilege="SELECT",
                    object_full="A.T_UNSUPPORTED",
                    reason="UNSUPPORTED_TARGET_DEPENDENCY_UNSUPPORTED",
                ),
                sdr.FilteredGrantEntry(
                    category="OBJECT",
                    grantee="U1",
                    privilege="DEBUG",
                    object_full="A.T_OK",
                    reason="UNSUPPORTED_OBJECT_PRIV_IN_OB",
                ),
            ],
            support_summary,
        )
        self.assertEqual(len(rows), 2)
        by_reason = {row.reason_code: row for row in rows}
        self.assertEqual(
            by_reason["UNSUPPORTED_TARGET_DEPENDENCY_UNSUPPORTED"].reason,
            "依赖不支持对象"
        )
        self.assertEqual(
            by_reason["UNSUPPORTED_OBJECT_PRIV_IN_OB"].reason,
            "OB 不支持该对象权限"
        )

    def test_export_grant_capability_detail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [
                sdr.GrantCapabilityDetailRow(
                    category="OBJECT",
                    source_privilege="DEBUG",
                    object_type="PROCEDURE",
                    target_catalog_privilege="OTHERS",
                    support_status=sdr.GRANT_CAPABILITY_SUPPORT_SUPPORTED_ALIAS,
                    decision=sdr.GRANT_CAPABILITY_DECISION_ALLOW,
                    reason_code="-",
                    error_code="-",
                    error_message="-",
                    sample_sql="GRANT DEBUG ON OMS_USER.P1 TO R1;",
                )
            ]
            path = sdr.export_grant_capability_detail(rows, Path(tmpdir), "20240101")
            self.assertIsNotNone(path)
            output = Path(path).read_text(encoding="utf-8")
            self.assertIn("授权能力标定明细", output)
            self.assertIn("DEBUG", output)
            self.assertIn("OTHERS", output)
            self.assertIn("PROCEDURE", output)

    def test_build_blacklist_missing_grant_target_rows(self):
        blacklist_tables = {
            ("SRC", "BLK_LONG"): {
                ("LONG", "LONG"): sdr.BlacklistEntry("LONG", "LONG", "RULES"),
            }
        }
        table_target_map = {
            ("SRC", "BLK_LONG"): ("TGT", "BLK_LONG")
        }
        ob_meta = sdr.ObMetadata(
            objects_by_type={"TABLE": set()},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=True,
            partition_key_columns={},
        )
        rows = sdr.build_blacklist_missing_grant_target_rows(
            blacklist_tables,
            table_target_map,
            ob_meta
        )
        self.assertIn("TGT.BLK_LONG", rows)
        row = rows["TGT.BLK_LONG"]
        self.assertEqual(row.support_state, sdr.SUPPORT_STATE_BLOCKED)
        self.assertTrue(row.reason_code.startswith("BLACKLIST_"))

    def test_filter_grant_plan_unsupported_targets_with_extra_rows(self):
        grant_plan = sdr.GrantPlan(
            object_grants={
                "U1": {
                    sdr.ObjectGrantEntry("SELECT", "TGT.BLK_LONG", False),
                    sdr.ObjectGrantEntry("SELECT", "TGT.T_OK", False),
                }
            },
            column_grants={},
            sys_privs={},
            role_privs={},
            role_ddls=[],
            filtered_grants=[],
            view_grant_targets=set(),
        )
        extra_rows = {
            "TGT.BLK_LONG": sdr.ObjectSupportReportRow(
                obj_type="TABLE",
                src_full="SRC.BLK_LONG",
                tgt_full="TGT.BLK_LONG",
                support_state=sdr.SUPPORT_STATE_BLOCKED,
                reason_code="BLACKLIST_LONG",
                reason="LONG 黑名单表目标端不存在",
                dependency="-",
                action="先补齐对象(OMS/改造)后再授权",
                detail="-",
                root_cause="SRC.BLK_LONG(LONG)",
            )
        }
        filtered_plan, skipped = sdr.filter_grant_plan_unsupported_targets(
            grant_plan,
            support_summary=None,
            extra_target_rows=extra_rows
        )
        self.assertEqual(skipped, 1)
        self.assertIsNotNone(filtered_plan)
        kept_entries = filtered_plan.object_grants.get("U1", set())
        self.assertIn(sdr.ObjectGrantEntry("SELECT", "TGT.T_OK", False), kept_entries)
        self.assertNotIn(sdr.ObjectGrantEntry("SELECT", "TGT.BLK_LONG", False), kept_entries)
        reasons = {x.reason for x in filtered_plan.filtered_grants}
        self.assertIn("UNSUPPORTED_TARGET_BLACKLIST_LONG", reasons)

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
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints=oracle_constraints,
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={},
            column_comments={},
            comments_complete=True,
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
            package_errors_complete=False, partition_key_columns={}, interval_partitions={}
        )
        ob_meta = sdr.ObMetadata(
            objects_by_type={},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints=ob_constraints,
            triggers={},
            sequences={},
            sequence_attrs={},
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

    def test_compare_constraints_for_table_fk_missing_target_ref_metadata_is_conservative(self):
        oracle_constraints = {
            ("A", "T1"): {
                "FK1": {
                    "type": "R",
                    "columns": ["C1"],
                    "ref_table_owner": "A",
                    "ref_table_name": "RT1",
                }
            }
        }
        ob_constraints = {
            ("X", "T1"): {
                "FK1_OB": {
                    "type": "R",
                    "columns": ["C1"],
                    "ref_metadata_complete": False,
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
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
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertIn("FK1", mismatch.missing_constraints)
        self.assertTrue(
            any("无法恢复引用表元数据" in detail for detail in mismatch.detail_mismatch)
        )

    def test_compare_constraints_for_table_fk_missing_ref_table_mapping_is_conservative(self):
        oracle_constraints = {
            ("A", "T1"): {
                "FK1": {
                    "type": "R",
                    "columns": ["C1"],
                    "ref_table_owner": "A",
                    "ref_table_name": "RT1",
                }
            }
        }
        ob_constraints = {
            ("X", "T1"): {
                "FK1_OB": {
                    "type": "R",
                    "columns": ["C1"],
                    "r_owner": "Y",
                    "r_constraint": "PK_RT1",
                    "ref_metadata_complete": True,
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
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
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertIn("FK1", mismatch.missing_constraints)
        self.assertTrue(
            any("无法恢复引用表元数据" in detail for detail in mismatch.detail_mismatch)
        )

    def test_compare_constraints_for_table_check_expr_match(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_SRC": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1>0",
                },
                "SYS_123": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1 IS NOT NULL",
                    "status": "ENABLED",
                },
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "CK_TGT": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1>0",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_constraints_for_table_ignores_obnotnull_only_difference(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_SRC": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1 > 0",
                }
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "CK_TGT": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1 > 0",
                },
                "T1_OBNOTNULL_12345": {
                    "type": "C",
                    "columns": ["C2"],
                    "search_condition": "\"C2\" IS NOT NULL",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_constraints_for_table_ignores_obcheck_notnull_only_difference(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_SRC": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1 > 0",
                }
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "CK_TGT": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1 > 0",
                },
                "T1_OBCHECK_1761134849332186": {
                    "type": "C",
                    "columns": ["C2"],
                    "search_condition": '("C2" is not null)',
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_constraints_for_table_ignores_enabled_notnull_check_backing_system_novalidate(self):
        oracle_constraints = {
            ("A", "T1"): {
                "SYS_C001": {
                    "type": "C",
                    "columns": ["C2"],
                    "search_condition": '"C2" IS NOT NULL',
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                }
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "NN_T1_C2": {
                    "type": "C",
                    "columns": ["C2"],
                    "search_condition": '"C2" IS NOT NULL',
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_constraints_for_table_check_ob_rewrite_between_like(self):
        oracle_constraints = {
            ("A", "T1"): {
                "SYS_C1": {
                    "type": "C",
                    "columns": ["QTY"],
                    "search_condition": "QTY BETWEEN 0 AND 999",
                },
                "SYS_C2": {
                    "type": "C",
                    "columns": ["CODE"],
                    "search_condition": "CODE IS NULL OR CODE LIKE 'X%'",
                },
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "T1_OBCHECK_1": {
                    "type": "C",
                    "columns": ["QTY"],
                    "search_condition": '(("QTY" >= 0) and ("QTY" <= 999))',
                },
                "T1_OBCHECK_2": {
                    "type": "C",
                    "columns": ["CODE"],
                    "search_condition": '(("CODE" is null) or ("CODE" like replace(\'X%\',\'\\\\\',\'\\\\\\\\\') escape \'\\\\\'))',
                },
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_is_temp_table_blacklist_reason_code_precision(self):
        self.assertTrue(sdr.is_temp_table_blacklist_reason_code("TEMP_TABLE"))
        self.assertTrue(sdr.is_temp_table_blacklist_reason_code("temporary_table"))
        self.assertTrue(sdr.is_temp_table_blacklist_reason_code("BLACKLIST_TEMP_TABLE"))
        self.assertTrue(sdr.is_temp_table_blacklist_reason_code("BLACKLIST_TEMPORARY_TABLE"))
        self.assertFalse(sdr.is_temp_table_blacklist_reason_code("NON_TEMP_TABLE_CASE"))
        self.assertFalse(sdr.is_temp_table_blacklist_reason_code("TEMP_TABLE_RELATED"))

    def test_compare_constraints_for_table_check_suppressed_counts_visible_when_mismatch_exists(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_A1": {"type": "C", "columns": ["C1"], "search_condition": "C1 IN ('0','1')"},
                "CK_A2": {"type": "C", "columns": ["C1"], "search_condition": "C1 IN('0','1')"},
                "CK_B": {"type": "C", "columns": ["C2"], "search_condition": "C2 > 0"},
                "CK_C": {"type": "C", "columns": ["C3"], "search_condition": "C3 IN ('X','Y')"},
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "CK_A_OB": {"type": "C", "columns": ["C1"], "search_condition": "(\"C1\" in ('0','1'))"},
                "CK_C_OB_1": {"type": "C", "columns": ["C3"], "search_condition": "(\"C3\" in ('X','Y'))"},
                "CK_C_OB_2": {"type": "C", "columns": ["C3"], "search_condition": "(\"C3\" in ('X','Y'))"},
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.check_suppressed_source_dup_count, 1)
        self.assertEqual(mismatch.check_suppressed_target_dup_count, 1)
        self.assertTrue(any("CHECK_SUPPRESSED: SOURCE_DUP_EXPR=1" in x for x in mismatch.detail_mismatch))
        self.assertTrue(any("CHECK_SUPPRESSED: TARGET_DUP_EXPR=1" in x for x in mismatch.detail_mismatch))

    def test_compare_constraints_for_table_check_duplicate_semantics_in_target(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_EAI_FLAG": {
                    "type": "C",
                    "columns": ["EAI_FLAG"],
                    "search_condition": "EAI_FLAG IN('0','1','2','3')",
                },
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "CK_EAI_FLAG_1": {
                    "type": "C",
                    "columns": ["EAI_FLAG"],
                    "search_condition": '("EAI_FLAG" in (\'0\',\'1\',\'2\',\'3\'))',
                },
                "CK_EAI_FLAG_2": {
                    "type": "C",
                    "columns": ["EAI_FLAG"],
                    "search_condition": '("EAI_FLAG" in (\'0\',\'1\',\'2\',\'3\'))',
                },
                "T1_OBCHECK_1769104366055819": {
                    "type": "C",
                    "columns": ["EAI_FLAG"],
                    "search_condition": '("EAI_FLAG" in (\'0\',\'1\',\'2\',\'3\'))',
                },
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_constraints_for_table_check_duplicate_semantics_in_source(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_EAI_FLAG_1": {
                    "type": "C",
                    "columns": ["EAI_FLAG"],
                    "search_condition": "EAI_FLAG IN ('0','1','2','3')",
                },
                "CK_EAI_FLAG_2": {
                    "type": "C",
                    "columns": ["EAI_FLAG"],
                    "search_condition": "EAI_FLAG IN('0','1','2','3')",
                },
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "CK_EAI_FLAG_OB": {
                    "type": "C",
                    "columns": ["EAI_FLAG"],
                    "search_condition": '("EAI_FLAG" in (\'0\',\'1\',\'2\',\'3\'))',
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_constraints_for_table_reports_duplicate_notnull_checks_in_target(self):
        oracle_constraints = {
            ("A", "T1"): {
                "SYS_C1": {
                    "type": "C",
                    "columns": ["PK_SERIAL#"],
                    "search_condition": '"PK_SERIAL#" IS NOT NULL',
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "NN_T1_PK_SERIAL": {
                    "type": "C",
                    "columns": ["PK_SERIAL#"],
                    "search_condition": '("PK_SERIAL#" IS NOT NULL)',
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ob_meta = ob_meta._replace(enabled_notnull_check_groups={
            ("A", "T1"): {
                "PK_SERIAL#": (
                    sdr.NotnullCheckEntry("NN_T1_PK_SERIAL", '"PK_SERIAL#" IS NOT NULL', "ENABLED", "NOT VALIDATED", False, False),
                    sdr.NotnullCheckEntry("T1_OBCHECK_1", '"PK_SERIAL#" IS NOT NULL', "ENABLED", "NOT VALIDATED", True, False),
                    sdr.NotnullCheckEntry("T1_OBCHECK_2", '"PK_SERIAL#" IS NOT NULL', "ENABLED", "NOT VALIDATED", True, False),
                )
            }
        })
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.extra_constraints, {"NN_T1_PK_SERIAL", "T1_OBCHECK_2"})
        self.assertEqual(
            mismatch.duplicate_notnull_extra_constraints,
            frozenset({"NN_T1_PK_SERIAL", "T1_OBCHECK_2"})
        )
        self.assertTrue(any("CHECK_DUPLICATE_NOTNULL" in line for line in mismatch.detail_mismatch))

    def test_compare_constraints_for_table_suppresses_missing_named_notnull_when_target_has_auto_semantics(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_NN_C1": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": '"C1" IS NOT NULL',
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints={("A", "T1"): {}})
        ob_meta = ob_meta._replace(enabled_notnull_check_groups={
            ("A", "T1"): {
                "C1": (
                    sdr.NotnullCheckEntry("T1_OBCHECK_1", '"C1" IS NOT NULL', "ENABLED", "VALIDATED", True, False),
                )
            }
        })
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_constraints_for_table_duplicate_notnull_prefers_ob_auto_when_source_is_system_named(self):
        oracle_constraints = {
            ("A", "T1"): {
                "SYS_C1": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": '"C1" IS NOT NULL',
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints={
            ("A", "T1"): {
                "NN_T1_C1": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": '"C1" IS NOT NULL',
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
            }
        })
        ob_meta = ob_meta._replace(enabled_notnull_check_groups={
            ("A", "T1"): {
                "C1": (
                    sdr.NotnullCheckEntry("NN_T1_C1", '"C1" IS NOT NULL', "ENABLED", "NOT VALIDATED", False, False),
                    sdr.NotnullCheckEntry("T1_OBCHECK_1", '"C1" IS NOT NULL', "ENABLED", "NOT VALIDATED", True, False),
                )
            }
        })
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.extra_constraints, {"NN_T1_C1"})
        self.assertEqual(
            mismatch.duplicate_notnull_extra_constraints,
            frozenset({"NN_T1_C1"})
        )
        self.assertTrue(any("保留=T1_OBCHECK_1" in line for line in mismatch.detail_mismatch))

    def test_compare_constraints_cache_does_not_short_circuit_duplicate_notnull_cleanup(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_NN_C1": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": '"C1" IS NOT NULL',
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints={
            ("A", "T1"): {
                "CK_NN_C1": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": '"C1" IS NOT NULL',
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
            }
        })
        ob_meta = ob_meta._replace(enabled_notnull_check_groups={
            ("A", "T1"): {
                "C1": (
                    sdr.NotnullCheckEntry("CK_NN_C1", '"C1" IS NOT NULL', "ENABLED", "NOT VALIDATED", False, False),
                    sdr.NotnullCheckEntry("T1_OBCHECK_1", '"C1" IS NOT NULL', "ENABLED", "NOT VALIDATED", True, False),
                )
            }
        })
        cache = sdr.build_constraint_cache_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {},
        )
        self.assertEqual(cache.src_sig, cache.tgt_sig)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {},
            cache=cache,
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.extra_constraints, {"T1_OBCHECK_1"})
        self.assertEqual(
            mismatch.duplicate_notnull_extra_constraints,
            frozenset({"T1_OBCHECK_1"})
        )
        self.assertTrue(any("CHECK_DUPLICATE_NOTNULL" in line for line in mismatch.detail_mismatch))

    def test_compare_constraints_for_table_reports_disabled_system_notnull_missing(self):
        oracle_constraints = {
            ("A", "T1"): {
                "SYS_C1": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": '"C1" IS NOT NULL',
                    "status": "DISABLED",
                    "validated": "NOT VALIDATED",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints={("A", "T1"): {}})
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.missing_constraints, {"SYS_C1"})
        self.assertTrue(any("CHECK: 源约束 SYS_C1" in line for line in mismatch.detail_mismatch))

    def test_compare_constraints_cache_keeps_disabled_system_notnull_in_signature(self):
        oracle_constraints = {
            ("A", "T1"): {
                "SYS_C1": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": '"C1" IS NOT NULL',
                    "status": "DISABLED",
                    "validated": "NOT VALIDATED",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints={("A", "T1"): {}})
        cache = sdr.build_constraint_cache_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {},
        )
        self.assertNotEqual(cache.src_sig, cache.tgt_sig)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {},
            cache=cache,
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.missing_constraints, {"SYS_C1"})

    def test_apply_noise_suppression_keeps_duplicate_notnull_extra_obcheck(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extra_targets": [],
            "remap_conflicts": [],
        }
        extra_results = {
            "index_ok": [],
            "index_mismatched": [],
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="A.T1",
                    missing_constraints=set(),
                    extra_constraints={"NN_T1_C1"},
                    detail_mismatch=["CHECK_DUPLICATE_NOTNULL: 列 C1 源端同语义数=1，目标端同语义数=2；保留=T1_OBCHECK_1；额外=NN_T1_C1。"],
                    downgraded_pk_constraints=set(),
                    duplicate_notnull_extra_constraints=frozenset({"NN_T1_C1"}),
                )
            ],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }
        result = sdr.apply_noise_suppression(
            tv_results,
            extra_results,
            {},
        )
        kept = result.extra_results.get("constraint_mismatched", [])
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0].extra_constraints, {"NN_T1_C1"})

    def test_collect_and_export_safe_duplicate_notnull_cleanup_candidates(self):
        extra_results = {
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="B.T1",
                    missing_constraints=set(),
                    extra_constraints={"NN_T1_C1", "T1_OBCHECK_2"},
                    detail_mismatch=["CHECK_DUPLICATE_NOTNULL: 列 C1 源端同语义数=1，目标端同语义数=3；保留=T1_OBCHECK_1；额外=NN_T1_C1,T1_OBCHECK_2。"],
                    downgraded_pk_constraints=set(),
                    duplicate_notnull_extra_constraints=frozenset({"NN_T1_C1", "T1_OBCHECK_2"}),
                )
            ]
        }
        raw_candidates = sdr.collect_safe_duplicate_notnull_cleanup_candidates(extra_results)
        self.assertEqual(
            {target for _obj_type, target, _source, _sql in raw_candidates},
            {"B.NN_T1_C1", "B.T1_OBCHECK_2"}
        )
        general_candidates = sdr.collect_extra_cleanup_candidates(None, extra_results)
        self.assertEqual(general_candidates, [])
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = sdr.export_extra_cleanup_candidates(
                Path(tmpdir),
                [],
                safe_duplicate_notnull_candidates=raw_candidates,
            )
            self.assertIsNotNone(output_path)
            text = output_path.read_text(encoding="utf-8")
            self.assertIn("SAFE_DUPLICATE_NOTNULL_DROP_SQL", text)
            self.assertIn('ALTER TABLE "B"."T1" DROP CONSTRAINT NN_T1_C1;', text)
            self.assertIn('ALTER TABLE "B"."T1" DROP CONSTRAINT T1_OBCHECK_2;', text)

    def test_export_safe_duplicate_notnull_cleanup_fixup_scripts(self):
        candidates = [
            ("CONSTRAINT", "B.NN_T1_C1", "SAFE_DUPLICATE_NOTNULL", 'ALTER TABLE "B"."T1" DROP CONSTRAINT NN_T1_C1;'),
            ("CONSTRAINT", "B.T1_OBCHECK_2", "SAFE_DUPLICATE_NOTNULL", 'ALTER TABLE "B"."T1" DROP CONSTRAINT T1_OBCHECK_2;'),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = sdr.export_safe_duplicate_notnull_cleanup_fixup_scripts(Path(tmpdir), candidates)
            rels = sorted(str(path.relative_to(tmpdir)) for path in paths)
            self.assertEqual(
                rels,
                [
                    "cleanup_safe/constraint/B.NN_T1_C1.drop.sql",
                    "cleanup_safe/constraint/B.T1_OBCHECK_2.drop.sql",
                ]
            )
            text = (Path(tmpdir) / "cleanup_safe" / "constraint" / "B.NN_T1_C1.drop.sql").read_text(encoding="utf-8")
            self.assertIn('ALTER SESSION SET CURRENT_SCHEMA = B;', text)
            self.assertIn('ALTER TABLE "B"."T1" DROP CONSTRAINT NN_T1_C1;', text)

    def test_classify_unsupported_check_constraints_filters_extra(self):
        oracle_meta = self._make_oracle_meta(
            constraints={
                ("A", "T1"): {
                    "CK1": {
                        "type": "C",
                        "columns": ["C1"],
                        "search_condition": "C1>0",
                        "deferrable": "DEFERRABLE",
                        "deferred": "IMMEDIATE",
                    }
                }
            }
        )
        extra_results = {
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="A.T1",
                    missing_constraints={"CK1"},
                    extra_constraints={"CK1"},
                    detail_mismatch=["CHECK: CK1 mismatch"],
                    downgraded_pk_constraints=set(),
                )
            ]
        }
        table_target_map = {("A", "T1"): ("A", "T1")}
        sdr.classify_unsupported_check_constraints(extra_results, oracle_meta, table_target_map)
        self.assertEqual(extra_results["constraint_mismatched"], [])

    def test_compare_constraints_for_table_derived_unique_constraint(self):
        oracle_constraints = {("A", "T1"): {}}
        oracle_indexes = {
            ("A", "T1"): {
                "UX_T1": {
                    "uniqueness": "UNIQUE",
                    "columns": ["C1"],
                    "expressions": {},
                    "descend": []
                }
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "UX_T1": {
                    "type": "U",
                    "columns": ["C1"]
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints, indexes=oracle_indexes)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_constraints_for_table_check_missing_without_expr(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_SRC": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": None,
                },
            }
        }
        ob_constraints = {}
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertIn("CK_SRC", mismatch.missing_constraints)

    def test_compare_constraints_for_table_check_deferrable_mismatch(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_SRC": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1>0",
                    "deferrable": "DEFERRABLE",
                    "deferred": "DEFERRED",
                }
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "CK_SRC": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1>0",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints, constraint_deferrable_supported=True)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertTrue(any("DEFERRABLE" in msg for msg in mismatch.detail_mismatch))

    def test_compare_constraints_for_table_check_deferrable_unknown_target(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_SRC": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1>0",
                    "deferrable": "DEFERRABLE",
                    "deferred": "DEFERRED",
                }
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "CK_SRC": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1>0",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints, constraint_deferrable_supported=False)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_constraints_for_table_check_expr_first_name_mismatch(self):
        oracle_constraints = {
            ("A", "T1"): {
                "CK_A": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1 > 0",
                }
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "CK_A": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1 > 1",
                },
                "CK_B": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1 > 0",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.missing_constraints, set())
        self.assertIn("CK_B", mismatch.extra_constraints)
        self.assertNotIn("CK_A", mismatch.extra_constraints)
        self.assertTrue(any("条件不一致" in item for item in mismatch.detail_mismatch))

    def test_classify_unsupported_check_constraints(self):
        oracle_constraints = {
            ("SRC", "T1"): {
                "CK_SYS": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "SYS_CONTEXT('USERENV','CURRENT_USER') IS NOT NULL",
                },
                "CK_DEF": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": "C1 > 0",
                    "deferrable": "DEFERRABLE",
                    "deferred": "DEFERRED",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        extra_results = {
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="TGT.T1",
                    missing_constraints={"CK_SYS", "CK_DEF"},
                    extra_constraints=set(),
                    detail_mismatch=[
                        "CHECK: 源约束 CK_SYS (条件 SYS_CONTEXT('USERENV','CURRENT_USER')) 在目标端未找到。",
                        "CHECK: 源约束 CK_DEF (条件 C1 > 0) 在目标端未找到。",
                    ],
                    downgraded_pk_constraints=set()
                )
            ]
        }
        table_target_map = {("SRC", "T1"): ("TGT", "T1")}
        rows = sdr.classify_unsupported_check_constraints(
            extra_results,
            oracle_meta,
            table_target_map
        )
        reason_codes = {row.reason_code for row in rows}
        self.assertEqual(reason_codes, {"CHECK_SYS_CONTEXT", "CHECK_DEFERRABLE"})
        self.assertEqual(extra_results["constraint_mismatched"], [])
        self.assertIn("TGT.T1", extra_results["constraint_ok"])

    def test_classify_unsupported_check_constraints_allows_self_ref_fk(self):
        oracle_constraints = {
            ("SRC", "T1"): {
                "PK_T1": {
                    "type": "P",
                    "columns": ["C1"],
                },
                "FK_SELF": {
                    "type": "R",
                    "columns": ["C1"],
                    "r_owner": "SRC",
                    "r_constraint": "PK_T1",
                    "ref_table_owner": "SRC",
                    "ref_table_name": "T1",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        extra_results = {
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="TGT.T1",
                    missing_constraints={"FK_SELF"},
                    extra_constraints=set(),
                    detail_mismatch=[
                        "FOREIGN KEY: 源约束 FK_SELF 在目标端未找到。"
                    ],
                    downgraded_pk_constraints=set()
                )
            ]
        }
        table_target_map = {("SRC", "T1"): ("TGT", "T1")}
        rows = sdr.classify_unsupported_check_constraints(
            extra_results,
            oracle_meta,
            table_target_map
        )
        self.assertEqual(rows, [])
        self.assertEqual(len(extra_results["constraint_mismatched"]), 1)
        self.assertEqual(extra_results["constraint_mismatched"][0].missing_constraints, {"FK_SELF"})

    def test_build_unsupported_summary_counts_includes_extra(self):
        support_summary = sdr.SupportClassificationResult(
            support_state_map={},
            missing_detail_rows=[],
            unsupported_rows=[],
            extra_missing_rows=[],
            missing_support_counts={
                "TABLE": {"supported": 0, "unsupported": 2, "blocked": 1},
                "VIEW": {"supported": 0, "unsupported": 0, "blocked": 3},
            },
            extra_blocked_counts={"INDEX": 4, "CONSTRAINT": 5},
            unsupported_table_keys=set(),
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[]
        )
        extra_results = {
            "index_unsupported": ["IDX1", "IDX2"],
            "constraint_unsupported": ["C1"]
        }
        counts = sdr.build_unsupported_summary_counts(support_summary, extra_results)
        self.assertEqual(counts["TABLE"], 3)
        self.assertEqual(counts["VIEW"], 3)
        self.assertEqual(counts["INDEX"], 6)
        self.assertEqual(counts["CONSTRAINT"], 6)

    def test_build_missing_breakdown_counts_clamps_unsupported_when_total_zero(self):
        support_summary = sdr.SupportClassificationResult(
            support_state_map={},
            missing_detail_rows=[],
            unsupported_rows=[],
            extra_missing_rows=[],
            missing_support_counts={},
            extra_blocked_counts={"TRIGGER": 5},
            unsupported_table_keys=set(),
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[]
        )
        totals, unsupported, fixable = sdr.build_missing_breakdown_counts(
            {"oracle": {}, "oceanbase": {}, "missing": {"TABLE": 2}, "extra": {}},
            support_summary,
            {"index_unsupported": [], "constraint_unsupported": []}
        )
        self.assertEqual(totals["TABLE"], 2)
        self.assertEqual(unsupported["TRIGGER"], 0)
        self.assertEqual(fixable["TRIGGER"], 0)

    def test_classify_unsupported_deferrable_pk(self):
        oracle_constraints = {
            ("SRC", "T1"): {
                "PK_DEF": {
                    "type": "P",
                    "columns": ["C1"],
                    "deferrable": "DEFERRABLE",
                    "deferred": "IMMEDIATE",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        extra_results = {
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="TGT.T1",
                    missing_constraints={"PK_DEF"},
                    extra_constraints=set(),
                    detail_mismatch=[
                        "PRIMARY KEY: 源约束 PK_DEF (列 ['C1']) 在目标端未找到。"
                    ],
                    downgraded_pk_constraints=set()
                )
            ]
        }
        table_target_map = {("SRC", "T1"): ("TGT", "T1")}
        rows = sdr.classify_unsupported_check_constraints(
            extra_results,
            oracle_meta,
            table_target_map
        )
        reason_codes = {row.reason_code for row in rows}
        self.assertEqual(reason_codes, {"PRIMARY_KEY_DEFERRABLE"})
        self.assertEqual(extra_results["constraint_mismatched"], [])
        self.assertIn("TGT.T1", extra_results["constraint_ok"])

    def test_classify_unsupported_indexes_desc(self):
        oracle_indexes = {
            ("SRC", "T1"): {
                "IDX_DESC": {
                    "uniqueness": "NONUNIQUE",
                    "columns": ["C1"],
                    "expressions": {},
                    "descend": ["DESC"]
                }
            }
        }
        oracle_meta = self._make_oracle_meta(indexes=oracle_indexes)
        extra_results = {
            "index_ok": [],
            "index_mismatched": [
                sdr.IndexMismatch(
                    table="TGT.T1",
                    missing_indexes={"IDX_DESC"},
                    extra_indexes=set(),
                    detail_mismatch=["索引列 ['C1'] 在目标端未找到。"]
                )
            ]
        }
        table_target_map = {("SRC", "T1"): ("TGT", "T1")}
        rows = sdr.classify_unsupported_indexes(
            extra_results,
            oracle_meta,
            table_target_map
        )
        reason_codes = {row.reason_code for row in rows}
        self.assertEqual(reason_codes, {"INDEX_DESC"})
        self.assertEqual(extra_results["index_mismatched"], [])
        self.assertIn("TGT.T1", extra_results["index_ok"])

    def test_compare_constraints_for_table_fk_delete_rule_mismatch(self):
        oracle_constraints = {
            ("A", "T1"): {
                "FK_SRC": {
                    "type": "R",
                    "columns": ["C1"],
                    "ref_table_owner": "A",
                    "ref_table_name": "P1",
                    "delete_rule": "CASCADE",
                }
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "FK_TGT": {
                    "type": "R",
                    "columns": ["C1"],
                    "ref_table_owner": "A",
                    "ref_table_name": "P1",
                    "delete_rule": "NO ACTION",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertTrue(any("DELETE_RULE" in item for item in mismatch.detail_mismatch))

    def test_compare_constraints_for_table_fk_update_rule_mismatch(self):
        oracle_constraints = {
            ("A", "T1"): {
                "FK_SRC": {
                    "type": "R",
                    "columns": ["C1"],
                    "ref_table_owner": "A",
                    "ref_table_name": "P1",
                    "update_rule": "CASCADE",
                }
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "FK_TGT": {
                    "type": "R",
                    "columns": ["C1"],
                    "ref_table_owner": "A",
                    "ref_table_name": "P1",
                    "update_rule": "NO ACTION",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(constraints=oracle_constraints)
        ob_meta = self._make_ob_meta(constraints=ob_constraints)
        ok, mismatch = sdr.compare_constraints_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            {}
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertTrue(any("UPDATE_RULE" in item for item in mismatch.detail_mismatch))

    def test_compare_indexes_expression_sys_nc_match(self):
        oracle_indexes = {
            ("A", "T1"): {
                "IDX1": {
                    "columns": ["SYS_NC00004$"],
                    "expressions": {1: "UPPER(NAME)"},
                    "uniqueness": "NONUNIQUE",
                }
            }
        }
        ob_indexes = {
            ("A", "T1"): {
                "IDX1": {
                    "columns": ["SYS_NC00004$"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(indexes=oracle_indexes)
        ob_meta = self._make_ob_meta(indexes=ob_indexes)
        ob_meta = ob_meta._replace(temporary_tables={("A", "T1")})
        ok, mismatch = sdr.compare_indexes_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_indexes_sys_nc_name_mismatch(self):
        oracle_indexes = {
            ("A", "T1"): {
                "IDX_SRC": {
                    "columns": ["SYS_NC00001$"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                }
            }
        }
        ob_indexes = {
            ("A", "T1"): {
                "IDX_TGT": {
                    "columns": ["SYS_NC00002$"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(indexes=oracle_indexes)
        ob_meta = self._make_ob_meta(indexes=ob_indexes)
        ob_meta = ob_meta._replace(temporary_tables=set())
        ok, mismatch = sdr.compare_indexes_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_indexes_expression_sys_nc_name_mismatch(self):
        oracle_indexes = {
            ("A", "T1"): {
                "IDX_SRC": {
                    "columns": ["SYS_NC00004$"],
                    "expressions": {1: "UPPER(NAME)"},
                    "uniqueness": "NONUNIQUE",
                }
            }
        }
        ob_indexes = {
            ("A", "T1"): {
                "IDX_TGT": {
                    "columns": ["SYS_NC00005$"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(indexes=oracle_indexes)
        ob_meta = self._make_ob_meta(indexes=ob_indexes)
        ob_meta = ob_meta._replace(temporary_tables={("A", "T1")})
        ok, mismatch = sdr.compare_indexes_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_indexes_expression_constraint_index_name(self):
        oracle_indexes = {
            ("A", "T1"): {
                "IDX_SRC": {
                    "columns": ["SYS_NC00004$"],
                    "expressions": {1: "UPPER(NAME)"},
                    "uniqueness": "NONUNIQUE",
                }
            }
        }
        ob_indexes = {
            ("A", "T1"): {
                "IDX_TGT": {
                    "columns": ["SYS_NC00004$"],
                    "expressions": {1: "UPPER(NAME)"},
                    "uniqueness": "UNIQUE",
                }
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "UK_TGT": {
                    "type": "U",
                    "columns": ["SYS_NC00004$"],
                    "index_name": "IDX_TGT",
                }
            }
        }
        oracle_meta = self._make_oracle_meta(indexes=oracle_indexes)
        ob_meta = self._make_ob_meta(indexes=ob_indexes, constraints=ob_constraints)
        ob_meta = ob_meta._replace(temporary_tables={("A", "T1")})
        ok, mismatch = sdr.compare_indexes_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_indexes_gtt_like_helper_name_without_temp_table_not_normalized(self):
        oracle_indexes = {
            ("A", "T1"): {
                "IX_BIZ": {
                    "columns": ["CODE"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                }
            }
        }
        ob_indexes = {
            ("A", "T1"): {
                "IDX_FOR_HEAP_GTT_T1": {
                    "columns": ["SYS_SESSION_ID"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                },
                "IX_BIZ": {
                    "columns": ["SYS_SESSION_ID", "CODE"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(indexes=oracle_indexes)
        ob_meta = self._make_ob_meta(indexes=ob_indexes)
        ok, mismatch = sdr.compare_indexes_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertIn("IX_BIZ", mismatch.missing_indexes)

    def test_compare_indexes_ob_gtt_internal_and_sys_session_id_normalized(self):
        oracle_indexes = {
            ("A", "T1"): {
                "IX_BIZ": {
                    "columns": ["C1"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                }
            }
        }
        ob_indexes = {
            ("A", "T1"): {
                "IDX_FOR_HEAP_GTT_T1": {
                    "columns": ["SYS_SESSION_ID"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                },
                "IX_BIZ": {
                    "columns": ["SYS_SESSION_ID", "C1"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(indexes=oracle_indexes)
        ob_meta = self._make_ob_meta(indexes=ob_indexes)
        ob_meta = ob_meta._replace(temporary_tables={("A", "T1")})

        cache = sdr.build_index_cache_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
        )
        ok, mismatch = sdr.compare_indexes_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
            cache=cache,
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_indexes_non_gtt_sys_session_id_not_normalized(self):
        oracle_indexes = {
            ("A", "T1"): {
                "IX_BIZ": {
                    "columns": ["C1"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                }
            }
        }
        ob_indexes = {
            ("A", "T1"): {
                "IX_BIZ": {
                    "columns": ["SYS_SESSION_ID", "C1"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(indexes=oracle_indexes)
        ob_meta = self._make_ob_meta(indexes=ob_indexes)
        ob_meta = ob_meta._replace(temporary_tables={("A", "T1")})

        ok, mismatch = sdr.compare_indexes_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
        )
        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertIn("IX_BIZ", mismatch.missing_indexes)
        self.assertIn("IX_BIZ", mismatch.extra_indexes)

    def test_compare_indexes_ob_gtt_expression_index_normalized(self):
        oracle_indexes = {
            ("A", "T1"): {
                "IX_EXPR": {
                    "columns": ["SYS_NC00004$"],
                    "expressions": {1: "UPPER(C4)"},
                    "uniqueness": "NONUNIQUE",
                }
            }
        }
        ob_indexes = {
            ("A", "T1"): {
                "IDX_FOR_HEAP_GTT_T1": {
                    "columns": ["SYS_SESSION_ID"],
                    "expressions": {},
                    "uniqueness": "NONUNIQUE",
                },
                "IX_EXPR": {
                    "columns": ["SYS_SESSION_ID", "SYS_NC20$"],
                    "expressions": {2: "UPPER(C4)"},
                    "uniqueness": "NONUNIQUE",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(indexes=oracle_indexes)
        ob_meta = self._make_ob_meta(indexes=ob_indexes)
        ob_meta = ob_meta._replace(temporary_tables={("A", "T1")})

        ok, mismatch = sdr.compare_indexes_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_compare_indexes_ob_gtt_normalized_by_constraint_backing_index(self):
        # 无 IDX_FOR_HEAP_GTT_* 内部索引名时，也应识别到 GTT 风格的 SYS_SESSION_ID 前缀
        oracle_indexes = {
            ("A", "T1"): {
                "PK_T1": {
                    "columns": ["ID"],
                    "expressions": {},
                    "uniqueness": "UNIQUE",
                },
                "UK_T1": {
                    "columns": ["CODE"],
                    "expressions": {},
                    "uniqueness": "UNIQUE",
                },
            }
        }
        ob_indexes = {
            ("A", "T1"): {
                "UK_T1": {
                    "columns": ["SYS_SESSION_ID", "CODE"],
                    "expressions": {},
                    "uniqueness": "UNIQUE",
                },
            }
        }
        ob_constraints = {
            ("A", "T1"): {
                "PK_T1": {
                    "type": "P",
                    "columns": ["ID"],
                    "index_name": "PK_T1",
                },
                "UK_T1": {
                    "type": "U",
                    "columns": ["CODE"],
                    "index_name": "UK_T1",
                },
            }
        }
        oracle_meta = self._make_oracle_meta(indexes=oracle_indexes)
        ob_meta = self._make_ob_meta(indexes=ob_indexes, constraints=ob_constraints)
        ob_meta = ob_meta._replace(temporary_tables={("A", "T1")})

        ok, mismatch = sdr.compare_indexes_for_table(
            oracle_meta,
            ob_meta,
            "A",
            "T1",
            "A",
            "T1",
        )
        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_find_source_backing_constraint_for_index(self):
        source_constraints = {
            "PK_T1": {
                "type": "P",
                "columns": ["ID"],
                "index_name": "IX_PK_T1",
            },
            "UK_T1_CODE": {
                "type": "U",
                "columns": ["CODE"],
            },
        }
        hit = sdr.find_source_backing_constraint_for_index(source_constraints, "ix_pk_t1")
        self.assertIsNotNone(hit)
        self.assertEqual(hit[0], "PK_T1")
        self.assertEqual(hit[1].get("type"), "P")

        fallback = sdr.find_source_backing_constraint_for_index(source_constraints, "UK_T1_CODE")
        self.assertIsNotNone(fallback)
        self.assertEqual(fallback[0], "UK_T1_CODE")

        miss = sdr.find_source_backing_constraint_for_index(source_constraints, "IX_BIZ")
        self.assertIsNone(miss)

    def test_has_equivalent_pk_uk_constraint(self):
        target_constraints = {
            "PK_T1": {"type": "P", "columns": ["ID"]},
            "CK_T1": {"type": "C", "columns": ["ID"]},
        }
        self.assertTrue(
            sdr.has_equivalent_pk_uk_constraint(target_constraints, ["ID"])
        )
        self.assertFalse(
            sdr.has_equivalent_pk_uk_constraint(target_constraints, ["CODE"])
        )

    def test_generate_fixup_skips_backing_index_when_constraint_planned(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": [],
        }
        extra_results = {
            "index_ok": [],
            "index_mismatched": [
                sdr.IndexMismatch(
                    table="TGT.T1",
                    missing_indexes={"NC_T1_PK"},
                    extra_indexes=set(),
                    detail_mismatch=[],
                )
            ],
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="TGT.T1",
                    missing_constraints={"NC_T1_PK"},
                    extra_constraints=set(),
                    detail_mismatch=[],
                    downgraded_pk_constraints=set(),
                )
            ],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta(
            indexes={
                ("SRC", "T1"): {
                    "NC_T1_PK": {
                        "columns": ["ID"],
                        "expressions": {},
                        "uniqueness": "UNIQUE",
                    }
                }
            },
            constraints={
                ("SRC", "T1"): {
                    "NC_T1_PK": {
                        "type": "P",
                        "columns": ["ID"],
                        "index_name": "NC_T1_PK",
                    }
                }
            },
        )
        ob_meta = self._make_ob_meta(indexes={}, constraints={})
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"INDEX", "CONSTRAINT"},
            "fixup_schema_list": set(),
            "name_collision_mode": "off",
        }
        fixup_skip_summary = {}
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value="4.2.5.7"):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    {"SRC.T1": {"TABLE": "TGT.T1"}},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={},
                    fixup_skip_summary=fixup_skip_summary,
                )
            self.assertTrue((Path(tmp_dir) / "constraint" / "TGT.NC_T1_PK.sql").exists())
            self.assertFalse((Path(tmp_dir) / "index" / "TGT.NC_T1_PK.sql").exists())
            self.assertEqual(
                fixup_skip_summary.get("INDEX", {}).get("skipped", {}).get("backing_constraint_planned"),
                1
            )

    def test_generate_fixup_keeps_constraint_validate_later_for_validated_source(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": [],
        }
        extra_results = {
            "index_ok": [],
            "index_mismatched": [],
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="TGT.T1",
                    missing_constraints={"CK1"},
                    extra_constraints=set(),
                    detail_mismatch=[],
                    downgraded_pk_constraints=set(),
                )
            ],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta(
            constraints={
                ("SRC", "T1"): {
                    "CK1": {
                        "type": "C",
                        "columns": ["C1"],
                        "search_condition": '"C1" IS NOT NULL',
                        "status": "ENABLED",
                        "validated": "VALIDATED",
                    }
                }
            },
        )
        ob_meta = self._make_ob_meta(constraints={})._replace(objects_by_type={"TABLE": {"TGT.T1"}})
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"CONSTRAINT"},
            "fixup_schema_list": set(),
            "name_collision_mode": "off",
            "constraint_missing_fixup_validate_mode": "safe_novalidate",
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value="4.2.5.7"):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    {"SRC.T1": {"TABLE": "TGT.T1"}},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=Path(tmp_dir),
                    report_timestamp="20260317_000000",
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={},
                )
            self.assertTrue((Path(tmp_dir) / "constraint" / "TGT.CK1.sql").exists())
            self.assertTrue((Path(tmp_dir) / "constraint_validate_later" / "TGT.constraint_validate.sql").exists())
            self.assertEqual(settings.get("_constraint_validate_deferred_count"), 1)

    def test_generate_fixup_skips_constraint_validate_later_for_not_validated_source(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": [],
        }
        extra_results = {
            "index_ok": [],
            "index_mismatched": [],
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="TGT.T1",
                    missing_constraints={"CK1"},
                    extra_constraints=set(),
                    detail_mismatch=[],
                    downgraded_pk_constraints=set(),
                )
            ],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta(
            constraints={
                ("SRC", "T1"): {
                    "CK1": {
                        "type": "C",
                        "columns": ["C1"],
                        "search_condition": '"C1" IS NOT NULL',
                        "status": "ENABLED",
                        "validated": "NOT VALIDATED",
                    }
                }
            },
        )
        ob_meta = self._make_ob_meta(constraints={})._replace(objects_by_type={"TABLE": {"TGT.T1"}})
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"CONSTRAINT"},
            "fixup_schema_list": set(),
            "name_collision_mode": "off",
            "constraint_missing_fixup_validate_mode": "safe_novalidate",
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value="4.2.5.7"):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    {"SRC.T1": {"TABLE": "TGT.T1"}},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=Path(tmp_dir),
                    report_timestamp="20260317_000000",
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={},
                )
            self.assertTrue((Path(tmp_dir) / "constraint" / "TGT.CK1.sql").exists())
            self.assertFalse((Path(tmp_dir) / "constraint_validate_later" / "TGT.constraint_validate.sql").exists())
            self.assertEqual(settings.get("_constraint_validate_deferred_count"), 0)

    def test_generate_fixup_rebuilds_disabled_system_notnull_check(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": [],
        }
        extra_results = {
            "index_ok": [],
            "index_mismatched": [],
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="TGT.T1",
                    missing_constraints={"SYS_C1"},
                    extra_constraints=set(),
                    detail_mismatch=[],
                    downgraded_pk_constraints=set(),
                )
            ],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta(
            constraints={
                ("SRC", "T1"): {
                    "SYS_C1": {
                        "type": "C",
                        "columns": ["C1"],
                        "search_condition": '"C1" IS NOT NULL',
                        "status": "DISABLED",
                        "validated": "NOT VALIDATED",
                    }
                }
            },
        )
        ob_meta = self._make_ob_meta(constraints={})._replace(objects_by_type={"TABLE": {"TGT.T1"}})
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"CONSTRAINT"},
            "fixup_schema_list": set(),
            "name_collision_mode": "off",
            "constraint_missing_fixup_validate_mode": "safe_novalidate",
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(sdr, "fetch_dbcat_schema_objects", return_value=({}, {})), \
                 mock.patch.object(sdr, "oracle_get_ddl_batch", return_value={}), \
                 mock.patch.object(sdr, "get_oceanbase_version", return_value="4.2.5.7"):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    master_list,
                    oracle_meta,
                    {"SRC.T1": {"TABLE": "TGT.T1"}},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=ob_meta,
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=Path(tmp_dir),
                    report_timestamp="20260325_000000",
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={},
                )
            sql_path = Path(tmp_dir) / "constraint" / "TGT.SYS_C1.sql"
            self.assertTrue(sql_path.exists())
            sql_text = sql_path.read_text(encoding="utf-8")
            self.assertIn('CHECK ("C1" IS NOT NULL)', sql_text.upper())
            self.assertIn("DISABLE", sql_text.upper())
            self.assertFalse((Path(tmp_dir) / "constraint_validate_later" / "TGT.constraint_validate.sql").exists())

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

    def test_build_blacklist_rehydration_state_rehydrates_existing_spe_table(self):
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "RID_COL": {"data_type": "ROWID"},
                "BIZ_ID": {"data_type": "NUMBER"},
            }
        })._replace(blacklist_tables={
            ("SRC", "T1"): {
                ("SPE", "ROWID"): sdr.BlacklistEntry("SPE", "ROWID", "RULE=UNSUPPORTED_TYPES"),
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"TGT.T1"}},
            {("TGT", "T1"): {"RID_COL": {"data_type": "VARCHAR2"}, "BIZ_ID": {"data_type": "NUMBER"}}},
        )
        state = sdr.build_blacklist_rehydration_state(
            oracle_meta.blacklist_tables,
            {("SRC", "T1"): ("TGT", "T1")},
            oracle_meta,
            ob_meta,
            "rehydrate_if_present",
        )
        self.assertEqual(state.effective_blacklist_tables, {})
        self.assertIn(("SRC", "T1"), state.rehydrated_table_keys)
        self.assertEqual(state.transformed_columns_by_table[("SRC", "T1")], {"RID_COL"})
        self.assertIn(("SRC", "T1"), state.manual_trigger_table_keys)

    def test_build_blacklist_rehydration_state_keeps_temporary_table_blocked(self):
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "RID_COL": {"data_type": "ROWID"},
            }
        })._replace(blacklist_tables={
            ("SRC", "T1"): {
                ("TEMPORARY_TABLE", "TEMPORARY"): sdr.BlacklistEntry("TEMPORARY_TABLE", "TEMPORARY", "RULE=TEMP"),
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"TGT.T1"}},
            {("TGT", "T1"): {"RID_COL": {"data_type": "VARCHAR2"}}},
        )
        state = sdr.build_blacklist_rehydration_state(
            oracle_meta.blacklist_tables,
            {("SRC", "T1"): ("TGT", "T1")},
            oracle_meta,
            ob_meta,
            "rehydrate_if_present",
        )
        self.assertIn(("SRC", "T1"), state.effective_blacklist_tables)
        self.assertNotIn(("SRC", "T1"), state.rehydrated_table_keys)

    def test_check_primary_objects_ignores_transformed_blacklist_columns(self):
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "RID_COL": {
                    "data_type": "ROWID",
                    "data_length": None,
                    "char_length": None,
                    "char_used": None,
                    "data_precision": None,
                    "data_scale": None,
                    "nullable": "Y",
                    "data_default": None,
                    "hidden": False,
                    "virtual": False,
                    "virtual_expr": None,
                },
                "BIZ_ID": {
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
                },
            }
        })
        ob_meta = self._make_ob_meta_with_columns(
            {"TABLE": {"TGT.T1"}},
            {
                ("TGT", "T1"): {
                    "RID_COL": {
                        "data_type": "VARCHAR2",
                        "data_length": 64,
                        "char_length": 64,
                        "char_used": "B",
                        "data_precision": None,
                        "data_scale": None,
                        "nullable": "Y",
                        "data_default": None,
                        "hidden": False,
                        "virtual": False,
                        "virtual_expr": None,
                    },
                    "BIZ_ID": {
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
                    },
                }
            }
        )
        results = sdr.check_primary_objects(
            master_list,
            [],
            ob_meta,
            oracle_meta,
            enabled_primary_types={"TABLE"},
            transformed_blacklist_columns_by_table={("SRC", "T1"): {"RID_COL"}},
        )
        self.assertEqual(results["mismatched"], [])

    def test_classify_unsupported_indexes_marks_transformed_blacklist_dependency(self):
        extra_results = {
            "index_ok": [],
            "index_mismatched": [
                sdr.IndexMismatch(
                    table="TGT.T1",
                    missing_indexes={"IDX_RID"},
                    extra_indexes=set(),
                    detail_mismatch=["索引列 ['RID_COL'] 在目标端未找到。"],
                )
            ],
        }
        oracle_meta = self._make_oracle_meta(
            indexes={
                ("SRC", "T1"): {
                    "IDX_RID": {"columns": ["RID_COL"], "expressions": {}, "uniqueness": "NONUNIQUE"}
                }
            }
        )
        rows = sdr.classify_unsupported_indexes(
            extra_results,
            oracle_meta,
            {("SRC", "T1"): ("TGT", "T1")},
            transformed_blacklist_columns_by_table={("SRC", "T1"): {"RID_COL"}},
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].reason_code, sdr.BLACKLIST_REHYDRATION_REASON_CODE)
        self.assertEqual(extra_results["index_mismatched"], [])

    def test_classify_unsupported_constraints_marks_transformed_blacklist_dependency(self):
        extra_results = {
            "constraint_ok": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="TGT.T1",
                    missing_constraints={"CK_RID"},
                    extra_constraints=set(),
                    detail_mismatch=["CHECK: 源约束 CK_RID (条件 \"RID_COL\" IS NOT NULL) 在目标端未找到。"],
                    downgraded_pk_constraints=set(),
                )
            ],
        }
        oracle_meta = self._make_oracle_meta(
            constraints={
                ("SRC", "T1"): {
                    "CK_RID": {
                        "type": "C",
                        "columns": ["RID_COL"],
                        "search_condition": '"RID_COL" IS NOT NULL',
                        "status": "ENABLED",
                        "validated": "VALIDATED",
                    }
                }
            }
        )
        rows = sdr.classify_unsupported_check_constraints(
            extra_results,
            oracle_meta,
            {("SRC", "T1"): ("TGT", "T1")},
            transformed_blacklist_columns_by_table={("SRC", "T1"): {"RID_COL"}},
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].reason_code, sdr.BLACKLIST_REHYDRATION_REASON_CODE)
        self.assertEqual(extra_results["constraint_mismatched"], [])

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
                },
                {
                    "id": "SAMPLE_DISABLED",
                    "tag": "disabled",
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
        self.assertEqual(len(rules), 2)
        self.assertEqual(rules[0].rule_id, "SAMPLE")
        self.assertTrue(rules[0].enabled)
        self.assertEqual(rules[1].rule_id, "SAMPLE_DISABLED")
        self.assertFalse(rules[1].enabled)

    def test_load_blacklist_rules_strict_parse_error_raises_fatal(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            handle.write('{"rules":[{"id":"R1","sql":"SELECT 1"},]}')
            path = handle.name
        try:
            with self.assertRaises(sdr.FatalError):
                sdr.load_blacklist_rules(path, strict=True)
        finally:
            Path(path).unlink(missing_ok=True)

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

    def test_build_blacklist_name_pattern_clause(self):
        clause = sdr.build_blacklist_name_pattern_clause(["_RENAME", "AB%C", "X!Y"])
        self.assertIn("UPPER(TABLE_NAME) LIKE '%!_RENAME%'", clause)
        self.assertIn("UPPER(TABLE_NAME) LIKE '%AB!%C%'", clause)
        self.assertIn("UPPER(TABLE_NAME) LIKE '%X!!Y%'", clause)
        self.assertIn("ESCAPE '!'", clause)

    def test_load_blacklist_name_patterns_inline_and_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as handle:
            handle.write("#comment\n_RENAME\n;\n  \nAA_BB\n")
            path = handle.name
        try:
            patterns = sdr.load_blacklist_name_patterns("_rename, cc_dd", path)
        finally:
            Path(path).unlink(missing_ok=True)
        self.assertEqual(patterns, ["_rename", "cc_dd", "AA_BB"])

    def test_is_rename_only_blacklist(self):
        rename_entries = {
            ("RENAME", "RENAME"): sdr.BlacklistEntry("RENAME", "RENAME", "RULE=R1")
        }
        mixed_entries = {
            ("RENAME", "RENAME"): sdr.BlacklistEntry("RENAME", "RENAME", "RULE=R1"),
            ("SPE", "XMLTYPE"): sdr.BlacklistEntry("SPE", "XMLTYPE", "RULE=R2"),
        }
        self.assertTrue(sdr.is_rename_only_blacklist(rename_entries))
        self.assertFalse(sdr.is_rename_only_blacklist(mixed_entries))

    def test_filter_nodes_for_rename_blacklist_exclusion(self):
        source_objects = {
            "A.T_RENAME_1": {"TABLE"},
            "A.V_RENAME_DEP": {"VIEW"},
            "A.P_RENAME_DEP": {"PROCEDURE"},
            "A.KEEP_T": {"TABLE"},
        }
        dependency_graph = {
            ("A.V_RENAME_DEP", "VIEW"): {("A.T_RENAME_1", "TABLE")},
            ("A.P_RENAME_DEP", "PROCEDURE"): {("A.V_RENAME_DEP", "VIEW")},
        }
        seed_nodes = {("A.T_RENAME_1", "TABLE")}
        blocked = sdr.build_blocked_dependency_map(
            dependency_graph,
            seed_nodes,
            source_objects=source_objects,
            object_parent_map={}
        )
        excluded_nodes = set(seed_nodes) | set(blocked.keys())

        master_list = [
            ("A.T_RENAME_1", "A.T_RENAME_1", "TABLE"),
            ("A.V_RENAME_DEP", "A.V_RENAME_DEP", "VIEW"),
            ("A.P_RENAME_DEP", "A.P_RENAME_DEP", "PROCEDURE"),
            ("A.KEEP_T", "A.KEEP_T", "TABLE"),
        ]
        filtered_master = sdr.filter_master_list_by_nodes(master_list, excluded_nodes)
        self.assertEqual(filtered_master, [("A.KEEP_T", "A.KEEP_T", "TABLE")])

        filtered_source = sdr.filter_source_objects_by_nodes(source_objects, excluded_nodes)
        self.assertEqual(filtered_source, {"A.KEEP_T": {"TABLE"}})

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

    def test_format_oracle_column_type_number_star(self):
        info_star = {"data_type": "NUMBER", "data_precision": None, "data_scale": 2}
        info_star_zero = {"data_type": "NUMBER", "data_precision": None, "data_scale": 0}
        self.assertEqual(sdr.format_oracle_column_type(info_star), "NUMBER(*,2)")
        self.assertEqual(sdr.format_oracle_column_type(info_star_zero), "NUMBER(*)")

    def test_format_oracle_column_type_varchar_char_semantics(self):
        info = {
            "data_type": "VARCHAR2",
            "char_used": "C",
            "char_length": 10,
            "data_length": 20,
        }
        self.assertEqual(sdr.format_oracle_column_type(info), "VARCHAR2(10 CHAR)")
        self.assertEqual(
            sdr.format_oracle_column_type(info, prefer_ob_varchar=True),
            "VARCHAR(10 CHAR)"
        )

    def test_generate_alter_for_table_columns_char_mismatch_uses_inner_char_semantics(self):
        oracle_meta = self._make_oracle_meta_with_columns({
            ("SRC", "T1"): {
                "VC_C": {
                    "data_type": "VARCHAR2",
                    "char_used": "C",
                    "char_length": 10,
                    "data_length": 20,
                    "nullable": "Y",
                    "data_default": None,
                    "virtual": False,
                    "virtual_expr": None,
                }
            }
        })
        sql = sdr.generate_alter_for_table_columns(
            oracle_meta=oracle_meta,
            src_schema="SRC",
            src_table="T1",
            tgt_schema="TGT",
            tgt_table="T1",
            missing_cols=set(),
            extra_cols=set(),
            length_mismatches=[sdr.ColumnLengthIssue("VC_C", 10, 10, 10, "char_mismatch")],
            type_mismatches=[],
            drop_sys_c_columns=False
        )
        self.assertIsNotNone(sql)
        self.assertIn("MODIFY (VC_C VARCHAR(10 CHAR));", sql)
        self.assertNotIn("VARCHAR(10) CHAR", sql)

    def test_load_remap_rules_strips_inline_comments(self):
        content = (
            "HERO_B.TREASURES = OLYMPIAN_A.LEGEND_TREASURES # Renamed from TREASURES\n"
            "MONSTER_A.SYN_SECRET = TITAN_B.SYN_SECRET_LAIR -- comment\n"
            "X.T1 = Y.T#1\n"
        )
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as fp:
            fp.write(content)
            remap_path = fp.name
        try:
            rules = sdr.load_remap_rules(remap_path)
        finally:
            Path(remap_path).unlink(missing_ok=True)
        self.assertEqual(rules.get("HERO_B.TREASURES"), "OLYMPIAN_A.LEGEND_TREASURES")
        self.assertEqual(rules.get("MONSTER_A.SYN_SECRET"), "TITAN_B.SYN_SECRET_LAIR")
        # 对象名中的 #（无前置空白）应保留
        self.assertEqual(rules.get("X.T1"), "Y.T#1")

    def test_normalize_check_constraint_expression_casefold(self):
        expr_upper = '"VOUCHER_STATUS" IS NOT NULL'
        expr_lower = '"VOUCHER_STATUS" is not null'
        norm_upper = sdr.normalize_check_constraint_expression(expr_upper, "NN1")
        norm_lower = sdr.normalize_check_constraint_expression(expr_lower, "NN1")
        self.assertEqual(norm_upper, norm_lower)

    def test_normalize_check_constraint_expression_preserves_literals(self):
        expr = "status = 'aBc' and flag = 'XyZ'"
        norm = sdr.normalize_check_constraint_expression(expr, "C1")
        self.assertIn("'aBc'", norm)
        self.assertIn("'XyZ'", norm)
        self.assertIn("AND", norm)

    def test_normalize_check_constraint_expression_redundant_parentheses(self):
        expr_oracle = "A > 0 AND B IN (1,2,3)"
        expr_ob = '("A" > 0) and ("B" in (1,2,3))'
        norm_oracle = sdr.normalize_check_constraint_expression(expr_oracle, "C1")
        norm_ob = sdr.normalize_check_constraint_expression(expr_ob, "C2")
        self.assertEqual(norm_oracle, norm_ob)

    def test_normalize_check_constraint_expression_between_equivalence(self):
        expr_oracle = "QTY BETWEEN 0 AND 999"
        expr_ob = '(("QTY" >= 0) and ("QTY" <= 999))'
        norm_oracle = sdr.normalize_check_constraint_expression(expr_oracle, "C1")
        norm_ob = sdr.normalize_check_constraint_expression(expr_ob, "C2")
        self.assertEqual(norm_oracle, norm_ob)

    def test_normalize_check_constraint_expression_like_escape_rewrite(self):
        expr_oracle = "CODE LIKE 'X%'"
        expr_ob = '("CODE" like replace(\'X%\',\'\\\\\',\'\\\\\\\\\') escape \'\\\\\')'
        norm_oracle = sdr.normalize_check_constraint_expression(expr_oracle, "C1")
        norm_ob = sdr.normalize_check_constraint_expression(expr_ob, "C2")
        self.assertEqual(norm_oracle, norm_ob)

    def test_normalize_check_constraint_expression_in_spacing_equivalence(self):
        expr_oracle = "eai_flag in('0','1','2','3')"
        expr_ob = '("EAI_FLAG" in (\'0\',\'1\',\'2\',\'3\'))'
        norm_oracle = sdr.normalize_check_constraint_expression(expr_oracle, "C1")
        norm_ob = sdr.normalize_check_constraint_expression(expr_ob, "C2")
        self.assertEqual(norm_oracle, norm_ob)

    def test_normalize_check_constraint_expression_function_spacing_equivalence(self):
        expr_oracle = "NVL (col_a, 0) > 0 AND EXISTS (SELECT 1 FROM dual)"
        expr_ob = '(nvl(col_a,0)>0 and exists(select 1 from dual))'
        norm_oracle = sdr.normalize_check_constraint_expression(expr_oracle, "C1")
        norm_ob = sdr.normalize_check_constraint_expression(expr_ob, "C2")
        self.assertEqual(norm_oracle, norm_ob)

    def test_normalize_index_expression_casefold(self):
        cols = ["SYS_NC0004$"]
        expr_upper = 'DECODE(\"CMS_RESULT\",\'PBB00\',\"BUSINESS_UNIQUE_ID\",NULL,\"BUSINESS_UNIQUE_ID\")'
        expr_lower = 'decode(\"CMS_RESULT\",\'PBB00\',\"BUSINESS_UNIQUE_ID\",null,\"BUSINESS_UNIQUE_ID\")'
        norm_upper = sdr.normalize_index_columns(cols, {1: expr_upper})
        norm_lower = sdr.normalize_index_columns(cols, {1: expr_lower})
        self.assertEqual(norm_upper, norm_lower)

    def test_clean_view_ddl_removes_force(self):
        ddl = "CREATE OR REPLACE FORCE VIEW A.V1 AS SELECT 1 FROM DUAL"
        cleaned = sdr.clean_view_ddl_for_oceanbase(ddl)
        self.assertIn("CREATE OR REPLACE VIEW", cleaned.upper())
        self.assertNotIn("FORCE VIEW", cleaned.upper())

    def test_clean_view_ddl_force_removal_preserves_check_option_by_version(self):
        ddl = (
            "CREATE OR REPLACE FORCE VIEW A.V1 AS SELECT 1 FROM DUAL "
            "WITH CHECK OPTION"
        )
        cleaned_new = sdr.clean_view_ddl_for_oceanbase(ddl, ob_version="4.2.5.7")
        cleaned_old = sdr.clean_view_ddl_for_oceanbase(ddl, ob_version="4.2.5.6")
        self.assertNotIn("FORCE VIEW", cleaned_new.upper())
        self.assertNotIn("FORCE VIEW", cleaned_old.upper())
        self.assertIn("WITH CHECK OPTION", cleaned_new.upper())
        self.assertNotIn("WITH CHECK OPTION", cleaned_old.upper())

    def test_clean_view_ddl_force_removal_preserves_read_only(self):
        ddl = (
            "CREATE OR REPLACE FORCE VIEW A.V1 AS SELECT 1 FROM DUAL "
            "WITH READ ONLY"
        )
        cleaned = sdr.clean_view_ddl_for_oceanbase(ddl, ob_version="4.2.5.7")
        self.assertNotIn("FORCE VIEW", cleaned.upper())
        self.assertIn("WITH READ ONLY", cleaned.upper())

    def test_classify_missing_objects_blocks_x_dollar(self):
        tv_results = {
            "missing": [("VIEW", "TGT.V1", "SRC.V1")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        full_mapping = {"SRC.V1": {"VIEW": "TGT.V1"}}
        source_objects = {"SRC.V1": {"VIEW"}}
        deps = {("SRC", "V1", "VIEW", "SYS", "X$KQF", "TABLE")}
        dependency_graph = sdr.build_dependency_graph(deps)
        settings = {"view_compat_rules": {}, "view_dblink_policy": "block"}
        ora_cfg = {"user": "u", "password": "p", "dsn": "d"}

        with mock.patch.object(
            sdr,
            "oracle_get_views_ddl_batch",
            return_value={("SRC", "V1"): "CREATE VIEW V1 AS SELECT * FROM X$KQF"}
        ):
            summary = sdr.classify_missing_objects(
                ora_cfg,
                settings,
                tv_results,
                {
                    "index_ok": [], "index_mismatched": [],
                    "constraint_ok": [], "constraint_mismatched": [],
                    "sequence_ok": [], "sequence_mismatched": [],
                    "trigger_ok": [], "trigger_mismatched": [],
                },
                oracle_meta,
                ob_meta,
                full_mapping,
                source_objects,
                dependency_graph=dependency_graph,
                object_parent_map=None,
                table_target_map={},
                synonym_meta_map={}
            )

        view_row = next(row for row in summary.missing_detail_rows if row.src_full == "SRC.V1")
        self.assertEqual(view_row.support_state, sdr.SUPPORT_STATE_UNSUPPORTED)
        self.assertEqual(view_row.reason_code, "VIEW_X$")

    def test_classify_missing_objects_allows_user_defined_x_dollar(self):
        tv_results = {
            "missing": [("VIEW", "TGT.V1", "SRC.V1")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        full_mapping = {"SRC.V1": {"VIEW": "TGT.V1"}, "SRC.X$CUSTOM": {"TABLE": "TGT.X$CUSTOM"}}
        source_objects = {"SRC.V1": {"VIEW"}, "SRC.X$CUSTOM": {"TABLE"}}
        deps = {("SRC", "V1", "VIEW", "SRC", "X$CUSTOM", "TABLE")}
        dependency_graph = sdr.build_dependency_graph(deps)
        settings = {"view_compat_rules": {}, "view_dblink_policy": "block"}
        ora_cfg = {"user": "u", "password": "p", "dsn": "d"}

        with mock.patch.object(
            sdr,
            "oracle_get_views_ddl_batch",
            return_value={("SRC", "V1"): "CREATE VIEW V1 AS SELECT * FROM X$CUSTOM"}
        ):
            summary = sdr.classify_missing_objects(
                ora_cfg,
                settings,
                tv_results,
                {
                    "index_ok": [], "index_mismatched": [],
                    "constraint_ok": [], "constraint_mismatched": [],
                    "sequence_ok": [], "sequence_mismatched": [],
                    "trigger_ok": [], "trigger_mismatched": [],
                },
                oracle_meta,
                ob_meta,
                full_mapping,
                source_objects,
                dependency_graph=dependency_graph,
                object_parent_map=None,
                table_target_map={},
                synonym_meta_map={}
            )

        view_row = next(row for row in summary.missing_detail_rows if row.src_full == "SRC.V1")
        self.assertEqual(view_row.support_state, sdr.SUPPORT_STATE_SUPPORTED)

    def test_detect_view_fixup_risks_identifies_force(self):
        ddl = "CREATE OR REPLACE FORCE VIEW A.V1 AS SELECT 1 FROM DUAL"
        risks = sdr.detect_view_fixup_risks(ddl)
        self.assertIn("VIEW_FORCE_RESIDUAL", risks)

    def test_classify_missing_objects_marks_view_risky(self):
        tv_results = {
            "missing": [("VIEW", "TGT.V1", "SRC.V1")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        oracle_meta = self._make_oracle_meta()
        ob_meta = self._make_ob_meta()
        full_mapping = {"SRC.V1": {"VIEW": "TGT.V1"}}
        source_objects = {"SRC.V1": {"VIEW"}}
        settings = {"view_compat_rules": {}, "view_dblink_policy": "allow"}
        ora_cfg = {"user": "u", "password": "p", "dsn": "d"}

        with mock.patch.object(
            sdr,
            "oracle_get_views_ddl_batch",
            return_value={("SRC", "V1"): "CREATE VIEW V1 AS SELECT * FROM T1"}
        ), mock.patch.object(
            sdr,
            "detect_view_fixup_risks",
            return_value=["VIEW_FORCE_RESIDUAL"]
        ):
            summary = sdr.classify_missing_objects(
                ora_cfg,
                settings,
                tv_results,
                {
                    "index_ok": [], "index_mismatched": [],
                    "constraint_ok": [], "constraint_mismatched": [],
                    "sequence_ok": [], "sequence_mismatched": [],
                    "trigger_ok": [], "trigger_mismatched": [],
                },
                oracle_meta,
                ob_meta,
                full_mapping,
                source_objects,
                dependency_graph=None,
                object_parent_map=None,
                table_target_map={},
                synonym_meta_map={}
            )

        view_row = next(row for row in summary.missing_detail_rows if row.src_full == "SRC.V1")
        self.assertEqual(view_row.support_state, sdr.SUPPORT_STATE_RISKY)
        self.assertEqual(view_row.reason_code, "VIEW_FIXUP_RISK")
        self.assertIn("VIEW_FORCE_RESIDUAL", view_row.detail)

    def test_split_view_grants(self):
        view_targets = {"TGT.V1"}
        expected_pairs = {("TGT.V1", "VIEW", "TGT2.T1", "TABLE")}
        grants = {
            "TGT": {
                sdr.ObjectGrantEntry("SELECT", "TGT2.T1", True)
            },
            "APP": {
                sdr.ObjectGrantEntry("SELECT", "TGT.V1", False)
            }
        }
        prereq, post, remaining, refresh_targets = sdr.split_view_grants(view_targets, expected_pairs, grants)
        self.assertIn("TGT", prereq)
        self.assertIn(sdr.ObjectGrantEntry("SELECT", "TGT2.T1", True), prereq["TGT"])
        self.assertIn("APP", post)
        self.assertIn(sdr.ObjectGrantEntry("SELECT", "TGT.V1", False), post["APP"])
        self.assertEqual(remaining, {})
        self.assertEqual(refresh_targets, {"TGT.V1"})

    def test_split_view_grants_routes_existing_view_dml_chain(self):
        view_targets = {"VOWNER.V1"}
        expected_pairs = {("VOWNER.V1", "VIEW", "TOWNER.T1", "TABLE")}
        grants = {
            "VOWNER": {
                sdr.ObjectGrantEntry("UPDATE", "TOWNER.T1", True),
            },
            "U3": {
                sdr.ObjectGrantEntry("UPDATE", "VOWNER.V1", True),
            },
        }
        prereq, post, remaining, refresh_targets = sdr.split_view_grants(view_targets, expected_pairs, grants)
        self.assertEqual(prereq, {
            "VOWNER": {sdr.ObjectGrantEntry("UPDATE", "TOWNER.T1", True)}
        })
        self.assertEqual(post, {
            "U3": {sdr.ObjectGrantEntry("UPDATE", "VOWNER.V1", True)}
        })
        self.assertEqual(remaining, {})
        self.assertEqual(refresh_targets, {"VOWNER.V1"})

    def test_compute_required_grants_skips_trigger_reference_type(self):
        expected_pairs = {
            ("A.P1", "PROCEDURE", "B.TRG_AUDIT", "TRIGGER"),
            ("A.P1", "PROCEDURE", "B.PKG_UTIL", "PACKAGE"),
        }
        grants = sdr.compute_required_grants(expected_pairs)
        self.assertNotIn(("EXECUTE", "B.TRG_AUDIT"), grants.get("A", set()))
        self.assertIn(("EXECUTE", "B.PKG_UTIL"), grants.get("A", set()))

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

    def test_adjust_ddl_prefers_non_identity_mapping(self):
        ddl = "CREATE OR REPLACE VIEW A.V AS SELECT * FROM A.T1;"
        adjusted = sdr.adjust_ddl_for_object(
            ddl,
            "A",
            "V",
            "A",
            "V",
            extra_identifiers=[
                (("A", "T1"), ("A", "T1")),
                (("A", "T1"), ("B", "T1")),
            ],
            obj_type="VIEW",
        )
        self.assertIn("B.T1", adjusted.upper())
        self.assertNotIn("FROM A.T1", adjusted.upper())

    def test_build_expected_dependency_pairs_skips_builtin_dual(self):
        deps = [
            sdr.DependencyRecord(
                owner="SRC",
                name="V1",
                object_type="VIEW",
                referenced_owner="PUBLIC",
                referenced_name="DUAL",
                referenced_type="SYNONYM",
            ),
            sdr.DependencyRecord(
                owner="SRC",
                name="V1",
                object_type="VIEW",
                referenced_owner="SRC",
                referenced_name="T1",
                referenced_type="TABLE",
            ),
        ]
        mapping = {"SRC.V1": {"VIEW": "TGT.V1"}}
        expected, skipped = sdr.build_expected_dependency_pairs(deps, mapping)
        self.assertEqual(expected, set())
        self.assertEqual(len(skipped), 2)
        reasons = {entry.referenced: entry.reason for entry in skipped}
        self.assertIn("PUBLIC.DUAL", reasons)
        self.assertIn("内建对象", reasons["PUBLIC.DUAL"])
        self.assertEqual(
            reasons["SRC.T1"],
            "被依赖对象未纳入受管范围或缺少 remap 规则，无法建立依赖。"
        )

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

    def test_parse_ddl_format_types_aliases(self):
        parsed = sdr.parse_ddl_format_types("package_body,TYPE_BODY,mview,table_alter,unknown")
        self.assertIn("PACKAGE BODY", parsed)
        self.assertIn("TYPE BODY", parsed)
        self.assertIn("MATERIALIZED VIEW", parsed)
        self.assertIn("TABLE_ALTER", parsed)
        self.assertNotIn("UNKNOWN", parsed)

    def test_resolve_sqlcl_executable_dir(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            sql_path = bin_dir / "sql"
            sql_path.write_text("#!/bin/sh\n", encoding="utf-8")
            sql_path.chmod(0o755)
            resolved = sdr.resolve_sqlcl_executable(str(root))
            self.assertEqual(resolved, sql_path)

    def test_strip_plsql_trailing_slash(self):
        ddl = "CREATE OR REPLACE PACKAGE P AS\nBEGIN NULL;\nEND P;\n/\n"
        cleaned, had_slash = sdr.strip_plsql_trailing_slash(ddl)
        self.assertTrue(had_slash)
        self.assertNotIn("\n/\n", cleaned)

    def test_format_fixup_outputs_formats_selected_types_and_restores_slash(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            fixup_dir = Path(tmp_dir) / "fixup"
            report_dir = Path(tmp_dir) / "reports"
            (fixup_dir / "view").mkdir(parents=True)
            (fixup_dir / "package").mkdir(parents=True)
            (fixup_dir / "grants_all").mkdir(parents=True)
            view_path = fixup_dir / "view" / "A.V1.sql"
            pkg_path = fixup_dir / "package" / "A.P1.sql"
            grant_path = fixup_dir / "grants_all" / "A.grants.sql"
            view_path.write_text("CREATE VIEW V1 AS SELECT 1 FROM DUAL;\n", encoding="utf-8")
            pkg_path.write_text(
                "CREATE OR REPLACE PACKAGE P AS\nBEGIN NULL;\nEND P;\n/\n",
                encoding="utf-8"
            )
            grant_path.write_text("GRANT SELECT ON A.T1 TO B;\n", encoding="utf-8")

            sqlcl_path = Path(tmp_dir) / "sql"
            sqlcl_path.write_text("#!/bin/sh\n", encoding="utf-8")
            sqlcl_path.chmod(0o755)

            settings = {
                "ddl_format_enable": True,
                "ddl_formatter": "sqlcl",
                "ddl_format_type_set": {"VIEW", "PACKAGE"},
                "ddl_format_batch_size": 10,
                "ddl_format_timeout": 5,
                "ddl_format_max_lines": 0,
                "ddl_format_max_bytes": 0,
                "ddl_format_fail_policy": "fallback",
                "sqlcl_bin": str(sqlcl_path),
                "sqlcl_profile_path": ""
            }

            def fake_run(cmd, capture_output, text, timeout, env):
                script_arg = cmd[-1]
                self.assertTrue(script_arg.startswith("@"))
                script_path = Path(script_arg[1:])
                lines = script_path.read_text(encoding="utf-8").splitlines()
                for line in lines:
                    if line.strip().upper().startswith("FORMAT FILE"):
                        parts = line.split('"')
                        in_path = parts[1]
                        out_path = parts[3]
                        content = Path(in_path).read_text(encoding="utf-8")
                        Path(out_path).write_text("-- formatted\n" + content, encoding="utf-8")
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with mock.patch.object(sdr.subprocess, "run", side_effect=fake_run):
                report_path = sdr.format_fixup_outputs(settings, fixup_dir, report_dir, "20240101")

            self.assertTrue(report_path and report_path.exists())
            view_text = view_path.read_text(encoding="utf-8")
            pkg_text = pkg_path.read_text(encoding="utf-8")
            grant_text = grant_path.read_text(encoding="utf-8")
            self.assertIn("-- formatted", view_text)
            self.assertIn("-- formatted", pkg_text)
            self.assertTrue(pkg_text.rstrip().endswith("/"))
            self.assertNotIn("-- formatted", grant_text)

    def test_format_fixup_outputs_skips_large_file_and_reports(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            fixup_dir = Path(tmp_dir) / "fixup"
            report_dir = Path(tmp_dir) / "reports"
            (fixup_dir / "view").mkdir(parents=True)
            view_path = fixup_dir / "view" / "A.V2.sql"
            view_path.write_text("line1\nline2\n", encoding="utf-8")

            sqlcl_path = Path(tmp_dir) / "sql"
            sqlcl_path.write_text("#!/bin/sh\n", encoding="utf-8")
            sqlcl_path.chmod(0o755)

            settings = {
                "ddl_format_enable": True,
                "ddl_formatter": "sqlcl",
                "ddl_format_type_set": {"VIEW"},
                "ddl_format_batch_size": 10,
                "ddl_format_timeout": 5,
                "ddl_format_max_lines": 1,
                "ddl_format_max_bytes": 0,
                "ddl_format_fail_policy": "fallback",
                "sqlcl_bin": str(sqlcl_path),
                "sqlcl_profile_path": ""
            }

            with mock.patch.object(sdr.subprocess, "run") as mocked_run:
                report_path = sdr.format_fixup_outputs(settings, fixup_dir, report_dir, "20240102")

            self.assertTrue(report_path and report_path.exists())
            self.assertFalse(mocked_run.called)
            report_text = report_path.read_text(encoding="utf-8")
            self.assertIn("size_lines>1", report_text)
            self.assertNotIn("-- formatted", view_path.read_text(encoding="utf-8"))

    def test_sanitize_view_ddl_repairs_inline_comment_collapse(self):
        ddl = (
            "CREATE OR REPLACE VIEW V1 AS\n"
            "SELECT a.col1, --说明 (a.col2 - a.col3) AS col2, a.col4\n"
            "FROM t\n"
        )
        cleaned = sdr.sanitize_view_ddl(ddl, column_names={"COL1", "COL2", "COL3", "COL4"})
        self.assertIn("--说明", cleaned)
        self.assertIn("\n (a.col2 - a.col3)", cleaned)

    def test_normalize_status_sync_modes(self):
        self.assertEqual(sdr.normalize_constraint_status_sync_mode("enabled"), "enabled_only")
        self.assertEqual(sdr.normalize_constraint_status_sync_mode("full"), "full")
        self.assertEqual(sdr.normalize_constraint_status_sync_mode(None), "full")
        self.assertEqual(sdr.normalize_constraint_status_sync_mode("bad"), "full")
        self.assertEqual(sdr.normalize_plain_not_null_fixup_mode(None), "runnable_if_no_nulls")
        self.assertEqual(sdr.normalize_plain_not_null_fixup_mode("safe"), "runnable_if_no_nulls")
        self.assertEqual(sdr.normalize_plain_not_null_fixup_mode("force"), "force_runnable")
        self.assertEqual(sdr.normalize_plain_not_null_fixup_mode("bad"), "runnable_if_no_nulls")
        self.assertEqual(
            sdr.normalize_constraint_missing_fixup_validate_mode("safe"),
            "safe_novalidate"
        )
        self.assertEqual(
            sdr.normalize_constraint_missing_fixup_validate_mode("source"),
            "source"
        )
        self.assertEqual(
            sdr.normalize_constraint_missing_fixup_validate_mode("force"),
            "force_validate"
        )
        self.assertEqual(
            sdr.normalize_constraint_missing_fixup_validate_mode("bad"),
            "safe_novalidate"
        )
        self.assertEqual(sdr.normalize_trigger_validity_sync_mode("on"), "compile")
        self.assertEqual(sdr.normalize_trigger_validity_sync_mode("off"), "off")
        self.assertEqual(sdr.normalize_trigger_validity_sync_mode("bad"), "compile")

    def test_apply_constraint_missing_validate_mode_to_ddl(self):
        ddl = (
            'ALTER TABLE "A"."T1" ADD CONSTRAINT "FK_T1" '
            'FOREIGN KEY ("C1") REFERENCES "A"."T2" ("ID")'
        )
        adjusted, keyword, reason = sdr.apply_constraint_missing_validate_mode_to_ddl(
            ddl,
            "safe_novalidate",
            "VALIDATED"
        )
        self.assertEqual(keyword, "NOVALIDATE")
        self.assertEqual(reason, "safe_novalidate")
        self.assertIn("ENABLE NOVALIDATE", adjusted.upper())

        adjusted_src, keyword_src, reason_src = sdr.apply_constraint_missing_validate_mode_to_ddl(
            ddl,
            "source",
            "VALIDATED"
        )
        self.assertEqual(keyword_src, "VALIDATE")
        self.assertEqual(reason_src, "source_validated")
        self.assertIn("ENABLE VALIDATE", adjusted_src.upper())

        adjusted_disabled, keyword_disabled, reason_disabled = sdr.apply_constraint_missing_validate_mode_to_ddl(
            ddl,
            "source",
            "VALIDATED",
            "DISABLED"
        )
        self.assertEqual(keyword_disabled, "DISABLE")
        self.assertEqual(reason_disabled, "source_disabled")
        self.assertIn("ADD CONSTRAINT", adjusted_disabled.upper())
        self.assertIn("DISABLE", adjusted_disabled.upper())
        self.assertNotIn("ENABLE VALIDATE", adjusted_disabled.upper())

    def test_apply_constraint_missing_validate_mode_to_ddl_pk_plain_add(self):
        ddl = (
            'ALTER TABLE "A"."T1" ADD CONSTRAINT "PK_T1" '
            'PRIMARY KEY ("ID")'
        )
        adjusted, keyword, reason = sdr.apply_constraint_missing_validate_mode_to_ddl(
            ddl,
            "source",
            "VALIDATED",
            "ENABLED",
            "P"
        )
        self.assertIsNone(keyword)
        self.assertEqual(reason, "pkuk_plain_add")
        self.assertIn('ADD CONSTRAINT "PK_T1" PRIMARY KEY', adjusted.upper())
        self.assertNotIn("ENABLE", adjusted.upper())
        self.assertNotIn("VALIDATE", adjusted.upper())

    def test_should_generate_constraint_validate_later(self):
        self.assertTrue(
            sdr.should_generate_constraint_validate_later("NOVALIDATE", "VALIDATED")
        )
        self.assertFalse(
            sdr.should_generate_constraint_validate_later("NOVALIDATE", "NOT VALIDATED")
        )
        self.assertTrue(
            sdr.should_generate_constraint_validate_later("NOVALIDATE", "UNKNOWN")
        )
        self.assertFalse(
            sdr.should_generate_constraint_validate_later("VALIDATE", "VALIDATED")
        )

    def test_build_trigger_status_fixup_sqls_compile_mode(self):
        row = sdr.TriggerStatusReportRow(
            trigger_full="OMS_USER.TRG_A",
            src_trigger_full="SRC.TRG_A",
            src_event="INSERT",
            tgt_event="INSERT",
            src_enabled="ENABLED",
            tgt_enabled="DISABLED",
            src_valid="VALID",
            tgt_valid="INVALID",
            detail="ENABLED,VALID",
        )
        sqls_off = sdr.build_trigger_status_fixup_sqls(row, "off")
        self.assertEqual(len(sqls_off), 1)
        self.assertIn("ENABLE", sqls_off[0])

        sqls_compile = sdr.build_trigger_status_fixup_sqls(row, "compile")
        self.assertEqual(len(sqls_compile), 2)
        self.assertIn("ENABLE", sqls_compile[0])
        self.assertIn("COMPILE", sqls_compile[1])

    def test_collect_constraint_status_drift_rows_semantic_match(self):
        oracle_meta = self._make_oracle_meta(constraints={
            ("OMS_USER", "T1"): {
                "CK_SRC": {
                    "type": "C",
                    "columns": [],
                    "search_condition": "VAL > 0",
                    "status": "ENABLED",
                    "validated": "VALIDATED",
                }
            }
        })
        ob_meta = self._make_ob_meta(constraints={
            ("OMS_USER", "T1"): {
                "CK_TGT": {
                    "type": "C",
                    "columns": [],
                    "search_condition": "(VAL > 0)",
                    "status": "DISABLED",
                    "validated": "NOT VALIDATED",
                }
            }
        })
        master_list = [("OMS_USER.T1", "OMS_USER.T1", "TABLE")]
        rows = sdr.collect_constraint_status_drift_rows(
            oracle_meta,
            ob_meta,
            master_list,
            full_object_mapping={},
            sync_mode="enabled_only",
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.src_constraint, "CK_SRC")
        self.assertEqual(row.tgt_constraint, "CK_TGT")
        self.assertIn("MATCH=CK_SRC->CK_TGT", row.detail)
        self.assertIn('ENABLE CONSTRAINT "CK_TGT"', row.action_sql)

    def test_collect_constraint_status_drift_rows_full_validated(self):
        oracle_meta = self._make_oracle_meta(constraints={
            ("OMS_USER", "T1"): {
                "CK_A": {
                    "type": "C",
                    "columns": [],
                    "search_condition": "VAL > 0",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                }
            }
        })
        ob_meta = self._make_ob_meta(constraints={
            ("OMS_USER", "T1"): {
                "CK_A": {
                    "type": "C",
                    "columns": [],
                    "search_condition": "VAL > 0",
                    "status": "ENABLED",
                    "validated": "VALIDATED",
                }
            }
        })
        rows = sdr.collect_constraint_status_drift_rows(
            oracle_meta,
            ob_meta,
            [("OMS_USER.T1", "OMS_USER.T1", "TABLE")],
            full_object_mapping={},
            sync_mode="full",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].detail, "VALIDATED")
        self.assertIn("ENABLE NOVALIDATE", rows[0].action_sql)

    def test_collect_constraint_status_drift_rows_notnull_semantic_match_uses_ob_auto_constraint(self):
        oracle_meta = self._make_oracle_meta(constraints={
            ("LIFEDATA", "T1"): {
                "CK_NN_C1": {
                    "type": "C",
                    "columns": ["C1"],
                    "search_condition": '"C1" IS NOT NULL',
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                }
            }
        })
        ob_meta = self._make_ob_meta(constraints={("UWSDATA", "T1"): {}})
        ob_meta = ob_meta._replace(enabled_notnull_check_groups={
            ("UWSDATA", "T1"): {
                "C1": (
                    sdr.NotnullCheckEntry("T1_OBCHECK_1", '"C1" IS NOT NULL', "ENABLED", "VALIDATED", True, False),
                )
            }
        })
        rows = sdr.collect_constraint_status_drift_rows(
            oracle_meta,
            ob_meta,
            [("LIFEDATA.T1", "UWSDATA.T1", "TABLE")],
            full_object_mapping={},
            sync_mode="full",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].src_constraint, "CK_NN_C1")
        self.assertEqual(rows[0].tgt_constraint, "T1_OBCHECK_1")
        self.assertEqual(rows[0].detail, "VALIDATED; MATCH=CK_NN_C1->T1_OBCHECK_1")
        self.assertIn('ENABLE NOVALIDATE CONSTRAINT "T1_OBCHECK_1"', rows[0].action_sql)

    def test_collect_missing_table_constraint_status_rows_preserves_novalidate(self):
        oracle_meta = self._make_oracle_meta(constraints={
            ("LIFEDATA", "EM_PREM_TBL"): {
                "FK_EM_PREM_PARENT": {
                    "type": "R",
                    "columns": ["PID"],
                    "ref_table_owner": "LIFEDATA",
                    "ref_table_name": "PARENT_TBL",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
                "CK_EM_PREM_AMT": {
                    "type": "C",
                    "search_condition": "AMT > 0",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
                "PK_EM_PREM": {
                    "type": "P",
                    "columns": ["ID"],
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
            }
        })
        rows = sdr.collect_missing_table_constraint_status_rows(
            oracle_meta,
            [("LIFEDATA", "EM_PREM_TBL", "BASEDATA", "EM_PREM_TBL")],
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            [row.constraint_name for row in rows],
            ["CK_EM_PREM_AMT", "FK_EM_PREM_PARENT"]
        )
        self.assertTrue(all(row.detail == "POST_CREATE_NOT_VALIDATED" for row in rows))
        self.assertTrue(all("ENABLE NOVALIDATE CONSTRAINT" in row.action_sql for row in rows))

    def test_collect_missing_table_constraint_status_rows_honors_enabled_only_mode(self):
        oracle_meta = self._make_oracle_meta(constraints={
            ("LIFEDATA", "EM_PREM_TBL"): {
                "FK_EM_PREM_PARENT": {
                    "type": "R",
                    "columns": ["PID"],
                    "ref_table_owner": "LIFEDATA",
                    "ref_table_name": "PARENT_TBL",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
                "CK_EM_PREM_AMT": {
                    "type": "C",
                    "search_condition": "AMT > 0",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                },
            }
        })
        rows = sdr.collect_missing_table_constraint_status_rows(
            oracle_meta,
            [("LIFEDATA", "EM_PREM_TBL", "BASEDATA", "EM_PREM_TBL")],
            sync_mode="enabled_only",
        )
        self.assertEqual(rows, [])

    def test_collect_constraint_status_drift_rows_fk_same_name_generates_validate(self):
        oracle_meta = self._make_oracle_meta(constraints={
            ("OMS_USER", "CHILD"): {
                "FK_CHILD_PARENT": {
                    "type": "R",
                    "columns": ["PARENT_ID"],
                    "ref_table_owner": "OMS_USER",
                    "ref_table_name": "PARENT",
                    "delete_rule": "NO ACTION",
                    "update_rule": "NO ACTION",
                    "status": "ENABLED",
                    "validated": "VALIDATED",
                }
            }
        })
        ob_meta = self._make_ob_meta(constraints={
            ("OMS_USER", "CHILD"): {
                "FK_CHILD_PARENT": {
                    "type": "R",
                    "columns": ["PARENT_ID"],
                    "ref_table_owner": "OMS_USER",
                    "ref_table_name": "PARENT",
                    "delete_rule": "NO ACTION",
                    "update_rule": "NO ACTION",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                }
            }
        })
        rows = sdr.collect_constraint_status_drift_rows(
            oracle_meta,
            ob_meta,
            [("OMS_USER.CHILD", "OMS_USER.CHILD", "TABLE")],
            full_object_mapping={},
            sync_mode="full",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].detail, "VALIDATED")
        self.assertIn('ENABLE VALIDATE CONSTRAINT "FK_CHILD_PARENT"', rows[0].action_sql)
        self.assertEqual(rows[0].src_table_full, "OMS_USER.CHILD")

    def test_collect_constraint_status_drift_rows_pk_validated_report_only(self):
        oracle_meta = self._make_oracle_meta(constraints={
            ("OMS_USER", "T1"): {
                "PK_T1": {
                    "type": "P",
                    "columns": ["ID"],
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                }
            }
        })
        ob_meta = self._make_ob_meta(constraints={
            ("OMS_USER", "T1"): {
                "PK_T1": {
                    "type": "P",
                    "columns": ["ID"],
                    "status": "ENABLED",
                    "validated": "VALIDATED",
                }
            }
        })
        rows = sdr.collect_constraint_status_drift_rows(
            oracle_meta,
            ob_meta,
            [("OMS_USER.T1", "OMS_USER.T1", "TABLE")],
            full_object_mapping={},
            sync_mode="full",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].constraint_type, "P")
        self.assertEqual(rows[0].detail, "VALIDATED")
        self.assertEqual(rows[0].action_sql, "-")

    def test_generate_fixup_writes_constraint_status_when_fixup_schema_matches_source(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": [],
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
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"CONSTRAINT"},
            "fixup_schema_list": {"LIFEDATA"},
            "name_collision_mode": "off",
            "generate_fixup": "true",
            "generate_status_fixup": "true",
            "check_status_drift_type_set": {"CONSTRAINT"},
            "status_fixup_type_set": {"CONSTRAINT"},
        }
        row = sdr.ConstraintStatusDriftRow(
            table_full="BASEDATA.EM_PREM_TBL",
            src_table_full="LIFEDATA.EM_PREM_TBL",
            constraint_type="R",
            src_constraint="FK_SRC",
            tgt_constraint="FK_TGT",
            src_status="ENABLED",
            tgt_status="ENABLED",
            src_validated="VALIDATED",
            tgt_validated="NOT VALIDATED",
            detail="VALIDATED",
            action_sql='ALTER TABLE "BASEDATA"."EM_PREM_TBL" ENABLE VALIDATE CONSTRAINT "FK_TGT";',
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            sdr.generate_fixup_scripts(
                {"user": "u", "password": "p", "dsn": "d"},
                {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                settings,
                tv_results,
                extra_results,
                [("LIFEDATA.EM_PREM_TBL", "BASEDATA.EM_PREM_TBL", "TABLE")],
                self._make_oracle_meta(),
                {"LIFEDATA.EM_PREM_TBL": {"TABLE": "BASEDATA.EM_PREM_TBL"}},
                {},
                grant_plan=None,
                enable_grant_generation=False,
                dependency_report={"missing": [], "unexpected": [], "skipped": []},
                ob_meta=self._make_ob_meta(),
                expected_dependency_pairs=set(),
                synonym_metadata={},
                trigger_filter_entries=None,
                trigger_filter_enabled=False,
                package_results=None,
                report_dir=None,
                report_timestamp=None,
                support_state_map={},
                unsupported_table_keys=set(),
                view_compat_map={},
                trigger_status_rows=[],
                constraint_status_rows=[row],
            )
            self.assertTrue(
                (Path(tmp_dir) / "status" / "constraint" / "BASEDATA.FK_TGT.status.sql").exists()
            )

    def test_generate_fixup_writes_missing_table_postcreate_constraint_status(self):
        tv_results = {
            "missing": [("TABLE", "BASEDATA.EM_PREM_TBL", "LIFEDATA.EM_PREM_TBL")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": [],
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
        oracle_meta = self._make_oracle_meta(constraints={
            ("LIFEDATA", "EM_PREM_TBL"): {
                "FK_EM_PREM_PARENT": {
                    "type": "R",
                    "columns": ["PID"],
                    "ref_table_owner": "LIFEDATA",
                    "ref_table_name": "PARENT_TBL",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                }
            }
        })
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"TABLE", "CONSTRAINT"},
            "fixup_schema_list": {"LIFEDATA"},
            "name_collision_mode": "off",
            "generate_fixup": "true",
            "generate_status_fixup": "true",
            "check_status_drift_type_set": {"CONSTRAINT"},
            "status_fixup_type_set": {"CONSTRAINT"},
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            with mock.patch.object(
                sdr,
                "fetch_dbcat_schema_objects",
                return_value=(
                    {
                        "LIFEDATA": {
                            "TABLE": {
                                "EM_PREM_TBL": 'CREATE TABLE "LIFEDATA"."EM_PREM_TBL" ("ID" NUMBER, "PID" NUMBER);'
                            }
                        }
                    },
                    {}
                )
            ):
                sdr.generate_fixup_scripts(
                    {"user": "u", "password": "p", "dsn": "d"},
                    {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                    settings,
                    tv_results,
                    extra_results,
                    [("LIFEDATA.EM_PREM_TBL", "BASEDATA.EM_PREM_TBL", "TABLE")],
                    oracle_meta,
                    {"LIFEDATA.EM_PREM_TBL": {"TABLE": "BASEDATA.EM_PREM_TBL"}},
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=self._make_ob_meta(),
                    expected_dependency_pairs=set(),
                    synonym_metadata={},
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={},
                    trigger_status_rows=[],
                    constraint_status_rows=[],
                )
            sql_path = Path(tmp_dir) / "status" / "constraint" / "BASEDATA.FK_EM_PREM_PARENT.status.sql"
            self.assertTrue(sql_path.exists())
            self.assertIn(
                'ALTER TABLE "BASEDATA"."EM_PREM_TBL" ENABLE NOVALIDATE CONSTRAINT "FK_EM_PREM_PARENT";',
                sql_path.read_text(encoding="utf-8")
            )

    def test_generate_fixup_writes_trigger_status_when_fixup_schema_matches_source(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": [],
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
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"TRIGGER"},
            "fixup_schema_list": {"LIFEDATA"},
            "name_collision_mode": "off",
            "generate_fixup": "true",
            "generate_status_fixup": "true",
            "check_status_drift_type_set": {"TRIGGER"},
            "status_fixup_type_set": {"TRIGGER"},
            "trigger_validity_sync_mode": "compile",
        }
        row = sdr.TriggerStatusReportRow(
            trigger_full="BASEDATA.TRG_EM_PREM_TBL",
            src_trigger_full="LIFEDATA.TRG_EM_PREM_TBL",
            src_event="INSERT",
            tgt_event="INSERT",
            src_enabled="ENABLED",
            tgt_enabled="DISABLED",
            src_valid="VALID",
            tgt_valid="VALID",
            detail="ENABLED",
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings["fixup_dir"] = tmp_dir
            sdr.generate_fixup_scripts(
                {"user": "u", "password": "p", "dsn": "d"},
                {"executable": "obclient", "host": "h", "port": "1", "user_string": "u", "password": "p"},
                settings,
                tv_results,
                extra_results,
                [("LIFEDATA.EM_PREM_TBL", "BASEDATA.EM_PREM_TBL", "TABLE")],
                self._make_oracle_meta(),
                {"LIFEDATA.EM_PREM_TBL": {"TABLE": "BASEDATA.EM_PREM_TBL"}},
                {},
                grant_plan=None,
                enable_grant_generation=False,
                dependency_report={"missing": [], "unexpected": [], "skipped": []},
                ob_meta=self._make_ob_meta(),
                expected_dependency_pairs=set(),
                synonym_metadata={},
                trigger_filter_entries=None,
                trigger_filter_enabled=False,
                package_results=None,
                report_dir=None,
                report_timestamp=None,
                support_state_map={},
                unsupported_table_keys=set(),
                view_compat_map={},
                trigger_status_rows=[row],
                constraint_status_rows=[],
            )
            self.assertTrue(
                (Path(tmp_dir) / "status" / "trigger" / "BASEDATA.TRG_EM_PREM_TBL.status.sql").exists()
            )

    def test_collect_constraint_status_drift_rows_uk_validated_report_only(self):
        oracle_meta = self._make_oracle_meta(constraints={
            ("OMS_USER", "T1"): {
                "UK_T1_CODE": {
                    "type": "U",
                    "columns": ["CODE"],
                    "status": "ENABLED",
                    "validated": "VALIDATED",
                }
            }
        })
        ob_meta = self._make_ob_meta(constraints={
            ("OMS_USER", "T1"): {
                "UK_T1_CODE": {
                    "type": "U",
                    "columns": ["CODE"],
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                }
            }
        })
        rows = sdr.collect_constraint_status_drift_rows(
            oracle_meta,
            ob_meta,
            [("OMS_USER.T1", "OMS_USER.T1", "TABLE")],
            full_object_mapping={},
            sync_mode="full",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].constraint_type, "U")
        self.assertEqual(rows[0].detail, "VALIDATED")
        self.assertEqual(rows[0].action_sql, "-")

    def test_collect_constraint_status_drift_rows_fk_semantic_match_generates_validate(self):
        oracle_meta = self._make_oracle_meta(constraints={
            ("OMS_USER", "CHILD"): {
                "FK_SRC_PARENT": {
                    "type": "R",
                    "columns": ["PARENT_ID"],
                    "ref_table_owner": "OMS_USER",
                    "ref_table_name": "PARENT",
                    "delete_rule": "NO ACTION",
                    "update_rule": "NO ACTION",
                    "status": "ENABLED",
                    "validated": "VALIDATED",
                }
            }
        })
        ob_meta = self._make_ob_meta(constraints={
            ("OMS_USER", "CHILD"): {
                "FK_TGT_PARENT_RENAMED": {
                    "type": "R",
                    "columns": ["PARENT_ID"],
                    "ref_table_owner": "OMS_USER",
                    "ref_table_name": "PARENT",
                    "delete_rule": "CASCADE",
                    "update_rule": "NO ACTION",
                    "status": "ENABLED",
                    "validated": "NOT VALIDATED",
                }
            }
        })
        rows = sdr.collect_constraint_status_drift_rows(
            oracle_meta,
            ob_meta,
            [("OMS_USER.CHILD", "OMS_USER.CHILD", "TABLE")],
            full_object_mapping={},
            sync_mode="full",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].detail, "VALIDATED; MATCH=FK_SRC_PARENT->FK_TGT_PARENT_RENAMED")
        self.assertIn('ENABLE VALIDATE CONSTRAINT "FK_TGT_PARENT_RENAMED"', rows[0].action_sql)


class TestNoiseSuppression(unittest.TestCase):
    def _empty_extra_results(self) -> Dict[str, List]:
        return {
            "index_ok": [],
            "index_mismatched": [],
            "index_unsupported": [],
            "constraint_ok": [],
            "constraint_mismatched": [],
            "constraint_unsupported": [],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }

    def test_noise_suppression_filters_table_auto_columns(self):
        tv_results = {
            "missing": [],
            "mismatched": [
                ("TABLE", "A.T1", set(), {"__PK_INCREMENT", "SYS_NC19$"}, [], [])
            ],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        comment_results = {"ok": [], "mismatched": [], "skipped_reason": None}
        result = sdr.apply_noise_suppression(
            tv_results,
            self._empty_extra_results(),
            comment_results
        )
        self.assertEqual(result.tv_results["mismatched"], [])
        self.assertIn(("TABLE", "A.T1"), result.tv_results["ok"])
        reasons = {row.reason for row in result.suppressed_details}
        self.assertIn(sdr.NOISE_REASON_AUTO_COLUMN, reasons)
        self.assertIn(sdr.NOISE_REASON_SYS_NC_COLUMN, reasons)

    def test_noise_suppression_filters_comment_columns(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        comment_results = {
            "ok": [],
            "mismatched": [
                sdr.CommentMismatch(
                    table="A.T1",
                    table_comment=None,
                    column_comment_diffs=[
                        ("__PK_INCREMENT", "x", "y"),
                        ("C1", "x", "y"),
                    ],
                    missing_columns=set(),
                    extra_columns=set()
                )
            ],
            "skipped_reason": None
        }
        result = sdr.apply_noise_suppression(
            tv_results,
            self._empty_extra_results(),
            comment_results
        )
        filtered = result.comment_results["mismatched"]
        self.assertEqual(len(filtered), 1)
        self.assertEqual([d[0] for d in filtered[0].column_comment_diffs], ["C1"])
        reasons = {row.reason for row in result.suppressed_details}
        self.assertIn(sdr.NOISE_REASON_AUTO_COLUMN, reasons)

    def test_noise_suppression_filters_oms_rowid_indexes(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        extra_results = self._empty_extra_results()
        extra_results["index_mismatched"] = [
            sdr.IndexMismatch(
                table="A.T1",
                missing_indexes=set(),
                extra_indexes={"IDX_OMS_ROWID"},
                detail_mismatch=[]
            )
        ]
        comment_results = {"ok": [], "mismatched": [], "skipped_reason": None}
        result = sdr.apply_noise_suppression(
            tv_results,
            extra_results,
            comment_results
        )
        self.assertEqual(result.extra_results["index_mismatched"], [])
        self.assertIn("A.T1", result.extra_results["index_ok"])

    def test_noise_suppression_filters_auto_sequences(self):
        tv_results = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": []
        }
        extra_results = self._empty_extra_results()
        extra_results["sequence_mismatched"] = [
            sdr.SequenceMismatch(
                src_schema="A",
                tgt_schema="A",
                missing_sequences={"ISEQ$$_1", "SEQ1"},
                extra_sequences={"ISEQ$$_2"},
                note=None,
                missing_mappings=[("A.ISEQ$$_1", "A.ISEQ$$_1"), ("A.SEQ1", "A.SEQ1")],
                detail_mismatch=[]
            )
        ]
        comment_results = {"ok": [], "mismatched": [], "skipped_reason": None}
        result = sdr.apply_noise_suppression(
            tv_results,
            extra_results,
            comment_results
        )
        seq_items = result.extra_results["sequence_mismatched"]
        self.assertEqual(len(seq_items), 1)
        self.assertEqual(seq_items[0].missing_sequences, {"SEQ1"})
        self.assertEqual(seq_items[0].extra_sequences, set())
        reasons = {row.reason for row in result.suppressed_details}
        self.assertIn(sdr.NOISE_REASON_AUTO_SEQUENCE, reasons)

    def test_is_oms_index_filters_rowid_name(self):
        self.assertTrue(sdr.is_oms_index("IDX_CODEX_OMS_ROWID", ["C1"]))

    def test_check_comments_skips_missing_target_table(self):
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        oracle_meta = sdr.OracleMetadata(
            table_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            table_comments={("SRC", "T1"): "table comment"},
            column_comments={("SRC", "T1"): {"C1": "col comment"}},
            comments_complete=True,
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
            interval_partitions={}
        )
        ob_meta = sdr.ObMetadata(
            objects_by_type={"TABLE": {"TGT.OTHER"}},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            constraint_deferrable_supported=False
        )
        result = sdr.check_comments(master_list, oracle_meta, ob_meta, True)
        self.assertEqual(result["mismatched"], [])
        self.assertEqual(result["ok"], [])


class TestReportDbHelpers(unittest.TestCase):
    def test_sql_quote_literal(self):
        self.assertEqual(sdr.sql_quote_literal(None), "NULL")
        self.assertEqual(sdr.sql_quote_literal("O'Reilly"), "'O''Reilly'")
        self.assertEqual(sdr.sql_quote_literal("A\x00B"), "'AB'")
        self.assertEqual(sdr.sql_quote_literal("A\x01B"), "'AB'")

    def test_sql_clob_literal_chunking(self):
        text = "a" * 4500
        clob = sdr.sql_clob_literal(text, chunk_size=2000)
        self.assertIn("TO_CLOB", clob)
        self.assertIn("||", clob)

    def test_generate_report_id_format(self):
        report_id = sdr.generate_report_id("20260203_123456")
        self.assertTrue(report_id.startswith("20260203_123456_"))
        self.assertEqual(len(report_id.split("_")[-1]), 8)
        other_id = sdr.generate_report_id("20260203_123456")
        self.assertNotEqual(report_id, other_id)

    def test_parse_report_db_detail_mode(self):
        self.assertEqual(
            sdr.parse_report_db_detail_mode(""),
            {"missing", "mismatched", "unsupported"}
        )
        self.assertEqual(
            sdr.parse_report_db_detail_mode("all"),
            set(sdr.REPORT_DB_DETAIL_MODE_VALUES)
        )
        self.assertEqual(
            sdr.parse_report_db_detail_mode("missing,ok"),
            {"missing", "ok"}
        )

    def test_build_report_counts_rows(self):
        counts = {
            "oracle": {"TABLE": 2},
            "oceanbase": {"TABLE": 1},
            "missing": {"TABLE": 1},
            "extra": {"TABLE": 0},
        }
        rows = sdr._build_report_counts_rows(counts, {"TABLE": 1}, {"TABLE": 0}, {"TABLE": 3})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["object_type"], "TABLE")
        self.assertEqual(rows[0]["oracle_count"], 2)
        self.assertEqual(rows[0]["missing_fixable_count"], 0)
        self.assertEqual(rows[0]["excluded_count"], 3)
        self.assertEqual(rows[0]["unsupported_count"], 1)

    def test_build_report_counts_rows_without_object_counts_uses_excluded_counts(self):
        rows = sdr._build_report_counts_rows(
            None,
            {},
            {},
            {"TABLE": 2, "VIEW": 1}
        )
        self.assertEqual(len(rows), 2)
        by_type = {row["object_type"]: row for row in rows}
        self.assertEqual(by_type["TABLE"]["excluded_count"], 2)
        self.assertEqual(by_type["VIEW"]["excluded_count"], 1)
        self.assertEqual(by_type["TABLE"]["oracle_count"], 0)
        self.assertEqual(by_type["VIEW"]["missing_count"], 0)

    def test_build_excluded_summary_counts(self):
        rows = [
            {"status": "APPLIED", "object_type": "TABLE"},
            {"status": "CASCADED", "object_type": "VIEW"},
            {"status": "SKIPPED", "object_type": "TABLE"},
            {"status": "APPLIED_SYSTEM", "object_type": "TABLE"},
            {"status": "FILTERED_BY_CREATED_AFTER_CUTOFF", "object_type": "TABLE"},
            {"status": "FILTERED_BY_MISSING_CREATED", "object_type": "TABLE"},
            {"status": "APPLIED_LEGACY_RENAME", "object_type": "TABLE"},
        ]
        counts = sdr.build_excluded_summary_counts(rows)
        self.assertEqual(counts.get("TABLE"), 4)
        self.assertEqual(counts.get("VIEW"), 1)

    def test_parse_object_created_before(self):
        self.assertEqual(
            sdr.parse_object_created_before("20260303 150000"),
            datetime(2026, 3, 3, 15, 0, 0)
        )
        self.assertEqual(
            sdr.parse_object_created_before("2026-03-03 15:00:00"),
            datetime(2026, 3, 3, 15, 0, 0)
        )
        self.assertIsNone(sdr.parse_object_created_before(""))
        self.assertIsNone(sdr.parse_object_created_before("2026/03/03 15:00:00"))

    def test_apply_object_created_before_filter(self):
        source_objects = {
            "A.T1": {"TABLE"},
            "A.V1": {"VIEW"},
        }
        created_map = {
            ("A.T1", "TABLE"): datetime(2026, 3, 1, 10, 0, 0),
            ("A.V1", "VIEW"): datetime(2026, 3, 3, 16, 0, 0),
        }
        cutoff = datetime(2026, 3, 3, 15, 0, 0)
        filtered, excluded_rows, excluded_nodes, missing_keys = sdr.apply_object_created_before_filter(
            source_objects,
            created_map,
            cutoff
        )
        self.assertEqual(filtered, {"A.T1": {"TABLE"}})
        self.assertEqual(len(excluded_rows), 1)
        row = excluded_rows[0]
        self.assertEqual(row["status"], "FILTERED_BY_CREATED_AFTER_CUTOFF")
        self.assertEqual(row["object_type"], "VIEW")
        self.assertEqual(row["schema_name"], "A")
        self.assertEqual(row["object_name"], "V1")
        self.assertIn("CREATED=", row["detail"])
        self.assertIn("CUTOFF=", row["detail"])
        self.assertIn(("A.V1", "VIEW"), excluded_nodes)
        self.assertEqual(missing_keys, [])

    def test_apply_object_created_before_filter_with_missing_created(self):
        source_objects = {"A.T1": {"TABLE"}}
        filtered, excluded_rows, excluded_nodes, missing_keys = sdr.apply_object_created_before_filter(
            source_objects,
            {},
            datetime(2026, 3, 3, 15, 0, 0)
        )
        self.assertEqual(filtered, {})
        self.assertEqual(excluded_rows, [])
        self.assertEqual(excluded_nodes, set())
        self.assertEqual(missing_keys, [("A.T1", "TABLE")])

    def test_apply_object_created_before_filter_with_missing_created_include_policy(self):
        source_objects = {"A.T1": {"TABLE"}}
        filtered, excluded_rows, excluded_nodes, missing_keys = sdr.apply_object_created_before_filter(
            source_objects,
            {},
            datetime(2026, 3, 3, 15, 0, 0),
            missing_created_policy="include_missing"
        )
        self.assertEqual(filtered, {"A.T1": {"TABLE"}})
        self.assertEqual(excluded_rows, [])
        self.assertEqual(excluded_nodes, set())
        self.assertEqual(missing_keys, [("A.T1", "TABLE")])

    def test_apply_object_created_before_filter_with_missing_created_exclude_policy(self):
        source_objects = {"A.T1": {"TABLE"}}
        filtered, excluded_rows, excluded_nodes, missing_keys = sdr.apply_object_created_before_filter(
            source_objects,
            {},
            datetime(2026, 3, 3, 15, 0, 0),
            missing_created_policy="exclude_missing"
        )
        self.assertEqual(filtered, {})
        self.assertEqual(len(excluded_rows), 1)
        self.assertEqual(excluded_rows[0]["status"], sdr.EXCLUDED_STATUS_FILTERED_BY_MISSING_CREATED)
        self.assertIn(("A.T1", "TABLE"), excluded_nodes)
        self.assertEqual(missing_keys, [("A.T1", "TABLE")])

    def test_summarize_missing_created_keys_groups_by_type(self):
        count_summary, sample_summary = sdr.summarize_missing_created_keys(
            [
                ("A.T1", "TABLE"),
                ("A.T2", "TABLE"),
                ("A.V1", "VIEW"),
                ("A.P1", "PROCEDURE"),
                ("A.P2", "PROCEDURE"),
            ],
            max_types=3,
            sample_per_type=2
        )
        self.assertEqual(count_summary, "PROCEDURE=2, TABLE=2, VIEW=1")
        self.assertEqual(
            sample_summary,
            "PROCEDURE:A.P1|A.P2; TABLE:A.T1|A.T2; VIEW:A.V1"
        )

    def test_build_missing_breakdown_counts(self):
        object_counts = {"missing": {"TRIGGER": 10, "TABLE": 3}}
        support_summary = sdr.SupportClassificationResult(
            support_state_map={},
            unsupported_rows=[],
            missing_detail_rows=[],
            extra_missing_rows=[],
            missing_support_counts={},
            extra_blocked_counts={"TRIGGER": 9},
            unsupported_table_keys=set(),
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[]
        )
        totals, unsupported, fixable = sdr.build_missing_breakdown_counts(
            object_counts,
            support_summary,
            {
                "index_ok": [], "index_mismatched": [],
                "constraint_ok": [], "constraint_mismatched": [],
                "sequence_ok": [], "sequence_mismatched": [],
                "trigger_ok": [], "trigger_mismatched": [],
                "constraint_unsupported": []
            }
        )
        self.assertEqual(totals.get("TRIGGER"), 10)
        self.assertEqual(unsupported.get("TRIGGER"), 9)
        self.assertEqual(fixable.get("TRIGGER"), 1)

    def test_build_missing_breakdown_counts_job_schedule_not_fixable(self):
        object_counts = {"missing": {"JOB": 2, "SCHEDULE": 1}}
        support_summary = sdr.SupportClassificationResult(
            support_state_map={},
            unsupported_rows=[],
            missing_detail_rows=[],
            extra_missing_rows=[],
            missing_support_counts={
                "JOB": {"supported": 0, "unsupported": 2, "blocked": 0, "risky": 0},
                "SCHEDULE": {"supported": 0, "unsupported": 1, "blocked": 0, "risky": 0},
            },
            extra_blocked_counts={},
            unsupported_table_keys=set(),
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[]
        )
        totals, unsupported, fixable = sdr.build_missing_breakdown_counts(
            object_counts,
            support_summary,
            {
                "index_ok": [], "index_mismatched": [],
                "constraint_ok": [], "constraint_mismatched": [],
                "sequence_ok": [], "sequence_mismatched": [],
                "trigger_ok": [], "trigger_mismatched": [],
                "constraint_unsupported": []
            }
        )
        self.assertEqual(totals.get("JOB"), 2)
        self.assertEqual(totals.get("SCHEDULE"), 1)
        self.assertEqual(unsupported.get("JOB"), 2)
        self.assertEqual(unsupported.get("SCHEDULE"), 1)
        self.assertEqual(fixable.get("JOB"), 0)
        self.assertEqual(fixable.get("SCHEDULE"), 0)

    def test_summarize_extra_missing_counts(self):
        extra_results = {
            "index_mismatched": [
                sdr.IndexMismatch("T.T1", {"I1", "I2"}, set(), []),
                sdr.IndexMismatch("T.T2", {"I3"}, set(), []),
            ],
            "constraint_mismatched": [
                sdr.ConstraintMismatch("T.T1", {"C1"}, set(), [], set()),
            ],
            "sequence_mismatched": [
                sdr.SequenceMismatch("SRC", "TGT", {"S1", "S2"}, set(), None, None, None),
            ],
            "trigger_mismatched": [
                sdr.TriggerMismatch("T.T1", {"TR1", "TR2"}, set(), [], None),
            ],
        }
        counts = sdr.summarize_extra_missing_counts(extra_results)
        self.assertEqual(counts["INDEX"], 3)
        self.assertEqual(counts["CONSTRAINT"], 1)
        self.assertEqual(counts["SEQUENCE"], 2)
        self.assertEqual(counts["TRIGGER"], 2)

    def test_build_run_summary_uses_object_level_extra_missing_counts(self):
        ctx = sdr.RunSummaryContext(
            start_time=datetime.now(),
            start_perf=0.0,
            phase_durations={},
            phase_skip_reasons={},
            enabled_primary_types={"TABLE"},
            enabled_extra_types={"INDEX", "CONSTRAINT", "SEQUENCE", "TRIGGER"},
            print_only_types=set(),
            total_checked=1,
            enable_dependencies_check=False,
            enable_comment_check=False,
            enable_grant_generation=False,
            enable_schema_mapping_infer=False,
            fixup_enabled=False,
            fixup_dir="fixup_scripts",
            dependency_chain_file=None,
            view_chain_file=None,
            trigger_list_summary=None,
            report_start_perf=0.0,
        )
        tv_results = {"missing": [], "mismatched": [], "extra_targets": [], "skipped": [], "extraneous": []}
        extra_results = {
            "index_mismatched": [sdr.IndexMismatch("T.T1", {"I1", "I2"}, set(), [])],
            "constraint_mismatched": [sdr.ConstraintMismatch("T.T1", {"C1"}, set(), [], set())],
            "sequence_mismatched": [sdr.SequenceMismatch("SRC", "TGT", {"S1", "S2"}, set(), None, None, None)],
            "trigger_mismatched": [sdr.TriggerMismatch("T.T1", {"TR1", "TR2"}, set(), [], None)],
            "index_unsupported": [],
            "constraint_unsupported": [],
        }
        support_summary = sdr.SupportClassificationResult(
            support_state_map={},
            missing_detail_rows=[],
            unsupported_rows=[],
            extra_missing_rows=[],
            missing_support_counts={},
            extra_blocked_counts={},
            unsupported_table_keys=set(),
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[],
        )
        summary = sdr.build_run_summary(
            ctx,
            tv_results,
            extra_results,
            {"ok": [], "mismatched": [], "skipped_reason": "skip"},
            {"missing": [], "unexpected": [], "skipped": []},
            [],
            [],
            {},
            None,
            support_summary=support_summary,
        )
        joined = "\n".join(summary.findings)
        self.assertIn("INDEX 缺失对象 2 (差异表 1)", joined)
        self.assertIn("SEQUENCE 缺失对象 2 (差异表 1)", joined)
        self.assertIn("TRIGGER 缺失对象 2 (差异表 1)", joined)

    def test_build_report_usability_rows(self):
        summary = sdr.UsabilitySummary(
            total_candidates=2,
            total_checked=2,
            total_usable=1,
            total_unusable=1,
            total_expected_unusable=0,
            total_unexpected_usable=0,
            total_timeout=0,
            total_skipped=0,
            total_sampled_out=0,
            duration_seconds=0.1,
            results=[
                sdr.UsabilityCheckResult(
                    schema="S1",
                    object_name="V1",
                    object_type="VIEW",
                    src_exists=True,
                    src_usable=True,
                    tgt_exists=True,
                    tgt_usable=True,
                    status=sdr.USABILITY_STATUS_OK,
                    src_error="-",
                    tgt_error="-",
                    root_cause="-",
                    recommendation="-",
                    src_time_ms=1,
                    tgt_time_ms=2
                ),
                sdr.UsabilityCheckResult(
                    schema="S1",
                    object_name="V2",
                    object_type="VIEW",
                    src_exists=True,
                    src_usable=True,
                    tgt_exists=True,
                    tgt_usable=False,
                    status=sdr.USABILITY_STATUS_UNUSABLE,
                    src_error="-",
                    tgt_error="ORA-00942",
                    root_cause="依赖对象不存在",
                    recommendation="修补依赖",
                    src_time_ms=1,
                    tgt_time_ms=2
                )
            ]
        )
        rows = sdr._build_report_usability_rows(summary)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["usable"], 1)
        self.assertEqual(rows[1]["usable"], 0)
        self.assertIn("status", rows[0]["detail_json"])

    def test_classify_table_presence_status(self):
        self.assertEqual(
            sdr._classify_table_presence_status(True, False),
            sdr.TABLE_PRESENCE_STATUS_RISK
        )
        self.assertEqual(
            sdr._classify_table_presence_status(True, True),
            sdr.TABLE_PRESENCE_STATUS_OK_BOTH_NONEMPTY
        )
        self.assertEqual(
            sdr._classify_table_presence_status(False, False),
            sdr.TABLE_PRESENCE_STATUS_OK_BOTH_EMPTY
        )
        self.assertEqual(
            sdr._classify_table_presence_status(False, True),
            sdr.TABLE_PRESENCE_STATUS_UNKNOWN
        )

    def test_parse_ob_presence_probe_output(self):
        token_map = {
            "PRESENCE|000001": ("A", "T1"),
            "PRESENCE|000002": ("A", "T2"),
        }
        out = "\n".join([
            "PRESENCE|000001|Y",
            "PRESENCE|000002|N",
            "ignored line",
        ])
        parsed = sdr._parse_ob_presence_probe_output(out, token_map)
        self.assertEqual(parsed[("A", "T1")], True)
        self.assertEqual(parsed[("A", "T2")], False)

    def test_normalize_table_num_rows_value(self):
        self.assertEqual(sdr._normalize_table_num_rows_value(0), 0)
        self.assertEqual(sdr._normalize_table_num_rows_value("12"), 12)
        self.assertEqual(sdr._normalize_table_num_rows_value("12.0"), 12)
        self.assertEqual(sdr._normalize_table_num_rows_value("NULL"), None)
        self.assertEqual(sdr._normalize_table_num_rows_value(""), None)
        self.assertEqual(sdr._normalize_table_num_rows_value(None), None)

    def test_check_table_data_presence_auto_skip(self):
        settings = {
            "table_data_presence_check_mode": "auto",
            "table_data_presence_auto_max_tables": 0,
            "table_data_presence_chunk_size": 100,
            "table_data_presence_obclient_timeout": 30,
        }
        master_list = [
            ("SRC.T1", "TGT.T1", "TABLE"),
            ("SRC.V1", "TGT.V1", "VIEW"),
        ]
        tv_results = {"missing": [], "mismatched": [], "ok": [], "skipped": []}
        ob_meta = sdr.ObMetadata(
            objects_by_type={"TABLE": {"TGT.T1"}},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            constraint_deferrable_supported=False,
        )
        summary = sdr.check_table_data_presence(
            settings,
            master_list,
            tv_results,
            {"executable": "/usr/bin/obclient"},
            {"user": "u", "password": "p", "dsn": "h:1/s"},
            ob_meta,
            {"TABLE", "VIEW"},
        )
        self.assertIsNotNone(summary)
        self.assertEqual(summary.total_candidates, 1)
        self.assertEqual(summary.total_checked, 0)
        self.assertEqual(summary.total_skipped, 1)
        self.assertEqual(summary.rows, [])

    @mock.patch.object(sdr, "_probe_ob_table_has_rows_batch")
    @mock.patch.object(sdr, "_load_ob_table_num_rows_from_stats")
    @mock.patch.object(sdr, "_load_oracle_table_num_rows_from_stats")
    @mock.patch.object(sdr.oracledb, "connect")
    def test_check_table_data_presence_auto_stats_only(
        self,
        mock_oracle_connect,
        mock_load_source_stats,
        mock_load_target_stats,
        mock_target_probe
    ):
        settings = {
            "table_data_presence_check_mode": "auto",
            "table_data_presence_auto_max_tables": 100,
            "table_data_presence_chunk_size": 100,
            "table_data_presence_obclient_timeout": 30,
        }
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        tv_results = {"missing": [], "mismatched": [], "ok": [], "skipped": []}
        ob_meta = sdr.ObMetadata(
            objects_by_type={"TABLE": {"TGT.T1"}},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            constraint_deferrable_supported=False,
        )
        mock_load_source_stats.return_value = ({("SRC", "T1"): 9}, "")
        mock_load_target_stats.return_value = ({("TGT", "T1"): 7}, "")
        mock_oracle_connect.side_effect = AssertionError("auto stats-only should not probe source table")
        mock_target_probe.side_effect = AssertionError("auto stats-only should not probe target table")

        summary = sdr.check_table_data_presence(
            settings,
            master_list,
            tv_results,
            {"executable": "/usr/bin/obclient"},
            {"user": "u", "password": "p", "dsn": "h:1/s"},
            ob_meta,
            {"TABLE"},
        )
        self.assertIsNotNone(summary)
        self.assertEqual(summary.total_checked, 1)
        self.assertEqual(summary.total_risk, 0)
        self.assertEqual(summary.rows[0].status, sdr.TABLE_PRESENCE_STATUS_OK_BOTH_NONEMPTY)
        self.assertIn("MODE=STATS_ONLY", summary.rows[0].detail)
        self.assertEqual(summary.rows[0].source_probe_ms, 0)
        self.assertEqual(summary.rows[0].target_probe_ms, 0)

    @mock.patch.object(sdr, "_probe_ob_table_has_rows_batch")
    @mock.patch.object(sdr, "_probe_oracle_table_has_rows_batch")
    @mock.patch.object(sdr, "_load_ob_table_num_rows_from_stats")
    @mock.patch.object(sdr, "_load_oracle_table_num_rows_from_stats")
    def test_check_table_data_presence_auto_zero_stats_probe_corrects_status(
        self,
        mock_load_source_stats,
        mock_load_target_stats,
        mock_source_probe,
        mock_target_probe
    ):
        settings = {
            "table_data_presence_check_mode": "auto",
            "table_data_presence_auto_max_tables": 100,
            "table_data_presence_chunk_size": 100,
            "table_data_presence_obclient_timeout": 30,
            "table_data_presence_zero_probe_workers": 3,
        }
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        tv_results = {"missing": [], "mismatched": [], "ok": [], "skipped": []}
        ob_meta = sdr.ObMetadata(
            objects_by_type={"TABLE": {"TGT.T1"}},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            constraint_deferrable_supported=False,
        )
        mock_load_source_stats.return_value = ({("SRC", "T1"): 0}, "")
        mock_load_target_stats.return_value = ({("TGT", "T1"): 0}, "")
        mock_source_probe.return_value = {("SRC", "T1"): (True, "", 4)}
        mock_target_probe.return_value = {("TGT", "T1"): (True, "", 5)}

        summary = sdr.check_table_data_presence(
            settings,
            master_list,
            tv_results,
            {"executable": "/usr/bin/obclient"},
            {"user": "u", "password": "p", "dsn": "h:1/s"},
            ob_meta,
            {"TABLE"},
        )
        self.assertIsNotNone(summary)
        self.assertEqual(summary.total_checked, 1)
        self.assertEqual(summary.total_risk, 0)
        self.assertEqual(summary.rows[0].status, sdr.TABLE_PRESENCE_STATUS_OK_BOTH_NONEMPTY)
        self.assertIn("SRC_ZERO_PROBE=NONEMPTY", summary.rows[0].detail)
        self.assertIn("TGT_ZERO_PROBE=NONEMPTY", summary.rows[0].detail)
        mock_source_probe.assert_called_once_with(
            {"user": "u", "password": "p", "dsn": "h:1/s"},
            [("SRC", "T1")],
            30000,
            3,
        )
        self.assertGreaterEqual(summary.rows[0].target_probe_ms, 0)
        self.assertGreaterEqual(summary.rows[0].source_probe_ms, 0)

    @mock.patch.object(sdr, "_probe_ob_table_has_rows_batch")
    @mock.patch.object(sdr, "_probe_oracle_table_has_rows_batch")
    @mock.patch.object(sdr, "_load_ob_table_num_rows_from_stats")
    @mock.patch.object(sdr, "_load_oracle_table_num_rows_from_stats")
    def test_check_table_data_presence_auto_missing_stats_probe_corrects_status(
        self,
        mock_load_source_stats,
        mock_load_target_stats,
        mock_source_probe,
        mock_target_probe
    ):
        settings = {
            "table_data_presence_check_mode": "auto",
            "table_data_presence_auto_max_tables": 100,
            "table_data_presence_chunk_size": 100,
            "table_data_presence_obclient_timeout": 30,
            "table_data_presence_zero_probe_workers": 2,
        }
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        tv_results = {"missing": [], "mismatched": [], "ok": [], "skipped": []}
        ob_meta = sdr.ObMetadata(
            objects_by_type={"TABLE": {"TGT.T1"}},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            constraint_deferrable_supported=False,
        )
        mock_load_source_stats.return_value = ({("SRC", "T1"): None}, "")
        mock_load_target_stats.return_value = ({("TGT", "T1"): None}, "")
        mock_source_probe.return_value = {("SRC", "T1"): (True, "", 6)}
        mock_target_probe.return_value = {("TGT", "T1"): (False, "", 5)}

        summary = sdr.check_table_data_presence(
            settings,
            master_list,
            tv_results,
            {"executable": "/usr/bin/obclient"},
            {"user": "u", "password": "p", "dsn": "h:1/s"},
            ob_meta,
            {"TABLE"},
        )
        self.assertIsNotNone(summary)
        self.assertEqual(summary.total_checked, 1)
        self.assertEqual(summary.total_risk, 1)
        self.assertEqual(summary.rows[0].status, sdr.TABLE_PRESENCE_STATUS_RISK)
        self.assertIn("SRC_MISSING_PROBE=NONEMPTY", summary.rows[0].detail)
        self.assertIn("TGT_MISSING_PROBE=EMPTY", summary.rows[0].detail)
        mock_source_probe.assert_called_once_with(
            {"user": "u", "password": "p", "dsn": "h:1/s"},
            [("SRC", "T1")],
            30000,
            2,
        )
        mock_target_probe.assert_called_once()

    @mock.patch.object(sdr, "_probe_ob_table_has_rows_batch")
    @mock.patch.object(sdr, "_probe_oracle_table_has_rows_batch")
    @mock.patch.object(sdr, "_load_ob_table_num_rows_from_stats")
    @mock.patch.object(sdr, "_load_oracle_table_num_rows_from_stats")
    def test_check_table_data_presence_auto_zero_probe_workers_capped(
        self,
        mock_load_source_stats,
        mock_load_target_stats,
        mock_source_probe,
        mock_target_probe
    ):
        settings = {
            "table_data_presence_check_mode": "auto",
            "table_data_presence_auto_max_tables": 100,
            "table_data_presence_chunk_size": 100,
            "table_data_presence_obclient_timeout": 30,
            "table_data_presence_zero_probe_workers": 999,
        }
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        tv_results = {"missing": [], "mismatched": [], "ok": [], "skipped": []}
        ob_meta = sdr.ObMetadata(
            objects_by_type={"TABLE": {"TGT.T1"}},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            constraint_deferrable_supported=False,
        )
        mock_load_source_stats.return_value = ({("SRC", "T1"): 0}, "")
        mock_load_target_stats.return_value = ({("TGT", "T1"): 0}, "")
        mock_source_probe.return_value = {("SRC", "T1"): (False, "", 4)}
        mock_target_probe.return_value = {("TGT", "T1"): (False, "", 5)}

        summary = sdr.check_table_data_presence(
            settings,
            master_list,
            tv_results,
            {"executable": "/usr/bin/obclient"},
            {"user": "u", "password": "p", "dsn": "h:1/s"},
            ob_meta,
            {"TABLE"},
        )
        self.assertIsNotNone(summary)
        mock_source_probe.assert_called_once_with(
            {"user": "u", "password": "p", "dsn": "h:1/s"},
            [("SRC", "T1")],
            30000,
            sdr.TABLE_PRESENCE_MAX_ZERO_PROBE_WORKERS,
        )

    @mock.patch.object(sdr, "_probe_ob_table_has_rows_batch")
    @mock.patch.object(sdr, "_load_ob_table_num_rows_from_stats")
    @mock.patch.object(sdr, "_load_oracle_table_num_rows_from_stats")
    @mock.patch.object(sdr.oracledb, "connect")
    def test_check_table_data_presence_auto_stats_error_fallback_probe(
        self,
        mock_oracle_connect,
        mock_load_source_stats,
        mock_load_target_stats,
        mock_target_probe
    ):
        settings = {
            "table_data_presence_check_mode": "auto",
            "table_data_presence_auto_max_tables": 100,
            "table_data_presence_chunk_size": 100,
            "table_data_presence_obclient_timeout": 30,
        }
        master_list = [("SRC.T1", "TGT.T1", "TABLE")]
        tv_results = {"missing": [], "mismatched": [], "ok": [], "skipped": []}
        ob_meta = sdr.ObMetadata(
            objects_by_type={"TABLE": {"TGT.T1"}},
            tab_columns={},
            invisible_column_supported=False,
            identity_column_supported=True,
            default_on_null_supported=True,
            indexes={},
            constraints={},
            triggers={},
            sequences={},
            sequence_attrs={},
            roles=set(),
            table_comments={},
            column_comments={},
            comments_complete=True,
            object_statuses={},
            package_errors={},
            package_errors_complete=False,
            partition_key_columns={},
            constraint_deferrable_supported=False,
        )
        mock_load_source_stats.return_value = ({}, "ORA-00000: stats query failed")
        mock_load_target_stats.return_value = ({("TGT", "T1"): 0}, "")
        mock_target_probe.return_value = {("TGT", "T1"): (False, "", 2)}

        mock_cursor = mock.MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        mock_conn = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_oracle_connect.return_value = mock_conn

        summary = sdr.check_table_data_presence(
            settings,
            master_list,
            tv_results,
            {"executable": "/usr/bin/obclient"},
            {"user": "u", "password": "p", "dsn": "h:1/s"},
            ob_meta,
            {"TABLE"},
        )
        self.assertIsNotNone(summary)
        self.assertEqual(summary.total_checked, 1)
        self.assertEqual(summary.rows[0].status, sdr.TABLE_PRESENCE_STATUS_OK_BOTH_EMPTY)
        self.assertGreaterEqual(summary.rows[0].target_probe_ms, 0)
        self.assertGreaterEqual(summary.rows[0].source_probe_ms, 0)
        self.assertTrue(mock_oracle_connect.called)
        self.assertTrue(mock_target_probe.called)

    def test_build_report_table_presence_rows(self):
        summary = sdr.TablePresenceSummary(
            total_candidates=2,
            total_checked=2,
            total_risk=1,
            total_ok_nonempty=1,
            total_ok_empty=0,
            total_unknown=0,
            total_skipped=0,
            duration_seconds=0.2,
            mode="on",
            rows=[
                sdr.TablePresenceResult(
                    source_schema="SRC",
                    source_table="T1",
                    target_schema="TGT",
                    target_table="T1",
                    source_has_rows="YES",
                    target_has_rows="NO",
                    status=sdr.TABLE_PRESENCE_STATUS_RISK,
                    detail="-",
                    source_probe_ms=3,
                    target_probe_ms=4,
                )
            ]
        )
        rows = sdr._build_report_table_presence_rows(summary)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], sdr.TABLE_PRESENCE_STATUS_RISK)
        self.assertEqual(rows[0]["source_has_rows"], "YES")

    def test_build_report_package_compare_rows(self):
        row = sdr.PackageCompareRow(
            src_full="SRC.PKG1",
            obj_type="PACKAGE",
            src_status="VALID",
            tgt_full="TGT.PKG1",
            tgt_status="VALID",
            result="OK",
            error_count=0,
            first_error=""
        )
        results = {"rows": [row], "summary": {}, "diff_rows": []}
        rows = sdr._build_report_package_compare_rows(results, None, None)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["schema_name"], "TGT")
        self.assertEqual(rows[0]["object_name"], "PKG1")
        self.assertEqual(rows[0]["diff_hash"], sdr._hash_package_compare_row(row))

    def test_build_report_trigger_status_rows(self):
        row = sdr.TriggerStatusReportRow(
            trigger_full="TGT.TRG1",
            src_trigger_full="SRC.TRG1",
            src_event="INSERT",
            tgt_event="INSERT",
            src_enabled="ENABLED",
            tgt_enabled="DISABLED",
            src_valid="VALID",
            tgt_valid="VALID",
            detail="ENABLED"
        )
        rows = sdr._build_report_trigger_status_rows([row])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["schema_name"], "TGT")
        self.assertEqual(rows[0]["trigger_name"], "TRG1")

    def test_build_report_dependency_rows(self):
        report = {
            "missing": [
                sdr.DependencyIssue("SRC.V1", "VIEW", "SRC.T1", "TABLE", "missing")
            ],
            "unexpected": [],
            "skipped": []
        }
        expected = {("SRC.V1", "VIEW", "SRC.T1", "TABLE")}
        rows, truncated, truncated_count = sdr._build_report_dependency_rows(
            report,
            expected,
            max_rows=0,
            store_expected=True
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertTrue(any(r["edge_status"] == "MISSING" for r in rows))
        self.assertTrue(any(r["edge_status"] == "EXPECTED" for r in rows))

    def test_build_report_view_chain_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "VIEWs_chain_20250101.txt"
            path.write_text(
                "00001. SCHEMA.V1[VIEW|EXISTS|GRANT_OK] -> SCHEMA.T1[TABLE|EXISTS|GRANT_OK]\n",
                encoding="utf-8"
            )
            rows, truncated, truncated_count = sdr._build_report_view_chain_rows(path, max_rows=0)
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["view_schema"], "SCHEMA")
        self.assertEqual(rows[0]["view_name"], "V1")

    def test_build_report_remap_conflict_rows(self):
        rows, truncated, truncated_count = sdr._build_report_remap_conflict_rows(
            [("VIEW", "SRC.V1", "reason")],
            max_rows=0
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(rows[0]["object_type"], "VIEW")
        self.assertEqual(rows[0]["source_schema"], "SRC")
        self.assertEqual(rows[0]["source_name"], "V1")

    def test_build_report_object_mapping_rows(self):
        full_mapping = {"SRC.V1": {"VIEW": "TGT.V1"}}
        remap_rules = {"SRC.V1": "TGT.V1"}
        rows, truncated, truncated_count = sdr._build_report_object_mapping_rows(
            full_mapping,
            remap_rules,
            max_rows=0
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(rows[0]["map_source"], "rule")

    def test_build_report_blacklist_rows(self):
        row = sdr.BlacklistReportRow(
            schema="SRC",
            table="T1",
            black_type="LONG",
            data_type="LONG",
            reason="LONG",
            status="SKIPPED",
            detail=""
        )
        rows, truncated, truncated_count = sdr._build_report_blacklist_rows([row], max_rows=0)
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(rows[0]["schema_name"], "SRC")
        self.assertEqual(rows[0]["table_name"], "T1")

    def test_build_report_excluded_objects_rows(self):
        rows, truncated, truncated_count = sdr._build_report_excluded_objects_rows(
            [
                {
                    "status": "applied",
                    "line_no": 12,
                    "object_type": "table",
                    "schema_name": "src",
                    "object_name": "t_rename",
                    "detail": "MATCHED",
                },
                {
                    "status": "skipped",
                    "line_no": 13,
                    "object_type": "VIEW",
                    "schema_name": "SRC",
                    "object_name": "V1",
                    "detail": "NOT_FOUND_IN_SOURCE_SCOPE",
                },
            ],
            max_rows=0
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["status"], "APPLIED")
        self.assertEqual(rows[0]["object_type"], "TABLE")
        self.assertEqual(rows[0]["schema_name"], "SRC")
        self.assertEqual(rows[0]["object_name"], "T_RENAME")

    def test_build_report_fixup_skip_rows(self):
        summary = {
            "TABLE": {
                "missing_total": 2,
                "task_total": 1,
                "generated": 1,
                "skipped": {"UNSUPPORTED": 1}
            }
        }
        rows, truncated, truncated_count = sdr._build_report_fixup_skip_rows(summary, max_rows=0)
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(rows[0]["object_type"], "TABLE")
        self.assertEqual(rows[0]["skip_reason"], "UNSUPPORTED")

    def test_build_report_oms_missing_rows(self):
        tv_results = {"missing": [("TABLE", "TGT.S1", "SRC.S1")]}
        rows, truncated, truncated_count = sdr._build_report_oms_missing_rows(
            tv_results,
            support_state_map={},
            blacklisted_tables=set(),
            max_rows=0
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(rows[0]["object_type"], "TABLE")
        self.assertEqual(rows[0]["src_schema"], "SRC")
        self.assertEqual(rows[0]["tgt_schema"], "TGT")

    def test_build_report_oms_missing_rows_filters_dependency_only_when_explicit_roots_provided(self):
        tv_results = {
            "missing": [
                ("TABLE", "TGT.ROOT_T", "SRC.ROOT_T"),
                ("TABLE", "TGT.DEP_T", "SRC.DEP_T"),
            ]
        }
        rows, truncated, truncated_count = sdr._build_report_oms_missing_rows(
            tv_results,
            support_state_map={},
            blacklisted_tables=set(),
            max_rows=0,
            explicit_root_fulls={"SRC.ROOT_T"},
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["src_schema"], "SRC")
        self.assertEqual(rows[0]["src_name"], "ROOT_T")

    def test_export_missing_table_view_mappings_filters_dependency_only_when_explicit_roots_provided(self):
        tv_results = {
            "missing": [
                ("TABLE", "TGT.ROOT_T", "SRC.ROOT_T"),
                ("VIEW", "TGT.DEP_V", "SRC.DEP_V"),
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            output = sdr.export_missing_table_view_mappings(
                tv_results,
                Path(tmpdir),
                blacklisted_tables=set(),
                support_state_map={},
                explicit_root_fulls={"SRC.ROOT_T"},
            )
            self.assertIsNotNone(output)
            root_file = Path(output) / "TGT_T.txt"
            self.assertTrue(root_file.exists())
            content = root_file.read_text(encoding="utf-8")
            self.assertIn("SRC.ROOT_T", content)
            dep_view_file = Path(output) / "TGT_V.txt"
            self.assertFalse(dep_view_file.exists())

    def test_build_report_artifact_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir)
            sample = report_dir / "missing_objects_detail_20250101.txt"
            sample.write_text("# 字段说明: A|B\nA|B\n", encoding="utf-8")
            sql_tpl = report_dir / "report_sql_20250101.txt"
            sql_tpl.write_text("SELECT * FROM DIFF_REPORT_SUMMARY WHERE report_id='X';\n", encoding="utf-8")
            rows = sdr._build_report_artifact_rows(
                report_dir,
                "full",
                {"missing", "mismatched", "unsupported"},
                False
            )
        self.assertTrue(rows)
        types = {row["artifact_type"] for row in rows}
        self.assertIn("MISSING_DETAIL", types)
        self.assertIn("REPORT_SQL_TEMPLATE", types)

    def test_export_objects_after_cutoff_detail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir)
            rows = [
                {
                    "status": "FILTERED_BY_CREATED_AFTER_CUTOFF",
                    "object_type": "TABLE",
                    "schema_name": "A",
                    "object_name": "T1",
                    "created_ts": "2026-03-03 16:00:00",
                    "cutoff_ts": "2026-03-03 15:00:00",
                    "detail": "CREATED=2026-03-03 16:00:00 > CUTOFF=2026-03-03 15:00:00",
                },
                {
                    "status": "FILTERED_BY_MISSING_CREATED",
                    "object_type": "VIEW",
                    "schema_name": "A",
                    "object_name": "V1",
                    "created_ts": "-",
                    "cutoff_ts": "2026-03-03 15:00:00",
                    "detail": "CREATED=<NULL>; POLICY=exclude_missing; CUTOFF=2026-03-03 15:00:00",
                }
            ]
            output = sdr.export_objects_after_cutoff_detail(rows, report_dir, "20260303_150100")
            self.assertIsNotNone(output)
            text = (output or Path("")).read_text(encoding="utf-8")
            self.assertIn("STATUS|OBJECT_TYPE|SCHEMA_NAME|OBJECT_NAME|CREATED_TS|CUTOFF_TS|DETAIL", text)
            self.assertIn("FILTERED_BY_CREATED_AFTER_CUTOFF|TABLE|A|T1|2026-03-03 16:00:00|2026-03-03 15:00:00", text)
            self.assertIn("FILTERED_BY_MISSING_CREATED|VIEW|A|V1|-|2026-03-03 15:00:00", text)

    def test_infer_artifact_status_full_marks_in_db(self):
        status, note = sdr._infer_artifact_status(
            "REPORT_SQL_TEMPLATE",
            "full",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status, "IN_DB")
        self.assertEqual(note, "")

    def test_infer_artifact_type_and_status_for_excluded_detail(self):
        artifact_type = sdr._infer_report_artifact_type("run_20260101/excluded_objects_detail_20260101.txt")
        self.assertEqual(artifact_type, "EXCLUDED_OBJECTS_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        status_summary, _ = sdr._infer_artifact_status(
            artifact_type,
            "summary",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")
        self.assertEqual(status_summary, "TXT_ONLY")

    def test_infer_artifact_type_for_non_table_trigger_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/triggers_non_table_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "TRIGGER_NON_TABLE_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False,
        )
        self.assertEqual(status_core, "IN_DB")

    def test_infer_artifact_type_for_unsupported_grant_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/unsupported_grant_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "UNSUPPORTED_GRANT_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")

    def test_infer_artifact_type_for_grant_capability_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/grant_capability_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "GRANT_CAPABILITY_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            set(),
            False,
        )
        self.assertEqual(status_core, "IN_DB")

    def test_infer_artifact_type_for_objects_after_cutoff_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/objects_after_cutoff_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "OBJECTS_AFTER_CUTOFF_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        status_summary, _ = sdr._infer_artifact_status(
            artifact_type,
            "summary",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")
        self.assertEqual(status_summary, "TXT_ONLY")

    def test_infer_artifact_type_for_temp_trigger_unsupported_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/triggers_temp_table_unsupported_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "TRIGGER_TEMP_UNSUPPORTED_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")

    def test_infer_artifact_type_for_sys_c_force_candidates_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/sys_c_force_candidates_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "SYS_C_FORCE_CANDIDATES_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")

    def test_infer_artifact_type_for_column_nullability_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/column_nullability_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "COLUMN_NULLABILITY_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")

    def test_infer_artifact_type_for_column_default_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/column_default_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "COLUMN_DEFAULT_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")

    def test_infer_artifact_type_for_column_default_on_null_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/column_default_on_null_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "COLUMN_DEFAULT_ON_NULL_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")

    def test_infer_artifact_type_for_column_visibility_skipped_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/column_visibility_skipped_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "COLUMN_VISIBILITY_SKIPPED_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")

    def test_infer_artifact_type_for_column_identity_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/column_identity_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "COLUMN_IDENTITY_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")

    def test_infer_artifact_type_for_column_identity_option_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/column_identity_option_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "COLUMN_IDENTITY_OPTION_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")

    def test_infer_artifact_type_for_ddl_cleanup_detail(self):
        artifact_type = sdr._infer_report_artifact_type(
            "run_20260101/ddl_cleanup_detail_20260101.txt"
        )
        self.assertEqual(artifact_type, "DDL_CLEANUP_DETAIL")
        status_core, _ = sdr._infer_artifact_status(
            artifact_type,
            "core",
            {"missing", "mismatched", "unsupported"},
            False
        )
        self.assertEqual(status_core, "IN_DB")

    def test_iter_report_artifact_line_rows_preserves_blank_lines(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir)
            sample = report_dir / "report_20250101.txt"
            sample.write_text("A\n\n#C\nD\n", encoding="utf-8")
            rows = list(sdr._iter_report_artifact_line_rows(report_dir))
        self.assertEqual(len(rows), 4)
        self.assertEqual([r["line_no"] for r in rows], [1, 2, 3, 4])
        self.assertEqual(rows[1]["line_text"], "")
        self.assertEqual(rows[2]["line_text"], "#C")

    def test_insert_report_artifact_line_rows(self):
        calls = []

        def _fake_exec(_cfg, sql, timeout=0):
            calls.append(sql)
            return True, "", ""

        rows = iter([
            {
                "artifact_type": "REPORT_MAIN",
                "file_path": "/tmp/a.txt",
                "line_no": 1,
                "line_text": "L1",
            },
            {
                "artifact_type": "REPORT_MAIN",
                "file_path": "/tmp/a.txt",
                "line_no": 2,
                "line_text": "L2",
            },
        ])
        with mock.patch.object(sdr, "obclient_run_sql_commit", side_effect=_fake_exec):
            ok, inserted = sdr._insert_report_artifact_line_rows(
                {"executable": "/usr/bin/obclient"},
                "",
                "RPT1",
                rows,
                batch_size=1
            )
        self.assertTrue(ok)
        self.assertEqual(inserted, 2)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all("DIFF_REPORT_ARTIFACT_LINE" in sql for sql in calls))

    def test_render_report_sql_template(self):
        content = "SELECT * FROM DIFF_REPORT_SUMMARY WHERE report_id = :report_id;"
        rendered = sdr._render_report_sql_template(content, "R1")
        self.assertIn("report_id = R1", rendered)

    def test_build_report_db_view_ddls_phase_b_views(self):
        ddls = sdr._build_report_db_view_ddls("")
        self.assertIn(sdr.REPORT_DB_VIEWS["pending_actions"], ddls)
        self.assertIn(sdr.REPORT_DB_VIEWS["grant_class"], ddls)
        self.assertIn(sdr.REPORT_DB_VIEWS["usability_class"], ddls)
        actions_sql = ddls[sdr.REPORT_DB_VIEWS["actions"]].upper()
        self.assertIn("NVL(S.WRITE_STATUS, 'READY') = 'READY'", actions_sql)
        self.assertIn("D.REPORT_TYPE = 'RISKY'", actions_sql)

    def test_build_report_detail_item_rows(self):
        tv_results = {
            "mismatched": [
                (
                    "TABLE",
                    "TGT.T1",
                    {"C1"},
                    {"C2"},
                    [("C3", 10, 20, 20, "LEN")],
                    [("C4", "NUMBER", "VARCHAR2", "NUMBER", "TYPE")],
                )
            ],
            "missing": [],
        }
        extra_results = {
            "index_mismatched": [],
            "constraint_mismatched": [],
            "sequence_mismatched": [
                sdr.SequenceMismatch("SRC", "TGT", {"SEQ1"}, set(), None, None, None)
            ],
            "trigger_mismatched": [],
            "index_unsupported": [],
            "constraint_unsupported": [],
        }
        table_target_map = {("SRC", "T1"): ("TGT", "T1")}
        rows, truncated, truncated_count = sdr._build_report_detail_item_rows(
            tv_results,
            extra_results,
            None,
            table_target_map,
            max_rows=0
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        item_types = {row["item_type"] for row in rows}
        self.assertIn("MISSING_COLUMN", item_types)
        self.assertIn("EXTRA_COLUMN", item_types)
        self.assertIn("LENGTH_MISMATCH", item_types)
        self.assertIn("TYPE_MISMATCH", item_types)
        self.assertIn("MISSING_SEQUENCE", item_types)

    def test_build_report_detail_item_rows_includes_constraint_suppressed_counts(self):
        rows, truncated, truncated_count = sdr._build_report_detail_item_rows(
            tv_results={"mismatched": [], "missing": []},
            extra_results={
                "constraint_mismatched": [
                    sdr.ConstraintMismatch(
                        table="TGT.T1",
                        missing_constraints={"CK_MISS"},
                        extra_constraints=set(),
                        detail_mismatch=["CHECK: 源约束 CK_MISS 在目标端未找到。"],
                        downgraded_pk_constraints=set(),
                        check_suppressed_source_dup_count=2,
                        check_suppressed_target_dup_count=3,
                    )
                ]
            },
            support_summary=None,
            table_target_map={},
            max_rows=0,
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        matched = {
            (row.get("item_type"), row.get("item_value"))
            for row in rows
            if row.get("object_type") == "CONSTRAINT"
        }
        self.assertIn(("SUPPRESSED_CHECK_DUP_SOURCE", "2"), matched)
        self.assertIn(("SUPPRESSED_CHECK_DUP_TARGET", "3"), matched)

    def test_build_report_detail_item_rows_deduplicates_support_rows(self):
        unsupported_row = sdr.ObjectSupportReportRow(
            obj_type="VIEW",
            src_full="A.V1",
            tgt_full="A.V1",
            support_state=sdr.SUPPORT_STATE_UNSUPPORTED,
            reason_code="VIEW_SYS_OBJ",
            reason="unsupported",
            dependency="-",
            action="改造/授权",
            detail="SYS.OBJ$",
            root_cause="A.V1(VIEW_SYS_OBJ)",
        )
        support_summary = sdr.SupportClassificationResult(
            support_state_map={("VIEW", "A.V1"): unsupported_row},
            missing_detail_rows=[unsupported_row],
            unsupported_rows=[unsupported_row],
            extra_missing_rows=[],
            missing_support_counts={},
            extra_blocked_counts={},
            unsupported_table_keys=set(),
            unsupported_view_keys={("A", "V1")},
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[],
        )
        rows, truncated, truncated_count = sdr._build_report_detail_item_rows(
            tv_results={"mismatched": [], "missing": []},
            extra_results={},
            support_summary=support_summary,
            table_target_map={},
            max_rows=0,
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        key_counts = {}
        for row in rows:
            key = (row.get("item_type"), row.get("item_value"))
            key_counts[key] = key_counts.get(key, 0) + 1
        self.assertEqual(key_counts.get(("REASON_CODE", "VIEW_SYS_OBJ")), 1)
        self.assertEqual(key_counts.get(("ACTION", "改造/授权")), 1)
        self.assertEqual(key_counts.get(("DETAIL", "SYS.OBJ$")), 1)

    def test_build_report_detail_item_rows_preserves_risky_report_type(self):
        risky_row = sdr.ObjectSupportReportRow(
            obj_type="VIEW",
            src_full="SRC.V_RISK",
            tgt_full="TGT.V_RISK",
            support_state=sdr.SUPPORT_STATE_RISKY,
            reason_code="VIEW_PARTIAL",
            reason="risky",
            dependency="-",
            action="REVIEW",
            detail="PARTIAL",
            root_cause="SRC.V_RISK(VIEW_PARTIAL)",
        )
        support_summary = sdr.SupportClassificationResult(
            support_state_map={("VIEW", "SRC.V_RISK"): risky_row},
            missing_detail_rows=[],
            unsupported_rows=[risky_row],
            extra_missing_rows=[],
            missing_support_counts={},
            extra_blocked_counts={},
            unsupported_table_keys=set(),
            unsupported_view_keys={("SRC", "V_RISK")},
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[],
        )
        rows, truncated, truncated_count = sdr._build_report_detail_item_rows(
            tv_results={"mismatched": [], "missing": []},
            extra_results={},
            support_summary=support_summary,
            table_target_map={},
            max_rows=0,
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        risky_rows = [row for row in rows if row.get("report_type") == "RISKY"]
        self.assertTrue(risky_rows)
        self.assertTrue(all(row.get("status") == sdr.SUPPORT_STATE_RISKY for row in risky_rows))

    def test_build_report_detail_item_rows_includes_temp_trigger_reason_code(self):
        unsupported_row = sdr.ObjectSupportReportRow(
            obj_type="TRIGGER",
            src_full="SRC.TRG_TEMP_BI",
            tgt_full="TGT.TRG_TEMP_BI",
            support_state=sdr.SUPPORT_STATE_UNSUPPORTED,
            reason_code=sdr.TRIGGER_TEMP_TABLE_UNSUPPORTED_REASON_CODE,
            reason=sdr.TRIGGER_TEMP_TABLE_UNSUPPORTED_REASON,
            dependency="TGT.T_TEMP",
            action="改造/不迁移",
            detail="TRIGGER",
            root_cause="SRC.T_TEMP(TEMP_TABLE)",
        )
        support_summary = sdr.SupportClassificationResult(
            support_state_map={("TRIGGER", "SRC.TRG_TEMP_BI"): unsupported_row},
            missing_detail_rows=[],
            unsupported_rows=[unsupported_row],
            extra_missing_rows=[],
            missing_support_counts={},
            extra_blocked_counts={},
            unsupported_table_keys={("SRC", "T_TEMP")},
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[],
        )
        rows, truncated, truncated_count = sdr._build_report_detail_item_rows(
            tv_results={"mismatched": [], "missing": []},
            extra_results={},
            support_summary=support_summary,
            table_target_map={},
            max_rows=0,
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        reason_rows = [
            row for row in rows
            if row.get("item_type") == "REASON_CODE"
            and row.get("item_value") == sdr.TRIGGER_TEMP_TABLE_UNSUPPORTED_REASON_CODE
        ]
        self.assertEqual(len(reason_rows), 1)

    def test_build_report_detail_rows_includes_extra_missing_rows(self):
        base_missing = sdr.ObjectSupportReportRow(
            obj_type="TABLE",
            src_full="SRC.T1",
            tgt_full="TGT.T1",
            support_state=sdr.SUPPORT_STATE_SUPPORTED,
            reason_code="-",
            reason="-",
            dependency="-",
            action="FIXUP",
            detail="-",
            root_cause="-",
        )
        extra_missing = sdr.ObjectSupportReportRow(
            obj_type="INDEX",
            src_full="SRC.IDX_T1",
            tgt_full="TGT.IDX_T1",
            support_state=sdr.SUPPORT_STATE_SUPPORTED,
            reason_code="-",
            reason="-",
            dependency="TGT.T1",
            action="FIXUP",
            detail="INDEX",
            root_cause="-",
        )
        support_summary = sdr.SupportClassificationResult(
            support_state_map={},
            missing_detail_rows=[base_missing],
            unsupported_rows=[],
            extra_missing_rows=[extra_missing],
            missing_support_counts={},
            extra_blocked_counts={},
            unsupported_table_keys=set(),
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[],
        )
        rows, truncated, truncated_count = sdr._build_report_detail_rows(
            {"report_db_detail_mode_set": {"missing"}, "report_db_detail_max_rows": 0},
            tv_results={"mismatched": [], "ok": [], "skipped": []},
            extra_results={},
            support_summary=support_summary,
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["object_type"] for row in rows}, {"TABLE", "INDEX"})

    def test_build_report_detail_rows_preserves_risky_report_type(self):
        risky_row = sdr.ObjectSupportReportRow(
            obj_type="VIEW",
            src_full="SRC.V_RISK",
            tgt_full="TGT.V_RISK",
            support_state=sdr.SUPPORT_STATE_RISKY,
            reason_code="VIEW_PARTIAL",
            reason="risky",
            dependency="-",
            action="REVIEW",
            detail="PARTIAL",
            root_cause="SRC.V_RISK(VIEW_PARTIAL)",
        )
        support_summary = sdr.SupportClassificationResult(
            support_state_map={("VIEW", "SRC.V_RISK"): risky_row},
            missing_detail_rows=[],
            unsupported_rows=[risky_row],
            extra_missing_rows=[],
            missing_support_counts={},
            extra_blocked_counts={},
            unsupported_table_keys=set(),
            unsupported_view_keys={("SRC", "V_RISK")},
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[],
        )
        rows, truncated, truncated_count = sdr._build_report_detail_rows(
            {"report_db_detail_mode_set": {"unsupported"}, "report_db_detail_max_rows": 0},
            tv_results={"mismatched": [], "ok": [], "skipped": []},
            extra_results={},
            support_summary=support_summary,
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["report_type"], "RISKY")
        self.assertEqual(rows[0]["status"], sdr.SUPPORT_STATE_RISKY)

    def test_build_package_missing_support_rows(self):
        pkg_rows = [
            sdr.PackageCompareRow(
                src_full="SRC.P1",
                obj_type="PACKAGE",
                src_status="VALID",
                tgt_full="TGT.P1",
                tgt_status="MISSING",
                result="MISSING_TARGET",
                error_count=0,
                first_error="",
            ),
            sdr.PackageCompareRow(
                src_full="SRC.P1",
                obj_type="PACKAGE BODY",
                src_status="VALID",
                tgt_full="TGT.P1",
                tgt_status="MISSING",
                result="MISSING_TARGET",
                error_count=0,
                first_error="",
            ),
            sdr.PackageCompareRow(
                src_full="SRC.P2",
                obj_type="PACKAGE",
                src_status="VALID",
                tgt_full="TGT.P2",
                tgt_status="VALID",
                result="OK",
                error_count=0,
                first_error="",
            ),
        ]
        rows = sdr.build_package_missing_support_rows({"rows": pkg_rows, "summary": {}})
        self.assertEqual(len(rows), 2)
        self.assertEqual({r.obj_type for r in rows}, {"PACKAGE", "PACKAGE BODY"})
        self.assertTrue(all(r.support_state == sdr.SUPPORT_STATE_SUPPORTED for r in rows))

    def test_build_report_detail_rows_includes_package_missing_additional_rows(self):
        support_summary = sdr.SupportClassificationResult(
            support_state_map={},
            missing_detail_rows=[],
            unsupported_rows=[],
            extra_missing_rows=[],
            missing_support_counts={},
            extra_blocked_counts={},
            unsupported_table_keys=set(),
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[],
        )
        pkg_rows = [
            sdr.ObjectSupportReportRow(
                obj_type="PACKAGE",
                src_full="SRC.P1",
                tgt_full="TGT.P1",
                support_state=sdr.SUPPORT_STATE_SUPPORTED,
                reason_code="-",
                reason="-",
                dependency="-",
                action="FIXUP",
                detail="-",
                root_cause="-",
            )
        ]
        rows, truncated, truncated_count = sdr._build_report_detail_rows(
            {"report_db_detail_mode_set": {"missing"}, "report_db_detail_max_rows": 0},
            tv_results={"mismatched": [], "ok": [], "skipped": []},
            extra_results={},
            support_summary=support_summary,
            additional_missing_rows=pkg_rows,
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["object_type"], "PACKAGE")

    def test_build_report_detail_rows_includes_case_sensitive_findings(self):
        support_summary = sdr.SupportClassificationResult(
            support_state_map={},
            missing_detail_rows=[],
            unsupported_rows=[],
            extra_missing_rows=[],
            missing_support_counts={},
            extra_blocked_counts={},
            unsupported_table_keys=set(),
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[],
        )
        findings = [
            sdr.CaseSensitiveIdentifierFinding(
                side="SOURCE",
                owner="SrcMix",
                object_name="ObjMix",
                object_type="TABLE",
                context="Oracle.DBA_OBJECTS",
                mode="warn",
            )
        ]
        rows, truncated, truncated_count = sdr._build_report_detail_rows(
            {"report_db_detail_mode_set": {"unsupported"}, "report_db_detail_max_rows": 0},
            tv_results={"mismatched": [], "ok": [], "skipped": []},
            extra_results={},
            support_summary=support_summary,
            case_sensitive_findings=findings,
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "CASE_SENSITIVE_IDENTIFIER")
        self.assertEqual(rows[0]["object_type"], "TABLE")

    def test_build_report_detail_item_rows_includes_case_sensitive_findings(self):
        rows, truncated, truncated_count = sdr._build_report_detail_item_rows(
            tv_results={"mismatched": [], "missing": []},
            extra_results={},
            support_summary=None,
            table_target_map={},
            max_rows=0,
            case_sensitive_findings=[
                sdr.CaseSensitiveIdentifierFinding(
                    side="TARGET",
                    owner="TgtMix",
                    object_name="ObjMix",
                    object_type="VIEW",
                    context="OceanBase.DBA_OBJECTS",
                    mode="warn",
                )
            ],
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        matched = [
            r for r in rows
            if r.get("item_type") == "CASE_SENSITIVE_IDENTIFIER"
            and r.get("object_type") == "VIEW"
            and r.get("target_schema") == "TgtMix"
            and r.get("target_name") == "ObjMix"
        ]
        self.assertEqual(len(matched), 1)

    def test_sum_primary_missing_count(self):
        summary = {
            "missing": {
                "TABLE": 9,
                "VIEW": 2,
                "FUNCTION": 1,
                "PROCEDURE": 1,
                "TYPE": 1,
                "PACKAGE": 1,
                "PACKAGE BODY": 1,
                "INDEX": 0,
            }
        }
        total = sdr.sum_primary_missing_count(
            summary,
            {"TABLE", "VIEW", "FUNCTION", "PROCEDURE", "TYPE", "PACKAGE", "PACKAGE BODY"}
        )
        self.assertEqual(total, 16)

    def test_compute_count_invariant_mismatches_excluded_not_in_rhs(self):
        mismatches = sdr.compute_count_invariant_mismatches(
            oracle_counts={"TABLE": 42, "INDEX": 33},
            ob_counts={"TABLE": 10, "INDEX": 32},
            fixable_missing_counts={"TABLE": 15, "INDEX": 0},
            unsupported_summary_counts={"TABLE": 17, "INDEX": 1},
            excluded_summary_counts={"TABLE": 1, "INDEX": 1},
            count_types=["TABLE", "INDEX"],
        )
        self.assertEqual(mismatches, [])

    def test_build_report_detail_item_rows_includes_extra_missing_rows(self):
        extra_missing = sdr.ObjectSupportReportRow(
            obj_type="INDEX",
            src_full="SRC.IDX_T1",
            tgt_full="TGT.IDX_T1",
            support_state=sdr.SUPPORT_STATE_SUPPORTED,
            reason_code="-",
            reason="-",
            dependency="TGT.T1",
            action="FIXUP",
            detail="INDEX",
            root_cause="-",
        )
        support_summary = sdr.SupportClassificationResult(
            support_state_map={},
            missing_detail_rows=[],
            unsupported_rows=[],
            extra_missing_rows=[extra_missing],
            missing_support_counts={},
            extra_blocked_counts={},
            unsupported_table_keys=set(),
            unsupported_view_keys=set(),
            view_compat_map={},
            view_constraint_cleaned_rows=[],
            view_constraint_uncleanable_rows=[],
        )
        rows, truncated, truncated_count = sdr._build_report_detail_item_rows(
            tv_results={"mismatched": [], "missing": []},
            extra_results={},
            support_summary=support_summary,
            table_target_map={},
            max_rows=0,
        )
        self.assertFalse(truncated)
        self.assertEqual(truncated_count, 0)
        self.assertTrue(any(r.get("report_type") == "MISSING" and r.get("object_type") == "INDEX" for r in rows))

    def test_insert_report_detail_item_rows_fallback_single_row(self):
        rows = [
            {
                "report_type": "MISSING",
                "object_type": "TABLE",
                "source_schema": "SRC",
                "source_name": "T1",
                "target_schema": "TGT",
                "target_name": "T1",
                "item_type": "DETAIL",
                "item_key": "",
                "src_value": "",
                "tgt_value": "",
                "item_value": "A",
                "status": "SUPPORTED",
            },
            {
                "report_type": "MISSING",
                "object_type": "TABLE",
                "source_schema": "SRC",
                "source_name": "T2",
                "target_schema": "TGT",
                "target_name": "T2",
                "item_type": "DETAIL",
                "item_key": "",
                "src_value": "",
                "tgt_value": "",
                "item_value": "B",
                "status": "SUPPORTED",
            },
        ]
        sql_calls = []

        def _fake_exec(_cfg, sql, timeout=0):
            sql_calls.append(sql)
            if sql.startswith("INSERT ALL") and sql.count("\n  INTO ") >= 2:
                return False, "", "batch failed"
            return True, "", ""

        with mock.patch.object(sdr, "obclient_run_sql_commit", side_effect=_fake_exec):
            with mock.patch.object(sdr, "_record_report_db_write_error") as mocked_err:
                ok = sdr._insert_report_detail_item_rows(
                    {"executable": "/usr/bin/obclient"},
                    "",
                    "RID",
                    rows,
                    batch_size=10
                )
        self.assertFalse(ok)
        self.assertGreaterEqual(len(sql_calls), 3)
        self.assertEqual(mocked_err.call_count, 1)

    def test_report_db_ddls_no_foreign_keys(self):
        source = inspect.getsource(sdr.ensure_report_db_tables_exist)
        self.assertNotIn("FOREIGN KEY", source)

    def test_report_db_excluded_status_column_width_and_migration(self):
        source = inspect.getsource(sdr.ensure_report_db_tables_exist)
        self.assertIn("STATUS              VARCHAR2(64) NOT NULL", source)
        self.assertIn("def _ensure_varchar2_column_min_len", source)

        all_tables_out = "\n".join(sorted(sdr.REPORT_DB_TABLES.values()))
        executed_commit_sql: List[str] = []

        def fake_run_sql(_cfg, sql, timeout=None):
            sql_u = (sql or "").upper()
            if "FROM ALL_TABLES" in sql_u:
                return True, all_tables_out, ""
            if "FROM ALL_CONSTRAINTS" in sql_u:
                return True, "", ""
            if (
                "FROM ALL_TAB_COLUMNS" in sql_u
                and "TABLE_NAME = 'DIFF_REPORT_EXCLUDED_OBJECT'" in sql_u
                and "COLUMN_NAME = 'STATUS'" in sql_u
            ):
                return True, "16", ""
            if "FROM ALL_TAB_COLUMNS" in sql_u:
                return True, "COLUMN_EXISTS", ""
            return True, "", ""

        def fake_run_sql_commit(_cfg, sql, timeout=None):
            executed_commit_sql.append(sql or "")
            return True, "", ""

        with mock.patch.object(sdr, "obclient_run_sql", side_effect=fake_run_sql), \
             mock.patch.object(sdr, "obclient_run_sql_commit", side_effect=fake_run_sql_commit):
            ok, err = sdr.ensure_report_db_tables_exist({"executable": "/usr/bin/obclient"}, {"report_db_schema": ""})

        self.assertTrue(ok)
        self.assertEqual(err, "")
        self.assertTrue(
            any(
                "ALTER TABLE DIFF_REPORT_EXCLUDED_OBJECT MODIFY (STATUS VARCHAR2(64))" in sql.upper()
                for sql in executed_commit_sql
            ),
            "expected STATUS column expansion DDL for DIFF_REPORT_EXCLUDED_OBJECT",
        )

    def test_report_db_summary_write_columns_migration(self):
        all_tables_out = "\n".join(sorted(sdr.REPORT_DB_TABLES.values()))
        executed_commit_sql: List[str] = []

        def fake_run_sql(_cfg, sql, timeout=None):
            sql_u = (sql or "").upper()
            if "FROM ALL_TABLES" in sql_u:
                return True, all_tables_out, ""
            if "FROM ALL_CONSTRAINTS" in sql_u:
                return True, "", ""
            if "FROM ALL_TAB_COLUMNS" in sql_u and "TABLE_NAME = 'DIFF_REPORT_SUMMARY'" in sql_u:
                if "COLUMN_NAME = 'WRITE_STATUS'" in sql_u:
                    return True, "", ""
                if "COLUMN_NAME = 'WRITE_NOTE'" in sql_u:
                    return True, "", ""
                if "COLUMN_NAME = 'WRITE_EXPECTED_ROWS'" in sql_u:
                    return True, "", ""
                if "COLUMN_NAME = 'WRITE_ACTUAL_ROWS'" in sql_u:
                    return True, "", ""
                if "COLUMN_NAME = 'WRITE_CHECKED_AT'" in sql_u:
                    return True, "", ""
                return True, "COLUMN_EXISTS", ""
            if "FROM ALL_TAB_COLUMNS" in sql_u:
                return True, "COLUMN_EXISTS", ""
            return True, "", ""

        def fake_run_sql_commit(_cfg, sql, timeout=None):
            executed_commit_sql.append(sql or "")
            return True, "", ""

        with mock.patch.object(sdr, "obclient_run_sql", side_effect=fake_run_sql), \
             mock.patch.object(sdr, "obclient_run_sql_commit", side_effect=fake_run_sql_commit):
            ok, err = sdr.ensure_report_db_tables_exist({"executable": "/usr/bin/obclient"}, {"report_db_schema": ""})

        self.assertTrue(ok)
        self.assertEqual(err, "")
        ddl_text = "\n".join(executed_commit_sql).upper()
        self.assertIn("ALTER TABLE DIFF_REPORT_SUMMARY ADD (WRITE_STATUS VARCHAR2(16) DEFAULT 'READY')".upper(), ddl_text)
        self.assertIn("ALTER TABLE DIFF_REPORT_SUMMARY ADD (WRITE_NOTE VARCHAR2(1000))".upper(), ddl_text)
        self.assertIn("ALTER TABLE DIFF_REPORT_SUMMARY ADD (WRITE_EXPECTED_ROWS NUMBER DEFAULT 0)".upper(), ddl_text)
        self.assertIn("ALTER TABLE DIFF_REPORT_SUMMARY ADD (WRITE_ACTUAL_ROWS NUMBER DEFAULT 0)".upper(), ddl_text)
        self.assertIn("ALTER TABLE DIFF_REPORT_SUMMARY ADD (WRITE_CHECKED_AT TIMESTAMP)".upper(), ddl_text)

    def test_apply_ob_feature_gates_auto_ob_442_plus(self):
        settings = {
            "generate_interval_partition_fixup": "auto",
            "mview_check_fixup_mode": "auto",
        }
        result = sdr.apply_ob_feature_gates(settings, "4.4.2.1")
        self.assertTrue(result["version_known"])
        self.assertTrue(result["is_ob_442_plus"])
        self.assertFalse(result["interval_enabled"])
        self.assertTrue(result["mview_enabled"])
        self.assertFalse(settings["effective_interval_fixup_enabled"])
        self.assertTrue(settings["effective_mview_enabled"])
        self.assertNotIn("MATERIALIZED VIEW", settings["effective_print_only_primary_types"])

    def test_apply_ob_feature_gates_auto_ob_below_442(self):
        settings = {
            "generate_interval_partition_fixup": "auto",
            "mview_check_fixup_mode": "auto",
        }
        result = sdr.apply_ob_feature_gates(settings, "4.2.5.7")
        self.assertTrue(result["version_known"])
        self.assertFalse(result["is_ob_442_plus"])
        self.assertTrue(result["interval_enabled"])
        self.assertFalse(result["mview_enabled"])
        self.assertTrue(settings["effective_interval_fixup_enabled"])
        self.assertFalse(settings["effective_mview_enabled"])
        self.assertIn("MATERIALIZED VIEW", settings["effective_print_only_primary_types"])

    def test_apply_ob_feature_gates_auto_unknown_version_fallback(self):
        settings = {
            "generate_interval_partition_fixup": "auto",
            "mview_check_fixup_mode": "auto",
        }
        result = sdr.apply_ob_feature_gates(settings, None)
        self.assertFalse(result["version_known"])
        self.assertEqual(result["interval_reason"], "auto(version_unknown_fallback)")
        self.assertEqual(result["mview_reason"], "auto(version_unknown_fallback)")
        self.assertTrue(result["interval_enabled"])
        self.assertFalse(result["mview_enabled"])

    def test_apply_ob_feature_gates_manual_override(self):
        settings = {
            "generate_interval_partition_fixup": "false",
            "mview_check_fixup_mode": "on",
        }
        result = sdr.apply_ob_feature_gates(settings, "4.2.5.7")
        self.assertFalse(result["interval_enabled"])
        self.assertTrue(result["mview_enabled"])
        self.assertEqual(result["interval_reason"], "manual(false)")
        self.assertEqual(result["mview_reason"], "manual(on)")
        self.assertEqual(settings["generate_interval_partition_fixup"], False)
        self.assertTrue(settings["effective_mview_enabled"])

    def test_apply_ob_feature_gates_normalize_legacy_values(self):
        settings = {
            "generate_interval_partition_fixup": "yes",
            "mview_check_fixup_mode": "OFF",
        }
        result = sdr.apply_ob_feature_gates(settings, "4.4.2.0")
        self.assertEqual(result["interval_mode"], "true")
        self.assertEqual(result["mview_mode"], "off")
        self.assertTrue(result["interval_enabled"])
        self.assertFalse(result["mview_enabled"])

    def test_sql_masker_handles_q_quote_with_inner_single_quote(self):
        sql = "SELECT q'[X'SRC.T1]' AS TXT FROM dual"
        masker = sdr.SqlMasker(sql)
        self.assertNotIn("SRC.T1", masker.masked_sql)
        self.assertEqual(masker.unmask(masker.masked_sql), sql)

    def test_clean_pragma_statements_keeps_verified_supported_pragmas(self):
        ddl = (
            "DECLARE\n"
            "  e_dup EXCEPTION;\n"
            "  PRAGMA EXCEPTION_INIT(e_dup, -1);\n"
            "  PRAGMA AUTONOMOUS_TRANSACTION;\n"
            "  PRAGMA SERIALLY_REUSABLE;\n"
            "BEGIN\n"
            "  NULL;\n"
            "END;\n"
        )
        cleaned = sdr.clean_pragma_statements(ddl)
        self.assertIn("PRAGMA EXCEPTION_INIT", cleaned.upper())
        self.assertIn("PRAGMA AUTONOMOUS_TRANSACTION", cleaned.upper())
        self.assertIn("PRAGMA SERIALLY_REUSABLE", cleaned.upper())

    def test_apply_ddl_cleanup_rules_trigger_keeps_autonomous_transaction(self):
        ddl = (
            "CREATE OR REPLACE TRIGGER SRC.TRG_A\n"
            "BEFORE INSERT ON SRC.T1\n"
            "DECLARE\n"
            "  PRAGMA AUTONOMOUS_TRANSACTION;\n"
            "BEGIN\n"
            "  NULL;\n"
            "END;\n"
            "/\n"
        )
        cleaned = sdr.apply_ddl_cleanup_rules(ddl, "TRIGGER")
        self.assertIn("PRAGMA AUTONOMOUS_TRANSACTION", cleaned.upper())

    def test_apply_ddl_cleanup_rules_package_keeps_autonomous_transaction(self):
        ddl = (
            "CREATE OR REPLACE PACKAGE BODY SRC.PKG AS\n"
            "  PROCEDURE P IS\n"
            "    PRAGMA AUTONOMOUS_TRANSACTION;\n"
            "  BEGIN\n"
            "    NULL;\n"
            "  END;\n"
            "END;\n"
            "/\n"
        )
        cleaned = sdr.apply_ddl_cleanup_rules(ddl, "PACKAGE BODY")
        self.assertIn("PRAGMA AUTONOMOUS_TRANSACTION", cleaned.upper())

    def test_apply_ddl_cleanup_rules_with_audit_marks_preserved_pragmas(self):
        ddl = (
            "CREATE OR REPLACE PACKAGE BODY SRC.PKG AS\n"
            "  PRAGMA SERIALLY_REUSABLE;\n"
            "  PROCEDURE P IS\n"
            "    PRAGMA AUTONOMOUS_TRANSACTION;\n"
            "  BEGIN\n"
            "    NULL;\n"
            "  END;\n"
            "END;\n"
            "/\n"
        )
        cleaned, actions = sdr.apply_ddl_cleanup_rules_with_audit(ddl, "PACKAGE BODY")
        self.assertIn("PRAGMA AUTONOMOUS_TRANSACTION", cleaned.upper())
        self.assertIn("PRAGMA SERIALLY_REUSABLE", cleaned.upper())
        names = {action.rule_name for action in actions if action.action_status == sdr.DDL_CLEAN_ACTION_PRESERVED}
        self.assertIn("preserve_autonomous_transaction", names)
        self.assertIn("preserve_serially_reusable", names)

    def test_apply_ddl_cleanup_rules_with_audit_marks_preserved_storage_clause(self):
        ddl = (
            "CREATE TABLE SRC.T1 (\n"
            "  ID NUMBER\n"
            ")\n"
            "TABLESPACE USERS\n"
            "STORAGE (INITIAL 64K NEXT 64K);"
        )
        cleaned, actions = sdr.apply_ddl_cleanup_rules_with_audit(ddl, "TABLE")
        self.assertIn("TABLESPACE USERS", cleaned.upper())
        self.assertIn("STORAGE (INITIAL 64K NEXT 64K)", cleaned.upper())
        names = {action.rule_name for action in actions if action.action_status == sdr.DDL_CLEAN_ACTION_PRESERVED}
        self.assertIn("preserve_storage_clause", names)
        self.assertIn("preserve_tablespace_clause", names)

    def test_protect_type_not_persistable_clause_only_for_type(self):
        ddl = 'CREATE TYPE A.T1 AS OBJECT (ID NUMBER) NOT PERSISTABLE'
        protected = sdr.protect_type_not_persistable_clause(ddl, 'TYPE')
        self.assertIn(sdr.TYPE_NOT_PERSISTABLE_TOKEN, protected)
        restored = sdr.restore_type_not_persistable_clause(protected, 'TYPE')
        self.assertIn("NOT PERSISTABLE", restored.upper())
        untouched = sdr.protect_type_not_persistable_clause(ddl, 'TABLE')
        self.assertEqual(untouched, ddl)

    def test_apply_ddl_cleanup_rules_type_keeps_not_persistable(self):
        ddl = (
            'CREATE OR REPLACE EDITIONABLE TYPE "SRC"."T1" '
            'AS OBJECT (ID NUMBER) NOT PERSISTABLE;\n'
        )
        cleaned = sdr.apply_ddl_cleanup_rules(ddl, "TYPE")
        self.assertIn("NOT PERSISTABLE", cleaned.upper())

    def test_apply_ddl_cleanup_rules_with_audit_type_keeps_not_persistable_without_extra_noise(self):
        ddl = (
            'CREATE OR REPLACE EDITIONABLE TYPE "SRC"."T1" '
            'AS OBJECT (ID NUMBER) NOT PERSISTABLE;\n'
        )
        cleaned, actions = sdr.apply_ddl_cleanup_rules_with_audit(ddl, "TYPE")
        self.assertIn("NOT PERSISTABLE", cleaned.upper())
        names = {action.rule_name for action in actions}
        self.assertNotIn("preserve_type_not_persistable", names)
        self.assertIn("clean_editionable_flags", names)

    def test_build_cleanup_action_from_rule_marks_sample_degradation(self):
        def bad_rule(_text):
            return "AFTER"
        bad_rule.__name__ = "bad_rule_for_test"

        original = sdr.DDL_CLEAN_RULE_META.get("bad_rule_for_test")
        sdr.DDL_CLEAN_RULE_META["bad_rule_for_test"] = sdr.DdlCleanupRuleMeta(
            rule_name="bad_rule_for_test",
            category=sdr.DDL_CLEAN_CATEGORY_FORMAT_ONLY,
            evidence_level=sdr.DDL_CLEAN_EVIDENCE_NOT_APPLICABLE,
            note="note",
            sample_builder=lambda before, after: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        try:
            action = sdr._build_cleanup_action_from_rule(bad_rule, "BEFORE", "AFTER")
            self.assertIsNotNone(action)
            self.assertIn("SAMPLE_EXTRACTION_DEGRADED", action.note)
            self.assertEqual(action.change_count, 1)
            self.assertEqual(action.samples, [])
        finally:
            if original is None:
                del sdr.DDL_CLEAN_RULE_META["bad_rule_for_test"]
            else:
                sdr.DDL_CLEAN_RULE_META["bad_rule_for_test"] = original

    def test_save_report_to_db_uses_nested_endpoint_info(self):
        now = datetime.now()
        run_summary = sdr.RunSummary(
            start_time=now,
            end_time=now,
            total_seconds=1.0,
            phases=[],
            actions_done=[],
            actions_skipped=[],
            findings=[],
            attention=[],
            manual_actions=[],
            change_notices=[],
            next_steps=[],
        )
        run_ctx = sdr.RunSummaryContext(
            start_time=now,
            start_perf=0.0,
            phase_durations={},
            phase_skip_reasons={},
            enabled_primary_types=set(),
            enabled_extra_types=set(),
            print_only_types=set(),
            total_checked=0,
            enable_dependencies_check=False,
            enable_comment_check=False,
            enable_grant_generation=False,
            enable_schema_mapping_infer=False,
            fixup_enabled=False,
            fixup_dir="fixup_scripts",
            dependency_chain_file=None,
            view_chain_file=None,
            trigger_list_summary=None,
            report_start_perf=0.0,
        )
        settings = {
            "report_to_db": True,
            "report_db_store_scope": "summary",
            "report_db_schema_prefix": "",
            "report_db_insert_batch": 10,
            "report_db_detail_mode_set": set(),
            "report_db_detail_item_enable": False,
            "report_db_save_full_json": False,
            "report_db_fail_abort": False,
            "source_schemas_list": ["SRC_A"],
            "enabled_primary_types": {"TABLE"},
            "enabled_extra_types": set(),
            "report_retention_days": 0,
        }
        endpoint_info = {
            "oracle": {
                "host": "10.0.0.1",
                "port": "1521",
                "service_name": "ORCL",
                "user": "SRC_USER",
            },
            "oceanbase": {
                "host": "10.0.0.2",
                "port": "2883",
                "current_database": "obtenant",
                "current_user": "OMS_USER",
            },
        }
        sql_calls: List[str] = []

        def _fake_commit(_ob_cfg, sql):
            sql_calls.append(sql)
            return True, "", ""

        with mock.patch.object(sdr, "ensure_report_db_tables_exist", return_value=(True, None)), \
             mock.patch.object(sdr, "obclient_run_sql_commit", side_effect=_fake_commit), \
             mock.patch.object(sdr, "_insert_report_counts_rows", return_value=True), \
             mock.patch.object(sdr, "_build_report_artifact_rows", return_value=[]), \
             mock.patch.object(sdr, "ensure_report_db_views_exist", return_value=[]), \
             mock.patch.object(sdr, "build_report_sql_template_file", return_value=None), \
             mock.patch.object(
                 sdr,
                 "_verify_report_db_row_consistency",
                 return_value=(True, {"summary": 1, "counts": 0}, "")
             ):
            ok, report_id = sdr.save_report_to_db(
                {"executable": "/usr/bin/obclient"},
                settings,
                run_summary,
                run_ctx,
                "20260212_160000",
                None,
                {"missing": [], "mismatched": [], "ok": [], "skipped": []},
                {},
                None,
                endpoint_info,
                None,
                None,
                set(),
                grant_plan=None,
                usability_summary=None,
                package_results=None,
                trigger_status_rows=None,
                constraint_status_rows=None,
                dependency_report=None,
                expected_dependency_pairs=None,
                view_chain_file=None,
                remap_conflicts=None,
                full_object_mapping=None,
                remap_rules=None,
                blacklist_report_rows=None,
                fixup_skip_summary=None,
                blacklisted_table_keys=None,
                excluded_object_rows=None,
            )
        self.assertTrue(ok)
        self.assertIsNotNone(report_id)
        self.assertGreaterEqual(len(sql_calls), 1)
        first_sql = sql_calls[0]
        self.assertIn("'10.0.0.1'", first_sql)
        self.assertIn("'10.0.0.2'", first_sql)
        self.assertIn("'ORCL'", first_sql)
        self.assertIn("'obtenant'", first_sql)

    def test_save_report_to_db_marks_warn_for_extra_constraint_mismatch(self):
        now = datetime.now()
        run_summary = sdr.RunSummary(
            start_time=now,
            end_time=now,
            total_seconds=1.0,
            phases=[],
            actions_done=[],
            actions_skipped=[],
            findings=[],
            attention=[],
            manual_actions=[],
            change_notices=[],
            next_steps=[],
        )
        run_ctx = sdr.RunSummaryContext(
            start_time=now,
            start_perf=0.0,
            phase_durations={},
            phase_skip_reasons={},
            enabled_primary_types={"TABLE"},
            enabled_extra_types={"CONSTRAINT"},
            print_only_types=set(),
            total_checked=1,
            enable_dependencies_check=False,
            enable_comment_check=False,
            enable_grant_generation=False,
            enable_schema_mapping_infer=False,
            fixup_enabled=False,
            fixup_dir="fixup_scripts",
            dependency_chain_file=None,
            view_chain_file=None,
            trigger_list_summary=None,
            report_start_perf=0.0,
        )
        settings = {
            "report_to_db": True,
            "report_db_store_scope": "summary",
            "report_db_schema_prefix": "",
            "report_db_insert_batch": 10,
            "report_db_detail_mode_set": set(),
            "report_db_detail_item_enable": False,
            "report_db_save_full_json": False,
            "report_db_fail_abort": False,
            "source_schemas_list": ["SRC_A"],
            "enabled_primary_types": {"TABLE"},
            "enabled_extra_types": {"CONSTRAINT"},
            "report_retention_days": 0,
        }
        sql_calls = []

        def _fake_commit(_ob_cfg, sql):
            sql_calls.append(sql)
            return True, "", ""

        extra_results = {
            "index_mismatched": [],
            "constraint_mismatched": [
                sdr.ConstraintMismatch(
                    table="TGT.T1",
                    missing_constraints=set(),
                    extra_constraints={"T1_OBCHECK_1"},
                    detail_mismatch=["CHECK_DUPLICATE_NOTNULL"],
                    downgraded_pk_constraints=set(),
                )
            ],
            "sequence_mismatched": [],
            "trigger_mismatched": [],
            "index_unsupported": [],
            "constraint_unsupported": [],
        }

        with mock.patch.object(sdr, "ensure_report_db_tables_exist", return_value=(True, None)), \
             mock.patch.object(sdr, "obclient_run_sql_commit", side_effect=_fake_commit), \
             mock.patch.object(sdr, "_insert_report_counts_rows", return_value=True), \
             mock.patch.object(sdr, "_build_report_artifact_rows", return_value=[]), \
             mock.patch.object(sdr, "ensure_report_db_views_exist", return_value=[]), \
             mock.patch.object(sdr, "build_report_sql_template_file", return_value=None), \
             mock.patch.object(
                 sdr,
                 "_verify_report_db_row_consistency",
                 return_value=(True, {"summary": 1, "counts": 0}, "")
             ):
            ok, report_id = sdr.save_report_to_db(
                {"executable": "/usr/bin/obclient"},
                settings,
                run_summary,
                run_ctx,
                "20260321_120000",
                None,
                {"missing": [], "mismatched": [], "ok": [("TABLE", "TGT.T1", "SRC.T1")], "skipped": []},
                extra_results,
                None,
                {},
                None,
                None,
                set(),
                grant_plan=None,
                usability_summary=None,
                package_results=None,
                trigger_status_rows=None,
                constraint_status_rows=None,
                dependency_report=None,
                expected_dependency_pairs=None,
                view_chain_file=None,
                remap_conflicts=None,
                full_object_mapping=None,
                remap_rules=None,
                blacklist_report_rows=None,
                fixup_skip_summary=None,
                blacklisted_table_keys=None,
                excluded_object_rows=None,
            )
        self.assertTrue(ok)
        self.assertIsNotNone(report_id)
        first_sql = sql_calls[0]
        self.assertIn("'WARN'", first_sql)
        self.assertIn("存在 1 个不匹配对象", first_sql)

    def test_save_report_to_db_marks_failed_when_row_consistency_mismatch(self):
        now = datetime.now()
        run_summary = sdr.RunSummary(
            start_time=now,
            end_time=now,
            total_seconds=1.0,
            phases=[],
            actions_done=[],
            actions_skipped=[],
            findings=[],
            attention=[],
            manual_actions=[],
            change_notices=[],
            next_steps=[],
        )
        run_ctx = sdr.RunSummaryContext(
            start_time=now,
            start_perf=0.0,
            phase_durations={},
            phase_skip_reasons={},
            enabled_primary_types=set(),
            enabled_extra_types=set(),
            print_only_types=set(),
            total_checked=0,
            enable_dependencies_check=False,
            enable_comment_check=False,
            enable_grant_generation=False,
            enable_schema_mapping_infer=False,
            fixup_enabled=False,
            fixup_dir="fixup_scripts",
            dependency_chain_file=None,
            view_chain_file=None,
            trigger_list_summary=None,
            report_start_perf=0.0,
        )
        settings = {
            "report_to_db": True,
            "report_db_store_scope": "summary",
            "report_db_insert_batch": 10,
            "report_db_detail_mode_set": set(),
            "report_db_detail_item_enable": False,
            "report_db_save_full_json": False,
            "report_db_fail_abort": True,
            "source_schemas_list": ["SRC_A"],
            "enabled_primary_types": {"TABLE"},
            "enabled_extra_types": set(),
            "report_retention_days": 0,
        }
        status_calls = []

        def _fake_set_status(*args, **kwargs):
            status_calls.append(kwargs.get("status", args[3] if len(args) > 3 else None))
            return True

        with mock.patch.object(sdr, "ensure_report_db_tables_exist", return_value=(True, None)), \
             mock.patch.object(sdr, "obclient_run_sql_commit", return_value=(True, "", "")), \
             mock.patch.object(sdr, "_insert_report_counts_rows", return_value=True), \
             mock.patch.object(sdr, "_build_report_artifact_rows", return_value=[]), \
             mock.patch.object(sdr, "ensure_report_db_views_exist", return_value=[]), \
             mock.patch.object(sdr, "build_report_sql_template_file", return_value=None), \
             mock.patch.object(
                 sdr,
                 "_verify_report_db_row_consistency",
                 return_value=(False, {"summary": 1}, "DIFF_REPORT_DETAIL expected=10, actual=9")
             ), \
             mock.patch.object(sdr, "_set_report_db_summary_write_status", side_effect=_fake_set_status):
            ok, err = sdr.save_report_to_db(
                {"executable": "/usr/bin/obclient"},
                settings,
                run_summary,
                run_ctx,
                "20260302_120000",
                None,
                {"missing": [], "mismatched": [], "ok": [], "skipped": []},
                {},
                None,
                {},
                None,
                None,
                set(),
                grant_plan=None,
                usability_summary=None,
                package_results=None,
                trigger_status_rows=None,
                constraint_status_rows=None,
                dependency_report=None,
                expected_dependency_pairs=None,
                view_chain_file=None,
                remap_conflicts=None,
                full_object_mapping=None,
                remap_rules=None,
                blacklist_report_rows=None,
                fixup_skip_summary=None,
                blacklisted_table_keys=None,
                excluded_object_rows=None,
            )
        self.assertFalse(ok)
        self.assertIn("写库行数复核失败", err)
        self.assertIn(sdr.REPORT_DB_WRITE_STATUS_FAILED, status_calls)


class TestPlsqlQualifiedReferenceRemap(unittest.TestCase):
    def test_remap_plsql_object_references_does_not_rewrite_local_variable(self):
        ddl = (
            "CREATE OR REPLACE PROCEDURE SRC.P AS\n"
            "  STATUS NUMBER;\n"
            "  V_CNT NUMBER;\n"
            "BEGIN\n"
            "  STATUS := 1;\n"
            "  SELECT COUNT(*) INTO V_CNT FROM STATUS;\n"
            "END;\n"
        )
        mapping = {
            "SRC.STATUS": {"TABLE": "TGT.STATUS"}
        }
        rewritten = sdr.remap_plsql_object_references(
            ddl,
            "PROCEDURE",
            mapping,
            source_schema="SRC"
        )
        self.assertIn("STATUS := 1;", rewritten.upper())
        self.assertIn("FROM TGT.STATUS", rewritten.upper())
        self.assertNotIn("TGT.STATUS :=", rewritten.upper())

    def test_remap_plsql_object_references_remaps_quoted_qualified_special_name(self):
        ddl = (
            'CREATE OR REPLACE PROCEDURE SRC.P AS\n'
            '  V_CNT NUMBER;\n'
            'BEGIN\n'
            '  SELECT COUNT(*) INTO V_CNT FROM "SRC"."OBJ$SRC";\n'
            'END;\n'
        )
        mapping = {
            "SRC.OBJ$SRC": {"TABLE": "TGT.OBJ$TGT"},
        }
        rewritten = sdr.remap_plsql_object_references(
            ddl,
            "PROCEDURE",
            mapping,
            source_schema="SRC",
        )
        self.assertIn('FROM "TGT"."OBJ$TGT"', rewritten)
        self.assertNotIn('"SRC"."OBJ$SRC"', rewritten)

    def test_remap_plsql_object_references_remaps_unquoted_dollar_qualified_name(self):
        ddl = (
            "CREATE OR REPLACE PROCEDURE SRC.P AS\n"
            "  V_CNT NUMBER;\n"
            "BEGIN\n"
            "  SELECT COUNT(*) INTO V_CNT FROM SRC.OBJ$SRC;\n"
            "END;\n"
        )
        mapping = {
            "SRC.OBJ$SRC": {"TABLE": "TGT.OBJ$TGT"},
        }
        rewritten = sdr.remap_plsql_object_references(
            ddl,
            "PROCEDURE",
            mapping,
            source_schema="SRC",
        )
        self.assertIn("FROM TGT.OBJ$TGT", rewritten.upper())
        self.assertNotIn("FROM SRC.OBJ$SRC", rewritten.upper())

    def test_remap_plsql_object_references_remaps_unquoted_hash_qualified_name(self):
        ddl = (
            "CREATE OR REPLACE PROCEDURE SRC.P AS\n"
            "  V_CNT NUMBER;\n"
            "BEGIN\n"
            "  SELECT COUNT(*) INTO V_CNT FROM SRC.OBJ#SRC;\n"
            "END;\n"
        )
        mapping = {
            "SRC.OBJ#SRC": {"TABLE": "TGT.OBJ#TGT"},
        }
        rewritten = sdr.remap_plsql_object_references(
            ddl,
            "PROCEDURE",
            mapping,
            source_schema="SRC",
        )
        self.assertIn("FROM TGT.OBJ#TGT", rewritten.upper())
        self.assertNotIn("FROM SRC.OBJ#SRC", rewritten.upper())

    def test_remap_plsql_object_references_keeps_special_name_in_comments_and_literals(self):
        ddl = (
            'CREATE OR REPLACE PROCEDURE SRC.P AS\n'
            '  V_CNT NUMBER;\n'
            '  V_SQL VARCHAR2(200);\n'
            'BEGIN\n'
            '  V_SQL := \'SELECT * FROM "SRC"."OBJ$SRC"\';\n'
            '  -- keep "SRC"."OBJ$SRC" here\n'
            '  SELECT COUNT(*) INTO V_CNT FROM "SRC"."OBJ$SRC";\n'
            'END;\n'
        )
        mapping = {
            "SRC.OBJ$SRC": {"TABLE": "TGT.OBJ$TGT"},
        }
        rewritten = sdr.remap_plsql_object_references(
            ddl,
            "PROCEDURE",
            mapping,
            source_schema="SRC",
        )
        self.assertIn('V_SQL := \'SELECT * FROM "SRC"."OBJ$SRC"\';', rewritten)
        self.assertIn('-- keep "SRC"."OBJ$SRC" here', rewritten)
        self.assertIn('FROM "TGT"."OBJ$TGT"', rewritten)

if __name__ == "__main__":
    unittest.main()
