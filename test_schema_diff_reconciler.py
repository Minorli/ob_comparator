import unittest
import logging
from unittest import mock
import sys
import types
import tempfile
import json
import inspect
import subprocess
from pathlib import Path
from typing import Dict, Set, List, Tuple

# schema_diff_reconciler 在 import 时强依赖 oracledb；
# 单元测试只覆盖纯函数，因此用 dummy 模块兜底，避免环境未安装时退出。
try:  # pragma: no cover
    import oracledb  # noqa: F401
except ImportError:  # pragma: no cover
    sys.modules["oracledb"] = types.ModuleType("oracledb")

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
        default_on_null_supported: bool = True
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
            constraint_deferrable_supported=constraint_deferrable_supported
        )

    def _make_oracle_meta_with_columns(
        self,
        table_columns: Dict,
        *,
        invisible_supported: bool = False,
        identity_supported: bool = True,
        default_on_null_supported: bool = True
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

    def _make_ob_meta_with_columns(
        self,
        objects_by_type: Dict,
        tab_columns: Dict,
        *,
        invisible_supported: bool = False,
        identity_supported: bool = True,
        default_on_null_supported: bool = True,
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
            constraint_deferrable_supported=constraint_deferrable_supported
        )

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
        self.assertIsNotNone(sql)
        self.assertIn('-- ALTER TABLE "TGT"."T1" DROP COLUMN SYS_C000123;', sql)
        self.assertNotIn('\nALTER TABLE "TGT"."T1" FORCE;', "\n" + sql)

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
            self.assertIn("GRANT SELECT ON APP.T1", content)
            self.assertNotIn("ALTER SESSION SET CURRENT_SCHEMA", content)

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

    def test_normalize_synonym_fixup_scope(self):
        self.assertEqual(sdr.normalize_synonym_fixup_scope(None), "public_only")
        self.assertEqual(sdr.normalize_synonym_fixup_scope("all"), "all")
        self.assertEqual(sdr.normalize_synonym_fixup_scope("public"), "public_only")
        self.assertEqual(sdr.normalize_synonym_fixup_scope("PUBLIC_ONLY"), "public_only")

    def test_normalize_sequence_remap_policy(self):
        self.assertEqual(sdr.normalize_sequence_remap_policy(None), "source_only")
        self.assertEqual(sdr.normalize_sequence_remap_policy("infer"), "infer")
        self.assertEqual(sdr.normalize_sequence_remap_policy("SOURCE"), "source_only")
        self.assertEqual(sdr.normalize_sequence_remap_policy("dominant"), "dominant_table")

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

    def test_pick_dependency_issue_missing_target(self):
        deps = [("SRC.T1", "TABLE")]
        full_mapping = {"SRC.T1": {"TABLE": "TGT.T1"}}
        missing_targets = {("TABLE", "TGT.T1")}
        issue = sdr._pick_dependency_issue(deps, full_mapping, {}, missing_targets)
        self.assertIsNotNone(issue)
        self.assertIn("依赖对象缺失", issue[0])

    def test_dump_ob_metadata_infers_char_used_from_lengths(self):
        def fake_run(_cfg, sql):
            if "NLS_LENGTH_SEMANTICS" in sql.upper():
                return True, "BYTE", ""
            return True, "", ""

        def fake_query(_cfg, sql_tpl, _owners, **_kwargs):
            sql = sql_tpl.upper()
            if "DBA_OBJECTS" in sql:
                return True, [], ""
            if "DBA_TAB_COLUMNS" in sql:
                line = "A\tT1\tC1\tVARCHAR2\t10\t20\t\t\t\tY\t"
                return True, [line], ""
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
            if "NLS_LENGTH_SEMANTICS" in sql.upper():
                return True, "BYTE", ""
            return True, "", ""

        def fake_query(_cfg, sql_tpl, _owners, **_kwargs):
            sql = sql_tpl.upper()
            if "DBA_OBJECTS" in sql:
                return True, [], ""
            if "DBA_TAB_COLUMNS" in sql:
                if "CHAR_USED" in sql:
                    return False, [], "no column"
                if "DATA_LENGTH" in sql and "DATA_PRECISION" in sql:
                    return False, [], "no column"
                line = "A\tT1\tC1\tVARCHAR2\t10\tY\t"
                return True, [line], ""
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

    def test_ensure_trigger_mappings_for_extra_checks_follow_table_target_schema(self):
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
            "TGT.TRG_BI"
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
            blacklist_tables={}, object_privileges=[], sys_privileges=[],
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

    def test_extract_view_dependencies_with_subquery(self):
        ddl = (
            "CREATE OR REPLACE VIEW A.V AS\n"
            "SELECT * FROM (SELECT * FROM T1) t\n"
            "JOIN B.T2 b ON t.ID=b.ID\n"
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

    def test_classify_unsupported_check_constraints_self_ref_fk(self):
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
        reason_codes = {row.reason_code for row in rows}
        self.assertEqual(reason_codes, {"FK_SELF_REF"})
        self.assertEqual(extra_results["constraint_mismatched"], [])
        self.assertIn("TGT.T1", extra_results["constraint_ok"])

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
        prereq, post, remaining = sdr.split_view_grants(view_targets, expected_pairs, grants)
        self.assertIn("TGT", prereq)
        self.assertIn(sdr.ObjectGrantEntry("SELECT", "TGT2.T1", True), prereq["TGT"])
        self.assertIn("APP", post)
        self.assertIn(sdr.ObjectGrantEntry("SELECT", "TGT.V1", False), post["APP"])
        self.assertEqual(remaining, {})

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
        rows = sdr._build_report_counts_rows(counts, {"TABLE": 1})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["object_type"], "TABLE")
        self.assertEqual(rows[0]["oracle_count"], 2)
        self.assertEqual(rows[0]["unsupported_count"], 1)

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

    def test_render_report_sql_template(self):
        content = "SELECT * FROM DIFF_REPORT_SUMMARY WHERE report_id = :report_id;"
        rendered = sdr._render_report_sql_template(content, "R1")
        self.assertIn("'R1'", rendered)

    def test_build_report_db_view_ddls_phase_b_views(self):
        ddls = sdr._build_report_db_view_ddls("")
        self.assertIn(sdr.REPORT_DB_VIEWS["pending_actions"], ddls)
        self.assertIn(sdr.REPORT_DB_VIEWS["grant_class"], ddls)
        self.assertIn(sdr.REPORT_DB_VIEWS["usability_class"], ddls)

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

    def test_report_db_ddls_no_foreign_keys(self):
        source = inspect.getsource(sdr.ensure_report_db_tables_exist)
        self.assertNotIn("FOREIGN KEY", source)


if __name__ == "__main__":
    unittest.main()
