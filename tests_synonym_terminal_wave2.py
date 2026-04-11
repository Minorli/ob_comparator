import tempfile
import unittest
from pathlib import Path
from unittest import mock

import schema_diff_reconciler as sdr


def make_oracle_meta() -> sdr.OracleMetadata:
    return sdr.OracleMetadata(
        table_columns={},
        invisible_column_supported=True,
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
        package_errors_complete=True,
        partition_key_columns={},
        interval_partitions={},
        loaded_schemas=frozenset(),
        privilege_family_counts=(),
        non_table_triggers=(),
        temporary_tables=frozenset(),
        identity_modes={},
        default_on_null_columns={},
        identity_options={},
        nested_table_storage_tables={},
    )


def make_ob_meta() -> sdr.ObMetadata:
    return sdr.ObMetadata(
        objects_by_type={},
        tab_columns={},
        invisible_column_supported=True,
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
        case_sensitive_findings=(),
        constraint_deferrable_supported=False,
        temporary_tables=frozenset(),
        identity_modes={},
        default_on_null_columns={},
        identity_options={},
        enabled_notnull_check_columns={},
        enabled_notnull_check_groups={},
    )


class TestSynonymTerminalWave2(unittest.TestCase):
    def test_scope_status_stays_on_real_table_under_public_package_collision(self):
        synonym_meta = {
            ("SCHEMA1", "SYN_T"): sdr.SynonymMeta("SCHEMA1", "SYN_T", "SCHEMA3", "TABLE_T", None),
            ("PUBLIC", "TABLE_T"): sdr.SynonymMeta("PUBLIC", "TABLE_T", "SCHEMA1", "PACKAGE_P", None),
        }
        terminal, state, detail = sdr.resolve_synonym_scope_status(
            "SCHEMA1",
            "SYN_T",
            synonym_meta,
            {
                "SCHEMA1.SYN_T": {"SYNONYM"},
                "PUBLIC.TABLE_T": {"SYNONYM"},
                "SCHEMA1.PACKAGE_P": {"PACKAGE"},
                "SCHEMA3.TABLE_T": {"TABLE"},
            },
            remap_rules={},
        )
        self.assertEqual(terminal, "SCHEMA3.TABLE_T")
        self.assertEqual(state, "in_scope")
        self.assertEqual(detail, "TABLE")

    def test_fixup_target_stays_on_real_table_under_public_package_collision(self):
        synonym_meta = {
            ("SCHEMA1", "SYN_T"): sdr.SynonymMeta("SCHEMA1", "SYN_T", "SCHEMA3", "TABLE_T", None),
            ("PUBLIC", "TABLE_T"): sdr.SynonymMeta("PUBLIC", "TABLE_T", "SCHEMA1", "PACKAGE_P", None),
        }
        full_object_mapping = {
            "SCHEMA1.SYN_T": {"SYNONYM": "TGT.SYN_T"},
            "PUBLIC.TABLE_T": {"SYNONYM": "PUBLIC.TABLE_T"},
            "SCHEMA1.PACKAGE_P": {"PACKAGE": "TGT.PACKAGE_P"},
            "SCHEMA3.TABLE_T": {"TABLE": "TGT.TABLE_T"},
        }
        target = sdr.resolve_synonym_fixup_target(
            "SCHEMA1",
            "SYN_T",
            synonym_meta,
            full_object_mapping,
            remap_rules={},
        )
        self.assertEqual(target, "TGT.TABLE_T")

    def test_generate_fixup_synonym_uses_real_table_terminal_under_public_package_collision(self):
        tv_results = {
            "missing": [("SYNONYM", "TGT.SYN_T", "SCHEMA1.SYN_T")],
            "mismatched": [],
            "ok": [],
            "skipped": [],
            "extraneous": [],
            "extra_targets": [],
            "remap_conflicts": [],
        }
        extra_results = {
            "index_ok": [], "index_mismatched": [],
            "constraint_ok": [], "constraint_mismatched": [],
            "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
        master_list = [("SCHEMA1.SYN_T", "TGT.SYN_T", "SYNONYM")]
        full_mapping = {
            "SCHEMA1.SYN_T": {"SYNONYM": "TGT.SYN_T"},
            "PUBLIC.TABLE_T": {"SYNONYM": "PUBLIC.TABLE_T"},
            "SCHEMA1.PACKAGE_P": {"PACKAGE": "TGT.PACKAGE_P"},
            "SCHEMA3.TABLE_T": {"TABLE": "TGT.TABLE_T"},
        }
        settings = {
            "fixup_dir": "",
            "fixup_workers": 1,
            "progress_log_interval": 999,
            "fixup_type_set": {"SYNONYM"},
            "fixup_schema_list": set(),
            "source_schemas_list": ["SCHEMA1", "SCHEMA3"],
            "synonym_fixup_scope": "all",
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
                    make_oracle_meta(),
                    full_mapping,
                    {},
                    grant_plan=None,
                    enable_grant_generation=False,
                    dependency_report={"missing": [], "unexpected": [], "skipped": []},
                    ob_meta=make_ob_meta()._replace(objects_by_type={"SYNONYM": set()}),
                    expected_dependency_pairs=set(),
                    synonym_metadata={
                        ("SCHEMA1", "SYN_T"): sdr.SynonymMeta("SCHEMA1", "SYN_T", "SCHEMA3", "TABLE_T", None),
                        ("PUBLIC", "TABLE_T"): sdr.SynonymMeta("PUBLIC", "TABLE_T", "SCHEMA1", "PACKAGE_P", None),
                    },
                    trigger_filter_entries=None,
                    trigger_filter_enabled=False,
                    package_results=None,
                    report_dir=None,
                    report_timestamp=None,
                    support_state_map={},
                    unsupported_table_keys=set(),
                    view_compat_map={},
                )
            synonym_path = Path(tmp_dir) / "synonym" / "TGT.SYN_T.sql"
            self.assertTrue(synonym_path.exists())
            content = synonym_path.read_text(encoding="utf-8").upper()
            self.assertIn('FOR "TGT"."TABLE_T";', content)
            self.assertNotIn('FOR "TGT"."PACKAGE_P";', content)


if __name__ == "__main__":
    unittest.main()
