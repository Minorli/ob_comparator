import sys
import types
import unittest

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


def _make_ob_meta(*, constraints=None, indexes=None, partition_key_columns=None):
    return sdr.ObMetadata(
        objects_by_type={},
        tab_columns={},
        invisible_column_supported=False,
        identity_column_supported=True,
        default_on_null_supported=True,
        indexes=indexes or {},
        constraints=constraints or {},
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
        partition_key_columns=partition_key_columns or {},
    )


class ObSourceConstraintPkRegressionTests(unittest.TestCase):
    def _constraint_mismatches_for_ob_source(self, source_ob_meta, target_ob_meta):
        source_meta = sdr.adapt_ob_metadata_to_source_oracle_metadata(source_ob_meta, {"SRC"})
        results = sdr.check_extra_objects(
            {"source_db_mode": "oceanbase", "extra_check_workers": 1},
            [("SRC.T1", "TGT.T1", "TABLE")],
            target_ob_meta,
            source_meta,
            {},
            enabled_extra_types={"CONSTRAINT"},
        )
        return list(results.get("constraint_mismatched", []) or [])

    def test_ob_source_partition_pk_downgrade_matches_target_unique_constraint(self):
        source_ob_meta = _make_ob_meta(
            constraints={
                ("SRC", "T1"): {
                    "PK_SRC": {
                        "type": "P",
                        "columns": ["ID"],
                    }
                }
            },
            partition_key_columns={("SRC", "T1"): ["TENANT_ID"]},
        )
        source_meta = sdr.adapt_ob_metadata_to_source_oracle_metadata(source_ob_meta, {"SRC"})
        target_ob_meta = _make_ob_meta(
            constraints={
                ("TGT", "T1"): {
                    "UK_TGT": {
                        "type": "U",
                        "columns": ["ID"],
                    }
                }
            },
        )

        ok, mismatch = sdr.compare_constraints_for_table(
            source_meta,
            target_ob_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            {},
        )

        self.assertTrue(ok)
        self.assertIsNone(mismatch)

    def test_ob_source_partition_pk_downgrade_reports_missing_when_no_target_key(self):
        source_ob_meta = _make_ob_meta(
            constraints={
                ("SRC", "T1"): {
                    "PK_SRC": {
                        "type": "P",
                        "columns": ["ID"],
                    }
                }
            },
            partition_key_columns={("SRC", "T1"): ["TENANT_ID"]},
        )
        source_meta = sdr.adapt_ob_metadata_to_source_oracle_metadata(source_ob_meta, {"SRC"})
        target_ob_meta = _make_ob_meta()

        ok, mismatch = sdr.compare_constraints_for_table(
            source_meta,
            target_ob_meta,
            "SRC",
            "T1",
            "TGT",
            "T1",
            {},
        )

        self.assertFalse(ok)
        self.assertIsNotNone(mismatch)
        self.assertEqual(mismatch.missing_constraints, {"PK_SRC"})
        self.assertEqual(mismatch.downgraded_pk_constraints, {"PK_SRC"})
        self.assertEqual(mismatch.extra_constraints, set())
        self.assertIn("PRIMARY KEY(降级为UNIQUE)", "\n".join(mismatch.detail_mismatch))

    def test_ob_source_strict_constraint_detects_target_pk_when_source_has_uk(self):
        source_ob_meta = _make_ob_meta(
            constraints={
                ("SRC", "T1"): {
                    "UK_SRC": {
                        "type": "U",
                        "columns": ["C1"],
                    }
                }
            }
        )
        target_ob_meta = _make_ob_meta(
            constraints={
                ("TGT", "T1"): {
                    "PK_TGT": {
                        "type": "P",
                        "columns": ["C1"],
                    }
                }
            }
        )

        mismatches = self._constraint_mismatches_for_ob_source(source_ob_meta, target_ob_meta)

        self.assertEqual(len(mismatches), 1)
        self.assertEqual(mismatches[0].missing_constraints, {"UK_SRC"})
        self.assertEqual(mismatches[0].extra_constraints, {"PK_TGT"})

    def test_ob_source_strict_constraint_detects_target_fk_when_source_has_pk(self):
        source_ob_meta = _make_ob_meta(
            constraints={
                ("SRC", "T1"): {
                    "PK_SRC": {
                        "type": "P",
                        "columns": ["C1"],
                    }
                }
            }
        )
        target_ob_meta = _make_ob_meta(
            constraints={
                ("TGT", "T1"): {
                    "FK_TGT": {
                        "type": "R",
                        "columns": ["C1"],
                        "ref_table_owner": "TGT",
                        "ref_table_name": "PARENT",
                    }
                }
            }
        )

        mismatches = self._constraint_mismatches_for_ob_source(source_ob_meta, target_ob_meta)

        self.assertEqual(len(mismatches), 1)
        self.assertEqual(mismatches[0].missing_constraints, {"PK_SRC"})
        self.assertEqual(mismatches[0].extra_constraints, {"FK_TGT"})

    def test_ob_source_fk_incomplete_metadata_is_not_hidden_by_cache_signature(self):
        source_ob_meta = _make_ob_meta(
            constraints={
                ("SRC", "T1"): {
                    "FK_SRC": {
                        "type": "R",
                        "columns": ["C1"],
                        "ref_metadata_complete": False,
                    }
                }
            }
        )
        target_ob_meta = _make_ob_meta(
            constraints={
                ("TGT", "T1"): {
                    "FK_TGT": {
                        "type": "R",
                        "columns": ["C1"],
                        "ref_metadata_complete": False,
                    }
                }
            }
        )

        mismatches = self._constraint_mismatches_for_ob_source(source_ob_meta, target_ob_meta)

        self.assertEqual(len(mismatches), 1)
        self.assertEqual(mismatches[0].missing_constraints, {"FK_SRC"})
        self.assertEqual(mismatches[0].extra_constraints, {"FK_TGT"})
        self.assertEqual(mismatches[0].fk_source_metadata_incomplete, frozenset({"FK_SRC"}))


if __name__ == "__main__":
    unittest.main()
