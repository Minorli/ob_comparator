import sys
import types
import unittest
from unittest import mock

# Provide a stub oracledb module for test import safety.
if "oracledb" not in sys.modules:
    stub = types.SimpleNamespace(
        Error=Exception,
        init_oracle_client=lambda **_kwargs: None
    )
    sys.modules["oracledb"] = stub

import collect_source_object_stats as cs


class TestCollectSourceObjectStats(unittest.TestCase):
    def test_fetch_table_stats_counts_uses_shared_templates(self):
        captured = []

        def fake_fetch(_conn, _owners, sql_tpl):
            captured.append(sql_tpl)
            return {}

        with mock.patch.object(cs, "fetch_table_group_counts", side_effect=fake_fetch):
            cs.fetch_table_stats_counts(conn=mock.Mock(), owners=["A"])

        self.assertEqual(
            captured,
            [
                cs.INDEX_TABLE_STATS_SQL,
                cs.CONSTRAINT_TABLE_STATS_SQL,
                cs.TRIGGER_TABLE_STATS_SQL,
            ]
        )

    def test_print_brief_report_uses_fetch_table_stats_counts(self):
        owners = ["A"]
        counts = {"A": {}}
        with mock.patch.object(cs, "fetch_table_stats_counts", return_value=({}, {}, {})) as mocked:
            cs.print_brief_report(
                config_path=cs.Path("config.ini"),
                owners=owners,
                counts_by_owner=counts,
                max_width=80,
                top_n=1,
                table_stats=True,
                conn=mock.Mock(),
                public_synonym_count=0,
                public_synonym_note="forced",
                public_synonym_error="",
            )
        mocked.assert_called_once()
