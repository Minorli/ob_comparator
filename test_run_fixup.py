import unittest
import tempfile
from pathlib import Path

import run_fixup as rf


class TestViewChainParsing(unittest.TestCase):
    def test_parse_view_chain_lines(self):
        lines = [
            "# VIEW fixup dependency chains",
            "00001. A.V1[VIEW|EXISTS|GRANT_OK] -> B.V2[VIEW|MISSING|GRANT_MISSING] -> C.T1[TABLE|EXISTS|GRANT_OK]",
            "[CYCLES]",
            "- A.V1[VIEW|EXISTS|GRANT_OK] -> A.V1[VIEW|EXISTS|GRANT_OK] (CYCLE)",
        ]
        chains = rf.parse_view_chain_lines(lines)
        self.assertIn("A.V1", chains)
        self.assertEqual(
            chains["A.V1"][0],
            [("A.V1", "VIEW"), ("B.V2", "VIEW"), ("C.T1", "TABLE")]
        )

    def test_topo_sort_nodes_dependency_order(self):
        chains = [
            [("A.V1", "VIEW"), ("B.V2", "VIEW"), ("C.T1", "TABLE")],
            [("A.V1", "VIEW"), ("D.T2", "TABLE")],
        ]
        nodes, edges = rf.build_view_dependency_graph(chains)
        order, cycles = rf.topo_sort_nodes(nodes, edges)
        self.assertEqual(cycles, [])
        self.assertLess(order.index(("C.T1", "TABLE")), order.index(("B.V2", "VIEW")))
        self.assertLess(order.index(("B.V2", "VIEW")), order.index(("A.V1", "VIEW")))
        self.assertLess(order.index(("D.T2", "TABLE")), order.index(("A.V1", "VIEW")))


class TestGrantLookupPriority(unittest.TestCase):
    def test_grant_lookup_prefers_miss(self):
        entry_miss = rf.GrantEntry(
            grantee="USER1",
            privileges=("SELECT",),
            object_name="SCHEMA1.T1",
            statement="GRANT SELECT ON SCHEMA1.T1 TO USER1;",
            source_path=Path("fixup_scripts/grants_miss/a.sql"),
            grant_type="OBJECT"
        )
        entry_all = rf.GrantEntry(
            grantee="USER1",
            privileges=("SELECT",),
            object_name="SCHEMA1.T1",
            statement="GRANT SELECT ON SCHEMA1.T1 TO USER1;",
            source_path=Path("fixup_scripts/grants_all/a.sql"),
            grant_type="OBJECT"
        )
        idx_miss = rf.GrantIndex(
            {("USER1", "SCHEMA1.T1"): [entry_miss]},
            {"SCHEMA1.T1": [entry_miss]},
            {}
        )
        idx_all = rf.GrantIndex(
            {("USER1", "SCHEMA1.T1"): [entry_all]},
            {"SCHEMA1.T1": [entry_all]},
            {}
        )
        entries, label = rf.find_grant_entries_by_priority(
            "USER1",
            "SCHEMA1.T1",
            "SELECT",
            idx_miss,
            idx_all
        )
        self.assertEqual(label, "grants_miss")
        self.assertEqual(entries, [entry_miss])

    def test_grant_lookup_falls_back_to_all(self):
        entry_all = rf.GrantEntry(
            grantee="USER1",
            privileges=("SELECT",),
            object_name="SCHEMA1.T1",
            statement="GRANT SELECT ON SCHEMA1.T1 TO USER1;",
            source_path=Path("fixup_scripts/grants_all/a.sql"),
            grant_type="OBJECT"
        )
        idx_miss = rf.GrantIndex({}, {}, {})
        idx_all = rf.GrantIndex(
            {("USER1", "SCHEMA1.T1"): [entry_all]},
            {"SCHEMA1.T1": [entry_all]},
            {}
        )
        entries, label = rf.find_grant_entries_by_priority(
            "USER1",
            "SCHEMA1.T1",
            "SELECT",
            idx_miss,
            idx_all
        )
        self.assertEqual(label, "grants_all")
        self.assertEqual(entries, [entry_all])


class TestViewChainHelpers(unittest.TestCase):
    def test_select_fixup_script_fallback_to_done(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_view = root / "view"
            done_view = root / "done" / "view"
            done_view.mkdir(parents=True)
            ddl_path = done_view / "A.V1.sql"
            ddl_path.write_text("CREATE VIEW A.V1 AS SELECT 1 FROM DUAL;", encoding="utf-8")

            primary_index, primary_name = rf.build_fixup_object_index([])
            done_index, done_name = rf.build_fixup_object_index([(0, ddl_path)])

            node = ("A.V1", "VIEW")
            selected, source = rf.select_fixup_script_for_node_with_fallback(
                node,
                primary_index,
                primary_name,
                done_index,
                done_name
            )
            self.assertEqual(selected, ddl_path)
            self.assertEqual(source, "done")

    def test_build_auto_grant_statement(self):
        stmt = rf.build_auto_grant_statement("app_user", "SRC.T1", "select")
        self.assertEqual(stmt, "GRANT SELECT ON SRC.T1 TO APP_USER;")
        stmt = rf.build_auto_grant_statement("app_user", "SRC.T1", "select", with_grant_option=True)
        self.assertEqual(stmt, "GRANT SELECT ON SRC.T1 TO APP_USER WITH GRANT OPTION;")

    def test_classify_view_chain_status_partial(self):
        status = rf.classify_view_chain_status(
            blocked=False,
            skipped=False,
            view_exists=True,
            failure_count=2
        )
        self.assertEqual(status, "PARTIAL")


if __name__ == "__main__":
    unittest.main()
