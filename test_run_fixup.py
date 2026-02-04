import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

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


class TestRunFixupConfig(unittest.TestCase):
    def test_load_ob_config_percent_password_and_timeout_fallback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "fixup_scripts").mkdir()
            cfg_path = root / "config.ini"
            cfg_path.write_text(
                "\n".join([
                    "[OCEANBASE_TARGET]",
                    "executable = /bin/obclient",
                    "host = 127.0.0.1",
                    "port = 2881",
                    "user_string = root@sys",
                    "password = p%w",
                    "[SETTINGS]",
                    "fixup_dir = fixup_scripts",
                    "obclient_timeout = 77",
                ]) + "\n",
                encoding="utf-8"
            )
            ob_cfg, fixup_path, _repo_root, _log_level, _report_path, fixup_settings, max_sql_bytes = rf.load_ob_config(cfg_path)
            self.assertEqual(ob_cfg["password"], "p%w")
            self.assertEqual(ob_cfg["timeout"], 77)
            self.assertTrue(fixup_settings.enabled)
            self.assertEqual(
                max_sql_bytes,
                rf.DEFAULT_FIXUP_MAX_SQL_FILE_MB * 1024 * 1024
            )
        self.assertEqual(fixup_path, (root / "fixup_scripts").resolve())


class TestDependencyChainParsing(unittest.TestCase):
    def test_parse_dependency_chains_target_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "dependency_chains_20260101_000000.txt"
            path.write_text(
                "\n".join([
                    "# header",
                    "[SOURCE - ORACLE] 依赖链:",
                    "00001. SRC.V1(VIEW) -> SRC.T1(TABLE)",
                    "",
                    "[TARGET - REMAPPED] 依赖链:",
                    "00001. TGT.V1(VIEW) -> TGT.T1(TABLE)",
                ]) + "\n",
                encoding="utf-8"
            )
            deps = rf.parse_dependency_chains_file(path)
            key = ("TGT.V1", "VIEW")
            self.assertIn(key, deps)
            self.assertIn(("TGT.T1", "TABLE"), deps[key])


class TestFixupAutoGrantTypes(unittest.TestCase):
    def test_parse_fixup_auto_grant_types_defaults(self):
        types = rf.parse_fixup_auto_grant_types("")
        self.assertIn("VIEW", types)
        self.assertIn("PACKAGE BODY", types)


class TestCurrentSchemaExecution(unittest.TestCase):
    def test_execute_sql_statements_applies_current_schema(self):
        sql_text = "\n".join([
            "ALTER SESSION SET CURRENT_SCHEMA = OB_SALES;",
            "CREATE TABLE T1(ID INT);",
            "CREATE VIEW V1 AS SELECT * FROM T1;"
        ])
        captured = []

        def fake_run_sql(_cmd, sql, _timeout):
            captured.append(sql.strip())
            return SimpleNamespace(returncode=0, stderr="", stdout="")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            summary = rf.execute_sql_statements([], sql_text, timeout=1)

        self.assertEqual(summary.failures, [])
        self.assertEqual(len(captured), 3)
        self.assertTrue(captured[0].startswith("ALTER SESSION SET CURRENT_SCHEMA = OB_SALES;"))
        self.assertTrue(captured[1].startswith("ALTER SESSION SET CURRENT_SCHEMA = OB_SALES;"))
        self.assertIn("CREATE TABLE T1", captured[1])
        self.assertTrue(captured[2].startswith("ALTER SESSION SET CURRENT_SCHEMA = OB_SALES;"))
        self.assertIn("CREATE VIEW V1", captured[2])


class TestExecuteSqlErrorDetection(unittest.TestCase):
    def test_execute_sql_statements_detects_stdout_error(self):
        def fake_run_sql(_cmd, _sql, _timeout):
            return SimpleNamespace(returncode=0, stderr="", stdout="ORA-00900: invalid SQL statement")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            summary = rf.execute_sql_statements([], "CREATE VIEW V1 AS SELECT 1 FROM dual;", timeout=1)

        self.assertEqual(summary.statements, 1)
        self.assertEqual(len(summary.failures), 1)
        self.assertIn("ORA-00900", summary.failures[0].error)

    def test_run_query_lines_detects_stdout_error(self):
        def fake_run_sql(_cmd, _sql, _timeout):
            return SimpleNamespace(returncode=0, stderr="", stdout="ERROR 1064 (42000): bad syntax")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            ok, lines, err = rf.run_query_lines([], "SELECT 1", timeout=1)

        self.assertFalse(ok)
        self.assertEqual(lines, [])
        self.assertIn("ERROR 1064", err)


class TestViewChainDdlSanitize(unittest.TestCase):
    def test_sanitize_view_chain_view_ddl_removes_force(self):
        ddl = 'CREATE OR REPLACE FORCE EDITIONABLE VIEW "A"."V1" AS SELECT 1 FROM dual;'
        cleaned = rf.sanitize_view_chain_view_ddl(ddl)
        self.assertIn("CREATE OR REPLACE VIEW", cleaned.upper())
        self.assertNotIn("FORCE", cleaned.upper())
        self.assertNotIn("EDITIONABLE", cleaned.upper())

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


class TestRunFixupHelpers(unittest.TestCase):
    def test_safe_first_line(self):
        self.assertEqual(rf.safe_first_line("", 80, "n/a"), "n/a")
        self.assertEqual(rf.safe_first_line(None, 80, "n/a"), "n/a")
        self.assertEqual(rf.safe_first_line("line1\nline2", 80, "n/a"), "line1")
        self.assertEqual(rf.safe_first_line("line1", 3, "n/a"), "lin")


class TestLimitedCache(unittest.TestCase):
    def test_eviction(self):
        cache = rf.LimitedCache(2)
        cache["a"] = 1
        cache["b"] = 2
        cache["c"] = 3
        self.assertEqual(len(cache), 2)
        self.assertNotIn("a", cache)


class TestSqlFileSizeLimit(unittest.TestCase):
    def test_large_sql_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sql_path = root / "big.sql"
            sql_path.write_text("x" * 1024, encoding="utf-8")
            result, summary = rf.execute_script_with_summary(
                [],
                sql_path,
                root,
                root,
                None,
                0,
                "[TEST]",
                max_sql_file_bytes=10
            )
            self.assertEqual(result.status, "ERROR")
            self.assertIn("文件过大", result.message)
            self.assertEqual(summary.statements, 0)


class TestViewChainCycle(unittest.TestCase):
    def test_build_view_chain_plan_blocks_cycle(self):
        chains = [
            [("A.V1", "VIEW"), ("B.V2", "VIEW")],
            [("B.V2", "VIEW"), ("A.V1", "VIEW")],
        ]
        grant_index = rf.GrantIndex({}, {}, {})
        plan, sql_lines, blocked = rf.build_view_chain_plan(
            "A.V1",
            chains,
            [],
            None,
            {},
            {},
            {},
            {},
            grant_index,
            grant_index,
            False,
            Path("."),
            {},
            {},
            {},
            {},
            {},
            set(),
            set(),
            set(),
            set(),
            set(),
            None
        )
        self.assertTrue(blocked)
        self.assertEqual(sql_lines, [])
        self.assertTrue(any("CYCLE" in line for line in plan))


class TestSqlParsing(unittest.TestCase):
    def test_split_nested_block_comments(self):
        sql = "/* outer /* inner */ still */\nSELECT 1 FROM dual;\n"
        statements = rf.split_sql_statements(sql)
        self.assertEqual(len(statements), 1)
        self.assertIn("SELECT 1", statements[0])


class TestErrorClassification(unittest.TestCase):
    def test_extended_error_codes(self):
        self.assertEqual(rf.classify_sql_error("ORA-00054: resource busy"), rf.FailureType.LOCK_TIMEOUT)
        self.assertEqual(rf.classify_sql_error("ORA-01017: invalid username/password"), rf.FailureType.AUTH_FAILED)
        self.assertEqual(rf.classify_sql_error("ORA-12170: TNS:Connect timeout"), rf.FailureType.CONNECTION_TIMEOUT)
        self.assertEqual(rf.classify_sql_error("ORA-04031: unable to allocate"), rf.FailureType.RESOURCE_EXHAUSTED)
        self.assertEqual(rf.classify_sql_error("ORA-01555: snapshot too old"), rf.FailureType.SNAPSHOT_ERROR)
        self.assertEqual(rf.classify_sql_error("ORA-00060: deadlock detected"), rf.FailureType.DEADLOCK)


class TestIterativeFixupSummary(unittest.TestCase):
    def test_cumulative_failed_counts_all_rounds(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            fixup_dir = repo_root / "fixup_scripts"
            (fixup_dir / "table").mkdir(parents=True)
            report_dir = repo_root / "reports"
            report_dir.mkdir()

            f1 = fixup_dir / "table" / "A.T1.sql"
            f2 = fixup_dir / "table" / "A.T2.sql"
            f3 = fixup_dir / "table" / "A.T3.sql"
            for fp in (f1, f2, f3):
                fp.write_text("SELECT 1;", encoding="utf-8")

            args = SimpleNamespace(
                config=str(repo_root / "config.ini"),
                smart_order=False,
                glob_patterns=None,
                recompile=False,
            )
            ob_cfg = {
                "executable": "obclient",
                "host": "127.0.0.1",
                "port": "2881",
                "user_string": "root@sys",
                "password": "p",
            }
            fixup_settings = rf.FixupAutoGrantSettings(
                enabled=False,
                types=set(),
                fallback=False,
                cache_limit=0
            )

            rounds = [
                [(0, f1), (0, f2)],
                [(0, f3)],
            ]

            def fake_collect(*_a, **_k):
                return rounds.pop(0)

            def fake_exec(_cmd, path, *_a, **_k):
                if path == f1:
                    return rf.ScriptResult(path, "SUCCESS"), rf.ExecutionSummary(1, [])
                failure = rf.StatementFailure(1, "ERR", "SELECT 1")
                return rf.ScriptResult(path, "FAILED", "ERR"), rf.ExecutionSummary(1, [failure])

            with mock.patch.object(rf, "collect_sql_files_by_layer", side_effect=fake_collect), \
                 mock.patch.object(rf, "execute_script_with_summary", side_effect=fake_exec), \
                 mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]), \
                 mock.patch.object(rf, "resolve_timeout_value", return_value=None), \
                 mock.patch.object(rf, "build_fixup_object_index", return_value=({}, {})):
                with self.assertLogs(rf.__name__, level="INFO") as cm:
                    with self.assertRaises(SystemExit):
                        rf.run_iterative_fixup(
                            args,
                            ob_cfg,
                            fixup_dir,
                            repo_root,
                            report_dir,
                            [],
                            [],
                            fixup_settings,
                            None,
                            max_rounds=2,
                            min_progress=1
                        )

            joined = "\n".join(cm.output)
            self.assertIn("总计失败: 2", joined)


class TestReportLookup(unittest.TestCase):
    def test_find_latest_report_prefers_latest_run_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_dir = root / "main_reports"
            report_dir.mkdir()
            run_old = report_dir / "run_20260101_000000"
            run_new = report_dir / "run_20260102_000000"
            run_old.mkdir()
            run_new.mkdir()
            old_file = run_old / "VIEWs_chain_20260103_000000.txt"
            new_file = run_new / "VIEWs_chain_20260101_000000.txt"
            old_file.write_text("old", encoding="utf-8")
            new_file.write_text("new", encoding="utf-8")

            chosen = rf.find_latest_report_file(report_dir, "VIEWs_chain")
            self.assertEqual(chosen, new_file)


class TestRecompileSkipTypes(unittest.TestCase):
    def test_recompile_skips_unsupported_types(self):
        invalid_batches = [
            [
                ("APP", "V1", "VIEW"),
                ("APP", "TB1", "TYPE BODY"),
                ("APP", "P1", "PROCEDURE"),
            ],
            [],
        ]

        def fake_query(_cmd, _timeout):
            return invalid_batches.pop(0)

        executed = []

        def fake_run_sql(_cmd, sql, _timeout):
            executed.append(sql)
            return SimpleNamespace(returncode=0, stderr="", stdout="")

        with mock.patch.object(rf, "query_invalid_objects", side_effect=fake_query), \
             mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            recompiled, remaining = rf.recompile_invalid_objects([], timeout=1, max_retries=2)

        self.assertEqual(recompiled, 1)
        self.assertEqual(remaining, 0)
        self.assertEqual(len(executed), 1)
        self.assertIn("ALTER PROCEDURE APP.P1 COMPILE;", executed[0])


if __name__ == "__main__":
    unittest.main()
