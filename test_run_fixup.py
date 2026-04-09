import unittest
import tempfile
import logging
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import List
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

    def test_parse_chain_node_meta_keeps_status_fields(self):
        node = rf.parse_chain_node_meta("A.V1[VIEW|MISSING|GRANT_MISSING]")
        self.assertEqual(node, ("A.V1", "VIEW", ("MISSING", "GRANT_MISSING")))

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

    def test_topo_sort_nodes_handles_deep_chain_without_recursion_error(self):
        nodes = set()
        edges = {}
        prev = None
        for idx in range(1205):
            node = (f"A.V{idx}", "VIEW")
            nodes.add(node)
            if prev is not None:
                edges.setdefault(prev, set()).add(node)
            prev = node

        order, cycles = rf.topo_sort_nodes(nodes, edges)

        self.assertEqual(cycles, [])
        self.assertEqual(len(order), 1205)
        self.assertLess(order.index(("A.V1204", "VIEW")), order.index(("A.V0", "VIEW")))


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

    def test_load_ob_config_fixup_exec_mode_and_fallback(self):
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
                    "fixup_exec_mode = file",
                    "fixup_exec_file_fallback = false",
                ]) + "\n",
                encoding="utf-8"
            )
            _ob_cfg, _fixup_path, _repo_root, _log_level, _report_path, fixup_settings, _max_sql_bytes = rf.load_ob_config(cfg_path)
            self.assertEqual(fixup_settings.exec_mode, "file")
            self.assertFalse(fixup_settings.exec_file_fallback)

    def test_load_ob_config_invalid_fixup_exec_mode_falls_back_to_auto(self):
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
                    "fixup_exec_mode = invalid_mode",
                ]) + "\n",
                encoding="utf-8"
            )
            _ob_cfg, _fixup_path, _repo_root, _log_level, _report_path, fixup_settings, _max_sql_bytes = rf.load_ob_config(cfg_path)
            self.assertEqual(fixup_settings.exec_mode, "auto")

    def test_build_obclient_command_hides_password_from_args(self):
        ob_cfg = {
            "executable": "/usr/bin/obclient",
            "host": "127.0.0.1",
            "port": "2881",
            "user_string": "root@sys",
            "password": "PAssw0rd01##",
        }
        try:
            cmd = rf.build_obclient_command(ob_cfg)
            cmd_text = " ".join(cmd)
            self.assertNotIn("PAssw0rd01##", cmd_text)
            defaults_opt = next((item for item in cmd if item.startswith(f"{rf.OBCLIENT_SECURE_OPT}=")), "")
            self.assertTrue(defaults_opt)
            defaults_path = Path(defaults_opt.split("=", 1)[1])
            self.assertTrue(defaults_path.exists())
        finally:
            rf._cleanup_secure_credential_files()


class TestFixupHotReload(unittest.TestCase):
    def _write_config(
        self,
        path: Path,
        *,
        log_level: str = "INFO",
        fixup_cli_timeout: int = 60,
        fixup_dir: str = "fixup_scripts",
        report_dir: str = "main_reports",
        fail_policy: str = "keep_last_good",
    ) -> None:
        path.write_text(
            "\n".join([
                "[OCEANBASE_TARGET]",
                "executable = /bin/obclient",
                "host = 127.0.0.1",
                "port = 2881",
                "user_string = root@sys",
                "password = p%w",
                "[SETTINGS]",
                f"fixup_dir = {fixup_dir}",
                f"report_dir = {report_dir}",
                f"log_level = {log_level}",
                f"fixup_cli_timeout = {fixup_cli_timeout}",
                "config_hot_reload_mode = round",
                "config_hot_reload_interval_sec = 1",
                f"config_hot_reload_fail_policy = {fail_policy}",
            ]) + "\n",
            encoding="utf-8"
        )

    def test_round_hot_reload_applies_reloadable_changes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "fixup_scripts").mkdir()
            (root / "main_reports").mkdir()
            cfg_path = root / "config.ini"
            self._write_config(cfg_path, log_level="INFO", fixup_cli_timeout=60)
            runtime = rf.init_fixup_hot_reload_runtime(cfg_path)

            ob_cfg, _fixup_path, _repo, _log_level, report_path, fixup_settings, max_sql_bytes = rf.load_ob_config(cfg_path)
            self._write_config(cfg_path, log_level="DEBUG", fixup_cli_timeout=15)

            with mock.patch.object(rf, "set_console_log_level") as m_set_level:
                next_ob_cfg, _next_fixup_settings, next_max_sql_bytes, changed = rf.apply_fixup_hot_reload_at_round(
                    runtime,
                    round_num=1,
                    current_ob_cfg=ob_cfg,
                    current_fixup_dir=root / "fixup_scripts",
                    current_report_dir=report_path,
                    current_fixup_settings=fixup_settings,
                    current_max_sql_file_bytes=max_sql_bytes
                )

            self.assertFalse(changed)
            self.assertEqual(next_ob_cfg.get("timeout"), 15)
            self.assertEqual(next_max_sql_bytes, max_sql_bytes)
            self.assertTrue(m_set_level.called)
            self.assertTrue(runtime.events)
            self.assertEqual(runtime.events[-1]["status"], "APPLIED")

    def test_round_hot_reload_invalid_config_keep_last_good(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "fixup_scripts").mkdir()
            (root / "main_reports").mkdir()
            cfg_path = root / "config.ini"
            self._write_config(cfg_path, fail_policy="keep_last_good")
            runtime = rf.init_fixup_hot_reload_runtime(cfg_path)
            ob_cfg, _fixup_path, _repo, _log_level, report_path, fixup_settings, max_sql_bytes = rf.load_ob_config(cfg_path)

            cfg_path.write_text("BROKEN_CONFIG", encoding="utf-8")
            next_ob_cfg, _next_fixup_settings, next_max_sql_bytes, _changed = rf.apply_fixup_hot_reload_at_round(
                runtime,
                round_num=2,
                current_ob_cfg=ob_cfg,
                current_fixup_dir=root / "fixup_scripts",
                current_report_dir=report_path,
                current_fixup_settings=fixup_settings,
                current_max_sql_file_bytes=max_sql_bytes
            )

            self.assertEqual(next_ob_cfg, ob_cfg)
            self.assertEqual(next_max_sql_bytes, max_sql_bytes)
            self.assertTrue(runtime.events)
            self.assertEqual(runtime.events[-1]["status"], "REJECTED")

    def test_round_hot_reload_invalid_config_abort(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "fixup_scripts").mkdir()
            (root / "main_reports").mkdir()
            cfg_path = root / "config.ini"
            self._write_config(cfg_path, fail_policy="abort")
            runtime = rf.init_fixup_hot_reload_runtime(cfg_path)
            ob_cfg, _fixup_path, _repo, _log_level, report_path, fixup_settings, max_sql_bytes = rf.load_ob_config(cfg_path)
            cfg_path.write_text("BROKEN_CONFIG", encoding="utf-8")

            with self.assertRaises(rf.ConfigError):
                rf.apply_fixup_hot_reload_at_round(
                    runtime,
                    round_num=3,
                    current_ob_cfg=ob_cfg,
                    current_fixup_dir=root / "fixup_scripts",
                    current_report_dir=report_path,
                    current_fixup_settings=fixup_settings,
                    current_max_sql_file_bytes=max_sql_bytes
                )

    def test_round_hot_reload_marks_immutable_change_requires_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "fixup_scripts").mkdir()
            (root / "fixup_scripts_v2").mkdir()
            (root / "main_reports").mkdir()
            cfg_path = root / "config.ini"
            self._write_config(cfg_path, fixup_dir="fixup_scripts")
            runtime = rf.init_fixup_hot_reload_runtime(cfg_path)
            ob_cfg, _fixup_path, _repo, _log_level, report_path, fixup_settings, max_sql_bytes = rf.load_ob_config(cfg_path)

            self._write_config(cfg_path, fixup_dir="fixup_scripts_v2")
            next_ob_cfg, _next_fixup_settings, next_max_sql_bytes, _changed = rf.apply_fixup_hot_reload_at_round(
                runtime,
                round_num=4,
                current_ob_cfg=ob_cfg,
                current_fixup_dir=root / "fixup_scripts",
                current_report_dir=report_path,
                current_fixup_settings=fixup_settings,
                current_max_sql_file_bytes=max_sql_bytes
            )

            self.assertEqual(next_ob_cfg, ob_cfg)
            self.assertEqual(next_max_sql_bytes, max_sql_bytes)
            self.assertTrue(runtime.events)
            self.assertEqual(runtime.events[-1]["status"], "REQUIRES_RESTART")
            self.assertIn("SETTINGS.fixup_dir", runtime.events[-1]["changed_keys"])


class TestFixupPrecheck(unittest.TestCase):
    def test_collect_target_schemas_from_scripts(self):
        files = [
            (0, Path("/tmp/fixup/table/ZZ_APP.T1.sql")),
            (1, Path("/tmp/fixup/view/ZZ_FIN.V1.sql")),
            (2, Path("/tmp/fixup/grants_miss/ZZ_APP.grants.sql")),
            (3, Path("/tmp/fixup/constraint/ZZ_APP.PK_T1.sql")),
        ]
        schemas = rf.collect_target_schemas_from_scripts(files)
        self.assertIn("ZZ_APP", schemas)
        self.assertIn("ZZ_FIN", schemas)
        self.assertNotIn("GRANTS_MISS", schemas)

    def test_build_fixup_precheck_summary_detects_missing_schema_and_privilege(self):
        files = [
            (0, Path("/tmp/fixup/table/ZZ_APP.T1.sql")),
            (1, Path("/tmp/fixup/view/ZZ_FIN.V1.sql")),
        ]

        def fake_run_query_lines(_cmd, sql_text, _timeout):
            sql_u = (sql_text or "").upper()
            if "FROM DBA_USERS" in sql_u:
                return True, ["USERNAME", "ZZ_FIN", "OMS_USER"], ""
            if "FROM DBA_SYS_PRIVS WHERE GRANTEE = 'OMS_USER'" in sql_u:
                return True, ["PRIVILEGE", "CREATE ANY VIEW"], ""
            if "FROM DBA_ROLE_PRIVS WHERE GRANTEE = 'OMS_USER'" in sql_u:
                return True, ["GRANTED_ROLE"], ""
            return False, [], "ORA-00942"

        with mock.patch.object(rf, "run_query_lines", side_effect=fake_run_query_lines):
            summary = rf.build_fixup_precheck_summary(
                {"user_string": "OMS_USER@ob4ora#observer147"},
                ["/usr/bin/obclient"],
                60,
                files
            )

        self.assertEqual(summary.current_user, "OMS_USER")
        self.assertIn("ZZ_APP", summary.target_schemas)
        self.assertIn("ZZ_APP", summary.missing_schemas)
        self.assertIn("CREATE ANY TABLE", summary.required_sys_privileges)
        self.assertIn("CREATE ANY TABLE", summary.missing_sys_privileges)
        self.assertNotIn("CREATE ANY VIEW", summary.missing_sys_privileges)


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

    def test_parse_dependency_chains_handles_non_utf8_bytes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "dependency_chains_20260101_000001.txt"
            path.write_bytes(
                b"[TARGET - REMAPPED] chain:\n"
                b"00001. TGT.V1(VIEW) -> TGT.T1(TABLE)\n"
                + b"#" + bytes([0xFF]) + b"broken\n"
            )
            deps = rf.parse_dependency_chains_file(path)
            self.assertIn(("TGT.V1", "VIEW"), deps)
            self.assertIn(("TGT.T1", "TABLE"), deps[("TGT.V1", "VIEW")])


class TestFixupAutoGrantTypes(unittest.TestCase):
    def test_parse_fixup_auto_grant_types_defaults(self):
        types = rf.parse_fixup_auto_grant_types("")
        self.assertIn("VIEW", types)
        self.assertIn("PACKAGE BODY", types)
        self.assertNotIn("TRIGGER", types)


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

    def test_execute_sql_statements_ignores_current_schema_in_comment(self):
        sql_text = "\n".join([
            "-- ALTER SESSION SET CURRENT_SCHEMA = OB_SALES;",
            "CREATE TABLE T1(ID INT);",
        ])
        captured = []

        def fake_run_sql(_cmd, sql, _timeout):
            captured.append(sql)
            return SimpleNamespace(returncode=0, stderr="", stdout="")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            summary = rf.execute_sql_statements([], sql_text, timeout=1)

        self.assertEqual(summary.failures, [])
        self.assertEqual(len(captured), 1)
        # Comment text may contain ALTER SESSION literal, but should not be
        # interpreted as an executable current_schema preamble.
        self.assertFalse(
            captured[0].lstrip().startswith("ALTER SESSION SET CURRENT_SCHEMA =")
        )
        self.assertIn("CREATE TABLE T1", captured[0])

    def test_execute_sql_statements_session_sensitive_runs_single_subprocess(self):
        sql_text = "\n".join([
            "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';",
            "CREATE TABLE T1(ID INT);",
            "INSERT INTO T1 VALUES(1);",
        ])
        captured = []

        def fake_run_sql(_cmd, sql, _timeout):
            captured.append(sql)
            return SimpleNamespace(returncode=0, stderr="", stdout="")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            summary = rf.execute_sql_statements([], sql_text, timeout=1)

        self.assertEqual(summary.failures, [])
        self.assertEqual(summary.statements, 3)
        self.assertEqual(len(captured), 1)
        self.assertIn("ALTER SESSION SET NLS_DATE_FORMAT", captured[0].upper())
        self.assertIn("CREATE TABLE T1", captured[0].upper())

    def test_execute_sql_statements_timeout_stops_later_statements(self):
        sql_text = "\n".join([
            "SELECT 1 FROM DUAL;",
            "SELECT 2 FROM DUAL;",
        ])
        captured = []

        def fake_run_sql(_cmd, sql, _timeout):
            captured.append(sql.strip())
            if len(captured) == 1:
                raise subprocess.TimeoutExpired(cmd="obclient", timeout=_timeout)
            return SimpleNamespace(returncode=0, stderr="", stdout="")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            summary = rf.execute_sql_statements([], sql_text, timeout=1)

        self.assertEqual(summary.statements, 2)
        self.assertEqual(len(summary.failures), 1)
        self.assertIn("执行超时", summary.failures[0].error)
        self.assertEqual(captured, ["SELECT 1 FROM DUAL;"])

    def test_execute_sql_statements_non_timeout_failure_still_continues(self):
        sql_text = "\n".join([
            "SELECT 1 FROM DUAL;",
            "SELECT 2 FROM DUAL;",
        ])
        captured = []

        def fake_run_sql(_cmd, sql, _timeout):
            captured.append(sql.strip())
            if len(captured) == 1:
                return SimpleNamespace(returncode=0, stderr="ORA-00900: invalid SQL statement", stdout="")
            return SimpleNamespace(returncode=0, stderr="", stdout="")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            summary = rf.execute_sql_statements([], sql_text, timeout=1)

        self.assertEqual(summary.statements, 2)
        self.assertEqual(len(summary.failures), 1)
        self.assertEqual(captured, ["SELECT 1 FROM DUAL;", "SELECT 2 FROM DUAL;"])

    def test_detect_session_sensitive_reason_ignores_current_schema_only(self):
        sql_text = "\n".join([
            "ALTER SESSION SET CURRENT_SCHEMA = APP;",
            "CREATE VIEW V1 AS SELECT 1 FROM DUAL;",
        ])
        reason = rf.detect_session_sensitive_reason(sql_text)
        self.assertIsNone(reason)


class TestExecuteSqlErrorDetection(unittest.TestCase):
    def test_execute_sql_statements_detects_stdout_error(self):
        def fake_run_sql(_cmd, _sql, _timeout):
            return SimpleNamespace(returncode=0, stderr="", stdout="ORA-00900: invalid SQL statement")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            summary = rf.execute_sql_statements([], "CREATE VIEW V1 AS SELECT 1 FROM dual;", timeout=1)

        self.assertEqual(summary.statements, 1)
        self.assertEqual(len(summary.failures), 1)
        self.assertIn("ORA-00900", summary.failures[0].error)

    def test_execute_sql_statements_detects_pls_stdout_error(self):
        def fake_run_sql(_cmd, _sql, _timeout):
            return SimpleNamespace(returncode=0, stderr="", stdout="PLS-00201: identifier 'MISSING_PROC' must be declared")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            summary = rf.execute_sql_statements([], "BEGIN missing_proc; END;\n/", timeout=1)

        self.assertEqual(summary.statements, 1)
        self.assertEqual(len(summary.failures), 1)
        self.assertIn("PLS-00201", summary.failures[0].error)

    def test_extract_execution_error_prefers_pls_over_ora_06550_wrapper(self):
        result = SimpleNamespace(
            returncode=0,
            stderr="ORA-06550: line 1, column 7:\nPLS-00201: identifier 'MISSING_PROC' must be declared",
            stdout="",
        )
        self.assertEqual(
            rf.extract_execution_error(result),
            "PLS-00201: identifier 'MISSING_PROC' must be declared"
        )

    def test_extract_execution_error_ignores_plain_warning_without_error_signal(self):
        result = SimpleNamespace(
            returncode=0,
            stderr="",
            stdout="Warning: statement completed with info only",
        )
        self.assertIsNone(rf.extract_execution_error(result))

    def test_execute_sql_statements_detects_obe_stdout_error(self):
        def fake_run_sql(_cmd, _sql, _timeout):
            return SimpleNamespace(returncode=0, stderr="", stdout="OBE-00600: internal error code")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            summary = rf.execute_sql_statements([], "CREATE VIEW V1 AS SELECT 1 FROM dual;", timeout=1)

        self.assertEqual(summary.statements, 1)
        self.assertEqual(len(summary.failures), 1)
        self.assertIn("OBE-00600", summary.failures[0].error)

    def test_run_query_lines_detects_stdout_error(self):
        def fake_run_sql(_cmd, _sql, _timeout):
            return SimpleNamespace(returncode=0, stderr="", stdout="ERROR 1064 (42000): bad syntax")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            ok, lines, err = rf.run_query_lines([], "SELECT 1", timeout=1)

        self.assertFalse(ok)
        self.assertEqual(lines, [])
        self.assertIn("ERROR 1064", err)


class TestExecutionModeRouting(unittest.TestCase):
    def test_resolve_script_exec_mode_auto(self):
        self.assertEqual(
            rf.resolve_script_exec_mode("auto", Path("/tmp/fixup/view/A.V1.sql")),
            "file"
        )
        self.assertEqual(
            rf.resolve_script_exec_mode("auto", Path("/tmp/fixup/grants_miss/A.grants.sql")),
            "statement"
        )

    def test_execute_sql_with_mode_file_fallback_to_statement(self):
        calls: List[str] = []

        def fake_execute(_cmd, _sql, _timeout, mode="statement"):
            calls.append(mode)
            if mode == "file":
                return rf.ExecutionSummary(
                    statements=2,
                    failures=[rf.StatementFailure(1, "ORA-00900", "bad ddl")]
                )
            return rf.ExecutionSummary(statements=2, failures=[])

        stats = rf.new_exec_mode_stats()
        with mock.patch.object(rf, "execute_sql_statements", side_effect=fake_execute):
            summary = rf.execute_sql_with_mode(
                [],
                "CREATE VIEW V1 AS SELECT 1 FROM DUAL;",
                timeout=1,
                exec_mode="file",
                exec_file_fallback=True,
                exec_stats=stats,
                context_label="[TEST]",
            )

        self.assertTrue(summary.success)
        self.assertEqual(calls, ["file", "statement"])
        self.assertEqual(stats["file"], 1)
        self.assertEqual(stats["fallback_retried"], 1)
        self.assertEqual(stats["fallback_success"], 1)


class TestFailureClassification(unittest.TestCase):
    def test_classify_sql_error_detects_constraint_validate_failure(self):
        error_text = "ORA-02298: cannot validate (SCHEMA.FK_TEST) - parent keys not found"
        self.assertEqual(
            rf.classify_sql_error(error_text),
            rf.FailureType.CONSTRAINT_VALIDATE_FAIL
        )


class TestViewChainDdlSanitize(unittest.TestCase):
    def test_sanitize_view_chain_view_ddl_removes_force(self):
        ddl = 'CREATE OR REPLACE FORCE EDITIONABLE VIEW "A"."V1" AS SELECT 1 FROM dual;'
        cleaned = rf.sanitize_view_chain_view_ddl(ddl)
        self.assertIn("CREATE OR REPLACE VIEW", cleaned.upper())
        self.assertNotIn("FORCE", cleaned.upper())
        self.assertNotIn("EDITIONABLE", cleaned.upper())

    def test_sanitize_view_chain_view_ddl_ignores_comment_view_token(self):
        ddl = (
            'CREATE OR REPLACE /* marker VIEW token */ FORCE EDITIONABLE VIEW "A"."V1" AS '
            "SELECT q'[literal with VIEW and FORCE]' AS C1 FROM dual;"
        )
        cleaned = rf.sanitize_view_chain_view_ddl(ddl)
        self.assertRegex(cleaned, r"(?is)CREATE\s+OR\s+REPLACE\b.*\bVIEW\s+\"A\"\.\"V1\"\s+AS")
        self.assertIn("q'[literal with VIEW and FORCE]'", cleaned)
        self.assertNotIn(" FORCE ", cleaned.upper())
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
            "TABLE",
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
            "TABLE",
            "SELECT",
            idx_miss,
            idx_all
        )
        self.assertEqual(label, "grants_all")
        self.assertEqual(entries, [entry_all])


class TestGrantPrivilegeImplications(unittest.TestCase):
    def test_has_required_privilege_select_any_dictionary_not_for_business_table(self):
        with mock.patch.object(rf, "load_tab_privs_for_identity", return_value=set()), \
             mock.patch.object(rf, "load_roles_for_grantee", return_value=set()), \
             mock.patch.object(rf, "load_sys_privs_for_identity", return_value={"SELECT ANY DICTIONARY"}):
            ok = rf.has_required_privilege(
                obclient_cmd=[],
                timeout=1,
                grantee="ZZ_BI",
                ref_full="ZZ_APP.T_EDGE_PARENT",
                target_type="TABLE",
                required_priv="SELECT",
                roles_cache={},
                tab_privs_cache={},
                tab_privs_grantable_cache={},
                sys_privs_cache={},
                planned_object_privs=set(),
                planned_object_privs_with_option=set(),
                planned_sys_privs=set(),
                require_grant_option=False
            )
        self.assertFalse(ok)

    def test_has_required_privilege_select_any_dictionary_for_sys_object(self):
        with mock.patch.object(rf, "load_tab_privs_for_identity", return_value=set()), \
             mock.patch.object(rf, "load_roles_for_grantee", return_value=set()), \
             mock.patch.object(rf, "load_sys_privs_for_identity", return_value={"SELECT ANY DICTIONARY"}):
            ok = rf.has_required_privilege(
                obclient_cmd=[],
                timeout=1,
                grantee="ZZ_BI",
                ref_full="SYS.OBJ$",
                target_type="TABLE",
                required_priv="SELECT",
                roles_cache={},
                tab_privs_cache={},
                tab_privs_grantable_cache={},
                sys_privs_cache={},
                planned_object_privs=set(),
                planned_object_privs_with_option=set(),
                planned_sys_privs=set(),
                require_grant_option=False
            )
        self.assertTrue(ok)

    def test_find_grant_entries_ignores_select_any_dictionary_for_business_table(self):
        sys_entry = rf.GrantEntry(
            grantee="ZZ_BI",
            privileges=("SELECT ANY DICTIONARY",),
            object_name=None,
            statement="GRANT SELECT ANY DICTIONARY TO ZZ_BI;",
            source_path=Path("fixup_scripts/grants_all/ZZ_BI.sys.sql"),
            grant_type="SYSTEM"
        )
        idx_all = rf.GrantIndex({}, {}, {"ZZ_BI": [sys_entry]})
        idx_miss = rf.GrantIndex({}, {}, {})

        entries, label = rf.find_grant_entries_by_priority(
            "ZZ_BI",
            "ZZ_APP.T_EDGE_PARENT",
            "TABLE",
            "SELECT",
            idx_miss,
            idx_all
        )
        self.assertEqual(entries, [])
        self.assertEqual(label, "")

    def test_find_grant_entries_keeps_select_any_dictionary_for_sys_object(self):
        sys_entry = rf.GrantEntry(
            grantee="ZZ_BI",
            privileges=("SELECT ANY DICTIONARY",),
            object_name=None,
            statement="GRANT SELECT ANY DICTIONARY TO ZZ_BI;",
            source_path=Path("fixup_scripts/grants_all/ZZ_BI.sys.sql"),
            grant_type="SYSTEM"
        )
        idx_all = rf.GrantIndex({}, {}, {"ZZ_BI": [sys_entry]})
        idx_miss = rf.GrantIndex({}, {}, {})

        entries, label = rf.find_grant_entries_by_priority(
            "ZZ_BI",
            "SYS.OBJ$",
            "TABLE",
            "SELECT",
            idx_miss,
            idx_all
        )
        self.assertEqual(label, "grants_all")
        self.assertEqual(entries, [sys_entry])


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
        self.assertEqual(stmt, 'GRANT SELECT ON "SRC"."T1" TO "APP_USER";')
        stmt = rf.build_auto_grant_statement("app_user", "SRC.T1", "select", with_grant_option=True)
        self.assertEqual(stmt, 'GRANT SELECT ON "SRC"."T1" TO "APP_USER" WITH GRANT OPTION;')
        stmt = rf.build_auto_grant_statement("app_user", "SRC.T SPACE", "select")
        self.assertEqual(stmt, 'GRANT SELECT ON "SRC"."T SPACE" TO "APP_USER";')

    def test_classify_view_chain_status_partial(self):
        status = rf.classify_view_chain_status(
            blocked=False,
            skipped=False,
            view_exists=True,
            failure_count=2
        )
        self.assertEqual(status, "PARTIAL")

    def test_execute_auto_grant_blocked_cache_skips_repeated_planning(self):
        grant_index = rf.GrantIndex({}, {}, {})
        ctx = rf.AutoGrantContext(
            settings=rf.FixupAutoGrantSettings(
                enabled=True,
                types={"VIEW"},
                fallback=True,
                cache_limit=100
            ),
            deps_by_object={},
            grant_index_miss=grant_index,
            grant_index_all=grant_index,
            obclient_cmd=[],
            timeout=1,
            roles_cache={},
            tab_privs_cache={},
            tab_privs_grantable_cache={},
            sys_privs_cache={},
            planned_statements=set(),
            planned_object_privs=set(),
            planned_object_privs_with_option=set(),
            planned_sys_privs=set(),
            applied_grants=set(),
            blocked_objects=set(),
            stats=rf.AutoGrantStats(),
        )
        with mock.patch.object(
            rf,
            "build_auto_grant_plan_for_object",
            return_value=(["BLOCK:missing grant path"], [], True)
        ) as mocked_plan:
            applied1, blocked1 = rf.execute_auto_grant_for_object(ctx, "A.V1", "VIEW", "[T]")
            applied2, blocked2 = rf.execute_auto_grant_for_object(ctx, "A.V1", "VIEW", "[T]")
        self.assertEqual(applied1, 0)
        self.assertEqual(applied2, 0)
        self.assertTrue(blocked1)
        self.assertTrue(blocked2)
        self.assertEqual(mocked_plan.call_count, 1)
        self.assertIn(("A.V1", "VIEW"), ctx.blocked_objects)
        self.assertEqual(ctx.stats.blocked, 1)
        self.assertEqual(ctx.stats.skipped, 1)

    def test_execute_auto_grant_blocked_with_sql_does_not_cache(self):
        grant_index = rf.GrantIndex({}, {}, {})
        ctx = rf.AutoGrantContext(
            settings=rf.FixupAutoGrantSettings(
                enabled=True,
                types={"VIEW"},
                fallback=True,
                cache_limit=100
            ),
            deps_by_object={},
            grant_index_miss=grant_index,
            grant_index_all=grant_index,
            obclient_cmd=[],
            timeout=1,
            roles_cache={},
            tab_privs_cache={},
            tab_privs_grantable_cache={},
            sys_privs_cache={},
            planned_statements=set(),
            planned_object_privs=set(),
            planned_object_privs_with_option=set(),
            planned_sys_privs=set(),
            applied_grants=set(),
            blocked_objects=set(),
            stats=rf.AutoGrantStats(),
        )
        with mock.patch.object(
            rf,
            "build_auto_grant_plan_for_object",
            return_value=(["PLAN"], ["GRANT SELECT ON \"A\".\"T1\" TO \"A\";"], True)
        ) as mocked_plan, mock.patch.object(
            rf,
            "execute_sql_statements",
            return_value=rf.ExecutionSummary(1, [])
        ):
            rf.execute_auto_grant_for_object(ctx, "A.V2", "VIEW", "[T]")
            rf.execute_auto_grant_for_object(ctx, "A.V2", "VIEW", "[T]")
        self.assertEqual(mocked_plan.call_count, 2)
        self.assertNotIn(("A.V2", "VIEW"), ctx.blocked_objects)

    def test_reset_auto_grant_round_cache_clears_blocked(self):
        grant_index = rf.GrantIndex({}, {}, {})
        ctx = rf.AutoGrantContext(
            settings=rf.FixupAutoGrantSettings(
                enabled=True,
                types={"VIEW"},
                fallback=True,
                cache_limit=100
            ),
            deps_by_object={},
            grant_index_miss=grant_index,
            grant_index_all=grant_index,
            obclient_cmd=[],
            timeout=1,
            roles_cache={},
            tab_privs_cache={},
            tab_privs_grantable_cache={},
            sys_privs_cache={},
            planned_statements=set(),
            planned_object_privs=set(),
            planned_object_privs_with_option=set(),
            planned_sys_privs=set(),
            applied_grants=set(),
            blocked_objects={("A.V1", "VIEW"), ("A.V2", "VIEW")},
            stats=rf.AutoGrantStats(),
        )
        cleared = rf.reset_auto_grant_round_cache(ctx, 2)
        self.assertEqual(cleared, 2)
        self.assertEqual(ctx.blocked_objects, set())

    def test_reset_auto_grant_round_cache_clears_privilege_query_caches(self):
        grant_index = rf.GrantIndex({}, {}, {})
        ctx = rf.AutoGrantContext(
            settings=rf.FixupAutoGrantSettings(
                enabled=True,
                types={"VIEW"},
                fallback=True,
                cache_limit=100
            ),
            deps_by_object={},
            grant_index_miss=grant_index,
            grant_index_all=grant_index,
            obclient_cmd=[],
            timeout=1,
            roles_cache={"U1": {"R1"}},
            tab_privs_cache={("U1", "APP", "T1"): {"SELECT"}},
            tab_privs_grantable_cache={("U1", "APP", "T1"): {"SELECT"}},
            sys_privs_cache={"U1": {"SELECT ANY TABLE"}},
            planned_statements=set(),
            planned_object_privs=set(),
            planned_object_privs_with_option=set(),
            planned_sys_privs=set(),
            applied_grants=set(),
            blocked_objects=set(),
            stats=rf.AutoGrantStats(),
        )
        rf.reset_auto_grant_round_cache(ctx, 2)
        self.assertEqual(ctx.roles_cache, {})
        self.assertEqual(ctx.tab_privs_cache, {})
        self.assertEqual(ctx.tab_privs_grantable_cache, {})
        self.assertEqual(ctx.sys_privs_cache, {})


class TestRunFixupHelpers(unittest.TestCase):
    def test_safe_first_line(self):
        self.assertEqual(rf.safe_first_line("", 80, "n/a"), "n/a")
        self.assertEqual(rf.safe_first_line(None, 80, "n/a"), "n/a")
        self.assertEqual(rf.safe_first_line("line1\nline2", 80, "n/a"), "line1")
        self.assertEqual(rf.safe_first_line("line1", 3, "n/a"), "lin")

    def test_move_sql_to_done_returns_error_when_backup_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            done_dir = root / "done"
            src_dir = root / "view"
            src_dir.mkdir(parents=True)
            src = src_dir / "A.V1.sql"
            src.write_text("SELECT 1;", encoding="utf-8")

            target_dir = done_dir / "view"
            target_dir.mkdir(parents=True)
            target = target_dir / "A.V1.sql"
            target.write_text("old", encoding="utf-8")

            with mock.patch.object(os, "replace", side_effect=OSError("backup fail")):
                note = rf.move_sql_to_done(src, done_dir)
            self.assertIn("移动失败", note)
            self.assertTrue(src.exists())
            self.assertEqual(target.read_text(encoding="utf-8"), "old")

    def test_move_sql_to_done_returns_error_when_target_reappears_after_backup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            done_dir = root / "done"
            src_dir = root / "view"
            src_dir.mkdir(parents=True)
            src = src_dir / "A.V1.sql"
            src.write_text("SELECT 1;", encoding="utf-8")

            target_dir = done_dir / "view"
            target_dir.mkdir(parents=True)
            target = target_dir / "A.V1.sql"
            target.write_text("old", encoding="utf-8")

            original_link = os.link
            call_count = {"count": 0}

            def fake_link(src_name, dst_name, *args, **kwargs):
                call_count["count"] += 1
                if call_count["count"] == 1:
                    Path(dst_name).write_text("new", encoding="utf-8")
                    raise FileExistsError("target reappeared")
                return original_link(src_name, dst_name, *args, **kwargs)

            with mock.patch.object(os, "link", new=fake_link):
                note = rf.move_sql_to_done(src, done_dir)

            self.assertIn("已移至", note)
            self.assertFalse(src.exists())
            self.assertEqual(target.read_text(encoding="utf-8"), "SELECT 1;")
            backups = sorted(target_dir.glob("A.V1.bak_*"))
            self.assertGreaterEqual(len(backups), 2)
            backup_contents = {path.read_text(encoding="utf-8") for path in backups}
            self.assertIn("old", backup_contents)
            self.assertIn("new", backup_contents)

    def test_move_sql_to_done_restores_source_when_publish_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            done_dir = root / "done"
            src_dir = root / "view"
            src_dir.mkdir(parents=True)
            src = src_dir / "A.V2.sql"
            src.write_text("SELECT 2;", encoding="utf-8")

            with mock.patch.object(os, "link", side_effect=OSError("publish fail")):
                note = rf.move_sql_to_done(src, done_dir)

            self.assertIn("移动失败", note)
            self.assertTrue(src.exists())
            self.assertEqual(src.read_text(encoding="utf-8"), "SELECT 2;")

    def test_invalidate_exists_cache_removes_all_planned_keys(self):
        cache = {
            ("A.V1", "VIEW"): False,
            ("A.T1", "TABLE"): False,
            ("B.T2", "TABLE"): True,
        }
        removed = rf.invalidate_exists_cache(cache, {("A.V1", "view"), ("A.T1", "TABLE"), ("X.Y", "VIEW")})
        self.assertEqual(removed, 2)
        self.assertNotIn(("A.V1", "VIEW"), cache)
        self.assertNotIn(("A.T1", "TABLE"), cache)
        self.assertIn(("B.T2", "TABLE"), cache)

    def test_parse_object_identity_from_path_matches_parse_object_from_filename(self):
        path = Path("A.B.C.sql")
        self.assertEqual(
            rf.parse_object_identity_from_path(path),
            rf.parse_object_from_filename(path),
        )

    def test_has_required_privilege_rechecks_after_round_cache_reset(self):
        calls = []
        state = {"round": 1}

        def fake_query_single_column(_cmd, sql, _timeout, _column):
            calls.append(sql)
            if "DBA_TAB_PRIVS" in sql:
                return set() if state["round"] == 1 else {"SELECT"}
            return set()

        grant_index = rf.GrantIndex({}, {}, {})
        ctx = rf.AutoGrantContext(
            settings=rf.FixupAutoGrantSettings(
                enabled=True,
                types={"VIEW"},
                fallback=True,
                cache_limit=100
            ),
            deps_by_object={},
            grant_index_miss=grant_index,
            grant_index_all=grant_index,
            obclient_cmd=[],
            timeout=1,
            roles_cache={},
            tab_privs_cache={},
            tab_privs_grantable_cache={},
            sys_privs_cache={},
            planned_statements=set(),
            planned_object_privs=set(),
            planned_object_privs_with_option=set(),
            planned_sys_privs=set(),
            applied_grants=set(),
            blocked_objects=set(),
            stats=rf.AutoGrantStats(),
        )

        with mock.patch.object(rf, "query_single_column", side_effect=fake_query_single_column):
            round1 = rf.has_required_privilege(
                [],
                None,
                "U1",
                "APP.T1",
                "TABLE",
                "SELECT",
                ctx.roles_cache,
                ctx.tab_privs_cache,
                ctx.tab_privs_grantable_cache,
                ctx.sys_privs_cache,
                set(),
                set(),
                set(),
                False,
            )
            self.assertFalse(round1)
            state["round"] = 2
            rf.reset_auto_grant_round_cache(ctx, 2)
            round2 = rf.has_required_privilege(
                [],
                None,
                "U1",
                "APP.T1",
                "TABLE",
                "SELECT",
                ctx.roles_cache,
                ctx.tab_privs_cache,
                ctx.tab_privs_grantable_cache,
                ctx.sys_privs_cache,
                set(),
                set(),
                set(),
                False,
            )
        self.assertTrue(round2)

    def test_acquire_fixup_run_lock_blocks_second_process(self):
        if rf.fcntl is None:
            self.skipTest("fcntl unavailable")
        with tempfile.TemporaryDirectory() as tmpdir:
            fixup_dir = Path(tmpdir)
            with rf.acquire_fixup_run_lock(fixup_dir):
                with self.assertRaises(rf.ConfigError):
                    with rf.acquire_fixup_run_lock(fixup_dir):
                        pass

    def test_execute_script_with_summary_marks_error_on_move_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sql_path = root / "fixup_scripts" / "view" / "A.V1.sql"
            sql_path.parent.mkdir(parents=True)
            sql_path.write_text("CREATE VIEW A.V1 AS SELECT 1 FROM DUAL;", encoding="utf-8")

            with mock.patch.object(rf, "execute_sql_statements", return_value=rf.ExecutionSummary(1, [])), \
                 mock.patch.object(rf, "move_sql_to_done", return_value="(移动失败: 目标已存在且备份失败)"):
                result, summary = rf.execute_script_with_summary(
                    [],
                    sql_path,
                    root,
                    root / "fixup_scripts" / "done",
                    timeout=1,
                    layer=0,
                    label_prefix="[TEST]",
                    max_sql_file_bytes=None,
                )
            self.assertEqual(summary.statements, 1)
            self.assertEqual(result.status, "ERROR")
            self.assertIn("移动失败", result.message)

    def test_execute_script_with_summary_auto_mode_routes_to_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sql_path = root / "fixup_scripts" / "view" / "A.V1.sql"
            sql_path.parent.mkdir(parents=True)
            sql_path.write_text("CREATE VIEW A.V1 AS SELECT 1 FROM DUAL;", encoding="utf-8")

            with mock.patch.object(rf, "execute_sql_with_mode", return_value=rf.ExecutionSummary(1, [])) as m_exec, \
                 mock.patch.object(rf, "move_sql_to_done", return_value="(已移至 done/view/)"):
                result, summary = rf.execute_script_with_summary(
                    [],
                    sql_path,
                    root,
                    root / "fixup_scripts" / "done",
                    timeout=1,
                    layer=0,
                    label_prefix="[TEST]",
                    max_sql_file_bytes=None,
                    exec_mode="auto",
                    exec_file_fallback=True,
                )
            self.assertEqual(result.status, "SUCCESS")
            self.assertEqual(summary.statements, 1)
            self.assertTrue(m_exec.called)
            self.assertEqual(m_exec.call_args.kwargs.get("exec_mode"), "file")

    def test_state_ledger_skips_replay_after_move_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup_scripts"
            sql_path = fixup_dir / "view" / "A.V1.sql"
            sql_path.parent.mkdir(parents=True)
            sql_path.write_text("CREATE VIEW A.V1 AS SELECT 1 FROM DUAL;", encoding="utf-8")
            done_dir = fixup_dir / "done"
            ledger = rf.FixupStateLedger(fixup_dir)

            with mock.patch.object(rf, "execute_sql_statements", return_value=rf.ExecutionSummary(1, [])), \
                 mock.patch.object(rf, "move_sql_to_done", return_value="(移动失败: nfs lock)"):
                result, _summary = rf.execute_script_with_summary(
                    [],
                    sql_path,
                    root,
                    done_dir,
                    timeout=1,
                    layer=0,
                    label_prefix="[TEST]",
                    max_sql_file_bytes=None,
                    state_ledger=ledger,
                )
            self.assertEqual(result.status, "ERROR")
            ledger.flush()

            reloaded_ledger = rf.FixupStateLedger(fixup_dir)
            with mock.patch.object(rf, "execute_sql_statements", side_effect=AssertionError("should not replay")):
                result2, summary2 = rf.execute_script_with_summary(
                    [],
                    sql_path,
                    root,
                    done_dir,
                    timeout=1,
                    layer=0,
                    label_prefix="[TEST]",
                    max_sql_file_bytes=None,
                    state_ledger=reloaded_ledger,
                )
            self.assertEqual(result2.status, "SKIPPED")
            self.assertIn("状态账本命中", result2.message)
            self.assertEqual(summary2.statements, 0)

    def test_state_ledger_flush_uses_atomic_replace(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixup_dir = Path(tmpdir)
            ledger = rf.FixupStateLedger(fixup_dir)
            ledger.mark_completed(Path("view/A.V1.sql"), "fp1", "ok")
            observed = {}
            original_replace = rf.os.replace

            def fake_replace(src, dst):
                observed["src"] = Path(src)
                observed["dst"] = Path(dst)
                self.assertTrue(Path(src).exists())
                return original_replace(src, dst)

            with mock.patch.object(rf.os, "replace", side_effect=fake_replace) as mocked_replace:
                ledger.flush()

            self.assertTrue(mocked_replace.called)
            self.assertEqual(observed["dst"], fixup_dir / rf.STATE_LEDGER_FILENAME)
            self.assertFalse(observed["src"].exists())
            payload = json.loads((fixup_dir / rf.STATE_LEDGER_FILENAME).read_text(encoding="utf-8"))
            self.assertIn("view/A.V1.sql", payload["completed"])

    def test_state_ledger_flush_keeps_dirty_when_atomic_replace_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixup_dir = Path(tmpdir)
            ledger = rf.FixupStateLedger(fixup_dir)
            ledger.mark_completed(Path("view/A.V1.sql"), "fp1", "ok")

            with mock.patch.object(rf.os, "replace", side_effect=OSError("disk full")):
                ledger.flush()

            self.assertTrue(ledger._dirty)
            self.assertFalse((fixup_dir / rf.STATE_LEDGER_FILENAME).exists())
            tmp_files = list(fixup_dir.glob(f"{rf.STATE_LEDGER_FILENAME}.*.tmp"))
            self.assertEqual(tmp_files, [])

    def test_build_run_fixup_change_notices_respects_selected_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixup_dir = Path(tmpdir)
            (fixup_dir / "table").mkdir()
            (fixup_dir / "view").mkdir()
            (fixup_dir / "grants_revoke").mkdir()
            args = SimpleNamespace(allow_table_create=False, view_chain_autofix=False)

            notices = rf.build_run_fixup_change_notices(args, fixup_dir, ["procedure"])

            self.assertEqual(notices, [])

    def test_build_run_fixup_change_notices_for_full_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixup_dir = Path(tmpdir)
            (fixup_dir / "table").mkdir()
            (fixup_dir / "view").mkdir()
            (fixup_dir / "grants_revoke").mkdir()
            args = SimpleNamespace(allow_table_create=False, view_chain_autofix=False)

            notices = rf.build_run_fixup_change_notices(args, fixup_dir, [])

            self.assertEqual(
                [notice.notice_id for notice in notices],
                ["fixup_table_safe_gate", "public_grants_revoke_audit", "view_chain_autofix"]
            )

    def test_load_manual_actions_report_and_select_relevant_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report = Path(tmpdir) / "manual_actions_required_123.txt"
            report.write_text(
                "\n".join([
                    "# title",
                    "# total=2",
                    "# report_kind=MANUAL_ACTION_REQUIRED",
                    "# schema_version=1",
                    "PRIORITY|STAGE|CATEGORY|COUNT|DEFAULT_BEHAVIOR|PRIMARY_ARTIFACT|RELATED_FIXUP_DIR|WHY|RECOMMENDED_ACTION",
                    "BLOCKER|BEFORE_FIXUP|UNSUPPORTED_OBJECT|3|GENERATED_BUT_DO_NOT_RUN|unsupported_objects_detail_123.txt|unsupported/|why1|action1",
                    "REVIEW|BEFORE_DIR_EXECUTE|UNSUPPORTED_GRANT|5|NOT_GENERATED|unsupported_grant_detail_123.txt|grants_miss/|why2|action2",
                ]) + "\n",
                encoding="utf-8",
            )
            rows = rf.load_manual_actions_report(report)
            self.assertEqual(len(rows), 2)
            selected = rf.select_relevant_manual_actions(rows, ["grants_miss"])
            self.assertEqual([row.category for row in selected], ["UNSUPPORTED_OBJECT", "UNSUPPORTED_GRANT"])

    def test_load_manual_actions_report_rejects_wrong_report_kind(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report = Path(tmpdir) / "manual_actions_required_123.txt"
            report.write_text(
                "\n".join([
                    "# title",
                    "# total=1",
                    "# report_kind=OTHER_KIND",
                    "# schema_version=1",
                    "PRIORITY|STAGE|CATEGORY|COUNT|DEFAULT_BEHAVIOR|PRIMARY_ARTIFACT|RELATED_FIXUP_DIR|WHY|RECOMMENDED_ACTION",
                    "BLOCKER|BEFORE_FIXUP|UNSUPPORTED_OBJECT|3|GENERATED_BUT_DO_NOT_RUN|unsupported_objects_detail_123.txt|unsupported/|why1|action1",
                ]) + "\n",
                encoding="utf-8",
            )
            rows = rf.load_manual_actions_report(report)
            self.assertEqual(rows, [])

    def test_load_notice_state_backs_up_corrupted_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / ".comparator_notice_state.json"
            state_path.write_text("{not-json", encoding="utf-8")
            with mock.patch.object(rf.logging.getLogger(__name__), "warning"):
                resolved, state = rf.load_notice_state(tmpdir)
            self.assertEqual(resolved, state_path)
            self.assertEqual(state["seen_notices"], {})
            backups = list(Path(tmpdir).glob(".comparator_notice_state.json.corrupted.*"))
            self.assertTrue(backups)

    def test_select_relevant_manual_actions_keeps_global_review_and_nested_dirs(self):
        rows = [
            rf.ManualActionNoticeRow(
                priority="REVIEW",
                stage="POST_COMPARE_REVIEW",
                category="GRANT_CAPABILITY_MANUAL",
                count=2,
                default_behavior="REPORT_ONLY",
                primary_artifact="grant_capability_detail_123.txt",
                related_fixup_dir="",
                why="why",
                recommended_action="action",
            ),
            rf.ManualActionNoticeRow(
                priority="REVIEW",
                stage="BEFORE_DIR_EXECUTE",
                category="NESTED_PATH",
                count=1,
                default_behavior="REPORT_ONLY",
                primary_artifact="nested.txt",
                related_fixup_dir="table/constraints",
                why="why",
                recommended_action="action",
            ),
        ]
        selected = rf.select_relevant_manual_actions(rows, ["table"])
        self.assertEqual([row.category for row in selected], ["GRANT_CAPABILITY_MANUAL", "NESTED_PATH"])

    def test_persist_seen_notices_merges_existing_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / ".comparator_notice_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "last_seen_tool_version": "0.9.8.7",
                        "seen_notices": {"A": "0.9.8.7"},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            rf.persist_seen_notices(
                state_path,
                {"seen_notices": {"B": "0.9.8.7"}},
                "0.9.8.8",
                [rf.RuntimeNotice("C", "0.9.8.8", "t", "m")],
            )
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["seen_notices"]["A"], "0.9.8.7")
            self.assertEqual(payload["seen_notices"]["B"], "0.9.8.7")
            self.assertEqual(payload["seen_notices"]["C"], "0.9.8.8")

    def test_find_latest_manual_actions_report_prefers_latest_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir)
            run1 = report_dir / "run_20260311_120000"
            run2 = report_dir / "run_20260311_130000"
            run1.mkdir(parents=True)
            run2.mkdir(parents=True)
            path1 = run1 / "manual_actions_required_20260311_120000.txt"
            path2 = run2 / "manual_actions_required_20260311_130000.txt"
            path1.write_text("x", encoding="utf-8")
            path2.write_text("y", encoding="utf-8")
            self.assertEqual(rf.find_latest_manual_actions_report(report_dir), path2)

    def test_main_persists_run_fixup_unseen_notices(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_path = root / "config.ini"
            config_path.write_text("", encoding="utf-8")
            fixup_dir = root / "fixup_scripts"
            fixup_dir.mkdir()
            notice_state = {"seen_notices": {}}
            notice_state_path = root / ".comparator_notice_state.json"
            notices = [
                rf.RuntimeNotice(
                    "fixup_table_safe_gate",
                    "0.9.8.7",
                    "建表脚本默认不执行",
                    "run_fixup 默认跳过 table/；确需建表请显式加 --allow-table-create。",
                )
            ]
            args = SimpleNamespace(
                config=str(config_path),
                only_dirs=[],
                exclude_dirs=[],
                only_types=[],
                allow_table_create=False,
                smart_order=False,
                recompile=False,
                max_retries=5,
                glob_patterns=None,
                iterative=False,
                max_rounds=10,
                min_progress=1,
                view_chain_autofix=False,
            )
            lock_ctx = mock.MagicMock()
            lock_ctx.__enter__.return_value = None
            lock_ctx.__exit__.return_value = False
            fixup_settings = rf.FixupAutoGrantSettings(False, set(), False, 0)

            with mock.patch.object(rf, "parse_args", return_value=args), \
                 mock.patch.object(
                     rf,
                     "load_ob_config",
                     return_value=({}, fixup_dir, root, "INFO", root / "main_reports", fixup_settings, None),
                 ), \
                 mock.patch.object(rf, "resolve_console_log_level", return_value=logging.INFO), \
                 mock.patch.object(rf, "set_console_log_level"), \
                 mock.patch.object(rf, "init_fixup_hot_reload_runtime", return_value=None), \
                 mock.patch.object(rf, "acquire_fixup_run_lock", return_value=lock_ctx), \
                 mock.patch.object(rf, "run_single_fixup"), \
                 mock.patch.object(rf, "load_notice_state", return_value=(notice_state_path, notice_state)), \
                 mock.patch.object(rf, "build_run_fixup_change_notices", return_value=notices), \
                 mock.patch.object(rf, "select_unseen_notices", return_value=notices), \
                 mock.patch.object(rf, "log_change_notices_block") as mock_log_notices, \
                 mock.patch.object(rf, "find_latest_manual_actions_report", return_value=None), \
                 mock.patch.object(rf, "load_manual_actions_report", return_value=[]), \
                 mock.patch.object(rf, "select_relevant_manual_actions", return_value=[]), \
                 mock.patch.object(rf, "log_manual_action_preflight") as mock_manual_preflight, \
                 mock.patch.object(rf, "persist_seen_notices") as mock_persist:
                rf.main()

            mock_log_notices.assert_called_once_with(notices)
            mock_manual_preflight.assert_called_once_with(None, [])
            mock_persist.assert_called_once_with(notice_state_path, notice_state, rf.__version__, notices)

    def test_main_rejects_iterative_and_view_chain_autofix_together(self):
        args = SimpleNamespace(
            config='config.ini',
            only_dirs=[],
            exclude_dirs=[],
            only_types=[],
            allow_table_create=False,
            smart_order=False,
            recompile=False,
            max_retries=5,
            glob_patterns=None,
            iterative=True,
            max_rounds=10,
            min_progress=1,
            view_chain_autofix=True,
        )
        with mock.patch.object(rf, 'parse_args', return_value=args),              mock.patch.object(rf.log, 'error') as log_error,              self.assertRaises(SystemExit) as cm:
            rf.main()
        self.assertEqual(cm.exception.code, 2)
        self.assertTrue(any('不能同时启用' in str(call.args[0]) for call in log_error.call_args_list))


class TestLimitedCache(unittest.TestCase):
    def test_eviction(self):
        cache = rf.LimitedCache(2)
        cache["a"] = 1
        cache["b"] = 2
        cache["c"] = 3
        self.assertEqual(len(cache), 2)
        self.assertNotIn("a", cache)


class TestErrorReport(unittest.TestCase):
    def test_write_error_report_marks_truncated_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixup_dir = Path(tmpdir)
            report = rf.write_error_report(
                [rf.ErrorReportEntry('a.sql', 1, 'ORA-00942', 'A.T1', 'boom')],
                fixup_dir,
                limit=1,
                truncated=True,
            )
            self.assertIsNotNone(report)
            content = Path(report).read_text(encoding='utf-8')
            self.assertIn('[... TRUNCATED ...]', content)


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

    def test_build_view_chain_plan_requires_grant_option_for_cross_schema_table(self):
        chains = [
            [("A.V1", "VIEW"), ("B.T1", "TABLE")],
        ]
        grant_index = rf.GrantIndex({}, {}, {})
        require_option_calls = []

        def fake_plan_object_grant_for_dependency(
            grantee,
            target_full,
            target_type,
            required_priv,
            require_grant_option,
            *_args,
            **_kwargs,
        ):
            require_option_calls.append(
                (grantee, target_full, target_type, required_priv, require_grant_option)
            )
            return False

        with mock.patch.object(rf, "plan_object_grant_for_dependency", side_effect=fake_plan_object_grant_for_dependency), \
             mock.patch.object(rf, "check_object_exists", return_value=True), \
             mock.patch.object(rf, "select_fixup_script_for_node_with_fallback", return_value=(None, None)):
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

        self.assertFalse(blocked)
        self.assertEqual(sql_lines, [])
        self.assertEqual(len(require_option_calls), 1)
        self.assertEqual(require_option_calls[0], ("A", "B.T1", "TABLE", "SELECT", True))

    def test_build_auto_grant_plan_for_view_requires_grant_option_for_cross_schema_table(self):
        ctx = rf.AutoGrantContext(
            settings=rf.FixupAutoGrantSettings(enabled=True, types={"VIEW"}, fallback=False, cache_limit=10000, exec_mode="auto", exec_file_fallback=True),
            deps_by_object={("A.V1", "VIEW"): {("B.T1", "TABLE")}},
            grant_index_miss=rf.GrantIndex({}, {}, {}),
            grant_index_all=rf.GrantIndex({}, {}, {}),
            obclient_cmd=[],
            timeout=None,
            roles_cache={},
            tab_privs_cache={},
            tab_privs_grantable_cache={},
            sys_privs_cache={},
            planned_statements=set(),
            planned_object_privs=set(),
            planned_object_privs_with_option=set(),
            planned_sys_privs=set(),
            applied_grants=set(),
            blocked_objects=set(),
            stats=rf.AutoGrantStats(),
        )
        require_option_calls = []

        def fake_plan_object_grant_for_dependency(
            grantee,
            target_full,
            target_type,
            required_priv,
            require_grant_option,
            *_args,
            **_kwargs,
        ):
            require_option_calls.append(
                (grantee, target_full, target_type, required_priv, require_grant_option)
            )
            return False

        with mock.patch.object(rf, "plan_object_grant_for_dependency", side_effect=fake_plan_object_grant_for_dependency):
            plan_lines, sql_lines, blocked = rf.build_auto_grant_plan_for_object(ctx, "A.V1", "VIEW")

        self.assertEqual(plan_lines, [])
        self.assertEqual(sql_lines, [])
        self.assertFalse(blocked)
        self.assertEqual(require_option_calls[0], ("A", "B.T1", "TABLE", "SELECT", True))

    def test_infer_required_privileges_from_failed_statement_for_view_grant(self):
        privs = rf.infer_required_privileges_from_failed_statement(
            "GRANT UPDATE ON MONSTER_A.V1 TO U1;",
            "MONSTER_A.V1",
            "VIEW",
        )
        self.assertEqual(privs, {"UPDATE"})

    def test_infer_permission_retry_target_for_view_post_grant(self):
        target = rf.infer_permission_retry_target(
            "GRANT UPDATE ON MONSTER_A.V1 TO U1;",
            Path("fixup/view_post_grants/MONSTER_A.update.sql"),
        )
        self.assertEqual(target, ("MONSTER_A.V1", "VIEW", {"UPDATE"}))

    def test_build_auto_grant_plan_for_view_uses_failed_grant_privilege_override(self):
        ctx = rf.AutoGrantContext(
            settings=rf.FixupAutoGrantSettings(enabled=True, types={"VIEW"}, fallback=False, cache_limit=10000, exec_mode="auto", exec_file_fallback=True),
            deps_by_object={("A.V1", "VIEW"): {("B.T1", "TABLE")}},
            grant_index_miss=rf.GrantIndex({}, {}, {}),
            grant_index_all=rf.GrantIndex({}, {}, {}),
            obclient_cmd=[],
            timeout=None,
            roles_cache={},
            tab_privs_cache={},
            tab_privs_grantable_cache={},
            sys_privs_cache={},
            planned_statements=set(),
            planned_object_privs=set(),
            planned_object_privs_with_option=set(),
            planned_sys_privs=set(),
            applied_grants=set(),
            blocked_objects=set(),
            stats=rf.AutoGrantStats(),
        )
        calls = []

        def fake_plan_object_grant_for_dependency(
            grantee,
            target_full,
            target_type,
            required_priv,
            require_grant_option,
            *_args,
            **_kwargs,
        ):
            calls.append((grantee, target_full, target_type, required_priv, require_grant_option))
            return False

        with mock.patch.object(rf, "plan_object_grant_for_dependency", side_effect=fake_plan_object_grant_for_dependency):
            plan_lines, sql_lines, blocked = rf.build_auto_grant_plan_for_object(
                ctx,
                "A.V1",
                "VIEW",
                required_privileges_override={"UPDATE"},
            )

        self.assertEqual(plan_lines, [])
        self.assertEqual(sql_lines, [])
        self.assertFalse(blocked)
        self.assertEqual(calls, [("A", "B.T1", "TABLE", "UPDATE", True)])

    def test_find_matching_view_refresh_script(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixup_dir = Path(tmpdir)
            refresh_dir = fixup_dir / "view_refresh"
            refresh_dir.mkdir(parents=True)
            target = refresh_dir / "MONSTER_A.V1.sql"
            target.write_text("CREATE OR REPLACE VIEW ...;", encoding="utf-8")
            found = rf.find_matching_view_refresh_script(fixup_dir, "MONSTER_A.V1", "VIEW")
            self.assertEqual(found, target)
            self.assertIsNone(rf.find_matching_view_refresh_script(fixup_dir, "MONSTER_A.V1", "TABLE"))

    def test_execute_view_refresh_before_retry_runs_matching_script(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixup_dir = Path(tmpdir)
            done_dir = fixup_dir / "done"
            done_dir.mkdir(parents=True)
            refresh_dir = fixup_dir / "view_refresh"
            refresh_dir.mkdir(parents=True)
            refresh_path = refresh_dir / "MONSTER_A.V1.sql"
            refresh_path.write_text("CREATE OR REPLACE VIEW MONSTER_A.V1 AS SELECT 1 FROM DUAL;", encoding="utf-8")

            with mock.patch.object(
                rf,
                "execute_script_with_summary",
                return_value=(rf.ScriptResult(refresh_path, "SUCCESS", "ok"), rf.ExecutionSummary(1, [])),
            ) as mocked_exec:
                result = rf.execute_view_refresh_before_retry(
                    fixup_dir=fixup_dir,
                    object_full="MONSTER_A.V1",
                    object_type="VIEW",
                    obclient_cmd=[],
                    done_dir=done_dir,
                    timeout=10,
                    layer=5,
                    label="[1/1]",
                    max_sql_file_bytes=None,
                    state_ledger=None,
                    exec_mode="auto",
                    exec_file_fallback=True,
                    exec_stats=rf.new_exec_mode_stats(),
                )

            self.assertIsNotNone(result)
            self.assertEqual(result.status, "SUCCESS")
            mocked_exec.assert_called_once()


class TestSqlParsing(unittest.TestCase):
    def test_split_nested_block_comments(self):
        sql = "/* outer /* inner */ still */\nSELECT 1 FROM dual;\n"
        statements = rf.split_sql_statements(sql)
        self.assertEqual(len(statements), 1)
        self.assertIn("SELECT 1", statements[0])

    def test_split_sql_ignores_block_start_tokens_inside_block_comment(self):
        sql = (
            "/*\n"
            "DECLARE\n"
            "  x number;\n"
            "BEGIN\n"
            "  NULL;\n"
            "END;\n"
            "*/\n"
            "SELECT 1 FROM dual;\n"
        )
        statements = rf.split_sql_statements(sql)
        self.assertEqual(len(statements), 1)
        self.assertIn("SELECT 1 FROM dual", statements[0])

    def test_split_plsql_block_without_slash_followed_by_sql(self):
        sql = (
            "CREATE OR REPLACE PROCEDURE P AS\n"
            "BEGIN\n"
            "  IF 1 = 1 THEN\n"
            "    NULL;\n"
            "  END IF;\n"
            "END;\n"
            "GRANT SELECT ON A.T1 TO U1;\n"
        )
        statements = rf.split_sql_statements(sql)
        self.assertEqual(len(statements), 2)
        self.assertIn("CREATE OR REPLACE PROCEDURE P AS", statements[0])
        self.assertIn("END IF;", statements[0])
        self.assertTrue(statements[1].strip().startswith("GRANT SELECT ON A.T1 TO U1"))

    def test_split_sql_keeps_q_quote_slash_line_inside_block(self):
        sql = (
            "CREATE FUNCTION F RETURN VARCHAR2 IS\n"
            "  v VARCHAR2(100) := q'[some\n"
            "/\n"
            "content]';\n"
            "BEGIN\n"
            "  RETURN v;\n"
            "END;\n"
            "/\n"
        )
        statements = rf.split_sql_statements(sql)
        self.assertEqual(len(statements), 1)
        self.assertIn("content]';", statements[0])
        self.assertIn("RETURN v;", statements[0])

    def test_is_comment_only_statement_with_string_literal(self):
        stmt = "SELECT '--not comment' AS C1 FROM DUAL;"
        self.assertFalse(rf.is_comment_only_statement(stmt))

    def test_parse_grant_object_ignores_leading_comments(self):
        stmt = "-- 自动追加相关授权语句\nGRANT SELECT ON LIFEDATA.T1 TO APP;"
        self.assertEqual(rf.parse_grant_object(stmt), "LIFEDATA.T1")


class TestErrorReporting(unittest.TestCase):
    def test_record_error_entry_uses_filename_for_non_grant_sql(self):
        entries = []
        ok = rf.record_error_entry(
            entries,
            10,
            Path("fixup_scripts/sequence/LIFEDATA.TEMP_SWITCH_TBL_LOG_SEQ.sql"),
            2,
            'CREATE SEQUENCE "LIFEDATA"."TEMP_SWITCH_TBL_LOG_SEQ" START WITH 1;',
            "OBE-00600: internal error code"
        )
        self.assertTrue(ok)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].error_code, "OBE-00600")
        self.assertEqual(entries[0].object_name, "LIFEDATA.TEMP_SWITCH_TBL_LOG_SEQ")


class TestErrorClassification(unittest.TestCase):
    def test_extended_error_codes(self):
        self.assertEqual(rf.classify_sql_error("ORA-00054: resource busy"), rf.FailureType.LOCK_TIMEOUT)
        self.assertEqual(rf.classify_sql_error("ORA-01017: invalid username/password"), rf.FailureType.AUTH_FAILED)
        self.assertEqual(rf.classify_sql_error("ORA-12170: TNS:Connect timeout"), rf.FailureType.CONNECTION_TIMEOUT)
        self.assertEqual(rf.classify_sql_error("ORA-04031: unable to allocate"), rf.FailureType.RESOURCE_EXHAUSTED)
        self.assertEqual(rf.classify_sql_error("ORA-01555: snapshot too old"), rf.FailureType.SNAPSHOT_ERROR)
        self.assertEqual(rf.classify_sql_error("ORA-00060: deadlock detected"), rf.FailureType.DEADLOCK)
        self.assertEqual(rf.classify_sql_error("ERROR 1146 (42S02): Table doesn't exist"), rf.FailureType.MISSING_OBJECT)
        self.assertEqual(rf.classify_sql_error("ERROR 1142 (42000): command denied"), rf.FailureType.PERMISSION_DENIED)
        self.assertEqual(rf.classify_sql_error("ERROR 1064 (42000): syntax error"), rf.FailureType.SYNTAX_ERROR)


class TestGrantExecution(unittest.TestCase):
    def test_apply_grant_entries_detects_stdout_error_even_returncode_zero(self):
        entry = rf.GrantEntry(
            grantee="APP",
            privileges=("SELECT",),
            object_name="SRC.T1",
            statement="GRANT SELECT ON SRC.T1 TO APP;",
            source_path=Path("fixup_scripts/grants_miss/APP.grants.sql"),
            grant_type="OBJECT",
        )
        applied_grants = set()

        def fake_run_sql(_cmd, _sql, _timeout):
            return SimpleNamespace(returncode=0, stderr="", stdout="ORA-01031: insufficient privileges")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            applied, failed = rf.apply_grant_entries([], [entry], timeout=1, applied_grants=applied_grants)

        self.assertEqual(applied, 0)
        self.assertEqual(failed, 1)
        self.assertEqual(len(applied_grants), 0)

    def test_execute_grant_file_with_prune_detects_error_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo_root = root
            sql_path = root / "fixup_scripts" / "grants_miss" / "A.grants.sql"
            sql_path.parent.mkdir(parents=True)
            sql_path.write_text("GRANT SELECT ON A.T1 TO B;\n", encoding="utf-8")
            done_dir = root / "fixup_scripts" / "done"
            done_dir.mkdir(parents=True)
            errors = []

            def fake_run_sql(_cmd, _sql, _timeout):
                return SimpleNamespace(returncode=0, stderr="", stdout="ORA-01031: insufficient privileges")

            with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
                result, summary, removed, kept, truncated = rf.execute_grant_file_with_prune(
                    [],
                    sql_path,
                    repo_root,
                    done_dir,
                    timeout=1,
                    layer=0,
                    label_prefix="[TEST]",
                    error_entries=errors,
                    error_limit=10,
                    max_sql_file_bytes=None,
                )

            self.assertEqual(result.status, "FAILED")
            self.assertEqual(summary.statements, 1)
            self.assertEqual(len(summary.failures), 1)
            self.assertEqual(removed, 0)
            self.assertEqual(kept, 1)
            self.assertFalse(truncated)

    def test_execute_grant_file_with_prune_skips_non_grant_statement_rewrite(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo_root = root
            sql_path = root / "fixup_scripts" / "grants_miss" / "A.grants.sql"
            sql_path.parent.mkdir(parents=True)
            sql_path.write_text(
                "ALTER SESSION SET CURRENT_SCHEMA = APP;\n"
                "GRANT SELECT ON A.T1 TO B;\n",
                encoding="utf-8",
            )
            done_dir = root / "fixup_scripts" / "done"
            done_dir.mkdir(parents=True)
            errors = []

            def fake_run_sql(_cmd, sql, _timeout):
                sql_u = sql.upper()
                if "ALTER SESSION SET CURRENT_SCHEMA" in sql_u:
                    return SimpleNamespace(returncode=0, stderr="", stdout="")
                return SimpleNamespace(returncode=1, stderr="ORA-01031: insufficient privileges", stdout="")

            with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
                result, summary, removed, kept, truncated = rf.execute_grant_file_with_prune(
                    [],
                    sql_path,
                    repo_root,
                    done_dir,
                    timeout=1,
                    layer=0,
                    label_prefix="[TEST]",
                    error_entries=errors,
                    error_limit=10,
                    max_sql_file_bytes=None,
                )

            rewritten = sql_path.read_text(encoding="utf-8").upper()
            self.assertEqual(result.status, "FAILED")
            self.assertEqual(summary.statements, 1)
            self.assertEqual(removed, 0)
            self.assertEqual(kept, 1)
            self.assertNotIn("ALTER SESSION SET CURRENT_SCHEMA = APP;", rewritten)
            self.assertIn("GRANT SELECT ON A.T1 TO B;", rewritten)
            self.assertFalse(truncated)

    def test_execute_grant_file_with_prune_executes_grant_after_section_comment(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo_root = root
            sql_path = root / "fixup_scripts" / "grants_miss" / "A.grants.sql"
            sql_path.parent.mkdir(parents=True)
            sql_path.write_text(
                "-- OBJECT_TYPE: TABLE (1)\n"
                "-- TABLE_COLUMN_GRANTS (1)\n"
                "GRANT INSERT (ID) ON A.T1 TO PUBLIC;\n",
                encoding="utf-8",
            )
            done_dir = root / "fixup_scripts" / "done"
            done_dir.mkdir(parents=True)
            errors = []
            executed_sqls = []

            def fake_run_sql(_cmd, sql, _timeout):
                executed_sqls.append(sql)
                return SimpleNamespace(returncode=0, stderr="", stdout="")

            with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
                result, summary, removed, kept, truncated = rf.execute_grant_file_with_prune(
                    [],
                    sql_path,
                    repo_root,
                    done_dir,
                    timeout=1,
                    layer=0,
                    label_prefix="[TEST]",
                    error_entries=errors,
                    error_limit=10,
                    max_sql_file_bytes=None,
                )

            self.assertEqual(result.status, "SUCCESS")
            self.assertEqual(summary.statements, 1)
            self.assertEqual(removed, 1)
            self.assertEqual(kept, 0)
            self.assertFalse(truncated)
            self.assertEqual(len(executed_sqls), 1)
            self.assertIn("GRANT INSERT (ID) ON A.T1 TO PUBLIC;", executed_sqls[0].upper())

    def test_execute_grant_file_with_prune_cleans_temp_file_on_rewrite_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo_root = root
            sql_path = root / "fixup_scripts" / "grants_miss" / "A.grants.sql"
            sql_path.parent.mkdir(parents=True)
            sql_path.write_text("GRANT SELECT ON A.T1 TO B;\n", encoding="utf-8")
            done_dir = root / "fixup_scripts" / "done"
            done_dir.mkdir(parents=True)
            errors = []

            def fake_run_sql(_cmd, _sql, _timeout):
                return SimpleNamespace(returncode=1, stderr="ORA-01031: insufficient privileges", stdout="")

            def fake_replace(_src, _dst):
                raise OSError("replace failed")

            with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql), \
                 mock.patch.object(rf.os, "replace", side_effect=fake_replace):
                result, summary, removed, kept, truncated = rf.execute_grant_file_with_prune(
                    [],
                    sql_path,
                    repo_root,
                    done_dir,
                    timeout=1,
                    layer=0,
                    label_prefix="[TEST]",
                    error_entries=errors,
                    error_limit=10,
                    max_sql_file_bytes=None,
                )

            self.assertEqual(result.status, "FAILED")
            self.assertEqual(summary.statements, 1)
            self.assertEqual(removed, 0)
            self.assertEqual(kept, 1)
            self.assertFalse(truncated)
            self.assertEqual(list(sql_path.parent.glob("*.tmp")), [])


class TestPriorityOrder(unittest.TestCase):
    def test_collect_sql_files_by_layer_non_smart_constraints_after_view(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup"
            (fixup_dir / "view").mkdir(parents=True)
            (fixup_dir / "constraint").mkdir(parents=True)
            view_sql = fixup_dir / "view" / "A.V1.sql"
            cons_sql = fixup_dir / "constraint" / "A.T1.C1.sql"
            view_sql.write_text("CREATE VIEW A.V1 AS SELECT 1 FROM DUAL;", encoding="utf-8")
            cons_sql.write_text("ALTER TABLE A.T1 ADD CONSTRAINT C1 CHECK (1=1);", encoding="utf-8")

            files = rf.collect_sql_files_by_layer(fixup_dir, smart_order=False)
            order = [str(path.relative_to(fixup_dir)) for _, path in files]
            self.assertLess(order.index("view/A.V1.sql"), order.index("constraint/A.T1.C1.sql"))

    def test_collect_sql_files_by_layer_name_collision_before_constraint_and_index(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup"
            (fixup_dir / "name_collision").mkdir(parents=True)
            (fixup_dir / "constraint").mkdir(parents=True)
            (fixup_dir / "index").mkdir(parents=True)
            nc_sql = fixup_dir / "name_collision" / "A.phase1_temp_rename.sql"
            cons_sql = fixup_dir / "constraint" / "A.T1.C1.sql"
            idx_sql = fixup_dir / "index" / "A.I1.sql"
            nc_sql.write_text("ALTER TABLE A.T1 RENAME CONSTRAINT C1 TO C2;", encoding="utf-8")
            cons_sql.write_text("ALTER TABLE A.T1 ADD CONSTRAINT C1 CHECK (1=1);", encoding="utf-8")
            idx_sql.write_text("CREATE INDEX A.I1 ON A.T1(C1);", encoding="utf-8")

            files = rf.collect_sql_files_by_layer(fixup_dir, smart_order=False)
            order = [str(path.relative_to(fixup_dir)) for _, path in files]
            self.assertLess(order.index("name_collision/A.phase1_temp_rename.sql"), order.index("constraint/A.T1.C1.sql"))
            self.assertLess(order.index("name_collision/A.phase1_temp_rename.sql"), order.index("index/A.I1.sql"))


class TestConnectivityChecks(unittest.TestCase):
    def test_run_iterative_fixup_exits_on_connectivity_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup_scripts"
            fixup_dir.mkdir(parents=True)
            report_dir = root / "reports"
            report_dir.mkdir(parents=True)
            args = SimpleNamespace(
                config=str(root / "config.ini"),
                smart_order=False,
                glob_patterns=None,
                recompile=False,
                max_retries=1,
            )
            ob_cfg = {
                "executable": "obclient",
                "host": "127.0.0.1",
                "port": "2881",
                "user_string": "root@sys",
                "password": "p",
                "timeout": 10,
            }
            fixup_settings = rf.FixupAutoGrantSettings(
                enabled=False, types=set(), fallback=False, cache_limit=100
            )

            with mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]), \
                 mock.patch.object(rf, "resolve_timeout_value", return_value=10), \
                 mock.patch.object(rf, "check_obclient_connectivity", return_value=(False, "connect fail")):
                with self.assertRaises(SystemExit) as cm:
                    rf.run_iterative_fixup(
                        args, ob_cfg, fixup_dir, root, report_dir,
                        only_dirs=[], exclude_dirs=[],
                        fixup_settings=fixup_settings,
                        max_sql_file_bytes=None,
                        max_rounds=1, min_progress=1,
                    )
            self.assertEqual(cm.exception.code, 1)

    def test_run_view_chain_autofix_exits_on_connectivity_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup_scripts"
            fixup_dir.mkdir(parents=True)
            report_dir = root / "reports"
            report_dir.mkdir(parents=True)
            chain = report_dir / "VIEWs_chain_20260101_000000.txt"
            chain.write_text("dummy", encoding="utf-8")
            args = SimpleNamespace(
                config=str(root / "config.ini"),
                smart_order=False,
                glob_patterns=None,
                recompile=False,
                max_retries=1,
            )
            ob_cfg = {
                "executable": "obclient",
                "host": "127.0.0.1",
                "port": "2881",
                "user_string": "root@sys",
                "password": "p",
                "timeout": 10,
            }
            fixup_settings = rf.FixupAutoGrantSettings(
                enabled=False, types=set(), fallback=False, cache_limit=100
            )

            with mock.patch.object(rf, "find_latest_view_chain_file", return_value=chain), \
                 mock.patch.object(rf, "parse_view_chain_file", return_value={"A.V1": [[("A.V1", "VIEW"), ("A.T1", "TABLE")]]}), \
                 mock.patch.object(rf, "collect_sql_files_by_layer", return_value=[]), \
                 mock.patch.object(rf, "collect_sql_files_from_root", return_value=[]), \
                 mock.patch.object(rf, "build_fixup_object_index", return_value=({}, {})), \
                 mock.patch.object(rf, "build_grant_index", return_value=rf.GrantIndex({}, {}, {})), \
                 mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]), \
                 mock.patch.object(rf, "resolve_timeout_value", return_value=10), \
                 mock.patch.object(rf, "check_obclient_connectivity", return_value=(False, "connect fail")):
                with self.assertRaises(SystemExit) as cm:
                    rf.run_view_chain_autofix(
                        args, ob_cfg, fixup_dir, root, report_dir,
                        only_dirs=[], exclude_dirs=[],
                        fixup_settings=fixup_settings,
                        max_sql_file_bytes=None,
                    )
            self.assertEqual(cm.exception.code, 1)


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
                 mock.patch.object(rf, "check_obclient_connectivity", return_value=(True, "")), \
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

    def test_failed_path_cleared_after_later_success(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            fixup_dir = repo_root / "fixup_scripts"
            (fixup_dir / "table").mkdir(parents=True)
            report_dir = repo_root / "reports"
            report_dir.mkdir()

            f1 = fixup_dir / "table" / "A.T1.sql"
            f2 = fixup_dir / "table" / "A.T2.sql"
            f1.write_text("SELECT 1;", encoding="utf-8")
            f2.write_text("SELECT 1;", encoding="utf-8")

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
                [(0, f1)],
            ]
            state = {"attempt": 0}

            def fake_collect(*_a, **_k):
                if rounds:
                    return rounds.pop(0)
                return []

            def fake_exec(_cmd, path, *_a, **_k):
                if path == f1 and state["attempt"] == 0:
                    state["attempt"] += 1
                    failure = rf.StatementFailure(1, "ERR", "SELECT 1")
                    return rf.ScriptResult(path, "FAILED", "ERR"), rf.ExecutionSummary(1, [failure])
                return rf.ScriptResult(path, "SUCCESS"), rf.ExecutionSummary(1, [])

            with mock.patch.object(rf, "collect_sql_files_by_layer", side_effect=fake_collect), \
                 mock.patch.object(rf, "execute_script_with_summary", side_effect=fake_exec), \
                 mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]), \
                 mock.patch.object(rf, "resolve_timeout_value", return_value=None), \
                 mock.patch.object(rf, "check_obclient_connectivity", return_value=(True, "")), \
                 mock.patch.object(rf, "build_fixup_object_index", return_value=({}, {})):
                with self.assertRaises(SystemExit) as cm:
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
            self.assertEqual(cm.exception.code, 0)

    def test_min_progress_stop_without_zero_success_skips_failure_analysis(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            fixup_dir = repo_root / "fixup_scripts"
            (fixup_dir / "table").mkdir(parents=True)
            report_dir = repo_root / "reports"
            report_dir.mkdir()
            f1 = fixup_dir / "table" / "A.T1.sql"
            f1.write_text("SELECT 1;", encoding="utf-8")

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
            fixup_settings = rf.FixupAutoGrantSettings(False, set(), False, 0)

            def fake_collect(*_a, **_k):
                return [(0, f1)]

            def fake_exec(_cmd, path, *_a, **_k):
                self.assertEqual(path, f1)
                return rf.ScriptResult(path, "SUCCESS"), rf.ExecutionSummary(1, [])

            def fake_analyze(results):
                if any(item.status == "FAILED" for item in results):
                    return {"UNKNOWN": 1}
                return {}

            with mock.patch.object(rf, "collect_sql_files_by_layer", side_effect=fake_collect), \
                 mock.patch.object(rf, "execute_script_with_summary", side_effect=fake_exec), \
                 mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]), \
                 mock.patch.object(rf, "resolve_timeout_value", return_value=None), \
                 mock.patch.object(rf, "check_obclient_connectivity", return_value=(True, "")), \
                 mock.patch.object(rf, "build_fixup_object_index", return_value=({}, {})), \
                 mock.patch.object(rf, "analyze_failure_patterns", side_effect=fake_analyze) as m_analyze, \
                 mock.patch.object(rf, "log_failure_analysis") as m_log:
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
                        max_rounds=1,
                        min_progress=2
                    )
            self.assertTrue(m_analyze.called)
            self.assertFalse(m_log.called)

    def test_zero_success_stop_emits_failure_analysis(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            fixup_dir = repo_root / "fixup_scripts"
            (fixup_dir / "table").mkdir(parents=True)
            report_dir = repo_root / "reports"
            report_dir.mkdir()
            f1 = fixup_dir / "table" / "A.T1.sql"
            f1.write_text("SELECT 1;", encoding="utf-8")

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
            fixup_settings = rf.FixupAutoGrantSettings(False, set(), False, 0)

            def fake_collect(*_a, **_k):
                return [(0, f1)]

            def fake_exec(_cmd, path, *_a, **_k):
                self.assertEqual(path, f1)
                failure = rf.StatementFailure(1, "ERR", "SELECT 1")
                return rf.ScriptResult(path, "FAILED", "ERR"), rf.ExecutionSummary(1, [failure])

            def fake_analyze(results):
                if any(item.status == "FAILED" for item in results):
                    return {"UNKNOWN": 1}
                return {}

            with mock.patch.object(rf, "collect_sql_files_by_layer", side_effect=fake_collect), \
                 mock.patch.object(rf, "execute_script_with_summary", side_effect=fake_exec), \
                 mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]), \
                 mock.patch.object(rf, "resolve_timeout_value", return_value=None), \
                 mock.patch.object(rf, "check_obclient_connectivity", return_value=(True, "")), \
                 mock.patch.object(rf, "build_fixup_object_index", return_value=({}, {})), \
                 mock.patch.object(rf, "analyze_failure_patterns", side_effect=fake_analyze) as m_analyze, \
                 mock.patch.object(rf, "log_failure_analysis") as m_log:
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
                        max_rounds=1,
                        min_progress=2
                    )
            self.assertTrue(m_analyze.called)
            self.assertTrue(m_log.called)

    def test_iterative_prunes_missing_failed_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            fixup_dir = repo_root / "fixup_scripts"
            (fixup_dir / "table").mkdir(parents=True)
            report_dir = repo_root / "reports"
            report_dir.mkdir()
            f1 = fixup_dir / "table" / "A.T1.sql"
            f2 = fixup_dir / "table" / "A.T2.sql"
            f1.write_text("SELECT 1;", encoding="utf-8")
            f2.write_text("SELECT 1;", encoding="utf-8")

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
            fixup_settings = rf.FixupAutoGrantSettings(False, set(), False, 0)
            calls = {"collect": 0}

            def fake_collect(*_a, **_k):
                calls["collect"] += 1
                if calls["collect"] == 1:
                    return [(0, f1), (0, f2)]
                return []

            def fake_exec(_cmd, path, *_a, **_k):
                if path == f1:
                    f1.unlink(missing_ok=True)
                    failure = rf.StatementFailure(1, "ERR", "SELECT 1")
                    return rf.ScriptResult(path, "FAILED", "ERR"), rf.ExecutionSummary(1, [failure])
                return rf.ScriptResult(path, "SUCCESS"), rf.ExecutionSummary(1, [])

            with mock.patch.object(rf, "collect_sql_files_by_layer", side_effect=fake_collect), \
                 mock.patch.object(rf, "execute_script_with_summary", side_effect=fake_exec), \
                 mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]), \
                 mock.patch.object(rf, "resolve_timeout_value", return_value=None), \
                 mock.patch.object(rf, "check_obclient_connectivity", return_value=(True, "")), \
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
            self.assertIn("历史失败脚本已不存在", joined)

    def test_iterative_keeps_existing_failed_relative_paths_outside_repo_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            fixup_dir = repo_root / "fixup_scripts"
            (fixup_dir / "table").mkdir(parents=True)
            report_dir = repo_root / "reports"
            report_dir.mkdir()
            f1 = fixup_dir / "table" / "A.T1.sql"
            f2 = fixup_dir / "table" / "A.T2.sql"
            f3 = fixup_dir / "table" / "A.T3.sql"
            f1.write_text("SELECT 1;", encoding="utf-8")
            f2.write_text("SELECT 1;", encoding="utf-8")
            f3.write_text("SELECT 1;", encoding="utf-8")

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
            fixup_settings = rf.FixupAutoGrantSettings(False, set(), False, 0)
            rounds = [
                [(0, f1), (0, f2)],
                [(0, f3)],
            ]

            def fake_collect(*_a, **_k):
                if rounds:
                    return rounds.pop(0)
                return []

            def fake_exec(_cmd, path, repo_root_arg, *_a, **_k):
                rel = path.relative_to(repo_root_arg)
                if path == f2:
                    return rf.ScriptResult(rel, "SUCCESS"), rf.ExecutionSummary(1, [])
                failure = rf.StatementFailure(1, "ERR", "SELECT 1")
                return rf.ScriptResult(rel, "FAILED", "ERR"), rf.ExecutionSummary(1, [failure])

            cwd = Path.cwd()
            try:
                os.chdir("/")
                with mock.patch.object(rf, "collect_sql_files_by_layer", side_effect=fake_collect), \
                     mock.patch.object(rf, "execute_script_with_summary", side_effect=fake_exec), \
                     mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]), \
                     mock.patch.object(rf, "resolve_timeout_value", return_value=None), \
                     mock.patch.object(rf, "check_obclient_connectivity", return_value=(True, "")), \
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
            finally:
                os.chdir(cwd)

            joined = "\n".join(cm.output)
            self.assertIn("累计失败: 2", joined)
            self.assertNotIn("历史失败脚本已不存在", joined)


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


class TestTableCreateSafety(unittest.TestCase):
    def _build_args(self, config_path: Path, **overrides):
        data = {
            "config": str(config_path),
            "smart_order": False,
            "recompile": False,
            "max_retries": 1,
            "iterative": False,
            "max_rounds": 1,
            "min_progress": 1,
            "view_chain_autofix": False,
            "only_dirs": None,
            "exclude_dirs": None,
            "only_types": None,
            "glob_patterns": None,
            "allow_table_create": False,
        }
        data.update(overrides)
        return SimpleNamespace(**data)

    def test_main_default_excludes_table_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup_scripts"
            fixup_dir.mkdir(parents=True)
            report_dir = root / "main_reports"
            report_dir.mkdir(parents=True)
            cfg = root / "config.ini"
            cfg.write_text("", encoding="utf-8")
            args = self._build_args(cfg)

            captured = {}

            def fake_run_single(_args, _ob_cfg, _fixup_dir, _repo_root, _report_dir, only_dirs, exclude_dirs, _fixup_settings, _max_sql_file_bytes):
                captured["only_dirs"] = only_dirs
                captured["exclude_dirs"] = exclude_dirs
                raise SystemExit(0)

            with mock.patch.object(rf, "parse_args", return_value=args), \
                 mock.patch.object(
                     rf,
                     "load_ob_config",
                     return_value=(
                         {"executable": "obclient", "host": "127.0.0.1", "port": "2881", "user_string": "u@tenant", "password": "p"},
                         fixup_dir,
                         root,
                         "INFO",
                         report_dir,
                         rf.FixupAutoGrantSettings(False, set(), False, 100),
                         None,
                     ),
                 ), \
                 mock.patch.object(rf, "resolve_console_log_level", return_value=logging.INFO), \
                 mock.patch.object(rf, "set_console_log_level"), \
                 mock.patch.object(rf, "run_single_fixup", side_effect=fake_run_single):
                with self.assertRaises(SystemExit) as cm:
                    rf.main()

            self.assertEqual(cm.exception.code, 0)
            self.assertIn("table", captured["exclude_dirs"])
            self.assertIn("sequence_restart", captured["exclude_dirs"])

    def test_main_allow_table_create_removes_default_table_exclude(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup_scripts"
            fixup_dir.mkdir(parents=True)
            report_dir = root / "main_reports"
            report_dir.mkdir(parents=True)
            cfg = root / "config.ini"
            cfg.write_text("", encoding="utf-8")
            args = self._build_args(cfg, allow_table_create=True)

            captured = {}

            def fake_run_single(_args, _ob_cfg, _fixup_dir, _repo_root, _report_dir, only_dirs, exclude_dirs, _fixup_settings, _max_sql_file_bytes):
                captured["only_dirs"] = only_dirs
                captured["exclude_dirs"] = exclude_dirs
                raise SystemExit(0)

            with mock.patch.object(rf, "parse_args", return_value=args), \
                 mock.patch.object(
                     rf,
                     "load_ob_config",
                     return_value=(
                         {"executable": "obclient", "host": "127.0.0.1", "port": "2881", "user_string": "u@tenant", "password": "p"},
                         fixup_dir,
                         root,
                         "INFO",
                         report_dir,
                         rf.FixupAutoGrantSettings(False, set(), False, 100),
                         None,
                     ),
                 ), \
                 mock.patch.object(rf, "resolve_console_log_level", return_value=logging.INFO), \
                 mock.patch.object(rf, "set_console_log_level"), \
                 mock.patch.object(rf, "run_single_fixup", side_effect=fake_run_single):
                with self.assertRaises(SystemExit) as cm:
                    rf.main()

            self.assertEqual(cm.exception.code, 0)
            self.assertNotIn("table", captured["exclude_dirs"])
            self.assertIn("sequence_restart", captured["exclude_dirs"])

    def test_main_only_dirs_sequence_restart_removes_default_sequence_restart_exclude(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup_scripts"
            fixup_dir.mkdir(parents=True)
            report_dir = root / "main_reports"
            report_dir.mkdir(parents=True)
            cfg = root / "config.ini"
            cfg.write_text("", encoding="utf-8")
            args = self._build_args(cfg, only_dirs=["sequence_restart"])

            captured = {}

            def fake_run_single(_args, _ob_cfg, _fixup_dir, _repo_root, _report_dir, only_dirs, exclude_dirs, _fixup_settings, _max_sql_file_bytes):
                captured["only_dirs"] = only_dirs
                captured["exclude_dirs"] = exclude_dirs
                raise SystemExit(0)

            with mock.patch.object(rf, "parse_args", return_value=args), \
                 mock.patch.object(
                     rf,
                     "load_ob_config",
                     return_value=(
                         {"executable": "obclient", "host": "127.0.0.1", "port": "2881", "user_string": "u@tenant", "password": "p"},
                         fixup_dir,
                         root,
                         "INFO",
                         report_dir,
                         rf.FixupAutoGrantSettings(False, set(), False, 100),
                         None,
                     ),
                 ), \
                 mock.patch.object(rf, "resolve_console_log_level", return_value=logging.INFO), \
                 mock.patch.object(rf, "set_console_log_level"), \
                 mock.patch.object(rf, "run_single_fixup", side_effect=fake_run_single):
                with self.assertRaises(SystemExit) as cm:
                    rf.main()

            self.assertEqual(cm.exception.code, 0)
            self.assertIn("sequence_restart", captured["only_dirs"])
            self.assertNotIn("sequence_restart", captured["exclude_dirs"])

    def test_main_only_dirs_table_still_blocked_without_allow_flag(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup_scripts"
            fixup_dir.mkdir(parents=True)
            report_dir = root / "main_reports"
            report_dir.mkdir(parents=True)
            cfg = root / "config.ini"
            cfg.write_text("", encoding="utf-8")
            args = self._build_args(cfg, only_dirs=["table"])

            captured = {}

            def fake_run_single(_args, _ob_cfg, _fixup_dir, _repo_root, _report_dir, only_dirs, exclude_dirs, _fixup_settings, _max_sql_file_bytes):
                captured["only_dirs"] = only_dirs
                captured["exclude_dirs"] = exclude_dirs
                raise SystemExit(0)

            with mock.patch.object(rf, "parse_args", return_value=args), \
                 mock.patch.object(
                     rf,
                     "load_ob_config",
                     return_value=(
                         {"executable": "obclient", "host": "127.0.0.1", "port": "2881", "user_string": "u@tenant", "password": "p"},
                         fixup_dir,
                         root,
                         "INFO",
                         report_dir,
                         rf.FixupAutoGrantSettings(False, set(), False, 100),
                         None,
                     ),
                 ), \
                 mock.patch.object(rf, "resolve_console_log_level", return_value=logging.INFO), \
                 mock.patch.object(rf, "set_console_log_level"), \
                 mock.patch.object(rf, "run_single_fixup", side_effect=fake_run_single):
                with self.assertRaises(SystemExit) as cm:
                    rf.main()

            self.assertEqual(cm.exception.code, 0)
            self.assertIn("table", captured["only_dirs"])
            self.assertIn("table", captured["exclude_dirs"])


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

        def fake_query(_cmd, _timeout, allowed_owners=None):
            self.assertIsNone(allowed_owners)
            return invalid_batches.pop(0)

        executed = []

        def fake_run_sql(_cmd, sql, _timeout):
            executed.append(sql)
            return SimpleNamespace(returncode=0, stderr="", stdout="")

        with mock.patch.object(rf, "query_invalid_objects", side_effect=fake_query), \
             mock.patch.object(rf, "run_sql", side_effect=fake_run_sql), \
             mock.patch.object(rf, "is_object_invalid", return_value=False):
            summary = rf.recompile_invalid_objects([], timeout=1, max_retries=2)

        self.assertEqual(summary.total_recompiled, 1)
        self.assertEqual(summary.remaining_invalid, 0)
        self.assertEqual(summary.recompile_failed, 0)
        self.assertEqual(summary.unsupported_types, 2)
        self.assertEqual(len(executed), 1)
        self.assertIn('ALTER PROCEDURE "APP"."P1" COMPILE;', executed[0])

    def test_build_compile_statement_quotes_identifiers(self):
        sql = rf.build_compile_statement("AppOwner", "Pkg.Name", "PACKAGE BODY")
        self.assertEqual(sql, 'ALTER PACKAGE "AppOwner"."Pkg.Name" COMPILE BODY;')

    def test_run_sql_uses_utf8_ignore(self):
        captured = {}

        def fake_subprocess_run(*args, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(returncode=0, stderr="", stdout="")

        with mock.patch.object(rf.subprocess, "run", side_effect=fake_subprocess_run):
            rf.run_sql(["obclient"], "SELECT 1", 3)

        self.assertEqual(captured.get("encoding"), "utf-8")
        self.assertEqual(captured.get("errors"), "ignore")

    def test_recompile_treats_stdout_ora_as_failure(self):
        invalid_batches = [
            [("APP", "P1", "PROCEDURE")],
            [],
        ]

        def fake_query(_cmd, _timeout, allowed_owners=None):
            self.assertIsNone(allowed_owners)
            return invalid_batches.pop(0)

        def fake_run_sql(_cmd, _sql, _timeout):
            return SimpleNamespace(returncode=0, stderr="", stdout="ORA-00900: invalid SQL statement")

        with mock.patch.object(rf, "query_invalid_objects", side_effect=fake_query), \
             mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            summary = rf.recompile_invalid_objects([], timeout=1, max_retries=1)

        self.assertEqual(summary.total_recompiled, 0)
        self.assertEqual(summary.remaining_invalid, 0)
        self.assertEqual(summary.recompile_failed, 1)

    def test_recompile_not_counted_when_object_still_invalid(self):
        invalid_batches = [
            [("APP", "P1", "PROCEDURE")],
            [("APP", "P1", "PROCEDURE")],
        ]

        def fake_query(_cmd, _timeout, allowed_owners=None):
            self.assertIsNone(allowed_owners)
            return invalid_batches.pop(0)

        def fake_run_sql(_cmd, _sql, _timeout):
            return SimpleNamespace(returncode=0, stderr="", stdout="")

        with mock.patch.object(rf, "query_invalid_objects", side_effect=fake_query), \
             mock.patch.object(rf, "run_sql", side_effect=fake_run_sql), \
             mock.patch.object(rf, "is_object_invalid", return_value=True):
            summary = rf.recompile_invalid_objects([], timeout=1, max_retries=1)

        self.assertEqual(summary.total_recompiled, 0)
        self.assertEqual(summary.remaining_invalid, 1)
        self.assertEqual(summary.recompile_failed, 1)

    def test_is_object_invalid_uses_cache_when_provided(self):
        cache = {}
        with mock.patch.object(rf, 'query_count', return_value=1) as query_count:
            self.assertTrue(rf.is_object_invalid([], 1, 'APP', 'P1', 'PROCEDURE', cache=cache))
            self.assertTrue(rf.is_object_invalid([], 1, 'APP', 'P1', 'PROCEDURE', cache=cache))
        query_count.assert_called_once()

    def test_recompile_retries_after_all_fail_round(self):
        invalid_batches = [
            [("APP", "P1", "PROCEDURE")],
            [("APP", "P1", "PROCEDURE")],
            [],
        ]
        run_results = [
            SimpleNamespace(returncode=0, stderr="", stdout="ORA-03113: end-of-file on communication channel"),
            SimpleNamespace(returncode=0, stderr="", stdout=""),
        ]

        def fake_query(_cmd, _timeout, allowed_owners=None):
            self.assertIsNone(allowed_owners)
            return invalid_batches.pop(0)

        with mock.patch.object(rf, "query_invalid_objects", side_effect=fake_query),              mock.patch.object(rf, "run_sql", side_effect=run_results),              mock.patch.object(rf, "is_object_invalid", return_value=False):
            summary = rf.recompile_invalid_objects([], timeout=1, max_retries=2)

        self.assertEqual(summary.total_recompiled, 1)
        self.assertEqual(summary.remaining_invalid, 0)
        self.assertEqual(summary.recompile_failed, 1)

    def test_score_execution_error_line_ignores_ora_06512_stack_frames(self):
        self.assertIsNone(rf.score_execution_error_line("ORA-06512: at package body APP.PKG, line 12"))

    def test_read_sql_text_with_limit_replaces_invalid_utf8_bytes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_path = Path(tmpdir) / "bad.sql"
            sql_path.write_bytes(b"SELECT '\xff';\n")
            text, err = rf.read_sql_text_with_limit(sql_path, None)
        self.assertIsNone(err)
        self.assertIsNotNone(text)
        self.assertIn("�", text)

    def test_parse_view_chain_file_replaces_invalid_utf8_bytes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "VIEWs_chain_1.txt"
            path.write_bytes(b"1. A.V1[VIEW] -> A.T1[TABLE]\n# bad:\xff\n")
            parsed = rf.parse_view_chain_file(path)
        self.assertIn("A.V1", parsed)

    def test_build_grant_index_replaces_invalid_utf8_bytes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixup_dir = Path(tmpdir)
            grant_dir = fixup_dir / "grants_miss"
            grant_dir.mkdir(parents=True)
            grant_file = grant_dir / "APP.grants.sql"
            grant_file.write_bytes(b"GRANT SELECT ON APP.T1 TO U1;\n-- \xff\n")
            index = rf.build_grant_index(fixup_dir, set(), include_dirs={"grants_miss"})
        self.assertIn("APP.T1", index.by_object)


class TestSqlCollectionRecursive(unittest.TestCase):
    def test_collect_sql_files_by_layer_includes_nested_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup"
            (fixup_dir / "table_alter").mkdir(parents=True)
            (fixup_dir / "table_alter" / "interval_add_20280301").mkdir(parents=True)
            (fixup_dir / "table_alter" / "A.T1.sql").write_text("SELECT 1;", encoding="utf-8")
            nested = fixup_dir / "table_alter" / "interval_add_20280301" / "A.T2.sql"
            nested.write_text("SELECT 2;", encoding="utf-8")

            files = rf.collect_sql_files_by_layer(fixup_dir, smart_order=False)
            rels = {str(path.relative_to(fixup_dir)) for _, path in files}
            self.assertIn("table_alter/A.T1.sql", rels)
            self.assertIn("table_alter/interval_add_20280301/A.T2.sql", rels)

    def test_collect_sql_files_view_grant_dirs_not_duplicated(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup"
            (fixup_dir / "view_prereq_grants").mkdir(parents=True)
            (fixup_dir / "view_refresh").mkdir(parents=True)
            (fixup_dir / "view_post_grants").mkdir(parents=True)
            (fixup_dir / "grants_miss").mkdir(parents=True)
            f1 = fixup_dir / "view_prereq_grants" / "A.grants.sql"
            f2 = fixup_dir / "view_refresh" / "A.V1.sql"
            f3 = fixup_dir / "view_post_grants" / "A.grants.sql"
            f4 = fixup_dir / "grants_miss" / "A.grants.sql"
            for path in (f1, f2, f3, f4):
                path.write_text("GRANT SELECT ON A.T1 TO A;", encoding="utf-8")

            files = rf.collect_sql_files_by_layer(fixup_dir, smart_order=True)
            rels = [str(path.relative_to(fixup_dir)) for _, path in files]
            self.assertEqual(rels.count("view_prereq_grants/A.grants.sql"), 1)
            self.assertEqual(rels.count("view_refresh/A.V1.sql"), 1)
            self.assertEqual(rels.count("view_post_grants/A.grants.sql"), 1)
            self.assertEqual(rels.count("grants_miss/A.grants.sql"), 1)
            self.assertLess(rels.index("view_prereq_grants/A.grants.sql"), rels.index("view_refresh/A.V1.sql"))
            self.assertLess(rels.index("view_refresh/A.V1.sql"), rels.index("view_post_grants/A.grants.sql"))

    def test_collect_sql_files_by_layer_supports_nested_only_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup"
            (fixup_dir / "status" / "constraint").mkdir(parents=True)
            (fixup_dir / "status" / "trigger").mkdir(parents=True)
            wanted = fixup_dir / "status" / "constraint" / "A.FK_T1.status.sql"
            skipped = fixup_dir / "status" / "trigger" / "A.TRG_T1.status.sql"
            wanted.write_text("ALTER TABLE A.T1 ENABLE VALIDATE CONSTRAINT FK_T1;", encoding="utf-8")
            skipped.write_text("ALTER TRIGGER A.TRG_T1 ENABLE;", encoding="utf-8")

            files = rf.collect_sql_files_by_layer(
                fixup_dir,
                smart_order=False,
                include_dirs={"status/constraint"}
            )
            rels = [str(path.relative_to(fixup_dir)) for _, path in files]
            self.assertEqual(rels, ["status/constraint/A.FK_T1.status.sql"])

    def test_collect_sql_files_from_root_supports_nested_only_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            done_dir = root / "done"
            (done_dir / "status" / "constraint").mkdir(parents=True)
            (done_dir / "status" / "trigger").mkdir(parents=True)
            wanted = done_dir / "status" / "constraint" / "A.FK_T1.status.sql"
            skipped = done_dir / "status" / "trigger" / "A.TRG_T1.status.sql"
            wanted.write_text("ALTER TABLE A.T1 ENABLE VALIDATE CONSTRAINT FK_T1;", encoding="utf-8")
            skipped.write_text("ALTER TRIGGER A.TRG_T1 ENABLE;", encoding="utf-8")

            files = rf.collect_sql_files_from_root(
                done_dir,
                include_dirs={"status/constraint"}
            )
            rels = [str(path.relative_to(done_dir)) for path in files]
            self.assertEqual(rels, ["status/constraint/A.FK_T1.status.sql"])

    def test_collect_sql_files_by_layer_allows_explicit_cleanup_safe_nested_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup"
            (fixup_dir / "cleanup_safe" / "constraint").mkdir(parents=True)
            target = fixup_dir / "cleanup_safe" / "constraint" / "A.NN_T1.drop.sql"
            target.write_text("ALTER TABLE A.T1 DROP CONSTRAINT NN_T1;", encoding="utf-8")

            files = rf.collect_sql_files_by_layer(
                fixup_dir,
                smart_order=False,
                include_dirs={"cleanup_safe/constraint"}
            )
            rels = [str(path.relative_to(fixup_dir)) for _, path in files]
            self.assertEqual(rels, ["cleanup_safe/constraint/A.NN_T1.drop.sql"])

    def test_collect_sql_files_by_layer_allows_explicit_cleanup_semantic_nested_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixup_dir = root / "fixup"
            (fixup_dir / "cleanup_semantic" / "constraint").mkdir(parents=True)
            target = fixup_dir / "cleanup_semantic" / "constraint" / "A.CK_T1_X.drop.sql"
            target.write_text("ALTER TABLE A.T1 DROP CONSTRAINT CK_T1_X;", encoding="utf-8")

            files = rf.collect_sql_files_by_layer(
                fixup_dir,
                smart_order=False,
                include_dirs={"cleanup_semantic/constraint"}
            )
            rels = [str(path.relative_to(fixup_dir)) for _, path in files]
            self.assertEqual(rels, ["cleanup_semantic/constraint/A.CK_T1_X.drop.sql"])


class TestInvalidObjectScope(unittest.TestCase):
    def test_query_invalid_objects_filters_owner(self):
        captured = {}

        def fake_run_sql(_cmd, sql, _timeout):
            captured["sql"] = sql
            return SimpleNamespace(returncode=0, stderr="", stdout="")

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            rows = rf.query_invalid_objects([], timeout=1, allowed_owners={"ZZ_APP", "zz_fin"})
        self.assertEqual(rows, [])
        self.assertIn("OWNER IN ('ZZ_APP', 'ZZ_FIN')", captured["sql"])

    def test_query_invalid_objects_skips_obclient_header_row(self):
        def fake_run_sql(_cmd, _sql, _timeout):
            return SimpleNamespace(
                returncode=0,
                stderr="",
                stdout="OWNER\tOBJECT_NAME\tOBJECT_TYPE\nAPP\tP1\tPROCEDURE\n",
            )

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            rows = rf.query_invalid_objects([], timeout=1)
        self.assertEqual(rows, [("APP", "P1", "PROCEDURE")])

    def test_query_invalid_objects_skips_warning_line_with_tabs(self):
        def fake_run_sql(_cmd, _sql, _timeout):
            return SimpleNamespace(
                returncode=0,
                stderr="",
                stdout=(
                    "Warning: Using\ta password\ton the command line interface can be insecure.\n"
                    "APP\tP1\tPROCEDURE\n"
                ),
            )

        with mock.patch.object(rf, "run_sql", side_effect=fake_run_sql):
            rows = rf.query_invalid_objects([], timeout=1)
        self.assertEqual(rows, [("APP", "P1", "PROCEDURE")])


class TestViewChainCacheRefresh(unittest.TestCase):
    def test_view_chain_mode_uses_state_ledger_skip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            fixup_dir = repo_root / "fixup_scripts"
            report_dir = repo_root / "main_reports"
            fixup_dir.mkdir(parents=True)
            report_dir.mkdir(parents=True)
            chain_file = report_dir / "VIEWs_chain_20260101_000000.txt"
            chain_file.write_text("dummy", encoding="utf-8")

            args = SimpleNamespace(
                config=str(repo_root / "config.ini"),
                smart_order=False,
                glob_patterns=None,
            )
            ob_cfg = {
                "executable": "obclient",
                "host": "127.0.0.1",
                "port": "2881",
                "user_string": "root@sys",
                "password": "p",
                "timeout": 10,
            }
            fixup_settings = rf.FixupAutoGrantSettings(
                enabled=False,
                types=set(),
                fallback=False,
                cache_limit=100
            )
            fake_ledger = mock.Mock()
            fake_ledger.is_completed.return_value = True

            with mock.patch.object(rf, "find_latest_view_chain_file", return_value=chain_file), \
                 mock.patch.object(rf, "parse_view_chain_file", return_value={"A.V1": [[("A.V1", "VIEW"), ("A.T1", "TABLE")]]}), \
                 mock.patch.object(rf, "collect_sql_files_by_layer", return_value=[]), \
                 mock.patch.object(rf, "collect_sql_files_from_root", return_value=[]), \
                 mock.patch.object(rf, "build_fixup_object_index", return_value=({}, {})), \
                 mock.patch.object(rf, "build_grant_index", return_value=rf.GrantIndex({}, {}, {})), \
                 mock.patch.object(rf, "build_view_chain_plan", return_value=(["OK"], ["CREATE VIEW A.V1 AS SELECT 1 FROM DUAL;"], False)), \
                 mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]), \
                 mock.patch.object(rf, "resolve_timeout_value", return_value=10), \
                 mock.patch.object(rf, "check_obclient_connectivity", return_value=(True, "")), \
                 mock.patch.object(rf, "check_object_exists", return_value=False), \
                 mock.patch.object(rf, "build_fixup_precheck_summary", return_value=mock.Mock()), \
                 mock.patch.object(rf, "write_fixup_precheck_report", return_value=None), \
                 mock.patch.object(rf, "log_fixup_precheck", return_value=None), \
                 mock.patch.object(rf, "execute_sql_statements") as m_exec, \
                 mock.patch.object(rf, "FixupStateLedger", return_value=fake_ledger):
                with self.assertRaises(SystemExit) as cm:
                    rf.run_view_chain_autofix(
                        args,
                        ob_cfg,
                        fixup_dir,
                        repo_root,
                        report_dir,
                        [],
                        [],
                        fixup_settings,
                        None
                    )

            self.assertEqual(cm.exception.code, 0)
            m_exec.assert_not_called()
            fake_ledger.is_completed.assert_called()
            fake_ledger.flush.assert_called_once()

    def test_view_chain_post_check_refreshes_exists_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            fixup_dir = repo_root / "fixup_scripts"
            report_dir = repo_root / "main_reports"
            fixup_dir.mkdir(parents=True)
            report_dir.mkdir(parents=True)
            chain_file = report_dir / "VIEWs_chain_20260101_000000.txt"
            chain_file.write_text("dummy", encoding="utf-8")

            args = SimpleNamespace(
                config=str(repo_root / "config.ini"),
                smart_order=False,
                glob_patterns=None,
            )
            ob_cfg = {
                "executable": "obclient",
                "host": "127.0.0.1",
                "port": "2881",
                "user_string": "root@sys",
                "password": "p",
                "timeout": 10,
            }
            fixup_settings = rf.FixupAutoGrantSettings(
                enabled=False,
                types=set(),
                fallback=False,
                cache_limit=100
            )
            state = {"first": True}

            def fake_check(_cmd, _timeout, full_name, obj_type, exists_cache, _planned, use_planned=False):
                key = (full_name.upper(), obj_type.upper())
                if key in exists_cache:
                    return exists_cache[key]
                if state["first"]:
                    state["first"] = False
                    exists_cache[key] = False
                    return False
                return True

            with mock.patch.object(rf, "find_latest_view_chain_file", return_value=chain_file), \
                 mock.patch.object(rf, "parse_view_chain_file", return_value={"A.V1": [[("A.V1", "VIEW"), ("A.T1", "TABLE")]]}), \
                 mock.patch.object(rf, "collect_sql_files_by_layer", return_value=[]), \
                 mock.patch.object(rf, "collect_sql_files_from_root", return_value=[]), \
                 mock.patch.object(rf, "build_fixup_object_index", return_value=({}, {})), \
                 mock.patch.object(rf, "build_grant_index", return_value=rf.GrantIndex({}, {}, {})), \
                 mock.patch.object(rf, "build_view_chain_plan", return_value=(["OK"], ["CREATE VIEW A.V1 AS SELECT 1 FROM DUAL;"], False)), \
                 mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]), \
                 mock.patch.object(rf, "resolve_timeout_value", return_value=10), \
                 mock.patch.object(rf, "check_obclient_connectivity", return_value=(True, "")), \
                 mock.patch.object(rf, "check_object_exists", side_effect=fake_check), \
                 mock.patch.object(rf, "execute_sql_statements", return_value=rf.ExecutionSummary(1, [])):
                with self.assertRaises(SystemExit) as cm:
                    rf.run_view_chain_autofix(
                        args,
                        ob_cfg,
                        fixup_dir,
                        repo_root,
                        report_dir,
                        [],
                        [],
                        fixup_settings,
                        None
                    )
            self.assertEqual(cm.exception.code, 0)

    def test_view_chain_planned_objects_are_isolated_per_root_view(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            fixup_dir = repo_root / "fixup_scripts"
            report_dir = repo_root / "main_reports"
            fixup_dir.mkdir(parents=True)
            report_dir.mkdir(parents=True)
            chain_file = report_dir / "VIEWs_chain_20260101_000000.txt"
            chain_file.write_text("dummy", encoding="utf-8")

            args = SimpleNamespace(
                config=str(repo_root / "config.ini"),
                smart_order=False,
                glob_patterns=None,
            )
            ob_cfg = {
                "executable": "obclient",
                "host": "127.0.0.1",
                "port": "2881",
                "user_string": "root@sys",
                "password": "p",
                "timeout": 10,
            }
            fixup_settings = rf.FixupAutoGrantSettings(
                enabled=False,
                types=set(),
                fallback=False,
                cache_limit=100
            )
            initial_sizes: List[int] = []

            def fake_build_view_chain_plan(*bargs, **_kwargs):
                planned_objects = bargs[21]
                initial_sizes.append(len(planned_objects))
                planned_objects.add(("A.T1", "TABLE"))
                return (["BLOCK: test"], [], True)

            with mock.patch.object(rf, "find_latest_view_chain_file", return_value=chain_file), \
                 mock.patch.object(
                     rf,
                     "parse_view_chain_file",
                     return_value={
                         "A.V1": [[("A.V1", "VIEW"), ("A.T1", "TABLE")]],
                         "A.V2": [[("A.V2", "VIEW"), ("A.T2", "TABLE")]],
                     },
                 ), \
                 mock.patch.object(rf, "collect_sql_files_by_layer", return_value=[]), \
                 mock.patch.object(rf, "collect_sql_files_from_root", return_value=[]), \
                 mock.patch.object(rf, "build_fixup_object_index", return_value=({}, {})), \
                 mock.patch.object(rf, "build_grant_index", return_value=rf.GrantIndex({}, {}, {})), \
                 mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]), \
                 mock.patch.object(rf, "resolve_timeout_value", return_value=10), \
                 mock.patch.object(rf, "check_obclient_connectivity", return_value=(True, "")), \
                 mock.patch.object(rf, "check_object_exists", return_value=False), \
                 mock.patch.object(rf, "build_view_chain_plan", side_effect=fake_build_view_chain_plan):
                with self.assertRaises(SystemExit) as cm:
                    rf.run_view_chain_autofix(
                        args,
                        ob_cfg,
                        fixup_dir,
                        repo_root,
                        report_dir,
                        [],
                        [],
                        fixup_settings,
                        None
                    )
            self.assertEqual(cm.exception.code, 1)
            self.assertEqual(initial_sizes, [0, 0])

    def test_view_chain_exists_cache_isolated_per_root_view(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            fixup_dir = repo_root / "fixup_scripts"
            report_dir = repo_root / "main_reports"
            fixup_dir.mkdir(parents=True)
            report_dir.mkdir(parents=True)
            chain_file = report_dir / "VIEWs_chain_20260101_000000.txt"
            chain_file.write_text("dummy", encoding="utf-8")

            args = SimpleNamespace(
                config=str(repo_root / "config.ini"),
                smart_order=False,
                glob_patterns=None,
            )
            ob_cfg = {
                "executable": "obclient",
                "host": "127.0.0.1",
                "port": "2881",
                "user_string": "root@sys",
                "password": "p",
                "timeout": 10,
            }
            fixup_settings = rf.FixupAutoGrantSettings(
                enabled=False,
                types=set(),
                fallback=False,
                cache_limit=100,
            )
            shared_dep_queries = 0

            def fake_check(_cmd, _timeout, full_name, obj_type, exists_cache, _planned, use_planned=False):
                nonlocal shared_dep_queries
                key = (full_name.upper(), obj_type.upper())
                if key in exists_cache:
                    return exists_cache[key]
                if key == ("A.SHARED_T", "TABLE"):
                    shared_dep_queries += 1
                    exists_cache[key] = False
                    return False
                exists_cache[key] = False
                return False

            def fake_build_view_chain_plan(*bargs, **_kwargs):
                rf.check_object_exists(
                    ["obclient"],
                    10,
                    "A.SHARED_T",
                    "TABLE",
                    bargs[12],
                    bargs[21],
                )
                return (["BLOCK: test"], [], True)

            with mock.patch.object(rf, "find_latest_view_chain_file", return_value=chain_file),                  mock.patch.object(
                     rf,
                     "parse_view_chain_file",
                     return_value={
                         "A.V1": [[("A.V1", "VIEW"), ("A.SHARED_T", "TABLE")]],
                         "A.V2": [[("A.V2", "VIEW"), ("A.SHARED_T", "TABLE")]],
                     },
                 ),                  mock.patch.object(rf, "collect_sql_files_by_layer", return_value=[]),                  mock.patch.object(rf, "collect_sql_files_from_root", return_value=[]),                  mock.patch.object(rf, "build_fixup_object_index", return_value=({}, {})),                  mock.patch.object(rf, "build_grant_index", return_value=rf.GrantIndex({}, {}, {})),                  mock.patch.object(rf, "build_obclient_command", return_value=["obclient"]),                  mock.patch.object(rf, "resolve_timeout_value", return_value=10),                  mock.patch.object(rf, "check_obclient_connectivity", return_value=(True, "")),                  mock.patch.object(rf, "check_object_exists", side_effect=fake_check),                  mock.patch.object(rf, "build_view_chain_plan", side_effect=fake_build_view_chain_plan):
                with self.assertRaises(SystemExit):
                    rf.run_view_chain_autofix(
                        args,
                        ob_cfg,
                        fixup_dir,
                        repo_root,
                        report_dir,
                        [],
                        [],
                        fixup_settings,
                        None,
                    )

            self.assertEqual(shared_dep_queries, 2)


if __name__ == "__main__":
    unittest.main()
