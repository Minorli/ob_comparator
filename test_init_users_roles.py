import unittest
import tempfile
from pathlib import Path

import init_users_roles as iur


class TestInitUsersRoles(unittest.TestCase):
    def test_admin_option_clause(self) -> None:
        self.assertEqual(iur.admin_option_clause("YES"), " WITH ADMIN OPTION")
        self.assertEqual(iur.admin_option_clause("Y"), " WITH ADMIN OPTION")
        self.assertEqual(iur.admin_option_clause("NO"), "")
        self.assertEqual(iur.admin_option_clause(None), "")

    def test_grant_satisfied(self) -> None:
        existing = {("A", "B"): {"NO"}}
        self.assertTrue(iur.grant_satisfied(existing, "A", "B", False))
        self.assertFalse(iur.grant_satisfied(existing, "A", "B", True))
        existing[("A", "B")].add("YES")
        self.assertTrue(iur.grant_satisfied(existing, "a", "b", True))

    def test_format_identifier(self) -> None:
        self.assertEqual(iur.format_identifier("TEST"), "TEST")
        self.assertEqual(iur.format_identifier("Test"), "\"Test\"")

    def test_load_config_percent_password_and_timeout_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg_path = root / "config.ini"
            cfg_path.write_text(
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
                    "fixup_dir = fixup_scripts",
                    "obclient_timeout = 55",
                ]) + "\n",
                encoding="utf-8"
            )
            _ora_cfg, ob_cfg, _settings, output_dir, ob_timeout, ddl_timeout = iur.load_config(cfg_path)
            self.assertEqual(ob_cfg["password"], "p%w")
            self.assertEqual(ob_timeout, 55)
            self.assertEqual(ddl_timeout, 55)
            self.assertIn("fixup_scripts", str(output_dir))


if __name__ == "__main__":
    unittest.main()
