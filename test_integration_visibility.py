import configparser
import os
import subprocess
import unittest

try:  # pragma: no cover
    import oracledb
except ImportError:  # pragma: no cover
    oracledb = None


@unittest.skipUnless(
    os.environ.get("RUN_INTEGRATION_TESTS") == "1",
    "requires RUN_INTEGRATION_TESTS=1 and live Oracle/OB",
)
class TestIntegrationColumnVisibility(unittest.TestCase):
    @staticmethod
    def _load_config() -> configparser.ConfigParser:
        cfg = configparser.ConfigParser()
        cfg.read("config.ini")
        return cfg

    def test_oracle_invisible_column_metadata(self):
        if oracledb is None:
            self.skipTest("oracledb not installed")
        cfg = self._load_config()
        settings = cfg["SETTINGS"]
        ora = cfg["ORACLE_SOURCE"]
        lib_dir = settings.get("oracle_client_lib_dir", "").strip()
        if lib_dir:
            oracledb.init_oracle_client(lib_dir=lib_dir)
        conn = oracledb.connect(
            user=ora.get("user"),
            password=ora.get("password"),
            dsn=ora.get("dsn"),
        )
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(*)
                FROM DBA_TAB_COLUMNS
                WHERE OWNER = 'SYS'
                  AND TABLE_NAME = 'DBA_TAB_COLUMNS'
                  AND COLUMN_NAME = 'INVISIBLE_COLUMN'
                """
            )
            row = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        self.assertIsNotNone(row)
        self.assertGreater(int(row[0]), 0)

    def test_ob_invisible_column_metadata(self):
        cfg = self._load_config()
        ob = cfg["OCEANBASE_TARGET"]
        executable = ob.get("executable", "obclient")
        sql = (
            "SELECT COUNT(*) "
            "FROM ALL_TAB_COLUMNS "
            "WHERE OWNER='SYS' "
            "AND TABLE_NAME='ALL_TAB_COLUMNS' "
            "AND COLUMN_NAME='INVISIBLE_COLUMN'"
        )
        cmd = [
            executable,
            "-h",
            ob.get("host"),
            "-P",
            ob.get("port"),
            "-u",
            ob.get("user_string"),
            "-p" + ob.get("password"),
            "-ss",
            "-e",
            sql,
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=60,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        out = result.stdout.strip()
        self.assertTrue(out, msg="empty output from obclient")
        self.assertGreater(int(out.splitlines()[-1].strip()), 0)
