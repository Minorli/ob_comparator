import sys
import types
import unittest
from pathlib import Path

# Provide a stub oracledb module for import safety.
if "oracledb" not in sys.modules:
    stub = types.SimpleNamespace(
        Error=Exception,
        DEFAULT_AUTH=object(),
        init_oracle_client=lambda **_kwargs: None,
    )
    sys.modules["oracledb"] = stub

import init_test as it


class TestInitTest(unittest.TestCase):
    def test_build_obclient_command_hides_password_from_args(self):
        try:
            cmd = it.build_obclient_command(
                executable="/usr/bin/obclient",
                host="127.0.0.1",
                port="2881",
                user_string="root@test",
                password="Secret##123",
            )
            text = " ".join(cmd)
            self.assertNotIn("Secret##123", text)
            defaults_opt = next((item for item in cmd if item.startswith(f"{it.OBCLIENT_SECURE_OPT}=")), "")
            self.assertTrue(defaults_opt)
            defaults_path = Path(defaults_opt.split("=", 1)[1])
            self.assertTrue(defaults_path.exists())
        finally:
            it._cleanup_secure_credential_files()


if __name__ == "__main__":
    unittest.main()
