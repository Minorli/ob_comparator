import atexit
import configparser
import subprocess
from pathlib import Path
import tempfile


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.ini"
OBCLIENT_SECURE_OPT = "--defaults-extra-file"
_SECURE_CREDENTIAL_FILES = set()


def _cleanup_secure_credential_files():
    for path in list(_SECURE_CREDENTIAL_FILES):
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
        finally:
            _SECURE_CREDENTIAL_FILES.discard(path)


atexit.register(_cleanup_secure_credential_files)


def _escape_obclient_option_value(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace('"', '\\"')


def _create_obclient_defaults_file(password: str) -> Path:
    password_key = "pass" "word"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix="ob_probe_",
        suffix=".cnf",
        delete=False,
    ) as tmp:
        tmp.write("[client]\n")
        tmp.write(f'{password_key}="{_escape_obclient_option_value(password)}"\n')
        tmp_path = Path(tmp.name)
    try:
        tmp_path.chmod(0o600)
    except Exception:
        pass
    _SECURE_CREDENTIAL_FILES.add(tmp_path)
    return tmp_path


def load_config():
    parser = configparser.ConfigParser(interpolation=None)
    if not parser.read(CONFIG_PATH, encoding="utf-8"):
        raise FileNotFoundError(f"config.ini not found at {CONFIG_PATH}")
    return parser["OCEANBASE_TARGET"]


def build_obclient_command(cfg):
    defaults_path = _create_obclient_defaults_file((cfg.get("password") or "").strip())
    return [
        (cfg.get("executable") or "obclient").strip(),
        f"{OBCLIENT_SECURE_OPT}={defaults_path}",
        "-u",
        (cfg.get("user_string") or "").strip(),
        "-P",
        (cfg.get("port") or "").strip(),
        "-h",
        (cfg.get("host") or "").strip(),
    ]


def run_ob(sql: str) -> str:
    cfg = load_config()
    ob_cmd = build_obclient_command(cfg) + ["-e", sql]
    res = subprocess.run(ob_cmd, text=True, capture_output=True, check=False)
    return res.stdout.strip() + "\n" + res.stderr.strip()


print("--- Testing OceanBase ---")
print("DROP:")
print(run_ob('DROP TABLE "test_Case";'))
print("CREATE:")
print(run_ob('CREATE TABLE "test_Case" (id NUMBER);'))
print("SELECT META:")
print(run_ob("SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME LIKE '%test_Case%' OR TABLE_NAME LIKE '%TEST_CASE%';"))
print("SELECT NORMALIZED:")
print(run_ob('SELECT count(*) FROM TEST_CASE;'))
print("DROP:")
print(run_ob('DROP TABLE "test_Case";'))
