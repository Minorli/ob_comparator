import configparser
from pathlib import Path

import oracledb


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.ini"


def load_oracle_config():
    parser = configparser.ConfigParser(interpolation=None)
    if not parser.read(CONFIG_PATH, encoding="utf-8"):
        raise FileNotFoundError(f"config.ini not found at {CONFIG_PATH}")
    sec = parser["ORACLE_SOURCE"]
    settings = parser["SETTINGS"] if parser.has_section("SETTINGS") else {}
    lib_dir = (settings.get("oracle_client_lib_dir") or "").strip()
    if lib_dir:
        try:
            oracledb.init_oracle_client(lib_dir=lib_dir)
        except Exception:
            pass
    return (
        (sec.get("user") or "").strip(),
        (sec.get("password") or "").strip(),
        (sec.get("dsn") or "").strip(),
    )


print("--- Testing Oracle Stats Lag ---")
try:
    user, password, dsn = load_oracle_config()
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    with conn.cursor() as cur:
        try:
            cur.execute("DROP TABLE stats_test")
        except Exception:
            pass

        cur.execute("CREATE TABLE stats_test (id NUMBER)")
        print("Table created.")

        cur.execute("INSERT INTO stats_test VALUES (1)")
        conn.commit()
        print("Inserted 1 row.")

        cur.execute("SELECT NUM_ROWS FROM USER_TABLES WHERE TABLE_NAME = 'STATS_TEST'")
        row = cur.fetchone()
        print(f"NUM_ROWS immediately after insert: {row[0] if row else 'None'}")

        cur.execute("DROP TABLE stats_test")
except Exception as e:
    print(f"Error: {e}")
