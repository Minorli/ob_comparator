import unittest
from pathlib import Path


class TestConfigTemplate(unittest.TestCase):
    def test_config_template_has_no_duplicate_keys(self):
        template_path = Path(__file__).resolve().parent / "config.ini.template"
        text = template_path.read_text(encoding="utf-8")

        section = None
        seen = {}
        duplicates = []

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue
            if line.startswith("[") and line.endswith("]"):
                section = line[1:-1].strip()
                seen.setdefault(section, set())
                continue
            if "=" not in line:
                continue
            key = line.split("=", 1)[0].strip()
            if section is None:
                section = ""
                seen.setdefault(section, set())
            if key in seen[section]:
                duplicates.append((section, key))
            else:
                seen[section].add(key)

        self.assertEqual(duplicates, [], f"Duplicate keys found: {duplicates}")
