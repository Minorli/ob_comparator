#!/usr/bin/env python3
"""
fix_public_synonyms.py

遍历指定目录下的 .sql 文件，规范 PUBLIC 同义词名称：
  CREATE [OR REPLACE] PUBLIC SYNONYM schema.synonym_name ...
改为
  CREATE [OR REPLACE] PUBLIC SYNONYM synonym_name ...
文件名保持不变，FOR 子句不修改。
"""

import argparse
from pathlib import Path
import re

# 匹配带 schema 前缀的 PUBLIC SYNONYM 名称，支持可选引号/空白
PATTERN = re.compile(
    r'(CREATE\s+(?:OR\s+REPLACE\s+)?PUBLIC\s+SYNONYM\s+)'
    r'(?P<schema>"?[A-Z0-9_\$#]+"?)\s*\.\s*(?P<name>"?[A-Z0-9_\$#]+"?)',
    flags=re.IGNORECASE,
)


def fix_content(text: str):
    def _repl(m: re.Match) -> str:
        return f"{m.group(1)}{m.group('name')}"

    new_text, edits = PATTERN.subn(_repl, text)
    return new_text, edits


def main():
    parser = argparse.ArgumentParser(description="Normalize PUBLIC SYNONYM names (drop schema prefix).")
    parser.add_argument(
        "root",
        nargs="?",
        default=".",
        help="根目录（默认当前目录），递归处理其中的 .sql 文件",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    if not root.exists():
        raise SystemExit(f"目录不存在: {root}")

    total_files = total_edits = 0
    for sql_file in root.rglob("*.sql"):
        try:
            text = sql_file.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        new_text, edits = fix_content(text)
        if edits:
            sql_file.write_text(new_text, encoding="utf-8")
            total_edits += edits
            print(f"[UPDATED] {sql_file} (public synonym names normalized, edits={edits})")
        total_files += 1

    print(f"扫描文件: {total_files}，修改次数: {total_edits}")


if __name__ == "__main__":
    main()
