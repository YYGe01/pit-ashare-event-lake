#!/usr/bin/env python3
"""在终端预览 SQLite：列出表、DDL 与每张表前若干行（标准库 sqlite3，无需 IDE 插件）。"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    p = db_path.resolve().as_posix()
    uri = f"file:{p}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def main() -> int:
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except OSError:
            pass

    parser = argparse.ArgumentParser(description="预览 SQLite 数据库（只读）")
    parser.add_argument(
        "db",
        type=Path,
        help="数据库文件路径，例如 data_lake/collection/metadata/pitlake.sqlite",
    )
    parser.add_argument(
        "-n",
        "--limit",
        type=int,
        default=5,
        help="每张表最多打印的行数（默认 5）",
    )
    parser.add_argument(
        "-t",
        "--table",
        help="只查看指定表（默认可查看全部用户表）",
    )
    args = parser.parse_args()

    if not args.db.is_file():
        print(f"错误：找不到文件 {args.db}", file=sys.stderr)
        return 1

    try:
        con = _connect_readonly(args.db)
    except sqlite3.Error as e:
        print(f"错误：无法打开数据库：{e}", file=sys.stderr)
        return 1

    with con:
        cur = con.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = [r[0] for r in cur.fetchall()]

    if args.table:
        if args.table not in tables:
            print(f"错误：表 {args.table!r} 不存在。可用表：{', '.join(tables) or '(无)'}", file=sys.stderr)
            return 1
        tables = [args.table]

    print(f"数据库: {args.db.resolve()}")
    print(f"用户表数量: {len(tables)}")
    print()

    for name in tables:
        with con:
            cur = con.cursor()
            cur.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            )
            row = cur.fetchone()
            ddl = row[0] if row else ""
        print("=" * 72)
        print(f"表: {name}")
        if ddl:
            print(ddl.strip() + ";")
        print("-" * 72)
        with con:
            cur = con.cursor()
            cur.execute(f'SELECT * FROM "{name}" LIMIT ?', (args.limit,))
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
        if cols:
            print(" | ".join(cols))
        for r in rows:
            print(" | ".join(str(x) if x is not None else "" for x in r))
        if not rows:
            print("(无行)")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
