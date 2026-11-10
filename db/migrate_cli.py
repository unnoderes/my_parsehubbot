"""SQLite → PostgreSQL 迁移命令行入口(UNI-47)。

用法::

    # 执行迁移(默认读取 .env 的 DATABASE_URL 作为目标, SQLITE_SOURCE_URL 作为源)
    uv run python -m db.migrate_cli

    # 显式指定连接串
    uv run python -m db.migrate_cli --source sqlite+aiosqlite:///data/db/database.db \
        --target postgresql+asyncpg://parsehub:parsehub_dev@localhost:5432/parsehub

    # 清空目标库数据(回滚到迁移前状态; 源 SQLite 永不修改)
    uv run python -m db.migrate_cli --reset

迁移完成后把 DATABASE_URL 切到 PostgreSQL 即完成切换;
如需回退, 将 DATABASE_URL 改回 SQLite 连接串并重启即可(源库未被改动)。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from db.migrate import DEFAULT_TABLES, migrate_sqlite_to_postgres, reset_target_tables

DEFAULT_SQLITE_URL = "sqlite+aiosqlite:///data/db/database.db"


def _default_target() -> str:
    from core.config import bs

    return bs.database_url


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SQLite -> PostgreSQL 数据迁移")
    parser.add_argument("--source", default=DEFAULT_SQLITE_URL, help="SQLite 源连接串")
    parser.add_argument("--target", default=None, help="PostgreSQL 目标连接串(默认读取 DATABASE_URL)")
    parser.add_argument("--tables", default=",".join(DEFAULT_TABLES), help="逗号分隔的表清单")
    parser.add_argument("--batch-size", type=int, default=1000, help="每批插入行数")
    parser.add_argument("--reset", action="store_true", help="清空目标库迁移表数据后退出")
    return parser


async def run(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    tables = tuple(t.strip() for t in args.tables.split(",") if t.strip())
    target = args.target or _default_target()

    if args.reset:
        await reset_target_tables(target, tables=tables)
        print("目标库已清空。")
        return 0

    if target.startswith("sqlite"):
        print("目标连接串是 SQLite, 请通过 --target 或 DATABASE_URL 指定 PostgreSQL 连接串。", file=sys.stderr)
        return 2

    report = await migrate_sqlite_to_postgres(args.source, target, tables=tables, batch_size=args.batch_size)
    print("迁移完成, 行数对比:")
    for t in report.tables:
        status = "跳过(源为空)" if t.skipped else "OK"
        print(f"  {t.table}: 源 {t.source_rows} 行 -> 目标 {t.target_rows} 行 [{status}]")
    print(f"合计: 源 {report.total_source_rows} 行 -> 目标 {report.total_target_rows} 行")
    print("迁移日志已写入。切换: 将 .env 中 DATABASE_URL 指向 PostgreSQL 后重启。")
    return 0


def main() -> None:
    sys.exit(asyncio.run(run(build_parser().parse_args())))


if __name__ == "__main__":
    main()
