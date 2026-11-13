"""UNI-47 迁移与 PostgreSQL 引擎测试。

使用临时 SQLite 文件 + 本地 PostgreSQL 临时 schema 做端到端迁移验证。
PostgreSQL 连接串通过环境变量 PG_TEST_URL 覆盖(默认指向 localhost:5432)。

为避免触及 BotSettings(需 Telegram 凭据), 测试仅直接引用 db.base / db.models / db.engine,
不经由 db.session 创建的默认引擎。
"""

from __future__ import annotations

import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest
import sqlalchemy as sa

import db.models  # noqa: F401  注册模型到 Base.metadata
from db.base import Base
from db.engine import POSTGRES_POOL_SIZE, create_engine_for_url, is_sqlite
from db.models.chat import ChatType


def _null_pool_type() -> type:
    return sa.pool.NullPool


PG_TEST_URL = os.environ.get(
    "PG_TEST_URL",
    "postgresql+asyncpg://parsehub:parsehub_dev@localhost:5432/parsehub",
)


def _sqlite_url(path: str) -> str:
    return f"sqlite+aiosqlite:///{path}"


def _dispose_sync(engine: sa.ext.asyncio.AsyncEngine) -> None:  # type: ignore[name-defined]
    engine.sync_engine.dispose()


def test_is_sqlite_detects_backends() -> None:
    assert is_sqlite("sqlite+aiosqlite:///data/db/database.db")
    assert is_sqlite(_sqlite_url("x.db"))
    assert not is_sqlite(PG_TEST_URL)


def test_create_engine_for_url_picks_pool_for_postgres() -> None:
    sqlite_engine = create_engine_for_url(_sqlite_url("x.db"))
    assert sqlite_engine.dialect.name == "sqlite"
    _dispose_sync(sqlite_engine)

    if PG_TEST_URL.startswith("postgresql"):
        pg_engine = create_engine_for_url(PG_TEST_URL)
        assert pg_engine.dialect.name == "postgresql"
        # PG 使用有界连接池(非 NullPool), 池容量等于配置常量
        pool = pg_engine.pool
        assert not isinstance(pool, _null_pool_type())  # null pool 仅用于 SQLite
        pool_size = pool.size()  # type: ignore[attr-defined]
        assert pool_size == POSTGRES_POOL_SIZE
        _dispose_sync(pg_engine)


async def _seed_sqlite(path: str) -> None:
    engine = create_engine_for_url(_sqlite_url(path))
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            Base.metadata.tables["users"].insert(),
            [
                {"telegram_user_id": 1001, "language_code": "zh"},
                {"telegram_user_id": 1002, "language_code": "en"},
            ],
        )
        await conn.execute(
            Base.metadata.tables["chats"].insert(),
            [{"telegram_chat_id": 2001, "type": ChatType.GROUP}],
        )
        await conn.execute(
            Base.metadata.tables["settings"].insert(),
            [
                {
                    "scope": "user",
                    "user_id": 1,
                    "chat_id": None,
                    "forum_topic_id": None,
                    "config": {"a": 1, "b": "x"},
                    "schema_version": 1,
                    "lock_version": 0,
                },
                {
                    "scope": "group",
                    "user_id": None,
                    "chat_id": 1,
                    "forum_topic_id": None,
                    "config": {"c": [1, 2, 3]},
                    "schema_version": 1,
                    "lock_version": 0,
                },
            ],
        )
        await conn.execute(
            Base.metadata.tables["cache"].insert(),
            [
                {
                    "key": "k1",
                    "url": "https://example.com/1",
                    "entry_json": {"parse_result": {"title": "t", "content": "c"}},
                    "accessed_at": datetime.now(UTC),
                },
            ],
        )
    await engine.dispose()


async def _clear_pg_schema() -> None:
    engine = create_engine_for_url(PG_TEST_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.mark.asyncio
async def test_migrate_sqlite_to_postgres_preserves_rows() -> None:
    if not PG_TEST_URL.startswith("postgresql"):
        pytest.skip("需要 PostgreSQL 测试库")

    tmp_dir = tempfile.mkdtemp(prefix="migrate-test-")
    try:
        sqlite_path = Path(tmp_dir) / "source.db"
        await _clear_pg_schema()
        await _seed_sqlite(str(sqlite_path))

        from db.migrate import migrate_sqlite_to_postgres

        report = await migrate_sqlite_to_postgres(_sqlite_url(str(sqlite_path)), PG_TEST_URL)

        assert report.verified
        by_name = {t.table: t for t in report.tables}
        assert by_name["users"].source_rows == 2
        assert by_name["chats"].source_rows == 1
        assert by_name["settings"].source_rows == 2
        assert by_name["cache"].source_rows == 1
        assert by_name["users"].target_rows == 2
        assert by_name["settings"].target_rows == 2
        assert by_name["cache"].target_rows == 1

        # 内容一致性: 读取 PG 目标, 确认 JSON 未二次编码
        pg = create_engine_for_url(PG_TEST_URL)
        async with pg.connect() as conn:
            row = (
                await conn.execute(
                    sa.select(Base.metadata.tables["cache"].c.entry_json).where(
                        Base.metadata.tables["cache"].c.key == "k1"
                    )
                )
            ).first()
            assert row is not None
            assert row[0]["parse_result"]["title"] == "t"
        await pg.dispose()
        await _clear_pg_schema()
    finally:
        import shutil

        shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_migrate_empty_source_skips_tables() -> None:
    if not PG_TEST_URL.startswith("postgresql"):
        pytest.skip("需要 PostgreSQL 测试库")

    with tempfile.TemporaryDirectory() as tmp:
        sqlite_path = Path(tmp) / "empty.db"
        engine = create_engine_for_url(_sqlite_url(str(sqlite_path)))
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await engine.dispose()

        from db.migrate import migrate_sqlite_to_postgres

        await _clear_pg_schema()
        report = await migrate_sqlite_to_postgres(_sqlite_url(str(sqlite_path)), PG_TEST_URL)

    assert report.verified
    assert all(t.skipped for t in report.tables)
    await _clear_pg_schema()
