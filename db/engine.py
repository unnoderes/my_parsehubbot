"""异步数据库引擎工厂。

根据 DATABASE_URL 选择 SQLite 或 PostgreSQL 后端(UNI-47):

- SQLite: 保留 WAL / 外键 / busy_timeout 等 PRAGMA, 维持单实例行为。
- PostgreSQL: 配置异步连接池(默认 20)、连接预检(pool_pre_ping)、连接/命令超时,
  支持高并发多进程访问。
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import event, make_url
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from core.config import bs

logger = logging.getLogger("db.engine")

POSTGRES_POOL_SIZE = 20
POSTGRES_MAX_OVERFLOW = 10
POSTGRES_CONNECT_TIMEOUT = 10
POSTGRES_COMMAND_TIMEOUT = 60


def _backend_name(database_url: str | URL) -> str:
    if isinstance(database_url, URL):
        return database_url.get_backend_name()
    return make_url(database_url).get_backend_name()


def is_sqlite(database_url: str | URL) -> bool:
    """判断连接串是否指向 SQLite 后端。"""
    return _backend_name(database_url) == "sqlite"


def create_engine_for_url(database_url: str) -> AsyncEngine:
    """按数据库后端创建异步 SQLAlchemy 引擎。"""
    if is_sqlite(database_url):
        return create_async_engine(database_url)

    return create_async_engine(
        database_url,
        pool_size=POSTGRES_POOL_SIZE,
        max_overflow=POSTGRES_MAX_OVERFLOW,
        pool_pre_ping=True,
        connect_args={
            "timeout": POSTGRES_CONNECT_TIMEOUT,
            "command_timeout": POSTGRES_COMMAND_TIMEOUT,
            "server_settings": {"application_name": "parsehubbot"},
        },
    )


engine: AsyncEngine = create_engine_for_url(bs.database_url)


@event.listens_for(engine.sync_engine, "connect")
def _set_sqlite_pragma(dbapi_connection: Any, connection_record: Any) -> None:
    if not is_sqlite(engine.url):
        return

    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=5000")
    finally:
        cursor.close()


async def close_db() -> None:
    await engine.dispose()
