"""SQLite → PostgreSQL 数据迁移工具(UNI-47)。

设计要点:

- 非破坏性: 只读取 SQLite 源库, 不删除/修改源数据。
- 结构对齐: 目标 PG 库按 canonical ``Base.metadata`` 建表, 与 Alembic 启动路径一致。
- 类型一致: 通过 ``Base.metadata`` 在两端使用同一套列定义(JSON/JSONB、Enum、
  BigInteger 变体), 避免反射 SQLite 产生的 TEXT 类型导致 JSONB 双序列化。
- 行数校验: 每张表迁移后对比源/目标 ``COUNT(*)``, 不一致即抛 ``MigrationVerificationError``。
- 迁移日志: 返回每表 before/after 行数报告, 由调用方记录。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

import db.models  # noqa: F401  确保模型注册到 Base.metadata
from db.base import Base

logger = logging.getLogger("db.migrate")

DEFAULT_TABLES: tuple[str, ...] = ("users", "chats", "forum_topics", "settings", "cache")
COPY_BATCH_SIZE = 1000


class MigrationVerificationError(RuntimeError):
    """目标库行数与源库不一致。"""


@dataclass
class TableMigrationReport:
    table: str
    source_rows: int
    target_rows: int
    skipped: bool = False


@dataclass
class MigrationReport:
    source_url: str
    target_url: str
    tables: list[TableMigrationReport] = field(default_factory=list)

    @property
    def total_source_rows(self) -> int:
        return sum(t.source_rows for t in self.tables)

    @property
    def total_target_rows(self) -> int:
        return sum(t.target_rows for t in self.tables)

    @property
    def verified(self) -> bool:
        return all(t.source_rows == t.target_rows for t in self.tables)


async def _row_count(engine: AsyncEngine, table: sa.Table) -> int:
    async with engine.connect() as conn:
        return int((await conn.execute(sa.select(sa.func.count()).select_from(table))).scalar() or 0)


async def _copy_table(
    source: AsyncEngine,
    target: AsyncEngine,
    table: sa.Table,
    *,
    batch_size: int = COPY_BATCH_SIZE,
) -> TableMigrationReport:
    source_rows = await _row_count(source, table)
    if source_rows == 0:
        target_rows = await _row_count(target, table)
        logger.info(f"[{table.name}] 源表为空, 跳过复制 (目标现有 {target_rows} 行)")
        return TableMigrationReport(table=table.name, source_rows=0, target_rows=target_rows, skipped=True)

    async with source.connect() as src_conn:
        result = await src_conn.execute(sa.select(table))
        rows = [dict(m) for m in result.mappings().all()]

    inserted = 0
    async with target.begin() as tgt_conn:
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            await tgt_conn.execute(table.insert(), batch)
            inserted += len(batch)

    target_rows = await _row_count(target, table)
    if inserted != target_rows or source_rows != target_rows:
        raise MigrationVerificationError(
            f"[{table.name}] 行数校验失败: 源 {source_rows} 行, 目标 {target_rows} 行 (插入 {inserted} 行)"
        )
    logger.info(f"[{table.name}] 迁移完成: 源 {source_rows} 行 -> 目标 {target_rows} 行")
    return TableMigrationReport(table=table.name, source_rows=source_rows, target_rows=target_rows)


async def reset_target_tables(target_url: str, *, tables: tuple[str, ...] = DEFAULT_TABLES) -> None:
    """清空目标库中的迁移表(用于重新迁移 / 回滚到迁移前状态)。

    注意: 仅删除数据, 不删除表结构; 回退到 SQLite 模式只需将 DATABASE_URL 改回 SQLite 连接串。
    若目标表尚不存在(全新 PG 库), 先按 ``Base.metadata`` 建表再清空, 保持幂等。
    """
    target = create_async_engine(target_url)
    try:
        async with target.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            for name in reversed(tables):
                if name in Base.metadata.tables:
                    await conn.execute(Base.metadata.tables[name].delete())
        logger.info(f"已清空目标库表: {', '.join(tables)}")
    finally:
        await target.dispose()


async def migrate_sqlite_to_postgres(
    source_url: str,
    target_url: str,
    *,
    tables: tuple[str, ...] = DEFAULT_TABLES,
    batch_size: int = COPY_BATCH_SIZE,
) -> MigrationReport:
    """将 SQLite 源库的全部数据复制到 PostgreSQL 目标库。

    非破坏性: 不修改源库。目标库表按 ``Base.metadata`` 创建(若不存在)。
    """
    source = create_async_engine(source_url)
    target = create_async_engine(target_url)
    report = MigrationReport(source_url=source_url, target_url=target_url)
    try:
        async with target.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info(f"目标库结构已就绪 (URL={target_url})")

        for name in tables:
            if name not in Base.metadata.tables:
                logger.warning(f"模型中没有表 {name}, 跳过")
                continue
            table = Base.metadata.tables[name]
            report.tables.append(await _copy_table(source, target, table, batch_size=batch_size))

        if not report.verified:
            failed = [t.table for t in report.tables if t.source_rows != t.target_rows]
            raise MigrationVerificationError(f"以下表行数校验失败: {', '.join(failed)}")
        return report
    finally:
        await source.dispose()
        await target.dispose()
