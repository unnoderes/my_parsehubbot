"""migrate_user_settings_to_settings

Revision ID: b3a0e10df4b2
Revises: a43d3810bad8
Create Date: 2026-07-19 15:03:21.570449

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from repo.settings import DEFAULT_SETTINGS_CONFIG

# revision identifiers, used by Alembic.
revision: str = "b3a0e10df4b2"
down_revision: str | Sequence[str] | None = "a43d3810bad8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    # 新数据库没有旧表，无需搬运数据。
    if not inspector.has_table("user_settings"):
        return

    metadata = sa.MetaData()
    users = sa.Table("users", metadata, autoload_with=bind)
    legacy_settings = sa.Table("user_settings", metadata, autoload_with=bind)
    settings = sa.Table("settings", metadata, autoload_with=bind)

    orphan_user_ids = (
        bind.execute(
            sa.select(legacy_settings.c.user_id)
            .select_from(
                legacy_settings.outerjoin(
                    users,
                    legacy_settings.c.user_id == users.c.id,
                )
            )
            .where(users.c.id.is_(None))
        )
        .scalars()
        .all()
    )
    if orphan_user_ids:
        # 删除孤儿 user_settings
        bind.execute(legacy_settings.delete().where(legacy_settings.c.user_id.in_(orphan_user_ids)))

    legacy_rows = (
        bind.execute(
            sa.select(
                legacy_settings.c.user_id,
                legacy_settings.c.settings_json,
                legacy_settings.c.lock_version,
                legacy_settings.c.created_at,
                legacy_settings.c.updated_at,
            )
        )
        .mappings()
        .all()
    )

    rows_to_insert = []
    for row in legacy_rows:
        config = dict(row["settings_json"] or {})
        config.pop("schema_version")
        config = {
            key: value
            for key, value in config.items()
            if value != DEFAULT_SETTINGS_CONFIG.model_dump(mode="json").get(key)
        }
        rows_to_insert.append(
            {
                "scope": "user",
                "user_id": row["user_id"],
                "chat_id": None,
                "forum_topic_id": None,
                "config": config,
                "schema_version": 1,  # 大重构, 版本统一重置为 1
                "lock_version": row["lock_version"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )
    if rows_to_insert:
        bind.execute(settings.insert(), rows_to_insert)

    migrated_count = bind.scalar(sa.select(sa.func.count()).select_from(settings).where(settings.c.scope == "user"))

    if migrated_count != len(legacy_rows):
        raise RuntimeError(
            "user_settings 迁移校验失败："
            f"旧表共 {len(legacy_rows)} 条，"
            f"settings 中共 {migrated_count} 条 user scope 配置。"
        )
    op.drop_table("user_settings")


def downgrade() -> None:
    """Downgrade schema."""
    raise NotImplementedError("user_settings 已迁移至 settings 并被删除，该迁移无法安全降级。请从数据库备份恢复。")
