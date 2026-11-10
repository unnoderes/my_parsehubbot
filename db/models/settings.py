from __future__ import annotations

import enum
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base

if TYPE_CHECKING:
    from db.models.chat import Chat
    from db.models.forum_topic import ForumTopic
    from db.models.user import User


class SettingsScope(enum.Enum):
    USER = "user"
    GROUP = "group"
    GROUP_MEMBER = "group_member"
    FORUM_TOPIC = "forum_topic"
    FORUM_TOPIC_MEMBER = "forum_topic_member"
    CHANNEL = "channel"


_SCOPE_SHAPE_SQL = (
    "(scope = 'user' AND user_id IS NOT NULL AND chat_id IS NULL AND forum_topic_id IS NULL)"
    " OR (scope = 'group' AND user_id IS NULL AND chat_id IS NOT NULL AND forum_topic_id IS NULL)"
    " OR (scope = 'group_member' AND user_id IS NOT NULL AND chat_id IS NOT NULL AND forum_topic_id IS NULL)"
    " OR (scope = 'forum_topic' AND user_id IS NULL AND chat_id IS NULL AND forum_topic_id IS NOT NULL)"
    " OR (scope = 'forum_topic_member' AND user_id IS NOT NULL AND chat_id IS NULL AND forum_topic_id IS NOT NULL)"
    " OR (scope = 'channel' AND user_id IS NULL AND chat_id IS NOT NULL AND forum_topic_id IS NULL)"
)


class Settings(Base):
    __tablename__ = "settings"
    __table_args__ = (
        CheckConstraint(_SCOPE_SHAPE_SQL, name="ck_settings_scope_shape"),
        # 每个作用域主体仅一份配置
        Index(
            "uq_settings_user",
            "user_id",
            unique=True,
            postgresql_where=text("scope = 'user'"),
            sqlite_where=text("scope = 'user'"),
        ),
        Index(
            "uq_settings_group",
            "chat_id",
            unique=True,
            postgresql_where=text("scope = 'group'"),
            sqlite_where=text("scope = 'group'"),
        ),
        Index(
            "uq_settings_group_member",
            "chat_id",
            "user_id",
            unique=True,
            postgresql_where=text("scope = 'group_member'"),
            sqlite_where=text("scope = 'group_member'"),
        ),
        Index(
            "uq_settings_forum_topic",
            "forum_topic_id",
            unique=True,
            postgresql_where=text("scope = 'forum_topic'"),
            sqlite_where=text("scope = 'forum_topic'"),
        ),
        Index(
            "uq_settings_forum_topic_member",
            "forum_topic_id",
            "user_id",
            unique=True,
            postgresql_where=text("scope = 'forum_topic_member'"),
            sqlite_where=text("scope = 'forum_topic_member'"),
        ),
        Index(
            "uq_settings_channel",
            "chat_id",
            unique=True,
            postgresql_where=text("scope = 'channel'"),
            sqlite_where=text("scope = 'channel'"),
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    scope: Mapped[SettingsScope] = mapped_column(
        Enum(SettingsScope, name="settings_scope", values_callable=lambda e: [m.value for m in e]),
        nullable=False,
    )
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=True)
    chat_id: Mapped[int | None] = mapped_column(ForeignKey("chats.id", ondelete="CASCADE"), nullable=True)
    forum_topic_id: Mapped[int | None] = mapped_column(
        ForeignKey("forum_topics.id", ondelete="CASCADE"),
        nullable=True,
    )
    config: Mapped[dict[str, Any]] = mapped_column(
        MutableDict.as_mutable(JSON().with_variant(JSONB(), "postgresql")),
        nullable=False,
        default=dict,
    )
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    lock_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    user: Mapped[User | None] = relationship("User", back_populates="settings")
    chat: Mapped[Chat | None] = relationship("Chat", back_populates="settings")
    forum_topic: Mapped[ForumTopic | None] = relationship("ForumTopic", back_populates="settings")

    __mapper_args__ = {"version_id_col": lock_version}
