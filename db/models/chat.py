from __future__ import annotations

import enum
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, Enum, Integer, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base

if TYPE_CHECKING:
    from db.models.forum_topic import ForumTopic
    from db.models.settings import Settings


class ChatType(enum.Enum):
    GROUP = "group"
    CHANNEL = "channel"


class Chat(Base):
    __tablename__ = "chats"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    telegram_chat_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    type: Mapped[ChatType] = mapped_column(Enum(ChatType), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    forum_topics: Mapped[list[ForumTopic]] = relationship(
        "ForumTopic",
        back_populates="chat",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    settings: Mapped[list[Settings]] = relationship(
        "Settings",
        back_populates="chat",
        passive_deletes=True,
    )
