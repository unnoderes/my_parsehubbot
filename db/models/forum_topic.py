from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base

if TYPE_CHECKING:
    from db.models.chat import Chat
    from db.models.settings import Settings


class ForumTopic(Base):
    __tablename__ = "forum_topics"
    __table_args__ = (UniqueConstraint("chat_id", "telegram_thread_id", name="uq_forum_topics_chat_thread"),)

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    chat_id: Mapped[int] = mapped_column(ForeignKey("chats.id", ondelete="CASCADE"), nullable=False)
    telegram_thread_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    chat: Mapped[Chat] = relationship("Chat", back_populates="forum_topics")
    settings: Mapped[list[Settings]] = relationship(
        "Settings",
        back_populates="forum_topic",
        passive_deletes=True,
    )
