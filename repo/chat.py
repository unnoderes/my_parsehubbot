from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.chat import Chat, ChatType


class ChatRepo:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_tg_chat_id(self, telegram_chat_id: int) -> Chat | None:
        chat = await self._session.scalar(select(Chat).where(Chat.telegram_chat_id == telegram_chat_id))
        return chat

    async def add(self, telegram_chat_id: int, chat_type: ChatType) -> Chat:
        chat = Chat(telegram_chat_id=telegram_chat_id, type=chat_type)
        self._session.add(chat)
        await self._session.flush()
        return chat
