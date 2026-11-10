from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Chat, ChatType
from repo.chat import ChatRepo


class ChatNotFoundError(Exception):
    pass


class ChatService:
    def __init__(self, session: AsyncSession) -> None:
        self.chat = ChatRepo(session)

    async def add(self, telegram_chat_id: int, chat_type: ChatType) -> Chat:
        return await self.chat.add(telegram_chat_id, chat_type=chat_type)

    async def add_group(self, telegram_chat_id: int) -> Chat:
        return await self.add(telegram_chat_id, chat_type=ChatType.GROUP)

    async def add_channel(self, telegram_chat_id: int) -> Chat:
        return await self.add(telegram_chat_id, chat_type=ChatType.CHANNEL)

    async def get(self, telegram_chat_id: int) -> Chat | None:
        return await self.chat.get_by_tg_chat_id(telegram_chat_id)

    async def get_or_raise(self, telegram_chat_id: int) -> Chat:
        if not (chat := await self.get(telegram_chat_id)):
            raise ChatNotFoundError(f"在数据库中找不到 Chat: {telegram_chat_id}")
        return chat

    async def ensure(self, telegram_chat_id: int, chat_type: ChatType) -> Chat:
        if not (chat := await self.get(telegram_chat_id)):
            return await self.add(telegram_chat_id, chat_type=chat_type)
        return chat

    async def ensure_group(self, telegram_chat_id: int) -> Chat:
        return await self.ensure(telegram_chat_id, chat_type=ChatType.GROUP)

    async def ensure_channel(self, telegram_chat_id: int) -> Chat:
        return await self.ensure(telegram_chat_id, chat_type=ChatType.CHANNEL)
