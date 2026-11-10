from sqlalchemy.ext.asyncio import AsyncSession

from db.models.forum_topic import ForumTopic
from repo.forum_topic import ForumTopicRepo
from services.chat import ChatService


class ForumTopicNotFoundError(Exception):
    pass


class ForumTopicService:
    def __init__(self, session: AsyncSession) -> None:
        self.chat = ChatService(session)
        self.forum_topic = ForumTopicRepo(session)

    async def add(self, telegram_chat_id: int, telegram_thread_id: int) -> ForumTopic:
        chat = await self.chat.ensure_group(telegram_chat_id)
        return await self.forum_topic.add(chat.id, telegram_thread_id)

    async def get(self, telegram_chat_id: int, telegram_thread_id: int) -> ForumTopic | None:
        if not (chat := await self.chat.get(telegram_chat_id)):
            return None
        return await self.forum_topic.get(chat.id, telegram_thread_id)

    async def get_or_raise(self, telegram_chat_id: int, telegram_thread_id: int) -> ForumTopic:
        if not (forum_topic := await self.get(telegram_chat_id, telegram_thread_id)):
            raise ForumTopicNotFoundError(
                f"在数据库中找不到 ForumTopic: chat={telegram_chat_id}, thread={telegram_thread_id}"
            )
        return forum_topic

    async def ensure(self, telegram_chat_id: int, telegram_thread_id: int) -> ForumTopic:
        if not (ft := await self.get(telegram_chat_id, telegram_thread_id)):
            chat = await self.chat.ensure_group(telegram_chat_id)
            ft = await self.forum_topic.add(chat.id, telegram_thread_id)
        return ft
