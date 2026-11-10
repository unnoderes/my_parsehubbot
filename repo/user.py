from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.user import User


class UserRepo:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_by_tg_user_id(self, telegram_user_id: int) -> User | None:
        user = await self._session.scalar(select(User).where(User.telegram_user_id == telegram_user_id))
        return user

    async def add(self, telegram_user_id: int) -> User:
        user = User(telegram_user_id=telegram_user_id)
        self._session.add(user)
        await self._session.flush()
        return user
