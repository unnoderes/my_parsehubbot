from sqlalchemy.ext.asyncio import AsyncSession

from core import bs
from db.models.user import User
from repo.user import UserRepo


class UserNotFoundError(Exception):
    pass


class UserService:
    def __init__(self, session: AsyncSession) -> None:
        self.user = UserRepo(session)

    async def add(self, telegram_user_id: int) -> User:
        return await self.user.add(telegram_user_id)

    async def get(self, telegram_user_id: int) -> User | None:
        return await self.user.get_by_tg_user_id(telegram_user_id)

    async def get_or_raise(self, telegram_user_id: int) -> User:
        if not (user := await self.get(telegram_user_id)):
            raise UserNotFoundError(f"在数据库中找不到用户: {telegram_user_id}")
        return user

    async def ensure(self, telegram_user_id: int) -> User:
        if not (user := await self.get(telegram_user_id)):
            return await self.add(telegram_user_id)
        return user

    async def get_lang(self, telegram_user_id: int) -> str:
        if user := await self.get(telegram_user_id):
            return user.language_code
        return bs.language

    async def set_lang(self, telegram_user_id: int, language_code: str) -> User:
        user = await self.ensure(telegram_user_id)
        user.language_code = language_code
        return user
