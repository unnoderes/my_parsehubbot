from dataclasses import dataclass
from typing import Any, Self

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.settings import Settings, SettingsScope
from log import logger
from repo.settings.migrations import REGISTRY
from repo.settings.schema import CURRENT_SCHEMA_VERSION, DEFAULT_SETTINGS_CONFIG, SettingsConfig


@dataclass(frozen=True, slots=True, kw_only=True)
class SettingsTarget:
    scope: SettingsScope
    user_id: int | None = None
    chat_id: int | None = None
    forum_topic_id: int | None = None

    @classmethod
    def user(cls, user_id: int) -> Self:
        return cls(scope=SettingsScope.USER, user_id=user_id)

    @classmethod
    def group(cls, chat_id: int) -> Self:
        return cls(scope=SettingsScope.GROUP, chat_id=chat_id)

    @classmethod
    def group_member(cls, chat_id: int, user_id: int) -> Self:
        return cls(scope=SettingsScope.GROUP_MEMBER, chat_id=chat_id, user_id=user_id)

    @classmethod
    def forum_topic(cls, forum_topic_id: int) -> Self:
        return cls(scope=SettingsScope.FORUM_TOPIC, forum_topic_id=forum_topic_id)

    @classmethod
    def forum_topic_member(cls, forum_topic_id: int, user_id: int) -> Self:
        return cls(scope=SettingsScope.FORUM_TOPIC_MEMBER, forum_topic_id=forum_topic_id, user_id=user_id)

    @classmethod
    def channel(cls, chat_id: int) -> Self:
        return cls(scope=SettingsScope.CHANNEL, chat_id=chat_id)

    def dump(self) -> dict[str, int | None | SettingsScope]:
        return {
            "scope": self.scope,
            "user_id": self.user_id,
            "chat_id": self.chat_id,
            "forum_topic_id": self.forum_topic_id,
        }


class SettingsRepo:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, target: SettingsTarget) -> Settings | None:
        settings = await self._session.scalar(select(Settings).filter_by(**target.dump()))
        return settings

    async def add(self, target: SettingsTarget, config: SettingsConfig | None = None) -> Settings:
        settings = Settings(
            **target.dump(),
            config=_config_to_patch(config or DEFAULT_SETTINGS_CONFIG),
        )
        self._session.add(settings)
        await self._session.flush()
        return settings

    async def get_raw_config(self, target: SettingsTarget) -> dict:
        """获取 config patch"""
        settings = await self.get(target)
        return settings.config if settings else {}

    async def get_current_config(self, target: SettingsTarget) -> SettingsConfig:
        """获取最新完整配置"""
        migrated = await self.migrate(target)
        if not migrated:
            return DEFAULT_SETTINGS_CONFIG
        return _hydrate_config_patch(migrated.config)

    async def _save_config_patch(self, target: SettingsTarget, config_patch: dict[str, Any]) -> Settings:
        settings = await self.get(target)
        if not settings:
            settings = Settings(**target.dump(), config=config_patch)
            self._session.add(settings)
        else:
            settings.config = config_patch
        await self._session.flush()
        return settings

    async def patch_config(self, target: SettingsTarget, **kwargs: Any) -> SettingsConfig:
        migrated = await self.migrate(target)
        current_patch = dict(migrated.config) if migrated else {}
        next_patch = current_patch | kwargs
        config = _hydrate_config_patch(next_patch)
        await self._save_config_patch(target, config.model_dump(mode="json", include=set(next_patch)))
        return config

    async def migrate(self, target: SettingsTarget) -> Settings | None:
        """对 config patch 进行迁移"""
        log = logger.bind(name="SettingsMigration")

        settings = await self.get(target)
        if not settings:
            return None

        schema_version = settings.schema_version

        if schema_version == CURRENT_SCHEMA_VERSION:
            return settings

        if schema_version > CURRENT_SCHEMA_VERSION:
            raise ValueError(
                f"未知的 settings schema_version={schema_version}，当前最大版本为 {CURRENT_SCHEMA_VERSION}。"
            )

        log.debug(f"开始迁移设置配置: schema_version={schema_version}, current={CURRENT_SCHEMA_VERSION}")

        while schema_version < CURRENT_SCHEMA_VERSION:
            fn = REGISTRY.get(schema_version)
            if fn is None:
                raise ValueError(
                    f"缺少设置配置迁移函数：v{schema_version} → v{schema_version + 1}，"
                    f"请在 migrations/ 下新增文件并注册到 REGISTRY。"
                )

            log.debug(f"执行设置配置迁移: v{schema_version} -> v{schema_version + 1}")
            config = fn(settings.config)
            settings.config = config
            settings.schema_version = schema_version + 1
            await self._session.flush()

        log.debug(f"设置配置迁移完成: schema_version={schema_version}")
        return settings


def _hydrate_config_patch(config_patch: dict[str, Any]) -> SettingsConfig:
    return SettingsConfig.model_validate(DEFAULT_SETTINGS_CONFIG.model_dump(mode="json") | config_patch)


def _config_to_patch(config: SettingsConfig, base: SettingsConfig = DEFAULT_SETTINGS_CONFIG) -> dict[str, Any]:
    config_data = config.model_dump(mode="json")
    base_data = base.model_dump(mode="json")

    return {key: value for key, value in config_data.items() if value != base_data.get(key)}
