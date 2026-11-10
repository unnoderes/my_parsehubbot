"""Redis 任务队列客户端。

Bot 与 Worker 解耦的消息队列协议:
- 任务负载序列化为 JSON 后 LPUSH 到 `parse_tasks` 队列
- 使用 SETNX + TTL 做幂等检查, 避免重复解析相同链接
- Worker 完成后 PUBLISH 结果到 `result:{bot_instance_id}` 频道
"""

import asyncio
import hashlib
import json
import socket
import uuid
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as aioredis

from core import bs
from log import logger

logger = logger.bind(name="RedisQueue")

PARSE_TASKS_QUEUE = "parse_tasks"
IDEMPOTENCY_TTL = 300  # 幂等键有效期(秒), 短时间内相同 URL 不重复入队
TASK_TIMEOUT = 60  # Worker 超时时间(秒), 超时后 Bot 编辑占位消息


class RedisQueueUnavailable(Exception):
    """Redis 不可用, 调用方应降级为同步解析。"""


def bot_instance_id() -> str:
    """当前 Bot 实例唯一标识, 用于结果 Pub/Sub 频道隔离。"""
    if not hasattr(bot_instance_id, "_id"):
        bot_instance_id._id = socket.gethostname() + "-" + uuid.uuid4().hex[:8]  # type: ignore[attr-defined]
    return bot_instance_id._id  # type: ignore[attr-defined]


def result_channel(instance_id: str | None = None) -> str:
    return f"result:{instance_id or bot_instance_id()}"


class ParseTaskPayload(dict):
    """任务负载字典, 序列化为 JSON 后入队。"""

    pass


def build_task_payload(
    *,
    chat_id: int | None,
    message_id: int | None,
    url: str,
    user_id: int | None,
    placeholder_message_id: int | None,
    mode: str,
    bypass_cache: bool,
    custom_content: str = "",
    delete_share_url_msg: bool = False,
) -> dict[str, Any]:
    return {
        "task_id": str(uuid.uuid4()),
        "bot_instance_id": bot_instance_id(),
        "chat_id": chat_id,
        "message_id": message_id,
        "placeholder_message_id": placeholder_message_id,
        "url": url,
        "user_id": user_id,
        "mode": mode,
        "bypass_cache": bypass_cache,
        "custom_content": custom_content,
        "delete_share_url_msg": delete_share_url_msg,
        "created_at": datetime.now(UTC).isoformat(),
    }


def url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


class RedisQueue:
    """异步 Redis 队列客户端, 管理连接、入队、幂等和结果订阅。"""

    def __init__(self) -> None:
        self._client: aioredis.Redis | None = None
        self._pubsub: aioredis.client.PubSub | None = None
        self._listener_task: asyncio.Task[None] | None = None
        self._result_handlers: list[Any] = []

    @property
    def connected(self) -> bool:
        return self._client is not None

    async def connect(self) -> None:
        """初始化 Redis 连接并做健康检查, 失败时抛出 RedisQueueUnavailable。"""
        try:
            client = aioredis.from_url(
                bs.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            await client.ping()
        except Exception as e:
            raise RedisQueueUnavailable(f"Redis 连接失败: {e}") from e
        self._client = client
        logger.success(f"Redis 连接成功: {bs.redis_url}")

    async def close(self) -> None:
        await self.stop_result_listener()
        if self._pubsub is not None:
            try:
                await self._pubsub.aclose()
            except Exception as e:
                logger.debug(f"关闭 Pub/Sub 失败: {e}")
            self._pubsub = None
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception as e:
                logger.debug(f"关闭 Redis 连接失败: {e}")
            self._client = None
        logger.info("Redis 连接已关闭")

    async def enqueue(self, payload: dict[str, Any], *, idempotency_key: str | None = None) -> bool:
        """LPUSH 任务到队列, 返回是否入队成功。

        幂等检查: 同一 idempotency_key 在 IDEMPOTENCY_TTL 内只允许入队一次。
        Redis 不可用时抛出 RedisQueueUnavailable。
        """
        if self._client is None:
            raise RedisQueueUnavailable("Redis 未连接")
        key = f"parse_idem:{idempotency_key or url_hash(payload['url'])}"
        try:
            # SETNX + TTL: 已存在说明短时间已入队过, 跳过
            acquired = await self._client.set(key, payload["task_id"], ex=IDEMPOTENCY_TTL, nx=True)
        except Exception as e:
            raise RedisQueueUnavailable(f"Redis 幂等检查失败: {e}") from e
        if not acquired:
            logger.debug(f"幂等命中, 跳过入队: key={key}")
            return False

        try:
            await self._client.lpush(PARSE_TASKS_QUEUE, json.dumps(payload, ensure_ascii=False))
        except Exception as e:
            # 入队失败要释放幂等键, 允许重试
            try:
                await self._client.delete(key)
            except Exception:
                pass
            raise RedisQueueUnavailable(f"Redis 入队失败: {e}") from e
        logger.debug(f"任务已入队: task_id={payload['task_id']}, url={payload['url']}")
        return True

    def on_result(self, handler: Any) -> None:
        """注册结果回调 handler(result: dict) -> Awaitable[None]"""
        self._result_handlers.append(handler)

    async def start_result_listener(self) -> None:
        """订阅本实例的结果频道, 后台任务分发回调。"""
        if self._client is None:
            raise RedisQueueUnavailable("Redis 未连接")
        if self._listener_task is not None:
            return
        self._pubsub = self._client.pubsub()
        await self._pubsub.subscribe(result_channel())
        self._listener_task = asyncio.create_task(self._listen_results())
        logger.info(f"已订阅结果频道: {result_channel()}")

    async def stop_result_listener(self) -> None:
        if self._listener_task is not None:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except (asyncio.CancelledError, Exception):
                pass
            self._listener_task = None

    async def _listen_results(self) -> None:
        assert self._pubsub is not None
        while True:
            try:
                message = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message is None or message["type"] != "message":
                    continue
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                result = json.loads(data)
                for handler in self._result_handlers:
                    try:
                        await handler(result)
                    except Exception as e:
                        logger.opt(exception=e).error("结果回调处理异常")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"结果监听异常, 1s 后继续: {e}")
                await asyncio.sleep(1)


redis_queue = RedisQueue()


async def init_queue(cli: Any) -> None:
    """Bot 启动时初始化队列: REDIS_URL 未配置或连接失败时降级为同步解析。"""
    if not bs.redis_url:
        logger.info("未配置 REDIS_URL, 队列已禁用, 使用同步解析")
        return
    try:
        await redis_queue.connect()
    except RedisQueueUnavailable as e:
        logger.warning(f"{e}, 队列已禁用, 降级为同步解析")
        return
    redis_queue.on_result(make_result_handler(cli))
    await redis_queue.start_result_listener()


async def close_queue() -> None:
    await redis_queue.close()


def make_result_handler(cli: Any) -> Any:
    """构建 Worker 结果回调: 收到结果后编辑占位消息发送解析内容。

    结果负载协议(由 Worker 定义, 见 UNI-46):
    {task_id, status: "success"|"error"|"timeout", ...}
    """
    from plugins.parse.queue_result import handle_queue_result

    async def handler(result: dict[str, Any]) -> None:
        await handle_queue_result(cli, result)

    return handler
