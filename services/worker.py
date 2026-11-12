"""Worker 异步任务处理服务。

从 Redis 队列取出 Bot 入队的解析任务, 调用现有解析流水线执行实际解析与下载,
将结果持久化并经由 Redis Pub/Sub 通知对应的 Bot 实例。

设计约束(见 UNI-46):
- 复用 core/queue.py 的任务负载字段与 result:{bot_instance_id} 通知频道
- 复用 services.pipeline.ParsePipeline, 不重复解析/下载逻辑
- 幂等: 相同 task_id 或 url_hash 的结果只解析一次, 完成的任务直接重发结果
- 失败时进入死信队列 parse_tasks_failed, 每 5 分钟重试, 最多 3 次, 之后告警
- 多 Worker 实例通过 WORKER_ID 区分, 均可安全阻塞消费与认领
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import redis.asyncio as aioredis
from pyrogram import Client

from core import bs
from core.queue import PARSE_TASKS_QUEUE, TASK_TIMEOUT, url_hash
from i18n import t_
from log import logger
from plugins.admin.alert import send_cookie_alert
from plugins.parse.context import ParseOptions
from repo.settings import ParseMode
from services import cookie_health, persistent_cache
from services.pipeline import ParsePipeline

logger = logger.bind(name="Worker")

DONE_PREFIX = "parse_done:"
URL_DONE_PREFIX = "parse_done_url:"
FAILED_QUEUE = "parse_tasks_failed"
RETRY_MAX = 3
RETRY_INTERVAL = 300  # 死信队列重试间隔(秒)
RESULT_TTL = 3600  # 完成结果缓存有效期(秒)


class WorkerError(Exception):
    """Worker 任务处理错误基类。"""


class WorkerTaskTimeout(WorkerError):
    """解析超过 task_timeout 上限。"""


class WorkerParseError(WorkerError):
    """流水线未产出结果。"""


class _NullReporter:
    """解析流水线状态上报的无操作实现(Worker 不直接对用户展示进度)。"""

    async def report(self, text: str) -> None: ...

    async def report_error(self, stage: str, error: Exception) -> None: ...

    async def dismiss(self) -> None: ...


def _mode_from_str(mode: str | None) -> ParseMode:
    try:
        return ParseMode(mode) if mode else ParseMode.PREVIEW
    except ValueError:
        return ParseMode.PREVIEW


def _summarize(parse_result: Any) -> str:
    """从解析结果提取可序列化的文本摘要(标题 + 内容 + 来源)。"""
    title = getattr(parse_result, "title", "") or ""
    content = getattr(parse_result, "content", "") or getattr(parse_result, "markdown_content", "") or ""
    raw_url = getattr(parse_result, "raw_url", "") or ""
    parts: list[str] = []
    if title:
        parts.append(f"**{title}**")
    if content:
        parts.append(content)
    body = "\n\n".join(parts).strip()
    if raw_url:
        body += f"\n\n<a href='{raw_url}'>Source</a>"
    return body


class ParseWorker:
    """Redis 解析任务消费者。"""

    def __init__(self, worker_id: str, client: Any | None = None) -> None:
        self.worker_id = worker_id
        self._client: Any = client or aioredis.from_url(
            bs.redis_url, decode_responses=True, socket_connect_timeout=5, socket_timeout=5
        )
        self._running = False
        self._retry_task: asyncio.Task[None] | None = None
        self._cli: Client | None = None

    async def connect(self) -> None:
        await self._client.ping()

    def connect_sync(self) -> None:
        import asyncio

        asyncio.run(self.connect())

    def run_sync(self) -> None:
        import asyncio

        asyncio.run(self.run())

    async def run(self) -> None:
        """主循环: 阻塞消费任务队列 + 后台死信重试。"""
        logger.success(f"Worker 启动: worker_id={self.worker_id}")

        from db.init import init_db

        await init_db()
        self._cli = Client(
            f"worker_{self.worker_id}",
            api_id=bs.api_id,
            api_hash=bs.api_hash,
            bot_token=bs.bot_token,
            workdir=str(bs.sessions_path),
        )
        # Telegram 对同一 bot token 的登录有频率限制, 多实例/反复重启会触发 FloodWait.
        # 按服务端要求 sleep 后重试, 避免进程崩溃被 Docker 反复重启加重限流
        from pyrogram.errors import FloodWait as _FloodWait

        for _att in range(30):
            try:
                await self._cli.start()
                break
            except _FloodWait as _fe:
                _wait = int(getattr(_fe, "value", 60) or 60)
                logger.warning(f"Telegram 登录限流, 等待 {_wait} 秒后重试 (第 {_att + 1}/30 次)")
                await asyncio.sleep(min(_wait, 300))
        cli = self._cli
        cookie_health.set_notifier(lambda alert: send_cookie_alert(cli, alert))
        self._running = True
        self._retry_task = asyncio.create_task(self._retry_loop())
        self._running = True
        # 连接可能在中途被 Redis 服务端/网络抖动关闭, 这里整体捕获连接类异常,
        # 重建连接后继续消费, 避免进程退出导致 Docker 反复重启 (设计文档"网络抖动"风险预案)
        try:
            while self._running:
                try:
                    try:
                        await self._client.ping()
                    except Exception:  # noqa: BLE001
                        logger.warning("Redis 连接已断开, 重建连接")
                        try:
                            await self._client.aclose()
                        except Exception:  # noqa: BLE001
                            pass
                        self._client = aioredis.from_url(
                            bs.redis_url,
                            decode_responses=True,
                            socket_connect_timeout=5,
                            socket_timeout=5,
                        )
                        await self._client.ping()
                        logger.success("Redis 连接已恢复")
                    item = await self._client.brpop(PARSE_TASKS_QUEUE, timeout=5)
                    if item is None:
                        continue
                    _, raw = item
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        logger.warning("丢弃无法解析的任务负载")
                        continue
                    await self.process_task(payload)
                except asyncio.CancelledError:
                    break
                except Exception as e:  # noqa: BLE001
                    logger.exception(e)
                    logger.warning("消费循环异常, 3 秒后重试")
                    await asyncio.sleep(3)
        finally:
            self._running = False
            if self._retry_task is not None:
                self._retry_task.cancel()
            if self._cli is not None:
                try:
                    await self._cli.stop()
                except Exception as e:  # noqa: BLE001
                    logger.debug(f"关闭 Telegram 客户端失败: {e}")
            try:
                await self._client.aclose()
            except Exception as e:  # noqa: BLE001
                logger.debug(f"关闭 Redis 连接失败: {e}")

    async def _retry_loop(self) -> None:
        """每 RETRY_INTERVAL 秒扫描一次死信队列, 处理到期的重试任务。"""
        while self._running:
            await asyncio.sleep(RETRY_INTERVAL)
            now = time.time()
            due = await self._client.zrangebyscore(FAILED_QUEUE, 0, now)
            for raw in due:
                await self._client.zrem(FAILED_QUEUE, raw)
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning("丢弃无法解析的死信任务")
                    continue
                await self.process_task(payload)

    async def process_task(self, payload: dict[str, Any]) -> None:
        """处理单条任务: 幂等检查后执行解析或重发已完成结果。"""
        task_id = payload.get("task_id")
        url = payload.get("url")
        if not task_id or not url:
            logger.warning(f"任务缺少必要字段, 丢弃: {payload}")
            return
        if not isinstance(task_id, str) or not isinstance(url, str):
            logger.warning(f"任务字段类型异常, 丢弃: {payload}")
            return
        if await self._is_done(task_id, url):
            logger.debug(f"任务已完成, 跳过并重新通知: task_id={task_id}")
            await self._republish_done(payload)
            return
        await self._execute(payload)

    async def _execute(self, payload: dict[str, Any]) -> None:
        try:
            text = await self._run_pipeline(payload)
        except WorkerTaskTimeout as e:
            await self._fail(payload, str(e))
            return
        except Exception as e:  # noqa: BLE001
            logger.exception(e)
            await self._fail(payload, str(e))
            return
        await self._store_result(payload, text)
        result = self._build_result(payload, "success", text=text)
        await self._mark_done(payload["task_id"], payload["url"], result)
        await self._publish(result)
        logger.success(f"任务完成: task_id={payload.get('task_id')}")

    async def _run_pipeline(self, payload: dict[str, Any]) -> str:
        """运行解析流水线并返回文本摘要。失败/超时以异常表达。"""
        url = payload["url"]
        mode = _mode_from_str(payload.get("mode", "preview"))
        options = ParseOptions.from_mode(mode, bypass_cache=bool(payload.get("bypass_cache", False)))
        reporter = _NullReporter()
        pipeline = ParsePipeline(
            url,
            raw_url=url,
            reporter=reporter,
            singleflight=options.singleflight,
            skip_media_processing=options.skip_media_processing,
            gif_only_skip_download_count_threshold=options.gif_only_skip_download_count_threshold,
            save_metadata=options.save_metadata,
            t=t_,  # type: ignore[arg-type]
        )
        try:
            async with asyncio.timeout(bs.task_timeout or TASK_TIMEOUT):
                result = await pipeline.run()
        except TimeoutError as e:
            raise WorkerTaskTimeout("解析超时") from e
        finally:
            pipeline.finish()
            pipeline.cleanup()
        if result is None:
            raise WorkerParseError("解析失败, 流水线返回空")
        return _summarize(result.parse_result)

    async def _store_result(self, payload: dict[str, Any], text: str) -> None:
        """将解析结果持久化到 PostgreSQL(复用 cache 表, 以 worker-result://task_id 为键)。"""
        from services.cache import CacheEntry, CacheParseResult

        entry = CacheEntry(parse_result=CacheParseResult(title="", content=text))
        key_url = f"worker-result://{payload['task_id']}"
        try:
            await persistent_cache.set(key_url, entry)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"结果持久化失败: {e}")

    def _build_result(
        self,
        payload: dict[str, Any],
        status: str,
        *,
        text: str | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        return {
            "task_id": payload.get("task_id"),
            "chat_id": payload.get("chat_id"),
            "message_id": payload.get("message_id"),
            "user_id": payload.get("user_id"),
            "placeholder_message_id": payload.get("placeholder_message_id"),
            "bot_instance_id": payload.get("bot_instance_id"),
            "url": payload.get("url"),
            "status": status,
            "text": text,
            "media": None,
            "error": error,
        }

    def _result_channel(self, bot_instance_id: str | None) -> str:
        return f"result:{bot_instance_id}" if bot_instance_id else "result:none"

    async def _publish(self, result: dict[str, Any]) -> None:
        channel = self._result_channel(result.get("bot_instance_id"))
        await self._client.publish(channel, json.dumps(result, ensure_ascii=False))

    async def _is_done(self, task_id: str, url: str) -> bool:
        if await self._client.get(f"{DONE_PREFIX}{task_id}"):
            return True
        if await self._client.get(f"{URL_DONE_PREFIX}{url_hash(url)}"):
            return True
        return False

    async def _mark_done(self, task_id: str, url: str, result: dict[str, Any]) -> None:
        result_json = json.dumps(result, ensure_ascii=False)
        await self._client.set(f"{DONE_PREFIX}{task_id}", result_json, ex=RESULT_TTL)
        await self._client.set(f"{URL_DONE_PREFIX}{url_hash(url)}", result_json, ex=RESULT_TTL)

    async def _republish_done(self, payload: dict[str, Any]) -> None:
        task_id = payload.get("task_id")
        url = payload.get("url")
        if not isinstance(task_id, str) or not isinstance(url, str):
            return
        raw = await self._client.get(f"{DONE_PREFIX}{task_id}") or await self._client.get(
            f"{URL_DONE_PREFIX}{url_hash(url)}"
        )
        if raw:
            await self._publish(json.loads(raw))

    async def _fail(self, payload: dict[str, Any], error: str) -> None:
        attempt = int(payload.get("_attempt", 0)) + 1
        payload = {**payload, "_attempt": attempt, "_last_error": error}
        if attempt > RETRY_MAX:
            logger.error(
                f"任务重试耗尽, 发送告警: task_id={payload.get('task_id')}, url={payload.get('url')}, error={error}"
            )
            await self._alert(payload, error)
            return
        score = time.time() + RETRY_INTERVAL
        await self._client.zadd(FAILED_QUEUE, {json.dumps(payload, ensure_ascii=False): score})
        logger.warning(f"任务进入死信队列(第 {attempt} 次重试): task_id={payload.get('task_id')}")

    async def _alert(self, payload: dict[str, Any], error: str) -> None:
        """重试耗尽后告警: 通过 /admin 通知管理员。

        复用 Bot 的告警通道, 将失败任务作为 CookieAlert 同级的管理通知送达 admin_ids。
        """
        message = (
            f"[Worker 告警] 任务解析失败且重试耗尽\n"
            f"task_id={payload.get('task_id')}\n"
            f"url={payload.get('url')}\n"
            f"error={error}"
        )
        logger.error(message)
        for uid in bs.admin_ids:
            try:
                if self._cli is None:
                    break
                await self._cli.send_message(uid, message)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"向管理员 {uid} 发送 Worker 告警失败: {e}")
