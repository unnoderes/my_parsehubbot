"""Worker 异步任务处理器测试(fake Redis, 不依赖外部服务)。"""

import asyncio
import json
import os
from typing import Any

os.environ.setdefault("BOT_TOKEN", "123456:worker-test-token")
os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "worker-test-hash")

from services.worker import (  # noqa: E402
    DONE_PREFIX,
    FAILED_QUEUE,
    RETRY_MAX,
    ParseWorker,
    _summarize,
)


class FakeRedis:
    """最小可用的异步 Redis 假实现, 只覆盖 Worker 用到的命令。"""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.published: list[tuple[str, str]] = []
        self.lists: dict[str, list[str]] = {}

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.store[key] = value
        return True

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        z = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            z[member] = score
        return len(mapping)

    async def zrangebyscore(self, key: str, lo: float, hi: float) -> list[str]:
        z = self.zsets.get(key, {})
        return [m for m, s in z.items() if lo <= s <= hi]

    async def zrem(self, key: str, member: str) -> int:
        z = self.zsets.get(key, {})
        if member in z:
            del z[member]
            return 1
        return 0

    async def brpop(self, key: str, timeout: int = 0) -> tuple[str, str] | None:
        items = self.lists.get(key)
        if not items:
            await asyncio.sleep(0)
            return None
        return key, items.pop()

    async def aclose(self) -> None:
        pass


def make_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "task_id": "t-1",
        "chat_id": 100,
        "message_id": 200,
        "user_id": 100,
        "url": "https://example.com/post/1",
        "bot_instance_id": "bot-a",
        "mode": "preview",
    }
    payload.update(overrides)
    return payload


def make_worker(fake: FakeRedis) -> ParseWorker:
    return ParseWorker("w-1", client=fake)


async def test_successful_task_publishes_result_and_marks_done() -> None:
    fake = FakeRedis()
    worker = make_worker(fake)

    async def fake_pipeline(payload: dict[str, Any]) -> str:
        return "内容"

    worker._run_pipeline = fake_pipeline  # type: ignore[method-assign]
    await worker.process_task(make_payload())

    assert fake.published, "结果应通过 Pub/Sub 发布"
    channel, raw = fake.published[0]
    assert channel == "result:bot-a"
    result = json.loads(raw)
    assert result["task_id"] == "t-1"
    assert result["status"] == "success"
    assert result["text"] == "内容"
    assert fake.store[f"{DONE_PREFIX}t-1"]


async def test_completed_task_is_not_parsed_twice() -> None:
    fake = FakeRedis()
    worker = make_worker(fake)

    calls = 0

    async def fake_pipeline(payload: dict[str, Any]) -> str:
        nonlocal calls
        calls += 1
        return "内容"

    worker._run_pipeline = fake_pipeline  # type: ignore[method-assign]
    await worker.process_task(make_payload())
    await worker.process_task(make_payload())

    assert calls == 1
    assert len(fake.published) == 2  # 第二次直接重发缓存结果


async def test_failed_task_enters_dead_letter_queue_then_alerts() -> None:
    fake = FakeRedis()
    worker = make_worker(fake)

    async def failing_pipeline(payload: dict[str, Any]) -> str:
        raise RuntimeError("boom")

    async def record_alert(payload: dict[str, Any], error: str) -> None:
        alerts.append((payload, error))

    alerts: list[tuple[dict[str, Any], str]] = []
    worker._run_pipeline = failing_pipeline  # type: ignore[method-assign]
    worker._alert = record_alert  # type: ignore[method-assign]

    await worker.process_task(make_payload())
    z = fake.zsets[FAILED_QUEUE]
    assert len(z) == 1
    queued = json.loads(next(iter(z)))
    assert queued["_attempt"] == 1

    # _attempt 达到 3 后, 第 4 次失败触发告警且不再入队
    for attempt in range(2, RETRY_MAX + 1):
        fake.zsets[FAILED_QUEUE].clear()  # 重试循环取出任务时先 zrem
        await worker.process_task(queued)
        queued = json.loads(next(iter(fake.zsets[FAILED_QUEUE])))
        assert queued["_attempt"] == attempt

    # 第 4 次失败(attempt=4)超过上限, 触发告警且不再入队
    fake.zsets[FAILED_QUEUE].clear()
    await worker.process_task(queued)
    assert not fake.zsets[FAILED_QUEUE]
    assert len(alerts) == 1
    assert alerts[0][1] == "boom"


async def test_timeout_task_is_marked_failed() -> None:
    from services.worker import WorkerTaskTimeout

    fake = FakeRedis()
    worker = make_worker(fake)

    async def timing_out(payload: dict[str, Any]) -> str:
        raise WorkerTaskTimeout("解析超时")

    worker._run_pipeline = timing_out  # type: ignore[method-assign]
    await worker.process_task(make_payload())

    assert FAILED_QUEUE in fake.zsets


async def test_malformed_payload_is_discarded() -> None:
    fake = FakeRedis()
    worker = make_worker(fake)
    await worker.process_task({"task_id": "only-id"})  # 缺 url
    await worker.process_task({})  # 全缺
    assert not fake.published
    assert not fake.zsets


def test_summarize_includes_title_content_source() -> None:
    class PR:
        title = "标题"
        content = "正文"
        raw_url = "https://example.com/x"

    text = _summarize(PR())
    assert "标题" in text
    assert "正文" in text
    assert "https://example.com/x" in text


def test_worker_id_distinguishes_instances() -> None:
    fake_a = FakeRedis()
    fake_b = FakeRedis()
    wa = ParseWorker("w-a", client=fake_a)
    wb = ParseWorker("w-b", client=fake_b)
    assert wa.worker_id != wb.worker_id
