"""Worker 解析结果回调处理。

收到 Worker 通过 Pub/Sub 推送的结果后, 编辑占位消息展示解析内容。
多 Bot 实例场景下, 消息由创建占位消息的实例(通过频道隔离)更新。

Worker 结果负载协议(见 UNI-46 worker.py):
{
    "task_id": str,
    "bot_instance_id": str,
    "status": "success" | "error" | "timeout",
    "error": str | None,
    "media": [...] | None,   # file_id 列表(成功时有)
    "text": str | None,      # 文本结果(富文本/Telegraph 链接)
}
"""

from typing import Any

from pyrogram import Client
from pyrogram.errors import MessageNotModified

from log import logger

logger = logger.bind(name="QueueResult")


async def handle_queue_result(cli: Client, result: dict[str, Any]) -> None:
    task_id = result.get("task_id")
    chat_id = result.get("chat_id")
    placeholder_id = result.get("placeholder_message_id")
    status = result.get("status")

    if not task_id or chat_id is None or placeholder_id is None:
        logger.warning(f"结果负载缺少必要字段: {result}")
        return

    logger.debug(f"收到解析结果: task_id={task_id}, status={status}")

    try:
        if status == "success":
            text = result.get("text") or "解析完成"
            media = result.get("media")
            if media:
                # 媒体消息: Worker 已上传到 Telegram, 这里编辑占位为文本提示
                await cli.edit_message_text(chat_id, placeholder_id, str(text))
            else:
                await cli.edit_message_text(chat_id, placeholder_id, str(text))
        elif status == "timeout":
            await cli.edit_message_text(chat_id, placeholder_id, "解析超时, 请稍后重试")
        else:
            error = result.get("error") or "解析失败"
            await cli.edit_message_text(chat_id, placeholder_id, f"解析失败: {error}")
    except MessageNotModified:
        pass
    except Exception as e:
        logger.warning(f"编辑占位消息失败: task_id={task_id}, error={e}")
