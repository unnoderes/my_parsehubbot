from pyrogram import Client
from pyrogram.errors import RPCError

from core import bs
from db import get_session
from i18n import t_
from log import logger
from plugins.admin.render import build_cookie_alert
from services import CookieAlert, UserService

logger = logger.bind(name="CookieAlert")


async def send_cookie_alert(cli: Client, alert: CookieAlert) -> None:
    for uid in bs.admin_ids:
        async with get_session() as session:
            _t = t_[await UserService(session).get_lang(uid)]
        try:
            await cli.send_rich_message(uid, build_cookie_alert(_t, alert))
        except RPCError as e:
            logger.warning(f"向管理员 {uid} 发送 cookie 失效通知失败: {e}")


async def send_text_alert(cli: Client, text: str) -> None:
    """向管理员发送纯文本告警(Worker 死信重试耗尽等)。"""
    for uid in bs.admin_ids:
        try:
            await cli.send_message(uid, text)
        except RPCError as e:
            logger.warning(f"向管理员 {uid} 发送告警失败: {e}")


async def send_cookie_alert_from_payload(cli: Client, alert: dict) -> None:
    """Worker 经 Redis 转发的 cookie 告警负载, 由 Bot 代发给管理员。

    Worker 不持有 Telegram 客户端, 只把告警所需字段序列化后转发;
    Bot 在此重建 CookieAlert 并使用自身连接发送, 复用现有渲染逻辑。
    """
    from services.cookie_health import CookieStat

    platform_id = alert.get("platform_id", "")
    cookie = alert.get("cookie", "")
    url = alert.get("url", "")
    stat = CookieStat(
        fail_streak=int(alert.get("fail_streak", 0) or 0),
        last_error=alert.get("last_error"),
    )
    cookie_alert = CookieAlert(platform_id=platform_id, cookie=cookie, stat=stat, url=url)
    for uid in bs.admin_ids:
        async with get_session() as session:
            _t = t_[await UserService(session).get_lang(uid)]
        try:
            await cli.send_rich_message(uid, build_cookie_alert(_t, cookie_alert))
        except RPCError as e:
            logger.warning(f"向管理员 {uid} 发送 cookie 失效通知失败: {e}")
