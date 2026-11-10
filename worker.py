"""Worker 进程入口。

环境变量:
- WORKER_ID: 实例标识(未配置时用 主机名-随机后缀 自动生成)
- REDIS_URL: Redis 连接地址(必填, 未配置时退出)
- 其余沿用 Bot 配置(数据库/平台配置/下载目录等)
"""

import os
import socket
import sys
import uuid

from core import bs
from log import logger, setup_logging
from services.worker import ParseWorker
from utils.event_loop import setup_optimized_event_loop


def main() -> None:
    setup_logging(debug=bs.debug)
    setup_optimized_event_loop()

    if not bs.redis_url:
        logger.error("未配置 REDIS_URL, Worker 无法启动")
        sys.exit(1)

    worker_id = os.getenv("WORKER_ID") or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
    logger.success(f"Worker 实例启动: worker_id={worker_id}")

    worker = ParseWorker(worker_id)
    try:
        worker.connect_sync()
        worker.run_sync()
    except KeyboardInterrupt:
        logger.info("收到退出信号, Worker 停止")


if __name__ == "__main__":
    main()
