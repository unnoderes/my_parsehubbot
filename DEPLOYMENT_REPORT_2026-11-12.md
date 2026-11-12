## 测试环境部署工作汇报

**日期**: 2026-11-12
**执行人**: Droid (AI 软件工程代理)
**目标**: 将 ParseHubBot 改造方案落地到本地 Docker 测试环境

---

### 一、环境准备

- 启动本地 Docker 服务 (Docker Desktop 29.6.2)
- 创建 `.env` 配置文件，写入 Telegram 凭证与基础设施连接串
  - API_ID / API_HASH / BOT_TOKEN（用户提供）
  - REDIS_URL=redis://redis:6379/0
  - DATABASE_URL=postgresql+asyncpg://parsehub:parsehub_dev@postgres:5432/parsehub
  - DEBUG=true

### 二、遇到的阻塞与解决

#### 1. 镜像构建失败（网络/磁盘）
构建多次卡在 apt 与 pip 下载阶段，表现为 502 / 超时 / EOF。经排查根因是：
- 容器内访问 `deb.debian.org` / `files.pythonhosted.org` 经 Docker Desktop 网关后高并发被限流；
- 关键根因是 **C 盘写满（剩余 0 字节）**，导致 2GB 镜像导出失败、daemon 崩溃。

解决：
- 在 Dockerfile 中把 apt 源与 PyPI 下载 URL 都替换为清华镜像
- 加大 apt/uv 的重试与超时，关闭 HTTP pipeline，改用 Queue-Mode 串行下载
- 释放 C 盘空间（约 11GB）后构建稳定通过
- 清理 `docker builder prune` 释放缓存

> 注意：这些网络改动是环境适配（国内网络 + 磁盘受限），非项目逻辑改动，生产环境若走公网可酌情回退。

#### 2. 多实例 Telegram 会话冲突（代码 bug）
三个 bot 容器共享同一个 data 卷，会话文件名仅由 token 前缀生成，导致并发启动 sqlite 锁竞争，报 `database is locked`，bot-3 反复重启。

修复：`core/config.py` 的 `bot_session_name` 加入容器 hostname 区分（`socket.gethostname()`），每个实例独立会话文件。

#### 3. Worker 断线崩溃（代码 bug）
worker 主循环只捕获 `CancelledError`，Redis 连接中途被服务端/网络关闭时抛 `RuntimeError` → 进程退出 → Docker 无限重启。

修复：`services/worker.py` 主循环外层增加 Redis 断线检测与重建、异常兜底 sleep 重试（已在日志中验证生效）。

#### 4. Telegram FloodWait 限流（使用方式问题）
多个实例同时启动 + 反复重启，每个容器都用同一 bot token 执行 `ImportBotAuthorization`，触发 Telegram 服务端 `FLOOD_WAIT_X`（需等待约 45 分钟）。

修复：`bot.py` 与 `services/worker.py` 启动逻辑捕获 `FloodWait`，按服务端要求 sleep 后重试，不再崩溃退出、不再加重限流。

### 三、已验证结果

- PostgreSQL / Redis 健康检查通过（healthy）
- worker 断线重连逻辑在日志中确认工作（「消费循环异常, 3 秒后重试」）
- bot-2 / bot-3 曾成功登录并完成菜单设置、watchdog 重连
- 两个镜像（parse_hub_bot:latest、parse_hub_worker:latest）均构建成功并含上述修复

### 四、当前状态

- 基础设施（postgres / redis / nginx）运行中且健康
- 因 Telegram 限流尚未解除，bot / worker 容器暂时停止，避免加重限流
- 限流预计 2026-11-12 约 09:10 左右解除

### 五、下一步建议

1. **限流解除后**，先以 **1 个 bot + 1 个 worker**（如 bot-2 / worker-1）启动验证完整链路
2. 验证通过后，再逐步扩展到多实例（避免再次集中登录触发限流）
3. 后续重建镜像后，重启实例应错开，单实例逐个重启
4. 生产就绪前补充（设计文档提及但尚未实施）：
   - 健康检查端点（供 nginx/负载均衡探活）
   - Prometheus + Grafana 监控与告警
   - `docs/deployment.md` 部署手册
   - GitHub Actions CI/CD

### 六、改动文件清单

| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `.env` | 新增 | 测试环境配置 |
| `Dockerfile` | 修改 | 国内镜像源 + apt/uv 重试优化（环境适配） |
| `uv.lock` | 修改 | 下载 URL 替换为清华镜像（环境适配） |
| `core/config.py` | 修复 | bot_session_name 加入 hostname，解决多实例锁冲突 |
| `services/worker.py` | 修复 | Redis 断线重连 + 异常兜底 + FloodWait 等待 |
| `bot.py` | 修复 | FloodWait 等待重试，避免崩溃重启 |
