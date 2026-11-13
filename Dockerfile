FROM python:3.12-slim AS build

COPY --from=ghcr.io/astral-sh/uv:0.10.11 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    UV_HTTP_TIMEOUT=120 \
    UV_RETRIES=10

COPY pyproject.toml uv.lock ./

RUN mkdir -p /etc/apt/apt.conf.d \
    && printf 'Acquire::Retries "20";\nAcquire::http::Pipeline-Depth "0";\nAcquire::http::No-Cache "true";\nAcquire::Queue-Mode "access";\nAPT::Get::Fix-Missing "true";\n' > /etc/apt/apt.conf.d/99docker-net \
    && sed -i 's#http://deb.debian.org#http://mirrors.tuna.tsinghua.edu.cn#g; s#http://security.debian.org#http://mirrors.tuna.tsinghua.edu.cn/debian-security#g' /etc/apt/sources.list.d/debian.sources \
    && for i in $(seq 1 40); do \
        apt-get update && apt-get install -y --no-install-recommends gcc python3-dev && break; \
        echo "attempt $i failed"; sleep 20; \
    done \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-install-project --frozen --index-url https://pypi.tuna.tsinghua.edu.cn/simple

COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --index-url https://pypi.tuna.tsinghua.edu.cn/simple

FROM python:3.12-slim AS runtime

RUN mkdir -p /etc/apt/apt.conf.d \
    && printf 'Acquire::Retries "20";\nAcquire::http::Pipeline-Depth "0";\nAcquire::http::No-Cache "true";\nAcquire::Queue-Mode "access";\nAPT::Get::Fix-Missing "true";\n' > /etc/apt/apt.conf.d/99docker-net \
    && sed -i 's#http://deb.debian.org#http://mirrors.tuna.tsinghua.edu.cn#g; s#http://security.debian.org#http://mirrors.tuna.tsinghua.edu.cn/debian-security#g' /etc/apt/sources.list.d/debian.sources \
    && for i in $(seq 1 40); do \
        apt-get update && apt-get install -y --no-install-recommends \
            libglib2.0-0 libjemalloc2 ffmpeg media-types curl unzip ca-certificates && break; \
        echo "attempt $i failed"; sleep 20; \
    done \
    && curl -fsSL https://deno.land/install.sh | sh \
    && rm -rf /var/lib/apt/lists/*

ENV DENO_INSTALL="/root/.deno"
ENV PATH="/app/.venv/bin:$DENO_INSTALL/bin:$PATH"
ENV LD_PRELOAD=libjemalloc.so.2

WORKDIR /app
COPY --from=build /app /app
COPY --from=build /bin/uv /bin/uvx /usr/local/bin/

CMD ["python", "bot.py"]
