# syntax=docker/dockerfile:1.7
# One image for the worker, the API and the migration job.

ARG PYTHON_VERSION=3.13

# ---------------------------------------------------------------- build wheels
FROM python:${PYTHON_VERSION}-slim AS builder
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY requirements.txt .
RUN pip wheel --wheel-dir /wheels -r requirements.txt

# ---------------------------------------------------------------- runtime
FROM python:${PYTHON_VERSION}-slim
ARG INSTALL_GIT=true

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    CONFIG_DIR=/app/config \
    DATA_DIR=/app/data/generated \
    GIT_PUBLISH_WORKDIR=/app/data/git-publish

# git is only needed for the optional snapshot publication to a git branch
RUN set -eux; \
    if [ "$INSTALL_GIT" = "true" ]; then \
      apt-get update; \
      apt-get install -y --no-install-recommends git ca-certificates; \
      rm -rf /var/lib/apt/lists/*; \
    fi; \
    groupadd --system --gid 10001 app; \
    useradd --system --uid 10001 --gid app --home-dir /app --shell /usr/sbin/nologin app

COPY --from=builder /wheels /wheels
RUN pip install --no-index --find-links=/wheels /wheels/*.whl && rm -rf /wheels

WORKDIR /app
COPY --chown=app:app alembic.ini ./
COPY --chown=app:app config ./config
COPY --chown=app:app src ./src
RUN python -m compileall -q src \
 && mkdir -p /app/data/generated /app/data/git-publish \
 && chown -R app:app /app/data

USER app
EXPOSE 8000 8081

# overridden per service in docker-compose.yml (worker | api | migrate)
CMD ["python", "-m", "proxy_quality", "api"]
