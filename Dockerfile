FROM python:3.14-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install dependencies first (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --no-install-project

# Copy source and install project
COPY . .
RUN uv sync --no-dev

# Ensure writable data directory for tokens.json
RUN mkdir -p /data
ENV HUGBUCKET_TOKENS_FILE=/data/tokens.json

EXPOSE 9000 9001

ENTRYPOINT ["uv", "run", "hugbucket"]
