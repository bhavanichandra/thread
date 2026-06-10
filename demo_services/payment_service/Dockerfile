FROM python:3.13.3-slim

# Install uv — pinned to match order-service for reproducible builds
COPY --from=ghcr.io/astral-sh/uv:0.5.1 /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first for better layer caching

COPY pyproject.toml uv.lock ./

RUN uv sync --no-dev --frozen

COPY . .

EXPOSE 8002

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]