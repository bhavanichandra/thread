FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY thread_platform/pyproject.toml thread_platform/uv.lock ./
RUN uv sync --no-dev --frozen
COPY thread_platform/ ./thread_platform/
EXPOSE 9000
CMD ["uv", "run", "uvicorn", "thread_platform.main:app", "--host", "0.0.0.0", "--port", "9000"]
