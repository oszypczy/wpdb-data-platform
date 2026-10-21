# Validating AVRO consumer (docs/design/04-kafka.md §6). Owner: database-cdc.
# Same base and tooling as the simulator image: dependencies locked in uv.lock
# (uv sync --frozen), no project install (the package is run as a module).
FROM python:3.12.15-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.9.18 /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:$PATH

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY consumer ./consumer

RUN useradd --system --uid 1001 --no-create-home consumer
USER consumer

ENTRYPOINT ["python", "-m", "consumer"]
