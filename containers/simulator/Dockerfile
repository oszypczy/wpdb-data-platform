# Business simulator (docs/design/01-business-system.md). Owner: business-simulator.
# Dependencies are locked in uv.lock (uv sync --frozen); the Olist CSVs and the truth
# views SQL are bind-mounted by docker-compose.yml, not copied into the image.
FROM python:3.12.15-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.9.18 /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:$PATH \
    SIM_HEARTBEAT_FILE=/tmp/simulator/heartbeat

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY simulator ./simulator
COPY healthcheck.py ./

RUN useradd --system --uid 1001 --no-create-home simulator \
    && mkdir -p /tmp/simulator && chown simulator /tmp/simulator
USER simulator

CMD ["python", "-m", "simulator"]
