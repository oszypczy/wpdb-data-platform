# Streamlit dashboard (docs/design/07-dashboard.md). Owner: analytics-dashboard.
# Same base and tooling as the other Python images: dependencies locked in uv.lock
# (uv sync --frozen), no project install. Reads only the gold/ops Delta tables in MinIO
# (delta-rs + DuckDB on Arrow, no JVM); the Brazil GeoJSON is vendored in assets/.
FROM python:3.12.15-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.9.18 /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:$PATH \
    TZ=UTC

WORKDIR /app
COPY pyproject.toml uv.lock ./
# the uv cache stays in a BuildKit cache mount, not in the image (~0.6 GB less)
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project

COPY .streamlit ./.streamlit
COPY assets ./assets
COPY dashboard ./dashboard
COPY views ./views
COPY app.py healthcheck.py ./

# Streamlit writes ~/.streamlit (machine id), so the user gets a home directory
RUN useradd --system --uid 1001 --create-home dashboard
USER dashboard

EXPOSE 8501
CMD ["streamlit", "run", "app.py"]
