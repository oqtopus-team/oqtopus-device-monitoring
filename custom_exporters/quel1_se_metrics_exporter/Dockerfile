# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Create appuser with configurable UID and GID to avoid permission issues
# You can override these values at build time like this:
#   docker compose build --build-arg UID=$(id -u) --build-arg GID=$(id -g)
# This ensures that files created in volumes or bind mounts are owned by the correct host user
ARG UID=1000
ARG GID=1000

# Install the project into `/app`
WORKDIR /app

# Add user
RUN groupadd -g ${GID} appuser \
    && useradd -m -u ${UID} -g ${GID} -s /bin/bash -c "" appuser \
    && chown -R appuser:appuser /app
RUN mkdir -p /logs && chown -R appuser:appuser /logs
USER appuser

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy
ENV PYTHONUNBUFFERED=1

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
COPY --chown=appuser:appuser . /app
RUN --mount=type=cache,target=~/.cache/uv \
    uv sync --locked --no-dev

# Environment variables for configuration
ENV QUEL1SE_EXPORTER_CONFIG_PATH=/config/config.yaml
ENV QUEL1SE_EXPORTER_LOGGING_CONFIG_PATH=/config/logging.yaml
ENV QUEL1SE_EXPORTER_LOGGING_DIR_PATH=/logs

# execute the QuEL-1 SE metrics exporter
CMD ["uv", "run", "python", "src/quel1_se_metrics_exporter.py"]
