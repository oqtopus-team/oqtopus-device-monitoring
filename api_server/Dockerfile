# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim


# Create appuser with configurable UID and GID to avoid permission issues
# You can override these values at build time like this:
#   docker compose build --build-arg UID=$(id -u) --build-arg GID=$(id -g)
# This ensures that files created in volumes or bind mounts are owned by the correct host user
ARG UID=1000
ARG GID=1000
# Install the project into `/app`
WORKDIR /app

# Install the project's dependencies using the lockfile and settings
RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Add user
RUN groupadd -g ${GID} appuser \
    && useradd -m -u ${UID} -g ${GID} -s /bin/bash -c "" appuser \
    && chown -R appuser:appuser /app
RUN mkdir -p /app/logs /app/data && chown -R appuser:appuser /app/logs /app/data
USER appuser

# Copy necessary files and install dependencies
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./
RUN uv sync --frozen --no-dev

# Run the application
CMD ["uv", "run", "python", "-m", "api_server"]
