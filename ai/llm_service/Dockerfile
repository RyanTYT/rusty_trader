# Stage 1: Build stage
FROM python:3.12-slim-bullseye AS build

WORKDIR /app

# Copy the uv binary from the official image
# This avoids 'manifest unknown' because we pull the binary, not the whole OS layer
COPY --from=ghcr.io/astral-sh/uv:0.6.0 /uv /uvbin/uv

# Put uv on the path
ENV PATH="/uvbin:${PATH}"
ENV UV_COMPILE_BYTECODE=1

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Sync dependencies 
# Using '--no-install-project' to cache the heavy download layer
RUN uv sync --frozen --no-install-project --no-dev

################################################################################
# Stage 2: Runtime stage
FROM python:3.12-slim-bullseye AS final

WORKDIR /app

# Fix for older Docker compatibility (seccomp/faccessat2 issues)
RUN echo 'APT::Update::Post-Invoke {"/bin/true"};' > /etc/apt/apt.conf.d/99fix-bad-proxy && \
    apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    rm -f /etc/apt/apt.conf.d/99fix-bad-proxy

# Copy the virtual environment
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy source code
COPY . .

# Security: Non-root user
RUN useradd -ms /bin/bash appuser && \
    chown -R appuser:appuser /app
RUN mkdir -p /data/settings && \
    chown -R appuser:appuser /app /data && \
    chmod -R 755 /data

USER appuser

EXPOSE 8001
ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
