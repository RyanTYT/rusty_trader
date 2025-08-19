# # Start from an official Python base image
# FROM --platform=linux/amd64 python:3.11-slim
#
# # Install system dependencies (if needed)
# RUN apt-get update && apt-get install -y \
#     build-essential \
#     unzip \
#     xvfb \
#     libxtst6 \
#     libxi6 \
#     libxrender1 \
#     libxrandr2 \
#     libfreetype6 \
#     libxext6 \
#     libx11-6 \
#     libxfixes3 \
#     libxinerama1 \
#     libxcursor1 \
#     libgl1-mesa-glx \
#     && rm -rf /var/lib/apt/lists/*
#
# RUN python3 -m venv venv
#
# # Copy dependency file and install dependencies
# COPY requirements.txt .
# RUN venv/bin/pip3 install --no-cache-dir -r requirements.txt
#
# COPY twsapi_macunix.1030.01.zip .
# RUN echo "A" | unzip twsapi_macunix.1030.01.zip; \
#     cd IBJts/source/pythonclient; \
#     venv/bin/python3 setup.py install; \
#     cd ../../..; \
#     venv/bin/pip3 install IBJts/source/pythonclient;
#
# # Copy the rest of the app
# COPY app/ ./app/
# COPY typings/ ./typings/
#
# COPY IBCLinux-3.21.2.zip .
# RUN unzip IBCLinux-3.21.2.zip -d /IBCLinux-3.21.2/; \
#     chown -R root /IBCLinux-3.21.2/; \
#     chmod -u+x -R /IBCLinux-3.21.2/;
#
# COPY IBCMacos-3.20.0/config.ini IBCLinux-3.21.2/config.ini
# COPY IBCLinux-3.21.2/scripts/ibcstart.sh IBCLinux-3.21.2/scripts/ibcstart.sh
# RUN chmod -u+x -R /IBCLinux-3.21.2/
#
# RUN mkdir -p logs/sqlalchemy
#
# # ENV HOME home/tws
#
# # Set environment variables (optional)
# ENV PYTHONUNBUFFERED=1
#
# # Set the default command to run your app
# # CMD ["venv/bin/python3", "-m", "app.run"]
#
# COPY entrypoint.sh /entrypoint.sh
# RUN chmod +x /entrypoint.sh
# ENTRYPOINT ["/entrypoint.sh"]
# Build stage
# FROM --platform=linux/amd64 python:3.11-slim AS builder
#
# RUN apt-get update && apt-get install -y \
#     build-essential unzip \
#     && rm -rf /var/lib/apt/lists/*
#
# COPY requirements.txt .
# RUN python3 -m venv /venv
# RUN /venv/bin/pip install --no-cache-dir -r requirements.txt

# -------- STAGE 1: Builder --------
FROM --platform=linux/amd64 python:3.11-slim AS builder

RUN apt-get update && apt-get install -y curl unzip build-essential && rm -rf /var/lib/apt/lists/*

ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh

ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev

COPY twsapi_macunix.1030.01.zip ./
RUN unzip twsapi_macunix.1030.01.zip && \
    uv pip install /app/IBJts/source/pythonclient

COPY app/ ./app/
COPY typings/ ./typings/

# -------- STAGE 2: Slim Runtime --------
FROM --platform=linux/amd64 python:3.11-slim

# Only install system libraries needed at runtime
RUN apt-get update && apt-get install -y \
    libxtst6 libxi6 libxrender1 libxrandr2 libfreetype6 libxext6 \
    libx11-6 libxfixes3 libxinerama1 libxcursor1 libgl1-mesa-glx \
    xdotool openbox \
    unzip xvfb \
    && rm -rf /var/lib/apt/lists/*

# Install IBC properly
COPY IBCLinux-3.21.2.zip /app/IBCLinux-3.21.2.zip
RUN unzip /app/IBCLinux-3.21.2.zip -d /IBCLinux-3.21.2/; \
    chown -R root /IBCLinux-3.21.2/; \
    chmod -u+x -R /IBCLinux-3.21.2/;
COPY IBCMacos-3.20.0/config.ini IBCLinux-3.21.2/config.ini
COPY IBCLinux-3.21.2/scripts/ibcstart.sh IBCLinux-3.21.2/scripts/ibcstart.sh
RUN chmod -u+x -R /IBCLinux-3.21.2/

# Copy installed venv from builder
COPY --from=builder /app/.venv /.venv
ENV PATH="/.venv/bin:$PATH"

COPY --from=builder /app/app /app
COPY entrypoint.sh /entrypoint.sh
RUN mkdir logs && \
    mkdir logs/sqlalchemy
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
