#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <service_name> [--no-cache]"
    exit 1
fi

SERVICE_NAME=""
NO_CACHE=""

for arg in "$@"; do
    case "$arg" in
        --no-cache)
            NO_CACHE="--no-cache"
            ;;
        *)
            if [[ -z "$SERVICE_NAME" ]]; then
                SERVICE_NAME="$arg"
            else
                echo "Unexpected argument: $arg"
                exit 1
            fi
            ;;
    esac
done

if [[ -z "$SERVICE_NAME" ]]; then
    echo "Error: service name is required"
    exit 1
fi

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

docker-compose stop "$SERVICE_NAME"
docker-compose build $NO_CACHE "$SERVICE_NAME"
docker-compose up "$SERVICE_NAME"
