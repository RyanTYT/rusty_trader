#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if docker info >/dev/null 2>&1; then
    DOCKER=(docker)
elif sudo -n docker info >/dev/null 2>&1; then
    DOCKER=(sudo -n docker)
else
    echo "Docker is not accessible (tried docker and sudo -n docker)." >&2
    exit 1
fi

ENV_FILE="${ENV_FILE:-.env}"
ENV_ARGS=()
[[ -f "$ENV_FILE" ]] && ENV_ARGS=(--env-file "$ENV_FILE")

if [[ "${DOCKER[0]}" == "sudo" ]]; then
    if [[ -n "${IMAGE_TAG:-}" ]]; then
        DOCKER=(sudo -n env "IMAGE_TAG=$IMAGE_TAG" docker)
    else
        DOCKER=(sudo -n env docker)
    fi
fi

usage() {
    cat <<'EOF'
Usage: scripts/down.sh <stack>
Stacks: trading, platform, monitoring, tests, data, all

  all   stops monitoring, then platform, then trading
        (use "tests" or "data" explicitly; they are separate stacks)
EOF
}

down_stack() {
    local stack="$1"
    local files=()

    local -a profile_args=()
    case "$stack" in
        trading) files=(-f compose/trading.yml); profile_args=(--profile console) ;;
        platform) files=(-f compose/platform.yml) ;;
        monitoring) files=(-f compose/monitoring.yml) ;;
        tests) files=(-f compose/tests.yml) ;;
        data) files=(-f scripts/data_puller/docker-compose.yml) ;;
        *) return 1 ;;
    esac

    "${DOCKER[@]}" compose "${ENV_ARGS[@]}" "${profile_args[@]}" "${files[@]}" down
}

STACK="${1:-}"
[[ -n "$STACK" ]] || { usage; exit 1; }

case "$STACK" in
    trading|platform|monitoring|tests|data)
        down_stack "$STACK"
        ;;
    all)
        down_stack monitoring
        down_stack platform
        down_stack trading
        ;;
    *)
        usage
        exit 1
        ;;
esac
