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
CONSOLE="${HOTPATH_CONSOLE:-1}"

if [[ "${DOCKER[0]}" == "sudo" ]]; then
    if [[ -n "${IMAGE_TAG:-}" ]]; then
        DOCKER=(sudo -n env "IMAGE_TAG=$IMAGE_TAG" docker)
    else
        DOCKER=(sudo -n env docker)
    fi
fi

usage() {
    cat <<'EOF'
Usage: scripts/up.sh <stack> [service]
Stacks: trading, platform, monitoring, tests, data, all

  all       starts trading, then platform, then monitoring
  tests     defaults to test-db; run test jobs with docker compose run
  data      defaults to tws; see scripts/data_puller/README.md for the puller
EOF
}

ensure_network() {
    "${DOCKER[@]}" network inspect autotrader >/dev/null 2>&1 || "${DOCKER[@]}" network create autotrader
}

stack_files() {
    case "$1" in
        trading) printf '%s\n' compose/trading.yml ;;
        platform) printf '%s\n' compose/platform.yml ;;
        monitoring) printf '%s\n' compose/monitoring.yml ;;
        tests) printf '%s\n' compose/tests.yml ;;
        data) printf '%s\n' scripts/data_puller/docker-compose.yml ;;
        *) return 1 ;;
    esac
}

up_stack() {
    local stack="$1"
    local service="${2:-}"
    local file
    local -a files=()

    case "$stack" in
        trading|platform|monitoring) ensure_network ;;
        tests)
            [[ -n "$service" ]] || service="test-db"
            ;;
        data)
            [[ -n "$service" ]] || service="tws"
            ;;
    esac

    while IFS= read -r file; do
        files+=(-f "$file")
    done < <(stack_files "$stack")

    local -a profile_args=()
    if [[ "$stack" == "trading" && "$CONSOLE" != "0" ]]; then
        profile_args=(--profile console)
    fi

    local -a cmd=("${DOCKER[@]}" compose "${ENV_ARGS[@]}" "${profile_args[@]}" "${files[@]}" up -d)
    [[ -n "$service" ]] && cmd+=("$service")

    "${cmd[@]}"
}

STACK="${1:-}"
[[ -n "$STACK" ]] || { usage; exit 1; }
shift

SERVICE="${1:-}"
[[ $# -le 1 ]] || { usage; exit 1; }

case "$STACK" in
    trading|platform|monitoring|tests|data)
        up_stack "$STACK" "$SERVICE"
        ;;
    all)
        [[ -z "$SERVICE" ]] || { echo "No service allowed with 'all'" >&2; exit 1; }
        up_stack trading
        up_stack platform
        up_stack monitoring
        ;;
    *)
        usage
        exit 1
        ;;
esac
