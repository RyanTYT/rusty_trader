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
Usage: scripts/build.sh <stack> [service] [--no-cache]
Stacks: trading, platform, tests, monitoring, data, all
EOF
}

stack_files() {
    case "$1" in
        trading)
            printf '%s\n' compose/trading.yml compose/trading.build.yml
            ;;
        platform)
            printf '%s\n' compose/platform.yml compose/platform.build.yml
            ;;
        tests)
            printf '%s\n' compose/tests.yml
            ;;
        monitoring)
            printf '%s\n' compose/monitoring.yml
            ;;
        data)
            printf '%s\n' scripts/data_puller/docker-compose.yml scripts/data_puller/compose.build.yml
            ;;
        *)
            return 1
            ;;
    esac
}

build_stack() {
    local stack="$1"
    local service="${2:-}"
    local no_cache="${3:-}"
    local file
    local -a files=()

    if [[ "$stack" == "monitoring" ]]; then
        echo "Skipping monitoring: it uses upstream images only."
        return 0
    fi

    while IFS= read -r file; do
        files+=(-f "$file")
    done < <(stack_files "$stack")

    local -a profile_args=()
    if [[ "$stack" == "trading" && "$CONSOLE" != "0" ]]; then
        profile_args=(--profile console)
    fi

    local -a cmd=("${DOCKER[@]}" compose "${ENV_ARGS[@]}" "${profile_args[@]}" "${files[@]}" build)
    [[ -n "$no_cache" ]] && cmd+=("$no_cache")
    [[ -n "$service" ]] && cmd+=("$service")

    "${cmd[@]}"
}

STACK="${1:-}"
[[ -n "$STACK" ]] || { usage; exit 1; }
shift

SERVICE=""
NO_CACHE=""
for arg in "$@"; do
    case "$arg" in
        --no-cache) NO_CACHE="--no-cache" ;;
        *)
            if [[ -z "$SERVICE" ]]; then
                SERVICE="$arg"
            else
                echo "Unexpected argument: $arg" >&2
                exit 1
            fi
            ;;
    esac
done

case "$STACK" in
    trading|platform|tests|monitoring|data)
        build_stack "$STACK" "$SERVICE" "$NO_CACHE"
        ;;
    all)
        [[ -z "$SERVICE" ]] || { echo "No service allowed with 'all'" >&2; exit 1; }
        for stack in trading platform tests data; do
            build_stack "$stack" "" "$NO_CACHE"
        done
        ;;
    *)
        usage
        exit 1
        ;;
esac
