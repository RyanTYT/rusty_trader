#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

usage() {
    cat <<'EOF'
Usage: scripts/rebuild.sh <service> [--no-cache]
Services: trading-bot, db, hotpath-console,
          backend, llm_service, scraper,
          test-trading-bot, test-db, backtester, optimizer,
          prometheus, alertmanager, grafana, autoheal,
          tws, pull-1min
EOF
}

SERVICE="${1:-}"
[[ -n "$SERVICE" ]] || { usage; exit 1; }
shift

NO_CACHE=""
for arg in "$@"; do
    case "$arg" in
        --no-cache) NO_CACHE="--no-cache" ;;
        *) echo "Unexpected argument: $arg" >&2; exit 1 ;;
    esac
done

case "$SERVICE" in
    db|trading-bot|hotpath-console) STACK=trading ;;
    test-db|test-trading-bot|backtester|optimizer) STACK=tests ;;
    backend|llm_service|scraper) STACK=platform ;;
    prometheus|alertmanager|grafana|autoheal) STACK=monitoring ;;
    tws|pull-1min) STACK=data ;;
    *)
        echo "Unknown service: $SERVICE" >&2
        usage
        exit 1
        ;;
esac

if [[ "$STACK" != monitoring && "$SERVICE" != db && "$SERVICE" != test-db ]]; then
    build_args=("$STACK" "$SERVICE")
    [[ -n "$NO_CACHE" ]] && build_args+=("$NO_CACHE")
    "$ROOT/scripts/build.sh" "${build_args[@]}"
fi
"$ROOT/scripts/up.sh" "$STACK" "$SERVICE"
