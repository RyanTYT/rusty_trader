#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SSH_TARGET="${SSH_TARGET:-trader@10.10.10.13}"
SSH_KEY="${SSH_KEY:-}"
REMOTE_DIR="${REMOTE_DIR:-/opt/autotrader}"
TAG=""
CONSOLE="${HOTPATH_CONSOLE:-1}"
DRY_RUN=0
USE_PREVIOUS=0

usage() {
    cat <<'EOF'
Usage: scripts/rollback.sh [--previous | --tag TAG] [--with-console|--without-console] [--dry-run]

Switch /opt/autotrader/current and IMAGE_TAG in shared/.env to an existing
release, then restart the stacks. Images are not transferred; they must already
be loaded on prod.

  --previous        Use the release before the current one (default)
  --tag TAG         Roll back to a specific release directory
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)
            [[ $# -ge 2 ]] || { usage; exit 1; }
            TAG="$2"
            shift 2
            ;;
        --previous)
            USE_PREVIOUS=1
            shift
            ;;
        --with-console)
            CONSOLE=1
            shift
            ;;
        --without-console)
            CONSOLE=0
            shift
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$SSH_KEY" ]]; then
    for candidate in "$HOME/.ssh/autotrader_deploy" "$HOME/.ssh/autotrader-deploy"; do
        if [[ -f "$candidate" ]]; then
            SSH_KEY="$candidate"
            break
        fi
    done
fi
if [[ -z "$SSH_KEY" || ! -f "$SSH_KEY" ]]; then
    echo "SSH key not found. Set SSH_KEY or create ~/.ssh/autotrader_deploy." >&2
    exit 1
fi

SSH=(ssh -i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10 "$SSH_TARGET")

remote() {
    "${SSH[@]}" "$@"
}

run_remote() {
    if [[ "$DRY_RUN" == "1" ]]; then
        printf 'DRY-RUN remote: %s\n' "$*"
    else
        remote "$@"
    fi
}

remote "set -e
test -d '$REMOTE_DIR/releases'
test -f '$REMOTE_DIR/shared/.env'"

current_link="$(remote "readlink '$REMOTE_DIR/current' 2>/dev/null || true")"
current_tag=""
if [[ -n "$current_link" ]]; then
    current_tag="$(basename "$current_link")"
fi

if [[ -z "$TAG" ]]; then
    USE_PREVIOUS=1
fi

if [[ "$USE_PREVIOUS" == "1" ]]; then
    mapfile -t releases < <(remote "ls -1 '$REMOTE_DIR/releases' | sort")
    if [[ ${#releases[@]} -eq 0 ]]; then
        echo "No releases found on prod." >&2
        exit 1
    fi
    candidate=""
    for ((i = ${#releases[@]} - 1; i >= 0; i--)); do
        if [[ "${releases[$i]}" != "$current_tag" ]]; then
            candidate="${releases[$i]}"
            break
        fi
    done
    if [[ -z "$candidate" ]]; then
        echo "No previous release available; current is $current_tag." >&2
        exit 1
    fi
    TAG="$candidate"
fi

if [[ "$TAG" == "$current_tag" ]]; then
    echo "Release $TAG is already current." >&2
    exit 1
fi

remote "test -d '$REMOTE_DIR/releases/$TAG'"

if remote "test -f '$REMOTE_DIR/shared/.env'"; then
    run_remote "cp -p '$REMOTE_DIR/shared/.env' '$REMOTE_DIR/shared/.env.bak.rollback-$TAG'"
fi

run_remote "if grep -q '^IMAGE_TAG=' '$REMOTE_DIR/shared/.env'; then sed -i 's/^IMAGE_TAG=.*/IMAGE_TAG=$TAG/' '$REMOTE_DIR/shared/.env'; else printf '\\nIMAGE_TAG=%s\\n' '$TAG' >> '$REMOTE_DIR/shared/.env'; fi"
run_remote "ln -sfn 'releases/$TAG' '$REMOTE_DIR/current'"
run_remote "sudo -n docker network inspect autotrader >/dev/null 2>&1 || sudo -n docker network create autotrader"

profile_args=""
[[ "$CONSOLE" == "1" ]] && profile_args="--profile console"

run_remote "cd '$REMOTE_DIR/current' && sudo -n docker compose --env-file '$REMOTE_DIR/shared/.env' -f compose/trading.yml $profile_args up -d"
if [[ "$CONSOLE" == "0" ]]; then
    run_remote "cd '$REMOTE_DIR/current' && sudo -n docker compose --env-file '$REMOTE_DIR/shared/.env' -f compose/trading.yml --profile console stop hotpath-console >/dev/null 2>&1 || true"
fi
run_remote "cd '$REMOTE_DIR/current' && sudo -n docker compose --env-file '$REMOTE_DIR/shared/.env' -f compose/platform.yml up -d"
run_remote "cd '$REMOTE_DIR/current' && sudo -n docker compose --env-file '$REMOTE_DIR/shared/.env' -f compose/monitoring.yml up -d"

echo "Rolled back to $TAG on $SSH_TARGET:$REMOTE_DIR/current"
echo "Console: $([[ "$CONSOLE" == "1" ]] && echo included || echo excluded)"
