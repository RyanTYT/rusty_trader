#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TAG="${TAG:-}"
CONSOLE="${HOTPATH_CONSOLE:-1}"
ALLOW_DIRTY="${ALLOW_DIRTY:-0}"
DRY_RUN="${DRY_RUN:-0}"
REUSE_LATEST="${REUSE_LATEST:-0}"

usage() {
    cat <<'EOF'
Usage: scripts/release.sh [--tag TAG] [--without-console] [--allow-dirty]
                          [--dry-run] [--reuse-latest]

Builds only the deployable images whose build inputs changed since the
previous release bundle, then assembles the runtime bundle in
.releases/<tag>/. The bundle contains compose files, runtime infra config,
helper scripts, image/source manifests, and TAG. It never contains .env or
config.ini; scripts/deploy.sh copies those to the prod shared/ dir.

--dry-run        Print which images would be built or reused; change nothing.
--reuse-latest   Bootstrap only: tag existing :latest images instead of
                 building. Use only when the source has not changed since the
                 :latest images were built.

Default tag: YYYYMMDD-HHMM-<git-short-sha>
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)
            [[ $# -ge 2 ]] || { usage; exit 1; }
            TAG="$2"
            shift 2
            ;;
        --without-console)
            CONSOLE=0
            shift
            ;;
        --with-console)
            CONSOLE=1
            shift
            ;;
        --allow-dirty)
            ALLOW_DIRTY=1
            shift
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        --reuse-latest)
            REUSE_LATEST=1
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

if [[ -z "$TAG" ]]; then
    TAG="$(git show -s --format=%cd --date=format:%Y%m%d-%H%M HEAD)-$(git rev-parse --short HEAD)"
fi

if [[ "$ALLOW_DIRTY" != "1" ]] && [[ -n "$(git status --porcelain)" ]]; then
    echo "Working tree is dirty. Commit first or pass --allow-dirty." >&2
    exit 1
fi

if [[ "$CONSOLE" != "0" && "$CONSOLE" != "1" ]]; then
    echo "HOTPATH_CONSOLE must be 0 or 1." >&2
    exit 1
fi

if docker info >/dev/null 2>&1; then
    DOCKER=(docker)
elif sudo -n docker info >/dev/null 2>&1; then
    DOCKER=(sudo -n docker)
else
    echo "Docker is not accessible (tried docker and sudo -n docker)." >&2
    exit 1
fi

free_kb="$(df -Pk "$ROOT" | awk 'NR == 2 { print $4 }')"
min_free_kb=$((8 * 1024 * 1024))
if (( free_kb < min_free_kb )); then
    echo "Not enough free disk space: $((free_kb / 1024)) MiB available, 8 GiB required." >&2
    echo "Free space first, for example:" >&2
    echo "  sudo docker builder prune -af" >&2
    echo "  sudo docker image prune -f" >&2
    exit 1
fi

BUNDLE="$ROOT/.releases/$TAG"

previous_bundles=()
if [[ -d "$ROOT/.releases" ]]; then
    while IFS= read -r dir; do
        [[ "$dir" == "$BUNDLE" ]] && continue
        if [[ -f "$dir/sources.txt" ]]; then
            previous_bundles+=("$dir")
        fi
    done < <(find "$ROOT/.releases" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' | sort -rn | cut -d' ' -f2-)
fi
previous_bundle="${previous_bundles[0]:-}"

hash_paths() {
    local path
    for path in "$@"; do
        [[ -e "$path" ]] || continue
        if [[ -d "$path" ]]; then
            find "$path" -type f \
                ! -path '*/target/*' \
                ! -path '*/node_modules/*' \
                ! -path '*/.venv/*' \
                ! -path '*/__pycache__/*' \
                ! -path '*/.pytest_cache/*' \
                ! -name '*.pyc' \
                -print0
        else
            printf '%s\0' "$path"
        fi
    done | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}'
}

service_inputs() {
    case "$1" in
        trading-bot)
            printf '%s\n' \
                .dockerignore \
                trading/trading-app/Dockerfile \
                trading/trading-app/entrypoint.sh \
                trading/trading-app/Cargo.toml \
                trading/trading-app/Cargo.lock \
                trading/trading-app/crud_insertable \
                trading/trading-app/crud_insertable_macro \
                trading/trading-app/crud_models \
                trading/trading-app/spmc-ring \
                trading/trading-app/.sqlx \
                trading/trading-app/migrations \
                trading/trading-app/src \
                infra/ib-gateway/ibgateway-stable-standalone-linux-x64.sh \
                infra/ib-gateway/IBCLinux-3.21.2.zip \
                infra/ib-gateway/IBCMacos-3.20.0/scripts/ibcstart.sh
            ;;
        hotpath-console)
            printf '%s\n' api/hotpath-console/Dockerfile
            ;;
        backend)
            printf '%s\n' \
                api/backend/Dockerfile \
                api/backend/.dockerignore \
                api/backend/Cargo.toml \
                api/backend/Cargo.lock \
                api/backend/src \
                api/backend/.sqlx \
                api/backend/crud_models \
                api/backend/crud_insertable \
                api/backend/.cargo
            ;;
        llm_service)
            printf '%s\n' \
                ai/llm_service/Dockerfile \
                ai/llm_service/pyproject.toml \
                ai/llm_service/uv.lock \
                ai/llm_service/.python-version \
                ai/llm_service/main.py \
                ai/llm_service/agents \
                ai/llm_service/api \
                ai/llm_service/models \
                ai/llm_service/tools
            ;;
        scraper)
            printf '%s\n' \
                ai/scraper/Dockerfile \
                ai/scraper/Cargo.toml \
                ai/scraper/Cargo.lock \
                ai/scraper/src
            ;;
        *)
            return 1
            ;;
    esac
}

service_image() {
    case "$1" in
        trading-bot) printf 'rusty_trader/trading-bot:%s\n' "$TAG" ;;
        hotpath-console) printf 'rusty_trader/hotpath-console:%s\n' "$TAG" ;;
        backend) printf 'rusty_trader/backend:%s\n' "$TAG" ;;
        llm_service) printf 'rusty_trader/llm-service:%s\n' "$TAG" ;;
        scraper) printf 'rusty_trader/scraper:%s\n' "$TAG" ;;
    esac
}

service_latest() {
    case "$1" in
        trading-bot) printf 'rusty_trader/trading-bot:latest\n' ;;
        hotpath-console) printf 'rusty_trader/hotpath-console:latest\n' ;;
        backend) printf 'rusty_trader/backend:latest\n' ;;
        llm_service) printf 'rusty_trader/llm-service:latest\n' ;;
        scraper) printf 'rusty_trader/scraper:latest\n' ;;
    esac
}

build_service() {
    local service="$1"
    local stack target
    case "$service" in
        trading-bot) stack=trading; target=trading-bot ;;
        hotpath-console) stack=trading; target=hotpath-console ;;
        backend) stack=platform; target=backend ;;
        llm_service) stack=platform; target=llm_service ;;
        scraper) stack=platform; target=scraper ;;
        *) echo "Unknown service: $service" >&2; exit 1 ;;
    esac
    IMAGE_TAG="$TAG" HOTPATH_CONSOLE="$CONSOLE" \
        "$ROOT/scripts/build.sh" "$stack" "$target"
}

find_reusable_line() {
    local service="$1"
    local hash="$2"
    local dir line
    for dir in "${previous_bundles[@]}"; do
        [[ -f "$dir/sources.txt" ]] || continue
        line="$(awk -F'\t' -v service="$service" -v hash="$hash" \
            '$1 == service && $2 == hash { print; exit }' "$dir/sources.txt")"
        if [[ -n "$line" ]]; then
            printf '%s\n' "$line"
            return 0
        fi
    done
    return 1
}

services=(trading-bot)
[[ "$CONSOLE" == "1" ]] && services+=(hotpath-console)
services+=(backend llm_service scraper)

source_lines=()
images=()
built=()
reused=()

for service in "${services[@]}"; do
    image="$(service_image "$service")"
    mapfile -t inputs < <(service_inputs "$service")
    hash="$(hash_paths "${inputs[@]}")"

    prev_hash=""
    prev_image=""
    prev_line="$(find_reusable_line "$service" "$hash" || true)"
    if [[ -n "$prev_line" ]]; then
        IFS=$'\t' read -r _ prev_hash prev_image <<< "$prev_line"
    fi

    should_reuse=0
    if [[ "$prev_hash" == "$hash" && -n "$prev_image" ]]; then
        should_reuse=1
    elif [[ "$REUSE_LATEST" == "1" && -z "$previous_bundle" ]]; then
        prev_image="$(service_latest "$service")"
        should_reuse=1
    fi

    if [[ "$should_reuse" == "1" ]] && "${DOCKER[@]}" image inspect "$prev_image" >/dev/null 2>&1; then
        if [[ "$DRY_RUN" == "1" ]]; then
            echo "REUSE  $service  <- $prev_image"
        else
            echo "Reusing $service from $prev_image"
            "${DOCKER[@]}" tag "$prev_image" "$image"
        fi
        reused+=("$service")
    else
        if [[ "$DRY_RUN" == "1" ]]; then
            echo "BUILD  $service  -> $image"
        else
            echo "Building $service -> $image"
            build_service "$service"
            if ! "${DOCKER[@]}" image inspect "$image" >/dev/null 2>&1; then
                echo "Build did not produce $image; check IMAGE_TAG propagation." >&2
                exit 1
            fi
        fi
        built+=("$service")
    fi

    source_lines+=("$(printf '%s\t%s\t%s' "$service" "$hash" "$image")")
    images+=("$image")
done

if [[ "$DRY_RUN" == "1" ]]; then
    echo "DRY-RUN: no images built and no bundle written."
    exit 0
fi

mkdir -p "$BUNDLE/compose" "$BUNDLE/infra" "$BUNDLE/scripts"
cp compose/*.yml "$BUNDLE/compose/"
cp compose/README.md "$BUNDLE/compose/README.md"
cp -R infra/prometheus "$BUNDLE/infra/prometheus"
cp -R infra/grafana "$BUNDLE/infra/grafana"
mkdir -p "$BUNDLE/infra/alertmanager"
cp infra/alertmanager/alertmanager.example.yml "$BUNDLE/infra/alertmanager/alertmanager.example.yml"
cp scripts/up.sh scripts/down.sh "$BUNDLE/scripts/"
cp README.md "$BUNDLE/README.md"
printf '%s\n' "$TAG" > "$BUNDLE/TAG"
printf '%s\n' "${source_lines[@]}" > "$BUNDLE/sources.txt"

: > "$BUNDLE/images.txt"
for image in "${images[@]}"; do
    id="$("${DOCKER[@]}" image inspect --format '{{.Id}}' "$image")"
    printf '%s\t%s\n' "$image" "$id" >> "$BUNDLE/images.txt"
done

echo "Release: $TAG"
echo "Bundle:  $BUNDLE"
echo "Built:   ${built[*]:-none}"
echo "Reused:  ${reused[*]:-none}"
echo "Console: $([[ "$CONSOLE" == "1" ]] && echo included || echo excluded)"
