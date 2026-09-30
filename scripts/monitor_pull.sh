#!/bin/bash
# monitor_pull.sh — runs the 1-min pull inside docker + auto-recovers from
# IB Gateway stalls. Checks the pull container's logs every 60s; if no chunk
# progress in 15 min (a normal chunk takes ~8-10 min), kills the pull,
# restarts the tws container (fresh Gateway), waits for login, then resumes
# the pull (resumes from the last saved chunk).
set -u
cd "/Users/user/Downloads/Personal Project/rusty_trader"

LOG=/tmp/pull_monitor.log
STALL_TIMEOUT=900   # 15 min without chunk progress = stalled
MAX_RESTARTS=40

log() { echo "[monitor $(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

wait_for_login() {
    for i in $(seq 1 10); do
        sleep 30
        if docker compose logs tws 2>/dev/null | grep -q "Login has completed"; then
            log "IB Gateway logged in (after $((i*30))s)"
            return 0
        fi
    done
    log "WARNING: IB Gateway did not log in within 300s"
    return 1
}

start_pull() {
    docker rm -f $(docker ps --filter "ancestor=rusty_trader-pull-1min" -q) 2>/dev/null
    PULL_START=2021-01-01 PULL_END=2026-09-28 \
    docker compose run -d --rm --no-deps --name puller \
        -e CHUNK_DAYS=10 -e MERGE_EVERY=20 -e IB_CLIENT_ID=55 \
        pull-1min >/dev/null 2>&1
    log "pull container started (puller)"
}

last_progress=0
last_progress_time=$(date +%s)
restarts=0

log "=== starting monitored pull ==="
[ -z "$(docker ps -q --filter name=^puller$)" ] && start_pull

while [ $restarts -lt $MAX_RESTARTS ]; do
    sleep 60

    # pull finished?
    if docker logs puller 2>/dev/null | grep -q "INTEGRITY CHECK"; then
        log "PULL COMPLETED — final output:"
        docker logs puller 2>/dev/null | grep -A 6 "INTEGRITY CHECK" | tee -a "$LOG"
        docker rm -f puller >/dev/null 2>&1
        exit 0
    fi
    # pull container died?
    if [ -z "$(docker ps -q --filter name=^puller$)" ]; then
        log "pull container died — restarting"
        docker compose up -d tws >/dev/null 2>&1
        wait_for_login
        start_pull
        last_progress_time=$(date +%s)
        continue
    fi

    # chunk progress?
    CHUNK_LINE=$(docker logs --tail 5 puller 2>/dev/null | grep -E "chunk [0-9]+" | tail -1)
    if [ -n "$CHUNK_LINE" ] && [ "$CHUNK_LINE" != "$last_progress" ]; then
        last_progress="$CHUNK_LINE"
        last_progress_time=$(date +%s)
        log "progress: $CHUNK_LINE"
    fi

    # stall check
    now=$(date +%s)
    elapsed=$((now - last_progress_time))
    if [ $elapsed -gt $STALL_TIMEOUT ]; then
        restarts=$((restarts+1))
        log "STALLED (${elapsed}s no progress, restart #$restarts) — restarting Gateway + pull"
        docker rm -f puller >/dev/null 2>&1
        docker compose down tws >/dev/null 2>&1
        docker compose up -d tws >/dev/null 2>&1
        wait_for_login
        start_pull
        last_progress_time=$(date +%s)
    fi
done

log "exceeded max restarts ($MAX_RESTARTS)"
exit 1
