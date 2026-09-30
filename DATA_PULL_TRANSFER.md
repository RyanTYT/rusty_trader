# 1-Min + 5-Min Data Pull — Transfer to Another Laptop

## What's on this laptop (DON'T lose this)

| File | Size | Description |
|---|---|---|
| `data/backtest-results/stats/raw_1min_bars.parquet` | ~400MB | **COMPLETE** 1-min bars, 41 megacaps, 2020-12-24 → 2026-09-25 (22.4M bars) |
| `data/backtest-results/stats/raw_bars_extended.parquet` | ~75MB+ | **IN PROGRESS** 5-min bars, 41 megacaps + mid/small-caps (resumable) |
| `data/backtest-results/stats/raw_bars.parquet` | ~85MB | Original 5-min bars, 41 megacaps (the seed file) |

## Files to commit + push (so the other laptop can pull)

```bash
# All the pull scripts + config
scripts/pull_1min_bars.py          # 1-min pull (COMPLETE, for reference)
scripts/pull_5min_bars.py          # 5-min pull (IN PROGRESS, resumable)
scripts/fill_1min_gaps.py          # gap filler
scripts/monitor_pull.sh             # stall-recovery monitor (1-min)
infra/ib-gateway/Dockerfile.ibc    # IB Gateway + IBC Dockerfile
infra/ib-gateway/entrypoint-ibc.sh # headless auto-login entrypoint
trading/trading-app/IBCMacos-3.20.0/config.ini  # IBC credentials (GITIGNORED — copy manually!)
```

## The IBKR "5 mins" gotcha

**Use `"5 mins"` (plural), NOT `"5 min"` (singular).** IBKR's API silently
rejects singular bar sizes for values > 1. The Gateway returns Error 366
("No historical data query found") + the request times out. This cost us
hours of debugging.

## Setup on the other laptop

### Prerequisites
1. Docker Desktop or colima (with Rosetta for amd64 emulation on Apple Silicon)
2. The repo (clone from git or copy the directory)
3. IBC credentials (`config.ini`) — copy from this laptop, it's gitignored
4. The IB Gateway 10.30 installer (`ibgateway-stable-standalone-linux-x64.sh`) — download from IBKR
5. The IBC Linux zip (`IBCLinux-3.21.2.zip`) — copy from `trading/trading-app/`

### Steps

```bash
# 1. Copy the data files (CRITICAL — the resume logic needs them)
scp this-laptop:rusty_trader/data/backtest-results/stats/raw_1min_bars.parquet data/backtest-results/stats/
scp this-laptop:rusty_trader/data/backtest-results/stats/raw_bars_extended.parquet data/backtest-results/stats/
scp this-laptop:rusty_trader/data/backtest-results/stats/raw_bars.parquet data/backtest-results/stats/

# 2. Copy the IBC config (credentials)
scp this-laptop:rusty_trader/trading/trading-app/IBCMacos-3.20.0/config.ini trading/trading-app/IBCMacos-3.20.0/

# 3. Copy the IBC zip + IB Gateway installer
scp this-laptop:rusty_trader/trading/trading-app/IBCLinux-3.21.2.zip trading/trading-app/
cp ~/Downloads/ibgateway-stable-standalone-linux-x64.sh infra/ib-gateway/

# 4. Build + start the IB Gateway
docker compose build tws
docker compose up -d tws
# Wait ~90s for "Login has completed"
docker compose logs -f tws | grep "Login has completed"

# 5. Build the pull container
docker compose build pull-1min

# 6. Resume the 5-min pull (it auto-resumes from the last saved chunk)
docker run -d --rm --name puller5 --network host \
  --entrypoint python \
  -e CHUNK_DAYS=60 -e MERGE_EVERY=20 -e IB_CLIENT_ID=55 \
  -e OUTPUT=/data/backtest-results/stats/raw_bars_extended.parquet \
  -e START_DATE=2021-01-01 -e END_DATE=2026-09-28 \
  -v "$(pwd)/scripts/pull_5min_bars.py:/app/pull_1min_bars.py:ro" \
  -v "$(pwd)/data:/data" \
  rusty_trader-pull-1min -u /app/pull_1min_bars.py

# 7. Monitor (per-asset progress + auto-recovery from Gateway stalls)
cat > /tmp/monitor5.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")"
LOG=/tmp/pull5_monitor.log
STALL_TIMEOUT=1500
log() { echo "[monitor $(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

start_pull() {
    docker rm -f puller5 2>/dev/null
    docker run -d --rm --name puller5 --network host \
      --entrypoint python \
      -e CHUNK_DAYS=60 -e MERGE_EVERY=20 -e IB_CLIENT_ID=55 \
      -e OUTPUT=/data/backtest-results/stats/raw_bars_extended.parquet \
      -e START_DATE=2021-01-01 -e END_DATE=2026-09-28 \
      -v "$(pwd)/scripts/pull_5min_bars.py:/app/pull_1min_bars.py:ro" \
      -v "$(pwd)/data:/data" \
      rusty_trader-pull-1min -u /app/pull_1min_bars.py >/dev/null 2>&1
    log "puller started"
}

last_progress=""
last_progress_time=$(date +%s)
restarts=0
log "=== monitoring 5-min pull ==="

while [ $restarts -lt 30 ]; do
    sleep 120
    if [ -z "$(docker ps -q --filter name=^puller5$)" ]; then
        restarts=$((restarts+1))
        log "puller died — restarting (#$restarts)"
        start_pull
        last_progress_time=$(date +%s)
        continue
    fi
    if docker logs puller5 2>/dev/null | grep -q "DONE:"; then
        log "PULL COMPLETED"
        docker logs puller5 2>/dev/null | grep -E "DONE:|assets" | tail -3 | tee -a "$LOG"
        docker rm -f puller5 >/dev/null 2>&1
        exit 0
    fi
    PROG=$(docker logs --tail 2 puller5 2>/dev/null | grep -E "^\s+\[|chunk" | tail -1)
    if [ -n "$PROG" ] && [ "$PROG" != "$last_progress" ]; then
        last_progress="$PROG"
        last_progress_time=$(date +%s)
    fi
    now=$(date +%s)
    elapsed=$((now - last_progress_time))
    if [ $elapsed -gt $STALL_TIMEOUT ]; then
        restarts=$((restarts+1))
        log "STALLED (${elapsed}s, restart #$restarts)"
        docker rm -f puller5 >/dev/null 2>&1
        docker compose down tws >/dev/null 2>&1
        docker compose up -d tws >/dev/null 2>&1
        for i in $(seq 1 10); do sleep 30; docker compose logs tws 2>/dev/null | grep -q "Login has completed" && break; done
        start_pull
        last_progress_time=$(date +%s)
    fi
done
log "exceeded max restarts"
EOF
chmod +x /tmp/monitor5.sh
nohup bash /tmp/monitor5.sh > /dev/null 2>&1 &

# 8. Track progress
docker logs -f puller5          # per-asset progress
tail -f /tmp/pull5_monitor.log  # monitor
```

## Key gotchas (learned the hard way)

1. **`"5 mins"` not `"5 min"`** — IBKR uses plural for bar sizes > 1
2. **The Gateway stalls every ~4-6 hours** — the monitor auto-restarts it
3. **The colima SSH mux can break port 4002 forwarding** — if ib_async
   times out from the Mac but works from inside a `--network host` Docker
   container, restart colima: `colima stop && colima start --vm-type vz --vz-rosetta`
4. **The install4j JRE is at `~/.local/share/i4j_jres/`** — NOT inside the
   ibgateway directory. The entrypoint finds it via `find`.
5. **Paper accounts don't need 2FA** — use `TradingMode=paper` in config.ini
6. **The `ReadOnlyApi=no` + `AcceptIncomingConnectionAction=accept` settings
   in config.ini are critical** — without them, the write-access dialog blocks
   all API connections

## Data files transfer checklist

Before shutting down this laptop, copy these files to the other machine:
```bash
# The big ones (use an external drive or rsync over network)
rsync -av --progress data/backtest-results/stats/ other-laptop:rusty_trader/data/backtest-results/stats/

# The config (small but critical)
scp trading/trading-app/IBCMacos-3.20.0/config.ini other-laptop:rusty_trader/trading/trading-app/IBCMacos-3.20.0/

# The IBC zip
scp trading/trading-app/IBCLinux-3.21.2.zip other-laptop:rusty_trader/trading/trading-app/
```

## Estimated time on the other laptop

If the other laptop is **native amd64** (not Apple Silicon + Rosetta):
- Each 5-min request takes ~2-3s instead of ~12-40s
- Total pull time: ~2-4 hours instead of ~13-15 hours

If the other laptop is also **Apple Silicon + Rosetta**:
- Same speed as this laptop (~13-15 hours remaining)
- The monitor handles Gateway stalls automatically
