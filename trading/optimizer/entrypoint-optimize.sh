#!/bin/bash
set -e

# =============================================================================
# Optimizer entrypoint
# Boots Xvfb (for IBC's GUI — needed when the bars aren't pre-loaded + the
# optimizer falls through to `load_market_data` → `with_gateway_retry` → IBC),
# waits for Postgres, then runs /bin/optimize. The binary does EVERYTHING:
#   1. SQLx migrations (idempotent).
#   2. Conditional data load: if bars exist for the period, skip the IBKR/
#      Alpaca fetch + just refresh the continuous aggregate; else, load via
#      load_market_data (with_gateway_retry → IBC, Alpaca fallback).
#   3. The optimization (grid/random/TPE) + robustness + Holdout/walk-forward.
#   4. The robustness report → robustness_report_{name}.html.
#
# The optimiser_params.json + the output are in /data (mounted).
# =============================================================================

XVFB_DISPLAY=:99
XVFB_RES="1920x1080x24"

# ---------- 1. Start Xvfb (virtual display for IB Gateway GUI) ----------
pkill Xvfb 2>/dev/null || true
LOCK_FILE="/tmp/.X${XVFB_DISPLAY#:}-lock"
[ -f "$LOCK_FILE" ] && rm -f "$LOCK_FILE"
echo "[optimize-entrypoint] Starting Xvfb on $XVFB_DISPLAY..."
Xvfb $XVFB_DISPLAY -screen 0 $XVFB_RES &
XVFB_PID=$!
export DISPLAY=$XVFB_DISPLAY

cleanup() {
    echo "[optimize-entrypoint] Cleaning up..."
    kill -TERM "$XVFB_PID" 2>/dev/null || true
    [ -f "$LOCK_FILE" ] && rm -f "$LOCK_FILE"
}
trap cleanup EXIT

# ---------- 2. Wait for Postgres (test-db) to be ready ----------
DB_HOST="${DATABASE_HOST:-test-db}"
DB_PORT="${DATABASE_PORT:-5432}"
DB_NAME="${DB_DB:-trading}"
DB_USER="${DB_USER:-trading}"

echo "[optimize-entrypoint] Waiting for Postgres at $DB_HOST:$DB_PORT..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" >/dev/null 2>&1; do
    echo "[optimize-entrypoint] Postgres not ready, retrying in 2s..."
    sleep 2
done
echo "[optimize-entrypoint] Postgres is ready."

# ---------- 3. Run the optimize binary ----------
# Env vars consumed by the binary:
#   DATABASE_URL / TEST_TRADING_DB_URL — pool connect string (required)
#   ALPACA_API_KEY, ALPACA_API_SECRET — for the Alpaca data fallback (optional)
#   BACKTEST_REPORT_PATH — where to write the report (default: /data/robustness_report_{name}.html)
# The period (start/end) + the optimization settings come from
# optimiser_params.json (in /data) — NOT from env vars.
echo "[optimize-entrypoint] ==========================================="
echo "[optimize-entrypoint] Running optimize binary"
echo "[optimize-entrypoint]   params: /data/optimiser_params.json"
echo "[optimize-entrypoint] ==========================================="

cd /data
exec /bin/optimize
