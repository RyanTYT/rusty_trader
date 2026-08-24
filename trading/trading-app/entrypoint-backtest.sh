#!/bin/bash
set -e

# =============================================================================
# Backtest entrypoint
# Boots Xvfb (for IBC's GUI — needed for the IBKR data-load step), waits for
# Postgres, then runs /bin/backtest. The binary does EVERYTHING else:
#   1. Runs SQLx migrations (so the schema exists).
#   2. Data loader: IBKR first (with_gateway_retry boots IBC internally),
#      Alpaca fallback if IBKR is unavailable.
#   3. Replayer: real on_bar_update + real handle_bar_update_outcome.
#   4. Results: PnL, equity curve, max DD, Sharpe, Sortino → JSON.
# =============================================================================

XVFB_DISPLAY=:99
XVFB_RES="1920x1080x24"

# ---------- 1. Start Xvfb (virtual display for IB Gateway GUI) ----------
pkill Xvfb 2>/dev/null || true
LOCK_FILE="/tmp/.X${XVFB_DISPLAY#:}-lock"
[ -f "$LOCK_FILE" ] && rm -f "$LOCK_FILE"
echo "[backtest-entrypoint] Starting Xvfb on $XVFB_DISPLAY..."
Xvfb $XVFB_DISPLAY -screen 0 $XVFB_RES &
XVFB_PID=$!
export DISPLAY=$XVFB_DISPLAY

cleanup() {
    echo "[backtest-entrypoint] Cleaning up..."
    kill -TERM "$XVFB_PID" 2>/dev/null || true
    [ -f "$LOCK_FILE" ] && rm -f "$LOCK_FILE"
}
trap cleanup EXIT

# ---------- 2. Wait for Postgres (test-db) to be ready ----------
DB_HOST="${DATABASE_HOST:-test-db}"
DB_PORT="${DATABASE_PORT:-5432}"
DB_NAME="${DB_DB:-trading}"
DB_USER="${DB_USER:-trading}"

echo "[backtest-entrypoint] Waiting for Postgres at $DB_HOST:$DB_PORT..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" >/dev/null 2>&1; do
    echo "[backtest-entrypoint] Postgres not ready, retrying in 2s..."
    sleep 2
done
echo "[backtest-entrypoint] Postgres is ready."

# ---------- 3. Run the backtest binary ----------
# Env vars consumed by the binary:
#   DATABASE_URL / TEST_TRADING_DB_URL — pool connect string (required)
#   BACKTEST_START, BACKTEST_END       — RFC3339 (required)
#   BACKTEST_STOCK (QQQ), BACKTEST_PRIMARY_EXCHANGE (NASDAQ),
#     BACKTEST_CURRENCY (USD), BACKTEST_CAPITAL (100000),
#     BACKTEST_SLIPPAGE_BPS (0), BACKTEST_COMM_PER_SHARE (0.005),
#     BACKTEST_COMM_MIN (1.0), BACKTEST_OUTPUT (backtest_results.json)
#   ALPACA_API_KEY, ALPACA_API_SECRET — for the Alpaca data fallback (optional)
echo "[backtest-entrypoint] ==========================================="
echo "[backtest-entrypoint] Running backtest binary"
echo "[backtest-entrypoint]   period: $BACKTEST_START → $BACKTEST_END"
echo "[backtest-entrypoint]   stock:  $BACKTEST_STOCK"
echo "[backtest-entrypoint] ==========================================="

exec /bin/backtest
