#!/bin/bash
set -e

# =============================================================================
# Optimizer entrypoint
# Waits for Postgres, then runs /bin/optimize. The binary does EVERYTHING:
#   1. SQLx migrations (idempotent).
#   2. Conditional data load: if bars exist for the period, skip the IBKR/
#      Alpaca fetch + just refresh the continuous aggregate; else, load via
#      load_market_data (with_gateway_retry internally, Alpaca fallback).
#   3. The optimization (grid/random/TPE) + robustness + Holdout/walk-forward.
#   4. The robustness report → robustness_report.html.
#
# The optimiser_params.json + the output are in /data (mounted).
# No Xvfb/IBC — the optimizer assumes the data is pre-loaded (via a prior
# `backtester` run) OR uses the Alpaca fallback.
# =============================================================================

DB_HOST="${DATABASE_HOST:-test-db}"
DB_PORT="${DATABASE_PORT:-5432}"
DB_NAME="${DB_DB:-trading}"
DB_USER="${DB_USER:-trading}"

echo "[optimize] Waiting for Postgres at $DB_HOST:$DB_PORT..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" >/dev/null 2>&1; do
    echo "[optimize] Postgres not ready, retrying in 2s..."
    sleep 2
done
echo "[optimize] Postgres is ready."

# Run the optimizer (reads optimiser_params.json from the working dir).
cd /data
exec /bin/optimize
