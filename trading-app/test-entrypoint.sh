#!/bin/bash
# NOTE: No `set -e` — we want to run ALL test binaries even if one fails,
# then report the aggregate result. The `if` constructs below handle individual
# failures gracefully.

# =============================================================================
# Test entrypoint for trading-app
# Boots Xvfb + IBC IB Gateway, waits for the gateway to be ready, then runs
# the full test suite (unit_tests, integration_tests, smoke_tests) and reports
# results. Exits with the test exit code so docker-compose propagates failure.
# =============================================================================

export CARGO_TERM_COLOR=always
export CLICOLOR_FORCE=1   # respected by many CLI tools (ripgrep, etc.) as a general "force color" convention

XVFB_DISPLAY=:99
XVFB_RES="1920x1080x24"
LOG_FILE=/tmp/ibc.log

# ---------- 1. Start Xvfb (virtual display for IB Gateway GUI) ----------
pkill Xvfb 2>/dev/null || true
LOCK_FILE="/tmp/.X${XVFB_DISPLAY#:}-lock"
[ -f "$LOCK_FILE" ] && rm -f "$LOCK_FILE"
echo "[test-entrypoint] Starting Xvfb on $XVFB_DISPLAY..."
Xvfb $XVFB_DISPLAY -screen 0 $XVFB_RES &
XVFB_PID=$!
export DISPLAY=$XVFB_DISPLAY

cleanup() {
    echo "[test-entrypoint] Cleaning up..."
    kill -TERM "$XVFB_PID" 2>/dev/null || true
    [ -f "$LOCK_FILE" ] && rm -f "$LOCK_FILE"
}
trap cleanup EXIT

# ---------- 2. Wait for Postgres (test-db) to be ready ----------
echo "[test-entrypoint] Waiting for Postgres at $DATABASE_HOST:5432..."
until pg_isready -h "$DATABASE_HOST" -p 5432 -d "${DB_DB:-trading}" -U "${DB_USER:-trading}" >/dev/null 2>&1; do
    echo "[test-entrypoint] Postgres not ready, retrying in 2s..."
    sleep 2
done
echo "[test-entrypoint] Postgres is ready."

# ---------- 3. Run SQLx migrations (idempotent) ----------
echo "[test-entrypoint] ==========================================="
echo "[test-entrypoint] Running trading-app test suite"
echo "[test-entrypoint] ==========================================="

export SQLX_OFFLINE=true
export RUST_BACKTRACE=1
export RUST_LOG=info

# Track overall result (0 = pass, non-zero = fail)
OVERALL_RESULT=0

run_test_binary() {
    local name="$1"
    local extra_args="$2"
    echo ""
    echo "[test-entrypoint] ─────────────────────────────────────────"
    echo "[test-entrypoint] Running: /app/bin/$name $extra_args"
    echo "[test-entrypoint] ─────────────────────────────────────────"
    # shellcheck disable=SC2086
    if /app/bin/"$name" $extra_args --nocapture --include-ignored --color=always 2>&1; then
        echo "[test-entrypoint] ✅ $name: PASSED"
    else
        echo "[test-entrypoint] ❌ $name: FAILED"
        OVERALL_RESULT=1
    fi
}

# 4a. Unit tests (no DB/IBKR needed) — fast
run_test_binary "unit_tests" ""

# 4b. Integration tests (need Postgres) — only run if DATABASE_URL is set
if [ -n "$DATABASE_URL" ]; then
    run_test_binary "integration_tests" ""
else
    echo "[test-entrypoint] ⏭️  integration_tests: SKIPPED (no DATABASE_URL)"
fi

# 4c. Smoke tests (need IB Gateway + Postgres) — boot IBC then run --ignored
echo ""
echo "[test-entrypoint] ─────────────────────────────────────────"
echo "[test-entrypoint] Booting IB Gateway via IBC..."
echo "[test-entrypoint] ─────────────────────────────────────────"
if [ -x /IBCLinux-3.21.2/scripts/ibcstart.sh ]; then
    # Start IBC in the background — the smoke tests call init_ibc_with_retry themselves
    # but we need the gateway running first. The smoke tests' live_ibkr() helper boots it.
    echo "[test-entrypoint] IBC script present. Smoke tests will boot the gateway on demand."
    # For smoke_tests, pass --ignored --nocapture as the test args (single --)
    echo ""
    echo "[test-entrypoint] ─────────────────────────────────────────"
    echo "[test-entrypoint] Running: cargo test --test smoke_tests -- --ignored --nocapture"
    echo "[test-entrypoint] ─────────────────────────────────────────"
    if /app/bin/smoke_tests --ignored --nocapture --test-threads=1 --color=always 2>&1; then
        echo "[test-entrypoint] ✅ smoke_tests: PASSED"
    else
        echo "[test-entrypoint] ❌ smoke_tests: FAILED"
        OVERALL_RESULT=1
    fi
else
    echo "[test-entrypoint] ⏭️  smoke_tests: SKIPPED (IBC not installed at /IBCLinux-3.21.2)"
fi

# ---------- 5. Final report ----------
echo ""
echo "[test-entrypoint] ==========================================="
echo "[test-entrypoint] FINAL TEST RESULTS"
echo "[test-entrypoint] ==========================================="
if [ $OVERALL_RESULT -eq 0 ]; then
    echo "[test-entrypoint] ✅ ALL TESTS PASSED"
else
    echo "[test-entrypoint] ❌ SOME TESTS FAILED (see output above)"
fi
echo "[test-entrypoint] ==========================================="

exit $OVERALL_RESULT
