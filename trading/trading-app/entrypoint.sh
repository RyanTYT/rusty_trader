#!/bin/bash
set -e

XVFB_DISPLAY=:99
XVFB_RES="1920x1080x24"
LOG_FILE=/tmp/ibc.log

# Start Xvfb
pkill Xvfb || true
# Remove stale lock files if they exist
LOCK_FILE="/tmp/.X${XVFB_DISPLAY#:}-lock"
if [ -f "$LOCK_FILE" ]; then
    echo "Removing stale lock file $LOCK_FILE"
    rm -f "$LOCK_FILE"
fi
echo "Starting Xvfb on $XVFB_DISPLAY..."
Xvfb $XVFB_DISPLAY -screen 0 $XVFB_RES &
XVFB_PID=$!
export DISPLAY=$XVFB_DISPLAY

# Cleanup on exit
cleanup() {
    echo "Cleaning up..."
    kill -TERM "$XVFB_PID" 2>/dev/null || true

    # Remove lock file if it exists
    LOCK_FILE="/tmp/.X${XVFB_DISPLAY#:}-lock"
    if [ -f "$LOCK_FILE" ]; then
        echo "Removing $LOCK_FILE"
        rm -f "$LOCK_FILE"
    fi
}
trap cleanup EXIT

# Run Rust tests if IBC logged in
# echo "Running Rust integration tests..."
# cargo test --locked --all-targets --all-features -- --nocapture
# cargo test --test models -- --nocapture
# cargo test -- --nocapture --skip models
/bin/server
