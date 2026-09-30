#!/bin/bash
# entrypoint-ibc.sh — headless IB Gateway auto-start via IBC.
#
# 1. Starts Xvfb (the IB Gateway is a Java GUI app — needs an X display even
#    in "headless" mode).
# 2. Starts IBC (ibcstart.sh) which launches the IB Gateway and auto-logins
#    using the credentials in /home/tws/ibc/config.ini (IbLoginId + IbPassword
#    + TradingMode=paper). IBC blocks in the foreground, keeping the container
#    alive while the Gateway runs.
#
# After IBC logs in, the IB Gateway listens on port 4002 (paper). The pull-1min
# service (or the trading-bot) connects as a client — no credentials needed
# for the client.
set -e

# ── display: use remote X server (headed via XQuartz) or start Xvfb (headless) ──
if [ -n "$DISPLAY" ] && [[ "$DISPLAY" != :* ]]; then
    echo "[entrypoint-ibc] headed mode: DISPLAY=$DISPLAY (remote X server)"
else
    # headless: start Xvfb
    mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix
    export DISPLAY=:99
    XVFB_RES="1920x1080x24"
    echo "[entrypoint-ibc] headless mode: starting Xvfb on $DISPLAY..."
    nohup Xvfb $DISPLAY -screen 0 $XVFB_RES > /tmp/xvfb.log 2>&1 &
    disown
    sleep 8
    if ! kill -0 $(pgrep -f "Xvfb :99") 2>/dev/null; then
        echo "[entrypoint-ibc] WARNING: Xvfb died! Log:"; cat /tmp/xvfb.log
    fi
fi

# ── IBC + IB Gateway paths ─────────────────────────────────────────────────
# IBC constructs the jars path as: ${tws_path}/ibgateway/${version}/jars
# Auto-detect the installed version from the jts4launch-XXXX.jar name so it
# works whether the installer is 10.30 or 10.50.
GW_INSTALL_DIR=/home/tws/ibgateway/1030
JAR=$(ls "$GW_INSTALL_DIR"/jars/jts4launch-*.jar 2>/dev/null | head -1)
DETECTED_VERSION=$(basename "$JAR" 2>/dev/null | sed -n 's/jts4launch-\([0-9]*\)\.jar/\1/p')
if [ -n "$DETECTED_VERSION" ] && [ "$DETECTED_VERSION" != "1030" ]; then
    mv "$GW_INSTALL_DIR" "/home/tws/ibgateway/$DETECTED_VERSION" 2>/dev/null || true
fi
TWS_VERSION=${DETECTED_VERSION:-1030}
echo "[entrypoint-ibc] detected IB Gateway version: $TWS_VERSION"
IBC_PATH=/home/tws/ibc
IBC_INI=$IBC_PATH/config.ini
TWS_PATH=/home/tws
export TWS_MAJOR_VRSN=$TWS_VERSION
export IBC_INI
export TRADING_MODE=${TRADING_MODE:-paper}
export TWS_PATH
export IBC_PATH
export LOG_PATH=/home/tws/ibc/logs

# ── Java path: find the install4j-bundled JRE (Java 8 with JavaFX), not the
# system OpenJDK 17 (which lacks JavaFX). The installer puts the JRE at
# /home/tws/.local/share/i4j_jres/<random>/<version>/bin/java ──────────────
I4J_JAVA=$(find /home/tws/.local/share/i4j_jres -name 'java' -type f 2>/dev/null | head -1)
if [ -n "$I4J_JAVA" ]; then
    JAVA_PATH=$(dirname "$I4J_JAVA")
else
    # fallback: bundled JRE in the install dir (10.50) or system Java
    if [ -d "/home/tws/ibgateway/${TWS_VERSION}/jre/bin" ]; then
        JAVA_PATH="/home/tws/ibgateway/${TWS_VERSION}/jre/bin"
    else
        JAVA_PATH="/usr/bin"
    fi
fi
echo "[entrypoint-ibc] using Java at: $JAVA_PATH ($("$JAVA_PATH/java" -version 2>&1 | head -1))"

echo "[entrypoint-ibc] starting IBC + IB Gateway"
echo "  mode=$TRADING_MODE  config=$IBC_INI  tws-path=$TWS_PATH  version=$TWS_VERSION"
echo "  (paper → IB Gateway listens on port 4002 after IBC logs in)"

# ── start IBC (blocks — keeps the container alive) ─────────────────────────
"$IBC_PATH/scripts/ibcstart.sh" "$TWS_VERSION" -g \
    --ibc-ini="$IBC_INI" \
    --ibc-path="$IBC_PATH" \
    --tws-path="$TWS_PATH" \
    --java-path="$JAVA_PATH" \
    --mode="$TRADING_MODE"
