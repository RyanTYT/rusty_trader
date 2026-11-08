#!/bin/bash
# Boot IB Gateway under Xvfb via IBC, wait for a genuine login, then run the
# configured command. Modelled on trading-app/src/ibc.rs (IBGateway::start):
# IBC's own "Login has completed" line is the success signal, and the API port
# must then actually accept a handshake before we hand off.
#
# One container holds both the gateway and the puller, so the client reaches
# the gateway on 127.0.0.1 -- `network_mode: host` does not work on Docker
# Desktop for macOS.
set -uo pipefail

XVFB_DISPLAY="${XVFB_DISPLAY:-:99}"
XVFB_RES="${XVFB_RES:-1920x1080x24}"
GATEWAY_VERSION="${GATEWAY_VERSION:-1030}"
GATEWAY_PORT="${GATEWAY_PORT:-4002}"
IBC_COMMAND_PORT="${IBC_COMMAND_PORT:-7462}"
STARTUP_TIMEOUT="${GATEWAY_STARTUP_TIMEOUT:-300}"
IBC_LOG=/tmp/ibc.log
TWS_PATH=/home/tws

log() { echo "[entrypoint-ibc] $*"; }

# ── Guard: config.ini ships with TradingMode=live ────────────────────────────
# ibcstart.sh --mode=paper overrides the ini. If that flag is ever dropped the
# container would log into the LIVE account with live credentials, so refuse to
# start rather than discover it downstream.
CONFIG_MODE="$(grep -iE '^[[:space:]]*TradingMode=' /IBCLinux-3.21.2/config.ini \
               | tail -1 | cut -d= -f2- | tr -d '[:space:]')"
log "config.ini TradingMode=${CONFIG_MODE:-<unset>} -- forcing paper via --mode=paper"

# ── Xvfb ────────────────────────────────────────────────────────────────────
log "starting Xvfb on $XVFB_DISPLAY ($XVFB_RES)"
pkill Xvfb 2>/dev/null || true
LOCK_FILE="/tmp/.X${XVFB_DISPLAY#:}-lock"
rm -f "$LOCK_FILE"
Xvfb "$XVFB_DISPLAY" -screen 0 "$XVFB_RES" &
XVFB_PID=$!
export DISPLAY="$XVFB_DISPLAY"

mkdir -p "$TWS_PATH"
: > "$IBC_LOG"

# Declared up-front so `set -u` in cleanup can't trip on an unbound variable
# if we exit before IBC is launched.
IBC_PID=""
XVFB_PID=""

cleanup() {
  log "shutting down"
  # Graceful IBC stop via its command server (same STOP/EXIT/quit sequence as
  # ibc.rs::send_stop_commands), using bash's /dev/tcp so the slim image needs
  # no netcat. The subshell keeps bash's "Connection refused" noise out of the
  # log when IBC never came up.
  if (exec 3<>"/dev/tcp/127.0.0.1/${IBC_COMMAND_PORT}") 2>/dev/null; then
    printf 'STOP\nEXIT\nquit\n' >&3 2>/dev/null || true
    exec 3>&- 2>/dev/null || true
    sleep 2
  fi
  if [[ -n "$IBC_PID" ]]; then
    kill -TERM "$IBC_PID" 2>/dev/null || true
    sleep 2
    kill -KILL "$IBC_PID" 2>/dev/null || true
  fi
  if [[ -n "$XVFB_PID" ]]; then
    kill -TERM "$XVFB_PID" 2>/dev/null || true
  fi
  rm -f "$LOCK_FILE"
}
trap cleanup EXIT INT TERM

# ── JRE diagnostic ───────────────────────────────────────────────────────────
# ibcstart.sh resolves Java itself: read_from_config reads the install4j cfg
# (which holds the JRE ROOT) and appends /bin, then validates "$java_path/java".
# ibc.rs deliberately passes `--java-path=` empty for exactly that reason, so we
# do the same by default and let IBC's own resolution stand. JAVA_PATH can
# still be overridden for debugging. We only resolve it here to LOG it, which
# turns an opaque E_NO_JAVA into a diagnosable message.
INSTALL4J="$TWS_PATH/ibgateway/$GATEWAY_VERSION/.install4j"

resolve_java() {
  local cand
  for f in "$INSTALL4J/pref_jre.cfg" "$INSTALL4J/inst_jre.cfg"; do
    [[ -f "$f" ]] || continue
    cand="$(cat "$f")"
    if [[ -x "$cand/bin/java" ]]; then echo "$cand/bin"; return 0; fi
    if [[ -x "$cand/java" ]]; then echo "$cand"; return 0; fi
  done
  cand="$(find /usr/local/i4j_jres -maxdepth 3 -type d -name bin 2>/dev/null | head -1)"
  if [[ -n "$cand" && -x "$cand/java" ]]; then echo "$cand"; return 0; fi
  if command -v java >/dev/null 2>&1; then
    dirname "$(readlink -f "$(command -v java)")"; return 0
  fi
  return 1
}

if JAVA_PATH="$(resolve_java)"; then
  log "JRE available at $JAVA_PATH/java ($("$JAVA_PATH/java" -version 2>&1 | head -1))"
else
  log "WARNING: could not locate a JRE; leaving --java-path empty and letting"
  log "         ibcstart.sh resolve. Expect E_NO_JAVA if it also fails."
fi

# ── IBC ─────────────────────────────────────────────────────────────────────
log "launching IBC (gateway $GATEWAY_VERSION, paper)"
/IBCLinux-3.21.2/scripts/ibcstart.sh "$GATEWAY_VERSION" \
  --gateway \
  --tws-path="$TWS_PATH" \
  --tws-settings-path="$TWS_PATH" \
  --ibc-path=/IBCLinux-3.21.2 \
  --ibc-ini=/IBCLinux-3.21.2/config.ini \
  --user= --pw= --fix-user= --fix-pw= --java-path="${JAVA_PATH:-}" \
  --mode=paper \
  --on2fatimeout=restart \
  >>"$IBC_LOG" 2>&1 &
IBC_PID=$!

# ── Wait for login ───────────────────────────────────────────────────────────
# ibc.rs waits on the literal strings IBC prints; do the same.
log "waiting for 'Login has completed' (timeout ${STARTUP_TIMEOUT}s)"
DEADLINE=$((SECONDS + STARTUP_TIMEOUT))
LOGGED_IN=0
while [ "$SECONDS" -lt "$DEADLINE" ]; do
  if ! kill -0 "$IBC_PID" 2>/dev/null; then
    log "FATAL: IBC exited before login"
    tail -40 "$IBC_LOG"
    exit 1
  fi
  if grep -q "Login has completed" "$IBC_LOG"; then
    LOGGED_IN=1
    break
  fi
  if grep -q "IBC returned exit status" "$IBC_LOG"; then
    log "FATAL: IBC reported failure"
    tail -40 "$IBC_LOG"
    exit 1
  fi
  sleep 2
done

if [ "$LOGGED_IN" -ne 1 ]; then
  log "FATAL: timed out after ${STARTUP_TIMEOUT}s waiting for login"
  tail -40 "$IBC_LOG"
  exit 1
fi
log "IBC reported login complete"

# ── Wait for the API port ───────────────────────────────────────────────────
# Readiness probe: a BARE TCP connect-and-close is not enough. Gateway expects
# an immediate API\0 + version negotiation; opening a socket and dropping it
# leaves a half-open handshake pending, and Gateway then refuses the real
# client's connection (ib_async reports "API connection failed: TimeoutError").
# ibc.rs::probe_gateway_handshake exists for exactly this reason, so port that
# handshake verbatim, using ib_async's own version range so the probe and the
# real client negotiate identically.
gateway_ready() {
  python3 - "$1" <<'PY'
import socket, struct, sys
host, _, port = sys.argv[1].rpartition(":")
try:
    s = socket.create_connection((host, int(port)), timeout=5)
    s.settimeout(5)
    payload = b"v157..178\0"                     # ib_async Min/MaxClientVersion
    s.sendall(b"API\0" + struct.pack(">I", len(payload)) + payload)
    n = struct.unpack(">I", s.recv(4))[0]        # server_version\0connection_time\0
    got = b""
    while len(got) < n:
        chunk = s.recv(n - len(got))
        if not chunk:
            break
        got += chunk
    s.close()
except Exception:
    sys.exit(1)
sys.exit(0 if len(got) == n and n > 0 else 1)
PY
}

log "waiting for 127.0.0.1:${GATEWAY_PORT} API handshake"
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
  if gateway_ready "127.0.0.1:${GATEWAY_PORT}"; then
    log "gateway API handshake OK on ${GATEWAY_PORT}"
    break
  fi
  sleep 1
done

if ! gateway_ready "127.0.0.1:${GATEWAY_PORT}"; then
  log "FATAL: ${GATEWAY_PORT} never completed an API handshake after login"
  tail -40 "$IBC_LOG"
  exit 1
fi

# Gateway readiness races; give the socket a beat to settle.
sleep 2

log "gateway ready -- running: ${RUN_COMMAND}"
# shellcheck disable=SC2086
exec bash -c "$RUN_COMMAND"
