#!/bin/bash

# Start virtual display
Xvfb :99 -screen 0 1920x1080x24 &

XVFB_PID=$!

# Wait until the X11 socket is created
echo "Waiting for /tmp/.X11-unix/X99 to appear..."
for i in {1..20}; do
    if [ -e /tmp/.X11-unix/X99 ]; then
        echo "X11 socket found!"
        break
    fi
    echo "Still waiting for X11 socket... (${i})"
    sleep 0.5
done

echo "DISPLAY is set to $DISPLAY"
# Now do a sanity check with xdpyinfo
for i in {1..10}; do
    pids=$(pidof /usr/bin/Xvfb)
    if [ -n "$pids" ]; then
        echo "Xvfb is ready!"
        break
    fi
    echo "Waiting for xdpyinfo to connect... (${i})"
    sleep 1
done

# JTS_INI="/home/tws/jts.ini"
# TWS_XML="/home/tws/tws.xml"
# IBC_LOG="/home/tws/ibc.log"
#
# # If jts.ini doesn't exist
# if [ ! -f "$JTS_INI" ]; then
#     echo "jts.ini not found, launching IBC..."
#
#     # Run IBC start script in the background and get its PID
#     /IBCLinux-3.21.2/scripts/ibcstart.sh 1030 \
#         --tws-path=/home/tws \
#         --tws-settings-path=/home/tws \
#         --ibc-path=/IBCLinux-3.21.2 \
#         --ibc-ini=/IBCLinux-3.21.2/config.ini \
#         --user= \
#         --pw= \
#         --fix-user= \
#         --fix-pw= \
#         --java-path= \
#         --mode=$TRADING_TYPE \
#         --on2fatimeout=restart > "$IBC_LOG" 2>&1 &
#
#     # Capture the PID of the IBC process
#     IBC_PID=$!
#
#     # Wait for the IBC process to start and find "Login has completed"
#     echo "Waiting for TWS login to complete..."
#     while true; do
#         if grep -q "Login has completed" "$IBC_LOG"; then
#             echo "Login detected, terminating IBC process..."
#             # kill -INT "$IBC_PID"  # Send a keyboard interrupt to stop the IBC process
#             kill -TERM "$IBC_PID"
#             break
#         fi
#         sleep 1
#     done
#
#     # Wait for jts.ini to be written
#     sleep 2
#
#     # Parse the jts.ini file to extract usernames
#     if [ -f "$JTS_INI" ]; then
#         USERNAME_LINE=$(grep 'UserNameToDirectory=' "$JTS_INI" | cut -d '=' -f2)
#         USERNAME_LINE=$(echo "$USERNAME_LINE" | tr -d '\r')
#         IFS=',' read -r -a USERNAMES <<< "$USERNAME_LINE"
#         
#         for username in "${USERNAMES[@]}"; do
#             echo "Creating directory for $username..."
#             USER_DIR="/home/tws/$username"
#             mkdir -p "$USER_DIR"
#
#             # Copy tws.xml if it exists
#             if [ -f "$TWS_XML" ]; then
#                 echo "Copying tws.xml to $USER_DIR..."
#                 cp "$TWS_XML" "$USER_DIR"
#             else
#                 echo "tws.xml not found at $TWS_XML"
#             fi
#         done
#     else
#         echo "jts.ini not found even after waiting."
#         exit 1
#     fi
# else
#     echo "jts.ini already exists. Skipping IBC launch."
# fi


# # Start the mouse/keyboard keep-alive in background
# echo "Starting TWS anti-idle script..."
# openbox &
# sleep 1
# (
#   while true; do
#     DOW_WINDOW=$(xdotool search --name "Dow Jon" | head -n 1)
#     IB_WINDOW=$(xdotool search --name "Interactive Brokers")
#     if [ -n "$IB_WINDOW" ]; then
#         UNLOCKER=$(xdotool search --name "Unlock")
#         if [ -n "$UNLOCKER" ]; then
#             echo "Closing unlocker"
#             xdotool windowactivate "$UNLOCKER"
#             xdotool key Tab
#             xdotool type "nzkiwi2040"
#             xdotool key Return
#         fi
#         xdotool windowactivate "$DOW_WINDOW"
#         sleep 1
#         xdotool windowactivate "$IB_WINDOW"
#         xdotool windowfocus "$IB_WINDOW"
#         sleep 1
#         xdotool key Tab
#         sleep 1
#         xdotool key "Super+e"
#         sleep 5
#         xdotool key Escape
#     else
#         echo "IBKR Window Not found"
#         xdotool search --name "" | echo
#         xdotool search --name "" getwindowname | echo
#     fi
#     sleep 20
#   done
# ) &

# Launch your Python app
exec python3 -m app.run
# exec uv run -m app.run
