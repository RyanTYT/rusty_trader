# Interactive Brokers' Trader Workstation (TWS) container with GUI
# displaying on the Docker host's X-server.
#
# This is based on alekna/ib-tws Interactive Brokers' Trader
# Workstation (TWS) docker container, which uses VNC to access the
# GUI.
#   https://github.com/alekna/docker-ib-tws
#
# It is also based on
#   https://tpaschalis.github.io/sandboxed-browser-with-docker
#
# and for a browser sandbox from a container and sound problems (?) fix.
#   https://github.com/TheBiggerGuy/docker-pulseaudio-example
#

FROM --platform=linux/amd64 debian:bullseye-slim

# Timezone is also in docker-compose file.
ENV HOME /root
ENV SHELL /bin/bash

RUN sed -i 's#\smain\s*\$# main contrib non-free#' /etc/apt/sources.list

RUN apt-get update; \
    apt-get install -y \
      unzip \
      --no-install-recommends \
      && rm -rf /var/lib/apt/lists/* \
      apt-get clean;

# Create a non-root account to run TWS with.
RUN groupadd --gid 501 tws && \
    useradd -ms /bin/bash --uid 501 --gid 501 tws && \
    usermod -G audio,video tws;


# Check the pulse-client.conf file (is your uid 1000 ?).
COPY config/pulse-client.conf /etc/pulse/client.conf

USER tws
WORKDIR /home/tws
ENV HOME /home/tws

RUN mkdir -p /home/tws/Desktop
RUN mkdir -p /home/tws/ibgateway/1030

# Retrieve and install TWS (and its embedded JRE). Choose between stable and latest.
# Stable: https://download2.interactivebrokers.com/installers/tws/stable/tws-stable-linux-x64.sh
# Latest: https://download2.interactivebrokers.com/installers/tws/latest/tws-latest-linux-x64.sh
# COPY --chown=tws tws-stable-standalone-linux-x64.sh /home/tws/tws-stable-standalone-linux-x64.sh
COPY --chown=tws:tws ibgateway-stable-standalone-linux-x64.sh /home/tws/ibgateway-stable-standalone-linux-x64.sh

RUN chmod +x ibgateway-stable-standalone-linux-x64.sh && \
    sh ibgateway-stable-standalone-linux-x64.sh -q -dir ibgateway/1030/ > ib-install.log 2>&1

# COPY --chown=tws tws.xml /home/tws/tws.xml

# The DISPLAY variable is required to display TWS on your desktop.
ENV PS1='$ '

ENTRYPOINT ["/bin/bash"]
