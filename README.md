# wakemc

Simple Minecraft Java Edition TCP proxy that wakes a backend server when a player
tries to log in.

## Usage with Docker Compose

1. Copy `wakemc.toml.example` into `wakemc.toml` and adjust it as needed;

2. Create `docker-compose.yml`:

```yaml
services:
  wakemc:
    image: ghcr.io/zapolab/wakemc:latest
    container_name: wakemc
    network_mode: host
    restart: unless-stopped
    volumes:
      - ./wakemc.toml:/app/wakemc.toml:ro
```

The host network mode makes the `listen` address and port from `wakemc.toml`
available directly on the host. It also lets wakemc reach a backend running on
the host through the address configured in `[backend]`;

The runtime image includes `wakeonlan`, `openssh-client`, and `iputils-ping`.
These tools are available to the configured `startup.command` and can be used
by its startup script.

### Wake-on-LAN example

This proxy supports running backend on external machines and boot them with wake-on-LAN.

```bash
#!/bin/bash
SERVER_IP="192.169.0.x"
SERVER_MAC="AA:BB:CC:DD:EE:FF"
SERVER_USER="user"
MC_PORT="25565"

if ping -c 1 -W 1 "$SERVER_IP"; then
    # server online, MC container stopped
    echo "Starting MC"
    ssh ${SERVER_USER}@${SERVER_IP} "docker start mc-server"
else
    # server offline
    echo "Booting server"
    until ssh -o ConnectTimeout=5 -o BatchMode=yes ${SERVER_USER}@${SERVER_IP} "docker start mc-server"; do
        wakeonlan "$SERVER_MAC"
        sleep 5
    done
    echo "Server online"
fi

# wait mc server 
until timeout 2 bash -c "echo > /dev/tcp/$SERVER_IP/$MC_PORT"; do
    sleep 5
done
echo "MC online"
```

Adjust it as needed.

## Description

wakemc tracks the backend through three states:

- **Offline**: status pings receive `offline_motd`. Each status request performs
  a readiness probe, so a backend started externally is detected without
  executing `startup.command`.
- **Starting**: the first login changes the state immediately, starts
  `startup.command` through `bash -c`, and returns the configured `kick`
  message. Further logins do not start additional commands. Status pings
  receive `starting_motd`.
- **Online**: once the backend accepts TCP connections, status and login
  traffic is forwarded directly to it.

The startup command is considered successful only when the backend becomes
reachable. wakemc checks it until `timeout_seconds` expires; on timeout, the
startup process group is terminated and the backend returns to `Offline`.
When the backend process later exits, wakemc marks it offline again. Concurrent
readiness checks are coalesced and cached for `probe_interval_ms`:

- The cache stores the result of the last TCP connection attempt to the
  configured backend address;
- While the entry is valid, `probe()` returns that result without opening
  another connection. The validity period starts when the probe completes, and
  is controlled by `backend.probe_interval_ms`;
- If several requests arrive after the entry expires, only the first one
  performs the TCP probe. The others wait for that probe to finish and then
  receive the same result;

The configuration file is TOML. See [`wakemc.toml.example`](wakemc.toml.example)
for all available settings.

On shutdown, wakemc stops accepting clients, closes active connections, and
waits up to five seconds for connection tasks to finish before aborting any
remaining tasks.

## Running locally

For development, wakemc can also be run directly with Cargo:

```sh
cargo run -- wakemc.toml
```