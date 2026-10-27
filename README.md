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

## Description

While the backend is offline, status pings receive `offline_motd`.  
A status ping also performs one readiness probe, allowing an externally started 
backend to be detected without starting the configured command.  
During startup, status pings receive `starting_motd`; a login starts the command 
once and receives the configured kick message.  
Once the backend accepts TCP connections, subsequent status and login traffic is 
forwarded directly.  
On shutdown, wakemc sends `messages.lost_connection` to active clients, and waits
up to five seconds for connection tasks to finish before aborting any remaining
tasks.

## Running locally

For development, wakemc can also be run directly with Cargo:

```sh
cargo run -- wakemc.toml
```