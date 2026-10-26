# wakemc

Simple Minecraft Java Edition TCP proxy that wakes a backend server when a player
tries to log in.

## Usage

Copy [`wakemc.toml.example`](./wakemc.toml.example) to `wakemc.toml`, adjust the
backend and startup command, then run:

```sh
cargo run -- wakemc.toml
```

The configuration contains:

- `listen`: proxy address, default `0.0.0.0:25565`;
- `[backend]`: backend host, port, and TCP probe interval in milliseconds;
- `[startup]`: bash command and maximum startup time in seconds. On Unix, a
  timed-out command is run in its own process group and the whole group is
  terminated and reaped;
- `[messages]`: offline MOTD, starting MOTD, and login kick text.

While the backend is offline, status pings receive `offline_motd`. A status ping
also performs one readiness probe, allowing an externally started backend to be
detected without starting the configured command. During startup, status pings
receive `starting_motd`; a login starts the command once and receives the
configured kick message. Once the backend accepts TCP connections, subsequent
status and login traffic is forwarded directly.
