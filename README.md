# clarity

Real-time peer-to-peer voice chat in Rust. Up to 5 peers per room, audio over direct UDP (never through the server), mute/unmute, low latency.

## Demo

https://github.com/user-attachments/assets/b307e6ea-c421-4e71-9152-8d2f1523a717


## What it is

A small two-binary Rust project:

- `clarity_be` — WebSocket signaling server. Manages room membership and relays SDP/ICE between peers. Never touches audio.
- `clarity_fe` — Iced desktop client. Captures microphone audio, encodes with Opus, negotiates WebRTC via str0m, and sends RTP directly to each peer over UDP.

Audio flows peer-to-peer over encrypted UDP (DTLS-SRTP). The server only sees the initial handshake messages.

## Tech stack

| Component              | Library                                         |
| ---------------------- | ----------------------------------------------- |
| GUI                    | [iced](https://iced.rs/) 0.14                   |
| WebRTC (sans-I/O)      | [str0m](https://github.com/algesten/str0m) 0.19 |
| Audio capture/playback | [cpal](https://github.com/RustAudio/cpal) 0.17  |
| Codec                  | [opus](https://crates.io/crates/opus) 0.3       |
| Async runtime          | tokio                                           |
| Signaling              | tokio-tungstenite (WebSockets over TCP)         |

## Architecture

```
┌──────────────────┐     WebSocket (TCP)     ┌──────────────────┐
│  clarity_fe (A)  │ ◄─── signaling only ───►│   clarity_be     │
│                  │                          │  (relay server)  │
│  ┌────────────┐  │                          │                  │
│  │ Iced GUI   │  │                          │  Rooms[5]        │
│  └────────────┘  │                          │  ── Peer list    │
│  ┌────────────┐  │                          │  ── Signal relay │
│  │ AudioCap.  │  │                          │                  │
│  │ + Opus enc │  │                          └────────┬─────────┘
│  └─────┬──────┘  │                                   │
│        │         │   ┌──────────────────┐            │
│  ┌─────▼──────┐  │   │  clarity_fe (B)  │ ◄──────────┘
│  │ PeerTask N │  │   └──────────────────┘
│  │ (str0m+UDP)│  │
│  └─────┬──────┘  │
└────────┼─────────┘
         │
         │  Direct UDP (RTP / DTLS-SRTP)
         └──────────────────────────────► other peers
```

Each remote peer is managed by its own tokio task (`PeerTask`) that owns an `Rtc` instance, a UDP socket, an Opus decoder, and a cpal output stream. Audio capture runs on a dedicated OS thread, encoding to Opus once and broadcasting to all PeerTasks (no per-peer re-encoding).

## Requirements

- Rust toolchain (edition 2024 — recent stable)
- macOS, Linux, or Windows (cpal supports all three)
- An audio input device (microphone)

System dependencies for the `opus` crate:

```bash
# macOS
brew install pkg-config opus

# Debian/Ubuntu
sudo apt-get install pkg-config libopus-dev
```

## Build and run

The two binaries live in separate workspace members. Open two terminals.

**Terminal 1 — start the signaling server:**

```bash
cd clarity_be
cargo run
```

The server listens on `localhost:7878`.

**Terminal 2 — start a client:**

```bash
cd clarity_fe
cargo run
```

Open additional terminals and run the same `cargo run` to launch more clients. Each client joins the same `localhost:7878` server and can join any of the 5 pre-created rooms.

To test voice chat end-to-end, launch two clients, click `Enter`, then `Join` on the same room from both. Once ICE completes you should see `● connected` next to the other peer's UUID and hear them speak.

## Project layout

```
clarity_fs/
├── clarity_be/               # Signaling server (one main.rs)
│   └── src/main.rs
├── clarity_fe/               # Desktop client
│   ├── src/
│   │   ├── main.rs           # Iced app, message handling, subscriptions
│   │   ├── client.rs         # WebSocket client + ClientMessage / ServerMessage
│   │   ├── peer.rs           # PeerTask, SDP handling, str0m poll loop
│   │   ├── audio.rs          # AudioCapture (mic + Opus) + PlaybackHandle
│   │   └── utility.rs        # Color palette helpers
│   └── Cargo.toml
└── docs/                     # Design spec + implementation plan
```

## Current limitations

- **LAN only.** Only host ICE candidates are gathered (no STUN/TURN), so peers must be on the same local network. Reaching across NATs requires adding STUN — see roadmap.
- **5 peers max.** Mesh topology means N(N-1)/2 connections; viable up to ~5 peers. Larger rooms would need an SFU.
- **Hardcoded server address.** Clients connect to `localhost:7878` (see `clarity_fe/src/client.rs`).
- **5 rooms, generated at startup.** No room creation/destruction protocol.
- **No persistent identity.** Each connection gets a fresh UUID.

## Roadmap

- STUN binding requests for server-reflexive candidates (cross-NAT)
- TURN fallback for symmetric NAT
- Speaking indicators (RMS detection on incoming audio)
- Per-peer volume sliders
- Configurable server address
- Reconnection on WebSocket drop
