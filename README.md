# Phonowire AudioSocket

A portable AudioSocket codec and incoming-session policy for Rust. The crate uses
caller-owned storage, allocates nothing, and has no external dependencies.

- Raw and typed framing across arbitrary byte chunks.
- Validated UUID, ASCII DTMF, all nine declared PCM rates and opaque payloads.
- Exact encoding with unchanged output on insufficient capacity.
- Explicit truncation, capacity refusal and terminal session outcomes.

The [protocol contract](docs/audiosocket.md) distinguishes wire rules from the
incoming 8 kHz session policy. Asterisk 23.2.0 is the selected laboratory target;
a real peer connection has not been tested. Network transports are outside this
crate.

Build with Rust 1.98.1 (Edition 2024):

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

The [incoming-session example](crates/phonowire-audiosocket/examples/incoming_session.rs)
feeds fragmented literal frames through the decoder and session, then reports a
clean end only after successful framing completion:

```sh
cargo run --example incoming_session
```

Licensed under [Apache-2.0](LICENSE). Copyright 2026 Yuriy Krasilnikov.
