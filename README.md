# Phonowire

Portable protocol components for telephony applications, starting with
AudioSocket. The `phonowire-audiosocket` crate provides allocation-free,
`no_std` framing and incoming-session policy with no external dependencies.
Applications own I/O and storage; the codec processes byte slices.
The `phonowire-receiver` crate adds bounded Linux TCP reception with owned
observations, explicit resource credits and caller-controlled shutdown.

## AudioSocket support

- Raw envelopes preserve every type byte and bounded payload.
- Typed messages validate UUID, ASCII DTMF, nine PCM rates and opaque values.
- Incremental decoders preserve framing across fragmented or coalesced input.
- Encoding preserves the destination when a complete frame cannot fit.
- Explicit errors distinguish malformed input, capacity refusal and truncation.
- Incoming sessions preserve UUID identity and separate terminal causes.

The [protocol contract](docs/audiosocket.md) separates general wire values from
incoming session policy. `IncomingSession` and `Receiver::bind` accept all nine
documented PCM rates by default; callers can use `IncomingProfile` to select a
narrower accepted set. Laboratory calls from Asterisk 23.2.0
confirmed exact incoming PCM and separate connection identities for two
simultaneous streams, including controlled fragmentation and natural TCP EOF.
The contract describes the tested profile and its compatibility limits.

## Storage and lifetime

```mermaid
flowchart LR
    Input["Received byte slice"] --> Decoder["Decoder with caller scratch"]
    Decoder --> Message["Borrowed typed message"]
    Message --> Session["IncomingSession"]
    Session --> Consumer["Application consumes event"]
```

The decoder copies payload bytes into caller-provided scratch. Its returned
view borrows the decoder, so that storage cannot be reused while the view is
still used. Retaining data beyond that borrow requires an explicit owned copy.
`finish` reports an incomplete frame; report clean session EOF only after it
succeeds. Allocation-free decoding therefore does not mean zero-copy decoding.

## Try the consumer

Requires Rust 1.98.1 and Edition 2024. The repository pins its toolchain.
From the workspace root:

```sh
cargo run --example incoming_session
```

The [example](crates/phonowire-audiosocket/examples/incoming_session.rs) feeds
fragmented literal frames through the decoder and session, then validates clean
completion. The [crate README](crates/phonowire-audiosocket/README.md) describes
the public entry points.

## Use the libraries

Both crates are currently unpublished. To use a local source checkout from a
sibling application, add the required dependencies to that application's
`Cargo.toml`:

```toml
[dependencies]
phonowire-receiver = { path = "../phonowire/crates/phonowire-receiver" }
phonowire-audiosocket = { path = "../phonowire/crates/phonowire-audiosocket" }
```

Paths are relative to the application's manifest. The receiver requires Linux.
A codec-only application needs just `phonowire-audiosocket`. Add the codec beside
the receiver when naming its public types, such as `Uuid` or `SampleRate`; use
both crates from the same checkout. The receiver itself already depends on the
codec. The workspace lockfile fixes this repository's dependency resolution;
an independent application manages its own lockfile.

Dependency direction is intentional: `phonowire-audiosocket` is the portable
codec; `phonowire-receiver` depends on that codec for Linux TCP reception; and
`phonowire-capture` depends on the receiver to write bounded diagnostic
recordings. Applications depend on the lowest layer that meets their needs.

## TCP reception

The [receiver API](crates/phonowire-receiver/README.md) separates network work
from record consumption. A caller-owned thread runs `Receiver::run`; another
consumer removes records, processes or retains their payloads, and returns
queue or byte capacity. Raw transport observations precede protocol events.
The receiver reports abandoned work at cancellation and keeps retained data
charged across worker restarts. These resource bounds do not establish a
measured call capacity or a latency guarantee.

`Recording` consumes one connection's records into exact wire bytes, accepted
PCM WAVE audio and diagnostic text. `Recording::with_sample_rate` writes a WAVE
header for its selected rate and rejects a different audio rate before appending
PCM; `Recording::new` is the explicit 8 kHz compatibility constructor.
Finalization preserves terminal meaning; writer failures report their stage and
confirmed output prefixes.
The [recording example](crates/phonowire-receiver/examples/record_incoming.rs)
executes a literal localhost call through both components.

## Capture application

The maintained `phonowire-capture` CLI receives 8 kHz-profile incoming
AudioSocket connections into bounded create-new output directories. This keeps
its existing per-connection WAVE contract explicit while per-recording rate
selection remains a separate application capability. Run it from source with
`cargo run -p phonowire-capture -- --output ./captures`; see the
[capture contract](docs/capture.md) for limits and shutdown behavior.

## Repository layout

```text
Cargo.toml                    Workspace metadata and lints
Cargo.lock                    Reproducible workspace dependency resolution
rust-toolchain.toml           Pinned compiler version
docs/audiosocket.md            Wire and session contract
crates/phonowire-audiosocket/
    Cargo.toml                Package metadata
    README.md                 Package overview
    src/lib.rs                Public API exports
    src/                      Wire values, framing, encoding, session and errors
    tests/                    Public API and conformance tests
    examples/                 Executable decoder/session consumer
crates/phonowire-receiver/
    Cargo.toml                Linux receiver dependencies
    README.md                 Resource, observation and shutdown contracts
    src/lib.rs                Receiver and owned record API
    src/                      Private connection, scheduling and handoff modules
    tests/                    Localhost behavior and lifecycle witnesses
LICENSE                       Apache License 2.0
NOTICE                        Copyright attribution
```

The virtual workspace shares package metadata and lints. The codec's modules
separate wire values, validation, incremental framing, encoding and session
ordering. Runtime and operating-system choices belong to consumers of this API.

## Validate and read the API

```sh
cargo fmt --all -- --check
cargo build --workspace --all-targets
cargo test --workspace --all-targets
cargo test --workspace --doc
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
cargo package -p phonowire-audiosocket --offline --locked
```

The tests compare observable bytes, cursor movement, errors and session events
against independent expectations. The finite conformance suite is not a proof
of every possible stream or a measurement of network capacity.

The package command above verifies the codec archive. Receiver use from the
source workspace is separate from registry packaging: its sibling codec must
also be available when resolving a normalized receiver package.

## Receiver run summary

`RunSummary` includes final-only accept-pressure counters: retryable errors, resource pause entries, and retry-quota backoffs. Adding these public fields is source-breaking for downstream exhaustive struct literals; consumers should construct with `RunSummary::default()` and update named fields where needed.

Licensed under [Apache-2.0](LICENSE).
Copyright 2026 Yuriy Krasilnikov; see [NOTICE](NOTICE).
