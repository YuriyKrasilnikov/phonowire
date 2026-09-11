# phonowire-audiosocket

Allocation-free, `no_std` AudioSocket framing with borrowed payloads and explicit
validation. `RawDecoder` preserves complete wire envelopes; `Decoder` validates
known message shapes before body storage and returns typed messages. Both use
caller-owned scratch and return at most one frame per feed. `finish` distinguishes
complete input from truncation. `encode` and `encode_raw` preserve the destination
if the entire frame does not fit.

`IncomingSession` applies the incoming UUID, PCM, DTMF and end policy to typed
messages. Its default profile accepts all nine documented PCM wire rates and
preserves each declared rate in the audio event. `IncomingProfile` lets a caller
select a narrower set. Decoded views borrow scratch; session events preserve
their payload borrow and protocol UUID. The crate owns no caller storage and
performs no I/O.

Run `cargo run --example incoming_session` for a complete decoder/session consumer
using fragmented literal input and explicit end-of-input validation.

Rust 1.98.1, Edition 2024. Licensed under Apache-2.0; copyright Yuriy Krasilnikov.
