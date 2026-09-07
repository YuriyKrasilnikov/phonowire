# AudioSocket protocol contract

The wire frame is a one-byte type followed by a big-endian `u16` payload length.
The documented types are `00` terminate, `01` UUID (exactly 16 bytes), `03` one
ASCII DTMF byte, `10`–`18` PCM16LE mono at 8k, 12k, 16k, 24k, 32k, 44.1k, 48k,
96k, and 192k Hz, and `ff` opaque peer error. Unassigned bytes remain explicit
unknown frames with opaque payloads.

The selected target is upstream Asterisk 23.2.0, AudioSocket dialplan application,
signed linear PCM16LE mono at 8kHz, direction Asterisk to service only. The wire
reference is [Asterisk AudioSocket documentation](https://docs.asterisk.org/Configuration/Channel-Drivers/AudioSocket/).
This is a pinned lab profile, not evidence of a successful peer connection, a
customer configuration, reverse playback, or deployment compatibility.

Raw envelopes preserve every type byte and payload up to 65535 bytes for diagnostics.
Typed conversion applies explicit policies: terminate requires an empty payload;
UUID requires 16 bytes; DTMF requires one ASCII byte; PCM requires an even payload
length; opaque error and unknown values retain validated bounded borrowed bodies.
The protocol type does not infer TCP framing, source sample measurements, business
identity, reconnect policy, or timing.

The crate has two incremental framing APIs over caller-provided body scratch.
`RawDecoder` returns every complete envelope that fits the scratch. `Decoder`
checks known declared lengths after consuming the three-byte header and before
copying a body, then returns only typed messages. Each `feed` call consumes at
most one frame and advances its input slice by the consumed prefix; empty feeds
need more input rather than declaring EOF. A returned frame borrows the decoder,
so the decoder cannot be fed again until that view is dropped. `finish` consumes
the decoder and reports an unfinished header or body explicitly. Terminal decode
errors are absorbing.

Outbound callers construct validated DTMF values with `Dtmf::new` and PCM payload
views with `AudioPayload::new`. The latter accepts only even payload lengths at
most 65535 bytes. These constructors let typed messages be encoded without first
creating a raw envelope or choosing a numeric type tag.

`encode` and `encode_raw` write a single exact frame into a caller destination.
They check the complete output capacity before changing it; on success they leave
the remaining destination suffix untouched.

## Incoming session policy

`IncomingSession` applies the selected AP1 incoming policy to already validated
`TypedMessage` values. Its first UUID establishes the protocol identity, including
an all-zero UUID. Audio and DTMF before that UUID fail with `MissingUuid`. Exactly
8 kHz PCM and the ASCII digits `0`–`9`, `*`, `#`, and `A`–`D` are accepted; other
rates and digits fail. A second UUID and every unknown type also fail. Each policy
failure ends the session, and every later message or reported EOF returns
`AfterEnd`.

Terminate, peer error, and clean end of input end a session without a policy
failure and remain distinct `SessionEnd` values. They may happen before a UUID,
so their event identity is optional. A peer-error event borrows its opaque payload
from the typed message; the session owns only its UUID state.

`IncomingSession` does not decode bytes. The caller reports clean EOF only after
its byte decoder successfully finishes at a frame boundary. A malformed or
truncated decoder result stays a decoder error rather than becoming a clean
session EOF.
