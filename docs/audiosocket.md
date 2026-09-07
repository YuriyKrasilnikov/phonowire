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
