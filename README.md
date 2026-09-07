# Phonowire AudioSocket

This workspace contains a portable, allocation-free AudioSocket wire-model crate.
It validates raw frame payloads into typed, borrowed messages. Streaming decoding,
network I/O, endpoint deployment, and peer interoperability are outside this slice.

The selected profile and its tested boundary are documented in
[`docs/audiosocket.md`](docs/audiosocket.md).
