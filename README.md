# Phonowire AudioSocket

This workspace contains a portable, allocation-free AudioSocket codec crate. It
provides raw and typed borrowed messages, incremental decoding with caller-owned
scratch storage, and bounded single-frame encoding. It performs no network I/O or
endpoint deployment; peer interoperability is not exercised here.

The selected profile and its tested boundary are documented in
[`docs/audiosocket.md`](docs/audiosocket.md).
