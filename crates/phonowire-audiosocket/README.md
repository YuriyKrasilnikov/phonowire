# phonowire-audiosocket

`no_std` AudioSocket framing and wire values with borrowed payload views and
explicit semantic validation. `RawDecoder` preserves each complete envelope;
`Decoder` refuses malformed known message shapes before body storage and returns
typed messages. Both decode at most one frame per call, use caller-owned scratch,
and require `finish` to declare EOF. `encode` and `encode_raw` write one complete
frame only when the whole destination is available. Returned values borrow caller
scratch; the crate owns no caller input, allocates nothing, and performs no I/O.
