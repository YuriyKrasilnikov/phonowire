# Phonowire capture

`phonowire-capture` records incoming AudioSocket connections to bounded,
create-new directories. It is a Linux source-workspace application.

Run with an existing output directory:

```sh
cargo run -p phonowire-capture -- --output ./captures --listen 127.0.0.1:9092
```

Each connection has transport-local naming and writes wire, WAVE, diagnostics,
and summary data. The CLI refuses collisions and has finite per-run recording
admission. SIGINT and SIGTERM request a drain; they do not promise lossless
capture or preempt filesystem operations. See `../../docs/capture.md`.

Build a local container from an allowlisted context with
`apps/phonowire-capture/build-image.sh phonowire-capture:local`.
