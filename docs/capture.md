# Capture CLI

`phonowire-capture` requires an existing output root; it does not create that
root. Install locally with `cargo install --path apps/phonowire-capture` or run
from the workspace with `cargo run -p phonowire-capture -- --output ./captures`.

`--listen` defaults to `127.0.0.1:9092`; `--max-connections` and
`--max-active-recordings` default to 16; `--queue-records` is 128;
`--payload-bytes` is 4096; `--turn-steps` is 64; `--retained-bytes` is
1,048,576; and `--max-recordings-per-run` is 64. Each data file has default
`--max-file-bytes` 67,108,864, each `summary.json` has default
`--max-recording-summary-bytes` 4096, and the single `run-status.json` has
default `--max-run-status-bytes` 4096.

The checked bound is `N * (3*C + S) + R`, where `N` is cumulative recording
admission, `C` is the final logical byte cap for wire, WAVE, and events, `S`
is the per-recording summary cap, and `R` is the one run-status cap. The WAVE
header occupies 44 logical bytes and its final replacement overwrites those
bytes; it is not a claim about cumulative write calls, filesystem quota, free
space, metadata, fsync, crash recovery, or a hard I/O timeout.

Each create-new connection directory contains `wire.bin`, `audio.wav`,
`events.log`, and `summary.json`; the output root contains `run-status.json`.
Names use receiver instance and sequence. Existing directories and files are
refused and are never reused. A partial directory remains after failure or a
crash, so a rerun with the same identity refuses the collision rather than
recovering or overwriting it. Failed and incomplete recordings make the process
exit unsuccessfully; an idle stop after clean recordings can exit successfully.
SIGINT/SIGTERM request orderly cancellation, drain queued records, and then
finalize active nonterminal output as incomplete.

`first_stop_cause` records the first successful application controller claim,
not the earliest wall-clock operating-system signal. The `signals` counter is
bounded diagnostic information and does not establish an ordering among
coalesced signal deliveries and local failures.
