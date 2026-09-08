# Phonowire receiver

A bounded incoming AudioSocket receiver for Linux. One caller-owned worker
handles nonblocking TCP connections and sends owned observations to a consumer.
The portable `phonowire-audiosocket` codec supplies framing and incoming session
policy; Mio supplies readiness notifications.

The incoming profile accepts one UUID before mono PCM16LE at 8 kHz or supported
DTMF. This describes the receiver's policy. Compatibility with a particular peer
also depends on its version, operating mode and codec configuration.

## Ownership and progress

```text
TCP readiness ──→ worker ──→ raw bytes ──→ decoder/session
                     │                          │
                     └──── bounded records ─────┘
                                  │
                               consumer
                                  │
                     queue slot / byte credit
                                  │
                              worker wake
```

The worker owns connection state, input buffers and decoder scratch. Borrowed
codec views end before scratch reuse. Retained output owns its storage and keeps
its shared `ByteBudget` charged until drop. A queue slot and a byte credit are
separate resources: consuming a record releases its slot, while retaining that
record keeps its payload charged.

A full output queue parks the corresponding work. Credit return wakes the worker
even when the sender sends no further bytes. A task that exhausts its turn budget
remains ready without waiting for a new network event. Application callbacks and
disk writes belong to the consuming thread.

`Receiver::run` executes synchronously on the calling thread. Move the receiver
into a `std::thread` when reception and consumption need separate threads.
`StopHandle` supplies an independent stop path. A stop cancels pending connection
work; it does not promise that every received byte reached the consumer. Queued
records remain available after the worker stops.

## Limits and observations

`Limits` validates connection count, queue capacity, decoder payload capacity and
turn size. Decoder capacity is a local resource limit, not a change to the wire
format. `fixed_buffer_capacity` bounds accessible transport and decoder buffers.
The shared output budget separately counts each retained `Box<[u8]>` allocation.
Reusing that budget for another worker preserves charges for old retained records.
A budget supports one active worker subscription.

These bounds exclude allocator bookkeeping, thread stacks, OS socket buffers and
RSS. No number of supported calls or latency guarantee follows from the limits.

Raw records carry the start offset of the observed read chunk. Protocol records
carry the end offset consumed by the decoder. Observation timestamps use the
receiver's monotonic clock; they are not source capture timestamps. Clean EOF,
truncation, malformed input, policy rejection, peer termination and transport
failure remain distinct. Already observed raw bytes remain separate from their
interpretation.

The receiver handles plaintext incoming TCP. TLS, WebSocket, reverse playback and
completion-based I/O require their own adapters and lifecycle contracts.

## Recording a connection

`Recording::new` accepts one connection identity and three caller-owned writers:
exact observed wire bytes, mono 8 kHz PCM16LE WAVE, and diagnostic text. Supply
empty outputs positioned at zero. The WAVE writer also implements `Seek` so
`finish` can replace its provisional sizes. Feed records in their delivered
order with `record(&record)`, then drop each owned record to return byte credit.
The adapter never holds a record or writes from the network worker.

The WAVE contains accepted audio only; the wire output also preserves malformed,
control and uninterpreted bytes observed by the receiver. Diagnostics describe
identity, offsets, receiver observation times and terminal/error metadata without
duplicating payloads. They are human-readable text, not a versioned interchange
protocol. Receiver observation time does not establish source capture time.

`finish` reports `RecordingEnd::Incomplete` when no terminal record was received.
A received error or truncation remains an error class in the summary even if its
accepted PCM prefix was finalized successfully. RIFF size overflow is refused
before adding the offending audio; there is no automatic format substitution.

Each output failure identifies its stage and the exact bytes accepted before
failure, including initial or replacement header prefixes. Later calls cannot
mutate a failed recording. Finalization seeks, writes the final header, and
flushes all three outputs; any failure returns an error. Dropping a recorder does
not finalize it. Successful writes and flushes do not promise filesystem `fsync`
or persistence across power loss.

Run a self-contained localhost call through the receiver and recorder:

```sh
cargo run -p phonowire-receiver --example record_incoming
```

The example verifies literal wire and PCM output in memory. Replace its caller
writers with newly created `File` values to persist those same outputs; keep
record consumption independent of the worker and handle `finish` explicitly.

## License

Apache License 2.0. Copyright Yuriy Krasilnikov. See `LICENSE` and `NOTICE`.
