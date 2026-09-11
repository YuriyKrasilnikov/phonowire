# Phonowire receiver

A bounded incoming AudioSocket receiver for Linux. One caller-owned worker
handles nonblocking TCP connections and sends owned observations to a consumer.
The portable `phonowire-audiosocket` codec supplies framing and incoming session
policy; Mio supplies readiness notifications.

`Receiver::bind` accepts one UUID before mono PCM16LE at any of the nine
documented wire rates or supported DTMF. `Receiver::bind_with_profile` lets a
caller select a narrower accepted rate set. This describes the receiver's
policy. Compatibility with a particular peer also depends on its version,
operating mode and codec configuration.
Laboratory calls from Asterisk 23.2.0's AudioSocket application on answered
Local/n channels preserved two independent incoming 8 kHz streams, including
controlled fragmentation and natural TCP EOF. This result does not cover a
customer SIP/RTP path, DTMF interoperability, reverse playback or load capacity.

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

Queue and byte allocation requests use separate FIFO admission orders. A new
receiver request joins behind existing requests, including when its connection
is already runnable. Only the oldest request may try the resource; success or
cancellation gives the next request a turn. Byte requests wait for their full
size, so a large request can delay smaller requests behind it. This prevents
younger receiver requests from repeatedly consuming its released capacity.

Progress requires continued worker execution, queue consumption, and eventual
release of retained bytes. Public `ByteBudget::try_copy` calls remain independent:
they can use available capacity while a receiver request waits. Fairness among
receiver requests does not guarantee progress against an external caller that
continually takes that capacity. No bytes are reserved merely by waiting, and
`ByteBudget::used` continues to report retained output capacity.

Known recoverable errors from an individual `accept` attempt preserve admitted
connections. Each turn limits attempts; descriptor/memory pressure or a turn
containing only retryable errors delays further accepts for 100 ms. Pending
listener readiness is retained, so retry does not require a new network edge.
Existing connections and stop control continue during that delay. Other socket,
registration, readiness and wake failures can still end the worker explicitly.

`Receiver::run` executes synchronously on the calling thread. Move the receiver
into a `std::thread` when reception and consumption need separate threads.
`StopHandle` supplies an independent stop path. A stop cancels pending connection
work; it does not promise that every received byte reached the consumer. Queued
records remain available after the worker stops.

## Application lifecycle

1. Create validated `Limits` and a shared `ByteBudget`, then call `Receiver::bind`,
   or `Receiver::bind_with_profile` for an explicit rate restriction.
2. Move the receiver to a caller-owned worker thread and run `Receiver::run`.
   Consume `Records` concurrently so a full queue can make progress.
3. Route records by `ConnectionId`. Give each connection its own `Recording`
   and output writers, starting with its `Connected` record. Handle each record
   in order and drop it when its payload is no longer needed.
4. After a terminal record, call `Recording::finish` and check both writer
   errors and the returned terminal meaning. Finishing the files does not
   turn a failed or truncated call into a successful call.
5. On normal application completion or a consumer failure, request stop and
   join the worker. Check its result and cancellation accounting. Queued records
   remain available; consume or discard them explicitly and release any retained
   payloads. Finish any remaining recorder with its incomplete outcome if no
   terminal record reached it.

Stopping before terminal records arrive cancels pending work. Applications that
need complete calls must consume their terminal records before requesting stop.

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

`Recording::new` is the explicit 8 kHz compatibility constructor. Use
`Recording::with_sample_rate` to select the declared source rate for its mono
PCM16LE WAVE output. Each accepted audio record must carry that exact rate; a
mismatched rate is refused before its PCM bytes are appended. Both constructors
accept one connection identity and three caller-owned writers: exact observed
wire bytes, WAVE audio, and diagnostic text. Supply empty outputs positioned at
zero. The WAVE writer also implements `Seek` so
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
not finalize it or explicitly flush its outputs. Destructors of owned writers
may perform their own I/O, including after failed `finish`; pass writers by
`&mut` to keep control of their lifetimes. Successful writes and flushes do not
promise filesystem `fsync` or persistence across power loss.

Run a self-contained localhost call through the receiver and recorder:

```sh
cargo run -p phonowire-receiver --example record_incoming
```

The example verifies literal wire and PCM output in memory. Replace its caller
writers with newly created `File` values to persist those same outputs; keep
record consumption independent of the worker and handle `finish` explicitly.

## License

Apache License 2.0. Copyright Yuriy Krasilnikov. See `LICENSE` and `NOTICE`.
