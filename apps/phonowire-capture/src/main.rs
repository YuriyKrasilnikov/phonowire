//! Bounded generic `AudioSocket` capture application.
use phonowire_receiver::{
    ByteBudget, ConnectionId, Limits, Receiver, Record, RecordKind, Recording, RecordingEnd,
    RecordingError, RecordingFailure, RecordingStage, RecordingSummary, RunSummary, StopHandle,
};
use signal_hook::{
    consts::signal::{SIGINT, SIGTERM},
    iterator::Signals,
};
use std::{
    collections::BTreeMap,
    env,
    fs::{self, File, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
    net::SocketAddr,
    num::{NonZeroU16, NonZeroUsize},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
const HEADER: u64 = 44;

struct LimitedFile {
    file: File,
    cap: u64,
}
impl LimitedFile {
    fn create(path: impl AsRef<Path>, cap: u64) -> io::Result<Self> {
        Ok(Self {
            file: OpenOptions::new().write(true).create_new(true).open(path)?,
            cap,
        })
    }
}
impl Write for LimitedFile {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let p = self.file.stream_position()?;
        let n = u64::try_from(b.len()).map_err(|_| io::Error::other("length overflow"))?;
        if p.checked_add(n).is_none_or(|e| e > self.cap) {
            return Err(io::Error::other("configured file limit exceeded"));
        }
        self.file.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
impl Seek for LimitedFile {
    fn seek(&mut self, p: SeekFrom) -> io::Result<u64> {
        self.file.seek(p)
    }
}

#[derive(Clone, Copy)]
enum Cause {
    Signal,
    Local,
    Admission,
}
impl Cause {
    const fn text(self) -> &'static str {
        match self {
            Self::Signal => "signal",
            Self::Local => "local_failure",
            Self::Admission => "admission_exhausted",
        }
    }
}
#[derive(Default)]
struct StopInfo {
    cause: Option<Cause>,
    wake: Option<String>,
    signals: u64,
}
struct Control {
    stop: Arc<StopHandle>,
    won: AtomicBool,
    info: Mutex<StopInfo>,
}
fn claim_stop(won: &AtomicBool, info: &Mutex<StopInfo>, cause: Cause) -> bool {
    if won.swap(true, Ordering::AcqRel) {
        return false;
    }
    info.lock().expect("stop mutex").cause = Some(cause);
    true
}
const fn stop_wake_failed(info: &StopInfo) -> bool {
    info.wake.is_some()
}
impl Control {
    fn request(&self, c: Cause) {
        if !claim_stop(&self.won, &self.info, c) {
            return;
        }
        let mut i = self.info.lock().expect("stop mutex");
        if let Err(e) = self.stop.stop() {
            i.wake = Some(e.kind().to_string());
        }
    }
    fn signal(&self) {
        let mut i = self.info.lock().expect("stop mutex");
        i.signals = i.signals.saturating_add(1);
        drop(i);
        self.request(Cause::Signal);
    }
}

#[derive(Debug)]
struct Receipt {
    stage: &'static str,
    class: &'static str,
    summary: RecordingSummary,
    dropped: u64,
    bytes: u64,
}
impl Receipt {
    const fn from_error(e: &RecordingError) -> Self {
        Self {
            stage: stage(e.stage()),
            class: match e.failure() {
                RecordingFailure::Io(_) => "io",
                RecordingFailure::Invalid(_) => "invalid",
            },
            summary: *e.summary(),
            dropped: 0,
            bytes: 0,
        }
    }
}
enum Entry {
    Active {
        recorder: Recording<LimitedFile, LimitedFile, LimitedFile>,
        dir: PathBuf,
    },
    Failed {
        receipt: Receipt,
        dir: PathBuf,
    },
}
#[derive(Default)]
struct Counts {
    admission: u64,
    stopped: u64,
    unowned: u64,
    unowned_bytes: u64,
    failed: u64,
    failed_bytes: u64,
    metadata: u64,
}

struct Config {
    output: PathBuf,
    listen: SocketAddr,
    connections: usize,
    queue: usize,
    payload: u16,
    turns: usize,
    retained: usize,
    active: usize,
    run: u64,
    file: u64,
    summary: u64,
    status: u64,
}
impl Config {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        if arguments.len() == 2 && arguments[1] == "--help" {
            return Err(help().to_string());
        }
        if arguments.iter().any(|argument| argument == "--help") {
            return Err("--help cannot be combined with other options".into());
        }
        let known = [
            "--output",
            "--listen",
            "--max-connections",
            "--queue-records",
            "--payload-bytes",
            "--turn-steps",
            "--retained-bytes",
            "--max-active-recordings",
            "--max-recordings-per-run",
            "--max-file-bytes",
            "--max-recording-summary-bytes",
            "--max-run-status-bytes",
        ];
        let mut values = BTreeMap::new();
        let mut index = 1;
        while index < arguments.len() {
            let option = &arguments[index];
            if !known.contains(&option.as_str()) {
                return Err(format!("unknown option: {option}"));
            }
            let Some(value) = arguments.get(index + 1) else {
                return Err(format!("missing value for {option}"));
            };
            if value.starts_with("--") {
                return Err(format!("missing value for {option}"));
            }
            if values.insert(option.as_str(), value.as_str()).is_some() {
                return Err(format!("duplicate option: {option}"));
            }
            index += 2;
        }
        let get = |name: &str, default: &str| -> Result<u64, String> {
            values
                .get(name)
                .copied()
                .unwrap_or(default)
                .parse()
                .map_err(|_| format!("invalid {name}"))
        };
        let usize_value = |name, default| {
            usize::try_from(get(name, default)?).map_err(|_| format!("invalid {name}"))
        };
        let c = Self {
            output: PathBuf::from(*values.get("--output").ok_or("--output is required")?),
            listen: values
                .get("--listen")
                .copied()
                .unwrap_or("127.0.0.1:9092")
                .parse()
                .map_err(|_| "invalid --listen")?,
            connections: usize_value("--max-connections", "16")?,
            queue: usize_value("--queue-records", "128")?,
            payload: u16::try_from(get("--payload-bytes", "4096")?)
                .map_err(|_| "invalid --payload-bytes")?,
            turns: usize_value("--turn-steps", "64")?,
            retained: usize_value("--retained-bytes", "1048576")?,
            active: usize_value("--max-active-recordings", "16")?,
            run: get("--max-recordings-per-run", "64")?,
            file: get("--max-file-bytes", "67108864")?,
            summary: get("--max-recording-summary-bytes", "4096")?,
            status: get("--max-run-status-bytes", "4096")?,
        };
        c.validate()?;
        Ok(c)
    }
    fn validate(&self) -> Result<(), String> {
        if !self.output.is_dir() {
            return Err("--output must be an existing directory".into());
        }
        if self.file < HEADER || self.summary == 0 || self.status == 0 {
            return Err("infeasible output limits".into());
        }
        if self.active == 0 || self.active > self.connections {
            return Err(
                "max active recordings must be positive and no greater than max connections".into(),
            );
        }
        if self.connections == 0
            || self.queue == 0
            || self.payload == 0
            || self.turns == 0
            || self.retained == 0
            || self.run == 0
        {
            return Err("receiver and recording limits must be positive".into());
        }
        checked_output_bound(self.run, self.file, self.summary, self.status)?;
        Ok(())
    }
}
fn checked_output_bound(
    recordings: u64,
    file_bytes: u64,
    summary_bytes: u64,
    status_bytes: u64,
) -> Result<u64, String> {
    let per_recording = file_bytes
        .checked_mul(3)
        .and_then(|value| value.checked_add(summary_bytes))
        .ok_or("output limit overflow")?;
    recordings
        .checked_mul(per_recording)
        .and_then(|value| value.checked_add(status_bytes))
        .ok_or_else(|| "output limit overflow".to_string())
}
const fn help() -> &'static str {
    "Usage: phonowire-capture --output DIRECTORY [options]\nOptions: --listen ADDR --max-connections N --queue-records N --payload-bytes N\n--turn-steps N --retained-bytes N --max-active-recordings N --max-recordings-per-run N\n--max-file-bytes N --max-recording-summary-bytes N --max-run-status-bytes N --help"
}
const fn stage(s: RecordingStage) -> &'static str {
    match s {
        RecordingStage::Validate => "validate",
        RecordingStage::Header => "header",
        RecordingStage::Wire => "wire",
        RecordingStage::Audio => "audio",
        RecordingStage::Events => "events",
        RecordingStage::Seek => "seek",
        RecordingStage::Patch => "patch",
        RecordingStage::FlushWire => "flush_wire",
        RecordingStage::FlushWave => "flush_wave",
        RecordingStage::FlushEvents => "flush_events",
    }
}
fn bytes(r: &Record) -> u64 {
    match &r.kind {
        RecordKind::Wire { bytes } | RecordKind::Audio { bytes, .. } => {
            u64::try_from(bytes.as_slice().len()).unwrap_or(u64::MAX)
        }
        _ => 0,
    }
}
fn add(n: &mut u64, b: &mut u64, r: &Record) {
    *n = n.saturating_add(1);
    *b = b.saturating_add(bytes(r));
}
fn json_summary(id: ConnectionId, o: &str, s: RecordingSummary, r: Option<&Receipt>) -> String {
    let (st, cl, dropped, bytes) = r.map_or(("null", "null", 0, 0), |x| {
        (x.stage, x.class, x.dropped, x.bytes)
    });
    format!(
        "{{\"connection\":{{\"instance\":{},\"sequence\":{}}},\"outcome\":\"{}\",\"recording\":{{\"wire_bytes\":{},\"audio_bytes\":{},\"event_bytes\":{},\"header_bytes\":{},\"patch_bytes\":{},\"end\":\"{:?}\"}},\"failure\":{{\"stage\":\"{}\",\"class\":\"{}\",\"dropped_records\":{},\"dropped_bytes\":{}}}}}",
        id.instance(),
        id.sequence(),
        o,
        s.wire_bytes,
        s.audio_bytes,
        s.event_bytes,
        s.header_bytes,
        s.patch_bytes,
        s.end,
        st,
        cl,
        dropped,
        bytes
    )
}
fn write_json(p: &Path, cap: u64, s: &str) -> io::Result<()> {
    let mut f = LimitedFile::create(p, cap)?;
    f.write_all(s.as_bytes())?;
    f.flush()
}
fn summary(
    dir: &Path,
    cap: u64,
    id: ConnectionId,
    o: &str,
    s: RecordingSummary,
    r: Option<&Receipt>,
) -> io::Result<()> {
    write_json(&dir.join("summary.json"), cap, &json_summary(id, o, s, r))
}
const fn successful_terminal(end: RecordingEnd) -> bool {
    matches!(
        end,
        RecordingEnd::Observed(
            phonowire_receiver::TerminalKind::CleanEof
                | phonowire_receiver::TerminalKind::Terminate
        )
    )
}
enum CreateError {
    Io,
    Recording(RecordingError),
}
fn create(
    dir: &Path,
    id: ConnectionId,
    c: u64,
) -> Result<Recording<LimitedFile, LimitedFile, LimitedFile>, CreateError> {
    let wire = LimitedFile::create(dir.join("wire.bin"), c).map_err(|_| CreateError::Io)?;
    let wave = LimitedFile::create(dir.join("audio.wav"), c).map_err(|_| CreateError::Io)?;
    let events = LimitedFile::create(dir.join("events.log"), c).map_err(|_| CreateError::Io)?;
    Recording::new(id, wire, wave, events).map_err(CreateError::Recording)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a: Vec<String> = env::args().collect();
    match Config::parse(&a) {
        Ok(c) => run(&c).map_err(Into::into),
        Err(e) if e == help() => {
            println!("{e}");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
#[allow(clippy::too_many_lines)] // Owns the ordered drain/finalize/join lifecycle.
fn run(c: &Config) -> Result<(), String> {
    let limits = Limits::new(
        NonZeroUsize::new(c.connections).ok_or("max connections is zero")?,
        NonZeroUsize::new(c.queue).ok_or("queue is zero")?,
        NonZeroU16::new(c.payload).ok_or("payload is zero")?,
        NonZeroUsize::new(c.turns).ok_or("turn steps is zero")?,
    )
    .map_err(|e| e.to_string())?;
    let budget = ByteBudget::new(NonZeroUsize::new(c.retained).ok_or("retained bytes is zero")?);
    let (receiver, records, stop) =
        Receiver::bind(c.listen, limits, budget.clone()).map_err(|e| e.to_string())?;
    let local_address = receiver.local_addr();
    let control = Arc::new(Control {
        stop: Arc::new(stop),
        won: AtomicBool::new(false),
        info: Mutex::new(StopInfo::default()),
    });
    let mut signals = Signals::new([SIGINT, SIGTERM]).map_err(|e| e.to_string())?;
    let handle = signals.handle();
    let monitor = Arc::clone(&control);
    let signal_thread = thread::spawn(move || {
        for _ in signals.forever() {
            monitor.signal();
        }
    });
    let worker = thread::spawn(move || receiver.run());
    // This is process readiness: signal delivery and both owned threads exist.
    eprintln!("listening on {local_address}");
    let (mut map, mut reserved, mut counts, mut bad) =
        (BTreeMap::new(), 0_u64, Counts::default(), false);
    while let Ok(record) = records.recv() {
        let id = record.connection;
        if matches!(record.kind, RecordKind::Connected { .. }) {
            if control.won.load(Ordering::Acquire) {
                counts.stopped = counts.stopped.saturating_add(1);
                continue;
            }
            if reserved == c.run || map.len() >= c.active {
                counts.admission = counts.admission.saturating_add(1);
                bad = true;
                control.request(if reserved == c.run {
                    Cause::Admission
                } else {
                    Cause::Local
                });
                continue;
            }
            reserved = reserved.saturating_add(1);
            let dir = c
                .output
                .join(format!("connection-{}-{}", id.instance(), id.sequence()));
            if fs::create_dir(&dir).is_err() {
                counts.admission = counts.admission.saturating_add(1);
                bad = true;
                control.request(Cause::Local);
            } else {
                match create(&dir, id, c.file) {
                    Ok(mut recorder) => match recorder.record(&record) {
                        Ok(()) => {
                            map.insert(id, Entry::Active { recorder, dir });
                        }
                        Err(error) => {
                            let receipt = Receipt::from_error(&error);
                            map.insert(id, Entry::Failed { receipt, dir });
                            bad = true;
                            control.request(Cause::Local);
                        }
                    },
                    Err(error) => {
                        let receipt = match error {
                            CreateError::Io => Receipt {
                                stage: "create",
                                class: "io",
                                summary: RecordingSummary::default(),
                                dropped: 0,
                                bytes: 0,
                            },
                            CreateError::Recording(error) => Receipt::from_error(&error),
                        };
                        map.insert(id, Entry::Failed { receipt, dir });
                        bad = true;
                        control.request(Cause::Local);
                    }
                }
            }
            continue;
        }
        match map.get_mut(&id) {
            Some(Entry::Active { recorder, dir }) => {
                let end = matches!(record.kind, RecordKind::Ended { .. });
                if let Err(e) = recorder.record(&record) {
                    let r = Receipt::from_error(&e);
                    *map.get_mut(&id).expect("entry") = Entry::Failed {
                        receipt: r,
                        dir: dir.clone(),
                    };
                    bad = true;
                    control.request(Cause::Local);
                } else if end {
                    let Some(Entry::Active { recorder, dir }) = map.remove(&id) else {
                        unreachable!()
                    };
                    match recorder.finish() {
                        Ok(s) => {
                            let outcome = if successful_terminal(s.end) {
                                "observed"
                            } else {
                                "failed"
                            };
                            if !successful_terminal(s.end) {
                                bad = true;
                            }
                            if summary(&dir, c.summary, id, outcome, s, None).is_err() {
                                counts.metadata = counts.metadata.saturating_add(1);
                                bad = true;
                                control.request(Cause::Local);
                            }
                        }
                        Err(e) => {
                            let r = Receipt::from_error(&e);
                            map.insert(id, Entry::Failed { receipt: r, dir });
                            bad = true;
                            control.request(Cause::Local);
                        }
                    }
                }
            }
            Some(Entry::Failed { receipt, .. }) => {
                add(&mut receipt.dropped, &mut receipt.bytes, &record);
                add(&mut counts.failed, &mut counts.failed_bytes, &record);
            }
            None => add(&mut counts.unowned, &mut counts.unowned_bytes, &record),
        }
    }
    for (id, e) in map {
        match e {
            Entry::Active { recorder, dir } => match recorder.finish() {
                Ok(s) => {
                    // A recorder without a terminal observation is an explicitly
                    // incomplete capture, never a successful run.
                    bad = true;
                    if summary(&dir, c.summary, id, "incomplete", s, None).is_err() {
                        counts.metadata = counts.metadata.saturating_add(1);
                        bad = true;
                    }
                }
                Err(x) => {
                    let r = Receipt::from_error(&x);
                    if summary(&dir, c.summary, id, "failed", r.summary, Some(&r)).is_err() {
                        counts.metadata = counts.metadata.saturating_add(1);
                    }
                    bad = true;
                }
            },
            Entry::Failed { receipt, dir } => {
                if summary(
                    &dir,
                    c.summary,
                    id,
                    "failed",
                    receipt.summary,
                    Some(&receipt),
                )
                .is_err()
                {
                    counts.metadata = counts.metadata.saturating_add(1);
                }
                bad = true;
            }
        }
    }
    handle.close();
    if signal_thread.join().is_err() {
        bad = true;
    }
    let worker = match worker.join() {
        Ok(Ok(s)) => Some(s),
        Ok(Err(e)) => {
            let summary = *e.summary();
            eprintln!("receiver failed: {e}");
            bad = true;
            Some(summary)
        }
        Err(_) => {
            bad = true;
            None
        }
    };
    let final_budget_used = budget.used();
    if final_budget_used != 0
        || counts.unowned != 0
        || counts.metadata != 0
        || worker.as_ref().is_some_and(|summary| {
            summary.abandoned != 0
                || summary.undelivered_raw_bytes != 0
                || summary.undelivered_records != 0
                || !summary.counters_complete
        })
    {
        bad = true;
    }
    let i = control.info.lock().expect("stop mutex");
    if stop_wake_failed(&i) {
        bad = true;
    }
    let status = format!(
        "{{\"reserved_recordings\":{},\"admission_refused\":{},\"stopped_refused\":{},\"unowned_records\":{},\"unowned_bytes\":{},\"failed_records\":{},\"failed_bytes\":{},\"metadata_failures\":{},\"final_budget_used\":{},\"first_stop_cause\":{},\"stop_wake_error\":{},\"signals\":{},\"receiver\":{}}}",
        reserved,
        counts.admission,
        counts.stopped,
        counts.unowned,
        counts.unowned_bytes,
        counts.failed,
        counts.failed_bytes,
        counts.metadata,
        final_budget_used,
        i.cause
            .map_or_else(|| "null".to_string(), |x| format!("\"{}\"", x.text())),
        i.wake
            .as_ref()
            .map_or_else(|| "null".to_string(), |x| format!("\"{x}\"")),
        i.signals,
        receiver_json(worker)
    );
    drop(i);
    if write_json(&c.output.join("run-status.json"), c.status, &status).is_err() {
        bad = true;
    }
    if bad {
        Err("capture completed with failures; see run-status.json and recording summaries".into())
    } else {
        Ok(())
    }
}
fn receiver_json(s: Option<RunSummary>) -> String {
    s.map_or_else(|| "null".into(), |x| format!(
        "{{\"accepted\":{},\"refused\":{},\"ended\":{},\"abandoned\":{},\"live\":{},\"raw_bytes_read\":{},\"raw_bytes_handed_off\":{},\"undelivered_raw_bytes\":{},\"records_handed_off\":{},\"undelivered_records\":{},\"retained_output_bytes\":{},\"fixed_buffer_capacity\":{},\"stopped_by\":\"{:?}\",\"counters_complete\":{}}}",
        x.accepted, x.refused, x.ended, x.abandoned, x.live, x.raw_bytes_read,
        x.raw_bytes_handed_off, x.undelivered_raw_bytes, x.records_handed_off,
        x.undelivered_records, x.retained_output_bytes, x.fixed_buffer_capacity,
        x.stopped_by, x.counters_complete))
}
#[cfg(test)]
mod tests {
    use super::*;
    use phonowire_receiver::{RecordingEnd, TerminalKind};
    use std::{
        io::Cursor,
        net::{Ipv4Addr, TcpListener, TcpStream},
        sync::{Arc, mpsc::RecvTimeoutError},
        time::Duration,
    };

    #[test]
    fn only_clean_terminal_observations_are_successful() {
        assert!(successful_terminal(RecordingEnd::Observed(
            TerminalKind::CleanEof
        )));
        assert!(successful_terminal(RecordingEnd::Observed(
            TerminalKind::Terminate
        )));
        assert!(!successful_terminal(RecordingEnd::Observed(
            TerminalKind::Truncated
        )));
        assert!(!successful_terminal(RecordingEnd::Observed(
            TerminalKind::PeerError
        )));
        assert!(!successful_terminal(RecordingEnd::Observed(
            TerminalKind::Transport
        )));
        assert!(!successful_terminal(RecordingEnd::Incomplete));
    }

    const UUID: [u8; 19] = [
        1, 0, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
    ];
    const PCM: [u8; 5] = [0x10, 0, 2, 0x34, 0x12];
    const TERM: [u8; 3] = [0, 0, 0];

    fn actual_records() -> Vec<Record> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("test listener");
        let address = listener.local_addr().expect("test address");
        drop(listener);
        let limits = Limits::new(
            NonZeroUsize::new(1).expect("one connection"),
            NonZeroUsize::new(16).expect("queue"),
            NonZeroU16::new(4096).expect("payload"),
            NonZeroUsize::new(64).expect("turns"),
        )
        .expect("valid limits");
        let (receiver, records, stop) = Receiver::bind(
            address,
            limits,
            ByteBudget::new(NonZeroUsize::new(4096).expect("budget")),
        )
        .expect("bind receiver");
        let worker = thread::spawn(move || receiver.run());
        let mut peer = TcpStream::connect(address).expect("connect receiver");
        peer.write_all(&[UUID.as_slice(), PCM.as_slice(), TERM.as_slice()].concat())
            .expect("literal frames");
        drop(peer);
        let mut observed = Vec::new();
        loop {
            match records.recv_timeout(Duration::from_secs(5)) {
                Ok(record) => {
                    let ended = matches!(record.kind, RecordKind::Ended { .. });
                    observed.push(record);
                    if ended {
                        break;
                    }
                }
                Err(RecvTimeoutError::Timeout) => panic!("receiver terminal observation"),
                Err(RecvTimeoutError::Disconnected) => panic!("receiver disconnected early"),
            }
        }
        stop.stop().expect("stop receiver");
        drop(records);
        worker
            .join()
            .expect("receiver worker join")
            .expect("receiver worker result");
        observed
    }

    fn two_active_record_sets() -> BTreeMap<ConnectionId, Vec<Record>> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("test listener");
        let address = listener.local_addr().expect("test address");
        drop(listener);
        let limits = Limits::new(
            NonZeroUsize::new(2).expect("two connections"),
            NonZeroUsize::new(32).expect("queue"),
            NonZeroU16::new(4096).expect("payload"),
            NonZeroUsize::new(64).expect("turns"),
        )
        .expect("valid limits");
        let (receiver, records, stop) = Receiver::bind(
            address,
            limits,
            ByteBudget::new(NonZeroUsize::new(4096).expect("budget")),
        )
        .expect("bind receiver");
        let worker = thread::spawn(move || receiver.run());
        let mut first = TcpStream::connect(address).expect("first peer");
        let mut second = TcpStream::connect(address).expect("second peer");
        first.write_all(&UUID).expect("first UUID");
        second.write_all(&UUID).expect("second UUID");
        let mut result = BTreeMap::<ConnectionId, Vec<Record>>::new();
        while result
            .values()
            .filter(|set| {
                set.iter()
                    .any(|record| matches!(record.kind, RecordKind::Started { .. }))
            })
            .count()
            < 2
        {
            let record = records
                .recv_timeout(Duration::from_secs(5))
                .expect("two active record sets");
            result.entry(record.connection).or_default().push(record);
        }
        stop.stop().expect("stop receiver");
        drop((first, second, records));
        worker
            .join()
            .expect("receiver worker join")
            .expect("receiver worker result");
        assert_eq!(result.len(), 2, "two independently admitted identities");
        result
    }

    #[derive(Clone, Default)]
    struct FlushRefusal {
        bytes: Arc<Mutex<Vec<u8>>>,
    }
    impl FlushRefusal {
        fn bytes(&self) -> Vec<u8> {
            self.bytes.lock().expect("writer bytes").clone()
        }
    }
    impl Write for FlushRefusal {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes
                .lock()
                .expect("writer bytes")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("controlled flush refusal"))
        }
    }
    #[test]
    fn replacement_is_not_extension() {
        let p = env::temp_dir().join(format!("cap-{}", std::process::id()));
        let _ = fs::remove_file(&p);
        let mut f = LimitedFile::create(&p, HEADER).unwrap();
        f.write_all(&[0; 44]).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&[1; 44]).unwrap();
        f.seek(SeekFrom::End(0)).unwrap();
        assert!(f.write_all(&[2]).is_err());
        drop(f);
        fs::remove_file(p).unwrap();
    }

    #[test]
    fn recorded_stop_wake_failure_is_unsuccessful() {
        let info = StopInfo {
            cause: Some(Cause::Signal),
            wake: Some("other".to_string()),
            signals: 1,
        };
        assert!(stop_wake_failed(&info));
    }

    #[test]
    fn controller_claim_is_exactly_once_and_immutable_in_both_orders() {
        for (first, second) in [(Cause::Signal, Cause::Local), (Cause::Local, Cause::Signal)] {
            let won = AtomicBool::new(false);
            let info = Mutex::new(StopInfo::default());
            assert!(claim_stop(&won, &info, first));
            assert!(!claim_stop(&won, &info, second));
            assert!(won.load(Ordering::Acquire));
            assert_eq!(
                info.lock().expect("info").cause.map(Cause::text),
                Some(first.text())
            );
        }
    }

    #[test]
    fn file_cap_44_allows_actual_empty_wave_and_refuses_pcm_prefix() {
        let path = env::temp_dir().join(format!("cap-wave-{}", std::process::id()));
        let _ = fs::remove_file(&path);
        let records = actual_records();
        let connected = records
            .iter()
            .find(|record| matches!(record.kind, RecordKind::Connected { .. }))
            .expect("public connected record");
        let mut empty = Recording::new(
            connected.connection,
            Cursor::new(Vec::new()),
            LimitedFile::create(&path, HEADER).expect("limited wave"),
            Cursor::new(Vec::new()),
        )
        .expect("actual header");
        empty.record(connected).expect("connected event");
        let summary = empty.finish().expect("empty wave finalization");
        assert_eq!(summary.audio_bytes, 0);
        assert_eq!(
            fs::metadata(&path).expect("empty wave metadata").len(),
            HEADER
        );
        fs::remove_file(&path).expect("remove empty wave");

        let mut recording = Recording::new(
            connected.connection,
            Cursor::new(Vec::new()),
            LimitedFile::create(&path, HEADER).expect("limited wave"),
            Cursor::new(Vec::new()),
        )
        .expect("actual header");
        for record in &records {
            if matches!(record.kind, RecordKind::Audio { .. }) {
                let error = recording.record(record).expect_err("PCM exceeds C=44");
                assert_eq!(error.stage(), RecordingStage::Audio);
                assert_eq!(error.summary().audio_bytes, 0, "no PCM prefix accepted");
                break;
            }
            recording.record(record).expect("actual prefix record");
        }
        fs::remove_file(path).expect("remove");
    }

    #[test]
    fn aggregate_counts_status_once_and_refuses_one_unit_over_limit() {
        let expected = u128::from(2_u64) * (u128::from(3_u64) * 44 + 7) + 11;
        assert_eq!(
            checked_output_bound(2, 44, 7, 11).expect("small bound"),
            u64::try_from(expected).expect("fits")
        );
        let largest_file = (u64::MAX - 2) / 3;
        assert_eq!(
            checked_output_bound(1, largest_file, 1, 1).expect("largest accepted file cap"),
            u64::MAX - 1
        );
        assert!(checked_output_bound(1, largest_file + 1, 1, 1).is_err());
    }

    #[test]
    fn controlled_writer_refusal_retains_prefix_and_finalizes_active_sibling() {
        let mut sets = two_active_record_sets().into_values();
        let failed_records = sets.next().expect("failed identity");
        let sibling_records = sets.next().expect("sibling identity");
        let failed_id = failed_records[0].connection;
        let sibling_id = sibling_records[0].connection;
        assert_ne!(failed_id, sibling_id);
        let expected_prefix: Vec<u8> = failed_records
            .iter()
            .filter_map(|record| match &record.kind {
                RecordKind::Wire { bytes } => Some(bytes.as_slice()),
                _ => None,
            })
            .flatten()
            .copied()
            .collect();
        assert!(!expected_prefix.is_empty(), "actual accepted wire prefix");
        let writer = FlushRefusal::default();
        let mut failed = Recording::new(
            failed_id,
            writer.clone(),
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
        )
        .expect("failed recorder header");
        let mut sibling = Recording::new(
            sibling_id,
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
        )
        .expect("sibling recorder header");
        for record in &failed_records {
            failed
                .record(record)
                .expect("failed recorder active prefix");
        }
        for record in &sibling_records {
            sibling.record(record).expect("sibling remains active");
        }
        let error = failed.finish().expect_err("controlled writer refusal");
        assert_eq!(error.stage(), RecordingStage::FlushWire);
        assert_eq!(
            usize::try_from(error.summary().wire_bytes).expect("small writer prefix"),
            expected_prefix.len()
        );
        assert_eq!(
            writer.bytes(),
            expected_prefix,
            "exact accepted writer prefix"
        );
        let sibling_summary = sibling.finish().expect("sibling finalization");
        assert_eq!(sibling_summary.end, RecordingEnd::Incomplete);
        assert_eq!(sibling_summary.wire_bytes, 19);
    }
}
