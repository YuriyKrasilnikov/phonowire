//! Localhost receiver-to-file observations with literal expected bytes.
use std::fs::{self, File};
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::num::{NonZeroU16, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use phonowire_receiver::{
    ByteBudget, Limits, Receiver, RecordKind, Recording, RecordingEnd, TerminalKind,
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
const WAIT: Duration = Duration::from_secs(3);
const UUID: [u8; 19] = [
    1, 0, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
];
const TERMINATED: [u8; 8] = [0x10, 0, 2, 0x34, 0x12, 0, 0, 0];
const TRUNCATED: [u8; 5] = [0x10, 0, 8, 0x34, 0x12];

#[derive(Clone, Copy)]
enum Ending {
    Terminate,
    Truncated,
}
impl Ending {
    const fn bytes(self) -> &'static [u8] {
        match self {
            Self::Terminate => &TERMINATED,
            Self::Truncated => &TRUNCATED,
        }
    }
    const fn terminal(self) -> TerminalKind {
        match self {
            Self::Terminate => TerminalKind::Terminate,
            Self::Truncated => TerminalKind::Truncated,
        }
    }
    const fn pcm(self) -> &'static [u8] {
        match self {
            Self::Terminate => &[0x34, 0x12],
            Self::Truncated => &[],
        }
    }
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "phonowire-recording-pipeline-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create exclusively owned test directory");
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("remove exclusively owned test directory");
    }
}

fn file(path: &Path) -> File {
    File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .expect("new output")
}
fn limits() -> Limits {
    Limits::new(
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        NonZeroU16::new(64).expect("payload"),
        NonZeroUsize::MIN,
    )
    .expect("limits")
}

fn run(ending: Ending) {
    let directory = Directory::new();
    let wire_path = directory.0.join("wire.bin");
    let wave_path = directory.0.join("audio.wav");
    let events_path = directory.0.join("events.txt");
    let budget = ByteBudget::new(NonZeroUsize::new(8192).expect("budget"));
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(),
        budget.clone(),
    )
    .expect("bind");
    let local = receiver.local_addr();
    let worker = thread::spawn(move || receiver.run());
    let (release, released) = mpsc::sync_channel(1);
    let sender = thread::spawn(move || {
        let mut peer = TcpStream::connect(local).expect("connect");
        peer.write_all(&UUID[..1]).expect("partial header");
        released
            .recv_timeout(WAIT)
            .expect("first read persisted before continuing");
        peer.write_all(&UUID[1..]).expect("remaining UUID");
        peer.write_all(ending.bytes()).expect("audio and ending");
    });
    let connected = records.recv_timeout(WAIT).expect("connection");
    let mut recording = Recording::new(
        connected.connection,
        file(&wire_path),
        file(&wave_path),
        file(&events_path),
    )
    .expect("recorder");
    recording.record(&connected).expect("connected output");
    let first = records.recv_timeout(WAIT).expect("first observed read");
    let RecordKind::Wire { bytes } = &first.kind else {
        panic!("wire precedes interpretation");
    };
    assert_eq!(bytes.as_slice(), &[1]);
    recording.record(&first).expect("first byte persisted");
    drop(first);
    release.send(()).expect("release remaining bytes");
    loop {
        let record = records.recv_timeout(WAIT).expect("receiver record");
        let terminal = matches!(&record.kind, RecordKind::Ended { .. });
        recording.record(&record).expect("record output");
        if terminal {
            break;
        }
    }
    sender.join().expect("sender completes");
    stop.stop().expect("stop");
    let network = worker.join().expect("worker completes").expect("run");
    let summary = recording.finish().expect("finish");
    let expected_raw: Vec<u8> = UUID.iter().chain(ending.bytes()).copied().collect();
    let raw = fs::read(&wire_path).expect("wire output");
    let wav = fs::read(&wave_path).expect("wave output");
    let diagnostics = fs::read_to_string(&events_path).expect("diagnostic text");
    assert_eq!(raw, expected_raw);
    assert_eq!(
        summary.wire_bytes,
        u64::try_from(expected_raw.len()).expect("small stream")
    );
    assert_eq!(summary.end, RecordingEnd::Observed(ending.terminal()));
    assert_eq!(
        summary.audio_bytes,
        u64::try_from(ending.pcm().len()).expect("small PCM")
    );
    assert_eq!(&wav[44..], ending.pcm());
    assert_eq!(network.raw_bytes_read, summary.wire_bytes);
    assert_eq!(network.raw_bytes_handed_off, summary.wire_bytes);
    assert_eq!(network.undelivered_raw_bytes, 0);
    assert_eq!(network.ended, 1);
    assert_eq!(network.abandoned, 0);
    assert!(network.counters_complete);
    assert!(diagnostics.contains("started uuid="));
    let last = diagnostics.lines().last().expect("terminal diagnostic");
    match ending {
        Ending::Terminate => assert!(last.ends_with("reason=terminate")),
        Ending::Truncated => assert!(last.contains("reason=truncated detail=")),
    }
    drop(records);
    assert_eq!(budget.used(), 0);
}

#[test]
fn fragmented_tcp_records_playable_terminated_pcm() {
    run(Ending::Terminate);
}
#[test]
fn fragmented_tcp_records_truncated_prefix_without_success() {
    run(Ending::Truncated);
}

struct CallOutput {
    recording: Recording<File, File, File>,
    path: PathBuf,
    sender: Option<u8>,
}
impl CallOutput {
    fn new(record: &phonowire_receiver::Record, directory: &Directory) -> Self {
        let path = directory.0.join(record.connection.sequence().to_string());
        fs::create_dir(&path).expect("connection output directory");
        let recording = Recording::new(
            record.connection,
            file(&path.join("wire.bin")),
            file(&path.join("audio.wav")),
            file(&path.join("events.txt")),
        )
        .expect("recording");
        Self {
            recording,
            path,
            sender: None,
        }
    }
    fn finish(self) -> u64 {
        let sender = self.sender.expect("UUID precedes terminal");
        let (wire, pcm) = concurrent_fixture(sender);
        let summary = self.recording.finish().expect("finalize call");
        assert_eq!(summary.end, RecordingEnd::Observed(TerminalKind::Terminate));
        assert_eq!(fs::read(self.path.join("wire.bin")).expect("wire"), wire);
        let wave = fs::read(self.path.join("audio.wav")).expect("wave");
        assert_eq!(&wave[44..], pcm);
        assert_eq!(&wave[40..44], &[0, 8, 0, 0]);
        assert_eq!(&wave[4..8], &[36, 8, 0, 0]);
        assert_eq!(summary.audio_bytes, 2048);
        assert_eq!(
            summary.wire_bytes,
            u64::try_from(wire.len()).expect("wire length")
        );
        let text = fs::read_to_string(self.path.join("events.txt")).expect("events");
        assert!(
            text.lines()
                .last()
                .expect("terminal")
                .ends_with("reason=terminate")
        );
        summary.wire_bytes
    }
}

fn concurrent_fixture(sender: u8) -> (Vec<u8>, Vec<u8>) {
    let mut wire = vec![1, 0, 16];
    wire.extend_from_slice(&[sender; 16]);
    let mut pcm = Vec::new();
    for sample in 0_u8..32 {
        let mut payload = [0_u8; 64];
        for pair in payload.as_chunks_mut::<2>().0 {
            pair.copy_from_slice(&[sample, sender]);
        }
        wire.extend_from_slice(&[0x10, 0, 64]);
        wire.extend_from_slice(&payload);
        pcm.extend_from_slice(&payload);
    }
    wire.extend_from_slice(&[0, 0, 0]);
    (wire, pcm)
}

fn concurrent_calls(turns: usize) {
    use std::collections::BTreeMap;
    let directory = Directory::new();
    let budget = ByteBudget::new(NonZeroUsize::new(4096).expect("budget"));
    let limits = Limits::new(
        NonZeroUsize::new(2).expect("connections"),
        NonZeroUsize::MIN,
        NonZeroU16::new(64).expect("payload"),
        NonZeroUsize::new(turns).expect("turns"),
    )
    .expect("limits");
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits,
        budget.clone(),
    )
    .expect("bind");
    let local = receiver.local_addr();
    let worker = thread::spawn(move || receiver.run());
    let mut senders = Vec::new();
    let mut start_commands = Vec::new();
    for sender in [1, 2] {
        let mut peer = TcpStream::connect(local).expect("connect");
        let (start, permission) = mpsc::sync_channel(1);
        start_commands.push(start);
        senders.push(thread::spawn(move || {
            permission.recv_timeout(WAIT).expect("both peers connected");
            peer.write_all(&concurrent_fixture(sender).0)
                .expect("whole offered stream");
        }));
    }
    for command in start_commands {
        command.send(()).expect("release connected sender");
    }
    let mut calls = BTreeMap::new();
    let mut completed = 0;
    let mut wire = 0;
    let deadline = std::time::Instant::now() + WAIT;
    while completed != 2 {
        let record = records
            .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
            .expect("concurrent receiver progress");
        if matches!(record.kind, RecordKind::Connected { .. }) {
            assert!(
                calls
                    .insert(record.connection, CallOutput::new(&record, &directory))
                    .is_none()
            );
        }
        let call = calls
            .get_mut(&record.connection)
            .expect("admitted identity");
        if let RecordKind::Started { uuid } = record.kind {
            let sender = uuid.bytes()[0];
            assert!([1, 2].contains(&sender));
            assert_eq!(uuid.bytes(), [sender; 16]);
            call.sender = Some(sender);
        }
        call.recording
            .record(&record)
            .expect("ordered connection observation");
        if matches!(record.kind, RecordKind::Ended { .. }) {
            wire += calls
                .remove(&record.connection)
                .expect("completed call")
                .finish();
            completed += 1;
        }
    }
    for sender in senders {
        sender.join().expect("sender completes");
    }
    stop.stop().expect("stop worker");
    let summary = worker
        .join()
        .expect("worker completes")
        .expect("worker result");
    assert_eq!(summary.accepted, 2);
    assert_eq!(summary.ended, 2);
    assert_eq!(summary.abandoned, 0);
    assert_eq!(summary.raw_bytes_read, wire);
    assert_eq!(summary.raw_bytes_handed_off, wire);
    assert_eq!(summary.undelivered_records, 0);
    assert_eq!(summary.undelivered_raw_bytes, 0);
    assert!(summary.counters_complete);
    drop(records);
    assert_eq!(budget.used(), 0);
}

#[test]
fn concurrent_calls_preserve_exact_files_with_one_slot_and_shared_bytes() {
    for turns in [1, 8, 128] {
        concurrent_calls(turns);
    }
}
