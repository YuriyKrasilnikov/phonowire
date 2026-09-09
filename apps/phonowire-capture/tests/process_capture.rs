//! End-to-end capture checks using literal `AudioSocket` frames.
use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

static NEXT: AtomicUsize = AtomicUsize::new(0);
const WAIT: Duration = Duration::from_secs(5);
const UUID: [u8; 19] = [
    1, 0, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
];
const TRUNCATED_UUID: [u8; 19] = [
    1, 0, 16, 17, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
];
const PCM: [u8; 5] = [0x10, 0, 2, 0x34, 0x12];
const TERM: [u8; 3] = [0, 0, 0];

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "phonowire-capture-it-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("new output root");
        Self(path)
    }
}
struct Capture {
    child: Option<Child>,
    stderr: Option<thread::JoinHandle<Vec<u8>>>,
}
impl Capture {
    fn finish(mut self) -> Output {
        let mut child = self.child.take().expect("owned child");
        let deadline = std::time::Instant::now() + WAIT;
        let status = loop {
            match child.try_wait().expect("capture status") {
                Some(status) => break status,
                None if std::time::Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(1));
                }
                None => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let stderr = self
                        .stderr
                        .take()
                        .expect("stderr drain owner")
                        .join()
                        .expect("stderr drain");
                    panic!(
                        "capture did not exit within {WAIT:?}; stderr: {}",
                        String::from_utf8_lossy(&stderr)
                    );
                }
            }
        };
        let stderr = self
            .stderr
            .take()
            .expect("stderr drain owner")
            .join()
            .expect("stderr drain");
        Output {
            status,
            stdout: Vec::new(),
            stderr,
        }
    }
    fn id(&self) -> u32 {
        self.child.as_ref().expect("live child").id()
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take()
            && child.try_wait().expect("capture status").is_none()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
        if !thread::panicking()
            && let Some(stderr) = self.stderr.take()
        {
            let _ = stderr.join();
        }
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn free_addr() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback");
    let address = listener.local_addr().expect("address");
    drop(listener);
    address
}
fn start(dir: &Temp, address: std::net::SocketAddr, extra: &[&str]) -> Capture {
    let mut command = Command::new(env!("CARGO_BIN_EXE_phonowire-capture"));
    command
        .args([
            "--output",
            dir.0.to_str().expect("utf8"),
            "--listen",
            &address.to_string(),
        ])
        .args(extra)
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("start capture");
    let stderr = child.stderr.take().expect("stderr");
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let stderr = thread::spawn(move || {
        let mut output = Vec::new();
        let mut ready = false;
        for line in BufReader::new(stderr).lines() {
            match line {
                Ok(line) => {
                    if !ready && line.starts_with("listening on ") {
                        let _ = ready_tx.send(());
                        ready = true;
                    }
                    output.extend_from_slice(line.as_bytes());
                    output.push(b'\n');
                }
                Err(error) => {
                    output.extend_from_slice(error.to_string().as_bytes());
                    break;
                }
            }
        }
        output
    });
    let capture = Capture {
        child: Some(child),
        stderr: Some(stderr),
    };
    if ready_rx.recv_timeout(WAIT).is_err() {
        drop(capture);
        panic!("capture readiness");
    }
    capture
}
fn stop(child: &Capture) {
    let result = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("invoke kill");
    assert!(result.success(), "SIGTERM delivery");
}
fn send(address: std::net::SocketAddr, bytes: &[u8]) {
    let mut peer = TcpStream::connect(address).expect("connect");
    peer.write_all(bytes).expect("literal frame write");
}
fn wait_for(description: &str, predicate: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + WAIT;
    while !predicate() {
        assert!(std::time::Instant::now() < deadline, "{description}");
        thread::yield_now();
    }
}
fn directories(dir: &Temp) -> Vec<PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(&dir.0)
        .expect("output root")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    paths.sort();
    paths
}
fn status_json(dir: &Temp) -> Value {
    serde_json::from_str(&fs::read_to_string(dir.0.join("run-status.json")).expect("status"))
        .expect("complete run-status JSON")
}
fn json_u64(status: &Value, field: &str) -> u64 {
    status[field].as_u64().expect("u64 status field")
}
fn json_bool(status: &Value, field: &str) -> bool {
    status[field].as_bool().expect("boolean status field")
}
fn receiver_json(status: &Value) -> &Value {
    assert!(status["receiver"].is_object(), "receiver status object");
    &status["receiver"]
}

#[test]
fn literal_concurrent_calls_preserve_wire_wave_and_terminal_output() {
    let dir = Temp::new();
    let address = free_addr();
    let child = start(&dir, address, &["--max-active-recordings", "2"]);
    let first = [UUID.as_slice(), PCM.as_slice(), TERM.as_slice()].concat();
    let second = [UUID.as_slice(), PCM.as_slice(), TERM.as_slice()].concat();
    let one = thread::spawn(move || send(address, &first));
    let two = thread::spawn(move || send(address, &second));
    one.join().expect("first sender");
    two.join().expect("second sender");
    let terminal_deadline = std::time::Instant::now() + WAIT;
    loop {
        let completed = fs::read_dir(&dir.0)
            .expect("root")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().join("summary.json").exists())
            .filter(|entry| {
                fs::read_to_string(entry.path().join("summary.json"))
                    .expect("terminal summary")
                    .contains("Observed(Terminate)")
            })
            .count();
        if completed == 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < terminal_deadline,
            "both terminal summaries before stop"
        );
        thread::yield_now();
    }
    stop(&child);
    let result = child.finish();
    assert!(
        result.status.success(),
        "clean terminated calls: {:?}",
        result.status
    );
    let mut directories: Vec<_> = fs::read_dir(&dir.0)
        .expect("outputs")
        .filter_map(Result::ok)
        .map(|x| x.path())
        .filter(|p| p.is_dir())
        .collect();
    directories.sort();
    assert_eq!(directories.len(), 2);
    for path in directories {
        assert_eq!(
            fs::read(path.join("wire.bin")).expect("wire"),
            [UUID.as_slice(), PCM.as_slice(), TERM.as_slice()].concat()
        );
        let wav = fs::read(path.join("audio.wav")).expect("wave");
        assert_eq!(wav.len(), 46);
        assert_eq!(&wav[44..], &[0x34, 0x12]);
        let summary = fs::read_to_string(path.join("summary.json")).expect("summary");
        assert!(summary.contains("Observed(Terminate)"));
    }
    let status = status_json(&dir);
    assert_eq!(status["first_stop_cause"], "signal");
}

#[test]
fn truncated_call_preserves_prefix_and_fails_aggregate_beside_healthy_neighbor() {
    let dir = Temp::new();
    let address = free_addr();
    let child = start(&dir, address, &["--max-active-recordings", "2"]);
    let healthy = [UUID.as_slice(), PCM.as_slice(), TERM.as_slice()].concat();
    let truncated = [TRUNCATED_UUID.as_slice(), PCM.as_slice(), &[0x10, 0][..]].concat();
    send(address, &healthy);
    send(address, &truncated);
    wait_for("healthy and truncated summaries", || {
        let summaries: Vec<_> = directories(&dir)
            .iter()
            .filter_map(|path| fs::read_to_string(path.join("summary.json")).ok())
            .collect();
        summaries.len() == 2
            && summaries
                .iter()
                .any(|summary| summary.contains("Observed(Terminate)"))
            && summaries
                .iter()
                .any(|summary| summary.contains("Observed(Truncated)"))
    });
    stop(&child);
    let output = child.finish();
    assert!(
        !output.status.success(),
        "a truncated call makes the aggregate process result nonzero"
    );

    let expected_healthy = [UUID.as_slice(), PCM.as_slice(), TERM.as_slice()].concat();
    let expected_truncated = [TRUNCATED_UUID.as_slice(), PCM.as_slice(), &[0x10, 0][..]].concat();
    let mut saw_healthy = false;
    let mut saw_truncated = false;
    for path in directories(&dir) {
        let summary = fs::read_to_string(path.join("summary.json")).expect("summary");
        let wire = fs::read(path.join("wire.bin")).expect("wire");
        let wav = fs::read(path.join("audio.wav")).expect("wave");
        assert_eq!(wav.len(), 46);
        assert_eq!(&wav[44..], &[0x34, 0x12]);
        if summary.contains("Observed(Terminate)") {
            assert_eq!(wire, expected_healthy);
            saw_healthy = true;
        } else {
            assert!(summary.contains("Observed(Truncated)"));
            assert_eq!(wire, expected_truncated);
            saw_truncated = true;
        }
    }
    assert!(saw_healthy, "healthy neighbor remains complete");
    assert!(
        saw_truncated,
        "truncated call retains its exact accepted prefix"
    );
    assert_eq!(status_json(&dir)["first_stop_cause"], "signal");
}

#[test]
fn cli_refuses_unknown_missing_and_overflow_before_bind() {
    let dir = Temp::new();
    for args in [
        &["--wat"][..],
        &["--listen"][..],
        &[
            "--max-file-bytes",
            "18446744073709551615",
            "--max-recordings-per-run",
            "2",
        ][..],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_phonowire-capture"))
            .arg("--output")
            .arg(&dir.0)
            .args(args)
            .output()
            .expect("run parser");
        assert!(!output.status.success());
        assert!(
            !dir.0.join("run-status.json").exists(),
            "invalid config binds or writes status"
        );
    }
}

#[test]
fn sigterm_with_active_call_records_incomplete_and_fails() {
    let dir = Temp::new();
    let address = free_addr();
    let child = start(&dir, address, &[]);
    let mut peer = TcpStream::connect(address).expect("connect active peer");
    peer.write_all(&[UUID.as_slice(), PCM.as_slice()].concat())
        .expect("write accepted prefix");
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        if fs::read_dir(&dir.0)
            .expect("output root")
            .filter_map(Result::ok)
            .any(|entry| entry.path().is_dir())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "recording directory readiness"
        );
        thread::yield_now();
    }
    stop(&child);
    let result = child.finish();
    assert!(!result.status.success(), "incomplete active call must fail");
    let directory = fs::read_dir(&dir.0)
        .expect("output root")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .expect("recording directory");
    let summary = fs::read_to_string(directory.join("summary.json")).expect("summary");
    assert!(summary.contains("incomplete"));
    let status = status_json(&dir);
    assert_eq!(status["first_stop_cause"], "signal");
    drop(peer);
}

#[test]
fn file_cap_writer_failure_keeps_failed_summary() {
    let dir = Temp::new();
    let address = free_addr();
    let child = start(&dir, address, &["--max-file-bytes", "44"]);
    send(address, &UUID);
    let result = child.finish();
    assert!(!result.status.success(), "writer cap must fail capture");
    let recording = fs::read_dir(&dir.0)
        .expect("output root")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .expect("partial recording directory");
    let summary = fs::read_to_string(recording.join("summary.json")).expect("failed summary");
    assert!(summary.contains("failed"));
    assert!(summary.contains("events"));
    assert!(summary.contains("dropped_records"));
}

#[test]
fn existing_connection_directory_is_refused_without_overwrite() {
    let dir = Temp::new();
    let collision = dir.0.join("connection-1-2");
    fs::create_dir(&collision).expect("preexisting directory");
    fs::write(collision.join("sentinel"), b"preserve").expect("sentinel");
    let address = free_addr();
    let child = start(&dir, address, &[]);
    send(
        address,
        &[UUID.as_slice(), PCM.as_slice(), TERM.as_slice()].concat(),
    );
    let output = child.finish();
    assert!(!output.status.success(), "collision must fail");
    assert_eq!(
        fs::read(collision.join("sentinel")).expect("sentinel"),
        b"preserve"
    );
    assert!(
        !collision.join("wire.bin").exists(),
        "collision is never reused"
    );
}

#[test]
fn cumulative_limit_is_not_released_after_clean_completion() {
    let dir = Temp::new();
    let address = free_addr();
    let child = start(&dir, address, &["--max-recordings-per-run", "1"]);
    let complete = [UUID.as_slice(), PCM.as_slice(), TERM.as_slice()].concat();
    send(address, &complete);
    let first_deadline = std::time::Instant::now() + WAIT;
    loop {
        if fs::read_dir(&dir.0)
            .expect("root")
            .filter_map(Result::ok)
            .any(|entry| entry.path().join("summary.json").exists())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < first_deadline,
            "first completion readiness"
        );
        thread::yield_now();
    }
    send(address, &complete);
    let output = child.finish();
    assert!(
        !output.status.success(),
        "cumulative admission exhaustion fails"
    );
    let directories: Vec<_> = fs::read_dir(&dir.0)
        .expect("root")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir())
        .collect();
    assert_eq!(
        directories.len(),
        1,
        "second reservation never creates a directory"
    );
    let status = status_json(&dir);
    assert_eq!(status["first_stop_cause"], "admission_exhausted");
}

#[test]
fn active_capacity_refusal_is_distinct_from_cumulative_reservation() {
    let dir = Temp::new();
    let address = free_addr();
    let child = start(
        &dir,
        address,
        &[
            "--max-active-recordings",
            "1",
            "--max-recordings-per-run",
            "2",
        ],
    );
    let mut first = TcpStream::connect(address).expect("first active connect");
    first
        .write_all(&[UUID.as_slice(), PCM.as_slice()].concat())
        .expect("first active prefix");
    wait_for("first active recording", || directories(&dir).len() == 1);
    send(
        address,
        &[UUID.as_slice(), PCM.as_slice(), TERM.as_slice()].concat(),
    );
    let output = child.finish();
    assert!(!output.status.success(), "active-cap refusal fails the run");
    assert_eq!(
        directories(&dir).len(),
        1,
        "refused identity has no directory"
    );
    let status = status_json(&dir);
    assert_eq!(json_u64(&status, "reserved_recordings"), 1);
    assert_eq!(json_u64(&status, "admission_refused"), 1);
    assert_eq!(json_u64(&status, "final_budget_used"), 0);
    assert!(json_bool(receiver_json(&status), "counters_complete"));
    drop(first);
}

#[test]
fn one_slot_queue_repeated_signals_drain_active_prefix() {
    let dir = Temp::new();
    let address = free_addr();
    let child = start(
        &dir,
        address,
        &["--queue-records", "1", "--max-active-recordings", "2"],
    );
    let mut active = TcpStream::connect(address).expect("active connect");
    active
        .write_all(&[UUID.as_slice(), PCM.as_slice()].concat())
        .expect("active prefix");
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        if fs::read_dir(&dir.0)
            .expect("root")
            .filter_map(Result::ok)
            .any(|entry| entry.path().is_dir())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "active ownership readiness"
        );
        thread::yield_now();
    }
    let queued = TcpStream::connect(address).expect("queued connect");
    let interrupt = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("SIGINT");
    assert!(interrupt.success());
    let terminate = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("SIGTERM");
    assert!(terminate.success());
    let output = child.finish();
    assert!(!output.status.success(), "active incomplete capture fails");
    let status = status_json(&dir);
    assert_eq!(json_u64(&status, "final_budget_used"), 0);
    assert_eq!(json_u64(&status, "metadata_failures"), 0);
    assert_eq!(json_u64(&status, "unowned_records"), 0);
    assert_eq!(json_u64(&status, "unowned_bytes"), 0);
    let receiver = receiver_json(&status);
    assert!(json_u64(receiver, "records_handed_off") >= 1);
    assert_eq!(json_u64(receiver, "accepted"), 2);
    assert_eq!(json_u64(receiver, "refused"), 0);
    assert_eq!(json_u64(receiver, "live"), 2);
    assert!(json_bool(receiver, "counters_complete"));
    assert!(json_u64(&status, "signals") >= 1);
    let summaries: Vec<_> = fs::read_dir(&dir.0)
        .expect("root")
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("summary.json"))
        .filter(|path| path.exists())
        .collect();
    assert!(!summaries.is_empty(), "active prefix receives a summary");
    assert!(summaries.into_iter().any(|path| {
        fs::read_to_string(path)
            .expect("summary")
            .contains("incomplete")
    }));
    drop((active, queued));
}

#[test]
#[allow(clippy::too_many_lines)] // Keeps all explicitly allowed race outcomes together.
fn queued_connected_and_terminal_race_has_only_accounted_outcomes() {
    let dir = Temp::new();
    let address = free_addr();
    let child = start(
        &dir,
        address,
        &[
            "--queue-records",
            "1",
            "--max-active-recordings",
            "1",
            "--max-recordings-per-run",
            "2",
        ],
    );
    let mut active = TcpStream::connect(address).expect("active connect");
    active.write_all(&UUID).expect("active UUID");
    wait_for("active recording", || directories(&dir).len() == 1);
    let race = Arc::new(Barrier::new(2));
    let terminal_race = Arc::clone(&race);
    let terminal = thread::spawn(move || {
        terminal_race.wait();
        active.write_all(&TERM).expect("terminal frame");
    });
    let connected_race = Arc::clone(&race);
    let connected = thread::spawn(move || {
        connected_race.wait();
        send(address, &UUID);
    });
    terminal.join().expect("terminal sender");
    connected.join().expect("connected sender");
    let deadline = std::time::Instant::now() + WAIT;
    let needs_stop = loop {
        if dir.0.join("run-status.json").exists() {
            break false;
        }
        let paths = directories(&dir);
        let terminal_seen = paths.iter().any(|path| {
            path.join("summary.json").exists()
                && fs::read_to_string(path.join("summary.json"))
                    .expect("summary")
                    .contains("Observed(Terminate)")
        });
        if paths.len() == 2 && terminal_seen {
            break true;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "race outcome readiness"
        );
        thread::yield_now();
    };
    if needs_stop {
        stop(&child);
    }
    let output = child.finish();
    let status = status_json(&dir);
    let receiver = receiver_json(&status);
    assert_eq!(json_u64(&status, "final_budget_used"), 0);
    assert_eq!(
        json_u64(receiver, "raw_bytes_read"),
        json_u64(receiver, "raw_bytes_handed_off") + json_u64(receiver, "undelivered_raw_bytes"),
        "receiver accounts for every raw byte on either side of the handoff"
    );
    assert!(json_bool(receiver, "counters_complete"));
    let summaries: Vec<_> = directories(&dir)
        .iter()
        .map(|path| fs::read_to_string(path.join("summary.json")).expect("summary"))
        .collect();
    let terminal_delivered = summaries
        .iter()
        .any(|summary| summary.contains("Observed(Terminate)"));
    if !terminal_delivered {
        assert!(
            json_u64(receiver, "undelivered_raw_bytes") > 0,
            "a terminal absent from summaries is explicitly retained as undelivered input"
        );
        assert!(
            !output.status.success(),
            "undelivered terminal input fails the run"
        );
        return;
    }
    match summaries.len() {
        1 => {
            assert!(!output.status.success(), "active-cap refusal ends the run");
            assert_eq!(json_u64(&status, "admission_refused"), 1);
            assert_eq!(status["first_stop_cause"], "local_failure");
        }
        2 => {
            let incomplete = summaries
                .iter()
                .any(|summary| summary.contains("\"incomplete\""));
            if incomplete {
                assert!(
                    !output.status.success(),
                    "post-terminal active capture is incomplete"
                );
            } else {
                assert!(
                    output.status.success(),
                    "both terminal records complete before quiescent stop"
                );
            }
            assert_eq!(status["first_stop_cause"], "signal");
        }
        count => panic!("unaccounted race directory count: {count}"),
    }
}

#[test]
fn readiness_observation_allows_immediate_orderly_signal() {
    let dir = Temp::new();
    let address = free_addr();
    let child = start(&dir, address, &[]);
    stop(&child);
    let output = child.finish();
    assert!(
        output.status.success(),
        "idle ready process handles SIGTERM orderly"
    );
    let status = fs::read_to_string(dir.0.join("run-status.json")).expect("status");
    assert!(status.contains("signal"));
}
