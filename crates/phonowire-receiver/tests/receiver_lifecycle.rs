//! Retained output and worker lifecycles through the public API.
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::{Duration, Instant};

use phonowire_receiver::{ByteBudget, Limits, READ_BYTES, Receiver, ReceiverFailure, RecordKind};

const UUID_FRAME: [u8; 19] = [
    1, 0, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
];
const DEADLINE: Duration = Duration::from_secs(3);

fn limits() -> Limits {
    Limits::new(
        NonZeroUsize::MIN,
        NonZeroUsize::new(8).expect("positive record capacity"),
        NonZeroU16::new(64).expect("positive payload capacity"),
        NonZeroUsize::MIN,
    )
    .expect("UUID fits")
}

fn address() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

#[test]
fn old_output_remains_charged_and_wakes_a_replacement_worker() {
    let budget = ByteBudget::new(NonZeroUsize::new(READ_BYTES).expect("positive read buffer"));
    let (receiver, records, stop) =
        Receiver::bind(address(), limits(), budget.clone()).expect("bind");
    let mut peer = TcpStream::connect(receiver.local_addr()).expect("first connection");
    let stream = UUID_FRAME;
    peer.write_all(&stream).expect("queue UUID before run");
    let worker = thread::spawn(move || receiver.run());
    let connected = records.recv_timeout(DEADLINE).expect("connected");
    assert!(matches!(connected.kind, RecordKind::Connected { .. }));
    let old_id = connected.connection;
    let deadline = Instant::now() + DEADLINE;
    let mut retained = Vec::new();
    let mut raw = Vec::new();
    while raw.len() < stream.len() {
        let first = records
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("raw observation");
        let RecordKind::Wire { bytes } = first.kind else {
            panic!("all UUID wire precedes its decoded event");
        };
        assert_eq!(
            first.offset.get(),
            u64::try_from(raw.len()).expect("offset")
        );
        raw.extend_from_slice(bytes.as_slice());
        retained.push(bytes);
    }
    assert_eq!(raw, stream);
    stop.stop().expect("stop first worker");
    let summary = worker
        .join()
        .expect("worker completes")
        .expect("worker result");
    drop(records);
    drop(peer);
    assert_eq!(summary.retained_output_bytes, UUID_FRAME.len());
    assert_eq!(budget.used(), UUID_FRAME.len());
    let padding = budget
        .try_copy(&vec![0; READ_BYTES - UUID_FRAME.len()])
        .expect("saturate exact remaining capacity");

    let (receiver, records, stop) =
        Receiver::bind(address(), limits(), budget.clone()).expect("rebind");
    let mut peer = TcpStream::connect(receiver.local_addr()).expect("replacement connection");
    peer.write_all(&UUID_FRAME).expect("send UUID once");
    let worker = thread::spawn(move || receiver.run());
    let connected = records
        .recv_timeout(DEADLINE)
        .expect("replacement connected");
    assert_ne!(connected.connection, old_id);
    assert!(matches!(connected.kind, RecordKind::Connected { .. }));
    assert!(matches!(
        records.recv_timeout(Duration::from_millis(30)),
        Err(RecvTimeoutError::Timeout)
    ));
    assert_eq!(budget.used(), READ_BYTES);
    drop(retained);
    let deadline = Instant::now() + DEADLINE;
    let mut raw = Vec::new();
    loop {
        let record = records
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("old storage release wakes new worker");
        match record.kind {
            RecordKind::Wire { bytes } => {
                assert_eq!(
                    record.offset.get(),
                    u64::try_from(raw.len()).expect("offset")
                );
                raw.extend_from_slice(bytes.as_slice());
            }
            RecordKind::Started { .. } => break,
            other => panic!("unexpected record: {other:?}"),
        }
    }
    assert_eq!(raw, UUID_FRAME);
    drop(padding);
    stop.stop().expect("stop replacement");
    worker
        .join()
        .expect("replacement joins")
        .expect("replacement result");
    drop(records);
    assert_eq!(budget.used(), 0);
}

#[test]
fn a_second_active_worker_cannot_replace_the_credit_subscriber() {
    let budget = ByteBudget::new(NonZeroUsize::new(READ_BYTES).expect("positive capacity"));
    let (first, records, stop) =
        Receiver::bind(address(), limits(), budget.clone()).expect("first bind");
    let _peer = TcpStream::connect(first.local_addr()).expect("first peer");
    let worker = thread::spawn(move || first.run());
    assert!(matches!(
        records
            .recv_timeout(DEADLINE)
            .expect("active first worker")
            .kind,
        RecordKind::Connected { .. }
    ));
    let (second, _second_records, _second_stop) =
        Receiver::bind(address(), limits(), budget).expect("second bind");
    let error = second.run().expect_err("one active budget subscriber");
    assert!(matches!(
        error.failure(),
        ReceiverFailure::BudgetAlreadySubscribed
    ));
    stop.stop().expect("first subscriber still wakes");
    worker.join().expect("first joins").expect("first result");
}
