//! Localhost behavioral witnesses for the public receiver API.
use std::io::Write;
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream};
use std::num::{NonZeroU16, NonZeroUsize};
use std::thread;
use std::time::{Duration, Instant};

use phonowire_audiosocket::SampleRate;
use phonowire_receiver::{
    ByteBudget, ConnectionCloseError, ConnectionCloseResult, EndReason, Limits, Receiver,
    RecordKind, Records,
};

fn limits(slots: usize, turns: usize) -> Limits {
    Limits::new(
        NonZeroUsize::new(2).expect("connection limit"),
        NonZeroUsize::new(slots).expect("slot limit"),
        NonZeroU16::new(64).expect("payload limit"),
        NonZeroUsize::new(turns).expect("turn limit"),
    )
    .expect("valid limits")
}

fn next_record(records: &Records, deadline: Instant) -> phonowire_receiver::Record {
    records
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .expect("record before deadline")
}

#[test]
fn fragmented_uuid_audio_and_terminate_preserve_raw_before_events() {
    let budget = ByteBudget::new(NonZeroUsize::new(8192).expect("budget"));
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let (receiver, records, stop) = Receiver::bind(address, limits(16, 1), budget).expect("bind");
    let local = receiver.local_addr();
    let worker = thread::spawn(move || receiver.run());
    let mut peer = TcpStream::connect(local).expect("connect");
    let bytes = [
        0x01, 0x00, 0x10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x10, 0x00, 0x02, 0, 0,
        0x00, 0x00, 0x00,
    ];
    peer.write_all(&bytes[..2]).expect("fragment one");
    peer.write_all(&bytes[2..19]).expect("fragment two");
    peer.write_all(&bytes[19..]).expect("fragment three");
    peer.flush().expect("flush");

    let mut raw = Vec::new();
    let mut started = false;
    let mut audio = false;
    let mut ended = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let record = next_record(&records, deadline);
        match record.kind {
            RecordKind::Wire { bytes } => {
                assert_eq!(
                    record.offset.get(),
                    u64::try_from(raw.len()).expect("offset")
                );
                raw.extend_from_slice(bytes.as_slice());
            }
            RecordKind::Started { .. } => {
                assert!(raw.len() >= 19);
                assert_eq!(record.offset.get(), 19);
                started = true;
            }
            RecordKind::Audio { bytes, .. } => {
                assert!(started && raw.len() >= 24);
                assert_eq!(record.offset.get(), 24);
                audio = bytes.as_slice() == [0, 0];
            }
            RecordKind::Ended {
                reason: EndReason::Terminate,
                ..
            } => {
                ended = true;
                break;
            }
            RecordKind::Connected { .. } | RecordKind::Dtmf { .. } | RecordKind::Ended { .. } => {}
        }
    }
    assert_eq!(raw, bytes);
    assert!(started);
    assert!(audio);
    assert!(ended);
    stop.stop().expect("stop wake");
    let summary = worker.join().expect("worker thread").expect("worker run");
    assert_eq!(
        summary.raw_bytes_read,
        u64::try_from(bytes.len()).expect("length")
    );
}

#[test]
fn eof_after_uuid_retains_identity_in_clean_terminal_record() {
    let budget = ByteBudget::new(NonZeroUsize::new(8192).expect("budget"));
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(16, 1),
        budget,
    )
    .expect("bind");
    let local = receiver.local_addr();
    let worker = thread::spawn(move || receiver.run());
    let mut peer = TcpStream::connect(local).expect("connect");
    peer.write_all(&[
        0x01, 0x00, 0x10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
    ])
    .expect("uuid");
    peer.shutdown(Shutdown::Write).expect("eof");
    let mut clean_uuid = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let record = next_record(&records, deadline);
        if let RecordKind::Ended {
            uuid: Some(_),
            reason: EndReason::CleanEof,
        } = record.kind
        {
            clean_uuid = true;
            break;
        }
    }
    assert!(clean_uuid);
    stop.stop().expect("stop wake");
    worker.join().expect("worker thread").expect("worker run");
}

#[test]
fn policy_rejection_after_uuid_retains_identity() {
    let budget = ByteBudget::new(NonZeroUsize::new(8192).expect("budget"));
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(16, 1),
        budget,
    )
    .expect("bind");
    let local = receiver.local_addr();
    let worker = thread::spawn(move || receiver.run());
    let mut peer = TcpStream::connect(local).expect("connect");
    peer.write_all(&[
        0x01, 0x00, 0x10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x02, 0x00, 0x00,
    ])
    .expect("frames");
    let mut policy_uuid = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let record = next_record(&records, deadline);
        if let RecordKind::Ended {
            uuid: Some(_),
            reason: EndReason::Policy(_),
        } = record.kind
        {
            policy_uuid = true;
            break;
        }
    }
    assert!(policy_uuid);
    stop.stop().expect("stop wake");
    worker.join().expect("worker thread").expect("worker run");
}

#[test]
fn one_slot_queue_resumes_after_consumer_credit_while_peer_is_silent() {
    let budget = ByteBudget::new(NonZeroUsize::new(8192).expect("budget"));
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(1, 8),
        budget,
    )
    .expect("bind");
    let local = receiver.local_addr();
    let worker = thread::spawn(move || receiver.run());
    let mut peer = TcpStream::connect(local).expect("connect");
    peer.write_all(&[
        0x01, 0x00, 0x10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
    ])
    .expect("uuid");
    let connected = records
        .recv_timeout(Duration::from_secs(2))
        .expect("connected");
    assert!(matches!(connected.kind, RecordKind::Connected { .. }));
    // Wire may consist of several read chunks; every receive returns a slot.
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut raw = Vec::new();
    loop {
        let record = next_record(&records, deadline);
        match record.kind {
            RecordKind::Wire { bytes } => {
                assert_eq!(
                    record.offset.get(),
                    u64::try_from(raw.len()).expect("offset")
                );
                raw.extend_from_slice(bytes.as_slice());
            }
            RecordKind::Started { .. } => break,
            other => panic!("unexpected event before UUID: {other:?}"),
        }
    }
    assert_eq!(
        raw,
        [
            0x01, 0x00, 0x10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
        ]
    );
    stop.stop().expect("stop wake");
    worker.join().expect("worker thread").expect("worker run");
}

#[test]
fn full_byte_budget_resumes_without_another_tcp_write() {
    let budget = ByteBudget::new(NonZeroUsize::new(4096).expect("budget"));
    let retained = budget.try_copy(&[0; 4096]).expect("fill byte budget");
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(16, 8),
        budget.clone(),
    )
    .expect("bind");
    let local = receiver.local_addr();
    let worker = thread::spawn(move || receiver.run());
    let mut peer = TcpStream::connect(local).expect("connect");
    let bytes = [
        0x01, 0x00, 0x10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x10, 0x00, 0x02, 0, 0,
        0, 0, 0,
    ];
    peer.write_all(&bytes).expect("send once");
    assert!(matches!(
        next_record(&records, Instant::now() + Duration::from_secs(2)).kind,
        RecordKind::Connected { .. }
    ));
    assert_eq!(budget.used(), 4096);
    assert!(matches!(
        records.recv_timeout(Duration::from_millis(30)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    // Full capacity is established by the owned allocation, independently of TCP reads.
    drop(retained);
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut raw = Vec::new();
    let mut audio = Vec::new();
    loop {
        let record = next_record(&records, deadline);
        match record.kind {
            RecordKind::Wire { bytes } => {
                assert_eq!(
                    record.offset.get(),
                    u64::try_from(raw.len()).expect("offset")
                );
                raw.extend_from_slice(bytes.as_slice());
            }
            RecordKind::Audio { bytes, .. } => audio.extend_from_slice(bytes.as_slice()),
            RecordKind::Started { .. } => {}
            RecordKind::Ended {
                reason: EndReason::Terminate,
                ..
            } => break,
            other => panic!("unexpected record: {other:?}"),
        }
    }
    assert_eq!(raw, bytes);
    assert_eq!(audio, [0, 0]);
    stop.stop().expect("stop wake");
    worker.join().expect("worker thread").expect("worker run");
}

#[test]
fn quota_one_admits_two_concurrent_uuid_streams() {
    let budget = ByteBudget::new(NonZeroUsize::new(8192).expect("budget"));
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(32, 1),
        budget,
    )
    .expect("bind");
    let local = receiver.local_addr();
    let worker = thread::spawn(move || receiver.run());
    let mut first = TcpStream::connect(local).expect("first connect");
    let mut second = TcpStream::connect(local).expect("second connect");
    first
        .write_all(&[
            0x01, 0x00, 0x10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
        ])
        .expect("first uuid");
    second
        .write_all(&[
            0x01, 0x00, 0x10, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
        ])
        .expect("second uuid");
    let mut first_seen = false;
    let mut second_seen = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let record = next_record(&records, deadline);
        if let RecordKind::Started { uuid } = record.kind {
            first_seen |= uuid.bytes()[0] == 1;
            second_seen |= uuid.bytes()[0] == 2;
            if first_seen && second_seen {
                break;
            }
        }
    }
    assert!(first_seen && second_seen);
    stop.stop().expect("stop wake");
    worker.join().expect("worker thread").expect("worker run");
}

#[test]
fn stop_cancels_an_accepted_task_without_assuming_its_wait_state() {
    let budget = ByteBudget::new(NonZeroUsize::new(8192).expect("budget"));
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(1, 8),
        budget,
    )
    .expect("bind");
    let local = receiver.local_addr();
    let worker = thread::spawn(move || receiver.run());
    let mut peer = TcpStream::connect(local).expect("connect");
    peer.write_all(&[
        0x01, 0x00, 0x10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
    ])
    .expect("uuid");
    let connected = records
        .recv_timeout(Duration::from_secs(2))
        .expect("connected");
    assert!(matches!(connected.kind, RecordKind::Connected { .. }));
    // Connected establishes admission, but does not establish a pending Queue wait.
    stop.stop().expect("stop wake");
    let summary = worker.join().expect("worker thread").expect("worker run");
    assert_eq!(
        summary.stopped_by,
        Some(phonowire_receiver::StopCause::Requested)
    );
    assert_eq!(summary.abandoned, 1);
    assert_eq!(summary.accepted, 1);
    assert!(summary.undelivered_records <= summary.abandoned);
    assert_eq!(
        summary.raw_bytes_read,
        summary.raw_bytes_handed_off + summary.undelivered_raw_bytes
    );
    let mut queued = 0_u64;
    while records.try_recv().is_ok() {
        queued += 1;
    }
    assert_eq!(summary.records_handed_off, queued + 1);
}

#[test]
fn scoped_close_retires_one_connection_while_output_is_full() {
    let budget = ByteBudget::new(NonZeroUsize::new(8192).expect("budget"));
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(3, 1),
        budget.clone(),
    )
    .expect("bind");
    let local = receiver.local_addr();
    let control = receiver.connection_control();
    let worker = thread::spawn(move || receiver.run());
    let mut peer = TcpStream::connect(local).expect("connect");
    let connected = next_record(&records, Instant::now() + Duration::from_secs(2));
    let id = connected.connection;
    peer.write_all(&[
        0x01, 0x00, 0x10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x10, 0x00, 0x02, 0, 0,
    ])
    .expect("uuid and audio");
    peer.flush().expect("flush");
    thread::sleep(Duration::from_millis(50));
    let ticket = control.close(id).expect("request exact close");
    let (foreign_receiver, _foreign_records, _foreign_stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(3, 1),
        ByteBudget::new(NonZeroUsize::new(8192).expect("foreign budget")),
    )
    .expect("foreign bind");
    assert!(matches!(
        foreign_receiver.connection_control().close(id),
        Err(ConnectionCloseError::UnknownOrStale)
    ));
    assert_eq!(
        ticket.wait_timeout(Duration::from_secs(2)),
        Some(ConnectionCloseResult::RetiredByRequest)
    );
    assert!(
        matches!(control.close(id), Err(ConnectionCloseError::UnknownOrStale)),
        "a retired identity cannot affect a later connection"
    );
    let first = next_record(&records, Instant::now() + Duration::from_secs(2));
    let second = next_record(&records, Instant::now() + Duration::from_secs(2));
    let third = next_record(&records, Instant::now() + Duration::from_secs(2));
    let audio = [first, second, third]
        .into_iter()
        .find(|record| matches!(record.kind, RecordKind::Audio { .. }))
        .expect("queued audio remains readable after physical retirement");
    assert_eq!(ticket.retained_output_bytes(), 2);
    drop(audio);
    assert_eq!(ticket.retained_output_bytes(), 0);
    stop.stop().expect("stop");
    worker.join().expect("worker thread").expect("worker run");
    assert_eq!(budget.used(), 0);
}

#[test]
fn scoped_close_of_a_does_not_interrupt_b_afterward() {
    let budget = ByteBudget::new(NonZeroUsize::new(8192).expect("budget"));
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(32, 1),
        budget.clone(),
    )
    .expect("bind");
    let local = receiver.local_addr();
    let control = receiver.connection_control();
    let worker = thread::spawn(move || receiver.run());
    let _a = TcpStream::connect(local).expect("connect A");
    let a_id = next_record(&records, Instant::now() + Duration::from_secs(2)).connection;
    let mut b = TcpStream::connect(local).expect("connect B");
    let b_id = next_record(&records, Instant::now() + Duration::from_secs(2)).connection;
    assert_ne!(a_id, b_id);

    let ticket = control.close(a_id).expect("close A");
    assert_eq!(
        ticket.wait_timeout(Duration::from_secs(2)),
        Some(ConnectionCloseResult::RetiredByRequest)
    );

    let b_payload = [0x34, 0x12];
    b.write_all(&[
        0x01,
        0x00,
        0x10,
        2,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0x10,
        0x00,
        0x02,
        b_payload[0],
        b_payload[1],
    ])
    .expect("B uuid and audio after A close");
    b.flush().expect("flush B");
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut b_audio = None;
    while Instant::now() < deadline {
        let record = next_record(&records, deadline);
        if record.connection == b_id
            && let RecordKind::Audio { rate, bytes, .. } = record.kind
        {
            b_audio = Some((rate, bytes.as_slice().to_vec()));
            break;
        }
    }
    assert_eq!(b_audio, Some((SampleRate::Khz8, b_payload.to_vec())));
    stop.stop().expect("global stop");
    worker.join().expect("worker thread").expect("worker run");
    assert_eq!(budget.used(), 0);
}
