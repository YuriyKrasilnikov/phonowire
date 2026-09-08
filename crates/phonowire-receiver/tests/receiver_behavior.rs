//! Localhost behavioral witnesses for the public receiver API.
use std::io::Write;
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream};
use std::num::{NonZeroU16, NonZeroUsize};
use std::thread;
use std::time::Duration;

use phonowire_receiver::{ByteBudget, EndReason, Limits, Receiver, RecordKind};

fn limits(slots: usize, turns: usize) -> Limits {
    Limits::new(
        NonZeroUsize::new(2).expect("connection limit"),
        NonZeroUsize::new(slots).expect("slot limit"),
        NonZeroU16::new(64).expect("payload limit"),
        NonZeroUsize::new(turns).expect("turn limit"),
    )
    .expect("valid limits")
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

    let mut raw = 0_usize;
    let mut started = false;
    let mut audio = false;
    let mut ended = false;
    for _ in 0..8 {
        let record = records
            .recv_timeout(Duration::from_secs(2))
            .expect("record");
        match record.kind {
            RecordKind::Wire { bytes } => raw += bytes.as_slice().len(),
            RecordKind::Started { .. } => started = true,
            RecordKind::Audio { bytes, .. } => {
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
    assert_eq!(raw, bytes.len());
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
    for _ in 0..4 {
        let record = records
            .recv_timeout(Duration::from_secs(2))
            .expect("record");
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
    for _ in 0..5 {
        let record = records
            .recv_timeout(Duration::from_secs(2))
            .expect("record");
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
    // No more writes occur: the queued wire and start records require only this credit.
    let wire = records
        .recv_timeout(Duration::from_secs(2))
        .expect("wire after credit");
    assert!(matches!(wire.kind, RecordKind::Wire { .. }));
    let started = records
        .recv_timeout(Duration::from_secs(2))
        .expect("started after credit");
    assert!(matches!(started.kind, RecordKind::Started { .. }));
    stop.stop().expect("stop wake");
    worker.join().expect("worker thread").expect("worker run");
}

#[test]
fn retained_raw_record_releases_byte_credit_without_another_tcp_write() {
    let budget = ByteBudget::new(NonZeroUsize::new(4096).expect("budget"));
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(16, 8),
        budget,
    )
    .expect("bind");
    let local = receiver.local_addr();
    let worker = thread::spawn(move || receiver.run());
    let mut peer = TcpStream::connect(local).expect("connect");
    let mut bytes = vec![0_u8; 4096];
    bytes[..24].copy_from_slice(&[
        0x01, 0x00, 0x10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x10, 0x00, 0x02, 0, 0,
    ]);
    peer.write_all(&bytes).expect("one full read");
    let _connected = records
        .recv_timeout(Duration::from_secs(2))
        .expect("connected");
    let retained = records
        .recv_timeout(Duration::from_secs(2))
        .expect("raw record");
    assert!(matches!(retained.kind, RecordKind::Wire { .. }));
    let _started = records
        .recv_timeout(Duration::from_secs(2))
        .expect("started");
    assert!(matches!(
        records.recv_timeout(Duration::from_millis(50)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    drop(retained);
    let audio = records
        .recv_timeout(Duration::from_secs(2))
        .expect("audio after byte credit");
    assert!(matches!(audio.kind, RecordKind::Audio { .. }));
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
    for _ in 0..8 {
        let record = records
            .recv_timeout(Duration::from_secs(2))
            .expect("record");
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
fn stop_cancels_task_waiting_behind_a_full_handoff_slot() {
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
    // The silent peer has no more bytes; Wire occupies the sole slot and Started is pending.
    stop.stop().expect("stop wake");
    let summary = worker.join().expect("worker thread").expect("worker run");
    assert_eq!(
        summary.stopped_by,
        Some(phonowire_receiver::StopCause::Requested)
    );
    assert_eq!(summary.abandoned, 1);
    assert!(summary.undelivered_records >= 1);
}
