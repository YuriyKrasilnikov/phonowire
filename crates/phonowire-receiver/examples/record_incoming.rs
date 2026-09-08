//! Receives one literal localhost call into caller-owned recording outputs.
use std::error::Error;
use std::io::{Cursor, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::num::{NonZeroU16, NonZeroUsize};
use std::thread;
use std::time::Duration;

use phonowire_receiver::{
    ByteBudget, Limits, READ_BYTES, Receiver, RecordKind, Recording, RecordingEnd,
    RecordingSummary, Records, TerminalKind,
};

const INPUT: [u8; 29] = [
    1, 0, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0x10, 0, 4, 0x34, 0x12, 0xcc,
    0xed, 0, 0, 0,
];
const PCM: [u8; 4] = [0x34, 0x12, 0xcc, 0xed];
const WAIT: Duration = Duration::from_secs(3);

fn main() -> Result<(), Box<dyn Error>> {
    let limits = Limits::new(
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        NonZeroU16::new(320).expect("PCM capacity is positive"),
        NonZeroUsize::MIN,
    )?;
    let budget = ByteBudget::new(NonZeroUsize::new(READ_BYTES).expect("read capacity is positive"));
    let (receiver, records, stop) = Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits,
        budget.clone(),
    )?;
    let mut peer = TcpStream::connect(receiver.local_addr())?;
    peer.write_all(&INPUT)?;
    let worker = thread::spawn(move || receiver.run());
    let output = record_call(&records);
    let stop_result = stop.stop();
    let network_result = worker
        .join()
        .expect("receiver thread preserves its invariants");
    drop(records);
    let summary = output?;
    stop_result?;
    let network = network_result?;
    assert_eq!(summary.end, RecordingEnd::Observed(TerminalKind::Terminate));
    assert_eq!(network.raw_bytes_handed_off, 29);
    assert_eq!(budget.used(), 0);
    println!(
        "{} wire bytes, {} PCM bytes; {:?}",
        summary.wire_bytes, summary.audio_bytes, summary.end
    );
    Ok(())
}

fn record_call(records: &Records) -> Result<RecordingSummary, Box<dyn Error>> {
    let connected = records.recv_timeout(WAIT)?;
    let mut wire = Vec::new();
    let mut wave = Cursor::new(Vec::new());
    let mut events = Vec::new();
    let mut recording = Recording::new(connected.connection, &mut wire, &mut wave, &mut events)?;
    recording.record(&connected)?;
    loop {
        let record = records.recv_timeout(WAIT)?;
        let terminal = matches!(&record.kind, RecordKind::Ended { .. });
        recording.record(&record)?;
        if terminal {
            break;
        }
    }
    let summary = recording.finish()?;
    assert_eq!(wire, INPUT);
    assert_eq!(&wave.get_ref()[44..], PCM);
    assert!(!events.is_empty());
    Ok(summary)
}
