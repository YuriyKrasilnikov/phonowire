//! Executable literal acceptance tests for the consumer-owned recorder.
use super::*;
use crate::{ByteBudget, OwnedBytes, WireOffset};
use std::io::{self, Seek, SeekFrom, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Default)]
struct Shared {
    bytes: Arc<Mutex<Vec<u8>>>,
    position: Arc<Mutex<usize>>,
    limit: Arc<Mutex<Option<usize>>>,
    allowance: Arc<Mutex<Option<usize>>>,
    fail_write: Arc<Mutex<bool>>,
    fail_seek: Arc<Mutex<bool>>,
    fail_flush: Arc<Mutex<bool>>,
    fail_when_exhausted: Arc<Mutex<bool>>,
    interrupt_once: Arc<Mutex<bool>>,
}
impl Shared {
    fn bytes(&self) -> Vec<u8> {
        self.bytes.lock().expect("test output lock").clone()
    }
}
impl Write for Shared {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let mut interrupted = self.interrupt_once.lock().expect("test interrupt lock");
        if *interrupted {
            *interrupted = false;
            drop(interrupted);
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        drop(interrupted);
        if *self.fail_write.lock().expect("test write fault lock") {
            return Err(io::Error::other("write fault"));
        }
        let limit = *self.limit.lock().expect("test limit lock");
        let mut allowance = self.allowance.lock().expect("test allowance lock");
        let permitted = allowance.map_or(input.len(), |value| value.min(input.len()));
        let count = limit.map_or(permitted, |value| value.min(permitted));
        if count == 0
            && *self
                .fail_when_exhausted
                .lock()
                .expect("test exhaustion lock")
        {
            return Err(io::Error::other("scheduled exhaustion"));
        }
        if count == 0 {
            return Ok(0);
        }
        if let Some(value) = allowance.as_mut() {
            *value -= count;
        }
        drop(allowance);
        let mut bytes = self.bytes.lock().expect("test output lock");
        let mut position = self.position.lock().expect("test position lock");
        let end = position.checked_add(count).expect("small test output");
        if bytes.len() < end {
            bytes.resize(end, 0);
        }
        bytes[*position..end].copy_from_slice(&input[..count]);
        *position = end;
        drop(position);
        drop(bytes);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        if *self.fail_flush.lock().expect("test flush fault lock") {
            Err(io::Error::other("flush fault"))
        } else {
            Ok(())
        }
    }
}
impl Seek for Shared {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        if *self.fail_seek.lock().expect("test seek fault lock") {
            return Err(io::Error::other("seek fault"));
        }
        let value = match from {
            SeekFrom::Start(value) => {
                usize::try_from(value).map_err(|_| io::Error::other("large test seek"))?
            }
            SeekFrom::Current(value) | SeekFrom::End(value) => {
                usize::try_from(value).map_err(|_| io::Error::other("negative test seek"))?
            }
        };
        *self.position.lock().expect("test position lock") = value;
        u64::try_from(value).map_err(|_| io::Error::other("large test seek"))
    }
}

fn id() -> ConnectionId {
    ConnectionId::new(9, 3)
}
fn record(offset: u64, kind: RecordKind) -> Record {
    Record {
        connection: id(),
        offset: WireOffset::new(offset),
        observed_at: Instant::now(),
        kind,
    }
}
fn owned(bytes: &[u8]) -> OwnedBytes {
    ByteBudget::new(NonZeroUsize::new(8192).expect("budget"))
        .try_copy(bytes)
        .expect("small owned bytes")
}
fn connected() -> Record {
    record(
        0,
        RecordKind::Connected {
            peer: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9),
        },
    )
}
fn uuid() -> phonowire_audiosocket::Uuid {
    phonowire_audiosocket::Uuid::new([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

#[test]
fn writes_literal_wav_header_pcm_wire_and_incomplete_summary() {
    const EXPECTED_HEADER: [u8; 44] = [
        b'R', b'I', b'F', b'F', 38, 0, 0, 0, b'W', b'A', b'V', b'E', b'f', b'm', b't', b' ', 16, 0,
        0, 0, 1, 0, 1, 0, 0x40, 0x1f, 0, 0, 0x80, 0x3e, 0, 0, 2, 0, 16, 0, b'd', b'a', b't', b'a',
        2, 0, 0, 0,
    ];
    let wire = Shared::default();
    let wave = Shared::default();
    let events = Shared::default();
    let mut recording =
        Recording::new(id(), wire.clone(), wave.clone(), events.clone()).expect("header");
    recording.record(&connected()).expect("connected");
    recording
        .record(&record(
            0,
            RecordKind::Wire {
                bytes: owned(&[1, 2, 3]),
            },
        ))
        .expect("wire");
    recording
        .record(&record(3, RecordKind::Started { uuid: uuid() }))
        .expect("started");
    recording
        .record(&record(
            3,
            RecordKind::Audio {
                uuid: uuid(),
                rate: SampleRate::Khz8,
                bytes: owned(&[0, 0]),
            },
        ))
        .expect("audio");
    let summary = recording.finish().expect("finish incomplete");
    assert_eq!(summary.end, RecordingEnd::Incomplete);
    assert_eq!(summary.wire_bytes, 3);
    assert_eq!(summary.audio_bytes, 2);
    assert_eq!(wire.bytes(), vec![1, 2, 3]);
    let wave_bytes = wave.bytes();

    assert_eq!(&wave_bytes[..44], EXPECTED_HEADER);
    assert_eq!(&wave_bytes[44..], &[0, 0]);
    let text = String::from_utf8(events.bytes()).expect("diagnostics UTF-8");
    let descriptions: Vec<_> = text.lines().collect();
    assert_eq!(descriptions.len(), 4);
    assert!(descriptions[0].contains("connected peer="));
    assert!(descriptions[1].ends_with("wire bytes=3"));
    assert!(descriptions[2].contains("started uuid="));
    assert!(descriptions[3].contains("audio uuid="));
    assert!(descriptions[3].ends_with("rate=Khz8 bytes=2"));
    assert!(!text.contains("ByteBudget"));
}

#[test]
fn zero_write_preserves_known_prefix_and_refuses_later_mutation() {
    let wire = Shared::default();
    *wire.allowance.lock().expect("allowance") = Some(1);
    let mut recording =
        Recording::new(id(), wire.clone(), Shared::default(), Shared::default()).expect("header");
    recording.record(&connected()).expect("connected");
    let error = recording
        .record(&record(
            0,
            RecordKind::Wire {
                bytes: owned(&[7, 8]),
            },
        ))
        .expect_err("zero write");
    assert_eq!(error.stage(), RecordingStage::Wire);
    assert_eq!(error.summary().wire_bytes, 1);
    assert_eq!(wire.bytes(), vec![7]);
    let later = recording
        .record(&record(1, RecordKind::Wire { bytes: owned(&[9]) }))
        .expect_err("failed state");
    assert_eq!(later.stage(), RecordingStage::Validate);
    assert!(matches!(
        later.failure(),
        RecordingFailure::Invalid(RecordingViolation::AfterFailure)
    ));
}

#[test]
fn header_wire_audio_and_events_faults_report_their_exact_stage() {
    let header = Shared::default();
    *header.fail_write.lock().expect("fault") = true;
    let Err(error) = Recording::new(id(), Shared::default(), header, Shared::default()) else {
        panic!("header fault must fail")
    };
    assert_eq!(error.stage(), RecordingStage::Header);
    assert_eq!(error.summary().header_bytes, 0);

    let wire = Shared::default();
    let mut recording =
        Recording::new(id(), wire.clone(), Shared::default(), Shared::default()).expect("new");
    recording.record(&connected()).expect("connected");
    *wire.fail_write.lock().expect("fault") = true;
    let error = recording
        .record(&record(0, RecordKind::Wire { bytes: owned(&[1]) }))
        .expect_err("wire fault");
    assert_eq!(error.stage(), RecordingStage::Wire);
    assert_eq!(error.summary().wire_bytes, 0);
    assert!(wire.bytes().is_empty());

    let wave = Shared::default();
    let mut recording =
        Recording::new(id(), Shared::default(), wave.clone(), Shared::default()).expect("new");
    recording.record(&connected()).expect("connected");
    recording
        .record(&record(0, RecordKind::Started { uuid: uuid() }))
        .expect("started");
    *wave.fail_write.lock().expect("fault") = true;
    let error = recording
        .record(&record(
            0,
            RecordKind::Audio {
                uuid: uuid(),
                rate: SampleRate::Khz8,
                bytes: owned(&[0, 0]),
            },
        ))
        .expect_err("audio fault");
    assert_eq!(error.stage(), RecordingStage::Audio);
    assert_eq!(error.summary().audio_bytes, 0);
    assert_eq!(wave.bytes().len(), 44);

    let events = Shared::default();
    let mut recording =
        Recording::new(id(), Shared::default(), Shared::default(), events.clone()).expect("new");
    *events.fail_write.lock().expect("fault") = true;
    let error = recording.record(&connected()).expect_err("event fault");
    assert_eq!(error.stage(), RecordingStage::Events);
    assert_eq!(error.summary().event_bytes, 0);
    assert!(events.bytes().is_empty());
}

#[test]
fn seek_patch_and_flush_faults_never_finalize() {
    for stage in [
        RecordingStage::Seek,
        RecordingStage::Patch,
        RecordingStage::FlushWire,
        RecordingStage::FlushWave,
        RecordingStage::FlushEvents,
    ] {
        let wire = Shared::default();
        let wave = Shared::default();
        let events = Shared::default();
        let recording =
            Recording::new(id(), wire.clone(), wave.clone(), events.clone()).expect("new");
        match stage {
            RecordingStage::Seek => *wave.fail_seek.lock().expect("fault") = true,
            RecordingStage::Patch => *wave.fail_write.lock().expect("fault") = true,
            RecordingStage::FlushWire => *wire.fail_flush.lock().expect("fault") = true,
            RecordingStage::FlushWave => *wave.fail_flush.lock().expect("fault") = true,
            RecordingStage::FlushEvents => *events.fail_flush.lock().expect("fault") = true,
            RecordingStage::Validate
            | RecordingStage::Header
            | RecordingStage::Wire
            | RecordingStage::Audio
            | RecordingStage::Events => unreachable!("finish stage"),
        }
        let error = recording.finish().expect_err("finish must fail");
        assert_eq!(error.stage(), stage);
        assert_eq!(error.summary().header_bytes, 44);
    }
}

#[test]
fn accepted_prefixes_then_io_error_and_interrupted_short_writes_are_exact() {
    let header = Shared::default();
    *header.allowance.lock().expect("allowance") = Some(1);
    *header.fail_when_exhausted.lock().expect("fault") = true;
    let Err(error) = Recording::new(id(), Shared::default(), header.clone(), Shared::default())
    else {
        panic!("header failure")
    };
    assert_eq!(error.stage(), RecordingStage::Header);
    assert_eq!(error.summary().header_bytes, 1);
    assert_eq!(header.bytes().len(), 1);

    let wire = Shared::default();
    *wire.limit.lock().expect("chunk") = Some(1);
    *wire.interrupt_once.lock().expect("interrupt") = true;
    let mut recording =
        Recording::new(id(), wire.clone(), Shared::default(), Shared::default()).expect("new");
    recording.record(&connected()).expect("connected");
    recording
        .record(&record(
            0,
            RecordKind::Wire {
                bytes: owned(&[1, 2]),
            },
        ))
        .expect("interrupted short wire recovers");
    assert_eq!(wire.bytes(), vec![1, 2]);

    let wave = Shared::default();
    let mut recording =
        Recording::new(id(), Shared::default(), wave.clone(), Shared::default()).expect("new");
    recording.record(&connected()).expect("connected");
    recording
        .record(&record(0, RecordKind::Started { uuid: uuid() }))
        .expect("started");
    *wave.allowance.lock().expect("allowance") = Some(1);
    *wave.fail_when_exhausted.lock().expect("fault") = true;
    let error = recording
        .record(&record(
            0,
            RecordKind::Audio {
                uuid: uuid(),
                rate: SampleRate::Khz8,
                bytes: owned(&[0, 0]),
            },
        ))
        .expect_err("audio prefix error");
    assert_eq!(error.stage(), RecordingStage::Audio);
    assert_eq!(error.summary().audio_bytes, 1);

    let events = Shared::default();
    let mut recording =
        Recording::new(id(), Shared::default(), Shared::default(), events.clone()).expect("new");
    *events.allowance.lock().expect("allowance") = Some(1);
    *events.fail_when_exhausted.lock().expect("fault") = true;
    let error = recording
        .record(&connected())
        .expect_err("event prefix error");
    assert_eq!(error.stage(), RecordingStage::Events);
    assert_eq!(error.summary().event_bytes, 1);

    let wave = Shared::default();
    let recording =
        Recording::new(id(), Shared::default(), wave.clone(), Shared::default()).expect("new");
    *wave.allowance.lock().expect("allowance") = Some(1);
    *wave.fail_when_exhausted.lock().expect("fault") = true;
    let error = recording.finish().expect_err("patch prefix error");
    assert_eq!(error.stage(), RecordingStage::Patch);
    assert_eq!(error.summary().patch_bytes, 1);
}
