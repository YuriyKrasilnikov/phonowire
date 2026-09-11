//! Format and lifecycle refusal through caller-visible recording outcomes.
use super::*;
use crate::{ByteBudget, OwnedBytes, WireOffset};
use std::io::Cursor;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::time::Instant;

fn identity() -> ConnectionId {
    ConnectionId::new(1, 2)
}
fn uuid() -> Uuid {
    Uuid::new([1; 16])
}
fn payload(bytes: &[u8]) -> OwnedBytes {
    ByteBudget::new(NonZeroUsize::new(64).expect("finite payload capacity"))
        .try_copy(bytes)
        .expect("fixture fits")
}
fn record(offset: u64, kind: RecordKind) -> Record {
    Record {
        connection: identity(),
        offset: WireOffset::new(offset),
        observed_at: Instant::now(),
        kind,
    }
}
fn connect() -> Record {
    record(
        0,
        RecordKind::Connected {
            peer: SocketAddr::from((Ipv4Addr::LOCALHOST, 9)),
        },
    )
}
fn wire() -> Record {
    record(
        0,
        RecordKind::Wire {
            bytes: payload(&[0; 64]),
        },
    )
}

#[test]
fn malformed_records_fail_before_mutating_any_output() {
    let mut foreign = record(
        19,
        RecordKind::Dtmf {
            uuid: uuid(),
            digit: phonowire_audiosocket::Dtmf::new(b'1').expect("ASCII"),
        },
    );
    foreign.connection = ConnectionId::new(1, 3);
    let cases = [
        (RecordingViolation::Connection, foreign),
        (
            RecordingViolation::Order,
            record(
                19,
                RecordKind::Connected {
                    peer: SocketAddr::from((Ipv4Addr::LOCALHOST, 9)),
                },
            ),
        ),
        (
            RecordingViolation::Order,
            record(19, RecordKind::Started { uuid: uuid() }),
        ),
        (
            RecordingViolation::Offset,
            record(
                63,
                RecordKind::Wire {
                    bytes: payload(&[1]),
                },
            ),
        ),
        (
            RecordingViolation::Offset,
            record(
                65,
                RecordKind::Audio {
                    uuid: uuid(),
                    rate: SampleRate::Khz8,
                    bytes: payload(&[1, 2]),
                },
            ),
        ),
        (
            RecordingViolation::Uuid,
            record(
                19,
                RecordKind::Audio {
                    uuid: Uuid::new([2; 16]),
                    rate: SampleRate::Khz8,
                    bytes: payload(&[1, 2]),
                },
            ),
        ),
        (
            RecordingViolation::AudioFormat,
            record(
                19,
                RecordKind::Audio {
                    uuid: uuid(),
                    rate: SampleRate::Khz16,
                    bytes: payload(&[1, 2]),
                },
            ),
        ),
        (
            RecordingViolation::AudioFormat,
            record(
                19,
                RecordKind::Audio {
                    uuid: uuid(),
                    rate: SampleRate::Khz8,
                    bytes: payload(&[1]),
                },
            ),
        ),
        (
            RecordingViolation::Uuid,
            record(
                19,
                RecordKind::Ended {
                    uuid: None,
                    reason: EndReason::CleanEof,
                },
            ),
        ),
    ];
    for (violation, invalid) in cases {
        assert_refused_without_output(violation, &invalid);
    }
}

fn assert_refused_without_output(violation: RecordingViolation, invalid: &Record) {
    let mut wire_output = Vec::new();
    let mut wave_output = Cursor::new(Vec::new());
    let mut events = Vec::new();
    let mut recording = Recording::new(identity(), &mut wire_output, &mut wave_output, &mut events)
        .expect("header");
    recording.record(&connect()).expect("connection");
    recording.record(&wire()).expect("raw prefix");
    recording
        .record(&record(19, RecordKind::Started { uuid: uuid() }))
        .expect("UUID");
    let before = recording.summary;
    let error = recording.record(invalid).expect_err("invalid record");
    assert_eq!(error.stage(), RecordingStage::Validate);
    assert!(matches!(error.failure(), RecordingFailure::Invalid(value) if *value == violation));
    assert_eq!(error.summary(), &before);
    let failed = recording
        .finish()
        .expect_err("failed adapter cannot finalize");
    assert!(matches!(
        failed.failure(),
        RecordingFailure::Invalid(RecordingViolation::AfterFailure)
    ));
    assert_eq!(failed.summary(), &before);
    assert_eq!(wire_output, [0; 64]);
    assert_eq!(wave_output.get_ref().len(), 44);
    assert_eq!(
        u64::try_from(events.len()).expect("small output"),
        before.event_bytes
    );
}

#[test]
fn terminal_class_survives_finalization_and_prohibits_more_input() {
    let mut recording = Recording::new(identity(), Vec::new(), Cursor::new(Vec::new()), Vec::new())
        .expect("header");
    recording.record(&connect()).expect("connect");
    recording
        .record(&record(
            0,
            RecordKind::Ended {
                uuid: None,
                reason: EndReason::Terminate,
            },
        ))
        .expect("pre-UUID terminal");
    let summary = recording.finish().expect("finalization");
    assert_eq!(summary.end, RecordingEnd::Observed(TerminalKind::Terminate));
    assert_eq!(summary.header_bytes, 44);
    assert_eq!(summary.patch_bytes, 44);

    let mut recording = Recording::new(identity(), Vec::new(), Cursor::new(Vec::new()), Vec::new())
        .expect("header");
    recording.record(&connect()).expect("connect");
    recording
        .record(&record(
            0,
            RecordKind::Ended {
                uuid: None,
                reason: EndReason::CleanEof,
            },
        ))
        .expect("EOF");
    let error = recording
        .record(&wire())
        .expect_err("no post-terminal wire");
    assert!(matches!(
        error.failure(),
        RecordingFailure::Invalid(RecordingViolation::AfterEnd)
    ));
    assert_eq!(error.summary().wire_bytes, 0);
    assert_eq!(
        error.summary().end,
        RecordingEnd::Observed(TerminalKind::CleanEof)
    );
}

#[test]
fn audio_requires_an_established_uuid_even_with_valid_samples() {
    let mut recording = Recording::new(identity(), Vec::new(), Cursor::new(Vec::new()), Vec::new())
        .expect("header");
    recording.record(&connect()).expect("connect");
    recording.record(&wire()).expect("raw prefix");
    let error = recording
        .record(&record(
            19,
            RecordKind::Audio {
                uuid: uuid(),
                rate: SampleRate::Khz8,
                bytes: payload(&[1, 2]),
            },
        ))
        .expect_err("missing UUID");
    assert!(matches!(
        error.failure(),
        RecordingFailure::Invalid(RecordingViolation::Uuid)
    ));
    assert_eq!(error.summary().audio_bytes, 0);
}

#[test]
fn riff_sizes_refuse_the_first_unrepresentable_even_pcm_length() {
    const MAX_EVEN_PCM: u64 = 0xffff_ffda;
    let header = wave_header(MAX_EVEN_PCM, SampleRate::Khz8).expect("largest even PCM fits RIFF");
    assert_eq!(&header[4..8], &[0xfe, 0xff, 0xff, 0xff]);
    assert_eq!(&header[40..44], &[0xda, 0xff, 0xff, 0xff]);
    assert_eq!(
        wave_header(MAX_EVEN_PCM + 2, SampleRate::Khz8),
        Err(RecordingViolation::WaveSize)
    );
    assert_eq!(
        wave_header(u64::from(u32::MAX) + 1, SampleRate::Khz8),
        Err(RecordingViolation::WaveSize)
    );
}
