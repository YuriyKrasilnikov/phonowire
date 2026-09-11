//! Literal consumer witnesses across framing and incoming-session boundaries.
use phonowire_audiosocket::{
    DecodeOutcome, Decoder, FinishError, IncomingEvent, IncomingSession, IncomingSessionError,
    SessionEnd, Uuid,
};

const UUID: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
const STREAM: [u8; 33] = [
    0x01, 0x00, 0x10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0x10, 0x00, 0x04, 0x00,
    0x80, 0xff, 0x7f, 0x03, 0x00, 0x01, b'5', 0x00, 0x00, 0x00,
];

#[test]
fn literal_frames_keep_the_same_session_events_across_chunk_schedules() {
    for boundaries in [
        &[33][..],
        &[1, 3, 19, 21, 26, 30, 33][..],
        &[2, 11, 20, 25, 29, 32, 33][..],
    ] {
        assert_literal_stream(boundaries);
    }
}

fn assert_literal_stream(boundaries: &[usize]) {
    let mut scratch = [0_u8; 16];
    let mut decoder = Decoder::new(&mut scratch);
    let mut session = IncomingSession::new();
    let mut previous = 0;
    let mut event_index = 0;

    for &boundary in boundaries {
        let mut input = &STREAM[previous..boundary];
        previous = boundary;
        while !input.is_empty() {
            match decoder.feed(&mut input) {
                Ok(DecodeOutcome::NeedInput) => assert!(input.is_empty()),
                Ok(DecodeOutcome::Frame(message)) => {
                    let event = match session.receive(message) {
                        Ok(value) => value,
                        Err(error) => panic!("literal session message rejected: {error:?}"),
                    };
                    assert_literal_event(event, event_index);
                    event_index += 1;
                }
                Err(error) => panic!("literal decoder rejected stream: {error:?}"),
            }
        }
    }

    assert_eq!(event_index, 4);
    assert_eq!(decoder.finish(), Ok(()));
    assert_eq!(session.end_of_input(), Err(IncomingSessionError::AfterEnd));
}

fn assert_literal_event(event: IncomingEvent<'_>, event_index: usize) {
    match (event_index, event) {
        (0, IncomingEvent::Started(uuid))
        | (
            3,
            IncomingEvent::Ended {
                uuid: Some(uuid),
                reason: SessionEnd::Terminate,
            },
        ) => assert_eq!(uuid.bytes(), UUID),
        (1, IncomingEvent::Audio { uuid, payload, .. }) => {
            assert_eq!(uuid.bytes(), UUID);
            assert_eq!(payload.bytes(), &[0x00, 0x80, 0xff, 0x7f]);
        }
        (2, IncomingEvent::Dtmf { uuid, digit }) => {
            assert_eq!(uuid.bytes(), UUID);
            assert_eq!(digit.value(), b'5');
        }
        (_, other) => panic!("unexpected literal event at index {event_index}: {other:?}"),
    }
}

#[test]
fn clean_eof_is_reported_only_after_a_successful_decoder_finish() {
    let bytes = [
        0x01, 0x00, 0x10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    ];
    let mut scratch = [0_u8; 16];
    let mut decoder = Decoder::new(&mut scratch);
    let mut input = bytes.as_slice();
    match decoder.feed(&mut input) {
        Ok(DecodeOutcome::Frame(message)) => {
            let mut session = IncomingSession::new();
            assert_eq!(
                session.receive(message),
                Ok(IncomingEvent::Started(Uuid::new(UUID)))
            );
            assert_eq!(decoder.finish(), Ok(()));
            assert_eq!(
                session.end_of_input(),
                Ok(IncomingEvent::Ended {
                    uuid: Some(Uuid::new(UUID)),
                    reason: SessionEnd::EndOfInput,
                })
            );
        }
        other => panic!("complete UUID did not decode: {other:?}"),
    }
    assert!(input.is_empty());
}

#[test]
fn truncated_audio_after_an_accepted_uuid_remains_a_decoder_finish_error() {
    let bytes = [
        0x01, 0x00, 0x10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0x10, 0x00, 0x04,
        0x00, 0x80, 0xff,
    ];
    let mut scratch = [0_u8; 16];
    let mut decoder = Decoder::new(&mut scratch);
    let mut session = IncomingSession::new();
    let mut input = bytes.as_slice();

    match decoder.feed(&mut input) {
        Ok(DecodeOutcome::Frame(message)) => {
            assert_eq!(
                session.receive(message),
                Ok(IncomingEvent::Started(Uuid::new(UUID)))
            );
        }
        other => panic!("complete UUID did not decode: {other:?}"),
    }
    assert_eq!(decoder.feed(&mut input), Ok(DecodeOutcome::NeedInput));
    assert!(input.is_empty());
    assert_eq!(
        decoder.finish(),
        Err(FinishError::TruncatedPayload {
            expected: 4,
            received: 3,
        })
    );
    assert_eq!(session.identity(), Some(Uuid::new(UUID)));
}

#[test]
fn decoded_pre_uuid_media_is_a_session_error_without_changing_framing() {
    let bytes = [
        0x10, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, b'5',
    ];
    let mut scratch = [0_u8; 2];
    let mut decoder = Decoder::new(&mut scratch);
    let mut session = IncomingSession::new();
    let mut input = bytes.as_slice();

    match decoder.feed(&mut input) {
        Ok(DecodeOutcome::Frame(message)) => {
            assert_eq!(
                session.receive(message),
                Err(IncomingSessionError::MissingUuid)
            );
        }
        other => panic!("pre-UUID audio did not decode: {other:?}"),
    }
    match decoder.feed(&mut input) {
        Ok(DecodeOutcome::Frame(phonowire_audiosocket::TypedMessage::Terminate)) => {
            assert_eq!(
                session.receive(phonowire_audiosocket::TypedMessage::Terminate),
                Err(IncomingSessionError::AfterEnd)
            );
        }
        other => panic!("terminate did not decode after media: {other:?}"),
    }
    match decoder.feed(&mut input) {
        Ok(DecodeOutcome::Frame(message)) => {
            assert_eq!(
                session.receive(message),
                Err(IncomingSessionError::AfterEnd)
            );
        }
        other => panic!("DTMF did not decode after terminate: {other:?}"),
    }
    assert!(input.is_empty());
    assert_eq!(decoder.finish(), Ok(()));
}

#[test]
fn decoded_messages_after_terminate_are_session_after_end_errors() {
    let bytes = [0x00, 0x00, 0x00, 0x03, 0x00, 0x01, b'5'];
    let mut scratch = [0_u8; 1];
    let mut decoder = Decoder::new(&mut scratch);
    let mut session = IncomingSession::new();
    let mut input = bytes.as_slice();

    match decoder.feed(&mut input) {
        Ok(DecodeOutcome::Frame(message)) => {
            assert_eq!(
                session.receive(message),
                Ok(IncomingEvent::Ended {
                    uuid: None,
                    reason: SessionEnd::Terminate,
                })
            );
        }
        other => panic!("terminate did not decode: {other:?}"),
    }
    match decoder.feed(&mut input) {
        Ok(DecodeOutcome::Frame(message)) => {
            assert_eq!(
                session.receive(message),
                Err(IncomingSessionError::AfterEnd)
            );
        }
        other => panic!("message after terminate did not decode: {other:?}"),
    }
    assert!(input.is_empty());
    assert_eq!(decoder.finish(), Ok(()));
}
