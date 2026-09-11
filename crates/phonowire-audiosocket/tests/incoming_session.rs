//! Black-box AP1 incoming-session behavior witnesses.
use phonowire_audiosocket::{
    IncomingEvent, IncomingProfile, IncomingSession, IncomingSessionError, RawEnvelope, SampleRate,
    SessionEnd, TypedMessage, Uuid, WireType,
};

const UUID_ONE: [u8; 16] = [1; 16];
const UUID_TWO: [u8; 16] = [2; 16];
const NIL_UUID: [u8; 16] = [0; 16];

fn typed(kind: u8, payload: &[u8]) -> TypedMessage<'_> {
    let envelope = match RawEnvelope::new(WireType::new(kind), payload) {
        Ok(value) => value,
        Err(error) => panic!("literal message was not representable: {error:?}"),
    };
    match envelope.typed() {
        Ok(value) => value,
        Err(error) => panic!("literal message was not structurally valid: {error:?}"),
    }
}

fn started(session: &mut IncomingSession, uuid: [u8; 16]) {
    assert_eq!(
        session.receive(typed(0x01, &uuid)),
        Ok(IncomingEvent::Started(Uuid::new(uuid)))
    );
}

fn assert_after_end(session: &mut IncomingSession) {
    assert_eq!(
        session.receive(typed(0x01, &UUID_TWO)),
        Err(IncomingSessionError::AfterEnd)
    );
    assert_eq!(
        session.receive(typed(0x10, &[0, 0])),
        Err(IncomingSessionError::AfterEnd)
    );
    assert_eq!(
        session.receive(typed(0x03, b"0")),
        Err(IncomingSessionError::AfterEnd)
    );
    assert_eq!(
        session.receive(typed(0x00, &[])),
        Err(IncomingSessionError::AfterEnd)
    );
    assert_eq!(
        session.receive(typed(0xff, &[0, 4, 255])),
        Err(IncomingSessionError::AfterEnd)
    );
    assert_eq!(
        session.receive(typed(0x02, &[202, 254])),
        Err(IncomingSessionError::AfterEnd)
    );
    assert_eq!(session.end_of_input(), Err(IncomingSessionError::AfterEnd));
}

#[test]
fn uuid_establishes_identity_once_and_nil_is_valid() {
    let mut session = IncomingSession::new();
    assert_eq!(session.identity(), None);
    started(&mut session, NIL_UUID);
    assert_eq!(session.identity(), Some(Uuid::new(NIL_UUID)));
    assert_eq!(
        session.receive(typed(0x01, &UUID_TWO)),
        Err(IncomingSessionError::DuplicateUuid)
    );
    assert_after_end(&mut session);
}

#[test]
fn media_before_uuid_has_priority_over_rate_and_digit_policy() {
    for rate in [
        (0x10, SampleRate::Khz8),
        (0x11, SampleRate::Khz12),
        (0x12, SampleRate::Khz16),
        (0x13, SampleRate::Khz24),
        (0x14, SampleRate::Khz32),
        (0x15, SampleRate::Khz44_1),
        (0x16, SampleRate::Khz48),
        (0x17, SampleRate::Khz96),
        (0x18, SampleRate::Khz192),
    ] {
        let mut session = IncomingSession::new();
        assert_eq!(
            session.receive(typed(rate.0, &[])),
            Err(IncomingSessionError::MissingUuid)
        );
        assert_after_end(&mut session);
    }
    for digit in 0_u8..=127 {
        let mut session = IncomingSession::new();
        assert_eq!(
            session.receive(typed(0x03, &[digit])),
            Err(IncomingSessionError::MissingUuid)
        );
        assert_after_end(&mut session);
    }
}

#[test]
fn explicit_8khz_profile_accepts_only_8khz_and_preserves_empty_payload() {
    let payload = [0, 128, 255, 127];
    let mut accepted = IncomingSession::with_profile(IncomingProfile::PCM_8_KHZ);
    started(&mut accepted, UUID_ONE);
    match accepted.receive(typed(0x10, &payload)) {
        Ok(IncomingEvent::Audio {
            uuid,
            payload: observed,
            ..
        }) => {
            assert_eq!(uuid, Uuid::new(UUID_ONE));
            assert_eq!(observed.bytes(), payload);
        }
        other => panic!("8 kHz audio produced {other:?}"),
    }
    match accepted.receive(typed(0x10, &[])) {
        Ok(IncomingEvent::Audio { payload, .. }) => assert!(payload.bytes().is_empty()),
        other => panic!("empty 8 kHz audio produced {other:?}"),
    }

    for (tag, rate) in [
        (0x11, SampleRate::Khz12),
        (0x12, SampleRate::Khz16),
        (0x13, SampleRate::Khz24),
        (0x14, SampleRate::Khz32),
        (0x15, SampleRate::Khz44_1),
        (0x16, SampleRate::Khz48),
        (0x17, SampleRate::Khz96),
        (0x18, SampleRate::Khz192),
    ] {
        let mut session = IncomingSession::with_profile(IncomingProfile::PCM_8_KHZ);
        started(&mut session, UUID_ONE);
        assert_eq!(
            session.receive(typed(tag, &[])),
            Err(IncomingSessionError::UnsupportedRate(rate))
        );
        assert_after_end(&mut session);
    }
}

#[test]
fn every_ascii_dtmf_byte_has_the_selected_after_uuid_outcome() {
    for digit in 0_u8..=127 {
        let mut session = IncomingSession::new();
        started(&mut session, UUID_ONE);
        let accepted = matches!(digit, b'0'..=b'9' | b'*' | b'#' | b'A'..=b'D');
        match session.receive(typed(0x03, &[digit])) {
            Ok(IncomingEvent::Dtmf {
                uuid,
                digit: observed,
            }) if accepted => {
                assert_eq!(uuid, Uuid::new(UUID_ONE));
                assert_eq!(observed.value(), digit);
            }
            Err(IncomingSessionError::UnsupportedDigit(observed)) if !accepted => {
                assert_eq!(observed.value(), digit);
                assert_after_end(&mut session);
            }
            other => panic!("ASCII DTMF {digit} produced {other:?}"),
        }
    }
}

#[test]
fn unknown_type_is_terminal_and_preserves_its_explicit_rejection() {
    let mut session = IncomingSession::new();
    let unknown = match phonowire_audiosocket::UnknownWireType::new(0x02) {
        Ok(value) => value,
        Err(error) => panic!("0x02 must be unknown: {error:?}"),
    };
    assert_eq!(
        session.receive(typed(0x02, &[202, 254])),
        Err(IncomingSessionError::UnsupportedType(unknown)),
    );
    assert_after_end(&mut session);
}

#[test]
fn end_reasons_are_distinct_before_and_after_uuid_and_absorb_later_input() {
    let peer_payload = [0, 4, 255];
    for with_uuid in [false, true] {
        let mut terminated = IncomingSession::new();
        if with_uuid {
            started(&mut terminated, UUID_ONE);
        }
        assert_eq!(
            terminated.receive(typed(0x00, &[])),
            Ok(IncomingEvent::Ended {
                uuid: if with_uuid {
                    Some(Uuid::new(UUID_ONE))
                } else {
                    None
                },
                reason: SessionEnd::Terminate,
            }),
        );
        assert_after_end(&mut terminated);

        let mut peer_error = IncomingSession::new();
        if with_uuid {
            started(&mut peer_error, UUID_ONE);
        }
        match peer_error.receive(typed(0xff, &peer_payload)) {
            Ok(IncomingEvent::Ended {
                uuid,
                reason: SessionEnd::PeerError(payload),
            }) => {
                assert_eq!(
                    uuid,
                    if with_uuid {
                        Some(Uuid::new(UUID_ONE))
                    } else {
                        None
                    }
                );
                assert_eq!(payload.bytes(), peer_payload);
            }
            other => panic!("peer error produced {other:?}"),
        }
        assert_after_end(&mut peer_error);

        let mut end_of_input = IncomingSession::new();
        if with_uuid {
            started(&mut end_of_input, UUID_ONE);
        }
        assert_eq!(
            end_of_input.end_of_input(),
            Ok(IncomingEvent::Ended {
                uuid: if with_uuid {
                    Some(Uuid::new(UUID_ONE))
                } else {
                    None
                },
                reason: SessionEnd::EndOfInput,
            }),
        );
        assert_after_end(&mut end_of_input);
    }
}

#[test]
fn explicit_profile_preserves_the_declared_16khz_rate() {
    let mut session =
        IncomingSession::with_profile(IncomingProfile::from_rates(&[SampleRate::Khz16]));
    started(&mut session, UUID_ONE);
    match session.receive(typed(0x12, &[0, 0])) {
        Ok(IncomingEvent::Audio {
            uuid,
            rate,
            payload,
        }) => {
            assert_eq!(uuid, Uuid::new(UUID_ONE));
            assert_eq!(rate, SampleRate::Khz16);
            assert_eq!(payload.bytes(), [0, 0]);
        }
        other => panic!("16 kHz profile result: {other:?}"),
    }
}

#[test]
fn default_profile_preserves_every_documented_wire_rate() {
    let rates = [
        (0x10, SampleRate::Khz8),
        (0x11, SampleRate::Khz12),
        (0x12, SampleRate::Khz16),
        (0x13, SampleRate::Khz24),
        (0x14, SampleRate::Khz32),
        (0x15, SampleRate::Khz44_1),
        (0x16, SampleRate::Khz48),
        (0x17, SampleRate::Khz96),
        (0x18, SampleRate::Khz192),
    ];
    for (wire_type, rate) in rates {
        let mut session = IncomingSession::new();
        started(&mut session, UUID_ONE);
        assert!(matches!(
            session.receive(typed(wire_type, &[0, 0])),
            Ok(IncomingEvent::Audio { rate: actual, payload, .. })
                if actual == rate && payload.bytes() == [0, 0]
        ));
    }
}
