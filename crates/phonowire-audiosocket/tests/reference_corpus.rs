//! Independent literal-byte reference corpus for the `AudioSocket` wire model.
use phonowire_audiosocket::{
    OpaquePayload, RawEnvelope, SampleRate, TypedMessage, TypedMessageError, UnknownWireType,
    WireType,
};

static TOO_LONG: [u8; 65_536] = [0; 65_536];

fn raw(kind: u8, payload: &[u8]) -> RawEnvelope<'_> {
    match RawEnvelope::new(WireType::new(kind), payload) {
        Ok(value) => value,
        Err(error) => panic!("unexpected literal envelope error: {error:?}"),
    }
}

#[test]
fn literal_numeric_type_matrix() {
    assert_eq!(raw(0x00, &[]).typed(), Ok(TypedMessage::Terminate));
    match raw(
        0x01,
        &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    )
    .typed()
    {
        Ok(TypedMessage::Uuid(value)) => assert_eq!(
            value.bytes(),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        ),
        other => panic!("unexpected UUID result: {other:?}"),
    }
    match raw(0x10, &[0x00, 0x80, 0xff, 0x7f]).typed() {
        Ok(TypedMessage::Audio { rate, payload }) => {
            assert_eq!(rate, SampleRate::Khz8);
            assert_eq!(payload.bytes(), &[0x00, 0x80, 0xff, 0x7f]);
        }
        other => panic!("unexpected PCM result: {other:?}"),
    }
    match raw(0x18, &[]).typed() {
        Ok(TypedMessage::Audio { rate, payload }) => {
            assert_eq!(rate, SampleRate::Khz192);
            assert_eq!(payload.bytes(), &[]);
        }
        other => panic!("unexpected PCM result: {other:?}"),
    }
    for (tag, expected_rate, expected_hertz) in [
        (0x10, SampleRate::Khz8, 8_000),
        (0x11, SampleRate::Khz12, 12_000),
        (0x12, SampleRate::Khz16, 16_000),
        (0x13, SampleRate::Khz24, 24_000),
        (0x14, SampleRate::Khz32, 32_000),
        (0x15, SampleRate::Khz44_1, 44_100),
        (0x16, SampleRate::Khz48, 48_000),
        (0x17, SampleRate::Khz96, 96_000),
        (0x18, SampleRate::Khz192, 192_000),
    ] {
        match raw(tag, &[]).typed() {
            Ok(TypedMessage::Audio { rate, .. }) => {
                assert_eq!(rate, expected_rate);
                assert_eq!(rate.hertz(), expected_hertz);
            }
            other => panic!("tag {tag:02x} produced {other:?}"),
        }
    }
}

#[test]
fn strict_policies_leave_raw_diagnostics_available() {
    let malformed = raw(0x01, &[0; 15]);
    assert_eq!(malformed.typed(), Err(TypedMessageError::UuidLength));
    assert_eq!(malformed.payload(), &[0; 15]);
    assert_eq!(raw(0x03, b"ab").typed(), Err(TypedMessageError::DtmfLength));
    assert_eq!(
        raw(0x03, &[0x80]).typed(),
        Err(TypedMessageError::DtmfNotAscii)
    );
    assert_eq!(
        raw(0x10, &[1]).typed(),
        Err(TypedMessageError::OddPcmLength)
    );
    assert_eq!(
        raw(0x00, &[1]).typed(),
        Err(TypedMessageError::TerminatePayload)
    );
}

#[test]
fn opaque_values_are_preserved_and_known_types_cannot_be_unknown() {
    let payload = [0xde, 0xad];
    match raw(0xff, &payload).typed() {
        Ok(TypedMessage::Error(observed)) => assert_eq!(observed.bytes(), &payload),
        other => panic!("unexpected error result: {other:?}"),
    }
    match raw(0x02, &payload).typed() {
        Ok(TypedMessage::Unknown {
            wire_type,
            payload: observed,
        }) => {
            assert_eq!(wire_type.value(), 0x02);
            assert_eq!(observed.bytes(), &payload);
        }
        other => panic!("unexpected unknown result: {other:?}"),
    }
    assert!(UnknownWireType::new(0x10).is_err());
}

#[test]
fn opaque_typed_payloads_enforce_the_wire_limit() {
    static MAX: [u8; 65_535] = [0; 65_535];
    assert!(OpaquePayload::new(&MAX).is_ok());
    assert!(OpaquePayload::new(&TOO_LONG).is_err());
}

#[test]
fn raw_envelope_does_not_accept_overlong_payload() {
    assert!(RawEnvelope::new(WireType::new(0x02), &TOO_LONG).is_err());
}
