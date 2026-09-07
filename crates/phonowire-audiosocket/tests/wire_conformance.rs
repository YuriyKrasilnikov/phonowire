//! Literal regression witnesses for the public typed codec boundary.
use phonowire_audiosocket::{
    AudioPayload, AudioPayloadError, DecodeError, DecodeOutcome, Decoder, Dtmf, EncodeError,
    FinishError, OpaquePayload, RawDecoder, RawEnvelope, SampleRate, TypedMessage, UnknownWireType,
    WireType, encode,
};

fn dtmf(value: u8) -> Dtmf {
    match Dtmf::new(value) {
        Ok(value) => value,
        Err(error) => panic!("literal ASCII DTMF rejected: {error:?}"),
    }
}

#[test]
fn literal_known_tag_partition_preserves_the_typed_unknown_complement() {
    let known = [
        0x00_u8, 0x01, 0x03, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0xff,
    ];
    for tag in known {
        assert!(UnknownWireType::new(tag).is_err());
    }
    for tag in 0_u8..=u8::MAX {
        if known.contains(&tag) {
            continue;
        }
        let payload = [0xca, 0xfe];
        let raw = match RawEnvelope::new(WireType::new(tag), &payload) {
            Ok(value) => value,
            Err(error) => panic!("literal unknown envelope rejected: {error:?}"),
        };
        match raw.typed() {
            Ok(TypedMessage::Unknown {
                wire_type,
                payload: body,
            }) => {
                assert_eq!(wire_type.value(), tag);
                assert_eq!(body.bytes(), &payload);
            }
            other => panic!("literal unassigned tag {tag:02x}: {other:?}"),
        }
    }

    let uuid = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    match literal_typed(0x00, &[]) {
        TypedMessage::Terminate => {}
        other => panic!("terminate: {other:?}"),
    }
    match literal_typed(0x01, &uuid) {
        TypedMessage::Uuid(value) => assert_eq!(value.bytes(), uuid),
        other => panic!("UUID: {other:?}"),
    }
    match literal_typed(0x03, b"5") {
        TypedMessage::Dtmf(value) => assert_eq!(value.value(), b'5'),
        other => panic!("DTMF: {other:?}"),
    }
    for (tag, rate) in [
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
        match literal_typed(tag, &[]) {
            TypedMessage::Audio {
                rate: actual,
                payload,
            } => {
                assert_eq!(actual, rate);
                assert!(payload.bytes().is_empty());
            }
            other => panic!("PCM tag {tag:02x}: {other:?}"),
        }
    }
    match literal_typed(0xff, &[0xca, 0xfe]) {
        TypedMessage::Error(payload) => assert_eq!(payload.bytes(), &[0xca, 0xfe]),
        other => panic!("error: {other:?}"),
    }
}

fn literal_typed(tag: u8, body: &[u8]) -> TypedMessage<'_> {
    let raw = match RawEnvelope::new(WireType::new(tag), body) {
        Ok(value) => value,
        Err(error) => panic!("literal known envelope rejected: {error:?}"),
    };
    match raw.typed() {
        Ok(value) => value,
        Err(error) => panic!("literal known typed conversion rejected: {error:?}"),
    }
}

fn pcm(bytes: &[u8]) -> AudioPayload<'_> {
    match AudioPayload::new(bytes) {
        Ok(value) => value,
        Err(error) => panic!("literal PCM rejected: {error:?}"),
    }
}

fn reference_frame(bytes: &[u8]) -> Option<(u8, usize, &[u8])> {
    if bytes.len() < 3 {
        return None;
    }
    let length = usize::from(bytes[1]) * 256 + usize::from(bytes[2]);
    let complete = 3 + length;
    if bytes.len() < complete {
        return None;
    }
    Some((bytes[0], length, &bytes[3..complete]))
}

#[test]
fn independent_literal_parser_checks_deterministic_chunked_raw_corpus() {
    let mut seed = 0x31_u8;
    for _ in 0..128 {
        seed = seed.wrapping_mul(17).wrapping_add(29);
        let tag = seed;
        let length = usize::from(seed & 7);
        let mut bytes = [0_u8; 10];
        bytes[0] = tag;
        bytes[1] = 0;
        bytes[2] = u8::try_from(length).expect("bounded corpus length fits u8");
        for index in 0..length {
            seed = seed.wrapping_mul(17).wrapping_add(29);
            bytes[3 + index] = seed;
        }
        let frame = &bytes[..3 + length];
        let Some((expected_tag, expected_length, expected_body)) = reference_frame(frame) else {
            panic!("independent parser rejected complete literal frame");
        };
        let mut scratch = [0_u8; 7];
        let mut decoder = RawDecoder::new(&mut scratch);
        let split = core::cmp::min(2, frame.len());
        let mut first = &frame[..split];
        assert_eq!(decoder.feed(&mut first), Ok(DecodeOutcome::NeedInput));
        let mut rest = &frame[split..];
        match decoder.feed(&mut rest) {
            Ok(DecodeOutcome::Frame(observed)) => {
                assert_eq!(observed.wire_type().value(), expected_tag);
                assert_eq!(observed.payload().len(), expected_length);
                assert_eq!(observed.payload(), expected_body);
            }
            other => panic!("chunked independent corpus result: {other:?}"),
        }
        assert!(rest.is_empty());
    }
}

#[test]
fn typed_constructors_enforce_the_wire_domains() {
    assert_eq!(
        Dtmf::new(0x80),
        Err(phonowire_audiosocket::TypedMessageError::DtmfNotAscii)
    );
    assert_eq!(AudioPayload::new(&[0]), Err(AudioPayloadError::OddLength));
    let maximum_even = std::vec![0_u8; 65_534];
    assert!(AudioPayload::new(&maximum_even).is_ok());
    let maximum_odd = std::vec![0_u8; 65_535];
    assert_eq!(
        AudioPayload::new(&maximum_odd),
        Err(AudioPayloadError::OddLength)
    );
    let overlong_even = std::vec![0_u8; 65_536];
    assert_eq!(
        AudioPayload::new(&overlong_even),
        Err(AudioPayloadError::PayloadTooLong)
    );
}

#[test]
fn literal_encoder_vectors_cover_each_typed_variant_and_rate() {
    let uuid =
        phonowire_audiosocket::Uuid::new([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let opaque = match OpaquePayload::new(&[0xca, 0xfe]) {
        Ok(value) => value,
        Err(error) => panic!("opaque failed: {error:?}"),
    };
    let unknown = match UnknownWireType::new(0x02) {
        Ok(value) => value,
        Err(error) => panic!("unknown failed: {error:?}"),
    };
    let vectors = [
        (TypedMessage::Terminate, &[0x00, 0x00, 0x00][..]),
        (
            TypedMessage::Uuid(uuid),
            &[
                0x01, 0x00, 0x10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            ],
        ),
        (TypedMessage::Dtmf(dtmf(b'5')), &[0x03, 0x00, 0x01, b'5']),
        (TypedMessage::Error(opaque), &[0xff, 0x00, 0x02, 0xca, 0xfe]),
        (
            TypedMessage::Unknown {
                wire_type: unknown,
                payload: opaque,
            },
            &[0x02, 0x00, 0x02, 0xca, 0xfe],
        ),
    ];
    for (message, expected) in vectors {
        let mut output = [0x5a_u8; 32];
        assert_eq!(encode(message, &mut output), Ok(expected.len()));
        assert_eq!(&output[..expected.len()], expected);
        assert!(output[expected.len()..].iter().all(|value| *value == 0x5a));
        for short in 0..expected.len() {
            let mut unchanged = [0x5a_u8; 32];
            let before = unchanged;
            assert_eq!(
                encode(message, &mut unchanged[..short]),
                Err(EncodeError::OutputTooShort {
                    required: expected.len(),
                    available: short
                })
            );
            assert_eq!(unchanged, before);
        }
    }
    for (rate, tag) in [
        (SampleRate::Khz8, 0x10),
        (SampleRate::Khz12, 0x11),
        (SampleRate::Khz16, 0x12),
        (SampleRate::Khz24, 0x13),
        (SampleRate::Khz32, 0x14),
        (SampleRate::Khz44_1, 0x15),
        (SampleRate::Khz48, 0x16),
        (SampleRate::Khz96, 0x17),
        (SampleRate::Khz192, 0x18),
    ] {
        let message = TypedMessage::Audio {
            rate,
            payload: pcm(&[0x00, 0x80]),
        };
        let mut output = [0x5a_u8; 6];
        assert_eq!(encode(message, &mut output), Ok(5));
        assert_eq!(output, [tag, 0, 2, 0, 0x80, 0x5a]);
    }
}

#[test]
fn every_uuid_and_dtmf_body_eof_position_is_reported() {
    for received in 0..16 {
        let mut scratch = [0_u8; 16];
        let mut decoder = RawDecoder::new(&mut scratch);
        let bytes = [
            0x01, 0, 16, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        ];
        let mut input = &bytes[..3 + received];
        assert_eq!(decoder.feed(&mut input), Ok(DecodeOutcome::NeedInput));
        assert_eq!(
            decoder.finish(),
            Err(FinishError::TruncatedPayload {
                expected: 16,
                received
            })
        );
    }
    let bytes = [0x03, 0, 1, b'5'];
    for received in 0..1 {
        let mut scratch = [0_u8; 1];
        let mut decoder = RawDecoder::new(&mut scratch);
        let mut input = &bytes[..3 + received];
        assert_eq!(decoder.feed(&mut input), Ok(DecodeOutcome::NeedInput));
        assert_eq!(
            decoder.finish(),
            Err(FinishError::TruncatedPayload {
                expected: 1,
                received
            })
        );
    }
    let mut scratch = [0_u8; 1];
    let mut decoder = Decoder::new(&mut scratch);
    let mut bad: &[u8] = &[3, 0, 1, 0x80];
    assert!(decoder.feed(&mut bad).is_err());
    assert_eq!(decoder.finish(), Err(FinishError::Failed));
}

#[test]
fn split_refusals_and_zero_scratch_priority_have_literal_cursors() {
    let mut empty = [];
    let mut raw = RawDecoder::new(&mut empty);
    let mut tag: &[u8] = &[2];
    assert_eq!(raw.feed(&mut tag), Ok(DecodeOutcome::NeedInput));
    let mut remainder: &[u8] = &[0, 1, 0xaa];
    assert_eq!(
        raw.feed(&mut remainder),
        Err(DecodeError::ResourceLimit {
            required: 1,
            capacity: 0
        })
    );
    assert_eq!(remainder, &[0xaa]);
    let mut strict = Decoder::new(&mut empty);
    let mut wrong: &[u8] = &[1, 0, 15, 0xaa];
    assert_eq!(
        strict.feed(&mut wrong),
        Err(DecodeError::InvalidLength(
            phonowire_audiosocket::TypedMessageError::UuidLength
        ))
    );
    assert_eq!(wrong, &[0xaa]);
}
