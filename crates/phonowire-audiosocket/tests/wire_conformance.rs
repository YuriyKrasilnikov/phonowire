//! Literal regression witnesses for the public typed codec boundary.
use phonowire_audiosocket::{
    AudioPayload, AudioPayloadError, DecodeError, DecodeOutcome, Decoder, Dtmf, EncodeError,
    FinishError, OpaquePayload, RawDecoder, RawEnvelope, SampleRate, TypedMessage, UnknownWireType,
    WireType, encode, encode_raw,
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

fn literal_frame(tag: u8, body: &[u8]) -> std::vec::Vec<u8> {
    assert!(body.len() <= 65_535, "literal body exceeds the wire limit");
    let high = match u8::try_from(body.len() / 256) {
        Ok(value) => value,
        Err(error) => panic!("literal high length byte: {error:?}"),
    };
    let low = match u8::try_from(body.len() % 256) {
        Ok(value) => value,
        Err(error) => panic!("literal low length byte: {error:?}"),
    };
    let mut frame = std::vec![tag, high, low];
    frame.extend_from_slice(body);
    frame
}

fn patterned_body(tag: u8, length: usize) -> std::vec::Vec<u8> {
    (0..length)
        .map(|index| match u8::try_from(index % 251) {
            Ok(offset) => tag.wrapping_add(offset).wrapping_mul(17),
            Err(error) => panic!("bounded payload index: {error:?}"),
        })
        .collect()
}

fn assert_raw_frame(tag: u8, body: &[u8], output: &[u8]) {
    let expected = literal_frame(tag, body);
    assert_eq!(output.len(), expected.len());
    assert_eq!(output, expected);
    assert_eq!(output[0], tag);
    assert_eq!(output[1], expected[1]);
    assert_eq!(output[2], expected[2]);
    assert_eq!(&output[3..], body);
}

fn assert_raw_decode(tag: u8, body: &[u8]) {
    let frame = literal_frame(tag, body);
    let mut scratch = std::vec![0_u8; body.len()];
    let mut decoder = RawDecoder::new(&mut scratch);
    let mut first = &frame[..1];
    assert_eq!(decoder.feed(&mut first), Ok(DecodeOutcome::NeedInput));
    assert!(first.is_empty());
    let mut remaining_header = &frame[1..3];
    if body.is_empty() {
        match decoder.feed(&mut remaining_header) {
            Ok(DecodeOutcome::Frame(observed)) => {
                assert_eq!(observed.wire_type().value(), tag);
                assert_eq!(observed.payload(), body);
            }
            other => panic!("empty literal raw frame: {other:?}"),
        }
        assert!(remaining_header.is_empty());
        return;
    }
    assert_eq!(
        decoder.feed(&mut remaining_header),
        Ok(DecodeOutcome::NeedInput)
    );
    let initial_body = core::cmp::min(1, body.len());
    let mut initial = &frame[3..3 + initial_body];
    if initial_body < body.len() {
        assert_eq!(decoder.feed(&mut initial), Ok(DecodeOutcome::NeedInput));
        assert!(initial.is_empty());
        let mut final_body = &frame[3 + initial_body..];
        match decoder.feed(&mut final_body) {
            Ok(DecodeOutcome::Frame(observed)) => {
                assert_eq!(observed.wire_type().value(), tag);
                assert_eq!(observed.payload(), body);
            }
            other => panic!("fragmented literal raw frame: {other:?}"),
        }
        assert!(final_body.is_empty());
        return;
    }
    match decoder.feed(&mut initial) {
        Ok(DecodeOutcome::Frame(observed)) => {
            assert_eq!(observed.wire_type().value(), tag);
            assert_eq!(observed.payload(), body);
        }
        other => panic!("one-byte literal raw frame: {other:?}"),
    }
    assert!(initial.is_empty());
}

#[test]
fn literal_raw_matrix_preserves_tags_lengths_and_fragmentation() {
    let tags = [0x01_u8, 0x03, 0x10, 0x02, 0xff];
    let lengths = [0_usize, 1, 2, 255, 256, 257, 65_535];
    for tag in tags {
        for length in lengths {
            let body = patterned_body(tag, length);
            assert_raw_decode(tag, &body);
        }
    }

    let first_body = patterned_body(0x01, 1);
    let second_body = patterned_body(0x02, 2);
    let first = literal_frame(0x01, &first_body);
    let second = literal_frame(0x02, &second_body);
    let mut combined = first;
    combined.extend_from_slice(&second);
    let mut scratch = [0_u8; 2];
    let mut decoder = RawDecoder::new(&mut scratch);
    let mut input = combined.as_slice();
    match decoder.feed(&mut input) {
        Ok(DecodeOutcome::Frame(observed)) => {
            assert_eq!(observed.wire_type().value(), 0x01);
            assert_eq!(observed.payload(), first_body);
        }
        other => panic!("first coalesced literal raw frame: {other:?}"),
    }
    match decoder.feed(&mut input) {
        Ok(DecodeOutcome::Frame(observed)) => {
            assert_eq!(observed.wire_type().value(), 0x02);
            assert_eq!(observed.payload(), second_body);
        }
        other => panic!("second coalesced literal raw frame: {other:?}"),
    }
    assert!(input.is_empty());
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
fn literal_raw_and_opaque_encoder_boundaries_preserve_every_byte() {
    let lengths = [0_usize, 1, 2, 15, 16, 17, 255, 256, 257, 65_534, 65_535];
    for length in lengths {
        let body = patterned_body(0x01, length);
        let expected = literal_frame(0x01, &body);
        let raw = match RawEnvelope::new(WireType::new(0x01), &body) {
            Ok(value) => value,
            Err(error) => panic!("known typed-invalid raw envelope rejected: {error:?}"),
        };
        let mut raw_destination = std::vec![0x5a_u8; expected.len() + 4];
        assert_eq!(encode_raw(raw, &mut raw_destination), Ok(expected.len()));
        assert_raw_frame(0x01, &body, &raw_destination[..expected.len()]);
        assert!(
            raw_destination[expected.len()..]
                .iter()
                .all(|value| *value == 0x5a)
        );

        let opaque = match OpaquePayload::new(&body) {
            Ok(value) => value,
            Err(error) => panic!("literal opaque payload rejected: {error:?}"),
        };
        let mut opaque_destination = std::vec![0x5a_u8; expected.len() + 4];
        assert_eq!(
            encode(TypedMessage::Error(opaque), &mut opaque_destination),
            Ok(expected.len())
        );
        assert_raw_frame(0xff, &body, &opaque_destination[..expected.len()]);
        assert!(
            opaque_destination[expected.len()..]
                .iter()
                .all(|value| *value == 0x5a)
        );
    }

    for (tag, length) in [
        (0x01_u8, 0_usize),
        (0x01, 1),
        (0x01, 255),
        (0x01, 256),
        (0x01, 65_535),
    ] {
        let body = patterned_body(tag, length);
        let required = 3 + body.len();
        let raw = match RawEnvelope::new(WireType::new(tag), &body) {
            Ok(value) => value,
            Err(error) => panic!("short-destination raw envelope rejected: {error:?}"),
        };
        for available in [0_usize, required.saturating_sub(1)] {
            let mut destination = std::vec![0x5a_u8; required + 2];
            let before = destination.clone();
            assert_eq!(
                encode_raw(raw, &mut destination[..available]),
                Err(EncodeError::OutputTooShort {
                    required,
                    available
                })
            );
            assert_eq!(destination, before);

            let opaque = match OpaquePayload::new(&body) {
                Ok(value) => value,
                Err(error) => panic!("short-destination opaque payload rejected: {error:?}"),
            };
            let mut opaque_destination = std::vec![0x5a_u8; required + 2];
            let opaque_before = opaque_destination.clone();
            assert_eq!(
                encode(
                    TypedMessage::Error(opaque),
                    &mut opaque_destination[..available]
                ),
                Err(EncodeError::OutputTooShort {
                    required,
                    available
                })
            );
            assert_eq!(opaque_destination, opaque_before);
        }
    }

    let pcm_body = patterned_body(0x10, 65_534);
    let pcm_message = TypedMessage::Audio {
        rate: SampleRate::Khz8,
        payload: pcm(&pcm_body),
    };
    let pcm_expected = literal_frame(0x10, &pcm_body);
    let mut pcm_destination = std::vec![0x5a_u8; pcm_expected.len() + 1];
    assert_eq!(
        encode(pcm_message, &mut pcm_destination),
        Ok(pcm_expected.len())
    );
    assert_raw_frame(0x10, &pcm_body, &pcm_destination[..pcm_expected.len()]);
    assert_eq!(pcm_destination[pcm_expected.len()], 0x5a);
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
