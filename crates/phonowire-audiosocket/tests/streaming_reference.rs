//! Literal framing witnesses that do not use production encoding as an oracle.
use phonowire_audiosocket::{
    DecodeError, DecodeOutcome, Decoder, FinishError, RawDecoder, RawEnvelope, SampleRate,
    TypedMessage, TypedMessageError, WireType, encode, encode_raw,
};

fn raw_frame(kind: u8, body: &[u8]) -> RawEnvelope<'_> {
    match RawEnvelope::new(WireType::new(kind), body) {
        Ok(frame) => frame,
        Err(error) => panic!("literal frame construction failed: {error:?}"),
    }
}

#[test]
fn every_short_uuid_partition_and_empty_feeds_has_the_literal_result() {
    let bytes = [
        0x01, 0x00, 0x10, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
        0x0c, 0x0d, 0x0e, 0x0f,
    ];
    let boundaries = bytes.len() - 1;
    for mask in 0_usize..(1_usize << boundaries) {
        let mut scratch = [0_u8; 16];
        let mut decoder = Decoder::new(&mut scratch);
        let mut start = 0;
        for boundary in 0..boundaries {
            let end = boundary + 1;
            if mask & (1_usize << boundary) != 0 {
                feed_uuid_part(&mut decoder, &bytes[start..end], false);
                feed_uuid_part(&mut decoder, &[], false);
                start = end;
            }
        }
        feed_uuid_part(&mut decoder, &bytes[start..], true);
        assert_eq!(decoder.finish(), Ok(()));
    }
}

fn feed_uuid_part(decoder: &mut Decoder<'_>, bytes: &[u8], final_part: bool) {
    let mut cursor = bytes;
    match decoder.feed(&mut cursor) {
        Ok(DecodeOutcome::NeedInput) if !final_part => {}
        Ok(DecodeOutcome::Frame(TypedMessage::Uuid(uuid))) if final_part => {
            assert_eq!(
                uuid.bytes(),
                [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
            );
        }
        other => panic!("unexpected partition result: {other:?}"),
    }
    assert!(cursor.is_empty());
}

#[test]
fn coalesced_frames_return_one_per_call_and_allow_reuse_after_view_drop() {
    let bytes = [0x02, 0x00, 0x01, 0xaa, 0x00, 0x00, 0x00];
    let mut scratch = [0_u8; 1];
    let mut decoder = RawDecoder::new(&mut scratch);
    let mut cursor = bytes.as_slice();
    match decoder.feed(&mut cursor) {
        Ok(DecodeOutcome::Frame(frame)) => {
            assert_eq!(frame.wire_type().value(), 0x02);
            assert_eq!(frame.payload(), &[0xaa]);
        }
        other => panic!("unexpected first coalesced result: {other:?}"),
    }
    assert_eq!(cursor, &[0x00, 0x00, 0x00]);
    match decoder.feed(&mut cursor) {
        Ok(DecodeOutcome::Frame(frame)) => assert_eq!(frame.wire_type().value(), 0x00),
        other => panic!("unexpected second coalesced result: {other:?}"),
    }
    assert!(cursor.is_empty());
}

#[test]
fn strict_length_and_capacity_errors_consume_only_the_header_then_absorb() {
    let mut no_scratch = [];
    let mut raw = RawDecoder::new(&mut no_scratch);
    let mut capacity_input: &[u8] = &[0x02, 0x00, 0x01, 0xaa];
    assert_eq!(
        raw.feed(&mut capacity_input),
        Err(DecodeError::ResourceLimit {
            required: 1,
            capacity: 0
        })
    );
    assert_eq!(capacity_input, &[0xaa]);
    assert_eq!(raw.feed(&mut capacity_input), Err(DecodeError::Failed));
    assert_eq!(capacity_input, &[0xaa]);

    let mut strict_scratch = [0_u8; 15];
    let mut strict = Decoder::new(&mut strict_scratch);
    let mut malformed: &[u8] = &[0x01, 0x00, 0x0f, 0xaa];
    assert_eq!(
        strict.feed(&mut malformed),
        Err(DecodeError::InvalidLength(TypedMessageError::UuidLength))
    );
    assert_eq!(malformed, &[0xaa]);
    assert_eq!(strict.feed(&mut malformed), Err(DecodeError::Failed));
    assert_eq!(malformed, &[0xaa]);
}

#[test]
fn strict_content_failure_consumes_body_and_raw_conversion_does_not_poison() {
    let mut strict_scratch = [0_u8; 1];
    let mut strict = Decoder::new(&mut strict_scratch);
    let mut malformed: &[u8] = &[0x03, 0x00, 0x01, 0x80];
    assert_eq!(
        strict.feed(&mut malformed),
        Err(DecodeError::InvalidContent(TypedMessageError::DtmfNotAscii))
    );
    assert!(malformed.is_empty());
    let mut suffix: &[u8] = &[0x00, 0x00, 0x00];
    assert_eq!(strict.feed(&mut suffix), Err(DecodeError::Failed));
    assert_eq!(suffix, &[0x00, 0x00, 0x00]);

    let mut raw_scratch = [0_u8; 1];
    let mut raw = RawDecoder::new(&mut raw_scratch);
    let mut both: &[u8] = &[0x03, 0x00, 0x01, 0x80, 0x00, 0x00, 0x00];
    match raw.feed(&mut both) {
        Ok(DecodeOutcome::Frame(frame)) => {
            assert_eq!(frame.typed(), Err(TypedMessageError::DtmfNotAscii));
        }
        other => panic!("unexpected raw malformed frame: {other:?}"),
    }
    match raw.feed(&mut both) {
        Ok(DecodeOutcome::Frame(frame)) => assert_eq!(frame.typed(), Ok(TypedMessage::Terminate)),
        other => panic!("raw decoder did not continue: {other:?}"),
    }
}

#[test]
fn eof_reports_each_partial_position_and_empty_feeds_are_not_eof() {
    let headers = [(&[0x01][..], 1), (&[0x01, 0x00][..], 2)];
    for (bytes, received) in headers {
        let mut scratch = [0_u8; 16];
        let mut decoder = RawDecoder::new(&mut scratch);
        let mut cursor = bytes;
        assert_eq!(decoder.feed(&mut cursor), Ok(DecodeOutcome::NeedInput));
        assert_eq!(
            decoder.finish(),
            Err(FinishError::TruncatedHeader {
                expected: 3,
                received
            })
        );
    }
    for (bytes, received) in [
        (&[0x01, 0x00, 0x10][..], 0),
        (&[0x01, 0x00, 0x10, 0][..], 1),
        (
            &[
                0x01, 0x00, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ][..],
            15,
        ),
    ] {
        let mut scratch = [0_u8; 16];
        let mut decoder = RawDecoder::new(&mut scratch);
        let mut cursor = bytes;
        assert_eq!(decoder.feed(&mut cursor), Ok(DecodeOutcome::NeedInput));
        assert_eq!(
            decoder.finish(),
            Err(FinishError::TruncatedPayload {
                expected: 16,
                received
            })
        );
    }
    let mut scratch = [];
    let mut decoder = RawDecoder::new(&mut scratch);
    let mut empty: &[u8] = &[];
    assert_eq!(decoder.feed(&mut empty), Ok(DecodeOutcome::NeedInput));
    assert_eq!(decoder.finish(), Ok(()));
}

#[test]
fn raw_handles_all_type_bytes_and_maximum_body_without_table_or_encoder_oracle() {
    for tag in 0_u8..=u8::MAX {
        let mut scratch = [];
        let mut decoder = RawDecoder::new(&mut scratch);
        let header = [tag, 0, 0];
        let mut cursor = header.as_slice();
        match decoder.feed(&mut cursor) {
            Ok(DecodeOutcome::Frame(frame)) => {
                assert_eq!(frame.wire_type().value(), tag);
                assert!(frame.payload().is_empty());
            }
            other => panic!("tag {tag:02x} did not preserve a raw empty envelope: {other:?}"),
        }
    }
    let mut scratch = std::vec![0_u8; 65_535];
    let mut decoder = RawDecoder::new(&mut scratch);
    let body = std::vec![0xa5_u8; 65_535];
    let header = [0xfe, 0xff, 0xff];
    let mut first = header.as_slice();
    assert_eq!(decoder.feed(&mut first), Ok(DecodeOutcome::NeedInput));
    let mut body_cursor = body.as_slice();
    match decoder.feed(&mut body_cursor) {
        Ok(DecodeOutcome::Frame(frame)) => {
            assert_eq!(frame.wire_type().value(), 0xfe);
            assert_eq!(frame.payload().len(), 65_535);
            assert!(frame.payload().iter().all(|byte| *byte == 0xa5));
        }
        other => panic!("maximum raw body did not complete: {other:?}"),
    }
    assert!(body_cursor.is_empty());
}

#[test]
fn literal_rate_mapping_and_encoder_bounds_preserve_unwritten_bytes() {
    let literal_rates = [
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
    for (tag, rate) in literal_rates {
        let mut scratch = [];
        let mut decoder = Decoder::new(&mut scratch);
        let header = [tag, 0, 0];
        let mut cursor = header.as_slice();
        match decoder.feed(&mut cursor) {
            Ok(DecodeOutcome::Frame(TypedMessage::Audio {
                rate: actual,
                payload,
            })) => {
                assert_eq!(actual, rate);
                assert!(payload.bytes().is_empty());
            }
            other => panic!("literal rate tag {tag:02x} failed: {other:?}"),
        }
    }
    let raw = raw_frame(0x10, &[0x00, 0x80, 0xff, 0x7f]);
    let typed = match raw.typed() {
        Ok(message) => message,
        Err(error) => panic!("literal PCM rejected: {error:?}"),
    };
    for size in 0..7 {
        let mut destination = [0x5a_u8; 9];
        let before = destination;
        assert_eq!(
            encode(typed, &mut destination[..size]),
            Err(phonowire_audiosocket::EncodeError::OutputTooShort {
                required: 7,
                available: size
            })
        );
        assert_eq!(destination, before);
        let mut raw_destination = [0x5a_u8; 9];
        let raw_before = raw_destination;
        assert_eq!(
            encode_raw(raw, &mut raw_destination[..size]),
            Err(phonowire_audiosocket::EncodeError::OutputTooShort {
                required: 7,
                available: size
            })
        );
        assert_eq!(raw_destination, raw_before);
    }
    let mut destination = [0x5a_u8; 9];
    assert_eq!(encode(typed, &mut destination), Ok(7));
    assert_eq!(
        destination,
        [0x10, 0x00, 0x04, 0x00, 0x80, 0xff, 0x7f, 0x5a, 0x5a]
    );
}
