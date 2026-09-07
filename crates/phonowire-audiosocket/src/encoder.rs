//! Bounded encoding of one wire frame.
use core::fmt;

use crate::{HEADER_BYTES, RawEnvelope, TypedMessage, WireType};

/// Failure to encode a frame into the supplied destination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EncodeError {
    /// The destination cannot hold the complete frame and was not changed.
    OutputTooShort {
        /// Bytes required for the complete frame.
        required: usize,
        /// Bytes available in the destination.
        available: usize,
    },
}

impl fmt::Display for EncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputTooShort {
                required,
                available,
            } => {
                write!(
                    formatter,
                    "output has {available} bytes but frame requires {required}"
                )
            }
        }
    }
}

impl core::error::Error for EncodeError {}

/// Encodes one validated typed message and returns its exact frame length.
///
/// The destination is unchanged when it is too short.
///
/// # Errors
///
/// Returns [`EncodeError::OutputTooShort`] without changing `destination` when
/// it cannot contain the whole frame.
pub fn encode(message: TypedMessage<'_>, destination: &mut [u8]) -> Result<usize, EncodeError> {
    match message {
        TypedMessage::Terminate => write_frame(WireType::TERMINATE, &[], destination),
        TypedMessage::Uuid(uuid) => {
            let bytes = uuid.bytes();
            write_frame(WireType::UUID, &bytes, destination)
        }
        TypedMessage::Dtmf(dtmf) => {
            let bytes = [dtmf.value()];
            write_frame(WireType::DTMF, &bytes, destination)
        }
        TypedMessage::Audio { rate, payload } => {
            let wire_type = match rate {
                crate::SampleRate::Khz8 => WireType::new(0x10),
                crate::SampleRate::Khz12 => WireType::new(0x11),
                crate::SampleRate::Khz16 => WireType::new(0x12),
                crate::SampleRate::Khz24 => WireType::new(0x13),
                crate::SampleRate::Khz32 => WireType::new(0x14),
                crate::SampleRate::Khz44_1 => WireType::new(0x15),
                crate::SampleRate::Khz48 => WireType::new(0x16),
                crate::SampleRate::Khz96 => WireType::new(0x17),
                crate::SampleRate::Khz192 => WireType::new(0x18),
            };
            write_frame(wire_type, payload.bytes(), destination)
        }
        TypedMessage::Error(payload) => write_frame(WireType::ERROR, payload.bytes(), destination),
        TypedMessage::Unknown { wire_type, payload } => write_frame(
            WireType::new(wire_type.value()),
            payload.bytes(),
            destination,
        ),
    }
}

/// Encodes one raw envelope and returns its exact frame length.
///
/// The destination is unchanged when it is too short.
///
/// # Errors
///
/// Returns [`EncodeError::OutputTooShort`] without changing `destination` when
/// it cannot contain the whole frame.
pub fn encode_raw(raw: RawEnvelope<'_>, destination: &mut [u8]) -> Result<usize, EncodeError> {
    write_frame(raw.wire_type(), raw.payload(), destination)
}

fn write_frame(
    wire_type: WireType,
    payload: &[u8],
    destination: &mut [u8],
) -> Result<usize, EncodeError> {
    let payload_length = u16::try_from(payload.len())
        .expect("validated message and raw envelope payloads fit the u16 wire length");
    let required = HEADER_BYTES + payload.len();
    if destination.len() < required {
        return Err(EncodeError::OutputTooShort {
            required,
            available: destination.len(),
        });
    }
    destination[0] = wire_type.value();
    destination[1..HEADER_BYTES].copy_from_slice(&payload_length.to_be_bytes());
    destination[HEADER_BYTES..required].copy_from_slice(payload);
    Ok(required)
}
