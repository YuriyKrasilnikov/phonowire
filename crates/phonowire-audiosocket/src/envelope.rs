//! Raw envelopes and strict typed conversion.
use crate::{
    AudioPayload, Dtmf, KnownWireType, OpaquePayload, SampleRate, TypedMessage, TypedMessageError,
    UnknownWireType, Uuid, WireType,
};
use core::fmt;

/// Failure to form a raw envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawEnvelopeError {
    /// Payload exceeds the u16 wire-length limit.
    PayloadTooLong,
}

impl fmt::Display for RawEnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("payload exceeds the u16 wire-length limit")
    }
}

impl core::error::Error for RawEnvelopeError {}

/// A borrowed raw frame body paired with any wire type byte.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawEnvelope<'a> {
    wire_type: WireType,
    payload: &'a [u8],
}

impl<'a> RawEnvelope<'a> {
    pub(crate) const fn from_wire_bounded(wire_type: WireType, payload: &'a [u8]) -> Self {
        Self { wire_type, payload }
    }

    /// Preserves any wire type and a payload no longer than 65535 bytes.
    ///
    /// # Errors
    ///
    /// Returns [`RawEnvelopeError::PayloadTooLong`] for a payload above the wire limit.
    pub const fn new(wire_type: WireType, payload: &'a [u8]) -> Result<Self, RawEnvelopeError> {
        if payload.len() > crate::MAX_BODY_BYTES {
            return Err(RawEnvelopeError::PayloadTooLong);
        }
        Ok(Self { wire_type, payload })
    }
    /// Returns raw wire type without classifying it.
    #[must_use]
    pub const fn wire_type(self) -> WireType {
        self.wire_type
    }
    /// Returns the borrowed raw payload.
    #[must_use]
    pub const fn payload(self) -> &'a [u8] {
        self.payload
    }
    /// Applies strict typed policies while preserving raw access on failure.
    ///
    /// # Errors
    ///
    /// Returns a precise [`TypedMessageError`] for a known type whose body violates policy.
    pub fn typed(self) -> Result<TypedMessage<'a>, TypedMessageError> {
        validate_typed_length(self.wire_type, self.payload.len())?;
        match self.wire_type.known() {
            Some(KnownWireType::Terminate) => Ok(TypedMessage::Terminate),
            Some(KnownWireType::Uuid) => Ok(self.uuid()),
            Some(KnownWireType::Dtmf) => self.dtmf(),
            Some(KnownWireType::Pcm8Khz) => Ok(self.audio(SampleRate::Khz8)),
            Some(KnownWireType::Pcm12Khz) => Ok(self.audio(SampleRate::Khz12)),
            Some(KnownWireType::Pcm16Khz) => Ok(self.audio(SampleRate::Khz16)),
            Some(KnownWireType::Pcm24Khz) => Ok(self.audio(SampleRate::Khz24)),
            Some(KnownWireType::Pcm32Khz) => Ok(self.audio(SampleRate::Khz32)),
            Some(KnownWireType::Pcm44Khz) => Ok(self.audio(SampleRate::Khz44_1)),
            Some(KnownWireType::Pcm48Khz) => Ok(self.audio(SampleRate::Khz48)),
            Some(KnownWireType::Pcm96Khz) => Ok(self.audio(SampleRate::Khz96)),
            Some(KnownWireType::Pcm192Khz) => Ok(self.audio(SampleRate::Khz192)),
            Some(KnownWireType::Error) => Ok(TypedMessage::Error(
                OpaquePayload::from_wire_bounded(self.payload),
            )),
            None => Ok(self.unknown()),
        }
    }
    const fn uuid(self) -> TypedMessage<'a> {
        let mut bytes = [0; 16];
        bytes.copy_from_slice(self.payload);
        TypedMessage::Uuid(Uuid::new(bytes))
    }
    fn dtmf(self) -> Result<TypedMessage<'a>, TypedMessageError> {
        Dtmf::new(self.payload[0]).map(TypedMessage::Dtmf)
    }
    const fn audio(self, rate: SampleRate) -> TypedMessage<'a> {
        TypedMessage::Audio {
            rate,
            payload: AudioPayload::from_validated(self.payload),
        }
    }
    const fn unknown(self) -> TypedMessage<'a> {
        let wire_type = UnknownWireType::from_unassigned(self.wire_type.value());
        TypedMessage::Unknown {
            wire_type,
            payload: OpaquePayload::from_wire_bounded(self.payload),
        }
    }
}

pub const fn validate_typed_length(
    wire_type: WireType,
    length: usize,
) -> Result<(), TypedMessageError> {
    match wire_type.value() {
        0x00 if length != 0 => Err(TypedMessageError::TerminatePayload),
        0x01 if length != 16 => Err(TypedMessageError::UuidLength),
        0x03 if length != 1 => Err(TypedMessageError::DtmfLength),
        0x10..=0x18 if !length.is_multiple_of(2) => Err(TypedMessageError::OddPcmLength),
        _ => Ok(()),
    }
}
