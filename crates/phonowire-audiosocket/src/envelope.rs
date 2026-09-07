//! Raw envelopes and strict typed conversion.
use crate::{
    AudioPayload, Dtmf, KnownWireType, OpaquePayload, SampleRate, TypedMessage, TypedMessageError,
    UnknownWireType, Uuid, WireType,
};

/// Failure to form a raw envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawEnvelopeError {
    /// Payload exceeds the u16 wire-length limit.
    PayloadTooLong,
}

/// A borrowed raw frame body paired with any wire type byte.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawEnvelope<'a> {
    wire_type: WireType,
    payload: &'a [u8],
}

impl<'a> RawEnvelope<'a> {
    /// Preserves any wire type and a payload no longer than 65535 bytes.
    ///
    /// # Errors
    ///
    /// Returns [`RawEnvelopeError::PayloadTooLong`] for a payload above the wire limit.
    pub fn new(wire_type: WireType, payload: &'a [u8]) -> Result<Self, RawEnvelopeError> {
        if payload.len() > usize::from(u16::MAX) {
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
        match self.wire_type.known() {
            Some(KnownWireType::Terminate) if self.payload.is_empty() => {
                Ok(TypedMessage::Terminate)
            }
            Some(KnownWireType::Terminate) => Err(TypedMessageError::TerminatePayload),
            Some(KnownWireType::Uuid) => self.uuid(),
            Some(KnownWireType::Dtmf) => self.dtmf(),
            Some(KnownWireType::Pcm8Khz) => self.audio(SampleRate::Khz8),
            Some(KnownWireType::Pcm12Khz) => self.audio(SampleRate::Khz12),
            Some(KnownWireType::Pcm16Khz) => self.audio(SampleRate::Khz16),
            Some(KnownWireType::Pcm24Khz) => self.audio(SampleRate::Khz24),
            Some(KnownWireType::Pcm32Khz) => self.audio(SampleRate::Khz32),
            Some(KnownWireType::Pcm44Khz) => self.audio(SampleRate::Khz44_1),
            Some(KnownWireType::Pcm48Khz) => self.audio(SampleRate::Khz48),
            Some(KnownWireType::Pcm96Khz) => self.audio(SampleRate::Khz96),
            Some(KnownWireType::Pcm192Khz) => self.audio(SampleRate::Khz192),
            Some(KnownWireType::Error) => Ok(TypedMessage::Error(
                OpaquePayload::new(self.payload)
                    .map_err(|_| TypedMessageError::UnknownTypeClassification)?,
            )),
            None => self.unknown(),
        }
    }
    fn uuid(self) -> Result<TypedMessage<'a>, TypedMessageError> {
        let bytes =
            <[u8; 16]>::try_from(self.payload).map_err(|_| TypedMessageError::UuidLength)?;
        Ok(TypedMessage::Uuid(Uuid::new(bytes)))
    }
    const fn dtmf(self) -> Result<TypedMessage<'a>, TypedMessageError> {
        if self.payload.len() != 1 {
            return Err(TypedMessageError::DtmfLength);
        }
        let value = self.payload[0];
        if !value.is_ascii() {
            return Err(TypedMessageError::DtmfNotAscii);
        }
        Ok(TypedMessage::Dtmf(Dtmf::new(value)))
    }
    const fn audio(self, rate: SampleRate) -> Result<TypedMessage<'a>, TypedMessageError> {
        if !self.payload.len().is_multiple_of(2) {
            return Err(TypedMessageError::OddPcmLength);
        }
        Ok(TypedMessage::Audio {
            rate,
            payload: AudioPayload::new(self.payload),
        })
    }
    fn unknown(self) -> Result<TypedMessage<'a>, TypedMessageError> {
        let wire_type = UnknownWireType::new(self.wire_type.value())
            .map_err(|_| TypedMessageError::UnknownTypeClassification)?;
        Ok(TypedMessage::Unknown {
            wire_type,
            payload: OpaquePayload::new(self.payload)
                .map_err(|_| TypedMessageError::UnknownTypeClassification)?,
        })
    }
}
