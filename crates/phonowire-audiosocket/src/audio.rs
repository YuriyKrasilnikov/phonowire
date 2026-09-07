//! PCM payload views and declared rates.
use core::fmt;

/// Failure to create a PCM16LE payload view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AudioPayloadError {
    /// PCM16LE requires an even number of bytes.
    OddLength,
    /// The payload exceeds the wire's `u16` length limit.
    PayloadTooLong,
}

impl fmt::Display for AudioPayloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::OddLength => "PCM payload has an odd byte length",
            Self::PayloadTooLong => "PCM payload exceeds the u16 wire-length limit",
        })
    }
}

impl core::error::Error for AudioPayloadError {}

/// A documented PCM sample rate declared by the wire type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SampleRate {
    /// 8 kHz.
    Khz8,
    /// 12 kHz.
    Khz12,
    /// 16 kHz.
    Khz16,
    /// 24 kHz.
    Khz24,
    /// 32 kHz.
    Khz32,
    /// 44.1 kHz.
    Khz44_1,
    /// 48 kHz.
    Khz48,
    /// 96 kHz.
    Khz96,
    /// 192 kHz.
    Khz192,
}

impl SampleRate {
    /// Returns the numeric declared rate.
    #[must_use]
    pub const fn hertz(self) -> u32 {
        match self {
            Self::Khz8 => 8_000,
            Self::Khz12 => 12_000,
            Self::Khz16 => 16_000,
            Self::Khz24 => 24_000,
            Self::Khz32 => 32_000,
            Self::Khz44_1 => 44_100,
            Self::Khz48 => 48_000,
            Self::Khz96 => 96_000,
            Self::Khz192 => 192_000,
        }
    }
}

/// A borrowed, even-length PCM16LE byte payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AudioPayload<'a>(&'a [u8]);

impl<'a> AudioPayload<'a> {
    /// Validates an even PCM16LE payload that fits the wire length field.
    ///
    /// # Errors
    ///
    /// Returns [`AudioPayloadError::OddLength`] for an odd byte count and
    /// [`AudioPayloadError::PayloadTooLong`] above the wire limit.
    pub const fn new(bytes: &'a [u8]) -> Result<Self, AudioPayloadError> {
        if bytes.len() > crate::MAX_BODY_BYTES {
            return Err(AudioPayloadError::PayloadTooLong);
        }
        if !bytes.len().is_multiple_of(2) {
            return Err(AudioPayloadError::OddLength);
        }
        Ok(Self(bytes))
    }

    pub(crate) const fn from_validated(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    /// Returns original PCM bytes.
    #[must_use]
    pub const fn bytes(self) -> &'a [u8] {
        self.0
    }
}
