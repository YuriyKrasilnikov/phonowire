//! PCM payload views and declared rates.

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
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    /// Returns original PCM bytes.
    #[must_use]
    pub const fn bytes(self) -> &'a [u8] {
        self.0
    }
}
