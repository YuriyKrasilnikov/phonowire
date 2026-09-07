//! Wire-type classification without losing unknown values.

/// A raw `AudioSocket` type byte.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireType(u8);

impl WireType {
    /// Terminate-frame type byte.
    pub const TERMINATE: Self = Self(0x00);
    /// UUID-frame type byte.
    pub const UUID: Self = Self(0x01);
    /// DTMF-frame type byte.
    pub const DTMF: Self = Self(0x03);
    /// Opaque peer-error type byte.
    pub const ERROR: Self = Self(0xff);

    /// Creates a raw type byte, including unassigned values.
    #[must_use]
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// Returns the preserved numeric type byte.
    #[must_use]
    pub const fn value(self) -> u8 {
        self.0
    }

    /// Classifies this byte when it names a documented type.
    #[must_use]
    pub const fn known(self) -> Option<KnownWireType> {
        match self.0 {
            0x00 => Some(KnownWireType::Terminate),
            0x01 => Some(KnownWireType::Uuid),
            0x03 => Some(KnownWireType::Dtmf),
            0x10 => Some(KnownWireType::Pcm8Khz),
            0x11 => Some(KnownWireType::Pcm12Khz),
            0x12 => Some(KnownWireType::Pcm16Khz),
            0x13 => Some(KnownWireType::Pcm24Khz),
            0x14 => Some(KnownWireType::Pcm32Khz),
            0x15 => Some(KnownWireType::Pcm44Khz),
            0x16 => Some(KnownWireType::Pcm48Khz),
            0x17 => Some(KnownWireType::Pcm96Khz),
            0x18 => Some(KnownWireType::Pcm192Khz),
            0xff => Some(KnownWireType::Error),
            _ => None,
        }
    }
}

/// Every documented `AudioSocket` wire type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KnownWireType {
    /// Termination.
    Terminate,
    /// UUID.
    Uuid,
    /// DTMF.
    Dtmf,
    /// PCM 8 kHz.
    Pcm8Khz,
    /// PCM 12 kHz.
    Pcm12Khz,
    /// PCM 16 kHz.
    Pcm16Khz,
    /// PCM 24 kHz.
    Pcm24Khz,
    /// PCM 32 kHz.
    Pcm32Khz,
    /// PCM 44.1 kHz.
    Pcm44Khz,
    /// PCM 48 kHz.
    Pcm48Khz,
    /// PCM 96 kHz.
    Pcm96Khz,
    /// PCM 192 kHz.
    Pcm192Khz,
    /// Opaque peer error.
    Error,
}

/// A type byte that is not assigned by the documented protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnknownWireType(u8);

impl UnknownWireType {
    /// Rejects bytes that identify a documented type.
    ///
    /// # Errors
    ///
    /// Returns [`UnknownWireTypeError::KnownType`] for documented bytes.
    pub const fn new(value: u8) -> Result<Self, UnknownWireTypeError> {
        if WireType::new(value).known().is_some() {
            Err(UnknownWireTypeError::KnownType)
        } else {
            Ok(Self(value))
        }
    }
    /// Returns the preserved unassigned type byte.
    #[must_use]
    pub const fn value(self) -> u8 {
        self.0
    }
}

/// Failure to create an unknown-type value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownWireTypeError {
    /// The supplied byte is a known protocol type.
    KnownType,
}
