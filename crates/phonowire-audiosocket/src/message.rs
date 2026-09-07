//! Validated typed message values.
use crate::{AudioPayload, RawEnvelopeError, SampleRate, UnknownWireType};

/// A sixteen-byte protocol UUID with no application identity interpretation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Uuid([u8; 16]);

impl Uuid {
    /// Creates a UUID from all sixteen protocol bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
    /// Returns exact wire bytes.
    #[must_use]
    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

/// One validated ASCII DTMF byte.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Dtmf(u8);

impl Dtmf {
    pub(crate) const fn new(value: u8) -> Self {
        Self(value)
    }
    /// Returns the original ASCII byte.
    #[must_use]
    pub const fn value(self) -> u8 {
        self.0
    }
}

/// A borrowed opaque payload whose length fits the wire's `u16` field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpaquePayload<'a>(&'a [u8]);

impl<'a> OpaquePayload<'a> {
    /// Validates an opaque payload for a wire-representable typed message.
    ///
    /// # Errors
    ///
    /// Returns [`RawEnvelopeError::PayloadTooLong`] above the `u16` wire limit.
    pub fn new(bytes: &'a [u8]) -> Result<Self, RawEnvelopeError> {
        if bytes.len() > usize::from(u16::MAX) {
            return Err(RawEnvelopeError::PayloadTooLong);
        }
        Ok(Self(bytes))
    }

    /// Returns the original opaque bytes.
    #[must_use]
    pub const fn bytes(self) -> &'a [u8] {
        self.0
    }
}

/// A semantically validated `AudioSocket` message borrowing payloads where needed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedMessage<'a> {
    /// A zero-payload termination frame.
    Terminate,
    /// A protocol UUID frame.
    Uuid(Uuid),
    /// An ASCII DTMF frame.
    Dtmf(Dtmf),
    /// A PCM16LE frame with declared wire rate.
    Audio {
        /// Declared wire rate.
        rate: SampleRate,
        /// Borrowed PCM bytes.
        payload: AudioPayload<'a>,
    },
    /// A peer-provided opaque error payload.
    Error(OpaquePayload<'a>),
    /// An unassigned type and opaque payload.
    Unknown {
        /// Verified unassigned type.
        wire_type: UnknownWireType,
        /// Borrowed opaque payload.
        payload: OpaquePayload<'a>,
    },
}
