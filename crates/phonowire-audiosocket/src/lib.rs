#![no_std]
#![forbid(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn, missing_docs)]
//! Borrowed, allocation-free `AudioSocket` wire values and streaming framing.
//!
//! ```
//! use phonowire_audiosocket::{RawEnvelope, TypedMessage, WireType};
//! let raw = RawEnvelope::new(WireType::TERMINATE, &[]).expect("small payload");
//! assert_eq!(raw.typed(), Ok(TypedMessage::Terminate));
//! ```

mod audio;
mod decoder;
mod encoder;
pub(crate) mod envelope;
mod error;
mod message;
mod session;
mod wire_type;

pub(crate) const HEADER_BYTES: usize = 3;
pub(crate) const MAX_BODY_BYTES: usize = 65_535;

pub use audio::{AudioPayload, AudioPayloadError, SampleRate};
pub use decoder::{DecodeError, DecodeOutcome, Decoder, FinishError, RawDecoder};
pub use encoder::{EncodeError, encode, encode_raw};
pub use envelope::{RawEnvelope, RawEnvelopeError};
pub use error::TypedMessageError;
pub use message::{Dtmf, OpaquePayload, TypedMessage, Uuid};
pub use session::{
    IncomingEvent, IncomingProfile, IncomingSession, IncomingSessionError, SessionEnd,
};
pub use wire_type::{KnownWireType, UnknownWireType, UnknownWireTypeError, WireType};
