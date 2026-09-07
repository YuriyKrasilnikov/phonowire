#![no_std]
#![forbid(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn, missing_docs)]
//! Borrowed, allocation-free `AudioSocket` wire values.
//!
//! ```
//! use phonowire_audiosocket::{RawEnvelope, TypedMessage, WireType};
//! let raw = RawEnvelope::new(WireType::TERMINATE, &[]).expect("small payload");
//! assert_eq!(raw.typed(), Ok(TypedMessage::Terminate));
//! ```

mod audio;
mod envelope;
mod error;
mod message;
mod wire_type;

pub use audio::{AudioPayload, SampleRate};
pub use envelope::{RawEnvelope, RawEnvelopeError};
pub use error::TypedMessageError;
pub use message::{Dtmf, OpaquePayload, TypedMessage, Uuid};
pub use wire_type::{KnownWireType, UnknownWireType, UnknownWireTypeError, WireType};
