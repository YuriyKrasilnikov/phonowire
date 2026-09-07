//! Typed-conversion failures.
use core::fmt;

/// A strict typed-message policy failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedMessageError {
    /// Termination must carry no bytes under the typed policy.
    TerminatePayload,
    /// UUID payload must contain exactly sixteen bytes.
    UuidLength,
    /// DTMF payload must contain exactly one byte.
    DtmfLength,
    /// DTMF byte must be ASCII.
    DtmfNotAscii,
    /// PCM16LE payload must have an even byte length.
    OddPcmLength,
    /// An internal raw classification was inconsistent.
    UnknownTypeClassification,
}

impl fmt::Display for TypedMessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TerminatePayload => "terminate payload is not empty",
            Self::UuidLength => "UUID payload does not contain sixteen bytes",
            Self::DtmfLength => "DTMF payload does not contain one byte",
            Self::DtmfNotAscii => "DTMF byte is not ASCII",
            Self::OddPcmLength => "PCM payload has an odd byte length",
            Self::UnknownTypeClassification => "wire type classification is inconsistent",
        })
    }
}

impl core::error::Error for TypedMessageError {}
