//! Typed-conversion failures.

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
