//! Owned observations produced by the receiver worker.
use std::io;
use std::net::SocketAddr;
use std::time::Instant;

use phonowire_audiosocket::{
    DecodeError, Dtmf, FinishError, IncomingSessionError, SampleRate, TypedMessageError, Uuid,
};

use crate::{BudgetError, OwnedBytes};

/// A worker-scoped connection identity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ConnectionId {
    instance: u64,
    sequence: usize,
}

impl ConnectionId {
    pub(crate) const fn new(instance: u64, sequence: usize) -> Self {
        Self { instance, sequence }
    }

    /// Returns the receiver instance that admitted this connection.
    #[must_use]
    pub const fn instance(self) -> u64 {
        self.instance
    }

    /// Returns the connection sequence within its receiver instance.
    #[must_use]
    pub const fn sequence(self) -> usize {
        self.sequence
    }
}

/// An absolute transport-byte offset for one connection.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WireOffset(u64);

impl WireOffset {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the absolute byte offset.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// An owned receiver observation.
#[derive(Debug)]
pub struct Record {
    /// Connection to which this observation belongs.
    pub connection: ConnectionId,
    /// Start of a transport read or end of a consumed protocol frame.
    pub offset: WireOffset,
    /// Instant at which the receiver observed this result.
    pub observed_at: Instant,
    /// The observed transport, protocol, or terminal state.
    pub kind: RecordKind,
}

/// The owned content of a receiver observation.
#[derive(Debug)]
pub enum RecordKind {
    /// A connection was admitted from this peer.
    Connected {
        /// Remote peer address at admission.
        peer: SocketAddr,
    },
    /// Bytes returned by a transport read before protocol interpretation.
    Wire {
        /// Owned bytes from one read chunk.
        bytes: OwnedBytes,
    },
    /// The session accepted its first UUID.
    Started {
        /// Accepted session UUID.
        uuid: Uuid,
    },
    /// The session accepted PCM bytes at their declared wire rate.
    Audio {
        /// Session UUID.
        uuid: Uuid,
        /// Declared sample rate.
        rate: SampleRate,
        /// Owned PCM payload.
        bytes: OwnedBytes,
    },
    /// The session accepted a DTMF digit.
    Dtmf {
        /// Session UUID.
        uuid: Uuid,
        /// Accepted digit.
        digit: Dtmf,
    },
    /// The connection reached a distinct terminal result.
    Ended {
        /// UUID established before termination, if any.
        uuid: Option<Uuid>,
        /// Terminal reason.
        reason: EndReason,
    },
}

/// A distinct connection terminal result.
#[derive(Debug)]
pub enum EndReason {
    /// EOF followed a successful decoder finish.
    CleanEof,
    /// The peer sent a terminate message.
    Terminate,
    /// The peer sent an opaque error payload.
    PeerError(OwnedBytes),
    /// Typed conversion rejected a completed frame.
    InvalidMessage(TypedMessageError),
    /// Raw framing failed.
    Decode(DecodeError),
    /// EOF interrupted framing.
    Truncated(FinishError),
    /// Incoming session policy rejected a typed frame.
    Policy(IncomingSessionError),
    /// A socket operation failed.
    Transport(io::Error),
    /// Output retention could not reserve the requested bytes.
    ResourceRefused(BudgetError),
    /// A checked transport offset could no longer advance.
    OffsetExhausted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_and_offsets_order_by_their_explicit_components() {
        let earlier = ConnectionId::new(3, 4);
        let later = ConnectionId::new(3, 5);
        assert!(earlier < later);
        assert_eq!(earlier.instance(), 3);
        assert_eq!(earlier.sequence(), 4);
        assert!(WireOffset::new(8) < WireOffset::new(9));
        assert_eq!(WireOffset::new(9).get(), 9);
    }
}
