//! Incoming `AudioSocket` session policy.
use core::fmt;

use crate::{AudioPayload, Dtmf, OpaquePayload, SampleRate, TypedMessage, UnknownWireType, Uuid};

/// The reason an incoming session ended without a policy rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionEnd<'a> {
    /// The peer sent a terminate message.
    Terminate,
    /// The peer sent an opaque error message.
    PeerError(OpaquePayload<'a>),
    /// The caller reported clean end of the decoded input.
    EndOfInput,
}

/// An accepted incoming-session event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncomingEvent<'a> {
    /// The first UUID established the session identity.
    Started(Uuid),
    /// An 8 kHz PCM payload belonging to the established identity.
    Audio {
        /// The established protocol UUID.
        uuid: Uuid,
        /// The original PCM payload.
        payload: AudioPayload<'a>,
    },
    /// An accepted DTMF byte belonging to the established identity.
    Dtmf {
        /// The established protocol UUID.
        uuid: Uuid,
        /// The original DTMF byte.
        digit: Dtmf,
    },
    /// The session ended without a policy rejection.
    Ended {
        /// The UUID established before ending, if one was received.
        uuid: Option<Uuid>,
        /// The distinct ending reason.
        reason: SessionEnd<'a>,
    },
}

/// A terminal incoming-session policy rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncomingSessionError {
    /// Audio or DTMF arrived before a UUID.
    MissingUuid,
    /// A second UUID arrived after the session identity was established.
    DuplicateUuid,
    /// Audio used a rate outside the incoming profile.
    UnsupportedRate(SampleRate),
    /// DTMF used a byte outside the incoming profile.
    UnsupportedDigit(Dtmf),
    /// An unassigned wire type arrived.
    UnsupportedType(UnknownWireType),
    /// A message or EOF arrived after the session ended.
    AfterEnd,
}

impl fmt::Display for IncomingSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingUuid => formatter.write_str("audio or DTMF arrived before UUID"),
            Self::DuplicateUuid => formatter.write_str("a second UUID arrived in the session"),
            Self::UnsupportedRate(_) => {
                formatter.write_str("audio rate is outside the incoming profile")
            }
            Self::UnsupportedDigit(_) => {
                formatter.write_str("DTMF digit is outside the incoming profile")
            }
            Self::UnsupportedType(_) => {
                formatter.write_str("wire type is outside the incoming profile")
            }
            Self::AfterEnd => formatter.write_str("message arrived after the session ended"),
        }
    }
}

impl core::error::Error for IncomingSessionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionState {
    AwaitingUuid,
    Active(Uuid),
    Ended,
}

/// Stateful AP1 policy for messages received from an `AudioSocket` peer.
///
/// The caller supplies already validated [`TypedMessage`] values. This type does
/// not decode bytes, generate samples, resample audio, infer reconnections, keep
/// clocks, or perform I/O. Call [`IncomingSession::end_of_input`] only after a
/// byte decoder has successfully finished at a frame boundary; a decoder failure
/// remains distinct from a clean session end.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IncomingSession {
    state: SessionState,
}

impl IncomingSession {
    /// Creates a session that requires a UUID before media or DTMF.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: SessionState::AwaitingUuid,
        }
    }

    /// Returns the established UUID, if the session is active.
    #[must_use]
    pub const fn identity(&self) -> Option<Uuid> {
        match self.state {
            SessionState::AwaitingUuid | SessionState::Ended => None,
            SessionState::Active(uuid) => Some(uuid),
        }
    }

    /// Applies one typed peer message to the incoming policy.
    ///
    /// # Errors
    ///
    /// Returns a terminal [`IncomingSessionError`] for a rejected message or a
    /// message after this session has already ended.
    pub fn receive<'a>(
        &mut self,
        message: TypedMessage<'a>,
    ) -> Result<IncomingEvent<'a>, IncomingSessionError> {
        if matches!(self.state, SessionState::Ended) {
            return Err(IncomingSessionError::AfterEnd);
        }
        match message {
            TypedMessage::Terminate => Ok(self.end(SessionEnd::Terminate)),
            TypedMessage::Error(payload) => Ok(self.end(SessionEnd::PeerError(payload))),
            TypedMessage::Uuid(uuid) => self.uuid(uuid),
            TypedMessage::Audio { rate, payload } => self.audio(rate, payload),
            TypedMessage::Dtmf(digit) => self.dtmf(digit),
            TypedMessage::Unknown { wire_type, .. } => {
                self.reject(IncomingSessionError::UnsupportedType(wire_type))
            }
        }
    }

    /// Ends a successfully decoded input stream at its frame boundary.
    ///
    /// # Errors
    ///
    /// Returns [`IncomingSessionError::AfterEnd`] when the session already ended.
    pub const fn end_of_input(&mut self) -> Result<IncomingEvent<'static>, IncomingSessionError> {
        if matches!(self.state, SessionState::Ended) {
            return Err(IncomingSessionError::AfterEnd);
        }
        Ok(self.end(SessionEnd::EndOfInput))
    }

    const fn uuid<'a>(&mut self, uuid: Uuid) -> Result<IncomingEvent<'a>, IncomingSessionError> {
        match self.state {
            SessionState::AwaitingUuid => {
                self.state = SessionState::Active(uuid);
                Ok(IncomingEvent::Started(uuid))
            }
            SessionState::Active(_) => self.reject(IncomingSessionError::DuplicateUuid),
            SessionState::Ended => Err(IncomingSessionError::AfterEnd),
        }
    }

    fn audio<'a>(
        &mut self,
        rate: SampleRate,
        payload: AudioPayload<'a>,
    ) -> Result<IncomingEvent<'a>, IncomingSessionError> {
        let uuid = match self.state {
            SessionState::AwaitingUuid => return self.reject(IncomingSessionError::MissingUuid),
            SessionState::Active(uuid) => uuid,
            SessionState::Ended => return Err(IncomingSessionError::AfterEnd),
        };
        if rate != SampleRate::Khz8 {
            return self.reject(IncomingSessionError::UnsupportedRate(rate));
        }
        Ok(IncomingEvent::Audio { uuid, payload })
    }

    const fn dtmf<'a>(&mut self, digit: Dtmf) -> Result<IncomingEvent<'a>, IncomingSessionError> {
        let uuid = match self.state {
            SessionState::AwaitingUuid => return self.reject(IncomingSessionError::MissingUuid),
            SessionState::Active(uuid) => uuid,
            SessionState::Ended => return Err(IncomingSessionError::AfterEnd),
        };
        if !matches!(digit.value(), b'0'..=b'9' | b'*' | b'#' | b'A'..=b'D') {
            return self.reject(IncomingSessionError::UnsupportedDigit(digit));
        }
        Ok(IncomingEvent::Dtmf { uuid, digit })
    }

    const fn end<'a>(&mut self, reason: SessionEnd<'a>) -> IncomingEvent<'a> {
        let uuid = self.identity();
        self.state = SessionState::Ended;
        IncomingEvent::Ended { uuid, reason }
    }

    const fn reject<'a>(
        &mut self,
        error: IncomingSessionError,
    ) -> Result<IncomingEvent<'a>, IncomingSessionError> {
        self.state = SessionState::Ended;
        Err(error)
    }
}

impl Default for IncomingSession {
    fn default() -> Self {
        Self::new()
    }
}
