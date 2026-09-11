//! Decode literal peer chunks into an AP1 incoming session without I/O.
use core::fmt;

use phonowire_audiosocket::{
    DecodeError, DecodeOutcome, Decoder, FinishError, IncomingEvent, IncomingSession,
    IncomingSessionError, SessionEnd,
};

const UUID: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
const BYTES: [u8; 30] = [
    0x01, 0x00, 0x10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0x10, 0x00, 0x04, 0x00,
    0x80, 0xff, 0x7f, 0x03, 0x00, 0x01, b'5',
];

#[derive(Debug)]
enum ExampleError {
    /// Decoding did not yield a valid typed message.
    Decode(DecodeError),
    /// Session policy rejected a decoded message.
    Session(IncomingSessionError),
    /// Input ended outside a frame boundary.
    Finish(FinishError),
    /// Decoded events did not match the literal input.
    Sequence,
}

impl fmt::Display for ExampleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(error) => write!(formatter, "decode error: {error}"),
            Self::Session(error) => write!(formatter, "session error: {error}"),
            Self::Finish(error) => write!(formatter, "finish error: {error}"),
            Self::Sequence => formatter.write_str("unexpected incoming event sequence"),
        }
    }
}

impl core::error::Error for ExampleError {}

impl From<DecodeError> for ExampleError {
    fn from(error: DecodeError) -> Self {
        Self::Decode(error)
    }
}

impl From<IncomingSessionError> for ExampleError {
    fn from(error: IncomingSessionError) -> Self {
        Self::Session(error)
    }
}

impl From<FinishError> for ExampleError {
    fn from(error: FinishError) -> Self {
        Self::Finish(error)
    }
}

fn main() -> Result<(), ExampleError> {
    let mut scratch = [0_u8; 16];
    let mut decoder = Decoder::new(&mut scratch);
    let mut session = IncomingSession::new();
    let mut event_index = 0;
    let mut start = 0;

    for end in [2, 19, 25, 30] {
        let mut input = &BYTES[start..end];
        start = end;
        while !input.is_empty() {
            match decoder.feed(&mut input)? {
                DecodeOutcome::NeedInput => {}
                DecodeOutcome::Frame(message) => {
                    let event = session.receive(message)?;
                    verify_event(event, event_index)?;
                    event_index += 1;
                }
            }
        }
    }

    if event_index != 3 {
        return Err(ExampleError::Sequence);
    }
    decoder.finish()?;
    match session.end_of_input()? {
        IncomingEvent::Ended {
            uuid: Some(uuid),
            reason: SessionEnd::EndOfInput,
        } if uuid.bytes() == UUID => Ok(()),
        IncomingEvent::Ended {
            uuid: Some(_) | None,
            reason: SessionEnd::EndOfInput,
        }
        | IncomingEvent::Ended {
            reason: SessionEnd::Terminate,
            ..
        }
        | IncomingEvent::Ended {
            reason: SessionEnd::PeerError(_),
            ..
        }
        | IncomingEvent::Started(_)
        | IncomingEvent::Audio { .. }
        | IncomingEvent::Dtmf { .. } => Err(ExampleError::Sequence),
    }
}

fn verify_event(event: IncomingEvent<'_>, event_index: usize) -> Result<(), ExampleError> {
    match (event_index, event) {
        (0, IncomingEvent::Started(uuid)) if uuid.bytes() == UUID => Ok(()),
        (1, IncomingEvent::Audio { uuid, payload, .. })
            if uuid.bytes() == UUID && payload.bytes() == [0x00, 0x80, 0xff, 0x7f] =>
        {
            Ok(())
        }
        (2, IncomingEvent::Dtmf { uuid, digit })
            if uuid.bytes() == UUID && digit.value() == b'5' =>
        {
            Ok(())
        }
        (
            _,
            IncomingEvent::Started(_)
            | IncomingEvent::Audio { .. }
            | IncomingEvent::Dtmf { .. }
            | IncomingEvent::Ended {
                reason: SessionEnd::Terminate,
                ..
            }
            | IncomingEvent::Ended {
                reason: SessionEnd::PeerError(_),
                ..
            }
            | IncomingEvent::Ended {
                reason: SessionEnd::EndOfInput,
                ..
            },
        ) => Err(ExampleError::Sequence),
    }
}
