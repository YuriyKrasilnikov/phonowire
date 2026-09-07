//! Incremental bounded decoding of one wire frame at a time.
use core::fmt;

use crate::envelope::validate_typed_length;
use crate::{RawEnvelope, TypedMessage, TypedMessageError, WireType};

/// The result of one non-terminal decoder feed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeOutcome<Message> {
    /// More bytes are needed to complete the current header or body.
    NeedInput,
    /// One complete borrowed message is available.
    Frame(Message),
}

/// A terminal error from `feed`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeError {
    /// A known type has an invalid declared body length.
    InvalidLength(TypedMessageError),
    /// The caller-owned scratch cannot contain the declared body.
    ResourceLimit {
        /// Declared body bytes required.
        required: usize,
        /// Available scratch bytes.
        capacity: usize,
    },
    /// A complete known body violates typed content rules.
    InvalidContent(TypedMessageError),
    /// An earlier terminal error has made this decoder unusable.
    Failed,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength(error) => {
                write!(formatter, "invalid declared message length: {error}")
            }
            Self::ResourceLimit { required, capacity } => {
                write!(
                    formatter,
                    "scratch capacity {capacity} is below required body length {required}"
                )
            }
            Self::InvalidContent(error) => write!(formatter, "invalid message content: {error}"),
            Self::Failed => formatter.write_str("decoder is in a failed state"),
        }
    }
}

impl core::error::Error for DecodeError {}

/// Result of explicitly ending an input stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinishError {
    /// EOF arrived in a partial three-byte header.
    TruncatedHeader {
        /// Header byte count.
        expected: usize,
        /// Header bytes received.
        received: usize,
    },
    /// EOF arrived in a partial body.
    TruncatedPayload {
        /// Declared body byte count.
        expected: usize,
        /// Body bytes received.
        received: usize,
    },
    /// A prior terminal error occurred.
    Failed,
}

impl fmt::Display for FinishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TruncatedHeader { expected, received } => {
                write!(
                    formatter,
                    "header ended after {received} of {expected} bytes"
                )
            }
            Self::TruncatedPayload { expected, received } => {
                write!(
                    formatter,
                    "payload ended after {received} of {expected} bytes"
                )
            }
            Self::Failed => formatter.write_str("decoder is in a failed state"),
        }
    }
}

impl core::error::Error for FinishError {}

/// A streaming decoder that preserves every wire envelope.
pub struct RawDecoder<'storage> {
    framer: Framer<'storage>,
}

impl<'storage> RawDecoder<'storage> {
    /// Creates a raw decoder using caller-owned body scratch storage.
    #[must_use]
    pub const fn new(scratch: &'storage mut [u8]) -> Self {
        Self {
            framer: Framer::new(scratch),
        }
    }

    /// Consumes one complete raw frame at most.
    ///
    /// `input` advances only by bytes consumed during this call.
    ///
    /// # Errors
    ///
    /// Returns a terminal capacity error after consuming the full header and no
    /// body bytes. Every later call returns [`DecodeError::Failed`].
    ///
    /// ```compile_fail
    /// use phonowire_audiosocket::{DecodeOutcome, RawDecoder};
    /// let mut scratch = [0_u8; 1];
    /// let mut decoder = RawDecoder::new(&mut scratch);
    /// let mut input: &[u8] = &[2, 0, 0];
    /// let frame = match decoder.feed(&mut input) {
    ///     Ok(DecodeOutcome::Frame(frame)) => frame,
    ///     _ => return,
    /// };
    /// let _ = decoder.feed(&mut input);
    /// assert_eq!(frame.payload(), &[]);
    /// ```
    pub fn feed<'decoder>(
        &'decoder mut self,
        input: &mut &[u8],
    ) -> Result<DecodeOutcome<RawEnvelope<'decoder>>, DecodeError> {
        match self.framer.feed(input, |_, _| Ok(()))? {
            Some(frame) => Ok(DecodeOutcome::Frame(Framer::raw(
                self.framer.scratch,
                frame,
            ))),
            None => Ok(DecodeOutcome::NeedInput),
        }
    }

    /// Finalizes the decoder at an explicit input boundary.
    ///
    /// # Errors
    ///
    /// Returns the unfinished header or payload size when EOF truncates it.
    pub const fn finish(self) -> Result<(), FinishError> {
        self.framer.finish()
    }
}

/// A streaming decoder that returns only semantically valid typed messages.
pub struct Decoder<'storage> {
    framer: Framer<'storage>,
}

impl<'storage> Decoder<'storage> {
    /// Creates a strict typed decoder using caller-owned body scratch storage.
    #[must_use]
    pub const fn new(scratch: &'storage mut [u8]) -> Self {
        Self {
            framer: Framer::new(scratch),
        }
    }

    /// Consumes one complete typed frame at most.
    ///
    /// `input` advances only by bytes consumed during this call.
    ///
    /// # Errors
    ///
    /// Returns a terminal length error after consuming the header and before
    /// consuming a body. Invalid typed content consumes its complete body first.
    pub fn feed<'decoder>(
        &'decoder mut self,
        input: &mut &[u8],
    ) -> Result<DecodeOutcome<TypedMessage<'decoder>>, DecodeError> {
        let completed = self.framer.feed(input, validate_typed_length)?;
        match completed {
            None => Ok(DecodeOutcome::NeedInput),
            Some(frame) => {
                let raw = Framer::raw(self.framer.scratch, frame);
                raw.typed().map_or_else(
                    |error| {
                        self.framer.state = FramerState::Failed;
                        Err(DecodeError::InvalidContent(error))
                    },
                    |message| Ok(DecodeOutcome::Frame(message)),
                )
            }
        }
    }

    /// Finalizes the decoder at an explicit input boundary.
    ///
    /// # Errors
    ///
    /// Returns the unfinished header or payload size when EOF truncates it.
    pub const fn finish(self) -> Result<(), FinishError> {
        self.framer.finish()
    }
}

#[derive(Clone, Copy)]
struct CompletedFrame {
    wire_type: WireType,
    length: usize,
}

enum FramerState {
    Header {
        received: usize,
    },
    Body {
        wire_type: WireType,
        length: usize,
        received: usize,
    },
    Failed,
}

struct Framer<'storage> {
    scratch: &'storage mut [u8],
    header: [u8; crate::HEADER_BYTES],
    state: FramerState,
}

impl<'storage> Framer<'storage> {
    const fn new(scratch: &'storage mut [u8]) -> Self {
        Self {
            scratch,
            header: [0; crate::HEADER_BYTES],
            state: FramerState::Header { received: 0 },
        }
    }

    fn feed(
        &mut self,
        input: &mut &[u8],
        validate_header: fn(WireType, usize) -> Result<(), TypedMessageError>,
    ) -> Result<Option<CompletedFrame>, DecodeError> {
        loop {
            match self.state {
                FramerState::Failed => return Err(DecodeError::Failed),
                FramerState::Header { received } => {
                    let copied = copy_prefix(&mut self.header[received..], input);
                    let received = received + copied;
                    if received < crate::HEADER_BYTES {
                        self.state = FramerState::Header { received };
                        return Ok(None);
                    }
                    let wire_type = WireType::new(self.header[0]);
                    let length = usize::from(u16::from_be_bytes([self.header[1], self.header[2]]));
                    if let Err(error) = validate_header(wire_type, length) {
                        self.fail();
                        return Err(DecodeError::InvalidLength(error));
                    }
                    if length > self.scratch.len() {
                        self.fail();
                        return Err(DecodeError::ResourceLimit {
                            required: length,
                            capacity: self.scratch.len(),
                        });
                    }
                    if length == 0 {
                        self.state = FramerState::Header { received: 0 };
                        return Ok(Some(CompletedFrame { wire_type, length }));
                    }
                    self.state = FramerState::Body {
                        wire_type,
                        length,
                        received: 0,
                    };
                }
                FramerState::Body {
                    wire_type,
                    length,
                    received,
                } => {
                    let copied = copy_prefix(&mut self.scratch[received..length], input);
                    let received = received + copied;
                    if received < length {
                        self.state = FramerState::Body {
                            wire_type,
                            length,
                            received,
                        };
                        return Ok(None);
                    }
                    self.state = FramerState::Header { received: 0 };
                    return Ok(Some(CompletedFrame { wire_type, length }));
                }
            }
        }
    }

    fn raw(scratch: &[u8], frame: CompletedFrame) -> RawEnvelope<'_> {
        RawEnvelope::from_wire_bounded(frame.wire_type, &scratch[..frame.length])
    }

    const fn fail(&mut self) {
        self.state = FramerState::Failed;
    }

    const fn finish(self) -> Result<(), FinishError> {
        match self.state {
            FramerState::Header { received: 0 } => Ok(()),
            FramerState::Header { received } => Err(FinishError::TruncatedHeader {
                expected: crate::HEADER_BYTES,
                received,
            }),
            FramerState::Body {
                length, received, ..
            } => Err(FinishError::TruncatedPayload {
                expected: length,
                received,
            }),
            FramerState::Failed => Err(FinishError::Failed),
        }
    }
}

fn copy_prefix(destination: &mut [u8], input: &mut &[u8]) -> usize {
    let count = core::cmp::min(destination.len(), input.len());
    let (prefix, remainder) = input.split_at(count);
    destination[..count].copy_from_slice(prefix);
    *input = remainder;
    count
}
