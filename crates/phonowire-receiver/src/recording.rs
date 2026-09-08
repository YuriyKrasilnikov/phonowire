//! Consumer-owned wire, PCM WAVE and diagnostic output.
use std::fmt;
use std::io::{self, Seek, SeekFrom, Write};

use phonowire_audiosocket::{SampleRate, Uuid};

use crate::{ConnectionId, EndReason, Record, RecordKind};

const HEADER_BYTES: usize = 44;
const RIFF_OVERHEAD: u32 = 36;
const PCM_FORMAT: u16 = 1;
const CHANNELS: u16 = 1;
const SAMPLE_RATE: u32 = 8000;
const SAMPLE_BYTES: u16 = 2;
const SAMPLE_BITS: u16 = 16;
const FORMAT_BYTES: u32 = 16;

/// The output operation at which a recording stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordingStage {
    /// A record violated identity, ordering or format requirements.
    Validate,
    /// Writing the initial provisional WAVE header.
    Header,
    /// Preserving observed transport bytes.
    Wire,
    /// Writing accepted PCM samples.
    Audio,
    /// Writing one diagnostic description.
    Events,
    /// Seeking to the beginning of the WAVE header.
    Seek,
    /// Replacing the provisional WAVE sizes.
    Patch,
    /// Flushing the wire writer.
    FlushWire,
    /// Flushing the WAVE writer.
    FlushWave,
    /// Flushing the diagnostic writer.
    FlushEvents,
}

/// A terminal observation class preserved in a recording summary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalKind {
    /// Clean transport EOF at a frame boundary.
    CleanEof,
    /// Explicit peer termination.
    Terminate,
    /// Opaque peer error.
    PeerError,
    /// Invalid typed message.
    InvalidMessage,
    /// Decoder failure.
    Decode,
    /// Truncated transport input.
    Truncated,
    /// Incoming policy rejection.
    Policy,
    /// Transport failure.
    Transport,
    /// Resource refusal.
    ResourceRefused,
    /// Exhausted byte offset.
    OffsetExhausted,
}

impl From<&EndReason> for TerminalKind {
    fn from(reason: &EndReason) -> Self {
        match reason {
            EndReason::CleanEof => Self::CleanEof,
            EndReason::Terminate => Self::Terminate,
            EndReason::PeerError(_) => Self::PeerError,
            EndReason::InvalidMessage(_) => Self::InvalidMessage,
            EndReason::Decode(_) => Self::Decode,
            EndReason::Truncated(_) => Self::Truncated,
            EndReason::Policy(_) => Self::Policy,
            EndReason::Transport(_) => Self::Transport,
            EndReason::ResourceRefused(_) => Self::ResourceRefused,
            EndReason::OffsetExhausted => Self::OffsetExhausted,
        }
    }
}

/// Whether the consumer observed a terminal record before finalization.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RecordingEnd {
    /// No terminal record was delivered; the accepted prefix is incomplete.
    #[default]
    Incomplete,
    /// A terminal record was accepted; its class does not imply successful media.
    Observed(TerminalKind),
}

/// Confirmed bytes accepted by writers and the observed terminal class.
///
/// These counts include successful prefixes before a later write failure.
/// Flush completion does not imply filesystem synchronization or durability.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecordingSummary {
    /// Observed transport bytes accepted by the wire writer.
    pub wire_bytes: u64,
    /// PCM bytes accepted by the WAVE writer, excluding headers.
    pub audio_bytes: u64,
    /// Diagnostic bytes accepted by the event writer.
    pub event_bytes: u64,
    /// Initial WAVE header bytes accepted by its writer.
    pub header_bytes: u64,
    /// Final WAVE header bytes accepted during size replacement.
    pub patch_bytes: u64,
    /// Terminal observation accepted by the recording adapter.
    pub end: RecordingEnd,
}

/// A rejected record or finalization request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordingViolation {
    /// The recording has already failed and cannot mutate another writer.
    AfterFailure,
    /// A record follows the terminal observation.
    AfterEnd,
    /// The record names a different connection.
    Connection,
    /// Connection/session records occurred in an invalid order.
    Order,
    /// The record does not match the established session UUID.
    Uuid,
    /// Wire or protocol offsets disagree with the preserved prefix.
    Offset,
    /// PCM is not even-length, mono 8 kHz PCM16LE.
    AudioFormat,
    /// PCM would exceed the RIFF WAVE length domain.
    WaveSize,
}

/// An output error or invalid record.
#[derive(Debug)]
pub enum RecordingFailure {
    /// A writer returned an I/O error.
    Io(io::Error),
    /// A record failed validation before its output operation.
    Invalid(RecordingViolation),
}

/// A stopped recording with the original cause and confirmed output counts.
#[derive(Debug)]
pub struct RecordingError {
    stage: RecordingStage,
    failure: RecordingFailure,
    summary: RecordingSummary,
}

impl RecordingError {
    /// Returns the failed operation.
    #[must_use]
    pub const fn stage(&self) -> RecordingStage {
        self.stage
    }
    /// Returns the original failure.
    #[must_use]
    pub const fn failure(&self) -> &RecordingFailure {
        &self.failure
    }
    /// Returns the confirmed output prefix at the failure boundary.
    #[must_use]
    pub const fn summary(&self) -> &RecordingSummary {
        &self.summary
    }
}

impl fmt::Display for RecordingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "recording {:?} failed: ", self.stage)?;
        match &self.failure {
            RecordingFailure::Io(error) => error.fmt(formatter),
            RecordingFailure::Invalid(violation) => write!(formatter, "{violation:?}"),
        }
    }
}
impl std::error::Error for RecordingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.failure {
            RecordingFailure::Io(error) => Some(error),
            RecordingFailure::Invalid(_) => None,
        }
    }
}

#[derive(Clone, Copy)]
enum State {
    AwaitingConnection,
    AwaitingUuid,
    Active(Uuid),
    Ended,
    Failed,
}

/// Records one connection into caller-owned wire, WAVE and diagnostic writers.
///
/// Supply empty writers positioned at zero. The adapter borrows each `Record`;
/// payload retention ends when the caller drops that record. Blocking writes run
/// on the consumer, independently of the receiver worker. Failure stops further
/// mutations. Dropping the adapter does not finalize or flush its outputs.
pub struct Recording<Wire: Write, Wave: Write + Seek, Events: Write> {
    id: ConnectionId,
    wire: Wire,
    wave: Wave,
    events: Events,
    state: State,
    last_event_offset: u64,
    summary: RecordingSummary,
}

impl<Wire: Write, Wave: Write + Seek, Events: Write> Recording<Wire, Wave, Events> {
    /// Creates a recorder and writes a provisional empty WAVE header.
    ///
    /// # Errors
    /// Returns the header writer's failure, including its accepted prefix.
    pub fn new(
        id: ConnectionId,
        wire: Wire,
        wave: Wave,
        events: Events,
    ) -> Result<Self, RecordingError> {
        let mut recording = Self {
            id,
            wire,
            wave,
            events,
            state: State::AwaitingConnection,
            last_event_offset: 0,
            summary: RecordingSummary::default(),
        };
        let header = wave_header(0).map_err(|violation| recording.invalid(violation))?;
        let result = write_count(
            &mut recording.wave,
            &header,
            &mut recording.summary.header_bytes,
        );
        recording.output_result(RecordingStage::Header, result)?;
        Ok(recording)
    }

    /// Preserves a validated record and appends its diagnostic metadata.
    ///
    /// # Errors
    /// Rejects wrong identity, ordering, offset or PCM format before that record
    /// writes output. An I/O failure preserves exact accepted prefixes and makes
    /// subsequent calls fail without touching writers.
    pub fn record(&mut self, record: &Record) -> Result<(), RecordingError> {
        let next = self
            .validate(record)
            .map_err(|violation| self.invalid(violation))?;
        let (stage, result) = match &record.kind {
            RecordKind::Wire { bytes } => (
                RecordingStage::Wire,
                write_count(
                    &mut self.wire,
                    bytes.as_slice(),
                    &mut self.summary.wire_bytes,
                ),
            ),
            RecordKind::Audio { bytes, .. } => (
                RecordingStage::Audio,
                write_count(
                    &mut self.wave,
                    bytes.as_slice(),
                    &mut self.summary.audio_bytes,
                ),
            ),
            RecordKind::Connected { .. }
            | RecordKind::Started { .. }
            | RecordKind::Dtmf { .. }
            | RecordKind::Ended { .. } => (RecordingStage::Events, Ok(())),
        };
        self.output_result(stage, result)?;
        let description = format!(
            "instance={} connection={} offset={} observed={:?} {}\n",
            self.id.instance(),
            self.id.sequence(),
            record.offset.get(),
            record.observed_at,
            Description(&record.kind)
        );
        let result = write_count(
            &mut self.events,
            description.as_bytes(),
            &mut self.summary.event_bytes,
        );
        self.output_result(RecordingStage::Events, result)?;
        if !matches!(record.kind, RecordKind::Wire { .. }) {
            self.last_event_offset = record.offset.get();
        }
        if let RecordKind::Ended { reason, .. } = &record.kind {
            self.summary.end = RecordingEnd::Observed(TerminalKind::from(reason));
        }
        self.state = next;
        Ok(())
    }

    /// Replaces WAVE sizes and flushes every output, preserving terminal meaning.
    ///
    /// Missing terminal input yields [`RecordingEnd::Incomplete`]. A truncated
    /// or rejected connection stays distinguishable after successful finalization.
    ///
    /// # Errors
    /// Returns prior failure, seek, header replacement or flush failure. No failed
    /// finalization is reported as success; already accepted bytes remain counted.
    pub fn finish(mut self) -> Result<RecordingSummary, RecordingError> {
        if matches!(self.state, State::Failed) {
            return Err(self.invalid(RecordingViolation::AfterFailure));
        }
        let header =
            wave_header(self.summary.audio_bytes).map_err(|violation| self.invalid(violation))?;
        let result = self.wave.seek(SeekFrom::Start(0)).map(|_| ());
        self.output_result(RecordingStage::Seek, result)?;
        let result = write_count(&mut self.wave, &header, &mut self.summary.patch_bytes);
        self.output_result(RecordingStage::Patch, result)?;
        let result = self.wire.flush();
        self.output_result(RecordingStage::FlushWire, result)?;
        let result = self.wave.flush();
        self.output_result(RecordingStage::FlushWave, result)?;
        let result = self.events.flush();
        self.output_result(RecordingStage::FlushEvents, result)?;
        Ok(self.summary)
    }

    fn validate(&self, record: &Record) -> Result<State, RecordingViolation> {
        let uuid = match self.state {
            State::Failed => return Err(RecordingViolation::AfterFailure),
            State::Ended => return Err(RecordingViolation::AfterEnd),
            State::AwaitingConnection | State::AwaitingUuid => None,
            State::Active(uuid) => Some(uuid),
        };
        if record.connection != self.id {
            return Err(RecordingViolation::Connection);
        }
        if matches!(self.state, State::AwaitingConnection)
            && !matches!(record.kind, RecordKind::Connected { .. })
        {
            return Err(RecordingViolation::Order);
        }
        let offset = record.offset.get();
        if !matches!(record.kind, RecordKind::Wire { .. })
            && (offset < self.last_event_offset || offset > self.summary.wire_bytes)
        {
            return Err(RecordingViolation::Offset);
        }
        match &record.kind {
            RecordKind::Connected { .. } => {
                if !matches!(self.state, State::AwaitingConnection) {
                    return Err(RecordingViolation::Order);
                }
                Ok(State::AwaitingUuid)
            }
            RecordKind::Wire { .. } => {
                if offset != self.summary.wire_bytes {
                    return Err(RecordingViolation::Offset);
                }
                Ok(self.state)
            }
            RecordKind::Started { uuid } => {
                if !matches!(self.state, State::AwaitingUuid) {
                    return Err(RecordingViolation::Order);
                }
                Ok(State::Active(*uuid))
            }
            RecordKind::Audio {
                uuid: identity,
                rate,
                bytes,
            } => {
                if uuid != Some(*identity) {
                    return Err(RecordingViolation::Uuid);
                }
                if *rate != SampleRate::Khz8
                    || bytes.as_slice().len() % usize::from(SAMPLE_BYTES) != 0
                {
                    return Err(RecordingViolation::AudioFormat);
                }
                let count = u64::try_from(bytes.as_slice().len())
                    .map_err(|_| RecordingViolation::WaveSize)?;
                let next = self
                    .summary
                    .audio_bytes
                    .checked_add(count)
                    .ok_or(RecordingViolation::WaveSize)?;
                wave_header(next)?;
                Ok(self.state)
            }
            RecordKind::Dtmf { uuid: identity, .. } => {
                if uuid != Some(*identity) {
                    return Err(RecordingViolation::Uuid);
                }
                Ok(self.state)
            }
            RecordKind::Ended { uuid: identity, .. } => {
                if uuid != *identity {
                    return Err(RecordingViolation::Uuid);
                }
                Ok(State::Ended)
            }
        }
    }

    const fn invalid(&mut self, violation: RecordingViolation) -> RecordingError {
        self.state = State::Failed;
        RecordingError {
            stage: RecordingStage::Validate,
            failure: RecordingFailure::Invalid(violation),
            summary: self.summary,
        }
    }

    fn output_result(
        &mut self,
        stage: RecordingStage,
        result: io::Result<()>,
    ) -> Result<(), RecordingError> {
        result.map_err(|error| {
            self.state = State::Failed;
            RecordingError {
                stage,
                failure: RecordingFailure::Io(error),
                summary: self.summary,
            }
        })
    }
}

fn write_count<Output: Write>(
    output: &mut Output,
    mut bytes: &[u8],
    count: &mut u64,
) -> io::Result<()> {
    let total =
        u64::try_from(bytes.len()).map_err(|_| io::Error::other("output count overflow"))?;
    count
        .checked_add(total)
        .ok_or_else(|| io::Error::other("output count overflow"))?;
    while !bytes.is_empty() {
        let written = match output.write(bytes) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Ok(written) => written,
            Err(error) => return Err(error),
        };
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "writer accepted no bytes",
            ));
        }
        assert!(
            written <= bytes.len(),
            "Write cannot accept more bytes than supplied"
        );
        *count += u64::try_from(written).expect("accepted prefix fits checked total");
        bytes = &bytes[written..];
    }
    Ok(())
}

fn wave_header(audio_bytes: u64) -> Result<[u8; HEADER_BYTES], RecordingViolation> {
    let data = u32::try_from(audio_bytes).map_err(|_| RecordingViolation::WaveSize)?;
    let riff = data
        .checked_add(RIFF_OVERHEAD)
        .ok_or(RecordingViolation::WaveSize)?;
    let mut header = [0; HEADER_BYTES];
    header[..4].copy_from_slice(b"RIFF");
    header[4..8].copy_from_slice(&riff.to_le_bytes());
    header[8..12].copy_from_slice(b"WAVE");
    header[12..16].copy_from_slice(b"fmt ");
    header[16..20].copy_from_slice(&FORMAT_BYTES.to_le_bytes());
    header[20..22].copy_from_slice(&PCM_FORMAT.to_le_bytes());
    header[22..24].copy_from_slice(&CHANNELS.to_le_bytes());
    header[24..28].copy_from_slice(&SAMPLE_RATE.to_le_bytes());
    header[28..32].copy_from_slice(&(SAMPLE_RATE * u32::from(SAMPLE_BYTES)).to_le_bytes());
    header[32..34].copy_from_slice(&SAMPLE_BYTES.to_le_bytes());
    header[34..36].copy_from_slice(&SAMPLE_BITS.to_le_bytes());
    header[36..40].copy_from_slice(b"data");
    header[40..44].copy_from_slice(&data.to_le_bytes());
    Ok(header)
}

struct Description<'a>(&'a RecordKind);
impl fmt::Display for Description<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            RecordKind::Connected { peer } => write!(formatter, "connected peer={peer}"),
            RecordKind::Wire { bytes } => {
                write!(formatter, "wire bytes={}", bytes.as_slice().len())
            }
            RecordKind::Started { uuid } => write!(formatter, "started uuid={:02x?}", uuid.bytes()),
            RecordKind::Audio { uuid, rate, bytes } => write!(
                formatter,
                "audio uuid={:02x?} rate={rate:?} bytes={}",
                uuid.bytes(),
                bytes.as_slice().len()
            ),
            RecordKind::Dtmf { uuid, digit } => write!(
                formatter,
                "dtmf uuid={:02x?} digit={:?}",
                uuid.bytes(),
                char::from(digit.value())
            ),
            RecordKind::Ended { uuid, reason } => write!(
                formatter,
                "ended uuid={uuid:?} reason={}",
                EndDescription(reason)
            ),
        }
    }
}
struct EndDescription<'a>(&'a EndReason);
impl fmt::Display for EndDescription<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            EndReason::CleanEof => formatter.write_str("clean-eof"),
            EndReason::Terminate => formatter.write_str("terminate"),
            EndReason::PeerError(bytes) => {
                write!(formatter, "peer-error bytes={}", bytes.as_slice().len())
            }
            EndReason::InvalidMessage(error) => {
                write!(formatter, "invalid-message detail={error:?}")
            }
            EndReason::Decode(error) => write!(formatter, "decode detail={error:?}"),
            EndReason::Truncated(error) => write!(formatter, "truncated detail={error:?}"),
            EndReason::Policy(error) => write!(formatter, "policy detail={error:?}"),
            EndReason::Transport(error) => {
                write!(formatter, "transport detail={:?}", error.to_string())
            }
            EndReason::ResourceRefused(error) => {
                write!(formatter, "resource-refused detail={error:?}")
            }
            EndReason::OffsetExhausted => formatter.write_str("offset-exhausted"),
        }
    }
}

#[cfg(test)]
#[path = "recording_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "recording_validation_tests.rs"]
mod validation_tests;
