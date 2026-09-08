//! Validated per-worker resource and scheduling capacities.
use std::fmt;
use std::num::{NonZeroU16, NonZeroUsize};

/// Bytes in each connection's transport read buffer.
pub const READ_BYTES: usize = 4096;

/// Maximum number of readiness events collected in one poll.
pub const EVENT_BATCH: usize = 128;
const UUID_BYTES: u16 = 16;

/// A configuration that cannot admit the required incoming UUID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LimitsError {
    /// The scratch capacity is smaller than the sixteen-byte UUID payload.
    PayloadBelowUuid,
    /// Fixed buffer capacity for all admitted connections exceeds `usize`.
    FixedStorageOverflow,
}

impl fmt::Display for LimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadBelowUuid => {
                formatter.write_str("payload capacity is below 16 UUID bytes")
            }
            Self::FixedStorageOverflow => {
                formatter.write_str("fixed buffer capacity exceeds usize")
            }
        }
    }
}

impl std::error::Error for LimitsError {}

/// Per-worker connection, handoff, payload and turn bounds.
///
/// The payload capacity is a caller-selected resource limit. It does not change
/// the wire protocol's sixteen-bit length domain. Fixed buffer storage is bounded
/// separately from the shared byte budget for retained output records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    max_connections: NonZeroUsize,
    queue_slots: NonZeroUsize,
    payload_capacity: NonZeroU16,
    turn_steps: NonZeroUsize,
    fixed_buffer_capacity: usize,
}

impl Limits {
    /// Validates capacities for incoming UUID and audio observations.
    ///
    /// One step bounds a transport attempt or one parsing/copying/handoff action.
    /// The driver retains ready work when the turn's step budget is exhausted.
    ///
    /// # Errors
    ///
    /// Rejects a payload capacity below sixteen or an overflowing aggregate
    /// fixed-buffer capacity.
    pub fn new(
        max_connections: NonZeroUsize,
        queue_slots: NonZeroUsize,
        payload_capacity: NonZeroU16,
        turn_steps: NonZeroUsize,
    ) -> Result<Self, LimitsError> {
        if payload_capacity.get() < UUID_BYTES {
            return Err(LimitsError::PayloadBelowUuid);
        }
        let fixed_buffer_capacity = max_connections
            .get()
            .checked_mul(READ_BYTES + usize::from(payload_capacity.get()))
            .ok_or(LimitsError::FixedStorageOverflow)?;
        Ok(Self {
            max_connections,
            queue_slots,
            payload_capacity,
            turn_steps,
            fixed_buffer_capacity,
        })
    }

    /// Maximum simultaneously admitted connections.
    #[must_use]
    pub const fn max_connections(self) -> usize {
        self.max_connections.get()
    }

    /// Maximum records waiting in the handoff channel.
    #[must_use]
    pub const fn queue_slots(self) -> usize {
        self.queue_slots.get()
    }

    pub(crate) const fn channel_capacity(self) -> NonZeroUsize {
        self.queue_slots
    }

    /// Maximum payload bytes that one decoder can assemble.
    #[must_use]
    pub fn payload_capacity(self) -> usize {
        usize::from(self.payload_capacity.get())
    }

    /// Maximum accounted steps before a ready task yields its turn.
    #[must_use]
    pub const fn turn_steps(self) -> usize {
        self.turn_steps.get()
    }

    /// Upper bound of accessible read and decoder buffer capacity for this worker.
    ///
    /// Task metadata, maps, queue entries, allocator bookkeeping, stacks and OS
    /// socket buffers are outside this number. Retained output uses `ByteBudget`.
    #[must_use]
    pub const fn fixed_buffer_capacity(self) -> usize {
        self.fixed_buffer_capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("test count is positive")
    }

    #[test]
    fn fixed_storage_bound_covers_every_admitted_buffer() {
        let limits = Limits::new(
            count(3),
            count(1),
            NonZeroU16::new(320).expect("audio capacity is positive"),
            count(2),
        )
        .expect("capacities admit the profile");
        assert_eq!(limits.fixed_buffer_capacity(), 3 * (4096 + 320));
        assert_eq!(limits.max_connections(), 3);
    }

    #[test]
    fn unusable_uuid_and_overflowing_fixed_storage_are_refused() {
        assert_eq!(
            Limits::new(count(1), count(1), NonZeroU16::MIN, count(1)),
            Err(LimitsError::PayloadBelowUuid)
        );
        assert_eq!(
            Limits::new(
                count(usize::MAX),
                count(1),
                NonZeroU16::new(16).expect("UUID length is positive"),
                count(1)
            ),
            Err(LimitsError::FixedStorageOverflow)
        );
    }
}
