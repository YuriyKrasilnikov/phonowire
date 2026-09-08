//! Bounded incoming `AudioSocket` reception on a caller-owned Linux worker thread.
//!
//! The worker sends owned wire observations and accepted session events through
//! a bounded queue. Consumers control how long the shared byte budget remains
//! charged by retaining or dropping those records.

mod config;
mod connection;
mod driver;
mod handoff;
mod memory;
mod record;
mod scheduler;

pub use config::{Limits, LimitsError, READ_BYTES};
pub use driver::{Receiver, ReceiverError, ReceiverFailure, RunSummary, StopCause, StopHandle};
pub use handoff::Records;
pub use memory::{BudgetError, ByteBudget, OwnedBytes};
pub use record::{ConnectionId, EndReason, Record, RecordKind, WireOffset};
