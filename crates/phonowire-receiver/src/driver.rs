//! Caller-thread receiver execution and explicit shutdown outcomes.
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll as TaskPoll, Waker};
use std::time::{Duration, Instant};

use mio::{Events, Interest, Poll, Token};

use crate::config::EVENT_BATCH;
use crate::connection;
use crate::handoff::{self, RecordSender};
use crate::scheduler::{
    Progress, Scheduler, Signal, Signals, TaskContext, TaskResources, WaitReason,
};
use crate::{BudgetError, ByteBudget, ConnectionId, Limits, Records};

const LISTENER: Token = Token(0);
const CONTROL: Token = Token(1);
const FIRST_CONNECTION: usize = 2;
const TURN_BATCH: usize = 128;
const STOP_RECHECK: Duration = Duration::from_millis(100);
static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(1);

/// The control event that stopped a worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopCause {
    /// The stop handle requested cancellation, including its destruction.
    Requested,
    /// The record consumer was destroyed.
    ConsumerDisconnected,
}

/// A bind or worker failure category.
#[derive(Debug)]
pub enum ReceiverFailure {
    /// Socket, readiness, or wake notification failed.
    Io(io::Error),
    /// The budget cannot retain one maximum output payload or read chunk.
    BudgetTooSmall(BudgetError),
    /// Receiver instance identities cannot be allocated without reuse.
    InstanceExhausted,
    /// Connection tokens cannot be allocated without reuse.
    TokenExhausted,
    /// Another active worker uses this output budget's notification slot.
    BudgetAlreadySubscribed,
    /// An observation counter cannot represent another result.
    CounterExhausted,
}

/// Counters for one worker invocation, including abandoned work.
///
/// Handoff means accepted by the record channel, not persisted by a consumer.
/// Byte retention is a snapshot after connection tasks have been destroyed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunSummary {
    /// Retryable transient accept errors observed during this run.
    pub transient_accept_errors: u64,
    /// Resource-pressure accept pauses entered during this run.
    pub resource_pause_entries: u64,
    /// All-retry bounded turns that installed an accept cooldown.
    pub retry_quota_backoffs: u64,
    /// Connections admitted to the worker.
    pub accepted: usize,
    /// Accepted sockets closed because the live connection bound was full.
    pub refused: usize,
    /// Connections whose terminal record entered the handoff channel.
    pub ended: usize,
    /// Connections destroyed without handing off a terminal record.
    pub abandoned: usize,
    /// Connections still active at the shutdown boundary.
    pub live: usize,
    /// Bytes observed by successful TCP reads.
    pub raw_bytes_read: u64,
    /// Raw bytes accepted by the record channel.
    pub raw_bytes_handed_off: u64,
    /// Observed raw bytes discarded before channel acceptance.
    pub undelivered_raw_bytes: u64,
    /// Records accepted by the channel.
    pub records_handed_off: u64,
    /// Constructed records discarded while waiting for a queue slot.
    pub undelivered_records: usize,
    /// Accessible output bytes still charged after task destruction.
    pub retained_output_bytes: usize,
    /// Upper bound of transport and decoder buffer capacity for this worker.
    pub fixed_buffer_capacity: usize,
    /// The observed control reason, when execution ended through control.
    pub stopped_by: Option<StopCause>,
    /// Whether all observation counters remain representable.
    pub counters_complete: bool,
}

impl Default for RunSummary {
    fn default() -> Self {
        Self {
            transient_accept_errors: 0,
            resource_pause_entries: 0,
            retry_quota_backoffs: 0,
            accepted: 0,
            refused: 0,
            ended: 0,
            abandoned: 0,
            live: 0,
            raw_bytes_read: 0,
            raw_bytes_handed_off: 0,
            undelivered_raw_bytes: 0,
            records_handed_off: 0,
            undelivered_records: 0,
            retained_output_bytes: 0,
            fixed_buffer_capacity: 0,
            stopped_by: None,
            counters_complete: true,
        }
    }
}

/// A failure together with the worker observations available at that boundary.
#[derive(Debug)]
pub struct ReceiverError {
    failure: ReceiverFailure,
    summary: Box<RunSummary>,
}

impl ReceiverError {
    /// Returns the cause of failure.
    #[must_use]
    pub const fn failure(&self) -> &ReceiverFailure {
        &self.failure
    }

    /// Returns observations collected before shutdown.
    #[must_use]
    pub fn summary(&self) -> &RunSummary {
        &self.summary
    }

    fn before_run(failure: ReceiverFailure) -> Self {
        Self {
            failure,
            summary: Box::new(RunSummary::default()),
        }
    }
}

impl fmt::Display for ReceiverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.failure {
            ReceiverFailure::Io(error) => write!(formatter, "receiver I/O failed: {error}"),
            ReceiverFailure::BudgetTooSmall(error) => {
                write!(formatter, "receiver budget is too small: {error}")
            }
            ReceiverFailure::InstanceExhausted => {
                formatter.write_str("receiver instance identities exhausted")
            }
            ReceiverFailure::TokenExhausted => {
                formatter.write_str("receiver connection tokens exhausted")
            }
            ReceiverFailure::BudgetAlreadySubscribed => {
                formatter.write_str("receiver budget already has an active worker")
            }
            ReceiverFailure::CounterExhausted => {
                formatter.write_str("receiver observation counters exhausted")
            }
        }
    }
}

impl std::error::Error for ReceiverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.failure {
            ReceiverFailure::Io(error) => Some(error),
            ReceiverFailure::BudgetTooSmall(error) => Some(error),
            ReceiverFailure::InstanceExhausted
            | ReceiverFailure::TokenExhausted
            | ReceiverFailure::BudgetAlreadySubscribed
            | ReceiverFailure::CounterExhausted => None,
        }
    }
}

/// Independent cancellation control for one receiver.
///
/// Dropping the handle requests stop. Queue capacity cannot prevent that request.
/// The worker checks a recorded wake failure at its next poll return; a 100 ms
/// poll timeout bounds its own retry interval, not OS scheduling or final delivery.
pub struct StopHandle {
    signals: Arc<Signals>,
}

impl StopHandle {
    /// Requests cancellation and notifies the worker's poller.
    ///
    /// # Errors
    ///
    /// Returns an I/O notification failure. The stop request remains recorded.
    pub fn stop(&self) -> io::Result<()> {
        self.signals.request(Signal::Stop)
    }
}

impl Drop for StopHandle {
    fn drop(&mut self) {
        self.signals.publish(Signal::Stop);
    }
}

/// A bound listener whose mutable connection tasks are created by [`Self::run`].
///
/// The receiver can move to a caller-owned thread before execution. It accepts
/// incoming plaintext `AudioSocket` with the codec's 8 kHz session policy.
pub struct Receiver {
    listener: TcpListener,
    local: SocketAddr,
    limits: Limits,
    budget: ByteBudget,
    signals: Arc<Signals>,
    sender: RecordSender,
    instance: u64,
}

impl Receiver {
    /// Binds a listener and creates its bounded record channel and stop handle.
    ///
    /// # Errors
    ///
    /// Returns invalid budget, exhausted instance identity, or socket setup errors.
    /// Another active subscriber to the budget is rejected when `run` starts.
    pub fn bind(
        address: SocketAddr,
        limits: Limits,
        budget: ByteBudget,
    ) -> Result<(Self, Records, StopHandle), ReceiverError> {
        let required = crate::READ_BYTES.max(limits.payload_capacity());
        if budget.capacity() < required {
            return Err(ReceiverError::before_run(ReceiverFailure::BudgetTooSmall(
                BudgetError::TooLarge {
                    required,
                    capacity: budget.capacity(),
                },
            )));
        }
        let instance = NEXT_INSTANCE
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ReceiverError::before_run(ReceiverFailure::InstanceExhausted))?;
        let listener = TcpListener::bind(address).map_err(bind_io)?;
        listener.set_nonblocking(true).map_err(bind_io)?;
        let local = listener.local_addr().map_err(bind_io)?;
        let signals = Signals::new();
        let (sender, records) = handoff::channel(
            limits.channel_capacity(),
            signals.waker(Signal::Queue),
            signals.waker(Signal::Disconnected),
        );
        Ok((
            Self {
                listener,
                local,
                limits,
                budget,
                signals: Arc::clone(&signals),
                sender,
                instance,
            },
            records,
            StopHandle { signals },
        ))
    }

    /// Returns the bound listener address.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local
    }

    /// Executes reception on the calling thread until stop, disconnection or error.
    ///
    /// The consumer must run independently when waiting for records. Stop cancels
    /// pending connection work; already queued records remain readable and charged.
    ///
    /// # Errors
    ///
    /// Known per-attempt accept errors preserve existing connections. Descriptor
    /// or memory pressure during accept pauses retries for 100 ms; an entire
    /// accept turn containing only retryable errors uses the same pause. Pending
    /// listener readiness is retained, and connections and stop remain serviced.
    /// This interval bounds worker retries, not OS scheduling or recovery time.
    ///
    /// Returns runtime setup, unclassified accept, socket registration, other
    /// socket, notification, identity or counter failure, together with the
    /// available summary. No runtime fallback is attempted.
    pub fn run(self) -> Result<RunSummary, ReceiverError> {
        self.run_with(SystemAccept)
    }

    fn run_with(self, mut accept: impl AcceptIo) -> Result<RunSummary, ReceiverError> {
        let poll = Poll::new().map_err(bind_io)?;
        let wake = Arc::new(mio::Waker::new(poll.registry(), CONTROL).map_err(bind_io)?);
        let _attachment = self.signals.attach(&wake);
        let _subscription = self
            .budget
            .subscribe(self.signals.waker(Signal::Bytes))
            .map_err(|_| ReceiverError::before_run(ReceiverFailure::BudgetAlreadySubscribed))?;
        let scheduler = Scheduler::new(self.limits.max_connections(), Arc::clone(&self.signals));
        let mut listener = mio::net::TcpListener::from_std(self.listener);
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)
            .map_err(bind_io)?;
        let mut worker = Worker {
            poll,
            listener,
            scheduler,
            signals: self.signals,
            limits: self.limits,
            budget: self.budget,
            sender: self.sender,
            instance: self.instance,
            live: BTreeMap::new(),
            next: FIRST_CONNECTION,
            listener_ready: true,
            accept_paused_at: None,
            summary: RunSummary {
                fixed_buffer_capacity: self.limits.fixed_buffer_capacity(),
                ..RunSummary::default()
            },
        };
        let result = worker.drive(&mut accept);
        if matches!(result, Err(ReceiverFailure::CounterExhausted)) {
            worker.summary.counters_complete = false;
        }
        worker.summary.live = worker.live.len();
        let cleanup = worker.retire_all();
        worker.summary.retained_output_bytes = worker.budget.used();
        match result.and_then(|cause| cleanup.map(|()| cause)) {
            Ok(cause) => {
                worker.summary.stopped_by = Some(cause);
                Ok(worker.summary)
            }
            Err(failure) => Err(ReceiverError {
                failure,
                summary: Box::new(worker.summary),
            }),
        }
    }
}

fn bind_io(error: io::Error) -> ReceiverError {
    ReceiverError::before_run(ReceiverFailure::Io(error))
}

// The operation boundary keeps accept and registration failures distinct.
trait AcceptIo {
    fn accept(
        &mut self,
        listener: &mio::net::TcpListener,
    ) -> io::Result<(mio::net::TcpStream, SocketAddr)>;

    fn register(
        &mut self,
        registry: &mio::Registry,
        socket: &mut mio::net::TcpStream,
        token: Token,
    ) -> io::Result<()> {
        registry.register(socket, token, Interest::READABLE)
    }

    fn now(&self) -> Instant {
        Instant::now()
    }
}

struct SystemAccept;

impl AcceptIo for SystemAccept {
    fn accept(
        &mut self,
        listener: &mio::net::TcpListener,
    ) -> io::Result<(mio::net::TcpStream, SocketAddr)> {
        listener.accept()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptFailure {
    Drained,
    Retry,
    Pause,
    Fatal,
}

fn classify_accept(error: &io::Error) -> AcceptFailure {
    // Linux reports pending TCP errors through accept4. Symbolic errno values
    // preserve distinctions that ErrorKind merges, including EPROTO and EBADF.
    // https://man7.org/linux/man-pages/man2/accept.2.html
    match error.raw_os_error() {
        Some(code) if code == libc::EAGAIN || code == libc::EWOULDBLOCK => AcceptFailure::Drained,
        Some(
            libc::EINTR
            | libc::ECONNABORTED
            | libc::ECONNRESET
            | libc::ETIMEDOUT
            | libc::ENETDOWN
            | libc::EPROTO
            | libc::ENOPROTOOPT
            | libc::EHOSTDOWN
            | libc::ENONET
            | libc::EHOSTUNREACH
            | libc::EOPNOTSUPP
            | libc::ENETUNREACH
            | libc::EPERM
            | libc::ESOCKTNOSUPPORT
            | libc::EPROTONOSUPPORT,
        ) => AcceptFailure::Retry,
        Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM | libc::ENOSR) => {
            AcceptFailure::Pause
        }
        Some(_) => AcceptFailure::Fatal,
        None => match error.kind() {
            io::ErrorKind::WouldBlock => AcceptFailure::Drained,
            io::ErrorKind::Interrupted
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::TimedOut
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::HostUnreachable => AcceptFailure::Retry,
            io::ErrorKind::OutOfMemory => AcceptFailure::Pause,
            // This kind can be returned by stable std before its variant name
            // is stabilized. Compare the OS-derived kind through stable APIs.
            kind if kind == io::Error::from_raw_os_error(libc::EMFILE).kind() => {
                AcceptFailure::Pause
            }
            _ => AcceptFailure::Fatal,
        },
    }
}

struct Entry {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
    context: TaskContext,
    waker: Waker,
}

struct Worker {
    poll: Poll,
    listener: mio::net::TcpListener,
    scheduler: Scheduler,
    signals: Arc<Signals>,
    limits: Limits,
    budget: ByteBudget,
    sender: RecordSender,
    instance: u64,
    live: BTreeMap<Token, Entry>,
    next: usize,
    listener_ready: bool,
    accept_paused_at: Option<Instant>,
    summary: RunSummary,
}

impl Worker {
    fn stop_cause(&self) -> Option<StopCause> {
        if self.signals.disconnected() {
            return Some(StopCause::ConsumerDisconnected);
        }
        self.signals
            .stop_requested()
            .then_some(StopCause::Requested)
    }

    fn drive(&mut self, accept: &mut impl AcceptIo) -> Result<StopCause, ReceiverFailure> {
        let mut events = Events::with_capacity(EVENT_BATCH);
        loop {
            if let Some(error) = self.signals.take_failure() {
                return Err(ReceiverFailure::Io(error));
            }
            if let Some(cause) = self.stop_cause() {
                return Ok(cause);
            }
            if self.signals.take_queue_credit() {
                self.scheduler.resume_credit(WaitReason::Queue);
            }
            if self.signals.take_byte_credit() {
                self.scheduler.resume_credit(WaitReason::Bytes);
            }
            self.run_ready()?;
            if let Some(cause) = self.stop_cause() {
                return Ok(cause);
            }
            self.accept_ready(accept)?;
            let timeout = self.poll_timeout(accept.now());
            match self.poll.poll(&mut events, Some(timeout)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(ReceiverFailure::Io(error)),
            }
            for event in &events {
                let token = event.token();
                if token == LISTENER {
                    self.listener_ready = true;
                } else if token != CONTROL {
                    self.scheduler.io_ready(token);
                }
            }
        }
    }

    fn run_ready(&mut self) -> Result<(), ReceiverFailure> {
        for _ in 0..TURN_BATCH {
            if self.stop_cause().is_some() {
                break;
            }
            let Some(token) = self.scheduler.take() else {
                break;
            };
            let entry = self
                .live
                .get_mut(&token)
                .expect("runnable token names a live task");
            entry.context.reset_turn(self.limits.turn_steps());
            let mut context = Context::from_waker(&entry.waker);
            let polled = entry.future.as_mut().poll(&mut context);
            if entry.context.progress().counter_overflow {
                return Err(ReceiverFailure::CounterExhausted);
            }
            match polled {
                TaskPoll::Ready(()) => {
                    self.retire(token)?;
                }
                TaskPoll::Pending => self.scheduler.verify_pending(token),
            }
        }
        Ok(())
    }

    fn accept_delay(&self, now: Instant) -> Duration {
        self.accept_paused_at.map_or(Duration::ZERO, |paused_at| {
            STOP_RECHECK.saturating_sub(now.saturating_duration_since(paused_at))
        })
    }

    fn poll_timeout(&self, now: Instant) -> Duration {
        if self.scheduler.has_ready() {
            Duration::ZERO
        } else if self.listener_ready {
            self.accept_delay(now)
        } else {
            STOP_RECHECK
        }
    }

    fn accept_ready(&mut self, accept: &mut impl AcceptIo) -> Result<(), ReceiverFailure> {
        if !self.listener_ready || !self.accept_delay(accept.now()).is_zero() {
            return Ok(());
        }
        self.accept_paused_at = None;
        let mut progressed = false;
        for _ in 0..self.limits.turn_steps().min(EVENT_BATCH) {
            if self.stop_cause().is_some() {
                return Ok(());
            }
            match accept.accept(&self.listener) {
                Ok((socket, peer)) => {
                    progressed = true;
                    if self.live.len() == self.limits.max_connections() {
                        self.summary.refused = self
                            .summary
                            .refused
                            .checked_add(1)
                            .ok_or(ReceiverFailure::CounterExhausted)?;
                        continue;
                    }
                    self.admit(socket, peer, accept)?;
                }
                Err(error) => match classify_accept(&error) {
                    AcceptFailure::Drained => {
                        self.listener_ready = false;
                        return Ok(());
                    }
                    AcceptFailure::Retry => {
                        self.summary.transient_accept_errors = self
                            .summary
                            .transient_accept_errors
                            .checked_add(1)
                            .ok_or(ReceiverFailure::CounterExhausted)?;
                    }
                    AcceptFailure::Pause => {
                        self.summary.resource_pause_entries = self
                            .summary
                            .resource_pause_entries
                            .checked_add(1)
                            .ok_or(ReceiverFailure::CounterExhausted)?;
                        self.accept_paused_at = Some(accept.now());
                        return Ok(());
                    }
                    AcceptFailure::Fatal => return Err(ReceiverFailure::Io(error)),
                },
            }
        }
        if !progressed {
            self.summary.retry_quota_backoffs = self
                .summary
                .retry_quota_backoffs
                .checked_add(1)
                .ok_or(ReceiverFailure::CounterExhausted)?;
            self.accept_paused_at = Some(accept.now());
        }
        Ok(())
    }

    fn admit(
        &mut self,
        mut socket: mio::net::TcpStream,
        peer: SocketAddr,
        accept: &mut impl AcceptIo,
    ) -> Result<(), ReceiverFailure> {
        let token = allocate_token(&mut self.next)?;
        accept
            .register(self.poll.registry(), &mut socket, token)
            .map_err(ReceiverFailure::Io)?;
        let context = TaskContext::new(
            ConnectionId::new(self.instance, token.0),
            peer,
            token,
            TaskResources {
                budget: self.budget.clone(),
                sender: self.sender.clone(),
                scheduler: self.scheduler.clone(),
            },
        );
        let waker = self.scheduler.insert(token);
        let future = Box::pin(connection::receive(
            socket,
            self.limits.payload_capacity(),
            context.clone(),
        ));
        assert!(
            self.live
                .insert(
                    token,
                    Entry {
                        future,
                        context,
                        waker
                    }
                )
                .is_none(),
            "connection token is fresh"
        );
        self.summary.accepted = self
            .summary
            .accepted
            .checked_add(1)
            .ok_or(ReceiverFailure::CounterExhausted)?;
        Ok(())
    }

    fn retire(&mut self, token: Token) -> Result<(), ReceiverFailure> {
        self.scheduler.retire(token);
        let entry = self.live.remove(&token).expect("retiring task is live");
        let progress = entry.context.progress();
        drop(entry);
        self.absorb(progress)
    }

    fn retire_all(&mut self) -> Result<(), ReceiverFailure> {
        let mut failure = None;
        while let Some(token) = self.live.keys().next().copied() {
            if let Err(error) = self.retire(token) {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    fn absorb(&mut self, progress: Progress) -> Result<(), ReceiverFailure> {
        let result = merge_progress(&mut self.summary, progress);
        if result.is_err() {
            self.summary.counters_complete = false;
        }
        result
    }
}

fn allocate_token(next: &mut usize) -> Result<Token, ReceiverFailure> {
    let token = Token(*next);
    *next = next.checked_add(1).ok_or(ReceiverFailure::TokenExhausted)?;
    Ok(token)
}

fn merge_progress(summary: &mut RunSummary, progress: Progress) -> Result<(), ReceiverFailure> {
    let mut next = *summary;
    if progress.terminal_queued {
        next.ended = next
            .ended
            .checked_add(1)
            .ok_or(ReceiverFailure::CounterExhausted)?;
    } else {
        next.abandoned = next
            .abandoned
            .checked_add(1)
            .ok_or(ReceiverFailure::CounterExhausted)?;
    }
    next.raw_bytes_read = next
        .raw_bytes_read
        .checked_add(progress.read)
        .ok_or(ReceiverFailure::CounterExhausted)?;
    next.raw_bytes_handed_off = next
        .raw_bytes_handed_off
        .checked_add(progress.wire_queued)
        .ok_or(ReceiverFailure::CounterExhausted)?;
    next.undelivered_raw_bytes = next
        .raw_bytes_read
        .checked_sub(next.raw_bytes_handed_off)
        .expect("handed-off bytes cannot exceed observed bytes");
    next.records_handed_off = next
        .records_handed_off
        .checked_add(progress.records_queued)
        .ok_or(ReceiverFailure::CounterExhausted)?;
    next.undelivered_records = next
        .undelivered_records
        .checked_add(usize::from(progress.pending_record))
        .ok_or(ReceiverFailure::CounterExhausted)?;
    next.counters_complete &= !progress.counter_overflow;
    *summary = next;
    if progress.counter_overflow {
        return Err(ReceiverFailure::CounterExhausted);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_tokens_never_wrap_or_change_the_counter() {
        let mut next = usize::MAX;
        assert!(matches!(
            allocate_token(&mut next),
            Err(ReceiverFailure::TokenExhausted)
        ));
        assert_eq!(next, usize::MAX);
    }

    #[test]
    fn an_exhausted_task_cannot_become_a_complete_worker_summary() {
        let mut summary = RunSummary::default();
        let result = merge_progress(
            &mut summary,
            Progress {
                read: u64::MAX,
                wire_queued: u64::MAX - 1,
                counter_overflow: true,
                ..Progress::default()
            },
        );
        assert!(matches!(result, Err(ReceiverFailure::CounterExhausted)));
        assert!(!summary.counters_complete);
        assert_eq!(summary.raw_bytes_read, u64::MAX);
        assert_eq!(summary.undelivered_raw_bytes, 1);
        assert_eq!(summary.abandoned, 1);
    }

    #[test]
    fn undelivered_raw_prefix_remains_visible_at_cancellation() {
        let mut summary = RunSummary::default();
        merge_progress(
            &mut summary,
            Progress {
                read: 12,
                wire_queued: 7,
                pending_record: true,
                ..Progress::default()
            },
        )
        .expect("finite counters fit");
        assert_eq!(summary.undelivered_raw_bytes, 5);
        assert_eq!(summary.undelivered_records, 1);
        assert_eq!(summary.abandoned, 1);
        assert_eq!(summary.ended, 0);
    }
}

#[cfg(test)]
#[path = "driver_accept_tests.rs"]
mod accept_tests;
