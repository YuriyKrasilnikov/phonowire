//! Explicit task readiness and resource notifications for one worker.
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, Wake, Waker};

use mio::{Token, Waker as MioWaker};

use crate::handoff::RecordSender;
use crate::{BudgetError, ByteBudget, ConnectionId, OwnedBytes, Record, RecordKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitReason {
    Kernel,
    Queue,
    Bytes,
}

#[derive(Clone, Copy)]
pub enum Signal {
    Queue,
    Bytes,
    Stop,
    Disconnected,
}

#[derive(Clone, Copy)]
struct WakeFailure {
    kind: io::ErrorKind,
    os_code: Option<i32>,
}

pub struct Signals {
    queue: AtomicBool,
    bytes: AtomicBool,
    stop: AtomicBool,
    disconnected: AtomicBool,
    wake: Mutex<Option<Weak<MioWaker>>>,
    failure: Mutex<Option<WakeFailure>>,
}

impl Signals {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            queue: AtomicBool::new(false),
            bytes: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            disconnected: AtomicBool::new(false),
            wake: Mutex::new(None),
            failure: Mutex::new(None),
        })
    }

    pub fn attach(self: &Arc<Self>, wake: &Arc<MioWaker>) -> Attachment {
        *lock(&self.wake) = Some(Arc::downgrade(wake));
        Attachment(Arc::clone(self))
    }

    fn set(&self, signal: Signal) {
        let flag = match signal {
            Signal::Queue => &self.queue,
            Signal::Bytes => &self.bytes,
            Signal::Stop => &self.stop,
            Signal::Disconnected => &self.disconnected,
        };
        flag.store(true, Ordering::Release);
    }

    pub fn request(&self, signal: Signal) -> io::Result<()> {
        self.set(signal);
        self.notify().inspect_err(|error| self.remember(error))
    }

    pub fn publish(&self, signal: Signal) {
        self.set(signal);
        self.wake();
    }

    fn notify(&self) -> io::Result<()> {
        let wake = lock(&self.wake).as_ref().and_then(Weak::upgrade);
        wake.map_or(Ok(()), |wake| wake.wake())
    }

    fn wake(&self) {
        if let Err(error) = self.notify() {
            self.remember(&error);
        }
    }

    fn remember(&self, error: &io::Error) {
        let mut failure = lock(&self.failure);
        if failure.is_none() {
            *failure = Some(WakeFailure {
                kind: error.kind(),
                os_code: error.raw_os_error(),
            });
        }
    }

    pub fn take_failure(&self) -> Option<io::Error> {
        lock(&self.failure).take().map(|failure| {
            failure.os_code.map_or_else(
                || io::Error::from(failure.kind),
                io::Error::from_raw_os_error,
            )
        })
    }

    pub fn stop_requested(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }

    pub fn disconnected(&self) -> bool {
        self.disconnected.load(Ordering::Acquire)
    }

    pub fn take_queue_credit(&self) -> bool {
        self.queue.swap(false, Ordering::AcqRel)
    }

    pub fn take_byte_credit(&self) -> bool {
        self.bytes.swap(false, Ordering::AcqRel)
    }

    pub fn waker(self: &Arc<Self>, signal: Signal) -> Waker {
        Waker::from(Arc::new(ResourceWake {
            signals: Arc::clone(self),
            signal,
        }))
    }
}

pub struct Attachment(Arc<Signals>);

impl Drop for Attachment {
    fn drop(&mut self) {
        *lock(&self.0.wake) = None;
    }
}

struct ResourceWake {
    signals: Arc<Signals>,
    signal: Signal,
}

impl Wake for ResourceWake {
    fn wake(self: Arc<Self>) {
        self.signals.publish(self.signal);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.signals.publish(self.signal);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Running,
    Queued,
    Waiting(WaitReason),
}

struct Ready {
    states: BTreeMap<Token, State>,
    queue: VecDeque<Token>,
    queue_waiters: BTreeSet<Token>,
    byte_waiters: BTreeSet<Token>,
    capacity: usize,
}

impl Ready {
    fn enqueue(&mut self, token: Token) -> bool {
        let Some(state) = self.states.get_mut(&token) else {
            return false;
        };
        if *state == State::Queued {
            return false;
        }
        *state = State::Queued;
        self.queue_waiters.remove(&token);
        self.byte_waiters.remove(&token);
        assert!(
            self.queue.len() < self.capacity,
            "one runnable entry per live connection"
        );
        self.queue.push_back(token);
        true
    }
}

struct SchedulerInner {
    ready: Mutex<Ready>,
    signals: Arc<Signals>,
}

#[derive(Clone)]
pub struct Scheduler(Arc<SchedulerInner>);

impl Scheduler {
    pub fn new(capacity: usize, signals: Arc<Signals>) -> Self {
        Self(Arc::new(SchedulerInner {
            ready: Mutex::new(Ready {
                states: BTreeMap::new(),
                queue: VecDeque::with_capacity(capacity),
                queue_waiters: BTreeSet::new(),
                byte_waiters: BTreeSet::new(),
                capacity,
            }),
            signals,
        }))
    }

    pub fn insert(&self, token: Token) -> Waker {
        let mut ready = lock(&self.0.ready);
        assert!(
            ready.states.len() < ready.capacity,
            "connection admission precedes scheduling"
        );
        assert!(
            ready.states.insert(token, State::Running).is_none(),
            "tokens are never reused"
        );
        ready.enqueue(token);
        drop(ready);
        Waker::from(Arc::new(TaskWake {
            token,
            scheduler: Arc::downgrade(&self.0),
        }))
    }

    pub fn take(&self) -> Option<Token> {
        let mut ready = lock(&self.0.ready);
        let token = ready.queue.pop_front()?;
        *ready.states.get_mut(&token).expect("queued task is live") = State::Running;
        drop(ready);
        Some(token)
    }

    pub fn has_ready(&self) -> bool {
        !lock(&self.0.ready).queue.is_empty()
    }

    pub fn io_ready(&self, token: Token) {
        let mut ready = lock(&self.0.ready);
        if ready.states.get(&token) == Some(&State::Waiting(WaitReason::Kernel)) {
            ready.enqueue(token);
        }
    }

    fn park(&self, token: Token, reason: WaitReason) {
        let mut ready = lock(&self.0.ready);
        *ready.states.get_mut(&token).expect("parking task is live") = State::Waiting(reason);
        ready.queue.retain(|queued| *queued != token);
        ready.queue_waiters.remove(&token);
        ready.byte_waiters.remove(&token);
        match reason {
            WaitReason::Kernel => {}
            WaitReason::Queue => {
                ready.queue_waiters.insert(token);
            }
            WaitReason::Bytes => {
                ready.byte_waiters.insert(token);
            }
        }
    }

    pub fn resume_credit(&self, reason: WaitReason) {
        let mut ready = lock(&self.0.ready);
        let waiters = match reason {
            WaitReason::Kernel => panic!("kernel readiness is addressed by token"),
            WaitReason::Queue => std::mem::take(&mut ready.queue_waiters),
            WaitReason::Bytes => std::mem::take(&mut ready.byte_waiters),
        };
        for token in waiters {
            ready.enqueue(token);
        }
    }

    pub fn verify_pending(&self, token: Token) {
        assert_ne!(
            lock(&self.0.ready).states.get(&token),
            Some(&State::Running),
            "private task Pending must name a wait or schedule continuation"
        );
    }

    pub fn retire(&self, token: Token) {
        let mut ready = lock(&self.0.ready);
        ready.states.remove(&token);
        ready.queue.retain(|queued| *queued != token);
        ready.queue_waiters.remove(&token);
        ready.byte_waiters.remove(&token);
    }
}

struct TaskWake {
    token: Token,
    scheduler: Weak<SchedulerInner>,
}

impl Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(scheduler) = self.scheduler.upgrade() {
            let inserted = lock(&scheduler.ready).enqueue(self.token);
            if inserted {
                scheduler.signals.wake();
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Progress {
    pub read: u64,
    pub consumed: u64,
    pub wire_queued: u64,
    pub records_queued: u64,
    pub pending_record: bool,
    pub terminal_queued: bool,
    pub counter_overflow: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct CounterOverflow;

#[derive(Clone, Copy, Debug)]
pub enum SendFailure {
    Disconnected,
    CounterOverflow,
}

#[derive(Clone)]
pub struct TaskContext {
    id: ConnectionId,
    peer: SocketAddr,
    token: Token,
    budget: ByteBudget,
    sender: RecordSender,
    scheduler: Scheduler,
    remaining: Arc<AtomicUsize>,
    progress: Arc<Mutex<Progress>>,
}

pub struct TaskResources {
    pub budget: ByteBudget,
    pub sender: RecordSender,
    pub scheduler: Scheduler,
}

impl TaskContext {
    pub fn new(id: ConnectionId, peer: SocketAddr, token: Token, resources: TaskResources) -> Self {
        Self {
            id,
            peer,
            token,
            budget: resources.budget,
            sender: resources.sender,
            scheduler: resources.scheduler,
            remaining: Arc::new(AtomicUsize::new(0)),
            progress: Arc::new(Mutex::new(Progress::default())),
        }
    }

    pub const fn id(&self) -> ConnectionId {
        self.id
    }
    pub const fn peer(&self) -> SocketAddr {
        self.peer
    }
    pub fn offset(&self) -> u64 {
        lock(&self.progress).consumed
    }
    pub fn progress(&self) -> Progress {
        *lock(&self.progress)
    }
    pub fn reset_turn(&self, steps: usize) {
        self.remaining.store(steps, Ordering::Relaxed);
    }

    pub fn permit(&self, context: &Context<'_>) -> bool {
        let remaining = self.remaining.load(Ordering::Relaxed);
        if remaining == 0 {
            context.waker().wake_by_ref();
            return false;
        }
        self.remaining.store(remaining - 1, Ordering::Relaxed);
        true
    }

    pub async fn step(&self) {
        poll_fn(|context| {
            if self.permit(context) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }

    pub fn park(&self, reason: WaitReason) {
        self.scheduler.park(self.token, reason);
    }

    pub fn observe_read(&self, count: usize) -> Result<u64, CounterOverflow> {
        let mut progress = lock(&self.progress);
        let start = progress.read;
        match add_observation(start, count) {
            Ok(next) => {
                progress.read = next;
                drop(progress);
                Ok(start)
            }
            Err(error) => {
                progress.counter_overflow = true;
                drop(progress);
                Err(error)
            }
        }
    }

    pub fn consume(&self, count: usize) -> Result<u64, CounterOverflow> {
        let mut progress = lock(&self.progress);
        match add_observation(progress.consumed, count) {
            Ok(next) => {
                assert!(
                    next <= progress.read,
                    "decoder consumes only observed bytes"
                );
                progress.consumed = next;
                drop(progress);
                Ok(next)
            }
            Err(error) => {
                progress.counter_overflow = true;
                drop(progress);
                Err(error)
            }
        }
    }

    pub async fn copy(&self, bytes: &[u8]) -> Result<OwnedBytes, BudgetError> {
        poll_fn(|context| {
            if !self.permit(context) {
                return Poll::Pending;
            }
            match self.budget.try_copy(bytes) {
                Ok(owned) => Poll::Ready(Ok(owned)),
                Err(BudgetError::Full { .. }) => {
                    self.park(WaitReason::Bytes);
                    Poll::Pending
                }
                Err(error @ BudgetError::TooLarge { .. }) => Poll::Ready(Err(error)),
            }
        })
        .await
    }

    pub async fn send(&self, record: Record) -> Result<(), SendFailure> {
        let mut pending = Some(record);
        lock(&self.progress).pending_record = true;
        poll_fn(|context| {
            if !self.permit(context) {
                return Poll::Pending;
            }
            let next_count = {
                let mut progress = lock(&self.progress);
                let Some(next) = progress.records_queued.checked_add(1) else {
                    progress.counter_overflow = true;
                    drop(progress);
                    return Poll::Ready(Err(SendFailure::CounterOverflow));
                };
                drop(progress);
                next
            };
            let record = pending
                .take()
                .expect("send retains its record until completion");
            let wire = match &record.kind {
                RecordKind::Wire { bytes } => {
                    u64::try_from(bytes.as_slice().len()).expect("wire record length fits u64")
                }
                RecordKind::Connected { .. }
                | RecordKind::Started { .. }
                | RecordKind::Audio { .. }
                | RecordKind::Dtmf { .. }
                | RecordKind::Ended { .. } => 0,
            };
            let terminal = matches!(&record.kind, RecordKind::Ended { .. });
            match self.sender.try_send(record) {
                Ok(()) => {
                    let mut progress = lock(&self.progress);
                    progress.pending_record = false;
                    progress.terminal_queued |= terminal;
                    progress.wire_queued = progress
                        .wire_queued
                        .checked_add(wire)
                        .expect("handed-off wire is bounded by observed bytes");
                    progress.records_queued = next_count;
                    drop(progress);
                    Poll::Ready(Ok(()))
                }
                Err(std::sync::mpsc::TrySendError::Full(record)) => {
                    pending = Some(record);
                    self.park(WaitReason::Queue);
                    Poll::Pending
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(_record)) => {
                    Poll::Ready(Err(SendFailure::Disconnected))
                }
            }
        })
        .await
    }
}

fn add_observation(current: u64, count: usize) -> Result<u64, CounterOverflow> {
    let count = u64::try_from(count).map_err(|_| CounterOverflow)?;
    current.checked_add(count).ok_or(CounterOverflow)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("internal scheduler state cannot be poisoned")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scheduler(capacity: usize) -> (Scheduler, Arc<Signals>) {
        let signals = Signals::new();
        (Scheduler::new(capacity, Arc::clone(&signals)), signals)
    }

    fn task_context() -> (TaskContext, crate::Records, Waker) {
        let signals = Signals::new();
        let scheduler = Scheduler::new(1, Arc::clone(&signals));
        let waker = scheduler.insert(Token(2));
        let (sender, records) = crate::handoff::channel(
            std::num::NonZeroUsize::MIN,
            signals.waker(Signal::Queue),
            signals.waker(Signal::Disconnected),
        );
        let context = TaskContext::new(
            ConnectionId::new(1, 2),
            SocketAddr::from(([127, 0, 0, 1], 9)),
            Token(2),
            TaskResources {
                budget: ByteBudget::new(std::num::NonZeroUsize::MIN),
                sender,
                scheduler,
            },
        );
        (context, records, waker)
    }

    #[test]
    fn byte_counter_exhaustion_preserves_the_prefix_and_marks_it_incomplete() {
        let (context, _records, _waker) = task_context();
        lock(&context.progress).read = u64::MAX;
        assert!(context.observe_read(1).is_err());
        assert_eq!(context.progress().read, u64::MAX);
        assert!(context.progress().counter_overflow);
        *lock(&context.progress) = Progress {
            read: u64::MAX,
            consumed: u64::MAX,
            ..Progress::default()
        };
        assert!(context.consume(1).is_err());
        assert_eq!(context.progress().consumed, u64::MAX);
        assert!(context.progress().counter_overflow);
    }

    #[test]
    fn record_counter_exhaustion_refuses_handoff_without_panicking() {
        use std::future::Future;
        let (context, records, waker) = task_context();
        lock(&context.progress).records_queued = u64::MAX;
        context.reset_turn(1);
        let mut operation = Box::pin(context.send(Record {
            connection: context.id(),
            offset: crate::WireOffset::new(0),
            observed_at: std::time::Instant::now(),
            kind: RecordKind::Connected {
                peer: context.peer(),
            },
        }));
        assert!(matches!(
            operation.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Err(SendFailure::CounterOverflow))
        ));
        assert_eq!(context.progress().records_queued, u64::MAX);
        assert!(context.progress().counter_overflow);
        assert!(context.progress().pending_record);
        assert!(matches!(
            records.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
    }

    #[test]
    fn repeated_wakes_preserve_one_fifo_turn_per_connection() {
        let (scheduler, _signals) = scheduler(2);
        let first = scheduler.insert(Token(2));
        let second = scheduler.insert(Token(3));
        assert_eq!(scheduler.take(), Some(Token(2)));
        first.wake_by_ref();
        first.wake_by_ref();
        second.wake_by_ref();
        assert_eq!(scheduler.take(), Some(Token(3)));
        second.wake_by_ref();
        assert_eq!(scheduler.take(), Some(Token(2)));
        scheduler.park(Token(2), WaitReason::Kernel);
        assert_eq!(scheduler.take(), Some(Token(3)));
        scheduler.park(Token(3), WaitReason::Kernel);
        assert_eq!(scheduler.take(), None);
    }

    #[test]
    fn readable_events_cannot_spin_connections_waiting_for_resources() {
        let (scheduler, _signals) = scheduler(3);
        let _queue = scheduler.insert(Token(2));
        let _bytes = scheduler.insert(Token(3));
        let _kernel = scheduler.insert(Token(4));
        scheduler.park(Token(2), WaitReason::Queue);
        scheduler.park(Token(3), WaitReason::Bytes);
        scheduler.park(Token(4), WaitReason::Kernel);
        for _ in 0..4 {
            scheduler.io_ready(Token(2));
            scheduler.io_ready(Token(3));
            assert_eq!(scheduler.take(), None);
        }
        scheduler.resume_credit(WaitReason::Queue);
        assert_eq!(scheduler.take(), Some(Token(2)));
        assert_eq!(scheduler.take(), None);
        scheduler.resume_credit(WaitReason::Bytes);
        assert_eq!(scheduler.take(), Some(Token(3)));
        assert_eq!(scheduler.take(), None);
        scheduler.io_ready(Token(4));
        assert_eq!(scheduler.take(), Some(Token(4)));
    }

    #[test]
    fn credit_before_or_after_registration_resumes_without_network_event() {
        for reason in [WaitReason::Queue, WaitReason::Bytes] {
            for credit_before_park in [true, false] {
                let (scheduler, signals) = scheduler(1);
                let _task = scheduler.insert(Token(2));
                assert_eq!(scheduler.take(), Some(Token(2)));
                let signal = match reason {
                    WaitReason::Queue => Signal::Queue,
                    WaitReason::Bytes => Signal::Bytes,
                    WaitReason::Kernel => panic!("resource test has no kernel wait"),
                };
                if credit_before_park {
                    signals.publish(signal);
                }
                scheduler.park(Token(2), reason);
                assert_eq!(scheduler.take(), None);
                if !credit_before_park {
                    signals.publish(signal);
                }
                let returned = match reason {
                    WaitReason::Queue => signals.take_queue_credit(),
                    WaitReason::Bytes => signals.take_byte_credit(),
                    WaitReason::Kernel => panic!("resource test has no kernel wait"),
                };
                assert!(returned);
                scheduler.resume_credit(reason);
                assert_eq!(scheduler.take(), Some(Token(2)));
                assert_eq!(scheduler.take(), None);
            }
        }
    }

    #[test]
    fn retired_wakers_cannot_target_replacement_connections() {
        let (scheduler, _signals) = scheduler(1);
        let old = scheduler.insert(Token(2));
        scheduler.park(Token(2), WaitReason::Bytes);
        scheduler.retire(Token(2));
        for token in 3..103 {
            let current = scheduler.insert(Token(token));
            old.wake_by_ref();
            assert_eq!(scheduler.take(), Some(Token(token)));
            scheduler.park(Token(token), WaitReason::Bytes);
            old.wake_by_ref();
            assert_eq!(scheduler.take(), None);
            scheduler.retire(Token(token));
            current.wake_by_ref();
            scheduler.resume_credit(WaitReason::Bytes);
            assert_eq!(scheduler.take(), None);
        }
    }
}
