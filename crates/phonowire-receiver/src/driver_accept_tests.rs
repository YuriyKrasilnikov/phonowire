//! Accept error, readiness and cancellation witnesses at the socket boundary.
use super::*;
use crate::{EndReason, RecordKind};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpStream};
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::atomic::{AtomicBool, AtomicI32};
use std::sync::mpsc;
use std::thread;

const DEADLINE: Duration = Duration::from_secs(3);

fn limits(turns: usize) -> Limits {
    Limits::new(
        NonZeroUsize::new(8).expect("connection bound"),
        NonZeroUsize::new(256).expect("record bound"),
        NonZeroU16::new(64).expect("payload bound"),
        NonZeroUsize::new(turns).expect("turn bound"),
    )
    .expect("valid receiver limits")
}

fn bind(turns: usize) -> (Receiver, Records, StopHandle) {
    Receiver::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        limits(turns),
        ByteBudget::new(NonZeroUsize::new(65536).expect("byte bound")),
    )
    .expect("bind loopback")
}

fn fixture(turns: usize) -> (Worker, Records, StopHandle) {
    let (receiver, records, stop) = bind(turns);
    let poll = Poll::new().expect("poll");
    let mut listener = mio::net::TcpListener::from_std(receiver.listener);
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)
        .expect("register listener");
    let worker = Worker {
        poll,
        listener,
        scheduler: Scheduler::new(
            receiver.limits.max_connections(),
            Arc::clone(&receiver.signals),
        ),
        signals: receiver.signals,
        limits: receiver.limits,
        budget: receiver.budget,
        sender: receiver.sender,
        instance: receiver.instance,
        profile: receiver.profile,
        controls: receiver.controls,
        live: BTreeMap::new(),
        next: FIRST_CONNECTION,
        listener_ready: true,
        accept_paused_at: None,
        summary: RunSummary::default(),
    };
    (worker, records, stop)
}

struct Sequence {
    errors: VecDeque<i32>,
    now: Instant,
    attempts: usize,
}

impl Sequence {
    fn new(errors: impl IntoIterator<Item = i32>) -> Self {
        Self {
            errors: errors.into_iter().collect(),
            now: Instant::now(),
            attempts: 0,
        }
    }
}

impl AcceptIo for Sequence {
    fn accept(
        &mut self,
        listener: &mio::net::TcpListener,
    ) -> io::Result<(mio::net::TcpStream, SocketAddr)> {
        self.attempts += 1;
        self.errors.pop_front().map_or_else(
            || listener.accept(),
            |error| Err(io::Error::from_raw_os_error(error)),
        )
    }

    fn now(&self) -> Instant {
        self.now
    }
}

#[test]
fn linux_accept_errors_preserve_their_distinct_failure_scopes() {
    for error in [libc::EAGAIN, libc::EWOULDBLOCK] {
        assert_eq!(
            classify_accept(&io::Error::from_raw_os_error(error)),
            AcceptFailure::Drained
        );
    }
    for error in [
        libc::EINTR,
        libc::ECONNABORTED,
        libc::ECONNRESET,
        libc::ETIMEDOUT,
        libc::ENETDOWN,
        libc::EPROTO,
        libc::ENOPROTOOPT,
        libc::EHOSTDOWN,
        libc::ENONET,
        libc::EHOSTUNREACH,
        libc::EOPNOTSUPP,
        libc::ENETUNREACH,
        libc::EPERM,
        libc::ESOCKTNOSUPPORT,
        libc::EPROTONOSUPPORT,
    ] {
        assert_eq!(
            classify_accept(&io::Error::from_raw_os_error(error)),
            AcceptFailure::Retry
        );
    }
    for error in [
        libc::EMFILE,
        libc::ENFILE,
        libc::ENOBUFS,
        libc::ENOMEM,
        libc::ENOSR,
    ] {
        assert_eq!(
            classify_accept(&io::Error::from_raw_os_error(error)),
            AcceptFailure::Pause
        );
    }
    for error in [
        libc::EBADF,
        libc::EFAULT,
        libc::EINVAL,
        libc::ENOTSOCK,
        libc::ENOSYS,
        libc::EACCES,
        libc::EIO,
        libc::ENOSPC,
    ] {
        assert_eq!(
            classify_accept(&io::Error::from_raw_os_error(error)),
            AcceptFailure::Fatal
        );
    }
    for kind in [
        io::ErrorKind::Unsupported,
        io::ErrorKind::PermissionDenied,
        io::ErrorKind::Other,
    ] {
        assert_eq!(
            classify_accept(&io::Error::from(kind)),
            AcceptFailure::Fatal
        );
    }
    assert_eq!(
        classify_accept(&io::Error::from(io::ErrorKind::ConnectionAborted)),
        AcceptFailure::Retry
    );
    assert_eq!(
        classify_accept(&io::Error::from(io::ErrorKind::OutOfMemory)),
        AcceptFailure::Pause
    );
    let descriptors = io::Error::from_raw_os_error(libc::EMFILE).kind();
    assert_eq!(
        classify_accept(&io::Error::from(descriptors)),
        AcceptFailure::Pause
    );
}

#[test]
fn error_storm_attempts_obey_the_turn_budget_and_cooldown_despite_readiness() {
    for error in [libc::EINTR, libc::ECONNABORTED, libc::EPROTO] {
        for turns in [1, 3, EVENT_BATCH + 1] {
            let bound = turns.min(EVENT_BATCH);
            let (mut worker, _records, _stop) = fixture(turns);
            let mut source = Sequence::new(std::iter::repeat_n(error, bound * 4));
            for round in 1..=4 {
                worker.accept_ready(&mut source).expect("retryable turn");
                assert_eq!(source.attempts, round * bound);
                assert_eq!(
                    worker.summary.transient_accept_errors,
                    (round * bound) as u64
                );
                assert_eq!(worker.summary.retry_quota_backoffs, round as u64);
                assert_eq!(worker.poll_timeout(source.now), STOP_RECHECK);
                source.now += STOP_RECHECK
                    .checked_sub(Duration::from_nanos(1))
                    .expect("pause exceeds one nanosecond");
                // Duplicate readiness must not bypass the outstanding pause.
                worker.listener_ready = true;
                worker.accept_ready(&mut source).expect("still paused");
                assert_eq!(source.attempts, round * bound);
                assert_eq!(worker.poll_timeout(source.now), Duration::from_nanos(1));
                source.now += Duration::from_nanos(1);
            }
        }
    }
}

#[test]
fn resource_pressure_retries_one_attempt_per_pause_then_clears_on_would_block() {
    for error in [
        libc::EMFILE,
        libc::ENFILE,
        libc::ENOBUFS,
        libc::ENOMEM,
        libc::ENOSR,
    ] {
        let (mut worker, _records, _stop) = fixture(8);
        let mut source = Sequence::new([error, error]);
        worker
            .accept_ready(&mut source)
            .expect("first resource failure");
        assert_eq!(worker.summary.resource_pause_entries, 1);
        worker
            .accept_ready(&mut source)
            .expect("no immediate retry");
        assert_eq!(source.attempts, 1);
        source.now += STOP_RECHECK;
        worker
            .accept_ready(&mut source)
            .expect("second resource failure");
        assert_eq!(worker.summary.resource_pause_entries, 2);
        assert_eq!(source.attempts, 2);
        source.now += STOP_RECHECK;
        worker.accept_ready(&mut source).expect("empty listener");
        assert_eq!(source.attempts, 3);
        assert_eq!(worker.poll_timeout(source.now), STOP_RECHECK);
        source.now += STOP_RECHECK;
        worker
            .accept_ready(&mut source)
            .expect("WouldBlock awaits a new edge");
        assert_eq!(source.attempts, 3);
    }
}

#[test]
fn accept_pressure_counters_exhaust_without_wrapping() {
    let (mut retry, _records, _stop) = fixture(8);
    retry.summary.transient_accept_errors = u64::MAX;
    let mut retry_source = Sequence::new([libc::EINTR]);
    assert!(matches!(
        retry.accept_ready(&mut retry_source),
        Err(ReceiverFailure::CounterExhausted)
    ));
    assert_eq!(retry.summary.transient_accept_errors, u64::MAX);

    let (mut pause, _records, _stop) = fixture(8);
    pause.summary.resource_pause_entries = u64::MAX;
    let mut pause_source = Sequence::new([libc::EMFILE]);
    assert!(matches!(
        pause.accept_ready(&mut pause_source),
        Err(ReceiverFailure::CounterExhausted)
    ));
    assert_eq!(pause.summary.resource_pause_entries, u64::MAX);

    let (mut quota, _records, _stop) = fixture(1);
    quota.summary.retry_quota_backoffs = u64::MAX;
    let mut quota_source = Sequence::new([libc::EINTR]);
    assert!(matches!(
        quota.accept_ready(&mut quota_source),
        Err(ReceiverFailure::CounterExhausted)
    ));
    assert_eq!(quota.summary.retry_quota_backoffs, u64::MAX);
}

#[test]
fn pending_backlog_recovers_after_error_and_quota_without_another_edge() {
    let (mut worker, _records, _stop) = fixture(1);
    let address = worker.listener.local_addr().expect("listener address");
    let peers = (0..3)
        .map(|_| TcpStream::connect(address).expect("backlog peer"))
        .collect::<Vec<_>>();
    let mut events = Events::with_capacity(8);
    worker
        .poll
        .poll(&mut events, Some(DEADLINE))
        .expect("consume initial edge");
    assert!(events.iter().any(|event| event.token() == LISTENER));
    let mut source = Sequence::new([libc::ECONNABORTED]);
    worker
        .accept_ready(&mut source)
        .expect("failed first attempt");
    assert_eq!(worker.summary.accepted, 0);
    source.now += STOP_RECHECK;
    // No readiness event is supplied after the first edge. Both the retry and
    // the subsequent quota boundaries retain the known pending backlog.
    for admitted in 1..=peers.len() {
        worker
            .accept_ready(&mut source)
            .expect("accept queued peer");
        assert_eq!(worker.summary.accepted, admitted);
        assert_eq!(worker.poll_timeout(source.now), Duration::ZERO);
    }
    assert_eq!(source.attempts, 4);
    worker.accept_ready(&mut source).expect("drain listener");
    assert_eq!(source.attempts, 5);
    worker
        .accept_ready(&mut source)
        .expect("no edge, no attempt after drain");
    assert_eq!(source.attempts, 5);
    worker
        .retire_all(ConnectionCloseResult::ReceiverStopped)
        .expect("close admitted peers");
}

#[test]
fn stop_requested_inside_an_error_attempt_prevents_the_next_attempt() {
    struct StopOnAccept {
        signals: Arc<Signals>,
        attempts: usize,
    }
    impl AcceptIo for StopOnAccept {
        fn accept(
            &mut self,
            _listener: &mio::net::TcpListener,
        ) -> io::Result<(mio::net::TcpStream, SocketAddr)> {
            self.attempts += 1;
            self.signals.publish(Signal::Stop);
            Err(io::Error::from_raw_os_error(libc::EINTR))
        }
    }
    let (mut worker, _records, _stop) = fixture(8);
    let mut source = StopOnAccept {
        signals: Arc::clone(&worker.signals),
        attempts: 0,
    };
    worker
        .accept_ready(&mut source)
        .expect("stop interrupts error turn");
    assert_eq!(source.attempts, 1);
    assert_eq!(
        worker.drive(&mut source).expect("stop observed"),
        StopCause::Requested
    );
    assert_eq!(source.attempts, 1);
}

struct Faults {
    accept: AtomicI32,
    repeat: AtomicBool,
    register: AtomicI32,
    reached: mpsc::Sender<i32>,
}

struct FaultSource(Arc<Faults>);

impl AcceptIo for FaultSource {
    fn accept(
        &mut self,
        listener: &mio::net::TcpListener,
    ) -> io::Result<(mio::net::TcpStream, SocketAddr)> {
        let error = if self.0.repeat.load(Ordering::Acquire) {
            self.0.accept.load(Ordering::Acquire)
        } else {
            self.0.accept.swap(0, Ordering::AcqRel)
        };
        if error == 0 {
            listener.accept()
        } else {
            self.0.reached.send(error).expect("observe injected error");
            Err(io::Error::from_raw_os_error(error))
        }
    }

    fn register(
        &mut self,
        registry: &mio::Registry,
        socket: &mut mio::net::TcpStream,
        token: Token,
    ) -> io::Result<()> {
        let error = self.0.register.swap(0, Ordering::AcqRel);
        if error == 0 {
            registry.register(socket, token, Interest::READABLE)
        } else {
            self.0
                .reached
                .send(error)
                .expect("observe register failure");
            Err(io::Error::from_raw_os_error(error))
        }
    }
}

struct WorkerThread {
    result: mpsc::Receiver<Result<RunSummary, ReceiverError>>,
    handle: thread::JoinHandle<()>,
}

impl WorkerThread {
    fn spawn(receiver: Receiver, source: FaultSource) -> Self {
        let (completed, result) = mpsc::channel();
        let handle = thread::spawn(move || {
            let outcome = receiver.run_with(source);
            let _ = completed.send(outcome);
        });
        Self { result, handle }
    }

    fn join(self) -> thread::Result<Result<RunSummary, ReceiverError>> {
        let result = self
            .result
            .recv_timeout(DEADLINE)
            .expect("worker completes before test deadline");
        self.handle.join().map(|()| result)
    }
}

struct Running {
    address: SocketAddr,
    records: Records,
    stop: StopHandle,
    faults: Arc<Faults>,
    reached: mpsc::Receiver<i32>,
    worker: WorkerThread,
}

impl Running {
    fn new(turns: usize) -> Self {
        let (receiver, records, stop) = bind(turns);
        let address = receiver.local_addr();
        let (sender, reached) = mpsc::channel();
        let faults = Arc::new(Faults {
            accept: AtomicI32::new(0),
            repeat: AtomicBool::new(false),
            register: AtomicI32::new(0),
            reached: sender,
        });
        let source = FaultSource(Arc::clone(&faults));
        let worker = WorkerThread::spawn(receiver, source);
        Self {
            address,
            records,
            stop,
            faults,
            reached,
            worker,
        }
    }

    fn inject(&self, error: i32, repeat: bool) {
        self.faults.repeat.store(repeat, Ordering::Release);
        self.faults.accept.store(error, Ordering::Release);
    }

    fn observed(&self, error: i32) {
        assert_eq!(
            self.reached
                .recv_timeout(DEADLINE)
                .expect("fault reached actual accept boundary"),
            error
        );
    }

    fn finish(self) -> RunSummary {
        self.stop.stop().expect("notify stop");
        self.worker
            .join()
            .expect("worker joins")
            .expect("worker succeeds")
    }
}

#[derive(Default)]
struct Transcript {
    peer: Option<SocketAddr>,
    raw: Vec<u8>,
    uuid: Option<[u8; 16]>,
    audio: Vec<u8>,
    ended: bool,
}

type Transcripts = BTreeMap<ConnectionId, Transcript>;

fn collect_until(
    records: &Records,
    transcripts: &mut Transcripts,
    done: impl Fn(&Transcripts) -> bool,
) {
    let deadline = Instant::now() + DEADLINE;
    while !done(transcripts) {
        let record = records
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("record before deadline");
        let transcript = transcripts.entry(record.connection).or_default();
        match record.kind {
            RecordKind::Connected { peer } => {
                assert_eq!(transcript.peer.replace(peer), None);
            }
            RecordKind::Wire { bytes } => {
                assert_eq!(
                    record.offset.get(),
                    u64::try_from(transcript.raw.len()).expect("wire offset")
                );
                transcript.raw.extend_from_slice(bytes.as_slice());
            }
            RecordKind::Started { uuid } => {
                assert_eq!(record.offset.get(), 19);
                assert_eq!(transcript.uuid.replace(uuid.bytes()), None);
            }
            RecordKind::Audio { bytes, uuid, .. } => {
                assert_eq!(record.offset.get(), 24);
                assert_eq!(transcript.uuid, Some(uuid.bytes()));
                transcript.audio.extend_from_slice(bytes.as_slice());
            }
            RecordKind::Ended {
                reason: EndReason::Terminate,
                uuid,
            } => {
                assert_eq!(record.offset.get(), 27);
                assert_eq!(
                    uuid.map(phonowire_audiosocket::Uuid::bytes),
                    transcript.uuid
                );
                assert!(!transcript.ended);
                transcript.ended = true;
            }
            other => panic!("unexpected stream event: {other:?}"),
        }
    }
}

fn uuid_frame(marker: u8) -> Vec<u8> {
    let mut frame = vec![1, 0, 16];
    frame.extend_from_slice(&[marker; 16]);
    frame
}

fn tail(marker: u8) -> [u8; 8] {
    [0x10, 0, 2, marker, 0, 0, 0, 0]
}

fn connect_started(running: &Running, marker: u8, transcripts: &mut Transcripts) -> TcpStream {
    let count = transcripts.len();
    let mut peer = TcpStream::connect(running.address).expect("connect TCP peer");
    peer.write_all(&uuid_frame(marker))
        .expect("write initial UUID");
    collect_until(&running.records, transcripts, |items| {
        items.values().filter(|item| item.uuid.is_some()).count() > count
    });
    peer
}

#[test]
fn two_established_streams_survive_third_attempt_error_and_another_peer_is_admitted() {
    for error in [libc::ECONNABORTED, libc::EPROTO] {
        let running = Running::new(8);
        let mut transcripts = Transcripts::new();
        let first = connect_started(&running, 1, &mut transcripts);
        let second = connect_started(&running, 2, &mut transcripts);
        running.inject(error, false);
        let mut third = TcpStream::connect(running.address).expect("third peer");
        third.write_all(&uuid_frame(3)).expect("third UUID");
        running.observed(error);
        let mut fourth = TcpStream::connect(running.address).expect("later peer");
        fourth.write_all(&uuid_frame(4)).expect("later UUID");
        let mut peers = [first, second, third, fourth];
        for (index, peer) in peers.iter_mut().enumerate() {
            peer.write_all(&tail(u8::try_from(index + 1).expect("marker")))
                .expect("continue live stream");
        }
        collect_until(&running.records, &mut transcripts, |items| {
            items.values().filter(|item| item.ended).count() == 4
        });
        for (index, peer) in peers.iter().enumerate() {
            let marker = u8::try_from(index + 1).expect("marker");
            let actual = transcripts
                .values()
                .find(|item| item.peer == Some(peer.local_addr().expect("peer address")))
                .expect("peer transcript");
            let mut expected = uuid_frame(marker);
            expected.extend_from_slice(&tail(marker));
            assert_eq!(actual.raw, expected);
            assert_eq!(actual.uuid, Some([marker; 16]));
            assert_eq!(actual.audio, [marker, 0]);
        }
        let summary = running.finish();
        assert_eq!(summary.accepted, 4);
        assert_eq!(summary.ended, 4);
        assert_eq!(summary.abandoned, 0);
        assert_eq!(summary.raw_bytes_read, 108);
        assert_eq!(summary.undelivered_raw_bytes, 0);
    }
}

#[test]
fn active_streams_and_stop_progress_during_persistent_accept_resource_pressure() {
    let running = Running::new(1);
    let mut transcripts = Transcripts::new();
    let mut first = connect_started(&running, 1, &mut transcripts);
    let second = connect_started(&running, 2, &mut transcripts);
    running.inject(libc::EMFILE, true);
    let _pending = TcpStream::connect(running.address).expect("pending peer triggers accept");
    running.observed(libc::EMFILE);
    first
        .write_all(&tail(1))
        .expect("continue established peer during pressure");
    collect_until(&running.records, &mut transcripts, |items| {
        items.values().any(|item| item.ended)
    });
    let finished = transcripts
        .values()
        .find(|item| item.ended)
        .expect("finished first peer");
    let mut expected = uuid_frame(1);
    expected.extend_from_slice(&tail(1));
    assert_eq!(finished.raw, expected);
    assert_eq!(finished.audio, [1, 0]);
    let summary = running.finish();
    assert_eq!(summary.stopped_by, Some(StopCause::Requested));
    assert_eq!(summary.accepted, 2);
    assert_eq!(summary.ended, 1);
    assert_eq!(summary.abandoned, 1);
    assert_eq!(summary.live, 1);
    drop(second);
}

#[test]
fn consumer_disconnection_stops_a_worker_in_accept_cooldown() {
    let running = Running::new(1);
    running.inject(libc::ENOMEM, true);
    let _peer = TcpStream::connect(running.address).expect("pending peer");
    running.observed(libc::ENOMEM);
    drop(running.records);
    let summary = running
        .worker
        .join()
        .expect("worker joins")
        .expect("consumer stop succeeds");
    assert_eq!(summary.stopped_by, Some(StopCause::ConsumerDisconnected));
    assert_eq!(summary.accepted, 0);
}

#[test]
fn fatal_accept_and_registration_failures_preserve_the_original_error_and_cleanup() {
    for (error, registration) in [
        (libc::EBADF, false),
        (libc::ENOSYS, false),
        (libc::ENOMEM, true),
    ] {
        let running = Running::new(8);
        let mut transcripts = Transcripts::new();
        let mut first = connect_started(&running, 1, &mut transcripts);
        if registration {
            running.faults.register.store(error, Ordering::Release);
        } else {
            running.inject(error, false);
        }
        let mut pending = TcpStream::connect(running.address).expect("trigger next accept");
        running.observed(error);
        let failed = running
            .worker
            .join()
            .expect("worker joins")
            .expect_err("fatal boundary");
        let ReceiverFailure::Io(actual) = failed.failure() else {
            panic!("expected original I/O error")
        };
        assert_eq!(actual.raw_os_error(), Some(error));
        let summary = failed.summary();
        assert_eq!(summary.accepted, 1);
        assert_eq!(summary.abandoned, 1);
        assert_eq!(summary.live, 1);
        assert_eq!(summary.raw_bytes_read, 19);
        assert_eq!(summary.undelivered_raw_bytes, 0);
        assert!(summary.counters_complete);
        first
            .set_read_timeout(Some(DEADLINE))
            .expect("read timeout");
        assert_eq!(first.read(&mut [0]).expect("established peer closed"), 0);
        pending
            .set_read_timeout(Some(DEADLINE))
            .expect("read timeout");
        match pending.read(&mut [0]) {
            Ok(0) => {}
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
            other => panic!("unadmitted peer must be closed: {other:?}"),
        }
    }
}

#[test]
fn timed_accept_retry_recovers_pending_peer_without_new_connect_or_write() {
    let running = Running::new(1);
    let mut transcripts = Transcripts::new();
    let first = connect_started(&running, 1, &mut transcripts);
    let second = connect_started(&running, 2, &mut transcripts);
    running.inject(libc::EMFILE, false);
    let mut pending = TcpStream::connect(running.address).expect("peer waiting in backlog");
    pending.write_all(&uuid_frame(3)).expect("queue UUID once");
    running.observed(libc::EMFILE);
    // Real Receiver::run, Poll and monotonic time perform the retry. No more
    // network operations occur until the waiting peer's UUID is delivered.
    collect_until(&running.records, &mut transcripts, |items| {
        items.values().filter(|item| item.uuid.is_some()).count() == 3
    });
    for marker in [1, 2, 3] {
        let transcript = transcripts
            .values()
            .find(|item| item.uuid == Some([marker; 16]))
            .expect("established UUID");
        assert_eq!(transcript.raw, uuid_frame(marker));
    }
    let summary = running.finish();
    assert_eq!(summary.accepted, 3);
    assert_eq!(summary.abandoned, 3);
    assert_eq!(summary.raw_bytes_read, 57);
    assert_eq!(summary.undelivered_raw_bytes, 0);
    drop((first, second, pending));
}
