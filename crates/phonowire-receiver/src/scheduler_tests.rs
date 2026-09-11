//! Resource progress through actual handoff and owned-byte operations.
use super::*;
use crate::memory::BudgetSubscription;
use crate::{Records, WireOffset};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::time::Instant;

struct Harness {
    scheduler: Scheduler,
    signals: Arc<Signals>,
    budget: ByteBudget,
    sender: RecordSender,
    records: Records,
    _subscription: BudgetSubscription,
}

struct Task<T> {
    context: TaskContext,
    waker: Waker,
    future: Pin<Box<dyn Future<Output = T> + Send>>,
}

impl Harness {
    fn new(connections: usize, slots: usize, budget: ByteBudget) -> Self {
        let signals = Signals::new();
        let scheduler = Scheduler::new(connections, Arc::clone(&signals));
        let (sender, records) = crate::handoff::channel(
            positive(slots),
            signals.waker(Signal::Queue),
            signals.waker(Signal::Disconnected),
        );
        let subscription = budget
            .subscribe(signals.waker(Signal::Bytes))
            .expect("one worker owns the test budget subscription");
        Self {
            scheduler,
            signals,
            budget,
            sender,
            records,
            _subscription: subscription,
        }
    }

    fn task<T, F>(&self, token: usize, operation: impl FnOnce(TaskContext) -> F) -> Task<T>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let token = Token(token);
        let waker = self.scheduler.insert(token);
        let context = TaskContext::new(
            ConnectionId::new(1, token.0),
            SocketAddr::from(([127, 0, 0, 1], 9)),
            token,
            TaskResources {
                budget: self.budget.clone(),
                retention: ByteBudget::retention_tracker(),
                sender: self.sender.clone(),
                scheduler: self.scheduler.clone(),
            },
        );
        let future = Box::pin(operation(context.clone()));
        Task {
            context,
            waker,
            future,
        }
    }

    fn poll<T>(&self, task: &mut Task<T>, quota: usize) -> Poll<T> {
        assert_eq!(self.scheduler.take(), Some(task.context.token));
        task.context.reset_turn(quota);
        let polled = task
            .future
            .as_mut()
            .poll(&mut Context::from_waker(&task.waker));
        if polled.is_pending() {
            self.scheduler.verify_pending(task.context.token);
        }
        let live = lock(&self.scheduler.0.ready).states.len();
        assert_membership_bounds(&self.scheduler, live);
        polled
    }

    fn resume(&self) {
        if self.signals.take_queue_credit() {
            self.scheduler.resume_credit(WaitReason::Queue);
        }
        if self.signals.take_byte_credit() {
            self.scheduler.resume_credit(WaitReason::Bytes);
        }
    }

    fn fill(&self, offset: u64) {
        self.sender
            .try_send(record(ConnectionId::new(1, 1), offset))
            .expect("initial record fills an available slot");
    }
}

fn positive(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test capacity is positive")
}

fn record(connection: ConnectionId, offset: u64) -> Record {
    Record {
        connection,
        offset: WireOffset::new(offset),
        observed_at: Instant::now(),
        kind: RecordKind::Connected {
            peer: SocketAddr::from(([127, 0, 0, 1], 9)),
        },
    }
}

async fn produce(context: TaskContext) {
    for offset in 0..256 {
        context
            .send(record(context.id(), offset))
            .await
            .expect("consumer remains connected");
    }
}

async fn send_one(context: TaskContext) -> Result<(), SendFailure> {
    context.send(record(context.id(), 0)).await
}

async fn copy_four(context: TaskContext) -> Result<OwnedBytes, BudgetError> {
    context.copy(&[1, 2, 3, 4]).await
}

async fn copy_two(context: TaskContext) -> Result<OwnedBytes, BudgetError> {
    context.copy(&[5, 6]).await
}

fn copied(polled: Poll<Result<OwnedBytes, BudgetError>>) -> OwnedBytes {
    match polled {
        Poll::Ready(Ok(bytes)) => bytes,
        other => panic!("copy must have acquired actual storage: {other:?}"),
    }
}

fn sent(polled: Poll<Result<(), SendFailure>>) {
    assert!(matches!(polled, Poll::Ready(Ok(()))), "{polled:?}");
}

#[test]
fn queue_turns_prevent_new_runnable_and_repeat_producers_from_bypassing() {
    let test = Harness::new(3, 1, ByteBudget::new(positive(4)));
    test.fill(0);
    let mut first = test.task(2, produce);
    let mut second = test.task(3, produce);
    assert!(test.poll(&mut first, 128).is_pending());
    assert!(test.poll(&mut second, 128).is_pending());
    test.records.try_recv().expect("remove initial record");

    // The younger task is already runnable and a new producer arrives before
    // the returned credit is observed. Both run before the entitled head.
    second.waker.wake_by_ref();
    let mut newcomer = test.task(4, produce);
    test.resume();
    assert!(test.poll(&mut second, 128).is_pending());
    assert!(test.poll(&mut newcomer, 128).is_pending());
    assert!(test.records.try_recv().is_err());
    assert!(test.poll(&mut first, 128).is_pending());
    assert_eq!(
        test.records
            .try_recv()
            .expect("first useful handoff")
            .connection,
        first.context.id()
    );

    let mut grants = [1, 0, 0];
    for round in 0..191 {
        test.resume();
        let index = (round + 1) % 3;
        let task = match index {
            0 => &mut first,
            1 => &mut second,
            2 => &mut newcomer,
            _ => unreachable!("remainder names one of three producers"),
        };
        assert!(test.poll(task, 128).is_pending());
        let received = test.records.try_recv().expect("one released slot is used");
        assert_eq!(received.connection, task.context.id());
        assert_eq!(received.offset.get(), grants[index]);
        grants[index] += 1;
    }
    assert_eq!(grants, [64, 64, 64]);
}

#[test]
fn unequal_byte_requests_accumulate_without_changing_external_allocation() {
    let budget = ByteBudget::new(positive(4));
    let first_hold = budget.try_copy(&[8, 8]).expect("first live allocation");
    let second_hold = budget.try_copy(&[9, 9]).expect("second live allocation");
    let test = Harness::new(2, 1, budget.clone());
    let mut large = test.task(2, copy_four);
    let mut small = test.task(3, copy_two);
    assert!(test.poll(&mut large, 8).is_pending());
    assert!(test.poll(&mut small, 8).is_pending());
    drop(first_hold);
    test.resume();
    assert!(test.poll(&mut large, 8).is_pending());
    small.waker.wake_by_ref();
    assert!(test.poll(&mut small, 8).is_pending());
    assert_eq!(
        budget.used(),
        2,
        "younger receiver leaves space for the head"
    );

    let external = budget
        .try_copy(&[7, 7])
        .expect("a waiting receiver does not reserve public capacity");
    assert_eq!(budget.used(), 4);
    assert!(matches!(
        budget.try_copy(&[0]),
        Err(BudgetError::Full {
            required: 1,
            available: 0,
        })
    ));
    assert!(matches!(
        budget.try_copy(&[0; 5]),
        Err(BudgetError::TooLarge {
            required: 5,
            capacity: 4,
        })
    ));
    drop(external);
    test.resume();
    assert!(test.poll(&mut large, 8).is_pending());
    assert_eq!(budget.used(), 2);
    drop(second_hold);
    test.resume();
    let large_output = copied(test.poll(&mut large, 8));
    assert_eq!(large_output.as_slice(), [1, 2, 3, 4]);
    assert_eq!(large_output.charged_capacity(), 4);
    assert!(test.poll(&mut small, 8).is_pending());
    std::thread::spawn(move || drop(large_output))
        .join()
        .expect("consumer releases large output on another thread");
    test.resume();
    let small_output = copied(test.poll(&mut small, 8));
    assert_eq!(small_output.as_slice(), [5, 6]);
    drop(small_output);
    assert_eq!(budget.used(), 0);
}

#[test]
fn impossible_byte_request_does_not_wait_behind_a_blocked_head() {
    let budget = ByteBudget::new(positive(4));
    let retained = budget.try_copy(&[0; 4]).expect("full live account");
    let test = Harness::new(2, 1, budget);
    let mut head = test.task(2, copy_four);
    let mut oversized = test.task(3, |context| async move { context.copy(&[0; 5]).await });
    assert!(test.poll(&mut head, 8).is_pending());
    assert!(matches!(
        test.poll(&mut oversized, 8),
        Poll::Ready(Err(BudgetError::TooLarge {
            required: 5,
            capacity: 4,
        }))
    ));
    drop(retained);
    test.resume();
    drop(copied(test.poll(&mut head, 8)));
}

#[test]
fn coalesced_slot_notifications_allow_every_available_handoff() {
    let test = Harness::new(3, 3, ByteBudget::new(positive(4)));
    for offset in 0..3 {
        test.fill(offset);
    }
    let mut tasks = [
        test.task(2, send_one),
        test.task(3, send_one),
        test.task(4, send_one),
    ];
    for task in &mut tasks {
        assert!(test.poll(task, 8).is_pending());
    }
    for _ in 0..3 {
        test.records.try_recv().expect("consumer returns a slot");
    }
    assert!(test.signals.take_queue_credit());
    assert!(!test.signals.take_queue_credit());
    test.scheduler.resume_credit(WaitReason::Queue);
    for task in &mut tasks {
        sent(test.poll(task, 8));
    }
    for task in &tasks {
        assert_eq!(
            test.records
                .try_recv()
                .expect("actual queued output")
                .connection,
            task.context.id()
        );
    }
}

#[test]
fn coalesced_byte_notifications_allow_every_fitting_copy() {
    let budget = ByteBudget::new(positive(6));
    let first_hold = budget.try_copy(&[0; 3]).expect("first live allocation");
    let second_hold = budget.try_copy(&[0; 3]).expect("second live allocation");
    let test = Harness::new(3, 1, budget.clone());
    let mut tasks = [
        test.task(2, copy_two),
        test.task(3, copy_two),
        test.task(4, copy_two),
    ];
    for task in &mut tasks {
        assert!(test.poll(task, 8).is_pending());
    }
    drop(first_hold);
    drop(second_hold);
    assert!(test.signals.take_byte_credit());
    assert!(!test.signals.take_byte_credit());
    test.scheduler.resume_credit(WaitReason::Bytes);
    let outputs: Vec<_> = tasks
        .iter_mut()
        .map(|task| copied(test.poll(task, 8)))
        .collect();
    assert_eq!(budget.used(), 6);
    for output in &outputs {
        assert_eq!(output.as_slice(), [5, 6]);
    }
    drop(outputs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn quota_yield_keeps_the_head_and_cancellation_reassigns_available_capacity() {
    let budget = ByteBudget::new(positive(4));
    let retained = budget.try_copy(&[0; 4]).expect("full live account");
    let test = Harness::new(2, 1, budget.clone());
    let mut head = test.task(2, copy_four);
    let mut next = test.task(3, copy_two);
    assert!(test.poll(&mut head, 8).is_pending());
    assert!(test.poll(&mut next, 8).is_pending());
    drop(retained);
    next.waker.wake_by_ref();
    test.resume();
    assert!(test.poll(&mut next, 8).is_pending());
    assert!(test.poll(&mut head, 0).is_pending());
    assert_eq!(budget.used(), 0, "an admission turn has no physical charge");
    assert!(test.poll(&mut head, 0).is_pending());
    let old_waker = head.waker.clone();
    drop(head);
    test.scheduler.retire(Token(2));
    old_waker.wake_by_ref();
    let output = copied(test.poll(&mut next, 8));
    assert_eq!(output.as_slice(), [5, 6]);
    drop(output);
    assert!(!test.scheduler.has_ready());
}

#[test]
fn a_byte_head_completes_after_quota_is_renewed_without_new_credit() {
    let budget = ByteBudget::new(positive(4));
    let retained = budget.try_copy(&[0; 4]).expect("full live account");
    let test = Harness::new(2, 1, budget);
    let mut head = test.task(2, copy_four);
    let mut next = test.task(3, copy_two);
    assert!(test.poll(&mut head, 8).is_pending());
    assert!(test.poll(&mut next, 8).is_pending());
    drop(retained);
    test.resume();
    assert!(test.poll(&mut head, 0).is_pending());
    let output = copied(test.poll(&mut head, 1));
    assert_eq!(output.as_slice(), [1, 2, 3, 4]);
    assert!(test.poll(&mut next, 1).is_pending());
    drop(output);
    test.resume();
    drop(copied(test.poll(&mut next, 1)));
}

#[test]
fn canceling_a_queue_head_activates_its_successor_without_another_credit() {
    let test = Harness::new(2, 1, ByteBudget::new(positive(4)));
    test.fill(0);
    let mut head = test.task(2, send_one);
    let mut next = test.task(3, send_one);
    assert!(test.poll(&mut head, 8).is_pending());
    assert!(test.poll(&mut next, 8).is_pending());
    test.records.try_recv().expect("free the one slot");
    test.resume();
    drop(head);
    test.scheduler.retire(Token(2));
    sent(test.poll(&mut next, 8));
    assert_eq!(
        test.records
            .try_recv()
            .expect("successor record")
            .connection,
        next.context.id()
    );
}

#[test]
fn canceling_a_non_head_preserves_the_remaining_byte_order() {
    let budget = ByteBudget::new(positive(4));
    let retained = budget.try_copy(&[0; 4]).expect("full live account");
    let test = Harness::new(3, 1, budget);
    let mut head = test.task(2, copy_two);
    let mut canceled = test.task(3, copy_two);
    let mut last = test.task(4, copy_two);
    assert!(test.poll(&mut head, 8).is_pending());
    assert!(test.poll(&mut canceled, 8).is_pending());
    assert!(test.poll(&mut last, 8).is_pending());
    let waker = canceled.waker.clone();
    drop(canceled);
    test.scheduler.retire(Token(3));
    waker.wake_by_ref();
    assert!(!test.scheduler.has_ready());
    drop(retained);
    test.resume();
    let first_output = copied(test.poll(&mut head, 8));
    let last_output = copied(test.poll(&mut last, 8));
    assert_eq!(first_output.as_slice(), [5, 6]);
    assert_eq!(last_output.as_slice(), [5, 6]);
    drop((first_output, last_output));
}

#[test]
fn a_byte_waiter_does_not_hold_the_queue_needed_to_release_owned_output() {
    let budget = ByteBudget::new(positive(4));
    let owned = budget.try_copy(&[7, 8, 9, 10]).expect("completed output");
    let test = Harness::new(2, 1, budget.clone());
    test.fill(0);
    let mut needs_bytes = test.task(2, copy_four);
    let mut sends_bytes = test.task(3, |context| async move {
        context.observe_read(4).expect("wire prefix observation");
        context
            .send(Record {
                connection: context.id(),
                offset: WireOffset::new(0),
                observed_at: Instant::now(),
                kind: RecordKind::Wire { bytes: owned },
            })
            .await
    });
    assert!(test.poll(&mut needs_bytes, 8).is_pending());
    assert!(test.poll(&mut sends_bytes, 8).is_pending());
    test.records
        .try_recv()
        .expect("consumer releases initial slot");
    test.resume();
    sent(test.poll(&mut sends_bytes, 8));
    let received = test
        .records
        .try_recv()
        .expect("owned output reached consumer");
    assert!(
        matches!(&received.kind, RecordKind::Wire { bytes } if bytes.as_slice() == [7, 8, 9, 10])
    );
    assert!(!test.scheduler.has_ready());
    drop(received);
    test.resume();
    drop(copied(test.poll(&mut needs_bytes, 8)));
    assert_eq!(budget.used(), 0);
}

#[test]
fn retiring_a_pending_owned_send_refunds_bytes_to_an_independent_waiter() {
    let budget = ByteBudget::new(positive(4));
    let owned = budget.try_copy(&[0; 4]).expect("completed output");
    let test = Harness::new(2, 1, budget.clone());
    test.fill(0);
    let mut needs_bytes = test.task(2, copy_four);
    let mut sends_bytes = test.task(3, |context| async move {
        context.observe_read(4).expect("wire prefix observation");
        context
            .send(Record {
                connection: context.id(),
                offset: WireOffset::new(0),
                observed_at: Instant::now(),
                kind: RecordKind::Wire { bytes: owned },
            })
            .await
    });
    assert!(test.poll(&mut needs_bytes, 8).is_pending());
    assert!(test.poll(&mut sends_bytes, 8).is_pending());
    test.scheduler.retire(Token(3));
    drop(sends_bytes);
    test.resume();
    drop(copied(test.poll(&mut needs_bytes, 8)));
    assert_eq!(budget.used(), 0);
    assert!(test.records.try_recv().is_ok());
    assert!(test.records.try_recv().is_err());
}

#[test]
fn retained_output_refunds_to_the_replacement_worker_subscription() {
    let budget = ByteBudget::new(positive(4));
    let old = Harness::new(1, 1, budget.clone());
    let mut task = old.task(2, copy_four);
    let retained = copied(old.poll(&mut task, 8));
    let old_waker = task.waker.clone();
    old.scheduler.retire(Token(2));
    drop(task);
    drop(old);

    let replacement = Harness::new(1, 1, budget.clone());
    let mut next = replacement.task(2, copy_four);
    assert!(replacement.poll(&mut next, 8).is_pending());
    old_waker.wake_by_ref();
    assert!(!replacement.scheduler.has_ready());
    assert_eq!(budget.used(), 4);
    std::thread::spawn(move || drop(retained))
        .join()
        .expect("old output's final owner drops storage");
    replacement.resume();
    let output = copied(replacement.poll(&mut next, 8));
    assert_eq!(budget.used(), 4);
    assert_eq!(output.as_slice(), [1, 2, 3, 4]);
    drop(output);
    assert_eq!(budget.used(), 0);
}

#[test]
fn wake_during_poll_survives_parking_and_wake_after_park_is_not_duplicated() {
    let signals = Signals::new();
    let scheduler = Scheduler::new(1, signals);
    let waker = scheduler.insert(Token(2));
    assert_eq!(scheduler.take(), Some(Token(2)));
    let during = waker.clone();
    std::thread::spawn(move || during.wake())
        .join()
        .expect("wake occurs during the current poll");
    scheduler.park(Token(2), WaitReason::Bytes);
    scheduler.verify_pending(Token(2));
    assert_eq!(scheduler.take(), Some(Token(2)));
    scheduler.park(Token(2), WaitReason::Bytes);
    assert!(!scheduler.has_ready());
    let after = waker.clone();
    std::thread::spawn(move || {
        for _ in 0..64 {
            after.wake_by_ref();
        }
    })
    .join()
    .expect("repeated wake occurs after park");
    assert_eq!(scheduler.take(), Some(Token(2)));
    assert_eq!(scheduler.take(), None);
    scheduler.retire(Token(2));
    waker.wake_by_ref();
    assert!(!scheduler.has_ready());
}

fn order_accesses(scheduler: &Scheduler) -> usize {
    let ready = lock(&scheduler.0.ready);
    ready.queue.accesses + ready.queue_waiters.accesses + ready.byte_waiters.accesses
}

fn assert_membership_bounds(scheduler: &Scheduler, live: usize) {
    let ready = lock(&scheduler.0.ready);
    assert_eq!(ready.states.len(), live);
    assert!(live <= ready.capacity);
    assert!(ready.queue.links.len() <= live);
    assert!(ready.queue_waiters.links.len() + ready.byte_waiters.links.len() <= live);
    for order in [&ready.queue, &ready.queue_waiters, &ready.byte_waiters] {
        let mut previous = None;
        let mut current = order.first;
        let mut visited = 0;
        while let Some(token) = current {
            assert!(ready.states.contains_key(&token));
            let links = order
                .links
                .get(&token)
                .expect("FIFO contains only live links");
            assert_eq!(links.previous, previous);
            previous = Some(token);
            current = links.next;
            visited += 1;
            assert!(visited <= live, "membership has no cycle");
        }
        assert_eq!(previous, order.last);
        assert_eq!(
            visited,
            order.links.len(),
            "no unreachable membership history"
        );
    }
    drop(ready);
}

#[test]
fn mass_park_retire_and_churn_use_bounded_link_work_and_live_metadata() {
    for capacity in [128, 1024, 4096] {
        let signals = Signals::new();
        let scheduler = Scheduler::new(capacity, signals);
        let wakers: Vec<_> = (0..capacity)
            .map(|token| scheduler.insert(Token(token)))
            .collect();
        let start = order_accesses(&scheduler);
        for token in 0..capacity {
            assert_eq!(scheduler.take(), Some(Token(token)));
            scheduler.park(Token(token), WaitReason::Bytes);
        }
        let parked = order_accesses(&scheduler) - start;
        assert!(
            parked <= 16 * capacity,
            "mass park has linear link accesses"
        );
        assert_membership_bounds(&scheduler, capacity);
        let start = order_accesses(&scheduler);
        for (token, waker) in wakers.iter().enumerate() {
            waker.wake_by_ref();
            scheduler.retire(Token(token));
            waker.wake_by_ref();
        }
        let retired = order_accesses(&scheduler) - start;
        assert!(
            retired <= 16 * capacity,
            "mass retire has linear link accesses"
        );
        assert_membership_bounds(&scheduler, 0);
        assert!(!scheduler.has_ready());

        // Also remove arbitrary queued entries without a preceding take.
        for token in 4 * capacity..5 * capacity {
            scheduler.insert(Token(token));
        }
        let start = order_accesses(&scheduler);
        for parity in [0, 1] {
            for offset in (parity..capacity).step_by(2) {
                scheduler.park(Token(4 * capacity + offset), WaitReason::Queue);
            }
        }
        let queued_park = order_accesses(&scheduler) - start;
        assert!(queued_park <= 16 * capacity);
        assert_membership_bounds(&scheduler, capacity);
        for parity in [1, 0] {
            for offset in (parity..capacity).step_by(2) {
                scheduler.retire(Token(4 * capacity + offset));
            }
        }
        assert_membership_bounds(&scheduler, 0);

        let start = order_accesses(&scheduler);
        for generation in 1..=3 {
            for token in 0..capacity {
                let current = Token(generation * capacity + token);
                let waker = scheduler.insert(current);
                scheduler.park(current, WaitReason::Queue);
                waker.wake_by_ref();
                scheduler.retire(current);
            }
            assert_membership_bounds(&scheduler, 0);
        }
        let churn = order_accesses(&scheduler) - start;
        assert!(
            churn <= 48 * capacity,
            "churn has no retained token history"
        );
        for waker in &wakers {
            waker.wake_by_ref();
        }
        assert!(!scheduler.has_ready());
        println!(
            "connections={capacity} park_link_accesses={parked} queued_park_link_accesses={queued_park} retire_link_accesses={retired} churn_link_accesses={churn} final_live=0"
        );
    }
}
