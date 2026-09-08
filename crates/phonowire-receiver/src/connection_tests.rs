//! Controlled reads establish resource waits independently of TCP packet boundaries.
use super::*;
use crate::scheduler::{Scheduler, Signal, Signals, TaskResources};
use crate::{ByteBudget, ConnectionId, Records};
use mio::Token;
use std::future::Future;
use std::io::Cursor;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Waker};

const FRAME: [u8; 27] = [
    1, 0, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x10, 0, 2, 7, 9, 0, 0, 0,
];
const TOKEN: Token = Token(2);

struct Harness {
    context: TaskContext,
    scheduler: Scheduler,
    signals: Arc<Signals>,
    waker: Waker,
    records: Records,
}
impl Harness {
    fn new(slots: usize, budget: ByteBudget) -> Self {
        let signals = Signals::new();
        let scheduler = Scheduler::new(1, Arc::clone(&signals));
        let waker = scheduler.insert(TOKEN);
        let (sender, records) = crate::handoff::channel(
            NonZeroUsize::new(slots).expect("slots"),
            signals.waker(Signal::Queue),
            signals.waker(Signal::Disconnected),
        );
        let context = TaskContext::new(
            ConnectionId::new(1, TOKEN.0),
            SocketAddr::from(([127, 0, 0, 1], 9)),
            TOKEN,
            TaskResources {
                budget,
                sender,
                scheduler: scheduler.clone(),
            },
        );
        Self {
            context,
            scheduler,
            signals,
            waker,
            records,
        }
    }
    fn poll<F: Future>(&self, future: Pin<&mut F>) -> Poll<F::Output> {
        assert_eq!(self.scheduler.take(), Some(TOKEN));
        self.context.reset_turn(128);
        future.poll(&mut Context::from_waker(&self.waker))
    }
    fn resume(&self) {
        if self.signals.take_queue_credit() {
            self.scheduler.resume_credit(WaitReason::Queue);
        }
        if self.signals.take_byte_credit() {
            self.scheduler.resume_credit(WaitReason::Bytes);
        }
    }
}

#[test]
fn held_wire_blocks_audio_copy_until_last_drop_without_more_input() {
    let budget = ByteBudget::new(NonZeroUsize::new(FRAME.len()).expect("budget"));
    let test = Harness::new(8, budget.clone());
    let _subscription = budget
        .subscribe(test.signals.waker(Signal::Bytes))
        .expect("subscription");
    let mut future = Box::pin(receive_from(Cursor::new(FRAME), 64, test.context.clone()));
    assert!(test.poll(future.as_mut()).is_pending());
    assert_eq!(test.context.progress().read, 27);
    assert_eq!(test.context.progress().consumed, 24);
    assert!(!test.context.progress().pending_record);
    assert!(!test.scheduler.has_ready());
    assert_eq!(budget.used(), 27);
    assert!(matches!(
        test.records.try_recv().expect("connected").kind,
        RecordKind::Connected { .. }
    ));
    let wire = test.records.try_recv().expect("wire");
    assert!(matches!(&wire.kind, RecordKind::Wire { bytes } if bytes.as_slice() == FRAME));
    assert!(matches!(
        test.records.try_recv().expect("started").kind,
        RecordKind::Started { .. }
    ));
    assert!(test.records.try_recv().is_err());
    test.resume();
    assert!(
        !test.scheduler.has_ready(),
        "queue slots cannot resume a byte wait"
    );
    std::thread::spawn(move || drop(wire))
        .join()
        .expect("last drop");
    test.resume();
    assert!(test.poll(future.as_mut()).is_ready());
    let audio = test.records.try_recv().expect("audio");
    assert!(matches!(&audio.kind, RecordKind::Audio { bytes, .. } if bytes.as_slice() == [7, 9]));
    assert!(matches!(
        test.records.try_recv().expect("terminal").kind,
        RecordKind::Ended {
            reason: EndReason::Terminate,
            ..
        }
    ));
    assert_eq!(test.context.progress().read, 27);
    drop(audio);
    drop(future);
    test.scheduler.retire(TOKEN);
    assert_eq!(budget.used(), 0);
}

#[test]
fn full_queue_retains_one_pending_record_until_cancellation() {
    let budget = ByteBudget::new(NonZeroUsize::new(4096).expect("budget"));
    let test = Harness::new(1, budget.clone());
    let mut future = Box::pin(receive_from(Cursor::new(FRAME), 64, test.context.clone()));
    assert!(test.poll(future.as_mut()).is_pending());
    assert!(test.context.progress().pending_record);
    assert_eq!(test.context.progress().records_queued, 1);
    assert_eq!(test.context.progress().read, 27);
    assert_eq!(test.context.progress().wire_queued, 0);
    assert!(!test.scheduler.has_ready());
    assert_eq!(budget.used(), 27);
    drop(future);
    test.scheduler.retire(TOKEN);
    assert_eq!(budget.used(), 0);
    assert!(matches!(
        test.records
            .try_recv()
            .expect("queued record survives task")
            .kind,
        RecordKind::Connected { .. }
    ));
    assert!(test.records.try_recv().is_err());
}

struct ShortReads {
    cursor: Cursor<[u8; 27]>,
    cap: usize,
}
impl Read for ShortReads {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = output.len().min(self.cap);
        self.cursor.read(&mut output[..count])
    }
}

#[test]
fn controlled_read_sizes_preserve_wire_pcm_offsets_and_end() {
    for cap in [1, 2, 3, 7, 24, 27] {
        let budget = ByteBudget::new(NonZeroUsize::new(4096).expect("budget"));
        let test = Harness::new(1, budget.clone());
        let mut future = Box::pin(receive_from(
            ShortReads {
                cursor: Cursor::new(FRAME),
                cap,
            },
            64,
            test.context.clone(),
        ));
        let mut raw = Vec::new();
        let mut audio = Vec::new();
        let mut ended = false;
        for _ in 0..128 {
            let finished = test.poll(future.as_mut()).is_ready();
            while let Ok(record) = test.records.try_recv() {
                match record.kind {
                    RecordKind::Wire { bytes } => {
                        assert_eq!(
                            record.offset.get(),
                            u64::try_from(raw.len()).expect("offset")
                        );
                        raw.extend_from_slice(bytes.as_slice());
                    }
                    RecordKind::Audio { bytes, .. } => {
                        assert!(raw.len() >= 24);
                        assert_eq!(record.offset.get(), 24);
                        audio.extend_from_slice(bytes.as_slice());
                    }
                    RecordKind::Started { .. } => assert_eq!(record.offset.get(), 19),
                    RecordKind::Ended {
                        reason: EndReason::Terminate,
                        ..
                    } => {
                        assert_eq!(record.offset.get(), 27);
                        ended = true;
                    }
                    RecordKind::Connected { .. } => {}
                    other => panic!("unexpected record: {other:?}"),
                }
            }
            if finished {
                break;
            }
            test.resume();
        }
        assert!(ended);
        assert_eq!(raw, FRAME);
        assert_eq!(audio, [7, 9]);
        drop(future);
        test.scheduler.retire(TOKEN);
        assert_eq!(budget.used(), 0);
    }
}
