//! Bounded record handoff with separate slot-credit and disconnect notifications.
use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::task::Waker;
use std::time::Duration;

use crate::Record;

/// Internal ownership-preserving result of a non-blocking record handoff.
#[derive(Debug)]
pub enum SendFailure {
    /// The bounded queue is full; the original record remains owned by the caller.
    Full(Box<Record>),
    /// The consumer disappeared; the original record remains owned by the caller.
    Disconnected(Box<Record>),
}

/// Sender held by the worker for non-blocking record handoff.
#[derive(Clone, Debug)]
pub struct RecordSender {
    sender: mpsc::SyncSender<Record>,
}

/// Consumer-facing receiver for bounded worker observations.
#[derive(Debug)]
pub struct Records {
    receiver: Option<mpsc::Receiver<Record>>,
    credit: Waker,
    disconnected: Waker,
}

pub fn channel(
    capacity: NonZeroUsize,
    credit: Waker,
    disconnected: Waker,
) -> (RecordSender, Records) {
    let (sender, receiver) = mpsc::sync_channel(capacity.get());
    (
        RecordSender { sender },
        Records {
            receiver: Some(receiver),
            credit,
            disconnected,
        },
    )
}

impl RecordSender {
    pub fn try_send(&self, record: Record) -> Result<(), SendFailure> {
        match self.sender.try_send(record) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(record)) => Err(SendFailure::Full(Box::new(record))),
            Err(mpsc::TrySendError::Disconnected(record)) => {
                Err(SendFailure::Disconnected(Box::new(record)))
            }
        }
    }
}

impl Records {
    /// Blocks until a record is available or every sender disconnects.
    ///
    /// # Errors
    /// Returns disconnection when no queued record or live sender remains.
    pub fn recv(&self) -> Result<Record, mpsc::RecvError> {
        let result = self.receiver().recv();
        if result.is_ok() {
            self.credit.wake_by_ref();
        }
        result
    }

    /// Returns immediately with a record, an empty result, or disconnection.
    ///
    /// # Errors
    /// Returns empty when no record is queued, or disconnection after all senders close.
    pub fn try_recv(&self) -> Result<Record, mpsc::TryRecvError> {
        let result = self.receiver().try_recv();
        if result.is_ok() {
            self.credit.wake_by_ref();
        }
        result
    }

    /// Waits up to `timeout` for one record.
    ///
    /// # Errors
    /// Returns timeout or disconnection when no record becomes available.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Record, mpsc::RecvTimeoutError> {
        let result = self.receiver().recv_timeout(timeout);
        if result.is_ok() {
            self.credit.wake_by_ref();
        }
        result
    }

    const fn receiver(&self) -> &mpsc::Receiver<Record> {
        self.receiver
            .as_ref()
            .expect("record receiver remains present until drop")
    }
}

impl Drop for Records {
    fn drop(&mut self) {
        let receiver = self.receiver.take();
        drop(receiver);
        self.disconnected.wake_by_ref();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;
    use std::time::Instant;

    use crate::{ConnectionId, RecordKind, WireOffset};

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn capacity(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("test capacity is positive")
    }

    fn record(offset: u64) -> Record {
        Record {
            connection: ConnectionId::new(1, 1),
            offset: WireOffset::new(offset),
            observed_at: Instant::now(),
            kind: RecordKind::Connected {
                peer: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9),
            },
        }
    }

    #[test]
    fn full_returns_the_exact_record_and_removal_wakes_credit() {
        let credit_counter = Arc::new(WakeCount::default());
        let disconnected_counter = Arc::new(WakeCount::default());
        let (sender, records) = channel(
            capacity(1),
            Waker::from(credit_counter.clone()),
            Waker::from(disconnected_counter),
        );
        match sender.try_send(record(7)) {
            Ok(()) => {}
            Err(error) => panic!("first record should fit: {error:?}"),
        }
        match sender.try_send(record(8)) {
            Err(SendFailure::Full(returned)) => assert_eq!(returned.offset.get(), 8),
            Err(error) => panic!("second record should be full: {error:?}"),
            Ok(()) => panic!("second record unexpectedly fit"),
        }
        let received = match records.try_recv() {
            Ok(record) => record,
            Err(error) => panic!("queued record should be available: {error:?}"),
        };
        assert_eq!(received.offset.get(), 7);
        assert_eq!(credit_counter.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn receiver_drop_wakes_disconnection_after_closing_the_channel() {
        let credit_counter = Arc::new(WakeCount::default());
        let disconnected_counter = Arc::new(WakeCount::default());
        let (sender, records) = channel(
            capacity(1),
            Waker::from(credit_counter),
            Waker::from(disconnected_counter.clone()),
        );
        drop(records);
        assert_eq!(disconnected_counter.0.load(Ordering::SeqCst), 1);
        match sender.try_send(record(1)) {
            Err(SendFailure::Disconnected(returned)) => {
                assert_eq!(returned.offset.get(), 1);
            }
            Err(error) => panic!("receiver should be disconnected: {error:?}"),
            Ok(()) => panic!("send unexpectedly succeeded after receiver drop"),
        }
    }
}
