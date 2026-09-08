//! Shared accounting for output bytes retained by receiver records.
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Waker;

/// A refusal to retain more output bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BudgetError {
    /// One request exceeds the entire configured capacity.
    TooLarge {
        /// Bytes requested.
        required: usize,
        /// Maximum retained bytes.
        capacity: usize,
    },
    /// Existing retained bytes leave insufficient capacity for this request.
    Full {
        /// Bytes requested.
        required: usize,
        /// Bytes currently available.
        available: usize,
    },
}

impl fmt::Display for BudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { required, capacity } => {
                write!(
                    formatter,
                    "requested {required} bytes exceeds capacity {capacity}"
                )
            }
            Self::Full {
                required,
                available,
            } => {
                write!(
                    formatter,
                    "requested {required} bytes with only {available} available"
                )
            }
        }
    }
}

impl std::error::Error for BudgetError {}

/// Shared retained-output capacity.
#[derive(Clone, Debug)]
pub struct ByteBudget {
    inner: Arc<BudgetInner>,
}

#[derive(Debug)]
struct BudgetInner {
    capacity: usize,
    state: Mutex<BudgetState>,
}

#[derive(Debug)]
struct BudgetState {
    used: usize,
    subscriber: Option<Waker>,
}

/// Owned bytes charged to a [`ByteBudget`] until their backing storage drops.
#[derive(Debug)]
pub struct OwnedBytes {
    bytes: Box<[u8]>,
    charge: Charge,
}

/// A reservation that refunds retained capacity unless transferred to storage.
#[derive(Debug)]
struct Charge {
    budget: ByteBudget,
    bytes: usize,
}

/// A worker's exclusive byte-credit notification registration.
#[derive(Debug)]
pub struct BudgetSubscription {
    budget: ByteBudget,
}

/// A second worker attempted to subscribe to the same budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetInUse;

impl ByteBudget {
    /// Creates a retained-output byte account with a positive capacity.
    #[must_use]
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(BudgetInner {
                capacity: capacity.get(),
                state: Mutex::new(BudgetState {
                    used: 0,
                    subscriber: None,
                }),
            }),
        }
    }

    /// Returns the maximum retained output bytes.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Returns bytes currently retained by live [`OwnedBytes`] values.
    #[must_use]
    pub fn used(&self) -> usize {
        self.locked().used
    }

    /// Copies bytes into separately owned storage after reserving retained capacity.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError::TooLarge`] when one request exceeds the entire
    /// account and [`BudgetError::Full`] when live output consumes the remainder.
    pub fn try_copy(&self, bytes: &[u8]) -> Result<OwnedBytes, BudgetError> {
        let required = bytes.len();
        if required > self.capacity() {
            return Err(BudgetError::TooLarge {
                required,
                capacity: self.capacity(),
            });
        }
        let charge = self.reserve(required)?;
        let storage: Box<[u8]> = Box::from(bytes);
        Ok(OwnedBytes {
            bytes: storage,
            charge,
        })
    }

    fn reserve(&self, required: usize) -> Result<Charge, BudgetError> {
        let mut state = self.locked();
        let available = self.capacity() - state.used;
        if required > available {
            return Err(BudgetError::Full {
                required,
                available,
            });
        }
        state.used += required;
        drop(state);
        Ok(Charge {
            budget: self.clone(),
            bytes: required,
        })
    }

    pub(crate) fn subscribe(&self, waker: Waker) -> Result<BudgetSubscription, BudgetInUse> {
        let mut state = self.locked();
        if state.subscriber.is_some() {
            return Err(BudgetInUse);
        }
        state.subscriber = Some(waker);
        drop(state);
        Ok(BudgetSubscription {
            budget: self.clone(),
        })
    }

    fn release(&self, released: usize) {
        if released == 0 {
            return;
        }
        let wake = {
            let mut state = self.locked();
            state.used = state
                .used
                .checked_sub(released)
                .expect("byte budget released more storage than it charged");
            state.subscriber.clone()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn locked(&self) -> MutexGuard<'_, BudgetState> {
        self.inner
            .state
            .lock()
            .expect("byte budget lock cannot be poisoned")
    }
}

impl OwnedBytes {
    /// Returns the accessible backing capacity charged to the shared account.
    #[must_use]
    pub const fn charged_capacity(&self) -> usize {
        self.charge.bytes
    }

    /// Returns the retained byte slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

impl Drop for BudgetSubscription {
    fn drop(&mut self) {
        let mut state = self.budget.locked();
        state.subscriber = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;
    use std::thread;

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

    #[test]
    fn clones_share_exact_full_and_oversized_refusal() {
        let budget = ByteBudget::new(capacity(3));
        let clone = budget.clone();
        assert!(matches!(
            clone.try_copy(&[1, 2, 3, 4]),
            Err(BudgetError::TooLarge {
                required: 4,
                capacity: 3,
            })
        ));
        let retained = match budget.try_copy(&[1, 2]) {
            Ok(bytes) => bytes,
            Err(error) => panic!("first copy should fit: {error:?}"),
        };
        assert_eq!(clone.used(), 2);
        assert!(matches!(
            clone.try_copy(&[3, 4]),
            Err(BudgetError::Full {
                required: 2,
                available: 1,
            })
        ));
        drop(retained);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn cross_thread_last_drop_wakes_the_current_subscriber() {
        let budget = ByteBudget::new(capacity(2));
        let counter = Arc::new(WakeCount::default());
        let waker = Waker::from(counter.clone());
        let subscription = match budget.subscribe(waker) {
            Ok(subscription) => subscription,
            Err(error) => panic!("first subscriber should register: {error:?}"),
        };
        let retained = match budget.try_copy(&[1, 2]) {
            Ok(bytes) => bytes,
            Err(error) => panic!("copy should fit: {error:?}"),
        };
        let worker = thread::spawn(move || drop(retained));
        worker.join().expect("drop thread completes");
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        assert_eq!(budget.used(), 0);
        drop(subscription);
    }

    #[test]
    fn subscription_is_exclusive_and_drop_allows_replacement() {
        let budget = ByteBudget::new(capacity(1));
        let first_counter = Arc::new(WakeCount::default());
        let first = match budget.subscribe(Waker::from(first_counter)) {
            Ok(subscription) => subscription,
            Err(error) => panic!("first subscriber should register: {error:?}"),
        };
        let second_counter = Arc::new(WakeCount::default());
        assert!(matches!(
            budget.subscribe(Waker::from(second_counter.clone())),
            Err(BudgetInUse)
        ));
        drop(first);
        let replacement = match budget.subscribe(Waker::from(second_counter.clone())) {
            Ok(subscription) => subscription,
            Err(error) => panic!("replacement subscriber should register: {error:?}"),
        };
        let retained = match budget.try_copy(&[1]) {
            Ok(bytes) => bytes,
            Err(error) => panic!("copy should fit: {error:?}"),
        };
        drop(retained);
        assert_eq!(second_counter.0.load(Ordering::SeqCst), 1);
        drop(replacement);
    }
}
