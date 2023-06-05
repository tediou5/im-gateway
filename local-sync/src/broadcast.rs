use crate::linked_list::LinkedList;
use std::{
    cell::Cell,
    marker::PhantomPinned,
    rc::Rc,
    task::{Context, Poll, Waker},
};

pub struct Sender<T> {
    shared: Rc<Shared<T>>,
}

pub struct Receiver<T> {
    shared: Rc<Shared<T>>,

    /// Next position to read from
    next: u64,
}

struct Shared<T> {
    buffer: Box<[std::cell::Cell<Slot<T>>]>,

    /// Mask a position -> index.
    mask: usize,

    /// Tail of the queue. Includes the rx wait list.
    tail: std::cell::Cell<Tail>,

    /// Number of outstanding Sender handles.
    num_tx: usize,
}

struct Slot<T> {
    /// Remaining number of receivers that are expected to see this value.
    ///
    /// When this goes to zero, the value is released.
    rem: usize,

    /// Uniquely identifies the `send` stored in the slot.
    pos: u64,

    /// The value being broadcast.
    ///
    /// The value is set by `send` when the write lock is held. When a reader
    /// drops, `rem` is decremented. When it hits zero, the value is dropped.
    val: UnsafeCell<Option<T>>,
}

struct Tail {
    /// Next position to write to.
    pos: u64,

    /// Number of active receivers.
    rx_cnt: usize,

    /// True if the channel is closed.
    closed: bool,

    /// Receivers waiting for a value.
    waiters: LinkedList<Waiter, <Waiter as crate::linked_list::Link>::Target>,
}

/// An entry in the wait queue.
struct Waiter {
    /// True if queued.
    queued: bool,

    /// Task waiting on the broadcast channel.
    waker: Option<Waker>,

    /// Intrusive linked-list pointers.
    pointers: crate::linked_list::Pointers<Waiter>,

    /// Should not be `Unpin`.
    _p: PhantomPinned,
}
