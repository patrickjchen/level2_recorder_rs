//! Bounded lock-free single-producer / single-consumer queue over `rtrb`,
//! with the two things the pipeline needs on top:
//!
//! * **close**: dropping the `Sender` closes the queue. `Receiver::is_closed()`
//!   reports that; consumers read it *before* draining so a close issued
//!   after the producer's final push can never hide that final item.
//! * **gauge**: a shared `QueueGauge` (len / peak) the main thread reads for
//!   the progress line and the summary, without touching the ring itself.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct QueueGauge {
    pub len: AtomicUsize,
    pub peak: AtomicUsize,
    pub capacity: AtomicUsize,
}

pub struct Sender<T> {
    p: rtrb::Producer<T>,
    gauge: Arc<QueueGauge>,
}

pub struct Receiver<T> {
    c: rtrb::Consumer<T>,
    gauge: Arc<QueueGauge>,
}

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>, Arc<QueueGauge>) {
    let (p, c) = rtrb::RingBuffer::<T>::new(capacity);
    let gauge = Arc::new(QueueGauge::default());
    gauge.capacity.store(capacity, Ordering::Relaxed);
    (Sender { p, gauge: gauge.clone() }, Receiver { c, gauge: gauge.clone() }, gauge)
}

impl<T> Sender<T> {
    /// Blocks (spin, then yield) until there is room. Never drops: for a
    /// recorder, back-pressure beats data loss. Returns `false` only if the
    /// consumer is gone (then the item is dropped and the caller should stop).
    pub fn push(&mut self, v: T) -> bool {
        let mut v = v;
        let mut spins = 0u32;
        loop {
            match self.p.push(v) {
                Ok(()) => {
                    let used = self.p.buffer().capacity() - self.p.slots();
                    self.gauge.len.store(used, Ordering::Relaxed);
                    if used > self.gauge.peak.load(Ordering::Relaxed) {
                        self.gauge.peak.store(used, Ordering::Relaxed);
                    }
                    return true;
                }
                Err(rtrb::PushError::Full(x)) => {
                    if self.p.is_abandoned() {
                        return false;
                    }
                    v = x;
                    spins += 1;
                    if spins > 64 {
                        std::thread::yield_now();
                    }
                }
            }
        }
    }
}

impl<T> Receiver<T> {
    /// Oldest item without removing it; valid until the next `pop()`.
    #[inline]
    pub fn peek(&self) -> Option<&T> {
        self.c.peek().ok()
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let r = self.c.pop().ok();
        if r.is_some() {
            self.gauge.len.store(self.c.slots(), Ordering::Relaxed);
        }
        r
    }

    /// Producer dropped. Items may still be queued: drain after checking.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.c.is_abandoned()
    }

    pub fn len(&self) -> usize {
        self.c.slots()
    }
    pub fn is_empty(&self) -> bool {
        self.c.is_empty()
    }
}
