use std::{
    collections::{BTreeMap, btree_map::Entry},
    future::Future,
    io,
    ops::Range,
    pin::Pin,
    task::{Context, Poll},
};

use proto::StreamId;
use tokio::sync::oneshot;

use crate::{WriteError, connection::State};

/// Evidence of local UDP submission for the tracked bytes of one stream write.
///
/// Succeeds when the local socket confirms submission of the entire tracked range. An empty
/// range completes immediately. Peer delivery and acknowledgment can happen later.
/// Dropping a receipt cancels observation. It neither withdraws accepted bytes nor keeps the
/// connection open. A receipt error can occur even when bytes reached the peer. Do not write the
/// accepted bytes again solely because their receipt failed; doing so can duplicate application data.
#[must_use = "await the receipt to observe local submission"]
#[derive(Debug)]
pub struct SendReceipt(Option<oneshot::Receiver<Submission>>);

// A socket may accept the transmit without confirming that every datagram was submitted.
type Submission = Result<bool, WriteError>;

// Writes on a stream have disjoint nonempty ranges. The end offset stays fixed as the first
// unconfirmed byte advances, so it also serves as the ordered lookup key for intersecting frames.
type Key = (StreamId, u64);

struct Pending {
    next: u64,
    result: oneshot::Sender<Submission>,
}

#[derive(Default)]
pub(crate) struct Receipts {
    pending: BTreeMap<Key, Pending>,
    // Metadata for the driver's single outstanding transmit, retained across WouldBlock.
    batch: Vec<(Key, Range<u64>)>,
}

impl SendReceipt {
    pub(crate) fn empty() -> Self {
        Self(None)
    }

    pub(crate) fn register(state: &mut State, stream: StreamId, end: u64, len: usize) -> Self {
        let pending = &mut state.receipts.get_or_insert_with(Default::default).pending;
        let Entry::Vacant(entry) = pending.entry((stream, end)) else {
            unreachable!("nonempty accepted writes have distinct end offsets");
        };
        let (tx, result) = oneshot::channel();
        entry.insert(Pending {
            next: end - len as u64,
            result: tx,
        });
        Self(Some(result))
    }
}

impl Future for SendReceipt {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(receiver) = &mut self.0 else {
            return Poll::Ready(Ok(()));
        };
        Pin::new(receiver).poll(cx).map(|result| match result {
            Ok(Ok(true)) => Ok(()),
            Ok(Ok(false)) => Err(io::Error::other("UDP socket did not confirm submission")),
            Ok(Err(error)) => Err(error.into()),
            Err(_) => Err(io::ErrorKind::ConnectionAborted.into()),
        })
    }
}

// Detach senders under the connection lock. Only the caller that released the lock may notify
// (or drop) them, because a receipt waker can synchronously call back into the connection.
#[derive(Default)]
pub(crate) struct ReceiptNotifications {
    first: Option<Completion>,
    more: Vec<Completion>,
}

enum Completion {
    One(oneshot::Sender<Submission>, Submission),
    All(Box<Receipts>, WriteError),
}

impl ReceiptNotifications {
    fn push(&mut self, completion: Completion) {
        if self.first.is_none() {
            self.first = Some(completion);
        } else {
            self.more.push(completion);
        }
    }

    pub(crate) fn fail_all(&mut self, receipts: Box<Receipts>, error: WriteError) {
        self.push(Completion::All(receipts, error));
    }

    pub(crate) fn notify(self) {
        for completion in self.first.into_iter().chain(self.more) {
            match completion {
                Completion::One(sender, result) => {
                    let _ = sender.send(result);
                }
                Completion::All(receipts, error) => {
                    for pending in receipts.pending.into_values() {
                        if !pending.result.is_closed() {
                            let _ = pending.result.send(Err(error.clone()));
                        }
                    }
                }
            }
        }
    }
}

impl Receipts {
    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(crate) fn capture(&mut self, stream: StreamId, range: Range<u64>) {
        for (&key, p) in self
            .pending
            .range((stream, range.start + 1)..=(stream, u64::MAX))
        {
            if p.next >= range.end {
                break;
            }
            self.batch
                .push((key, range.start.max(p.next)..range.end.min(key.1)));
        }
    }

    pub(crate) fn needs_status(&mut self, notifications: &mut ReceiptNotifications) -> bool {
        // A canceled write costs at most one entry per still-queued nonempty prefix. Reclaim
        // it on its next transmit (or stream/connection teardown), including a buffered retry.
        while let Some((key, _)) = self.batch.last() {
            if let Entry::Occupied(entry) = self.pending.entry(*key) {
                if !entry.get().result.is_closed() {
                    return true;
                }
                notifications.push(Completion::One(entry.remove().result, Ok(false)));
            }
            self.batch.pop();
        }
        false
    }

    pub(crate) fn submitted(&mut self, confirmed: bool, notifications: &mut ReceiptNotifications) {
        for (key, range) in self.batch.drain(..) {
            let Entry::Occupied(mut entry) = self.pending.entry(key) else {
                continue;
            };
            let pending = entry.get_mut();
            if range.end <= pending.next {
                continue;
            }
            if confirmed {
                // Fresh STREAM data is packetized in offset order. Retransmits may revisit a
                // prefix, but must never fill a gap merely by carrying a later byte.
                if range.start > pending.next {
                    continue;
                }
                pending.next = range.end;
                if pending.next < key.1 && !pending.result.is_closed() {
                    continue;
                }
            }
            let complete = confirmed && pending.next == key.1;
            notifications.push(Completion::One(entry.remove().result, Ok(complete)));
        }
    }

    pub(crate) fn fail_stream(
        &mut self,
        stream: StreamId,
        error: WriteError,
        notifications: &mut ReceiptNotifications,
    ) {
        while let Some((&key, _)) = self.pending.range((stream, 0)..=(stream, u64::MAX)).next() {
            notifications.push(Completion::One(
                self.pending.remove(&key).unwrap().result,
                Err(error.clone()),
            ));
        }
        self.batch.retain(|(key, _)| key.0 != stream);
    }
}
