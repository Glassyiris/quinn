use std::{
    collections::BTreeMap,
    future::Future,
    io,
    ops::Range,
    pin::Pin,
    task::{Context, Poll, ready},
};

use proto::StreamId;
use tokio::sync::oneshot;

use crate::connection::{ConnectionRef, State};

/// Evidence of local UDP submission for the bytes accepted by one stream write.
///
/// Resolves successfully only after all those bytes have been submitted to the local socket.
/// This is not a peer acknowledgment. If the socket cannot confirm a submission, the receipt
/// fails without changing QUIC's normal loss recovery. Dropping it cancels observation, not I/O.
#[must_use = "await the receipt to observe local submission"]
#[derive(Debug)]
pub struct SendReceipt(Option<Registration>);

#[derive(Debug)]
struct Registration {
    conn: ConnectionRef,
    key: Key,
    result: oneshot::Receiver<io::Result<()>>,
}

// Writes on a stream have disjoint nonempty ranges. The end offset stays fixed as the first
// unconfirmed byte advances, so it also serves as the ordered lookup key for intersecting frames.
type Key = (StreamId, u64);

struct Pending {
    next: u64,
    result: oneshot::Sender<io::Result<()>>,
}

#[derive(Default)]
pub(crate) struct Receipts {
    pending: BTreeMap<Key, Pending>,
    // Metadata for the driver's single outstanding transmit, retained across WouldBlock.
    batch: Vec<(Key, Range<u64>)>,
}

impl SendReceipt {
    pub(crate) fn register(
        state: &mut State,
        conn: &ConnectionRef,
        stream: StreamId,
        end: u64,
        len: usize,
    ) -> Self {
        if len == 0 {
            return Self(None);
        }
        let (tx, result) = oneshot::channel();
        let key = (stream, end);
        let old = state
            .receipts
            .get_or_insert_with(Default::default)
            .pending
            .insert(
                key,
                Pending {
                    next: end - len as u64,
                    result: tx,
                },
            );
        debug_assert!(old.is_none());
        Self(Some(Registration {
            conn: conn.clone(),
            key,
            result,
        }))
    }
}

impl Future for SendReceipt {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(registration) = &mut self.0 else {
            return Poll::Ready(Ok(()));
        };
        let result = ready!(Pin::new(&mut registration.result).poll(cx));
        self.0 = None;
        Poll::Ready(result.unwrap_or_else(|_| Err(io::ErrorKind::ConnectionAborted.into())))
    }
}

impl Drop for SendReceipt {
    fn drop(&mut self) {
        if let Some(r) = &mut self.0 {
            r.result.close();
            let Some(mut state) = r.conn.state.lock_if_unpoisoned("SendReceipt::drop") else {
                return;
            };
            if let Some(book) = &mut state.receipts {
                // After rejected 0-RTT the same stream ID and offset can belong to a new write.
                if book
                    .pending
                    .get(&r.key)
                    .is_some_and(|p| p.result.is_closed())
                {
                    book.pending.remove(&r.key);
                }
                if book.is_empty() {
                    state.receipts = None;
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

    pub(crate) fn needs_status(&self) -> bool {
        !self.batch.is_empty()
    }

    pub(crate) fn submitted(&mut self, confirmed: bool) {
        for (key, range) in self.batch.drain(..) {
            let Some(pending) = self.pending.get_mut(&key) else {
                continue;
            };
            if range.end <= pending.next {
                continue;
            }
            let result = if confirmed {
                // Fresh STREAM data is packetized in offset order. Retransmits may revisit a
                // prefix, but must never fill a gap merely by carrying a later byte.
                if range.start > pending.next {
                    continue;
                }
                pending.next = range.end;
                if pending.next < key.1 {
                    continue;
                }
                Ok(())
            } else {
                Err(io::Error::other("UDP socket did not confirm submission"))
            };
            let _ = self.pending.remove(&key).unwrap().result.send(result);
        }
    }

    pub(crate) fn fail_stream(&mut self, stream: StreamId, error: crate::WriteError) {
        while let Some((&key, _)) = self.pending.range((stream, 0)..=(stream, u64::MAX)).next() {
            let _ = self
                .pending
                .remove(&key)
                .unwrap()
                .result
                .send(Err(error.clone().into()));
        }
        self.batch.retain(|(key, _)| key.0 != stream);
    }

    pub(crate) fn fail_all(self, error: crate::WriteError) {
        for pending in self.pending.into_values() {
            let _ = pending.result.send(Err(error.clone().into()));
        }
    }
}
