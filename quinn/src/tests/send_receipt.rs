use std::{
    future::Future,
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::{Pin, pin},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use bytes::Bytes;
use tokio::{io::AsyncWrite, sync::Notify, time::timeout};

use super::{EndpointFactory, gen_data, localhost_socket, new_count_waker};
use crate::{
    ApplicationClose, Connection, ConnectionError, Endpoint, SendReceipt, TransportConfig,
    WriteError,
    runtime::{AsyncUdpSocket, UdpPoller},
};

mod rebind;
mod suffix;

const DEADLINE: Duration = Duration::from_secs(5);
const STREAM_WINDOW: u32 = 3_000;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Mode {
    #[default]
    Open,
    BlockNext,
    Blocked,
    One,
    ForwardUnconfirmed,
    DropOne,
    DowngradeNextBatch,
    Fail,
}

#[derive(Debug, Default)]
struct Gate {
    mode: Mode,
    // The fixture has one connection driver per socket.
    waiter: Option<Waker>,
    held_segments: usize,
    invalid_transmit: bool,
}

#[derive(Debug)]
struct GatedSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    segment_limit: AtomicUsize,
    gate: Mutex<Gate>,
    blocked: Notify,
}

impl GatedSocket {
    fn new() -> Arc<Self> {
        Self::with_segment_limit(1)
    }

    fn with_segment_limit(segment_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: localhost_socket(),
            segment_limit: AtomicUsize::new(segment_limit),
            gate: Mutex::default(),
            blocked: Notify::new(),
        })
    }

    fn set_mode(&self, mode: Mode) {
        let waiter = {
            let mut gate = self.gate.lock().unwrap();
            gate.mode = mode;
            gate.waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    fn block_next(&self, connection: &Connection) -> u64 {
        let frames = connection.stats().frame_tx.stream;
        self.set_mode(Mode::BlockNext);
        frames
    }

    async fn wait_for_stream_packet(&self, connection: &Connection, frames_before: u64) {
        loop {
            let waiting = self.gate.lock().unwrap().waiter.is_some();
            if !waiting {
                self.blocked.notified().await;
                continue;
            }
            // The frame counter advances during packet construction.
            if connection.stats().frame_tx.stream > frames_before {
                return;
            }
            // Let a control-only transmit through and stop at the next send attempt.
            self.set_mode(Mode::One);
        }
    }

    fn send(&self, transmit: &udp::Transmit<'_>, confirm: bool) -> io::Result<bool> {
        let mut gate = self.gate.lock().unwrap();
        let segments = transmit
            .segment_size
            .map_or(1, |stride| transmit.contents.len().div_ceil(stride));
        if segments > self.max_transmit_segments() {
            gate.invalid_transmit = true;
            return Err(io::ErrorKind::InvalidInput.into());
        }
        match gate.mode {
            Mode::BlockNext | Mode::Blocked => {
                gate.mode = Mode::Blocked;
                gate.held_segments = segments;
                return Err(io::ErrorKind::WouldBlock.into());
            }
            Mode::Fail => return Err(io::Error::other("injected UDP send failure")),
            Mode::ForwardUnconfirmed => {
                self.forward(transmit, false)?;
                gate.mode = Mode::BlockNext;
                return Ok(false);
            }
            Mode::DropOne => {
                gate.mode = Mode::BlockNext;
                return Ok(false);
            }
            Mode::DowngradeNextBatch if segments > 1 => {
                self.segment_limit.store(1, Ordering::Relaxed);
                gate.mode = Mode::Open;
                return Ok(false);
            }
            _ => {}
        }

        let submitted = self.forward(transmit, confirm)?;
        if gate.mode == Mode::One {
            gate.mode = Mode::BlockNext;
        }
        Ok(submitted)
    }

    fn forward(&self, transmit: &udp::Transmit<'_>, confirm: bool) -> io::Result<bool> {
        let send = |datagram: &udp::Transmit<'_>| {
            if confirm {
                self.inner.try_send_with_status(datagram)
            } else {
                self.inner.try_send(datagram).map(|()| false)
            }
        };
        let Some(stride) = transmit.segment_size else {
            return send(transmit);
        };

        // Submit each segment on the real socket so this fixture also works without native GSO.
        let mut confirmed = true;
        for (index, contents) in transmit.contents.chunks(stride).enumerate() {
            let datagram = udp::Transmit {
                contents,
                segment_size: None,
                ..transmit.clone()
            };
            match send(&datagram) {
                Ok(submitted) => confirmed &= submitted,
                // The prefix was already attempted. Retrying the whole batch could replay it.
                Err(err) if index > 0 && err.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(false);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(confirmed)
    }
}

impl AsyncUdpSocket for GatedSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(GatedPoller {
            inner: self.inner.clone().create_io_poller(),
            socket: self,
        })
    }

    fn try_send(&self, transmit: &udp::Transmit<'_>) -> io::Result<()> {
        self.send(transmit, false).map(|_| ())
    }

    fn try_send_with_status(&self, transmit: &udp::Transmit<'_>) -> io::Result<bool> {
        self.send(transmit, true)
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_recv(cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        self.segment_limit.load(Ordering::Relaxed)
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

#[derive(Debug)]
struct GatedPoller {
    inner: Pin<Box<dyn UdpPoller>>,
    socket: Arc<GatedSocket>,
}

impl UdpPoller for GatedPoller {
    fn poll_writable(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        {
            let mut gate = self.socket.gate.lock().unwrap();
            if gate.mode == Mode::Blocked {
                match &mut gate.waiter {
                    Some(waiter) => waiter.clone_from(cx.waker()),
                    None => {
                        gate.waiter = Some(cx.waker().clone());
                        self.socket.blocked.notify_one();
                    }
                }
                return Poll::Pending;
            }
        }
        self.inner.as_mut().poll_writable(cx)
    }
}

fn loopback_endpoints(
    client_socket: Arc<dyn AsyncUdpSocket>,
    server_socket: Arc<dyn AsyncUdpSocket>,
) -> (Endpoint, Endpoint) {
    let factory = EndpointFactory::new();
    let transport = || {
        let mut config = TransportConfig::default();
        config
            .stream_receive_window(STREAM_WINDOW.into())
            .initial_mtu(1_200)
            .mtu_discovery_config(None);
        config
    };
    let client = factory.endpoint_with_config(transport());
    let server = factory.endpoint_with_config(transport());
    client.rebind_abstract(client_socket).unwrap();
    server.rebind_abstract(server_socket).unwrap();
    (client, server)
}

async fn establish(client: &Endpoint, server: &Endpoint) -> (Connection, Connection) {
    tokio::try_join!(
        client
            .connect(server.local_addr().unwrap(), "localhost")
            .unwrap(),
        async { server.accept().await.unwrap().await }
    )
    .unwrap()
}

async fn connect() -> (Connection, Connection, Arc<GatedSocket>, Arc<GatedSocket>) {
    let gate = GatedSocket::new();
    let ack_gate = GatedSocket::new();
    let (client, server) = loopback_endpoints(gate.clone(), ack_gate.clone());
    let (client, server) = establish(&client, &server).await;
    (client, server, gate, ack_gate)
}

fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    pin!(future).poll(&mut Context::from_waker(Waker::noop()))
}

fn assert_write_error(error: io::Error, expected: WriteError) {
    assert_eq!(
        error.get_ref().and_then(|cause| cause.downcast_ref()),
        Some(&expected)
    );
}

struct ReceiptWake {
    client: Connection,
    unlocked: AtomicBool,
    notified: Notify,
}

impl ReceiptWake {
    fn new(client: &Connection) -> Arc<Self> {
        Arc::new(Self {
            client: client.clone(),
            unlocked: AtomicBool::new(true),
            notified: Notify::new(),
        })
    }

    fn register(self: &Arc<Self>, receipt: &mut SendReceipt) {
        let waker = Waker::from(self.clone());
        assert!(
            Pin::new(receipt)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }

    async fn wait(&self) {
        self.notified.notified().await;
        // These tests run on one thread, so contention here comes from the publishing call.
        assert!(self.unlocked.load(Ordering::Relaxed));
    }
}

impl Wake for ReceiptWake {
    fn wake(self: Arc<Self>) {
        self.unlocked
            .fetch_and(self.client.state_lock_available(), Ordering::Relaxed);
        self.notified.notify_one();
    }
}

#[tokio::test]
async fn ranges_and_legacy_flush() {
    timeout(DEADLINE, async {
        let (client, server, gate, ack_gate) = connect().await;
        let mut send = client.open_uni().await.unwrap();
        ack_gate.set_mode(Mode::Blocked);
        let frames = gate.block_next(&client);
        let (n, first) = send.write_with_receipt(b"first").await.unwrap();
        assert_eq!(n, 5);
        let mut first = pin!(first);
        gate.wait_for_stream_packet(&client, frames).await;
        assert!(poll_once(first.as_mut()).is_pending());

        let (n, empty) = send.write_with_receipt(b"").await.unwrap();
        assert_eq!(n, 0);
        assert!(matches!(poll_once(empty), Poll::Ready(Ok(()))));
        let (n, next) = send.write_with_receipt(b"next").await.unwrap();
        assert_eq!(n, 4);
        let mut next = pin!(next);
        let mut other_send = client.open_uni().await.unwrap();
        let (n, other) = other_send.write_with_receipt(b"other").await.unwrap();
        assert_eq!(n, 5);
        let mut other = pin!(other);
        {
            let mut cx = Context::from_waker(Waker::noop());
            assert!(matches!(
                AsyncWrite::poll_write(Pin::new(&mut other_send), &mut cx, b" legacy"),
                Poll::Ready(Ok(7))
            ));
            assert!(matches!(
                AsyncWrite::poll_flush(Pin::new(&mut other_send), &mut cx),
                Poll::Ready(Ok(()))
            ));
        }

        gate.set_mode(Mode::One);
        first.await.unwrap();
        let mut recv = server.accept_uni().await.unwrap();
        assert_eq!(recv.id(), send.id());
        let mut first_bytes = [0; 5];
        recv.read_exact(&mut first_bytes).await.unwrap();
        assert_eq!(&first_bytes, b"first");
        assert!(poll_once(next.as_mut()).is_pending());
        assert!(poll_once(other.as_mut()).is_pending());
        assert!(poll_once(server.accept_uni()).is_pending());

        send.finish().unwrap();
        other_send.finish().unwrap();
        ack_gate.set_mode(Mode::Open);
        gate.set_mode(Mode::Open);
        next.await.unwrap();
        other.await.unwrap();
        assert_eq!(recv.read_to_end(4).await.unwrap().as_slice(), b"next");
        let mut recv = server.accept_uni().await.unwrap();
        assert_eq!(recv.id(), other_send.id());
        assert_eq!(
            recv.read_to_end(12).await.unwrap().as_slice(),
            b"other legacy"
        );
    })
    .await
    .expect("timed out checking receipt ranges");
}

#[tokio::test]
async fn partial_scalar_matches_ordinary_write() {
    timeout(DEADLINE, async {
        let (client, server, gate, _) = connect().await;
        let payload = gen_data(4_096, 1);
        let mut ordinary = client.open_uni().await.unwrap();
        let empty_count = ordinary.write(b"").await.unwrap();
        let accepted = ordinary.write(&payload).await.unwrap();
        assert_eq!(accepted, STREAM_WINDOW as usize);
        ordinary.finish().unwrap();
        let mut recv = server.accept_uni().await.unwrap();
        assert_eq!(recv.id(), ordinary.id());
        assert_eq!(
            recv.read_to_end(accepted).await.unwrap(),
            payload[..accepted]
        );

        let mut send = client.open_uni().await.unwrap();
        let frames = gate.block_next(&client);
        let (n, empty) = send.write_with_receipt(b"").await.unwrap();
        assert_eq!(n, empty_count);
        assert_eq!(n, 0);
        assert!(matches!(poll_once(empty), Poll::Ready(Ok(()))));
        let (n, mut receipt) = send.write_with_receipt(&payload).await.unwrap();
        assert_eq!(n, accepted);
        gate.wait_for_stream_packet(&client, frames).await;
        assert!(poll_once(&mut receipt).is_pending());

        gate.set_mode(Mode::One);
        let mut recv = server.accept_uni().await.unwrap();
        assert_eq!(recv.id(), send.id());
        let mut first = [0];
        recv.read_exact(&mut first).await.unwrap();
        assert_eq!(first[0], payload[0]);
        assert!(poll_once(&mut receipt).is_pending());

        send.finish().unwrap();
        gate.set_mode(Mode::Open);
        receipt.await.unwrap();
        assert_eq!(
            recv.read_to_end(accepted - 1).await.unwrap(),
            payload[1..accepted]
        );
    })
    .await
    .expect("timed out checking a partial scalar write");
}

#[tokio::test]
async fn partial_chunks_across_datagrams() {
    timeout(DEADLINE, async {
        let (client, server, gate, _) = connect().await;
        let mut send = client.open_uni().await.unwrap();
        let payload = Bytes::from((0..4_096).map(|i| (i % 251) as u8).collect::<Vec<_>>());
        let mut chunks = [Bytes::from_static(b"!"), payload.clone()];
        let mut ordinary_chunks = chunks.clone();
        let frames = gate.block_next(&client);
        let mut empty = [Bytes::new(), Bytes::new()];
        let (empty_written, empty_receipt) =
            send.write_chunks_with_receipt(&mut empty).await.unwrap();
        assert_eq!((empty_written.bytes, empty_written.chunks), (0, 2));
        assert!(matches!(poll_once(empty_receipt), Poll::Ready(Ok(()))));
        let (written, receipt) = send.write_chunks_with_receipt(&mut chunks).await.unwrap();
        assert_eq!(written.bytes, STREAM_WINDOW as usize);
        assert_eq!(written.chunks, 1);
        assert!(chunks[0].is_empty());
        let accepted = written.bytes - 1;
        assert_eq!(chunks[1], payload.slice(accepted..));
        let mut receipt = pin!(receipt);
        gate.wait_for_stream_packet(&client, frames).await;
        assert!(poll_once(receipt.as_mut()).is_pending());

        gate.set_mode(Mode::One);
        let mut recv = server.accept_uni().await.unwrap();
        let mut first = [0];
        recv.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, b"!");
        // The one permitted datagram cannot carry the full accepted prefix.
        assert!(poll_once(receipt.as_mut()).is_pending());

        send.finish().unwrap();
        gate.set_mode(Mode::Open);
        receipt.await.unwrap();
        assert_eq!(
            recv.read_to_end(accepted).await.unwrap().as_slice(),
            &payload[..accepted]
        );

        let mut ordinary = client.open_uni().await.unwrap();
        assert_eq!(
            ordinary.write_chunks(&mut empty).await.unwrap(),
            empty_written
        );
        assert_eq!(
            ordinary.write_chunks(&mut ordinary_chunks).await.unwrap(),
            written
        );
        assert_eq!(ordinary_chunks, chunks);
        ordinary.finish().unwrap();
        let mut recv = server.accept_uni().await.unwrap();
        assert_eq!(recv.id(), ordinary.id());
        let ordinary_bytes = recv.read_to_end(written.bytes).await.unwrap();
        assert_eq!(ordinary_bytes[0], b'!');
        assert_eq!(&ordinary_bytes[1..], &payload[..accepted]);
    })
    .await
    .expect("timed out checking a partial chunk write");
}

#[derive(Debug)]
struct LegacySocket {
    gate: Arc<GatedSocket>,
    submitted: Mutex<Vec<Bytes>>,
}

impl AsyncUdpSocket for LegacySocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        self.gate.clone().create_io_poller()
    }

    fn try_send(&self, transmit: &udp::Transmit<'_>) -> io::Result<()> {
        self.gate.try_send(transmit)?;
        self.submitted
            .lock()
            .unwrap()
            .push(Bytes::copy_from_slice(transmit.contents));
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.gate.poll_recv(cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.gate.local_addr()
    }

    fn max_receive_segments(&self) -> usize {
        self.gate.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.gate.may_fragment()
    }
}

#[tokio::test]
async fn unconfirmed_send_fails_receipt() {
    timeout(DEADLINE, async {
        let gate = GatedSocket::new();
        let legacy = Arc::new(LegacySocket {
            gate: gate.clone(),
            submitted: Mutex::default(),
        });
        let ack_gate = GatedSocket::new();
        let (client, server) = loopback_endpoints(legacy.clone(), ack_gate.clone());
        let (client, server) = establish(&client, &server).await;
        let mut send = client.open_uni().await.unwrap();
        ack_gate.set_mode(Mode::Blocked);
        let frames = gate.block_next(&client);
        let (n, receipt) = send.write_with_receipt(b"unconfirmed").await.unwrap();
        assert_eq!(n, 11);
        let mut receipt = pin!(receipt);
        gate.wait_for_stream_packet(&client, frames).await;
        assert!(poll_once(receipt.as_mut()).is_pending());
        send.finish().unwrap();

        let before = legacy.submitted.lock().unwrap().len();
        gate.set_mode(Mode::One);
        assert_eq!(receipt.await.unwrap_err().kind(), io::ErrorKind::Other);
        let mut recv = server.accept_uni().await.unwrap();
        let mut payload = [0; 11];
        recv.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"unconfirmed");
        assert_eq!(legacy.submitted.lock().unwrap().len(), before + 1);
        ack_gate.set_mode(Mode::Open);
        gate.set_mode(Mode::Open);
        assert_eq!(recv.read(&mut [0]).await.unwrap(), None);
        assert_eq!(send.stopped().await.unwrap(), None);
        let submitted = legacy.submitted.lock().unwrap();
        let target = &submitted[before];
        assert_eq!(
            submitted
                .iter()
                .filter(|datagram| *datagram == target)
                .count(),
            1
        );
    })
    .await
    .expect("timed out checking an unconfirmed send");
}

#[tokio::test]
async fn tolerated_send_failure_preserves_connection() {
    timeout(DEADLINE, async {
        let (client, server, gate, _) = connect().await;
        let mut send = client.open_uni().await.unwrap();
        let frames = gate.block_next(&client);
        let (n, mut receipt) = send.write_with_receipt(b"recover me").await.unwrap();
        assert_eq!(n, 10);
        gate.wait_for_stream_packet(&client, frames).await;
        assert!(poll_once(&mut receipt).is_pending());
        send.finish().unwrap();

        gate.set_mode(Mode::DropOne);
        assert_eq!(receipt.await.unwrap_err().kind(), io::ErrorKind::Other);
        assert!(client.close_reason().is_none());
        assert!(poll_once(server.accept_uni()).is_pending());

        gate.set_mode(Mode::Open);
        let mut recv = server.accept_uni().await.unwrap();
        assert_eq!(recv.read_to_end(n).await.unwrap(), b"recover me");
        assert_eq!(send.stopped().await.unwrap(), None);
    })
    .await
    .expect("timed out checking tolerated send failure");
}

#[tokio::test]
async fn dropping_receipt_leaves_bytes_and_other_receipts() {
    timeout(DEADLINE, async {
        let (client, server, gate, _) = connect().await;
        let mut send = client.open_uni().await.unwrap();
        let frames = gate.block_next(&client);
        let (n, abandoned) = send.write_with_receipt(b"abandoned").await.unwrap();
        assert_eq!(n, 9);
        let (n, mut kept) = send.write_with_receipt(b" kept").await.unwrap();
        assert_eq!(n, 5);
        gate.wait_for_stream_packet(&client, frames).await;
        let (waker, wakes) = new_count_waker();
        {
            let mut abandoned = pin!(abandoned);
            assert!(
                abandoned
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        let wakes_after_drop = wakes.wakes();
        let wake = ReceiptWake::new(&client);
        wake.register(&mut kept);
        send.finish().unwrap();

        gate.set_mode(Mode::Open);
        wake.wait().await;
        assert!(matches!(poll_once(kept), Poll::Ready(Ok(()))));
        let mut recv = server.accept_uni().await.unwrap();
        assert_eq!(
            recv.read_to_end(14).await.unwrap().as_slice(),
            b"abandoned kept"
        );
        assert_eq!(wakes.wakes(), wakes_after_drop);
    })
    .await
    .expect("timed out checking receipt cancellation");
}

#[tokio::test]
async fn fatal_send_releases_endpoint_with_unpolled_receipt() {
    timeout(DEADLINE, async {
        let gate = GatedSocket::new();
        let (endpoint, server) = loopback_endpoints(gate.clone(), localhost_socket());
        let (client, _peer) = establish(&endpoint, &server).await;
        let mut send = client.open_uni().await.unwrap();
        let frames = gate.block_next(&client);
        let (n, receipt) = send.write_with_receipt(b"queued").await.unwrap();
        assert_eq!(n, 6);
        gate.wait_for_stream_packet(&client, frames).await;
        let mut idle = pin!(endpoint.wait_idle());
        assert!(poll_once(idle.as_mut()).is_pending());

        gate.set_mode(Mode::Fail);
        assert_eq!(client.closed().await, ConnectionError::LocallyClosed);
        let closed = WriteError::ConnectionLost(ConnectionError::LocallyClosed);
        assert_eq!(send.write_with_receipt(b"late").await.unwrap_err(), closed);
        drop(send);
        drop(client);

        idle.await;
        assert_write_error(receipt.await.unwrap_err(), closed);
    })
    .await
    .expect("timed out checking fatal send cleanup");
}

#[tokio::test]
async fn unpolled_receipt_does_not_keep_connection_open() {
    timeout(DEADLINE, async {
        let gate = GatedSocket::new();
        let (endpoint, server) = loopback_endpoints(gate.clone(), localhost_socket());
        let (client, peer) = establish(&endpoint, &server).await;
        let mut send = client.open_uni().await.unwrap();
        let (n, submitted) = send.write_with_receipt(b"sent").await.unwrap();
        assert_eq!(n, 4);
        let mut recv = peer.accept_uni().await.unwrap();
        let mut seen = [0; 4];
        recv.read_exact(&mut seen).await.unwrap();
        assert_eq!(&seen, b"sent");

        let frames = gate.block_next(&client);
        let (n, receipt) = send.write_with_receipt(b"queued").await.unwrap();
        assert_eq!(n, 6);
        gate.wait_for_stream_packet(&client, frames).await;
        let mut idle = pin!(endpoint.wait_idle());
        assert!(poll_once(idle.as_mut()).is_pending());

        drop(send);
        drop(client);
        gate.set_mode(Mode::Open);

        assert_eq!(
            peer.closed().await,
            ConnectionError::ApplicationClosed(ApplicationClose {
                error_code: 0u32.into(),
                reason: Bytes::new(),
            })
        );
        idle.await;
        assert!(matches!(poll_once(submitted), Poll::Ready(Ok(()))));
        assert_write_error(
            receipt.await.unwrap_err(),
            WriteError::ConnectionLost(ConnectionError::LocallyClosed),
        );
    })
    .await
    .expect("timed out waiting for implicit close with an unpolled receipt");
}

#[tokio::test]
async fn local_close_publishes_receipt_failure_after_unlocking() {
    timeout(DEADLINE, async {
        let (client, peer, gate, _) = connect().await;
        let mut send = client.open_uni().await.unwrap();
        let frames = gate.block_next(&client);
        let (n, mut receipt) = send.write_with_receipt(b"queued").await.unwrap();
        assert_eq!(n, 6);
        gate.wait_for_stream_packet(&client, frames).await;
        let wake = ReceiptWake::new(&client);
        wake.register(&mut receipt);

        client.close(23u32.into(), b"test close");
        wake.wait().await;
        let closed = WriteError::ConnectionLost(ConnectionError::LocallyClosed);
        assert_write_error(receipt.await.unwrap_err(), closed.clone());
        assert_eq!(client.close_reason(), Some(ConnectionError::LocallyClosed));
        assert_eq!(send.write_with_receipt(b"late").await.unwrap_err(), closed);

        gate.set_mode(Mode::Open);
        assert_eq!(
            peer.closed().await,
            ConnectionError::ApplicationClosed(ApplicationClose {
                error_code: 23u32.into(),
                reason: Bytes::from_static(b"test close"),
            })
        );
    })
    .await
    .expect("timed out checking local close");
}

#[tokio::test]
async fn peer_close_fails_pending_receipt_with_peer_reason() {
    timeout(DEADLINE, async {
        let (client, peer, gate, _) = connect().await;
        let mut send = client.open_uni().await.unwrap();
        let frames = gate.block_next(&client);
        let (n, mut receipt) = send.write_with_receipt(b"queued").await.unwrap();
        assert_eq!(n, 6);
        gate.wait_for_stream_packet(&client, frames).await;
        assert!(poll_once(&mut receipt).is_pending());

        peer.close(73u32.into(), b"peer close");
        let reason = ConnectionError::ApplicationClosed(ApplicationClose {
            error_code: 73u32.into(),
            reason: Bytes::from_static(b"peer close"),
        });
        assert_eq!(client.closed().await, reason);
        let closed = WriteError::ConnectionLost(reason);
        assert_write_error(receipt.await.unwrap_err(), closed.clone());
        assert_eq!(send.write_with_receipt(b"late").await.unwrap_err(), closed);
    })
    .await
    .expect("timed out checking peer close");
}

#[tokio::test]
async fn peer_stop_fails_pending_receipt() {
    timeout(DEADLINE, async {
        let (client, server, gate, _) = connect().await;
        let mut send = client.open_uni().await.unwrap();
        send.write_all(b"open").await.unwrap();
        let mut recv = server.accept_uni().await.unwrap();
        let mut seen = [0; 4];
        recv.read_exact(&mut seen).await.unwrap();
        assert_eq!(&seen, b"open");

        let frames = gate.block_next(&client);
        let (n, receipt) = send.write_with_receipt(b"queued").await.unwrap();
        assert_eq!(n, 6);
        let mut receipt = pin!(receipt);
        gate.wait_for_stream_packet(&client, frames).await;
        assert!(poll_once(receipt.as_mut()).is_pending());
        recv.stop(42u32.into()).unwrap();
        assert_eq!(send.stopped().await.unwrap(), Some(42u32.into()));
        let stopped = WriteError::Stopped(42u32.into());
        assert_write_error(receipt.await.unwrap_err(), stopped.clone());
        assert_eq!(send.write_with_receipt(b"late").await.unwrap_err(), stopped);
        assert!(client.close_reason().is_none());
    })
    .await
    .expect("timed out checking STOP_SENDING");
}

#[test]
fn receipts_cannot_outlive_their_driver() {
    let runtime = super::rt_basic();
    let (mut stream, receipt, wake, _peer) = runtime.block_on(async {
        timeout(DEADLINE, async {
            let (client, peer, gate, _) = connect().await;
            let mut stream = client.open_uni().await.unwrap();
            let frames = gate.block_next(&client);
            let (_, mut receipt) = stream.write_with_receipt(b"queued").await.unwrap();
            gate.wait_for_stream_packet(&client, frames).await;
            let wake = ReceiptWake::new(&client);
            wake.register(&mut receipt);
            (stream, receipt, wake, peer)
        })
        .await
        .unwrap()
    });
    drop(runtime);
    assert!(poll_once(wake.wait()).is_ready());
    let closed = WriteError::ConnectionLost(ConnectionError::LocallyClosed);
    let Poll::Ready(Err(error)) = poll_once(receipt) else {
        panic!("driver shutdown must settle the pending receipt");
    };
    assert_write_error(error, closed.clone());
    assert!(matches!(
        poll_once(stream.write_with_receipt(b"late")),
        Poll::Ready(Err(error)) if error == closed
    ));
}

#[tokio::test]
async fn reset_fails_only_the_reset_streams_receipt() {
    timeout(DEADLINE, async {
        let (client, peer, gate, _) = connect().await;
        let mut first = client.open_uni().await.unwrap();
        let frames = gate.block_next(&client);
        let (_, mut cancelled) = first.write_with_receipt(b"old").await.unwrap();
        gate.wait_for_stream_packet(&client, frames).await;
        assert!(poll_once(&mut cancelled).is_pending());
        let mut other = client.open_uni().await.unwrap();
        let (_, mut kept) = other.write_with_receipt(b"new").await.unwrap();
        first.reset(1u32.into()).unwrap();
        let Poll::Ready(Err(error)) = poll_once(cancelled) else {
            panic!("reset must settle the pending receipt");
        };
        assert_write_error(error, WriteError::ClosedStream);
        assert!(poll_once(&mut kept).is_pending());
        gate.set_mode(Mode::Open);
        kept.await.unwrap();
        other.finish().unwrap();
        loop {
            let mut recv = peer.accept_uni().await.unwrap();
            if recv.id() == other.id() {
                assert_eq!(recv.read_to_end(3).await.unwrap(), b"new");
                break;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rejected_early_write_cannot_settle_reused_stream_id() {
    timeout(DEADLINE, async {
        let factory = EndpointFactory::new();
        let client = factory.endpoint();
        let first_server = factory.endpoint();
        let (first, peer) = tokio::try_join!(
            client
                .connect(first_server.local_addr().unwrap(), "localhost")
                .unwrap(),
            async { first_server.accept().await.unwrap().await }
        )
        .unwrap();
        // A post-handshake message lets the client process the session ticket first.
        let mut signal = peer.open_uni().await.unwrap();
        signal.write_all(b"ticket").await.unwrap();
        signal.finish().unwrap();
        assert_eq!(
            first
                .accept_uni()
                .await
                .unwrap()
                .read_to_end(6)
                .await
                .unwrap(),
            b"ticket"
        );
        first.close(0u32.into(), b"done");
        client.wait_idle().await;

        // Same certificate and name, new server session storage: early data is rejected.
        let server = factory.endpoint();
        let gate = GatedSocket::new();
        client.rebind_abstract(gate.clone()).unwrap();
        let (conn, accepted) = client
            .connect(server.local_addr().unwrap(), "localhost")
            .unwrap()
            .into_0rtt()
            .expect("client must offer early data");
        let incoming = server.accept().await.unwrap();
        let mut old = conn.open_uni().await.unwrap();
        let frames = gate.block_next(&conn);
        let (old_prefix, mut rejected) = old.write_with_receipt(b"old").await.unwrap();
        let (old_suffix, stale) = old.write_with_receipt(b" stale").await.unwrap();
        assert_eq!((old_prefix, old_suffix), (3, 6));
        gate.wait_for_stream_packet(&conn, frames).await;
        assert!(poll_once(&mut rejected).is_pending());
        let peer = incoming.accept().unwrap();
        assert!(!accepted.await);
        let mut replacement = conn.open_uni().await.unwrap();
        assert_eq!(old.id(), replacement.id());
        let (new_prefix, mut fresh) = replacement.write_with_receipt(b"new").await.unwrap();
        let (new_suffix, mut kept) = replacement.write_with_receipt(b" fresh").await.unwrap();
        assert_eq!((new_prefix, new_suffix), (old_prefix, old_suffix));
        let Poll::Ready(Err(error)) = poll_once(&mut rejected) else {
            panic!("0-RTT rejection must settle the old receipt");
        };
        assert_write_error(error, WriteError::ZeroRttRejected);
        // The unpolled receipt has the same stream ID and end offset as `kept`.
        drop(stale);
        assert!(poll_once(&mut fresh).is_pending());
        assert!(poll_once(&mut kept).is_pending());
        // Park the replacement STREAM transmit before checking either fresh receipt.
        let before = conn.stats().frame_tx.stream;
        gate.set_mode(Mode::One);
        gate.wait_for_stream_packet(&conn, before).await;
        assert!(poll_once(&mut fresh).is_pending());
        assert!(poll_once(&mut kept).is_pending());
        replacement.finish().unwrap();
        gate.set_mode(Mode::Open);
        fresh.await.unwrap();
        kept.await.unwrap();
        assert_eq!(
            peer.await
                .unwrap()
                .accept_uni()
                .await
                .unwrap()
                .read_to_end(9)
                .await
                .unwrap(),
            b"new fresh"
        );
    })
    .await
    .unwrap();
}
