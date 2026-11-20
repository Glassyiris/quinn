use std::{
    future::Future,
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::{Pin, pin},
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use tokio::{io::AsyncWrite, sync::Notify, time::timeout};

use super::{EndpointFactory, localhost_socket, new_count_waker};
use crate::{
    Connection, ConnectionError, TransportConfig,
    runtime::{AsyncUdpSocket, UdpPoller},
};

const DEADLINE: Duration = Duration::from_secs(5);
const STREAM_WINDOW: u32 = 3_000;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Mode {
    #[default]
    Open,
    BlockNext,
    Blocked,
    One,
    Unconfirmed,
    Fail,
}

#[derive(Debug, Default)]
struct Gate {
    mode: Mode,
    // The fixture has one connection driver per socket.
    waiter: Option<Waker>,
}

#[derive(Debug)]
struct GatedSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    gate: Mutex<Gate>,
    blocked: Notify,
}

impl GatedSocket {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: localhost_socket(),
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
            // Let a control-only datagram through and stop at the next send attempt.
            self.set_mode(Mode::One);
        }
    }

    fn send(&self, transmit: &udp::Transmit<'_>, confirm: bool) -> io::Result<bool> {
        let mut gate = self.gate.lock().unwrap();
        match gate.mode {
            Mode::BlockNext | Mode::Blocked => {
                gate.mode = Mode::Blocked;
                return Err(io::ErrorKind::WouldBlock.into());
            }
            Mode::Fail => return Err(io::Error::other("injected UDP send failure")),
            _ => {}
        }

        let submitted = if confirm && gate.mode != Mode::Unconfirmed {
            self.inner.try_send_with_status(transmit)?
        } else {
            self.inner.try_send(transmit)?;
            false
        };
        if gate.mode == Mode::One {
            gate.mode = Mode::BlockNext;
        }
        Ok(submitted)
    }
}

// Keep the default one segment per transmit so the gate can release one datagram at a time.
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

async fn connect() -> (Connection, Connection, Arc<GatedSocket>, Arc<GatedSocket>) {
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
    let gate = GatedSocket::new();
    let ack_gate = GatedSocket::new();
    client.rebind_abstract(gate.clone()).unwrap();
    server.rebind_abstract(ack_gate.clone()).unwrap();
    let (client, server) = tokio::try_join!(
        client
            .connect(server.local_addr().unwrap(), "localhost")
            .unwrap(),
        async { server.accept().await.unwrap().await }
    )
    .unwrap();
    (client, server, gate, ack_gate)
}

fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    pin!(future).poll(&mut Context::from_waker(Waker::noop()))
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
async fn partial_chunks_across_datagrams() {
    timeout(DEADLINE, async {
        let (client, server, gate, _) = connect().await;
        let mut send = client.open_uni().await.unwrap();
        let payload = Bytes::from((0..4_096).map(|i| (i % 251) as u8).collect::<Vec<_>>());
        let mut chunks = [Bytes::from_static(b"!"), payload.clone()];
        let frames = gate.block_next(&client);
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
    })
    .await
    .expect("timed out checking a partial chunk write");
}

#[tokio::test]
async fn unconfirmed_send_fails_receipt() {
    timeout(DEADLINE, async {
        let (client, server, gate, ack_gate) = connect().await;
        let mut send = client.open_uni().await.unwrap();
        ack_gate.set_mode(Mode::Blocked);
        let frames = gate.block_next(&client);
        let (n, receipt) = send.write_with_receipt(b"unconfirmed").await.unwrap();
        assert_eq!(n, 11);
        let mut receipt = pin!(receipt);
        gate.wait_for_stream_packet(&client, frames).await;
        assert!(poll_once(receipt.as_mut()).is_pending());
        send.finish().unwrap();

        gate.set_mode(Mode::Unconfirmed);
        assert!(receipt.await.is_err());
        let mut recv = server.accept_uni().await.unwrap();
        assert_eq!(
            recv.read_to_end(11).await.unwrap().as_slice(),
            b"unconfirmed"
        );
        ack_gate.set_mode(Mode::Open);
        assert_eq!(send.stopped().await.unwrap(), None);
    })
    .await
    .expect("timed out checking an unconfirmed send");
}

#[tokio::test]
async fn dropping_receipt_leaves_bytes_and_other_receipts() {
    timeout(DEADLINE, async {
        let (client, server, gate, _) = connect().await;
        let mut send = client.open_uni().await.unwrap();
        let frames = gate.block_next(&client);
        let (n, abandoned) = send.write_with_receipt(b"abandoned").await.unwrap();
        assert_eq!(n, 9);
        let (n, kept) = send.write_with_receipt(b" kept").await.unwrap();
        assert_eq!(n, 5);
        let mut kept = pin!(kept);
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
        assert!(poll_once(kept.as_mut()).is_pending());
        send.finish().unwrap();

        gate.set_mode(Mode::Open);
        kept.await.unwrap();
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
async fn send_failure_and_close_fail_pending_receipts() {
    for close in [false, true] {
        timeout(DEADLINE, async {
            let (client, _server, gate, _) = connect().await;
            let mut send = client.open_uni().await.unwrap();
            let frames = gate.block_next(&client);
            let (n, receipt) = send.write_with_receipt(b"queued").await.unwrap();
            assert_eq!(n, 6);
            let mut receipt = pin!(receipt);
            gate.wait_for_stream_packet(&client, frames).await;
            assert!(poll_once(receipt.as_mut()).is_pending());

            if close {
                client.close(0u32.into(), b"test close");
            } else {
                gate.set_mode(Mode::Fail);
            }
            assert!(receipt.await.is_err());
            assert!(matches!(
                client.closed().await,
                ConnectionError::LocallyClosed
            ));
        })
        .await
        .expect("timed out checking receipt failure");
    }
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
        assert!(receipt.await.is_err());
    })
    .await
    .expect("timed out checking STOP_SENDING");
}

#[test]
fn receipts_cannot_outlive_their_driver() {
    let runtime = super::rt_basic();
    let (mut stream, receipt, _peer) = runtime.block_on(async {
        timeout(DEADLINE, async {
            let (client, peer, gate, _) = connect().await;
            let mut stream = client.open_uni().await.unwrap();
            let frames = gate.block_next(&client);
            let (_, mut receipt) = stream.write_with_receipt(b"queued").await.unwrap();
            gate.wait_for_stream_packet(&client, frames).await;
            assert!(poll_once(&mut receipt).is_pending());
            (stream, receipt, peer)
        })
        .await
        .unwrap()
    });
    drop(runtime);
    assert!(matches!(poll_once(receipt), Poll::Ready(Err(_))));
    assert!(matches!(
        poll_once(stream.write_with_receipt(b"late")),
        Poll::Ready(Err(crate::WriteError::ConnectionLost(_)))
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
        assert!(matches!(poll_once(cancelled), Poll::Ready(Err(_))));
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
        let (_, mut rejected) = old.write_with_receipt(b"old").await.unwrap();
        gate.wait_for_stream_packet(&conn, frames).await;
        assert!(poll_once(&mut rejected).is_pending());
        let peer = incoming.accept().unwrap();
        assert!(!accepted.await);
        let mut replacement = conn.open_uni().await.unwrap();
        assert_eq!(old.id(), replacement.id());
        let (_, mut fresh) = replacement.write_with_receipt(b"new").await.unwrap();
        assert!(matches!(poll_once(&mut rejected), Poll::Ready(Err(_))));
        assert!(poll_once(&mut fresh).is_pending());
        // Only the old buffered datagram is released; the new write still needs its own send.
        let before = conn.stats().frame_tx.stream;
        gate.set_mode(Mode::One);
        gate.wait_for_stream_packet(&conn, before).await;
        assert!(poll_once(&mut fresh).is_pending());
        replacement.finish().unwrap();
        gate.set_mode(Mode::Open);
        fresh.await.unwrap();
        assert_eq!(
            peer.await
                .unwrap()
                .accept_uni()
                .await
                .unwrap()
                .read_to_end(3)
                .await
                .unwrap(),
            b"new"
        );
    })
    .await
    .unwrap();
}
