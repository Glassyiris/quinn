use std::io;

use tokio::time::timeout;

use super::{
    DEADLINE, GatedSocket, STREAM_WINDOW, establish, gen_data, localhost_socket,
    loopback_endpoints, poll_once,
};

#[tokio::test]
async fn buffered_stream_bytes_recover_after_socket_downgrade() {
    timeout(DEADLINE, async {
        let old = GatedSocket::with_segment_limit(2);
        let (endpoint, peer_endpoint) = loopback_endpoints(old.clone(), localhost_socket());
        let (client, peer) = establish(&endpoint, &peer_endpoint).await;
        let payload = gen_data(STREAM_WINDOW as usize, 2);
        let mut send = client.open_uni().await.unwrap();
        let frames = old.block_next(&client);
        let (n, mut receipt) = send.write_with_receipt(&payload).await.unwrap();
        assert_eq!(n, payload.len());
        send.finish().unwrap();
        old.wait_for_stream_packet(&client, frames).await;
        assert_eq!(old.gate.lock().unwrap().held_segments, 2);
        assert!(poll_once(&mut receipt).is_pending());
        assert!(poll_once(peer.accept_uni()).is_pending());

        // Keep the original socket gated for the remainder of the test.
        let single = GatedSocket::new();
        endpoint.rebind_abstract(single.clone()).unwrap();
        let result = receipt.await;
        assert!(!single.gate.lock().unwrap().invalid_transmit);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);

        let mut recv = peer.accept_uni().await.unwrap();
        assert_eq!(recv.id(), send.id());
        assert_eq!(recv.read_to_end(n).await.unwrap(), payload);
        assert_eq!(send.stopped().await.unwrap(), None);
        assert!(!single.gate.lock().unwrap().invalid_transmit);
    })
    .await
    .expect("timed out checking recovery after a socket downgrade");
}

#[tokio::test]
async fn fresh_transmit_uses_limit_after_a_dropped_batch() {
    timeout(DEADLINE, async {
        let socket = GatedSocket::with_segment_limit(2);
        let (endpoint, server) = loopback_endpoints(socket.clone(), localhost_socket());
        let (client, peer) = establish(&endpoint, &server).await;
        let mut first = client.open_uni().await.unwrap();
        first.set_priority(1).unwrap();
        let mut second = client.open_uni().await.unwrap();
        let a = gen_data(STREAM_WINDOW as usize, 3);
        let b = gen_data(STREAM_WINDOW as usize, 4);
        socket.set_mode(super::Mode::DowngradeNextBatch);
        let (n, dropped) = first.write_with_receipt(&a).await.unwrap();
        let (m, fresh) = second.write_with_receipt(&b).await.unwrap();
        assert_eq!((n, m), (a.len(), b.len()));
        first.finish().unwrap();
        second.finish().unwrap();

        assert_eq!(dropped.await.unwrap_err().kind(), io::ErrorKind::Other);
        fresh.await.unwrap();
        assert!(!socket.gate.lock().unwrap().invalid_transmit);
        for _ in 0..2 {
            let mut stream = peer.accept_uni().await.unwrap();
            let expected = if stream.id() == first.id() {
                &a
            } else {
                assert_eq!(stream.id(), second.id());
                &b
            };
            assert_eq!(
                stream.read_to_end(expected.len()).await.unwrap().as_slice(),
                expected.as_slice()
            );
        }
    })
    .await
    .expect("both streams must recover at the new segment limit");
}
