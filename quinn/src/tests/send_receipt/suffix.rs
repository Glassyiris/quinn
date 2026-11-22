use super::{Bytes, DEADLINE, Mode, STREAM_WINDOW, connect, io, poll_once, timeout};

#[tokio::test]
async fn an_unconfirmed_opener_from_the_same_scatter_does_not_poison_its_suffix() {
    timeout(DEADLINE, async {
        for whole in [true, false] {
            let (client, server, gate, _) = connect().await;
            let mut send = client.open_uni().await.unwrap();
            send.write_all(b"@").await.unwrap();
            let mut recv = server.accept_uni().await.unwrap();
            assert_eq!(recv.id(), send.id());
            let mut lead = [0];
            recv.read_exact(&mut lead).await.unwrap();
            assert_eq!(&lead, b"@");

            let opener = Bytes::from(vec![b'H'; 1_500]);
            let body = Bytes::from(vec![b'T'; 2_000]);
            let mut chunks = [opener.slice(..23), opener.slice(23..), body.clone()];
            let frames = gate.block_next(&client);
            let (w, mut receipt) = if whole {
                send.write_chunks_with_receipt(&mut chunks).await
            } else {
                send.write_chunks_with_receipt_after(&mut chunks, opener.len())
                    .await
            }
            .unwrap();
            assert_eq!(w.chunks, 2);
            assert!(w.bytes > opener.len() && w.bytes <= STREAM_WINDOW as usize);
            assert!(chunks[0].is_empty() && chunks[1].is_empty());
            assert_eq!(chunks[2], body.slice(w.bytes - opener.len()..));
            send.finish().unwrap();
            gate.wait_for_stream_packet(&client, frames).await;
            assert!(poll_once(&mut receipt).is_pending());

            gate.set_mode(Mode::ForwardUnconfirmed);
            let first = recv.read_chunk(usize::MAX, true).await.unwrap().unwrap();
            assert_eq!(first.offset, 1);
            let n = first.bytes.len();
            assert!(n > 0 && n < opener.len());
            assert_eq!(first.bytes, opener.slice(..n));
            if whole {
                assert_eq!(
                    (&mut receipt).await.unwrap_err().kind(),
                    io::ErrorKind::Other
                );
            } else {
                assert!(poll_once(&mut receipt).is_pending());
            }
            gate.set_mode(Mode::Open);
            if !whole {
                receipt.await.unwrap();
            }
            let rest = recv.read_to_end(w.bytes - n).await.unwrap();
            assert_eq!(&rest[..opener.len() - n], &opener[n..]);
            assert_eq!(&rest[opener.len() - n..], &body[..w.bytes - opener.len()]);
        }
    })
    .await
    .expect("timed out checking the untracked prefix");
}

#[tokio::test]
async fn empty_suffix_preserves_short_write_and_blocked_retry() {
    timeout(DEADLINE, async {
        for skip in [2, usize::MAX] {
            let (client, server, gate, _) = connect().await;
            client.set_send_window(2);
            let mut send = client.open_uni().await.unwrap();
            let frames = gate.block_next(&client);
            let mut empty = [Bytes::new()];
            let (zero, receipt) = send
                .write_chunks_with_receipt_after(&mut empty, skip)
                .await
                .unwrap();
            assert_eq!((zero.bytes, zero.chunks), (0, 1));
            assert!(matches!(poll_once(receipt), std::task::Poll::Ready(Ok(()))));
            let mut chunks = [Bytes::new(), Bytes::from_static(b"abc")];
            let (written, receipt) = send
                .write_chunks_with_receipt_after(&mut chunks, skip)
                .await
                .unwrap();
            assert_eq!((written.bytes, written.chunks), (2, 1));
            assert_eq!(&chunks[1][..], b"c");
            assert!(matches!(poll_once(receipt), std::task::Poll::Ready(Ok(()))));
            assert!(poll_once(send.write_chunks_with_receipt_after(&mut chunks, 0)).is_pending());
            assert_eq!(&chunks[1][..], b"c");
            gate.wait_for_stream_packet(&client, frames).await;
            send.finish().unwrap();
            gate.set_mode(Mode::Open);
            let mut recv = server.accept_uni().await.unwrap();
            assert_eq!(recv.id(), send.id());
            assert_eq!(recv.read_to_end(2).await.unwrap(), b"ab");
        }
    })
    .await
    .expect("timed out checking the empty suffix");
}

#[tokio::test]
async fn suffix_includes_the_last_byte_of_a_short_write() {
    for skip_first_packet in [false, true] {
        check_short_write_boundary(skip_first_packet).await;
    }
}

async fn check_short_write_boundary(skip_first_packet: bool) {
    timeout(DEADLINE, async {
        let (client, server, gate, ack_gate) = connect().await;
        let mut warm = client.open_uni().await.unwrap();
        warm.write_all(b"!").await.unwrap();
        warm.finish().unwrap();
        assert_eq!(
            server
                .accept_uni()
                .await
                .unwrap()
                .read_to_end(1)
                .await
                .unwrap(),
            b"!"
        );
        assert_eq!(warm.stopped().await.unwrap(), None);

        // Measure the payload in one data datagram instead of assuming QUIC header sizes.
        let calibration = [b'C'; 2_000];
        let mut probe = client.open_uni().await.unwrap();
        let frames = gate.block_next(&client);
        probe.write_all(&calibration).await.unwrap();
        probe.finish().unwrap();
        gate.wait_for_stream_packet(&client, frames).await;
        gate.set_mode(Mode::One);
        let mut first = server.accept_uni().await.unwrap();
        assert_eq!(first.id(), probe.id());
        let chunk = first.read_chunk(usize::MAX, true).await.unwrap().unwrap();
        let boundary = chunk.bytes.len();
        assert_eq!(chunk.offset, 0);
        assert_eq!(&chunk.bytes[..], &calibration[..boundary]);
        gate.set_mode(Mode::Open);
        assert_eq!(
            first
                .read_to_end(calibration.len() - boundary)
                .await
                .unwrap(),
            calibration[boundary..]
        );

        assert_eq!(probe.stopped().await.unwrap(), None);
        ack_gate.set_mode(Mode::Blocked);
        client.set_send_window((boundary + 1) as u64);
        let mut send = client.open_uni().await.unwrap();
        let frames = gate.block_next(&client);
        let mut data = vec![b'A'; boundary];
        data.extend_from_slice(b"Z!");
        let data = Bytes::from(data);
        let mut chunks = [data.slice(..1), data.slice(1..)];
        let skip = if skip_first_packet { boundary } else { 1 };
        let (w, mut receipt) = send
            .write_chunks_with_receipt_after(&mut chunks, skip)
            .await
            .unwrap();
        assert_eq!((w.bytes, w.chunks), (boundary + 1, 1));
        assert_eq!(&chunks[1][..], b"!");
        send.finish().unwrap();
        gate.wait_for_stream_packet(&client, frames).await;
        let first_frames = client.stats().frame_tx.stream;
        gate.set_mode(if skip_first_packet {
            Mode::ForwardUnconfirmed
        } else {
            Mode::One
        });
        let mut recv = server.accept_uni().await.unwrap();
        assert_eq!(recv.id(), send.id());
        let chunk = recv.read_chunk(usize::MAX, true).await.unwrap().unwrap();
        assert_eq!(chunk.offset, 0);
        assert_eq!(chunk.bytes, data.slice(..boundary));
        assert!(poll_once(recv.read_chunk(1, true)).is_pending());
        assert!(
            poll_once(&mut receipt).is_pending(),
            "the final accepted byte is still held"
        );
        gate.wait_for_stream_packet(&client, first_frames).await;
        gate.set_mode(if skip_first_packet {
            Mode::One
        } else {
            Mode::ForwardUnconfirmed
        });
        let chunk = recv.read_chunk(usize::MAX, true).await.unwrap().unwrap();
        assert_eq!(chunk.offset, boundary as u64);
        assert_eq!(&chunk.bytes[..], b"Z");
        if skip_first_packet {
            receipt.await.unwrap();
        } else {
            assert_eq!(receipt.await.unwrap_err().kind(), io::ErrorKind::Other);
        }
        gate.set_mode(Mode::Open);
        ack_gate.set_mode(Mode::Open);
        assert!(recv.read_to_end(0).await.unwrap().is_empty());
    })
    .await
    .expect("timed out checking the final suffix byte");
}
