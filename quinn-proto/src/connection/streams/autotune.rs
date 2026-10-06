use crate::{Duration, Instant, VarInt};

/// Receive window auto-tuning driven by application reads
///
/// This is the receive-buffer auto-tuning of Chromium and quic-go: once more than half of the
/// window was read in an epoch and reading `fraction` of the window took less than
/// `4 * fraction * rtt`, the window cannot cover four times the delivery rate times the RTT and
/// doubles. Growth follows bytes read rather than bytes received, so a reader that stalls does
/// not inflate the window, and the window never shrinks.
#[derive(Debug, Default)]
pub(super) struct ReceiveAutotune {
    /// Upper bound for the window, if auto-tuning is enabled
    max: Option<u64>,
    /// Start time and bytes consumed at the start of the current epoch
    ///
    /// The first epoch starts with the first received data. Later epochs restart only when
    /// evaluated, so time spent not reading counts against growth.
    epoch: Option<(Instant, u64)>,
}

impl ReceiveAutotune {
    pub(super) fn set_max(&mut self, max: Option<VarInt>) {
        self.max = max.map(u64::from);
    }

    /// Returns the doubled window if `window` should grow
    ///
    /// `consumed` is the number of bytes the application has read so far, and `receiving`
    /// whether any stream data has arrived yet.
    pub(super) fn next_window(
        &mut self,
        now: Instant,
        rtt: Duration,
        window: u64,
        consumed: u64,
        receiving: bool,
    ) -> Option<VarInt> {
        let max = self.max.filter(|&max| max > window)?;
        if self.epoch.is_none() {
            if !receiving {
                return None;
            }
            self.epoch = Some((now, consumed));
        }
        let (start, offset) = self.epoch?;
        let read = consumed - offset;
        if read <= window / 2 || rtt.is_zero() {
            return None;
        }
        let fraction = read as f64 / window as f64;
        self.epoch = Some((now, consumed));
        if now.saturating_duration_since(start) >= rtt.mul_f64(4.0 * fraction) {
            return None;
        }
        VarInt::try_from(window.saturating_mul(2).min(max)).ok()
    }
}
