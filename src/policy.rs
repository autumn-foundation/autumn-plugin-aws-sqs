//! Pure retry, delay, and batch rules.
//!
//! This is the verified core. `verus/policy.rs` holds the spec and proofs.
//! Keep both files in step.

/// SQS limit for `DelaySeconds`.
pub const MAX_DELAY_SECS: u64 = 900;
/// SQS limit for the visibility timeout (12 h).
pub const MAX_VISIBILITY_SECS: u64 = 43_200;
/// SQS limit for entries in one batch request.
pub const MAX_BATCH: usize = 10;
/// SQS limit for the total payload of one batch request (1 MiB).
pub const MAX_BATCH_BYTES: usize = 1_048_576;
/// Safety margin under the 12 h visibility budget of one receive.
pub const VISIBILITY_MARGIN_SECS: u64 = 5;

/// Decision after a handler fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Try again after a backoff.
    Retry,
    /// No attempts remain. Dead-letter the message.
    Exhausted,
}

/// Returns [`Decision::Retry`] only while `attempt < max_attempts`.
#[must_use]
pub const fn decide(attempt: u32, max_attempts: u32) -> Decision {
    if attempt < max_attempts {
        Decision::Retry
    } else {
        Decision::Exhausted
    }
}

/// Converts the SQS `ApproximateReceiveCount` to a 1-based attempt.
#[must_use]
pub const fn attempt_from_receive_count(count: Option<u32>) -> u32 {
    match count {
        Some(c) if c >= 1 => c,
        _ => 1,
    }
}

/// Backoff for `attempt`, in whole seconds, rounded up.
///
/// The step is `initial_ms * 2^(attempt - 1)`. The result is never above
/// `max_secs`. `max_secs` is clamped to [`MAX_VISIBILITY_SECS`].
#[must_use]
pub const fn backoff_secs(initial_ms: u64, attempt: u32, max_secs: u64) -> u64 {
    let max_secs = if max_secs < MAX_VISIBILITY_SECS {
        max_secs
    } else {
        MAX_VISIBILITY_SECS
    };
    let cap_ms = max_secs * 1000;
    let mut ms = if initial_ms < cap_ms {
        initial_ms
    } else {
        cap_ms
    };
    let mut i: u32 = 1;
    // Stop early at 0 or at the cap: later steps do not change the value.
    while i < attempt && ms != 0 && ms != cap_ms {
        ms = if ms <= cap_ms / 2 { ms * 2 } else { cap_ms };
        i += 1;
    }
    let q = ms / 1000;
    let r = ms % 1000;
    if r == 0 { q } else { q + 1 }
}

/// Largest visibility timeout that SQS accepts now.
///
/// SQS allows 12 h of visibility from the receive. `elapsed_secs` is the time
/// since the receive. The result is never above `requested`. A result of 0
/// means the budget is spent.
#[must_use]
pub const fn clamp_visibility(requested: u64, elapsed_secs: u64) -> u64 {
    let used = elapsed_secs.saturating_add(VISIBILITY_MARGIN_SECS);
    if used >= MAX_VISIBILITY_SECS {
        return 0;
    }
    let left = MAX_VISIBILITY_SECS - used;
    if requested < left { requested } else { left }
}

/// Delay to send now, and the part that stays for later hops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelaySplit {
    /// Seconds for `DelaySeconds` on this send.
    pub now_secs: u64,
    /// Seconds that remain after this send.
    pub later_secs: u64,
}

/// Splits a delay into the SQS limit and a remainder.
#[must_use]
pub const fn split_delay(remaining_secs: u64) -> DelaySplit {
    if remaining_secs <= MAX_DELAY_SECS {
        DelaySplit {
            now_secs: remaining_secs,
            later_secs: 0,
        }
    } else {
        DelaySplit {
            now_secs: MAX_DELAY_SECS,
            later_secs: remaining_secs - MAX_DELAY_SECS,
        }
    }
}

/// End (exclusive) of the batch that starts at `start`.
///
/// A batch has 1 to [`MAX_BATCH`] entries. Its total size is `max_bytes` or
/// less, unless it holds one entry that is larger. Returns `start` only when
/// `start >= sizes.len()`.
#[must_use]
pub const fn batch_end(sizes: &[usize], start: usize, max_bytes: usize) -> usize {
    if start >= sizes.len() {
        return start;
    }
    let mut end = start;
    let mut total: usize = 0;
    while end < sizes.len() && end - start < MAX_BATCH {
        let size = sizes[end];
        if size > max_bytes - total {
            if end == start {
                // One entry over the limit goes alone. SQS rejects it per entry.
                return start + 1;
            }
            break;
        }
        total += size;
        end += 1;
    }
    end
}

/// Interval between visibility extensions: half the timeout, 1 s or more.
#[must_use]
pub const fn heartbeat_interval_secs(visibility_secs: u64) -> u64 {
    let half = visibility_secs / 2;
    if half >= 1 { half } else { 1 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Closed form of `spec_backoff_ms` in `verus/policy.rs`:
    /// `min(initial * 2^(attempt - 1), cap)`, with saturation.
    fn spec_backoff_ms(initial_ms: u64, attempt: u32, cap_ms: u64) -> u64 {
        let factor = 2u64
            .checked_pow(attempt.saturating_sub(1))
            .unwrap_or(u64::MAX);
        initial_ms.saturating_mul(factor).min(cap_ms)
    }

    #[test]
    fn decide_retries_below_limit() {
        assert_eq!(decide(1, 5), Decision::Retry);
        assert_eq!(decide(4, 5), Decision::Retry);
    }

    #[test]
    fn decide_exhausts_at_limit() {
        assert_eq!(decide(5, 5), Decision::Exhausted);
        assert_eq!(decide(9, 5), Decision::Exhausted);
        assert_eq!(decide(1, 1), Decision::Exhausted);
    }

    #[test]
    fn receive_count_defaults_to_one() {
        assert_eq!(attempt_from_receive_count(None), 1);
        assert_eq!(attempt_from_receive_count(Some(0)), 1);
        assert_eq!(attempt_from_receive_count(Some(3)), 3);
    }

    #[test]
    fn backoff_doubles_and_rounds_up() {
        assert_eq!(backoff_secs(250, 1, 60), 1);
        assert_eq!(backoff_secs(1_000, 1, 60), 1);
        assert_eq!(backoff_secs(1_000, 2, 60), 2);
        assert_eq!(backoff_secs(1_000, 3, 60), 4);
        assert_eq!(backoff_secs(1_500, 2, 60), 3);
    }

    #[test]
    fn backoff_zero_initial_is_zero() {
        assert_eq!(backoff_secs(0, 7, 60), 0);
    }

    #[test]
    fn backoff_caps_at_max() {
        assert_eq!(backoff_secs(1_000, 40, 60), 60);
        assert_eq!(backoff_secs(u64::MAX, u32::MAX, 60), 60);
    }

    #[test]
    fn backoff_clamps_max_to_sqs_limit() {
        assert_eq!(backoff_secs(u64::MAX, 1, u64::MAX), MAX_VISIBILITY_SECS);
    }

    #[test]
    fn split_short_delay_needs_no_hop() {
        assert_eq!(
            split_delay(0),
            DelaySplit {
                now_secs: 0,
                later_secs: 0
            }
        );
        assert_eq!(
            split_delay(900),
            DelaySplit {
                now_secs: 900,
                later_secs: 0
            }
        );
    }

    #[test]
    fn split_long_delay_hops() {
        assert_eq!(
            split_delay(901),
            DelaySplit {
                now_secs: 900,
                later_secs: 1
            }
        );
        assert_eq!(
            split_delay(86_400),
            DelaySplit {
                now_secs: 900,
                later_secs: 85_500
            }
        );
    }

    #[test]
    fn batch_end_splits_by_count() {
        let sizes = [1usize; 11];
        assert_eq!(batch_end(&sizes, 0, MAX_BATCH_BYTES), 10);
        assert_eq!(batch_end(&sizes, 10, MAX_BATCH_BYTES), 11);
        assert_eq!(batch_end(&sizes, 11, MAX_BATCH_BYTES), 11);
        assert_eq!(batch_end(&[], 0, MAX_BATCH_BYTES), 0);
    }

    #[test]
    fn batch_end_splits_by_bytes() {
        // Four 300 KB entries: SQS rejects them in one request.
        let sizes = [300_000usize; 4];
        assert_eq!(batch_end(&sizes, 0, MAX_BATCH_BYTES), 3);
        assert_eq!(batch_end(&sizes, 3, MAX_BATCH_BYTES), 4);
    }

    #[test]
    fn batch_end_takes_one_oversize_entry_alone() {
        let sizes = [5usize, 20, 5];
        assert_eq!(batch_end(&sizes, 0, 10), 1);
        assert_eq!(batch_end(&sizes, 1, 10), 2);
        assert_eq!(batch_end(&sizes, 2, 10), 3);
    }

    #[test]
    fn clamp_visibility_keeps_budget() {
        assert_eq!(clamp_visibility(30, 0), 30);
        assert_eq!(
            clamp_visibility(43_200, 0),
            MAX_VISIBILITY_SECS - VISIBILITY_MARGIN_SECS
        );
        assert_eq!(clamp_visibility(30, 43_190), 5);
        assert_eq!(clamp_visibility(30, 43_195), 0);
        assert_eq!(clamp_visibility(30, u64::MAX), 0);
    }

    /// The constants in `verus/policy.rs` match this file.
    #[test]
    fn verus_constants_match() {
        let spec = include_str!("../verus/policy.rs");
        for line in [
            format!(
                "pub const MAX_DELAY_SECS: u64 = {};",
                fmt_num(MAX_DELAY_SECS)
            ),
            format!(
                "pub const MAX_VISIBILITY_SECS: u64 = {};",
                fmt_num(MAX_VISIBILITY_SECS)
            ),
            format!("pub const MAX_BATCH: usize = {MAX_BATCH};"),
            format!(
                "pub const MAX_BATCH_BYTES: usize = {};",
                fmt_num(MAX_BATCH_BYTES as u64)
            ),
            format!("pub const VISIBILITY_MARGIN_SECS: u64 = {VISIBILITY_MARGIN_SECS};"),
        ] {
            assert!(spec.contains(&line), "verus/policy.rs lacks: {line}");
        }
    }

    /// Formats like rustfmt source: `43_200`.
    fn fmt_num(n: u64) -> String {
        let digits = n.to_string();
        if digits.len() <= 3 {
            return digits;
        }
        let mut out = String::new();
        for (i, c) in digits.chars().enumerate() {
            if i > 0 && (digits.len() - i).is_multiple_of(3) {
                out.push('_');
            }
            out.push(c);
        }
        out
    }

    #[test]
    fn heartbeat_is_half_and_at_least_one() {
        assert_eq!(heartbeat_interval_secs(0), 1);
        assert_eq!(heartbeat_interval_secs(1), 1);
        assert_eq!(heartbeat_interval_secs(30), 15);
        assert_eq!(heartbeat_interval_secs(31), 15);
    }

    proptest! {
        // Postconditions of `backoff_secs` in the Verus spec.
        #[test]
        fn prop_backoff_matches_spec(initial in any::<u64>(), attempt in any::<u32>(), max in 0u64..=MAX_VISIBILITY_SECS) {
            let s = backoff_secs(initial, attempt, max);
            let spec = spec_backoff_ms(initial, attempt, max * 1000);
            prop_assert!(s <= max);
            prop_assert!(s * 1000 >= spec);
            prop_assert!(s * 1000 < spec + 1000);
        }

        #[test]
        fn prop_backoff_monotone(initial in any::<u64>(), attempt in 1u32..u32::MAX, max in 0u64..=MAX_VISIBILITY_SECS) {
            prop_assert!(backoff_secs(initial, attempt, max) <= backoff_secs(initial, attempt + 1, max));
        }

        #[test]
        fn prop_decide_matches_spec(attempt in any::<u32>(), max in any::<u32>()) {
            prop_assert_eq!(decide(attempt, max) == Decision::Retry, attempt < max);
        }

        #[test]
        fn prop_split_delay(remaining in any::<u64>()) {
            let d = split_delay(remaining);
            prop_assert!(d.now_secs <= MAX_DELAY_SECS);
            prop_assert_eq!(d.now_secs + d.later_secs, remaining);
            if d.later_secs > 0 {
                prop_assert_eq!(d.now_secs, MAX_DELAY_SECS);
            }
        }

        // Batches cover all entries in order. Each has 1..=10 entries and fits
        // the byte limit, or is one oversize entry.
        #[test]
        fn prop_batches_cover_exactly(sizes in proptest::collection::vec(0usize..400_000, 0..60), max in 1usize..=MAX_BATCH_BYTES) {
            let mut start = 0;
            while start < sizes.len() {
                let end = batch_end(&sizes, start, max);
                prop_assert!(end > start && end <= sizes.len());
                prop_assert!(end - start <= MAX_BATCH);
                let total: usize = sizes[start..end].iter().sum();
                prop_assert!(total <= max || end - start == 1);
                start = end;
            }
            prop_assert_eq!(batch_end(&sizes, sizes.len(), max), sizes.len());
        }

        #[test]
        fn prop_backoff_clamps_large_max(initial in any::<u64>(), attempt in any::<u32>(), max in MAX_VISIBILITY_SECS..=u64::MAX) {
            prop_assert_eq!(backoff_secs(initial, attempt, max), backoff_secs(initial, attempt, MAX_VISIBILITY_SECS));
        }

        #[test]
        fn prop_clamp_visibility(requested in any::<u64>(), elapsed in any::<u64>()) {
            let v = clamp_visibility(requested, elapsed);
            prop_assert!(v <= requested);
            prop_assert!(u128::from(v) + u128::from(elapsed) + u128::from(VISIBILITY_MARGIN_SECS) <= u128::from(MAX_VISIBILITY_SECS) || v == 0);
        }

        #[test]
        fn prop_heartbeat(v in any::<u64>()) {
            let s = heartbeat_interval_secs(v);
            if v >= 2 {
                prop_assert_eq!(s, v / 2);
            } else {
                prop_assert_eq!(s, 1);
            }
        }

        #[test]
        fn prop_attempt(count in proptest::option::of(any::<u32>())) {
            let a = attempt_from_receive_count(count);
            match count {
                Some(c) if c >= 1 => prop_assert_eq!(a, c),
                _ => prop_assert_eq!(a, 1),
            }
        }
    }
}
