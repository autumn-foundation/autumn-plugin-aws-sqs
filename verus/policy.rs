//! Verus spec and proof for `src/policy.rs`.
//!
//! Keep this file in step with `src/policy.rs`.
//! Run: `verus verus/policy.rs`.

use vstd::prelude::*;

verus! {

/// SQS limit for `DelaySeconds`.
pub const MAX_DELAY_SECS: u64 = 900;
/// SQS limit for the visibility timeout.
pub const MAX_VISIBILITY_SECS: u64 = 43_200;
/// SQS limit for entries in one batch.
pub const MAX_BATCH: usize = 10;
/// SQS limit for the total payload of one batch request.
pub const MAX_BATCH_BYTES: usize = 1_048_576;
/// Safety margin under the 12 h visibility budget of one receive.
pub const VISIBILITY_MARGIN_SECS: u64 = 5;

// ---------------------------------------------------------------- attempts

/// Decision after a handler fails.
pub enum Decision {
    Retry,
    Exhausted,
}

/// Spec: retry only while the attempt is below the limit.
pub open spec fn spec_retry(attempt: u32, max_attempts: u32) -> bool {
    attempt < max_attempts
}

pub fn decide(attempt: u32, max_attempts: u32) -> (d: Decision)
    ensures
        (d is Retry) == spec_retry(attempt, max_attempts),
{
    if attempt < max_attempts { Decision::Retry } else { Decision::Exhausted }
}

/// Spec: SQS gives a receive count of 1 or more. Treat 0 or none as 1.
pub fn attempt_from_receive_count(count: Option<u32>) -> (a: u32)
    ensures
        a >= 1,
        count matches Some(c) && c >= 1 ==> a == count->Some_0,
        (count is None || count->Some_0 == 0) ==> a == 1,
{
    match count {
        Some(c) if c >= 1 => c,
        _ => 1,
    }
}

// ----------------------------------------------------------------- backoff

pub open spec fn spec_min(a: int, b: int) -> int {
    if a <= b { a } else { b }
}

/// Spec: exponential step, `initial * 2^(attempt - 1)`, capped at `cap`.
pub open spec fn spec_backoff_ms(initial_ms: int, attempt: int, cap_ms: int) -> int
    decreases attempt,
{
    if attempt <= 1 {
        spec_min(initial_ms, cap_ms)
    } else {
        spec_min(spec_backoff_ms(initial_ms, attempt - 1, cap_ms) * 2, cap_ms)
    }
}

/// Spec: the effective cap in seconds.
pub open spec fn spec_cap_secs(max_secs: int) -> int {
    spec_min(max_secs, MAX_VISIBILITY_SECS as int)
}

proof fn lemma_backoff_bounds(initial_ms: int, attempt: int, cap_ms: int)
    requires
        initial_ms >= 0,
        cap_ms >= 0,
    ensures
        0 <= spec_backoff_ms(initial_ms, attempt, cap_ms) <= cap_ms,
    decreases attempt,
{
    if attempt > 1 {
        lemma_backoff_bounds(initial_ms, attempt - 1, cap_ms);
    }
}

/// Monotone: a later attempt never waits less.
proof fn lemma_backoff_monotone(initial_ms: int, attempt: int, cap_ms: int)
    requires
        initial_ms >= 0,
        cap_ms >= 0,
        attempt >= 1,
    ensures
        spec_backoff_ms(initial_ms, attempt, cap_ms) <= spec_backoff_ms(
            initial_ms,
            attempt + 1,
            cap_ms,
        ),
{
    lemma_backoff_bounds(initial_ms, attempt, cap_ms);
}

/// A fixed point stays fixed. Used for the early loop exit at 0 or at the cap.
proof fn lemma_fixed_point(initial_ms: int, i: int, j: int, cap_ms: int)
    requires
        1 <= i <= j,
        spec_min(spec_backoff_ms(initial_ms, i, cap_ms) * 2, cap_ms) == spec_backoff_ms(
            initial_ms,
            i,
            cap_ms,
        ),
    ensures
        spec_backoff_ms(initial_ms, j, cap_ms) == spec_backoff_ms(initial_ms, i, cap_ms),
    decreases j - i,
{
    if j > i {
        lemma_fixed_point(initial_ms, i, j - 1, cap_ms);
    }
}

/// Closed form: `spec_backoff_ms == min(initial * 2^(attempt - 1), cap)`.
/// The property tests in `src/policy.rs` use this form as the oracle.
proof fn lemma_backoff_closed_form(initial_ms: int, attempt: int, cap_ms: int)
    requires
        initial_ms >= 0,
        cap_ms >= 0,
        attempt >= 1,
    ensures
        spec_backoff_ms(initial_ms, attempt, cap_ms) == spec_min(
            initial_ms * vstd::arithmetic::power2::pow2((attempt - 1) as nat) as int,
            cap_ms,
        ),
    decreases attempt,
{
    vstd::arithmetic::power2::lemma_pow2_pos((attempt - 1) as nat);
    if attempt == 1 {
        vstd::arithmetic::power2::lemma2_to64();
        assert(initial_ms * vstd::arithmetic::power2::pow2(0) as int == initial_ms);
    }
    if attempt > 1 {
        lemma_backoff_closed_form(initial_ms, attempt - 1, cap_ms);
        let p = vstd::arithmetic::power2::pow2((attempt - 2) as nat) as int;
        vstd::arithmetic::power2::lemma_pow2_unfold((attempt - 1) as nat);
        vstd::arithmetic::power2::lemma_pow2_pos((attempt - 2) as nat);
        assert(vstd::arithmetic::power2::pow2((attempt - 1) as nat) as int == 2 * p);
        let x = initial_ms * p;
        assert(initial_ms * (2 * p) == 2 * x) by (nonlinear_arith)
            requires
                x == initial_ms * p,
        ;
        assert(x >= 0) by (nonlinear_arith)
            requires
                initial_ms >= 0,
                p > 0,
                x == initial_ms * p,
        ;
        // min(2 * min(x, cap), cap) == min(2x, cap) for x, cap >= 0.
        assert(spec_min(2 * spec_min(x, cap_ms), cap_ms) == spec_min(2 * x, cap_ms));
        assert(spec_backoff_ms(initial_ms, attempt, cap_ms) == spec_min(
            spec_backoff_ms(initial_ms, attempt - 1, cap_ms) * 2,
            cap_ms,
        ));
        assert(initial_ms * vstd::arithmetic::power2::pow2((attempt - 1) as nat) as int == 2 * x);
    }
}

/// Backoff in whole seconds, rounded up, never above the cap.
pub fn backoff_secs(initial_ms: u64, attempt: u32, max_secs: u64) -> (s: u64)
    ensures
        s <= spec_cap_secs(max_secs as int),
        s * 1000 >= spec_backoff_ms(
            initial_ms as int,
            attempt as int,
            spec_cap_secs(max_secs as int) * 1000,
        ),
        s * 1000 < spec_backoff_ms(
            initial_ms as int,
            attempt as int,
            spec_cap_secs(max_secs as int) * 1000,
        ) + 1000,
{
    let max_secs: u64 = if max_secs < MAX_VISIBILITY_SECS { max_secs } else { MAX_VISIBILITY_SECS };
    let cap_ms: u64 = max_secs * 1000;
    let ghost cap: int = cap_ms as int;
    let mut ms: u64 = if initial_ms < cap_ms { initial_ms } else { cap_ms };
    let mut i: u32 = 1;
    proof {
        lemma_backoff_bounds(initial_ms as int, 1, cap);
    }
    while i < attempt && ms != 0 && ms != cap_ms
        invariant
            1 <= i,
            i <= attempt || i == 1,
            max_secs <= MAX_VISIBILITY_SECS,
            cap_ms == max_secs * 1000,
            cap == cap_ms as int,
            ms <= cap_ms,
            ms as int == spec_backoff_ms(initial_ms as int, i as int, cap),
        decreases attempt - i,
    {
        ms = if ms <= cap_ms / 2 { ms * 2 } else { cap_ms };
        i = i + 1;
    }
    proof {
        let a = attempt as int;
        let v = spec_backoff_ms(initial_ms as int, i as int, cap);
        if a <= 1 {
            // Loop did not run; attempt 0 and 1 share the first step.
            assert(spec_backoff_ms(initial_ms as int, a, cap) == v);
        } else if (i as int) < a {
            // Early exit at 0 or at the cap: both are fixed points.
            lemma_fixed_point(initial_ms as int, i as int, a, cap);
        }
    }
    let q: u64 = ms / 1000;
    let r: u64 = ms % 1000;
    proof {
        vstd::arithmetic::div_mod::lemma_fundamental_div_mod(ms as int, 1000);
        assert(ms as int == 1000 * q + r);
        assert(r < 1000);
    }
    let s: u64 = if r == 0 { q } else { q + 1 };
    s
}

// ------------------------------------------------------------------- delay

/// Delay to send now, and the part that stays for later hops.
pub struct DelaySplit {
    pub now_secs: u64,
    pub later_secs: u64,
}

pub fn split_delay(remaining_secs: u64) -> (d: DelaySplit)
    ensures
        d.now_secs <= MAX_DELAY_SECS,
        d.now_secs + d.later_secs == remaining_secs,
        // Progress: a hop always moves time forward by the full limit.
        d.later_secs > 0 ==> d.now_secs == MAX_DELAY_SECS,
{
    if remaining_secs <= MAX_DELAY_SECS {
        DelaySplit { now_secs: remaining_secs, later_secs: 0 }
    } else {
        DelaySplit { now_secs: MAX_DELAY_SECS, later_secs: remaining_secs - MAX_DELAY_SECS }
    }
}

// ------------------------------------------------------------------- batch

/// Spec: total of `s[lo..hi]`.
pub open spec fn spec_sum(s: Seq<usize>, lo: int, hi: int) -> int
    decreases hi - lo,
{
    if hi <= lo { 0 } else { spec_sum(s, lo, hi - 1) + s[hi - 1] }
}

/// End (exclusive) of the batch that starts at `start`.
pub fn batch_end(sizes: &[usize], start: usize, max_bytes: usize) -> (end: usize)
    ensures
        start >= sizes.len() ==> end == start,
        start < sizes.len() ==> {
            // Progress, bounds, count limit.
            &&& start < end <= sizes.len()
            &&& end - start <= MAX_BATCH
            // Byte limit, unless one entry is larger than the limit.
            &&& (spec_sum(sizes@, start as int, end as int) <= max_bytes || end == start + 1)
        },
{
    if start >= sizes.len() {
        return start;
    }
    let mut end = start;
    let mut total: usize = 0;
    while end < sizes.len() && end - start < MAX_BATCH
        invariant
            start < sizes.len(),
            start <= end <= sizes.len(),
            end - start <= MAX_BATCH,
            total <= max_bytes,
            total as int == spec_sum(sizes@, start as int, end as int),
        ensures
            start < end <= sizes.len(),
            end - start <= MAX_BATCH,
            total <= max_bytes,
            total as int == spec_sum(sizes@, start as int, end as int),
        decreases sizes.len() - end,
    {
        let size = sizes[end];
        if size > max_bytes - total {
            if end == start {
                return start + 1;
            }
            break;
        }
        total = total + size;
        end = end + 1;
    }
    end
}

// -------------------------------------------------------------- visibility

/// Largest visibility timeout that SQS accepts now.
pub fn clamp_visibility(requested: u64, elapsed_secs: u64) -> (v: u64)
    ensures
        v <= requested,
        v > 0 ==> v + elapsed_secs + VISIBILITY_MARGIN_SECS <= MAX_VISIBILITY_SECS,
        // No needless clamp: a request inside the budget passes unchanged.
        requested + elapsed_secs + VISIBILITY_MARGIN_SECS <= MAX_VISIBILITY_SECS ==> v == requested,
{
    let used = elapsed_secs.saturating_add(VISIBILITY_MARGIN_SECS);
    if used >= MAX_VISIBILITY_SECS {
        return 0;
    }
    let left = MAX_VISIBILITY_SECS - used;
    if requested < left { requested } else { left }
}

// --------------------------------------------------------------- heartbeat

/// Interval between visibility extensions: half the timeout, 1 s or more.
pub fn heartbeat_interval_secs(visibility_secs: u64) -> (s: u64)
    ensures
        visibility_secs >= 2 ==> s == visibility_secs / 2,
        visibility_secs < 2 ==> s == 1,
{
    if visibility_secs / 2 >= 1 { visibility_secs / 2 } else { 1 }
}

fn main() {}

} // verus!
