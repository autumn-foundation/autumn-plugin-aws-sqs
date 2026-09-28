//! Receive loop: long poll, run handlers, ack, retry, dead-letter, drain.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_web::AppState;
use autumn_web::reexports::tokio_util::sync::CancellationToken;
use futures::FutureExt as _;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::config::WorkerConfig;
use crate::envelope::{ATTR_DEAD_ATTEMPTS, ATTR_DEAD_REASON, ATTR_DEAD_SOURCE};
use crate::metrics::SqsMetrics;
use crate::policy::{
    Decision, attempt_from_receive_count, backoff_secs, clamp_visibility, decide,
    heartbeat_interval_secs,
};
use crate::transport::{
    BoxFuture, MAX_ATTRIBUTES, MAX_MESSAGE_BYTES, OutboundMessage, ReceiveOptions, ReceivedMessage,
    SqsTransport, is_fifo, is_valid_sqs_text, sanitize_sqs_text,
};

/// Largest dead-letter reason kept in the attribute.
const MAX_REASON_CHARS: usize = 256;
/// Shortest wait for a message that stays for the SQS redrive policy.
pub(crate) const MIN_DEFER_SECS: u64 = 30;
/// First wait after a receive error. Doubles to [`MAX_ERROR_BACKOFF`].
const ERROR_BACKOFF: Duration = Duration::from_secs(1);
const MAX_ERROR_BACKOFF: Duration = Duration::from_secs(30);

/// Retry rule for a failed message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetryRule {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
}

/// Result of one handler run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Done. Delete the message.
    Ack,
    /// Done by a new message (delay hop). Delete this one.
    Hopped,
    /// Failed. Retry under the rule.
    Retry { error: String, rule: RetryRule },
    /// Failed with no retry. Dead-letter now.
    Poison(String),
}

/// Runs one message.
pub(crate) trait Dispatch: Send + Sync + 'static {
    /// The retry rule for this message. The worker reads it before the run.
    fn rule(&self, message: &ReceivedMessage) -> RetryRule;
    fn dispatch(&self, state: AppState, message: ReceivedMessage) -> BoxFuture<'static, Outcome>;
}

/// One queue to drain.
#[derive(Clone)]
pub(crate) struct WorkerSpec {
    /// Metrics label.
    pub label: String,
    pub queue_url: String,
    pub dispatch: Arc<dyn Dispatch>,
    pub dead_letter_url: Option<String>,
}

/// Shared worker parts.
#[derive(Clone)]
pub(crate) struct WorkerCtx {
    pub transport: Arc<dyn SqsTransport>,
    pub config: WorkerConfig,
    pub metrics: Arc<SqsMetrics>,
    pub state: AppState,
    pub cancel: CancellationToken,
    pub running: Arc<AtomicUsize>,
}

/// Splits one receive into units of work.
///
/// A standard queue gives one unit per message. A FIFO queue gives one unit
/// per message group, in receive order, so a group runs in sequence.
pub(crate) fn units(fifo: bool, messages: Vec<ReceivedMessage>) -> Vec<Vec<ReceivedMessage>> {
    if !fifo {
        return messages.into_iter().map(|m| vec![m]).collect();
    }
    let mut out: Vec<Vec<ReceivedMessage>> = Vec::new();
    let mut index: BTreeMap<Option<String>, usize> = BTreeMap::new();
    for m in messages {
        if let Some(&i) = index.get(&m.group_id) {
            out[i].push(m);
        } else {
            index.insert(m.group_id.clone(), out.len());
            out.push(vec![m]);
        }
    }
    out
}

/// Drains one queue until `ctx.cancel` fires. Then waits for in-flight
/// handlers up to `drain_timeout_secs`.
pub(crate) async fn run(ctx: WorkerCtx, spec: WorkerSpec) {
    // The caller counts this loop in `running` before spawn.
    tracing::info!(queue = %spec.label, "aws_sqs worker started");
    let fifo = is_fifo(&spec.queue_url);
    let sem = Arc::new(Semaphore::new(ctx.config.max_in_flight));
    let mut tasks = JoinSet::new();
    let mut error_backoff = ERROR_BACKOFF;
    loop {
        while tasks.try_join_next().is_some() {}
        if ctx.cancel.is_cancelled() {
            break;
        }
        let free = sem.available_permits();
        if free == 0 {
            tokio::select! {
                () = ctx.cancel.cancelled() => break,
                Some(_) = tasks.join_next() => {}
            }
            continue;
        }
        let max_messages = u32::try_from(free)
            .unwrap_or(u32::MAX)
            .min(ctx.config.max_messages);
        let options = ReceiveOptions {
            max_messages,
            wait_secs: ctx.config.wait_time_secs,
            visibility_secs: ctx.config.visibility_timeout_secs,
        };
        let received = tokio::select! {
            () = ctx.cancel.cancelled() => break,
            r = ctx.transport.receive(&spec.queue_url, options) => r,
        };
        let received_at = Instant::now();
        match received {
            Ok(messages) => {
                error_backoff = ERROR_BACKOFF;
                // Units never outnumber messages, and messages never outnumber
                // the free permits, so each unit gets a permit.
                for unit in units(fifo, messages) {
                    let Ok(permit) = Arc::clone(&sem).try_acquire_owned() else {
                        // Not reachable. The messages come back after the timeout.
                        continue;
                    };
                    let n = unit.len() as u64;
                    ctx.metrics.update(&spec.label, |c| {
                        c.received += n;
                        c.in_flight += n;
                    });
                    let (ctx, spec) = (ctx.clone(), spec.clone());
                    tasks.spawn(async move {
                        handle_unit(&ctx, &spec, unit, received_at).await;
                        drop(permit);
                    });
                }
            }
            Err(e) => {
                ctx.metrics.update(&spec.label, |c| c.receive_errors += 1);
                tracing::warn!(queue = %spec.label, error = %e, "aws_sqs receive failed");
                tokio::select! {
                    () = ctx.cancel.cancelled() => break,
                    () = tokio::time::sleep(error_backoff) => {}
                }
                error_backoff = (error_backoff * 2).min(MAX_ERROR_BACKOFF);
            }
        }
    }
    drain(&ctx, &spec, tasks).await;
    ctx.running.fetch_sub(1, Ordering::SeqCst);
    tracing::info!(queue = %spec.label, "aws_sqs worker stopped");
}

async fn drain(ctx: &WorkerCtx, spec: &WorkerSpec, mut tasks: JoinSet<()>) {
    if tasks.is_empty() {
        return;
    }
    let limit = Duration::from_secs(ctx.config.drain_timeout_secs);
    let waited =
        tokio::time::timeout(limit, async { while tasks.join_next().await.is_some() {} }).await;
    if waited.is_err() {
        tracing::warn!(
            queue = %spec.label,
            left = tasks.len(),
            "aws_sqs drain timed out; the messages come back after the visibility timeout"
        );
        tasks.abort_all();
    }
}

/// Receipts that the heartbeat keeps invisible.
type Pending = Arc<Mutex<Vec<String>>>;

fn forget(pending: &Pending, receipt: &str) {
    pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|r| r != receipt);
}

/// Runs a unit in order. When a message stays in the queue, the rest of a
/// FIFO group waits for the same time. They come back together, in queue
/// order, so a later message never runs first.
async fn handle_unit(
    ctx: &WorkerCtx,
    spec: &WorkerSpec,
    unit: Vec<ReceivedMessage>,
    received_at: Instant,
) {
    let pending: Pending = Arc::new(Mutex::new(
        unit.iter().map(|m| m.receipt_handle.clone()).collect(),
    ));
    let work = async {
        let mut rest = unit.into_iter();
        while let Some(message) = rest.next() {
            let stays = handle(ctx, spec, &message, received_at, &pending).await;
            ctx.metrics
                .update(&spec.label, |c| c.in_flight = c.in_flight.saturating_sub(1));
            if let Some(wait) = stays {
                for later in rest.by_ref() {
                    forget(&pending, &later.receipt_handle);
                    set_visibility(ctx, spec, &later.receipt_handle, wait, received_at).await;
                    ctx.metrics
                        .update(&spec.label, |c| c.in_flight = c.in_flight.saturating_sub(1));
                }
            }
        }
    };
    if ctx.config.heartbeat {
        with_heartbeat(ctx, spec, &pending, received_at, work).await;
    } else {
        work.await;
    }
}

/// Runs one message and applies the outcome. Returns the visibility wait
/// when the message stays in the queue.
async fn handle(
    ctx: &WorkerCtx,
    spec: &WorkerSpec,
    message: &ReceivedMessage,
    received_at: Instant,
    pending: &Pending,
) -> Option<u64> {
    let attempt = attempt_from_receive_count(message.receive_count);
    let rule = spec.dispatch.rule(message);
    // A message over its limit does not run again. This covers a failed
    // dead-letter send and a queue with no DLQ.
    let outcome = if attempt > rule.max_attempts {
        Outcome::Retry {
            error: "no attempts remain".to_owned(),
            rule,
        }
    } else {
        // The async block puts the synchronous part of `dispatch` inside
        // `catch_unwind` too.
        let run = AssertUnwindSafe(async {
            spec.dispatch
                .dispatch(ctx.state.clone(), message.clone())
                .await
        })
        .catch_unwind();
        run.await
            .unwrap_or_else(|panic| Outcome::Poison(panic_text(panic.as_ref())))
    };
    forget(pending, &message.receipt_handle);
    match outcome {
        Outcome::Ack | Outcome::Hopped => {
            let hopped = matches!(outcome, Outcome::Hopped);
            match ctx
                .transport
                .delete(&spec.queue_url, &message.receipt_handle)
                .await
            {
                Ok(()) => ctx.metrics.update(&spec.label, |c| {
                    if hopped {
                        c.hops += 1;
                    } else {
                        c.succeeded += 1;
                    }
                }),
                Err(e) => {
                    ctx.metrics.update(&spec.label, |c| c.ack_errors += 1);
                    tracing::warn!(queue = %spec.label, error = %e, "aws_sqs delete failed; the message can run again");
                }
            }
            None
        }
        Outcome::Retry { error, rule } => match decide(attempt, rule.max_attempts) {
            Decision::Retry => {
                let delay = backoff_secs(
                    rule.initial_backoff_ms,
                    attempt,
                    ctx.config.max_backoff_secs,
                );
                tracing::info!(queue = %spec.label, attempt, delay, error = %error, "aws_sqs retry");
                ctx.metrics.update(&spec.label, |c| c.retried += 1);
                set_visibility(ctx, spec, &message.receipt_handle, delay, received_at).await;
                Some(delay)
            }
            Decision::Exhausted => {
                let reason = format!("exhausted after {attempt} attempts: {error}");
                dead_letter(ctx, spec, message, attempt, &reason, rule, received_at).await
            }
        },
        Outcome::Poison(reason) => {
            ctx.metrics.update(&spec.label, |c| c.poisoned += 1);
            dead_letter(ctx, spec, message, attempt, &reason, rule, received_at).await
        }
    }
}

/// Polls `work` and extends visibility of the pending receipts each half
/// timeout, inside the 12 h budget of the receive.
async fn with_heartbeat<F: std::future::Future<Output = ()>>(
    ctx: &WorkerCtx,
    spec: &WorkerSpec,
    pending: &Pending,
    received_at: Instant,
    work: F,
) {
    let every = Duration::from_secs(heartbeat_interval_secs(ctx.config.visibility_timeout_secs));
    let mut ticker = tokio::time::interval_at(Instant::now() + every, every);
    let mut beating = true;
    tokio::pin!(work);
    loop {
        tokio::select! {
            () = &mut work => return,
            _ = ticker.tick(), if beating => {
                let elapsed = received_at.elapsed().as_secs();
                let secs = clamp_visibility(ctx.config.visibility_timeout_secs, elapsed);
                if secs == 0 {
                    tracing::warn!(queue = %spec.label, "aws_sqs heartbeat stops: the 12 h visibility budget is spent");
                    beating = false;
                    continue;
                }
                let receipts = pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                for receipt in receipts {
                    match ctx
                        .transport
                        .change_visibility(&spec.queue_url, &receipt, secs)
                        .await
                    {
                        Ok(()) => ctx.metrics.update(&spec.label, |c| c.heartbeats += 1),
                        Err(e) => {
                            ctx.metrics.update(&spec.label, |c| c.ack_errors += 1);
                            tracing::warn!(queue = %spec.label, error = %e, "aws_sqs heartbeat failed");
                        }
                    }
                }
            }
        }
    }
}

/// Changes visibility inside the 12 h budget of the receive.
async fn set_visibility(
    ctx: &WorkerCtx,
    spec: &WorkerSpec,
    receipt: &str,
    secs: u64,
    received_at: Instant,
) {
    let secs = clamp_visibility(secs, received_at.elapsed().as_secs());
    if let Err(e) = ctx
        .transport
        .change_visibility(&spec.queue_url, receipt, secs)
        .await
    {
        ctx.metrics.update(&spec.label, |c| c.ack_errors += 1);
        tracing::warn!(queue = %spec.label, error = %e, "aws_sqs visibility change failed");
    }
}

/// Builds the dead-letter copy of `message`.
///
/// It adds three attributes: reason, source queue, attempts. It keeps up to
/// seven original attributes, so the total stays at 10 or less. It drops
/// attributes until the size is inside the SQS limit.
pub(crate) fn dead_letter_message(
    message: &ReceivedMessage,
    source_url: &str,
    dead_letter_url: &str,
    attempt: u32,
    reason: &str,
) -> OutboundMessage {
    let mut reason: String = sanitize_sqs_text(reason)
        .chars()
        .take(MAX_REASON_CHARS)
        .collect();
    if reason.trim().is_empty() {
        "unknown".clone_into(&mut reason);
    }
    let meta = [
        (ATTR_DEAD_REASON, reason),
        (ATTR_DEAD_SOURCE, source_url.to_owned()),
        (ATTR_DEAD_ATTEMPTS, attempt.to_string()),
    ];
    let mut copied: Vec<(String, String)> = message
        .attributes
        .iter()
        .filter(|(k, v)| {
            !meta.iter().any(|(m, _)| m == k)
                && !k.is_empty()
                && !v.is_empty()
                && is_valid_sqs_text(k)
                && is_valid_sqs_text(v)
        })
        .take(MAX_ATTRIBUTES - meta.len())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let fifo = is_fifo(dead_letter_url);
    let build = |copied: &[(String, String)], meta: &[(&str, String)]| OutboundMessage {
        body: message.body.clone(),
        delay_secs: 0,
        group_id: fifo.then(|| {
            message
                .group_id
                .clone()
                .unwrap_or_else(|| "dead-letter".to_owned())
        }),
        dedup_id: fifo.then(|| message.message_id.clone()),
        attributes: copied
            .iter()
            .cloned()
            .chain(meta.iter().map(|(k, v)| ((*k).to_owned(), v.clone())))
            .collect(),
    };
    let mut out = build(&copied, &meta);
    while out.size_bytes() > MAX_MESSAGE_BYTES && copied.pop().is_some() {
        out = build(&copied, &meta);
    }
    if out.size_bytes() > MAX_MESSAGE_BYTES {
        out = build(&[], &[]);
    }
    out
}

/// Sends the message to the DLQ and deletes it. With no DLQ, leaves it for
/// the SQS redrive policy. Returns the visibility wait when the message stays.
async fn dead_letter(
    ctx: &WorkerCtx,
    spec: &WorkerSpec,
    message: &ReceivedMessage,
    attempt: u32,
    reason: &str,
    rule: RetryRule,
    received_at: Instant,
) -> Option<u64> {
    tracing::error!(queue = %spec.label, attempt, reason = %reason, "aws_sqs dead letter");
    // A message that stays waits at least MIN_DEFER_SECS. It does not spin.
    let defer = backoff_secs(
        rule.initial_backoff_ms,
        attempt,
        ctx.config.max_backoff_secs,
    )
    .max(MIN_DEFER_SECS);
    let Some(dlq) = &spec.dead_letter_url else {
        ctx.metrics.update(&spec.label, |c| c.redrive_deferred += 1);
        set_visibility(ctx, spec, &message.receipt_handle, defer, received_at).await;
        return Some(defer);
    };
    let out = dead_letter_message(message, &spec.queue_url, dlq, attempt, reason);
    match ctx.transport.send(dlq, out).await {
        Ok(_) => {
            ctx.metrics.update(&spec.label, |c| c.dead_lettered += 1);
            if let Err(e) = ctx
                .transport
                .delete(&spec.queue_url, &message.receipt_handle)
                .await
            {
                ctx.metrics.update(&spec.label, |c| c.ack_errors += 1);
                tracing::warn!(queue = %spec.label, error = %e, "aws_sqs delete after dead letter failed");
            }
            None
        }
        Err(e) => {
            ctx.metrics.update(&spec.label, |c| c.send_errors += 1);
            tracing::warn!(queue = %spec.label, error = %e, "aws_sqs dead-letter send failed; the message stays");
            set_visibility(ctx, spec, &message.receipt_handle, defer, received_at).await;
            Some(defer)
        }
    }
}

fn panic_text(panic: &(dyn std::any::Any + Send)) -> String {
    let detail = panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown".to_owned());
    format!("handler panicked: {detail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::validate_outbound;

    fn received(n: usize, group: Option<&str>) -> ReceivedMessage {
        ReceivedMessage {
            message_id: format!("m{n}"),
            receipt_handle: format!("r{n}"),
            body: format!("b{n}"),
            receive_count: Some(1),
            attributes: BTreeMap::new(),
            group_id: group.map(str::to_owned),
        }
    }

    #[test]
    fn panic_text_reads_str_and_string() {
        let a: Box<dyn std::any::Any + Send> = Box::new("boom");
        assert_eq!(panic_text(a.as_ref()), "handler panicked: boom");
        let b: Box<dyn std::any::Any + Send> = Box::new(String::from("bang"));
        assert_eq!(panic_text(b.as_ref()), "handler panicked: bang");
        let c: Box<dyn std::any::Any + Send> = Box::new(7_u8);
        assert_eq!(panic_text(c.as_ref()), "handler panicked: unknown");
    }

    #[test]
    fn units_split_standard_per_message_and_fifo_per_group() {
        let msgs = vec![
            received(1, Some("a")),
            received(2, Some("b")),
            received(3, Some("a")),
        ];
        assert_eq!(units(false, msgs.clone()).len(), 3);
        let fifo = units(true, msgs);
        let ids: Vec<Vec<&str>> = fifo
            .iter()
            .map(|u| u.iter().map(|m| m.message_id.as_str()).collect())
            .collect();
        assert_eq!(ids, vec![vec!["m1", "m3"], vec!["m2"]]);
    }

    #[test]
    fn dead_letter_message_keeps_ten_attributes_or_less() {
        let mut m = received(1, None);
        for i in 0..10 {
            m.attributes.insert(format!("a{i}"), "v".into());
        }
        let out = dead_letter_message(&m, "https://q/src", "https://q/dlq", 3, "boom");
        assert_eq!(out.attributes.len(), MAX_ATTRIBUTES);
        assert_eq!(out.attributes[ATTR_DEAD_REASON], "boom");
        assert_eq!(out.attributes[ATTR_DEAD_SOURCE], "https://q/src");
        assert_eq!(out.attributes[ATTR_DEAD_ATTEMPTS], "3");
        assert!(validate_outbound("https://q/dlq", &out).is_ok());
    }

    #[test]
    fn dead_letter_reason_is_valid_text() {
        let m = received(1, None);
        let out = dead_letter_message(&m, "s", "https://q/dlq", 1, "");
        assert_eq!(out.attributes[ATTR_DEAD_REASON], "unknown");
        let long = "é\u{0}".repeat(1_000);
        let out = dead_letter_message(&m, "s", "https://q/dlq", 1, &long);
        let reason = &out.attributes[ATTR_DEAD_REASON];
        assert_eq!(reason.chars().count(), MAX_REASON_CHARS);
        assert!(is_valid_sqs_text(reason));
        assert!(validate_outbound("https://q/dlq", &out).is_ok());
    }

    #[test]
    fn dead_letter_message_fits_size_limit() {
        let mut m = received(1, None);
        m.body = "x".repeat(MAX_MESSAGE_BYTES - 40);
        m.attributes.insert("k".into(), "v".repeat(30));
        let out = dead_letter_message(&m, "https://q/src", "https://q/dlq", 1, "why");
        assert!(out.size_bytes() <= MAX_MESSAGE_BYTES);
        assert!(validate_outbound("https://q/dlq", &out).is_ok());
    }

    #[test]
    fn dead_letter_message_for_fifo_keeps_group_and_dedups_by_id() {
        let m = received(7, Some("tenant-1"));
        let out = dead_letter_message(&m, "s", "https://q/dlq.fifo", 1, "why");
        assert_eq!(out.group_id.as_deref(), Some("tenant-1"));
        assert_eq!(out.dedup_id.as_deref(), Some("m7"));
    }
}
