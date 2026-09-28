//! Receive loop: long poll, run handlers, ack, retry, dead-letter, drain.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_web::AppState;
use autumn_web::reexports::tokio_util::sync::CancellationToken;
use futures::FutureExt as _;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::config::WorkerConfig;
use crate::envelope::{ATTR_DEAD_ATTEMPTS, ATTR_DEAD_REASON, ATTR_DEAD_SOURCE};
use crate::metrics::SqsMetrics;
use crate::policy::{
    Decision, attempt_from_receive_count, backoff_secs, decide, heartbeat_interval_secs,
};
use crate::transport::{
    BoxFuture, OutboundMessage, ReceiveOptions, ReceivedMessage, SqsTransport, is_fifo,
};

/// Largest dead-letter reason kept in the attribute.
const MAX_REASON_CHARS: usize = 512;
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

/// Drains one queue until `ctx.cancel` fires. Then waits for in-flight
/// handlers up to `drain_timeout_secs`.
pub(crate) async fn run(ctx: WorkerCtx, spec: WorkerSpec) {
    // The caller counts this loop in `running` before spawn.
    tracing::info!(queue = %spec.label, "aws_sqs worker started");
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
        match received {
            Ok(messages) => {
                error_backoff = ERROR_BACKOFF;
                for message in messages {
                    let Ok(permit) = Arc::clone(&sem).try_acquire_owned() else {
                        // Not reachable: we asked for no more than the free permits.
                        // Let the message time out and come back.
                        continue;
                    };
                    ctx.metrics.update(&spec.label, |c| {
                        c.received += 1;
                        c.in_flight += 1;
                    });
                    let (ctx, spec) = (ctx.clone(), spec.clone());
                    tasks.spawn(async move {
                        handle(&ctx, &spec, message).await;
                        ctx.metrics
                            .update(&spec.label, |c| c.in_flight = c.in_flight.saturating_sub(1));
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

/// Runs one message and applies the outcome.
async fn handle(ctx: &WorkerCtx, spec: &WorkerSpec, message: ReceivedMessage) {
    let attempt = attempt_from_receive_count(message.receive_count);
    let run =
        AssertUnwindSafe(spec.dispatch.dispatch(ctx.state.clone(), message.clone())).catch_unwind();
    let result = if ctx.config.heartbeat {
        with_heartbeat(ctx, spec, &message.receipt_handle, run).await
    } else {
        run.await
    };
    let outcome = result.unwrap_or_else(|panic| Outcome::Poison(panic_text(panic.as_ref())));
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
                set_visibility(ctx, spec, &message.receipt_handle, delay).await;
            }
            Decision::Exhausted => {
                let reason = format!("exhausted after {attempt} attempts: {error}");
                dead_letter(ctx, spec, &message, attempt, &reason, rule).await;
            }
        },
        Outcome::Poison(reason) => {
            ctx.metrics.update(&spec.label, |c| c.poisoned += 1);
            // Without a DLQ the message waits for redrive. Do not spin.
            let rule = RetryRule {
                max_attempts: 1,
                initial_backoff_ms: 1_000,
            };
            dead_letter(ctx, spec, &message, attempt, &reason, rule).await;
        }
    }
}

/// Polls `run` and extends visibility each half timeout until it ends.
async fn with_heartbeat<F: std::future::Future>(
    ctx: &WorkerCtx,
    spec: &WorkerSpec,
    receipt: &str,
    run: F,
) -> F::Output {
    let every = Duration::from_secs(heartbeat_interval_secs(ctx.config.visibility_timeout_secs));
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    tokio::pin!(run);
    loop {
        tokio::select! {
            out = &mut run => return out,
            _ = ticker.tick() => {
                match ctx
                    .transport
                    .change_visibility(&spec.queue_url, receipt, ctx.config.visibility_timeout_secs)
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

async fn set_visibility(ctx: &WorkerCtx, spec: &WorkerSpec, receipt: &str, secs: u64) {
    if let Err(e) = ctx
        .transport
        .change_visibility(&spec.queue_url, receipt, secs)
        .await
    {
        ctx.metrics.update(&spec.label, |c| c.ack_errors += 1);
        tracing::warn!(queue = %spec.label, error = %e, "aws_sqs visibility change failed");
    }
}

/// Sends the message to the DLQ and deletes it. With no DLQ, leaves it for
/// the SQS redrive policy.
async fn dead_letter(
    ctx: &WorkerCtx,
    spec: &WorkerSpec,
    message: &ReceivedMessage,
    attempt: u32,
    reason: &str,
    rule: RetryRule,
) {
    let reason: String = reason.chars().take(MAX_REASON_CHARS).collect();
    tracing::error!(queue = %spec.label, attempt, reason = %reason, "aws_sqs dead letter");
    let Some(dlq) = &spec.dead_letter_url else {
        ctx.metrics.update(&spec.label, |c| c.redrive_deferred += 1);
        let delay = backoff_secs(
            rule.initial_backoff_ms,
            attempt,
            ctx.config.max_backoff_secs,
        );
        set_visibility(ctx, spec, &message.receipt_handle, delay).await;
        return;
    };
    let mut attributes: BTreeMap<String, String> = message.attributes.clone();
    attributes.insert(ATTR_DEAD_REASON.to_owned(), reason);
    attributes.insert(ATTR_DEAD_SOURCE.to_owned(), spec.queue_url.clone());
    attributes.insert(ATTR_DEAD_ATTEMPTS.to_owned(), attempt.to_string());
    let fifo = is_fifo(dlq);
    let out = OutboundMessage {
        body: message.body.clone(),
        delay_secs: 0,
        group_id: fifo.then(|| "dead-letter".to_owned()),
        dedup_id: fifo.then(|| message.message_id.clone()),
        attributes,
    };
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
        }
        Err(e) => {
            ctx.metrics.update(&spec.label, |c| c.send_errors += 1);
            tracing::warn!(queue = %spec.label, error = %e, "aws_sqs dead-letter send failed; the message stays");
            let delay = backoff_secs(
                rule.initial_backoff_ms,
                attempt,
                ctx.config.max_backoff_secs,
            );
            set_visibility(ctx, spec, &message.receipt_handle, delay).await;
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

    #[test]
    fn panic_text_reads_str_and_string() {
        let a: Box<dyn std::any::Any + Send> = Box::new("boom");
        assert_eq!(panic_text(a.as_ref()), "handler panicked: boom");
        let b: Box<dyn std::any::Any + Send> = Box::new(String::from("bang"));
        assert_eq!(panic_text(b.as_ref()), "handler panicked: bang");
        let c: Box<dyn std::any::Any + Send> = Box::new(7_u8);
        assert_eq!(panic_text(c.as_ref()), "handler panicked: unknown");
    }
}
