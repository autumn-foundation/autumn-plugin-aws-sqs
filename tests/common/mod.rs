//! Shared test helpers.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use autumn_plugin_aws_sqs::SqsConfig;

pub const JOBS: &str = "https://sqs.local/000/jobs";
pub const CRITICAL: &str = "https://sqs.local/000/critical";
pub const DLQ: &str = "https://sqs.local/000/dlq";
pub const EVENTS: &str = "https://sqs.local/000/events";
pub const FIFO: &str = "https://sqs.local/000/jobs.fifo";

/// Config with the test queues and short timeouts.
pub fn config() -> SqsConfig {
    let mut cfg = SqsConfig::default();
    cfg.queues.insert("default".into(), JOBS.into());
    cfg.queues.insert("critical".into(), CRITICAL.into());
    cfg.queues.insert("dlq".into(), DLQ.into());
    cfg.queues.insert("events".into(), EVENTS.into());
    cfg.jobs.dead_letter_queue = Some("dlq".into());
    cfg.worker.wait_time_secs = 1;
    cfg.worker.visibility_timeout_secs = 10;
    cfg.worker.max_backoff_secs = 60;
    cfg.worker.drain_timeout_secs = 5;
    cfg.worker.stats_interval_secs = 0;
    cfg
}

/// Waits (virtual time) until `cond` is true. Panics after `limit`.
pub async fn wait_until(limit: Duration, mut cond: impl FnMut() -> bool) {
    let start = tokio::time::Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < limit,
            "condition not met within {limit:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wall clock that moves with `tokio::time`, so paused tests can skip ahead.
pub struct TokioClock {
    base: autumn_web::reexports::chrono::DateTime<autumn_web::reexports::chrono::Utc>,
    start: tokio::time::Instant,
}

impl autumn_web::time::ClockSource for TokioClock {
    fn now(&self) -> autumn_web::reexports::chrono::DateTime<autumn_web::reexports::chrono::Utc> {
        self.base
            + autumn_web::reexports::chrono::TimeDelta::from_std(self.start.elapsed())
                .expect("elapsed fits")
    }
}

/// A [`TokioClock`] that starts now.
pub fn clock() -> std::sync::Arc<dyn autumn_web::time::ClockSource> {
    std::sync::Arc::new(TokioClock {
        base: autumn_web::reexports::chrono::Utc::now(),
        start: tokio::time::Instant::now(),
    })
}

/// Faults to inject into [`FaultyTransport`].
#[derive(Default)]
pub struct Faults {
    /// Receive calls that fail before receive works again.
    pub receive_failures: u32,
    /// Sends to this URL fail.
    pub fail_send_to: Option<String>,
    /// Deletes fail.
    pub fail_delete: bool,
}

/// `ChangeMessageVisibility` calls, in order: (receipt, seconds, time).
pub type VisibilityLog = Vec<(String, u64, tokio::time::Instant)>;

/// A [`MemoryTransport`] with switchable failures and a call log.
#[derive(Clone, Default)]
pub struct FaultyTransport {
    pub inner: autumn_plugin_aws_sqs::transport::MemoryTransport,
    pub faults: std::sync::Arc<std::sync::Mutex<Faults>>,
    pub visibility: std::sync::Arc<std::sync::Mutex<VisibilityLog>>,
}

impl FaultyTransport {
    pub fn new(inner: autumn_plugin_aws_sqs::transport::MemoryTransport) -> Self {
        Self {
            inner,
            ..Self::default()
        }
    }

    pub fn faults(&self) -> std::sync::MutexGuard<'_, Faults> {
        self.faults.lock().unwrap()
    }
}

fn injected() -> autumn_plugin_aws_sqs::SqsError {
    autumn_plugin_aws_sqs::SqsError::Service("injected fault".into())
}

impl autumn_plugin_aws_sqs::transport::SqsTransport for FaultyTransport {
    fn send<'a>(
        &'a self,
        queue_url: &'a str,
        message: autumn_plugin_aws_sqs::transport::OutboundMessage,
    ) -> autumn_plugin_aws_sqs::transport::BoxFuture<
        'a,
        Result<String, autumn_plugin_aws_sqs::SqsError>,
    > {
        let fail = self.faults().fail_send_to.as_deref() == Some(queue_url);
        Box::pin(async move {
            if fail {
                return Err(injected());
            }
            self.inner.send(queue_url, message).await
        })
    }

    fn send_batch<'a>(
        &'a self,
        queue_url: &'a str,
        messages: Vec<autumn_plugin_aws_sqs::transport::OutboundMessage>,
    ) -> autumn_plugin_aws_sqs::transport::BoxFuture<
        'a,
        Result<
            Vec<autumn_plugin_aws_sqs::transport::BatchEntryResult>,
            autumn_plugin_aws_sqs::SqsError,
        >,
    > {
        self.inner.send_batch(queue_url, messages)
    }

    fn receive<'a>(
        &'a self,
        queue_url: &'a str,
        options: autumn_plugin_aws_sqs::transport::ReceiveOptions,
    ) -> autumn_plugin_aws_sqs::transport::BoxFuture<
        'a,
        Result<
            Vec<autumn_plugin_aws_sqs::transport::ReceivedMessage>,
            autumn_plugin_aws_sqs::SqsError,
        >,
    > {
        let fail = {
            let mut f = self.faults();
            if f.receive_failures > 0 {
                f.receive_failures -= 1;
                true
            } else {
                false
            }
        };
        Box::pin(async move {
            if fail {
                return Err(injected());
            }
            self.inner.receive(queue_url, options).await
        })
    }

    fn delete<'a>(
        &'a self,
        queue_url: &'a str,
        receipt_handle: &'a str,
    ) -> autumn_plugin_aws_sqs::transport::BoxFuture<'a, Result<(), autumn_plugin_aws_sqs::SqsError>>
    {
        let fail = self.faults().fail_delete;
        Box::pin(async move {
            if fail {
                return Err(injected());
            }
            self.inner.delete(queue_url, receipt_handle).await
        })
    }

    fn change_visibility<'a>(
        &'a self,
        queue_url: &'a str,
        receipt_handle: &'a str,
        visibility_secs: u64,
    ) -> autumn_plugin_aws_sqs::transport::BoxFuture<'a, Result<(), autumn_plugin_aws_sqs::SqsError>>
    {
        self.visibility.lock().unwrap().push((
            receipt_handle.to_owned(),
            visibility_secs,
            tokio::time::Instant::now(),
        ));
        self.inner
            .change_visibility(queue_url, receipt_handle, visibility_secs)
    }

    fn queue_stats<'a>(
        &'a self,
        queue_url: &'a str,
    ) -> autumn_plugin_aws_sqs::transport::BoxFuture<
        'a,
        Result<autumn_plugin_aws_sqs::transport::QueueStats, autumn_plugin_aws_sqs::SqsError>,
    > {
        self.inner.queue_stats(queue_url)
    }
}
