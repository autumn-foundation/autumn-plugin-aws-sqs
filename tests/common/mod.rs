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
