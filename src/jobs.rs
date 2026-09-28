//! `#[job]` transport: enqueue to SQS, run the registered handler.

use std::collections::HashMap;
use std::hash::{BuildHasher as _, Hasher as _};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use autumn_web::AppState;
use autumn_web::job::JobInfo;
use autumn_web::reexports::chrono::{DateTime, Utc};
use autumn_web::time::ClockSource;
use serde::Serialize;
use serde_json::Value;

use crate::envelope::{ATTR_JOB, ATTR_KIND, JobEnvelope, KIND_JOB_V1};
use crate::error::SqsError;
use crate::policy::split_delay;
use crate::producer::SqsProducer;
use crate::transport::{BoxFuture, OutboundMessage, ReceivedMessage, is_fifo};
use crate::worker::{Dispatch, Outcome, RetryRule};

/// Options for [`SqsJobClient::enqueue_with`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnqueueOptions {
    /// Run after this delay. Standard queues only.
    pub delay: Option<Duration>,
    /// FIFO group ID. Default: the job name.
    pub group_id: Option<String>,
    /// FIFO deduplication ID. Default: a new unique ID.
    pub dedup_id: Option<String>,
}

impl EnqueueOptions {
    /// Sets the delay.
    #[must_use]
    pub const fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Sets the FIFO group ID.
    #[must_use]
    pub fn group_id(mut self, id: impl Into<String>) -> Self {
        self.group_id = Some(id.into());
        self
    }

    /// Sets the FIFO deduplication ID.
    #[must_use]
    pub fn dedup_id(mut self, id: impl Into<String>) -> Self {
        self.dedup_id = Some(id.into());
        self
    }
}

/// One registered job and its queue.
#[derive(Clone)]
pub(crate) struct JobRoute {
    pub info: JobInfo,
    /// Queue URL.
    pub url: String,
    /// Metrics label: the queue alias.
    pub label: String,
    pub rule: RetryRule,
}

pub(crate) struct JobsShared {
    pub routes: HashMap<String, JobRoute>,
    pub producer: SqsProducer,
    pub clock: Arc<dyn ClockSource>,
}

/// Sends `#[job]` work to SQS. Get it with [`SqsJobClient::from_state`].
///
/// Pass the job name. The `#[job]` macro makes a `NAME` constant:
/// `client.enqueue(SendEmailJob::NAME, &args)`.
#[derive(Clone)]
pub struct SqsJobClient {
    shared: Arc<JobsShared>,
}

impl std::fmt::Debug for SqsJobClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsJobClient").finish_non_exhaustive()
    }
}

static DEDUP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique ID for FIFO deduplication.
fn unique_id(now: DateTime<Utc>) -> String {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(DEDUP_COUNTER.fetch_add(1, Ordering::Relaxed));
    format!(
        "{:x}-{:x}-{:016x}",
        now.timestamp_micros(),
        DEDUP_COUNTER.load(Ordering::Relaxed),
        h.finish()
    )
}

/// Whole seconds, rounded up.
fn ceil_secs(d: Duration) -> u64 {
    d.as_secs() + u64::from(d.subsec_nanos() > 0)
}

impl SqsJobClient {
    pub(crate) const fn new(shared: Arc<JobsShared>) -> Self {
        Self { shared }
    }

    /// Returns the client that the plugin put in the app state.
    ///
    /// # Errors
    /// Returns [`SqsError::NotStarted`] before the plugin starts.
    pub fn from_state(state: &AppState) -> Result<Self, SqsError> {
        state
            .extension::<Self>()
            .map(|c| (*c).clone())
            .ok_or(SqsError::NotStarted)
    }

    /// Returns the queue URL for a job.
    ///
    /// # Errors
    /// Returns [`SqsError::UnknownJob`] for a job that is not registered.
    pub fn queue_url(&self, job: &str) -> Result<String, SqsError> {
        self.route(job).map(|r| r.url.clone())
    }

    fn route(&self, job: &str) -> Result<&JobRoute, SqsError> {
        self.shared
            .routes
            .get(job)
            .ok_or_else(|| SqsError::UnknownJob(job.to_owned()))
    }

    /// Sends a job to run now. Returns the SQS message ID.
    ///
    /// # Errors
    /// Returns [`SqsError::UnknownJob`], a JSON error, or a send error.
    pub async fn enqueue<A: Serialize + Sync>(
        &self,
        job: &str,
        args: &A,
    ) -> Result<String, SqsError> {
        self.enqueue_with(job, args, EnqueueOptions::default())
            .await
    }

    /// Sends a job to run after `delay`.
    ///
    /// # Errors
    /// Also returns [`SqsError::FifoDelay`] for a FIFO queue.
    pub async fn enqueue_in<A: Serialize + Sync>(
        &self,
        job: &str,
        args: &A,
        delay: Duration,
    ) -> Result<String, SqsError> {
        self.enqueue_with(job, args, EnqueueOptions::default().delay(delay))
            .await
    }

    /// Sends a job to run at `when`. A past time runs now.
    ///
    /// # Errors
    /// Also returns [`SqsError::FifoDelay`] for a FIFO queue and a future time.
    pub async fn enqueue_at<A: Serialize + Sync>(
        &self,
        job: &str,
        args: &A,
        when: DateTime<Utc>,
    ) -> Result<String, SqsError> {
        let delay = (when - self.shared.clock.now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        self.enqueue_in(job, args, delay).await
    }

    /// Sends a job with options.
    ///
    /// # Errors
    /// Returns [`SqsError::UnknownJob`], [`SqsError::FifoDelay`], a JSON
    /// error, or a send error.
    pub async fn enqueue_with<A: Serialize + Sync>(
        &self,
        job: &str,
        args: &A,
        options: EnqueueOptions,
    ) -> Result<String, SqsError> {
        let payload = serde_json::to_value(args)?;
        self.enqueue_value(job, payload, options).await
    }

    /// Sends a job with a JSON payload.
    ///
    /// # Errors
    /// Same as [`Self::enqueue_with`].
    pub async fn enqueue_value(
        &self,
        job: &str,
        payload: Value,
        options: EnqueueOptions,
    ) -> Result<String, SqsError> {
        let route = self.route(job)?;
        // Match the #[job] enqueue path: wrap only versioned jobs.
        let payload = if route.info.version > 1 {
            autumn_web::payload_version::wrap(route.info.version, payload)
        } else {
            payload
        };
        let now = self.shared.clock.now();
        let delay = options.delay.map_or(0, ceil_secs);
        let fifo = is_fifo(&route.url);
        if fifo && delay > 0 {
            return Err(SqsError::FifoDelay(route.url.clone()));
        }
        let split = split_delay(delay);
        let not_before = (split.later_secs > 0).then(|| {
            now.timestamp()
                .saturating_add(i64::try_from(delay).unwrap_or(i64::MAX))
        });
        let envelope = JobEnvelope {
            v: 1,
            job: job.to_owned(),
            payload,
            not_before,
            enqueued_at: now.timestamp(),
        };
        let message = job_message(&envelope, split.now_secs, fifo, &options, now)?;
        self.shared
            .producer
            .send_to_url(&route.label, &route.url, message)
            .await
    }
}

fn job_message(
    envelope: &JobEnvelope,
    delay_secs: u64,
    fifo: bool,
    options: &EnqueueOptions,
    now: DateTime<Utc>,
) -> Result<OutboundMessage, SqsError> {
    let mut m = OutboundMessage::new(serde_json::to_string(envelope)?)
        .delay_secs(delay_secs)
        .attribute(ATTR_JOB, envelope.job.clone())
        .attribute(ATTR_KIND, KIND_JOB_V1);
    if fifo {
        m.group_id = Some(
            options
                .group_id
                .clone()
                .unwrap_or_else(|| envelope.job.clone()),
        );
        m.dedup_id = Some(options.dedup_id.clone().unwrap_or_else(|| unique_id(now)));
    }
    Ok(m)
}

/// Runs job envelopes with the registered `#[job]` handler.
pub(crate) struct JobDispatcher {
    pub shared: Arc<JobsShared>,
}

impl Dispatch for JobDispatcher {
    fn dispatch(&self, state: AppState, message: ReceivedMessage) -> BoxFuture<'static, Outcome> {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move {
            let Some(envelope) = JobEnvelope::parse(&message.body) else {
                return Outcome::Poison("body is not a job envelope".to_owned());
            };
            let Some(route) = shared.routes.get(&envelope.job) else {
                return Outcome::Poison(format!("unknown job: {}", envelope.job));
            };
            let now = shared.clock.now();
            if let Some(due) = envelope.not_before
                && due > now.timestamp()
            {
                return hop(&shared, route, &envelope, due, now).await;
            }
            match (route.info.handler)(state, envelope.payload).await {
                Ok(()) => Outcome::Ack,
                Err(e) => Outcome::Retry {
                    error: e.to_string(),
                    rule: route.rule,
                },
            }
        })
    }
}

/// Sends the envelope again with the next delay step.
async fn hop(
    shared: &JobsShared,
    route: &JobRoute,
    envelope: &JobEnvelope,
    due: i64,
    now: DateTime<Utc>,
) -> Outcome {
    let remaining = u64::try_from(due.saturating_sub(now.timestamp())).unwrap_or(0);
    let split = split_delay(remaining);
    let options = EnqueueOptions::default();
    let message = match job_message(envelope, split.now_secs, false, &options, now) {
        Ok(m) => m,
        Err(e) => return Outcome::Poison(e.to_string()),
    };
    match shared.producer.transport().send(&route.url, message).await {
        Ok(_) => Outcome::Hopped,
        Err(e) => Outcome::Retry {
            error: format!("delay hop failed: {e}"),
            rule: route.rule,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceil_secs_rounds_up() {
        assert_eq!(ceil_secs(Duration::ZERO), 0);
        assert_eq!(ceil_secs(Duration::from_millis(1)), 1);
        assert_eq!(ceil_secs(Duration::from_secs(5)), 5);
        assert_eq!(ceil_secs(Duration::from_millis(5_001)), 6);
    }

    #[test]
    fn unique_ids_differ() {
        let now = Utc::now();
        assert_ne!(unique_id(now), unique_id(now));
    }

    #[test]
    fn options_builder() {
        let o = EnqueueOptions::default()
            .delay(Duration::from_secs(1))
            .group_id("g")
            .dedup_id("d");
        assert_eq!(o.delay, Some(Duration::from_secs(1)));
        assert_eq!(o.group_id.as_deref(), Some("g"));
        assert_eq!(o.dedup_id.as_deref(), Some("d"));
    }
}
