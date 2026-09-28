//! `#[job]` transport: enqueue to SQS, run the registered handler.

use std::collections::HashMap;
use std::hash::{BuildHasher as _, Hasher as _};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use autumn_web::AppState;
use autumn_web::interceptor::JobInterceptor;
use autumn_web::job::JobInfo;
use autumn_web::reexports::chrono::{DateTime, Utc};
use autumn_web::time::ClockSource;
use serde::Serialize;
use serde_json::Value;
use tracing::Instrument as _;

use crate::envelope::{ATTR_JOB, ATTR_KIND, JobEnvelope, KIND_JOB_V1};
use crate::error::SqsError;
use crate::policy::split_delay;
use crate::producer::SqsProducer;
use crate::transport::{BoxFuture, OutboundMessage, ReceivedMessage, is_fifo};
use crate::worker::{Dispatch, Outcome, RetryRule};

/// Prefix of the `#[job]` macro error for args that do not decode.
const ARGS_DECODE_ERROR: &str = "job args deserialization failed";
/// Text in every autumn-web payload version error.
const PAYLOAD_VERSION_TEXT: &str = "stored payload version";
/// Text of the autumn-web error for a payload from a newer version.
const NEWER_VERSION_TEXT: &str = "is newer than expected";

/// Longest job delay: 366 days. A longer `not_before` is poison.
pub const MAX_JOB_DELAY_SECS: u64 = 366 * 24 * 60 * 60;

/// Options for [`SqsJobClient::enqueue_with`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
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
    /// The app `JobInterceptor`, from `AppBuilder::with_job_interceptor`.
    pub interceptor: Option<Arc<dyn JobInterceptor>>,
    /// Rule for a message that names no known job.
    pub default_rule: RetryRule,
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
        if delay > MAX_JOB_DELAY_SECS {
            return Err(SqsError::InvalidRequest(format!(
                "delay {delay} s is over {MAX_JOB_DELAY_SECS} s"
            )));
        }
        let fifo = is_fifo(&route.url);
        if fifo && delay > 0 {
            return Err(SqsError::FifoDelay(route.url.clone()));
        }
        let split = split_delay(delay);
        let not_before = (split.later_secs > 0).then(|| {
            now.timestamp()
                .saturating_add(i64::try_from(delay).unwrap_or(i64::MAX))
        });
        let seen = self.shared.interceptor.as_ref().map(|_| payload.clone());
        let envelope = JobEnvelope {
            v: 1,
            job: job.to_owned(),
            payload,
            not_before,
            enqueued_at: now.timestamp(),
        };
        let message = job_message(&envelope, split.now_secs, fifo, &options, now)?;
        let send = self
            .shared
            .producer
            .send_to_url(&route.label, &route.url, message);
        match (&self.shared.interceptor, seen) {
            (Some(interceptor), Some(payload)) => {
                intercepted_send(interceptor.as_ref(), job, &payload, send).await
            }
            _ => send.await,
        }
    }
}

/// Runs `send` inside `JobInterceptor::intercept_enqueue`.
async fn intercepted_send(
    interceptor: &dyn JobInterceptor,
    job: &str,
    payload: &Value,
    send: impl std::future::Future<Output = Result<String, SqsError>> + Send,
) -> Result<String, SqsError> {
    let slot: Arc<std::sync::Mutex<Option<Result<String, SqsError>>>> = Arc::default();
    let out = Arc::clone(&slot);
    let next = Box::pin(async move {
        let result = send.await;
        let status = result
            .as_ref()
            .map(|_| ())
            .map_err(|e| autumn_web::AutumnError::service_unavailable(e.clone()));
        *out.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
        status
    });
    let verdict = interceptor.intercept_enqueue(job, payload, next).await;
    let outcome = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    match (outcome, verdict) {
        (Some(Ok(id)), Ok(())) => Ok(id),
        (Some(Err(e)), _) => Err(e),
        (Some(Ok(_)) | None, Err(e)) => Err(SqsError::Intercepted(e.to_string())),
        (None, Ok(())) => Err(SqsError::Intercepted(
            "the interceptor did not send the job".to_owned(),
        )),
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

/// Runs job envelopes from one queue with the registered `#[job]` handler.
pub(crate) struct JobDispatcher {
    pub shared: Arc<JobsShared>,
    /// This queue. A job routed to another queue does not run here.
    pub queue_url: String,
}

impl Dispatch for JobDispatcher {
    fn rule(&self, message: &ReceivedMessage) -> RetryRule {
        JobEnvelope::parse(&message.body)
            .and_then(|e| self.shared.routes.get(&e.job).map(|r| r.rule))
            .unwrap_or(self.shared.default_rule)
    }

    fn dispatch(&self, state: AppState, message: ReceivedMessage) -> BoxFuture<'static, Outcome> {
        let shared = Arc::clone(&self.shared);
        let queue_url = self.queue_url.clone();
        Box::pin(async move {
            let Some(envelope) = JobEnvelope::parse(&message.body) else {
                return Outcome::Poison("body is not a job envelope".to_owned());
            };
            let Some(route) = shared.routes.get(&envelope.job) else {
                return Outcome::Poison(format!("unknown job: {}", envelope.job));
            };
            if route.url != queue_url {
                return Outcome::Poison(format!(
                    "job {} does not belong to this queue",
                    envelope.job
                ));
            }
            let now = shared.clock.now();
            if let Some(due) = envelope.not_before
                && due > now.timestamp()
            {
                let limit = i64::try_from(MAX_JOB_DELAY_SECS).unwrap_or(i64::MAX);
                if due.saturating_sub(now.timestamp()) > limit {
                    return Outcome::Poison(format!(
                        "not_before is more than {MAX_JOB_DELAY_SECS} s away"
                    ));
                }
                return hop(&shared, route, &envelope, due, now).await;
            }
            let span = tracing::info_span!(
                "job.execute",
                job = %envelope.job,
                transport = "sqs",
                message_id = %message.message_id
            );
            let result = execute(&shared, route, state, envelope.payload)
                .instrument(span)
                .await;
            match result {
                Ok(()) => Outcome::Ack,
                Err(e) => classify_handler_error(&e.to_string(), route.rule),
            }
        })
    }
}

/// Maps a handler error to an outcome.
///
/// Args that do not decode are poison: they do not get better on retry. The
/// `#[job]` macro puts the serde text, with input values, in these errors, so
/// the reason keeps only the text before it. A payload from a newer version
/// retries: a newer worker in a rolling deploy can run it.
fn classify_handler_error(message: &str, rule: RetryRule) -> Outcome {
    if message.contains(ARGS_DECODE_ERROR) {
        return Outcome::Poison(ARGS_DECODE_ERROR.to_owned());
    }
    if autumn_web::payload_version::is_payload_version_error(message)
        && !message.contains(NEWER_VERSION_TEXT)
    {
        // Text: `job "x": stored payload version N ...: <serde source>`.
        let head = message
            .find(PAYLOAD_VERSION_TEXT)
            .and_then(|at| message[at..].find(": ").map(|end| &message[..at + end]))
            .unwrap_or(message);
        return Outcome::Poison(head.to_owned());
    }
    Outcome::Retry {
        error: message.to_owned(),
        rule,
    }
}

/// Runs the handler inside the app `JobInterceptor`, if one is set.
async fn execute(
    shared: &JobsShared,
    route: &JobRoute,
    state: AppState,
    payload: Value,
) -> autumn_web::AutumnResult<()> {
    let handler = route.info.handler;
    match &shared.interceptor {
        Some(interceptor) => {
            let seen = payload.clone();
            let next = Box::pin(handler(state, payload));
            interceptor
                .intercept_execute(&route.info.name, &seen, next)
                .await
        }
        None => handler(state, payload).await,
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

    #[test]
    fn handler_errors_classify() {
        let rule = RetryRule {
            max_attempts: 3,
            initial_backoff_ms: 10,
        };
        assert_eq!(
            classify_handler_error("job args deserialization failed: invalid type: \"x\"", rule),
            Outcome::Poison(ARGS_DECODE_ERROR.to_owned())
        );
        let shape = "job \"j\": stored payload version 2 does not match the current args shape: \
                     invalid type: string \"secret\"";
        assert_eq!(
            classify_handler_error(shape, rule),
            Outcome::Poison(
                "job \"j\": stored payload version 2 does not match the current args shape"
                    .to_owned()
            )
        );
        let newer = "job \"j\": stored payload version 3 is newer than expected version 2; \
                     this worker cannot decode it";
        assert!(matches!(
            classify_handler_error(newer, rule),
            Outcome::Retry { .. }
        ));
        assert!(matches!(
            classify_handler_error("db down", rule),
            Outcome::Retry { .. }
        ));
    }
}
