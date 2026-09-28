//! `AwsSqsPlugin` and the started runtime.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_web::actuator::{HealthIndicator, MetricsSource};
use autumn_web::app::AppBuilder;
use autumn_web::job::JobInfo;
use autumn_web::plugin::Plugin;
use autumn_web::reexports::tokio_util::sync::CancellationToken;
use autumn_web::time::{ClockSource, SystemClock};
use autumn_web::{AppState, AutumnError, ProcessRole};

use crate::config::{SECTION, SqsConfig};
use crate::consumer::SqsConsumer;
use crate::error::SqsError;
use crate::health::{HealthTarget, SqsHealth};
use crate::jobs::{JobDispatcher, JobRoute, JobsShared, SqsJobClient};
use crate::metrics::SqsMetrics;
use crate::producer::SqsProducer;
use crate::transport::{AwsSqsTransport, SqsTransport};
use crate::worker::{self, RetryRule, WorkerCtx, WorkerSpec};

/// Plugin name for duplicate detection.
pub const PLUGIN_NAME: &str = "autumn-plugin-aws-sqs";

/// Time limit for each queue check at startup. Startup does not wait longer.
const STARTUP_CHECK_TIMEOUT: Duration = Duration::from_secs(2);

/// How often the probe watcher reads the drain flag.
const PROBE_POLL: Duration = Duration::from_millis(250);

/// Amazon SQS plugin.
///
/// ```rust,ignore
/// autumn_web::app()
///     .plugin(
///         AwsSqsPlugin::new()
///             .jobs(jobs![send_welcome_email])
///             .consumer(SqsConsumer::new("uploads", "uploads", on_upload)),
///     )
///     .run()
///     .await;
/// ```
pub struct AwsSqsPlugin {
    config: Option<SqsConfig>,
    readiness: Option<bool>,
    jobs: Vec<JobInfo>,
    consumers: Vec<SqsConsumer>,
    transport: Option<Arc<dyn SqsTransport>>,
    clock: Option<Arc<dyn ClockSource>>,
}

impl std::fmt::Debug for AwsSqsPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsSqsPlugin")
            .field("config", &self.config)
            .field(
                "jobs",
                &self.jobs.iter().map(|j| &j.name).collect::<Vec<_>>(),
            )
            .field("consumers", &self.consumers)
            .finish_non_exhaustive()
    }
}

impl Default for AwsSqsPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl AwsSqsPlugin {
    /// Makes the plugin. It reads `[aws_sqs]` from `autumn.toml` at startup,
    /// with the app profile, `.env`, and `AUTUMN_AWS_SQS__*` env vars.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            config: None,
            readiness: None,
            jobs: Vec::new(),
            consumers: Vec::new(),
            transport: None,
            clock: None,
        }
    }

    /// Makes the plugin with this config. It reads no files.
    #[must_use]
    pub fn with_config(config: SqsConfig) -> Self {
        Self {
            config: Some(config),
            ..Self::new()
        }
    }

    /// Adds `#[job]` handlers. Pass `jobs![...]`.
    #[must_use]
    pub fn jobs(mut self, jobs: Vec<JobInfo>) -> Self {
        self.jobs.extend(jobs);
        self
    }

    /// Adds a consumer.
    #[must_use]
    pub fn consumer(mut self, consumer: SqsConsumer) -> Self {
        self.consumers.push(consumer);
        self
    }

    /// Uses this transport. Default: [`AwsSqsTransport`] from the config.
    #[must_use]
    pub fn with_transport(self, transport: impl SqsTransport) -> Self {
        self.with_transport_arc(Arc::new(transport))
    }

    /// Uses this shared transport.
    #[must_use]
    pub fn with_transport_arc(mut self, transport: Arc<dyn SqsTransport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Uses this clock for delays. Default: the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn ClockSource>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Puts the health indicator in `/ready` too. Default: `/health` only.
    ///
    /// autumn reads this when the app builds, before the config loads. So it
    /// is a builder setting, not a config key.
    #[must_use]
    pub const fn readiness(mut self, on: bool) -> Self {
        self.readiness = Some(on);
        self
    }

    fn readiness_flag(&self) -> bool {
        self.readiness.unwrap_or(false)
    }

    /// Starts the plugin outside an app build, with the role of `state`.
    ///
    /// Use it in tests. [`Plugin::build`] does the same at app startup.
    ///
    /// # Errors
    /// Returns a config, queue, or transport error.
    pub async fn start(self, state: &AppState) -> Result<SqsRuntime, SqsError> {
        self.start_with_role(state, state.role()).await
    }

    /// Starts the plugin with no workers, like the `web` role.
    ///
    /// # Errors
    /// Same as [`Self::start`].
    pub async fn start_without_workers(self, state: &AppState) -> Result<SqsRuntime, SqsError> {
        self.start_with_role(state, ProcessRole::Web).await
    }

    /// Starts the plugin as if the process has `role`.
    ///
    /// # Errors
    /// Same as [`Self::start`].
    pub async fn start_with_role(
        self,
        state: &AppState,
        role: ProcessRole,
    ) -> Result<SqsRuntime, SqsError> {
        let health = Arc::new(SqsHealth::new(self.readiness_flag()));
        self.start_inner(state, role, Arc::new(SqsMetrics::new()), health)
            .await
    }

    #[allow(clippy::too_many_lines)] // One linear setup sequence.
    async fn start_inner(
        self,
        state: &AppState,
        role: ProcessRole,
        metrics: Arc<SqsMetrics>,
        health: Arc<SqsHealth>,
    ) -> Result<SqsRuntime, SqsError> {
        let config = match self.config {
            Some(c) => c,
            // autumn sets the state profile from the config; "default" means none.
            None => SqsConfig::load(Some(state.profile()).filter(|p| *p != "default"))?,
        };
        let interceptor = state
            .extension::<Arc<dyn autumn_web::interceptor::JobInterceptor>>()
            .map(|i| Arc::clone(&*i));
        config.validate()?;
        let transport: Arc<dyn SqsTransport> = match self.transport {
            Some(t) => t,
            None => Arc::new(AwsSqsTransport::from_config(&config).await?),
        };
        let clock = self.clock.unwrap_or_else(|| Arc::new(SystemClock));
        let app_jobs = state.config().jobs;
        let dead_letter_url = config
            .jobs
            .dead_letter_queue
            .as_deref()
            .map(|q| config.resolve_queue(q))
            .transpose()?;

        // Job routes.
        let mut routes: HashMap<String, JobRoute> = HashMap::new();
        if !self.jobs.is_empty() {
            let default_label = config.jobs.default_queue.clone();
            let default_url = config.resolve_queue(&default_label).map_err(|_| {
                SqsError::Config(format!(
                    "jobs need a queue: set [aws_sqs.queues] {default_label} = \"https://...\""
                ))
            })?;
            for info in self.jobs {
                if routes.contains_key(&info.name) {
                    return Err(SqsError::Config(format!(
                        "job {} is registered twice",
                        info.name
                    )));
                }
                let route = job_route(&config, info, &default_label, &default_url, &app_jobs);
                routes.insert(route.info.name.clone(), route);
            }
        }

        // Worker specs: one per job queue URL, one per consumer.
        let producer =
            SqsProducer::new(Arc::clone(&transport), config.clone(), Arc::clone(&metrics));
        let shared = Arc::new(JobsShared {
            routes,
            producer: producer.clone(),
            clock,
            interceptor,
            default_rule: RetryRule {
                max_attempts: app_jobs.max_attempts.max(1),
                initial_backoff_ms: app_jobs.initial_backoff_ms,
            },
        });
        let mut specs: Vec<WorkerSpec> = Vec::new();
        let mut by_url: BTreeMap<String, String> = BTreeMap::new();
        let mut job_urls: BTreeMap<String, String> = BTreeMap::new();
        for route in shared.routes.values() {
            job_urls
                .entry(route.url.clone())
                .or_insert_with(|| route.label.clone());
        }
        for (url, label) in job_urls {
            check_not_dlq(&url, dead_letter_url.as_deref(), &label)?;
            by_url.insert(url.clone(), format!("jobs ({label})"));
            specs.push(WorkerSpec {
                label,
                queue_url: url.clone(),
                dispatch: Arc::new(JobDispatcher {
                    shared: Arc::clone(&shared),
                    queue_url: url,
                }),
                dead_letter_url: dead_letter_url.clone(),
            });
        }
        let mut names = std::collections::HashSet::new();
        for consumer in self.consumers {
            if !names.insert(consumer.name.clone()) {
                return Err(SqsError::Config(format!(
                    "consumer {} is registered twice",
                    consumer.name
                )));
            }
            let url = config.resolve_queue(&consumer.queue)?;
            check_not_dlq(&url, dead_letter_url.as_deref(), &consumer.name)?;
            if let Some(owner) = by_url.get(&url) {
                return Err(SqsError::Config(format!(
                    "consumer {} shares queue {url} with {owner}; use one handler per queue",
                    consumer.name
                )));
            }
            by_url.insert(url.clone(), format!("consumer {}", consumer.name));
            specs.push(WorkerSpec {
                label: consumer.name.clone(),
                queue_url: url,
                dispatch: Arc::new(consumer),
                dead_letter_url: dead_letter_url.clone(),
            });
        }

        // Queues to check and sample: all aliases plus used URLs.
        let mut watched: BTreeMap<String, String> = config.queues.clone();
        for spec in &specs {
            if !watched.values().any(|u| u == &spec.queue_url) {
                // Keys must be unique: two raw URLs share the label "unconfigured".
                let mut key = spec.label.clone();
                let mut n = 2;
                while watched.contains_key(&key) {
                    key = format!("{}-{n}", spec.label);
                    n += 1;
                }
                watched.insert(key, spec.queue_url.clone());
            }
        }

        state.insert_extension(producer);
        state.insert_extension(SqsJobClient::new(Arc::clone(&shared)));
        health.set(HealthTarget {
            transport: Arc::clone(&transport),
            queues: watched.clone(),
        });

        let cancel = CancellationToken::new();
        let running = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        handles.push(tokio::spawn(watch_probes(state.clone(), cancel.clone())));
        if config.worker.stats_interval_secs > 0 {
            handles.push(tokio::spawn(sample_stats(
                Arc::clone(&transport),
                Arc::clone(&metrics),
                watched,
                Duration::from_secs(config.worker.stats_interval_secs),
                cancel.clone(),
            )));
        }
        let workers = if role.runs_workers() { specs.len() } else { 0 };
        if role.runs_workers() {
            if dead_letter_url.is_none() {
                warn_without_dead_letter_path(transport.as_ref(), &specs).await;
            }
            let ctx = WorkerCtx {
                transport,
                config: config.worker.clone(),
                metrics: Arc::clone(&metrics),
                state: state.clone(),
                cancel: cancel.clone(),
                running: Arc::clone(&running),
            };
            for spec in specs {
                // Count before spawn, so `workers_running` is exact after start.
                running.fetch_add(1, Ordering::SeqCst);
                let ctx = ctx.clone();
                handles.push(tokio::spawn(async move {
                    worker::run(ctx, spec).await;
                }));
            }
        }
        tracing::info!(
            role = ?role,
            workers,
            jobs = shared.routes.len(),
            "aws_sqs started"
        );
        Ok(SqsRuntime {
            cancel,
            handles,
            running,
            metrics,
            health,
        })
    }
}

/// Resolves the queue and retry rule for one job.
fn job_route(
    config: &SqsConfig,
    info: JobInfo,
    default_label: &str,
    default_url: &str,
    app_jobs: &autumn_web::config::JobConfig,
) -> JobRoute {
    let (label, url) = config.resolve_queue(&info.queue).map_or_else(
        |_| {
            tracing::warn!(
                job = %info.name,
                queue = %info.queue,
                "aws_sqs: no queue alias for this job queue; using the default queue"
            );
            (config.label_for(default_label), default_url.to_owned())
        },
        |url| (config.label_for(&info.queue), url),
    );
    if info.uniqueness.is_some() || info.concurrency.is_some() {
        tracing::warn!(
            job = %info.name,
            "aws_sqs does not apply #[job(unique)] or #[job(concurrency)]; use a FIFO dedup_id"
        );
    }
    // Zero means "not set" in `#[job]`: use the app `[jobs]` defaults.
    let rule = RetryRule {
        max_attempts: if info.max_attempts == 0 {
            app_jobs.max_attempts.max(1)
        } else {
            info.max_attempts
        },
        initial_backoff_ms: if info.initial_backoff_ms == 0 {
            app_jobs.initial_backoff_ms
        } else {
            info.initial_backoff_ms
        },
    };
    JobRoute {
        info,
        url,
        label,
        rule,
    }
}

/// Warns for each worker queue that has no SQS redrive policy. With no DLQ
/// either, a failed message stays in the queue until its retention ends.
async fn warn_without_dead_letter_path(transport: &dyn SqsTransport, specs: &[WorkerSpec]) {
    let checks = specs.iter().map(|spec| async move {
        let stats = tokio::time::timeout(
            STARTUP_CHECK_TIMEOUT,
            transport.queue_stats(&spec.queue_url),
        )
        .await;
        (spec, stats)
    });
    for (spec, stats) in futures::future::join_all(checks).await {
        if let Ok(Ok(stats)) = stats
            && stats.redrive_target.is_none()
        {
            tracing::warn!(
                queue = %spec.label,
                "aws_sqs: no dead_letter_queue and no redrive policy; failed messages stay \
                 until the queue retention ends"
            );
        }
    }
}

fn check_not_dlq(url: &str, dlq: Option<&str>, owner: &str) -> Result<(), SqsError> {
    if dlq == Some(url) {
        return Err(SqsError::Config(format!(
            "{owner} reads the dead-letter queue {url}; dead letters would loop"
        )));
    }
    Ok(())
}

/// Cancels the runtime when the app starts to drain (`/ready` is 503).
async fn watch_probes(state: AppState, cancel: CancellationToken) {
    loop {
        if state.probes().is_shutting_down() {
            cancel.cancel();
            return;
        }
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(PROBE_POLL) => {}
        }
    }
}

async fn sample_stats(
    transport: Arc<dyn SqsTransport>,
    metrics: Arc<SqsMetrics>,
    queues: BTreeMap<String, String>,
    every: Duration,
    cancel: CancellationToken,
) {
    loop {
        for (label, url) in &queues {
            match transport.queue_stats(url).await {
                Ok(s) => metrics.update(label, |c| c.stats = Some(s)),
                Err(e) => tracing::debug!(queue = %label, error = %e, "aws_sqs stats failed"),
            }
        }
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(every) => {}
        }
    }
}

/// A started plugin. Call [`SqsRuntime::shutdown`] to drain.
pub struct SqsRuntime {
    cancel: CancellationToken,
    handles: Vec<tokio::task::JoinHandle<()>>,
    running: Arc<AtomicUsize>,
    metrics: Arc<SqsMetrics>,
    health: Arc<SqsHealth>,
}

impl std::fmt::Debug for SqsRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsRuntime")
            .field("workers_running", &self.workers_running())
            .finish_non_exhaustive()
    }
}

impl SqsRuntime {
    /// Returns the metrics.
    #[must_use]
    pub fn metrics(&self) -> Arc<SqsMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Returns the health indicator.
    #[must_use]
    pub fn health(&self) -> Arc<SqsHealth> {
        Arc::clone(&self.health)
    }

    /// Returns the number of worker loops that run now.
    #[must_use]
    pub fn workers_running(&self) -> usize {
        self.running.load(Ordering::SeqCst)
    }

    /// Returns the live worker count, for checks after [`Self::shutdown`].
    #[must_use]
    pub fn running_handle(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.running)
    }

    /// Stops receive, waits for in-flight handlers, and stops all tasks.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        for handle in self.handles {
            if let Err(e) = handle.await {
                tracing::warn!(error = %e, "aws_sqs task ended with an error");
            }
        }
    }
}

impl Plugin for AwsSqsPlugin {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed(PLUGIN_NAME)
    }

    fn build(self, app: AppBuilder) -> AppBuilder {
        let metrics = Arc::new(SqsMetrics::new());
        let health = Arc::new(SqsHealth::new(self.readiness_flag()));
        let pending = Arc::new(Mutex::new(Some(self)));
        let started: Arc<Mutex<Option<SqsRuntime>>> = Arc::default();
        let (m, h, s) = (
            Arc::clone(&metrics),
            Arc::clone(&health),
            Arc::clone(&started),
        );
        let stop = Arc::clone(&started);
        app.config_section(SECTION)
            .metrics_source("aws_sqs", metrics as Arc<dyn MetricsSource>)
            .health_indicator("aws_sqs", health as Arc<dyn HealthIndicator>)
            .on_startup(move |state| {
                let plugin = pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                let (m, h, s) = (Arc::clone(&m), Arc::clone(&h), Arc::clone(&s));
                async move {
                    let Some(plugin) = plugin else {
                        return Ok(());
                    };
                    let role = state.role();
                    let rt = plugin
                        .start_inner(&state, role, m, h)
                        .await
                        .map_err(AutumnError::internal_server_error)?;
                    *s.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(rt);
                    Ok(())
                }
            })
            .on_shutdown(move || {
                let rt = stop
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                async move {
                    if let Some(rt) = rt {
                        rt.shutdown().await;
                    }
                }
            })
    }
}
