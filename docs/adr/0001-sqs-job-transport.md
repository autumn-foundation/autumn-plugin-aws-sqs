# ADR 0001: SQS job transport reuses `#[job]` metadata

- Status: Accepted
- Date: 2026-09-28

## Context

autumn-web 0.7 has three job backends: `local`, `postgres`, and `redis`.
The backend is a closed set. There is no trait for a new backend.
We want `#[job]` handlers to run from SQS.

## Options

1. **Reuse `JobInfo` from `jobs![]`.** The plugin keeps its own job registry.
   `SqsJobClient` sends. Workers call `JobInfo::handler`.
2. **Route with `JobInterceptor::intercept_enqueue`.** Send to SQS and skip `next`.
3. **Fork or patch autumn-web.**

## Decision

Option 1.

## Reasons

- Option 1 needs no upstream change. It keeps the `#[job]` function, name,
  retry settings, queue, and payload version.
- `JobContext::current()` gives a no-op context outside the autumn runtime,
  so a direct `handler` call is safe.
- Option 2 loses the due time. `intercept_enqueue` gets only the name and the
  payload. A delayed job would run at once. It also takes the single
  interceptor slot from the app.
- Option 3 is a fork to maintain.

## Results

- Good: all `#[job]` handlers work on SQS with no change to the handler.
- Bad: callers use `SqsJobClient::enqueue(XJob::NAME, &args)`, not
  `XJob::enqueue(args)`.
- Bad: `unique`, `concurrency`, tracked jobs, and the admin jobs page do not
  apply to SQS jobs.

## Future: upstream seam

If autumn-web adds a job backend trait, this plugin can become a backend.
Then `XJob::enqueue`, tracked jobs, and the admin page work with SQS.
Proposed shape:

```rust
pub trait JobBackend: Send + Sync + 'static {
    /// Store a job. `due_at` is `None` for "now".
    fn enqueue(&self, job: EnqueueRequest) -> BoxFuture<'_, AutumnResult<EnqueueOutcome>>;
    /// Claim up to `max` due jobs from `queues`.
    fn claim(&self, queues: &[String], max: usize) -> BoxFuture<'_, AutumnResult<Vec<ClaimedJob>>>;
    /// Settle a claimed job.
    fn complete(&self, claim: &ClaimedJob) -> BoxFuture<'_, AutumnResult<()>>;
    fn retry(&self, claim: &ClaimedJob, after: Duration, error: &str) -> BoxFuture<'_, AutumnResult<()>>;
    fn dead_letter(&self, claim: &ClaimedJob, error: &str) -> BoxFuture<'_, AutumnResult<()>>;
    /// Keep a long job claimed.
    fn extend(&self, claim: &ClaimedJob, by: Duration) -> BoxFuture<'_, AutumnResult<()>>;
}

pub struct EnqueueRequest {
    pub id: String,
    pub name: String,
    pub queue: String,
    pub payload: serde_json::Value,
    pub due_at: Option<DateTime<Utc>>,
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
}

// AppBuilder::with_job_backend(impl JobBackend), selected by `[jobs] backend = "custom"`.
```

The autumn runtime keeps retry math, uniqueness, tracking, and metrics.
The backend only stores, claims, and settles.
