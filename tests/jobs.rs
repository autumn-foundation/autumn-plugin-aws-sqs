//! `#[job]` transport over SQS (AC2, AC3, AC4, AC5, AC6).
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use autumn_plugin_aws_sqs::transport::{MemoryTransport, SqsTransport};
use autumn_plugin_aws_sqs::{AwsSqsPlugin, EnqueueOptions, SqsError, SqsJobClient};
use autumn_web::prelude::*;
use autumn_web::{AppState, jobs};
use common::{CRITICAL, DLQ, FIFO, JOBS, config, wait_until};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Args {
    pub n: u32,
}

static OK_SEEN: Mutex<Vec<u32>> = Mutex::new(Vec::new());

#[job(name = "ok_job")]
async fn ok_job(_state: AppState, args: Args) -> AutumnResult<()> {
    OK_SEEN.lock().unwrap().push(args.n);
    Ok(())
}

static FLAKY_CALLS: AtomicU32 = AtomicU32::new(0);

#[job(name = "flaky_job", max_attempts = 5, backoff_ms = 1000)]
async fn flaky_job(_state: AppState, _args: Args) -> AutumnResult<()> {
    let n = FLAKY_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    if n < 3 {
        return Err(AutumnError::internal_server_error_msg("flaky"));
    }
    Ok(())
}

static DOOMED_CALLS: AtomicU32 = AtomicU32::new(0);

#[job(name = "doomed_job", max_attempts = 2, backoff_ms = 1000)]
async fn doomed_job(_state: AppState, _args: Args) -> AutumnResult<()> {
    DOOMED_CALLS.fetch_add(1, Ordering::SeqCst);
    Err(AutumnError::internal_server_error_msg("always fails"))
}

#[job(name = "panic_job", max_attempts = 5)]
async fn panic_job(_state: AppState, _args: Args) -> AutumnResult<()> {
    panic!("boom");
}

static CRITICAL_SEEN: AtomicU32 = AtomicU32::new(0);

#[job(name = "critical_job", queue = "critical")]
async fn critical_job(_state: AppState, _args: Args) -> AutumnResult<()> {
    CRITICAL_SEEN.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

static UNMAPPED_SEEN: AtomicU32 = AtomicU32::new(0);

#[job(name = "unmapped_queue_job", queue = "no_such_alias")]
async fn unmapped_queue_job(_state: AppState, _args: Args) -> AutumnResult<()> {
    UNMAPPED_SEEN.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2Args {
    pub name: String,
}

static V2_SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());

#[job(name = "versioned_job", version = 2)]
async fn versioned_job(_state: AppState, args: V2Args) -> AutumnResult<()> {
    V2_SEEN.lock().unwrap().push(args.name);
    Ok(())
}

static SLOW_DONE: AtomicU32 = AtomicU32::new(0);

#[job(name = "slow_job")]
async fn slow_job(_state: AppState, _args: Args) -> AutumnResult<()> {
    tokio::time::sleep(Duration::from_secs(35)).await;
    SLOW_DONE.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

static DELAYED_SEEN: AtomicU32 = AtomicU32::new(0);

#[job(name = "delayed_job")]
async fn delayed_job(_state: AppState, _args: Args) -> AutumnResult<()> {
    DELAYED_SEEN.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn transport() -> MemoryTransport {
    MemoryTransport::new()
        .with_queue(JOBS)
        .with_queue(CRITICAL)
        .with_queue(DLQ)
        .with_queue(FIFO)
}

async fn start(
    t: &MemoryTransport,
    jobs: Vec<autumn_web::job::JobInfo>,
) -> (AppState, autumn_plugin_aws_sqs::SqsRuntime) {
    let state = AppState::for_test();
    let rt = AwsSqsPlugin::new(config())
        .with_transport(t.clone())
        .with_clock(common::clock())
        .jobs(jobs)
        .start(&state)
        .await
        .expect("runtime starts");
    (state, rt)
}

#[tokio::test(start_paused = true)]
async fn enqueue_runs_same_handler_and_deletes() {
    let t = transport();
    let (state, rt) = start(&t, jobs![ok_job]).await;
    let client = SqsJobClient::from_state(&state).unwrap();
    client
        .enqueue(OkJobJob::NAME, &Args { n: 7 })
        .await
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        OK_SEEN.lock().unwrap().contains(&7)
    })
    .await;
    wait_until(Duration::from_secs(10), || t.messages(JOBS).is_empty()).await;
    let m = rt.metrics().snapshot();
    assert!(m["default"].succeeded >= 1);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn enqueue_writes_job_envelope_and_attributes() {
    let t = transport();
    let state = AppState::for_test();
    // No workers: inspect the stored message.
    let rt = AwsSqsPlugin::new(config())
        .with_transport(t.clone())
        .with_clock(common::clock())
        .jobs(jobs![ok_job])
        .start_without_workers(&state)
        .await
        .unwrap();
    SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue("ok_job", &Args { n: 1 })
        .await
        .unwrap();
    let msgs = t.messages(JOBS);
    assert_eq!(msgs.len(), 1);
    let body: serde_json::Value = serde_json::from_str(&msgs[0].body).unwrap();
    assert_eq!(body["job"], "ok_job");
    assert_eq!(body["payload"]["n"], 1);
    assert_eq!(msgs[0].attributes["autumn-job"], "ok_job");
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn unknown_job_name_is_rejected_at_enqueue() {
    let t = transport();
    let (state, rt) = start(&t, jobs![ok_job]).await;
    let err = SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue("typo_job", &Args { n: 1 })
        .await
        .unwrap_err();
    assert_eq!(err, SqsError::UnknownJob("typo_job".into()));
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn client_before_start_is_not_started() {
    let state = AppState::for_test();
    assert!(matches!(
        SqsJobClient::from_state(&state),
        Err(SqsError::NotStarted)
    ));
}

#[tokio::test(start_paused = true)]
async fn failure_retries_with_backoff_then_succeeds() {
    let t = transport();
    let (state, rt) = start(&t, jobs![flaky_job]).await;
    SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue("flaky_job", &Args { n: 1 })
        .await
        .unwrap();
    wait_until(Duration::from_secs(60), || {
        FLAKY_CALLS.load(Ordering::SeqCst) >= 3
    })
    .await;
    wait_until(Duration::from_secs(10), || t.messages(JOBS).is_empty()).await;
    let m = rt.metrics().snapshot();
    assert_eq!(m["default"].retried, 2);
    assert!(t.messages(DLQ).is_empty());
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn exhausted_job_goes_to_dead_letter_queue() {
    let t = transport();
    let (state, rt) = start(&t, jobs![doomed_job]).await;
    SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue("doomed_job", &Args { n: 9 })
        .await
        .unwrap();
    wait_until(Duration::from_secs(60), || t.messages(DLQ).len() == 1).await;
    assert_eq!(DOOMED_CALLS.load(Ordering::SeqCst), 2);
    assert!(t.messages(JOBS).is_empty());
    let dead = &t.messages(DLQ)[0];
    assert!(dead.attributes["autumn-dead-letter-reason"].contains("always fails"));
    assert_eq!(dead.attributes["autumn-attempts"], "2");
    assert_eq!(rt.metrics().snapshot()["default"].dead_lettered, 1);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn panic_is_dead_lettered_at_once() {
    let t = transport();
    let (state, rt) = start(&t, jobs![panic_job]).await;
    SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue("panic_job", &Args { n: 1 })
        .await
        .unwrap();
    wait_until(Duration::from_secs(10), || t.messages(DLQ).len() == 1).await;
    assert!(t.messages(DLQ)[0].attributes["autumn-dead-letter-reason"].contains("panic"));
    assert_eq!(t.messages(DLQ)[0].attributes["autumn-attempts"], "1");
    // The worker keeps running after a panic.
    assert_eq!(rt.workers_running(), 1);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn malformed_and_unknown_messages_are_poison() {
    let t = transport();
    let (_state, rt) = start(&t, jobs![ok_job]).await;
    t.send(
        JOBS,
        autumn_plugin_aws_sqs::transport::OutboundMessage::new("not json"),
    )
    .await
    .unwrap();
    t.send(
        JOBS,
        autumn_plugin_aws_sqs::transport::OutboundMessage::new(
            r#"{"v":1,"job":"ghost_job","payload":{},"enqueued_at":0}"#,
        ),
    )
    .await
    .unwrap();
    wait_until(Duration::from_secs(10), || t.messages(DLQ).len() == 2).await;
    let reasons: Vec<String> = t
        .messages(DLQ)
        .iter()
        .map(|m| m.attributes["autumn-dead-letter-reason"].clone())
        .collect();
    assert!(
        reasons.iter().any(|r| r.contains("ghost_job")),
        "{reasons:?}"
    );
    assert!(
        reasons.iter().any(|r| r.contains("envelope")),
        "{reasons:?}"
    );
    assert_eq!(rt.metrics().snapshot()["default"].poisoned, 2);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn queue_attribute_routes_to_alias_url() {
    let t = transport();
    let (state, rt) = start(&t, jobs![critical_job, unmapped_queue_job]).await;
    let client = SqsJobClient::from_state(&state).unwrap();
    assert_eq!(client.queue_url("critical_job").unwrap(), CRITICAL);
    // An unmapped queue falls back to the default queue.
    assert_eq!(client.queue_url("unmapped_queue_job").unwrap(), JOBS);
    client
        .enqueue("critical_job", &Args { n: 1 })
        .await
        .unwrap();
    client
        .enqueue("unmapped_queue_job", &Args { n: 1 })
        .await
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        CRITICAL_SEEN.load(Ordering::SeqCst) == 1 && UNMAPPED_SEEN.load(Ordering::SeqCst) == 1
    })
    .await;
    // One worker per distinct queue URL.
    assert_eq!(rt.workers_running(), 2);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn versioned_payload_is_wrapped_and_decoded() {
    let t = transport();
    let state = AppState::for_test();
    let rt = AwsSqsPlugin::new(config())
        .with_transport(t.clone())
        .with_clock(common::clock())
        .jobs(jobs![versioned_job])
        .start_without_workers(&state)
        .await
        .unwrap();
    SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue("versioned_job", &V2Args { name: "ada".into() })
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&t.messages(JOBS)[0].body).unwrap();
    assert_eq!(body["payload"]["__autumn_schema_version"], 2);
    rt.shutdown().await;

    let (_state, rt) = start(&t, jobs![versioned_job]).await;
    wait_until(Duration::from_secs(10), || {
        V2_SEEN.lock().unwrap().contains(&"ada".to_owned())
    })
    .await;
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn short_delay_uses_delay_seconds() {
    let t = transport();
    let state = AppState::for_test();
    let rt = AwsSqsPlugin::new(config())
        .with_transport(t.clone())
        .with_clock(common::clock())
        .jobs(jobs![delayed_job])
        .start_without_workers(&state)
        .await
        .unwrap();
    SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue_in("delayed_job", &Args { n: 1 }, Duration::from_secs(120))
        .await
        .unwrap();
    let depth = t.queue_stats(JOBS).await.unwrap();
    assert_eq!(depth.delayed, 1);
    let body: serde_json::Value = serde_json::from_str(&t.messages(JOBS)[0].body).unwrap();
    assert!(body.get("not_before").is_none());
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn long_delay_hops_until_due_without_using_attempts() {
    let t = transport();
    let (state, rt) = start(&t, jobs![delayed_job]).await;
    let client = SqsJobClient::from_state(&state).unwrap();
    client
        .enqueue_in("delayed_job", &Args { n: 1 }, Duration::from_secs(2_000))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&t.messages(JOBS)[0].body).unwrap();
    assert!(body["not_before"].as_i64().is_some());
    tokio::time::sleep(Duration::from_secs(1_000)).await;
    assert_eq!(DELAYED_SEEN.load(Ordering::SeqCst), 0);
    wait_until(Duration::from_secs(2_100), || {
        DELAYED_SEEN.load(Ordering::SeqCst) == 1
    })
    .await;
    let m = rt.metrics().snapshot();
    assert!(m["default"].hops >= 2, "{:?}", m["default"]);
    assert_eq!(m["default"].retried, 0);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn enqueue_at_past_runs_now() {
    let t = transport();
    let (state, rt) = start(&t, jobs![ok_job]).await;
    let past = autumn_web::reexports::chrono::Utc::now()
        - autumn_web::reexports::chrono::TimeDelta::hours(1);
    SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue_at("ok_job", &Args { n: 42 }, past)
        .await
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        OK_SEEN.lock().unwrap().contains(&42)
    })
    .await;
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn fifo_queue_rejects_delay_and_sets_group() {
    let t = transport();
    let mut cfg = config();
    cfg.queues.insert("default".into(), FIFO.into());
    let state = AppState::for_test();
    let rt = AwsSqsPlugin::new(cfg)
        .with_transport(t.clone())
        .with_clock(common::clock())
        .jobs(jobs![ok_job])
        .start_without_workers(&state)
        .await
        .unwrap();
    let client = SqsJobClient::from_state(&state).unwrap();
    let err = client
        .enqueue_in("ok_job", &Args { n: 1 }, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(matches!(err, SqsError::FifoDelay(_)));
    client.enqueue("ok_job", &Args { n: 1 }).await.unwrap();
    client
        .enqueue_with(
            "ok_job",
            &Args { n: 2 },
            EnqueueOptions::default()
                .group_id("tenant-9")
                .dedup_id("once"),
        )
        .await
        .unwrap();
    client
        .enqueue_with(
            "ok_job",
            &Args { n: 2 },
            EnqueueOptions::default()
                .group_id("tenant-9")
                .dedup_id("once"),
        )
        .await
        .unwrap();
    let msgs = t.messages(FIFO);
    assert_eq!(msgs.len(), 2, "dedup id drops the repeat");
    assert_eq!(msgs[0].group_id.as_deref(), Some("ok_job"));
    assert_eq!(msgs[1].group_id.as_deref(), Some("tenant-9"));
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn heartbeat_keeps_long_handler_invisible() {
    let t = transport();
    let (state, rt) = start(&t, jobs![slow_job]).await;
    SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue("slow_job", &Args { n: 1 })
        .await
        .unwrap();
    // Handler runs 35 s; visibility is 10 s. Without a heartbeat SQS
    // redelivers and the handler runs twice.
    wait_until(Duration::from_secs(60), || {
        SLOW_DONE.load(Ordering::SeqCst) == 1
    })
    .await;
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(SLOW_DONE.load(Ordering::SeqCst), 1);
    assert!(rt.metrics().snapshot()["default"].heartbeats >= 3);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn missing_default_queue_fails_start() {
    let t = transport();
    let mut cfg = config();
    cfg.queues.remove("default");
    let err = AwsSqsPlugin::new(cfg)
        .with_transport(t)
        .with_clock(common::clock())
        .jobs(jobs![ok_job])
        .start(&AppState::for_test())
        .await
        .unwrap_err();
    assert!(
        matches!(err, SqsError::Config(ref m) if m.contains("default")),
        "{err:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn duplicate_job_names_fail_start() {
    let t = transport();
    let err = AwsSqsPlugin::new(config())
        .with_transport(t)
        .with_clock(common::clock())
        .jobs(jobs![ok_job, ok_job])
        .start(&AppState::for_test())
        .await
        .unwrap_err();
    assert!(
        matches!(err, SqsError::Config(ref m) if m.contains("ok_job")),
        "{err:?}"
    );
}
