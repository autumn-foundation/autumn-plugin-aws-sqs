//! Failure paths, FIFO order, and hardening (review round 1).
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_plugin_aws_sqs::transport::{MemoryTransport, OutboundMessage, SqsTransport};
use autumn_plugin_aws_sqs::{
    AwsSqsPlugin, ConsumerError, SqsConsumer, SqsJobClient, SqsMessage, SqsRuntime,
};
use autumn_web::prelude::*;
use autumn_web::{AppState, jobs};
use common::{CRITICAL, DLQ, EVENTS, FaultyTransport, JOBS, config, wait_until};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

const EVENTS_FIFO: &str = "https://sqs.local/000/events.fifo";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Args {
    pub n: u32,
}

fn memory() -> MemoryTransport {
    MemoryTransport::new()
        .with_queue(JOBS)
        .with_queue(CRITICAL)
        .with_queue(DLQ)
        .with_queue(EVENTS)
        .with_queue(EVENTS_FIFO)
}

async fn start_with(
    t: impl SqsTransport,
    cfg: autumn_plugin_aws_sqs::SqsConfig,
    build: impl FnOnce(AwsSqsPlugin) -> AwsSqsPlugin,
) -> (AppState, SqsRuntime) {
    let state = AppState::for_test();
    let plugin = AwsSqsPlugin::with_config(cfg)
        .with_transport(t)
        .with_clock(common::clock());
    let rt = build(plugin).start(&state).await.unwrap();
    (state, rt)
}

fn no_dlq() -> autumn_plugin_aws_sqs::SqsConfig {
    let mut cfg = config();
    cfg.jobs.dead_letter_queue = None;
    cfg
}

// ------------------------------------------------------------- no DLQ path

#[tokio::test(start_paused = true)]
async fn no_dlq_poison_stays_and_waits() {
    let t = memory();
    let consumer = SqsConsumer::new(
        "nodlq_poison",
        "events",
        |_s: AppState, _m: SqsMessage| async { Err(ConsumerError::reject("bad")) },
    );
    let (_state, rt) = start_with(t.clone(), no_dlq(), |p| p.consumer(consumer)).await;
    t.send(EVENTS, OutboundMessage::new("x")).await.unwrap();
    wait_until(Duration::from_secs(10), || {
        rt.metrics()
            .snapshot()
            .get("nodlq_poison")
            .is_some_and(|c| c.redrive_deferred == 1)
    })
    .await;
    // The message stays, and it is not visible for at least 30 s.
    assert_eq!(t.messages(EVENTS).len(), 1);
    tokio::time::sleep(Duration::from_secs(20)).await;
    assert!(t.messages(EVENTS)[0].in_flight);
    assert_eq!(t.messages(EVENTS)[0].receive_count, 1);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn no_dlq_handler_stops_after_max_attempts() {
    static CALLS: AtomicU32 = AtomicU32::new(0);
    let t = memory();
    let consumer = SqsConsumer::new(
        "nodlq_retry",
        "events",
        |_s: AppState, _m: SqsMessage| async {
            CALLS.fetch_add(1, Ordering::SeqCst);
            Err(ConsumerError::retry("down"))
        },
    )
    .max_attempts(2)
    .backoff_ms(1_000);
    let (_state, rt) = start_with(t.clone(), no_dlq(), |p| p.consumer(consumer)).await;
    t.send(EVENTS, OutboundMessage::new("x")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(600)).await;
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        2,
        "the handler never runs a third time"
    );
    assert_eq!(t.messages(EVENTS).len(), 1, "no message is lost");
    rt.shutdown().await;
}

// ------------------------------------------------------ DLQ send failures

#[tokio::test(start_paused = true)]
async fn dlq_send_failure_keeps_message_then_dead_letters_without_rerun() {
    static CALLS: AtomicU32 = AtomicU32::new(0);
    let t = FaultyTransport::new(memory());
    t.faults().fail_send_to = Some(DLQ.to_owned());
    let consumer = SqsConsumer::new("dlq_fail", "events", |_s: AppState, _m: SqsMessage| async {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Err(ConsumerError::retry("down"))
    })
    .max_attempts(1);
    let (_state, rt) = start_with(t.clone(), config(), |p| p.consumer(consumer)).await;
    t.inner
        .send(EVENTS, OutboundMessage::new("x"))
        .await
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        rt.metrics()
            .snapshot()
            .get("dlq_fail")
            .is_some_and(|c| c.send_errors == 1)
    })
    .await;
    assert_eq!(t.inner.messages(EVENTS).len(), 1, "the message stays");
    assert!(t.inner.messages(DLQ).is_empty());
    // Heal the DLQ. The next receive dead-letters it and does not run the handler.
    t.faults().fail_send_to = None;
    wait_until(Duration::from_secs(120), || {
        t.inner.messages(DLQ).len() == 1
    })
    .await;
    assert_eq!(CALLS.load(Ordering::SeqCst), 1);
    assert!(t.inner.messages(EVENTS).is_empty());
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn delete_after_dead_letter_failure_counts_ack_error() {
    let t = FaultyTransport::new(memory());
    t.faults().fail_delete = true;
    let consumer = SqsConsumer::new(
        "del_after_dl",
        "events",
        |_s: AppState, _m: SqsMessage| async { Err(ConsumerError::reject("bad")) },
    );
    let (_state, rt) = start_with(t.clone(), config(), |p| p.consumer(consumer)).await;
    t.inner
        .send(EVENTS, OutboundMessage::new("x"))
        .await
        .unwrap();
    wait_until(Duration::from_secs(10), || t.inner.messages(DLQ).len() == 1).await;
    let m = rt.metrics().snapshot();
    assert_eq!(m["del_after_dl"].ack_errors, 1);
    assert_eq!(m["del_after_dl"].dead_lettered, 1);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn delete_failure_on_ack_counts_ack_error_and_redelivers() {
    static CALLS: AtomicU32 = AtomicU32::new(0);
    let t = FaultyTransport::new(memory());
    t.faults().fail_delete = true;
    let consumer = SqsConsumer::new("ack_fail", "events", |_s: AppState, _m: SqsMessage| async {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    let (_state, rt) = start_with(t.clone(), config(), |p| p.consumer(consumer)).await;
    t.inner
        .send(EVENTS, OutboundMessage::new("x"))
        .await
        .unwrap();
    wait_until(Duration::from_secs(60), || {
        CALLS.load(Ordering::SeqCst) >= 2
    })
    .await;
    let m = rt.metrics().snapshot();
    assert!(m["ack_fail"].ack_errors >= 1);
    assert_eq!(m["ack_fail"].succeeded, 0);
    rt.shutdown().await;
}

// ---------------------------------------------------------- timing rules

#[tokio::test(start_paused = true)]
async fn retry_backoff_doubles() {
    static TIMES: Mutex<Vec<Instant>> = Mutex::new(Vec::new());
    let t = memory();
    let consumer = SqsConsumer::new("backoff", "events", |_s: AppState, _m: SqsMessage| async {
        let n = {
            let mut times = TIMES.lock().unwrap();
            times.push(Instant::now());
            times.len()
        };
        if n < 3 {
            Err(ConsumerError::retry("not yet"))
        } else {
            Ok(())
        }
    })
    .backoff_ms(1_000);
    let (_state, rt) = start_with(t.clone(), config(), |p| p.consumer(consumer)).await;
    t.send(EVENTS, OutboundMessage::new("x")).await.unwrap();
    wait_until(Duration::from_secs(60), || TIMES.lock().unwrap().len() == 3).await;
    let times = TIMES.lock().unwrap().clone();
    assert!(
        times[1] - times[0] >= Duration::from_secs(1),
        "first backoff is 1 s"
    );
    assert!(
        times[2] - times[1] >= Duration::from_secs(2),
        "second backoff is 2 s"
    );
    wait_until(Duration::from_secs(10), || {
        rt.metrics().snapshot()["backoff"].succeeded == 1
    })
    .await;
    assert_eq!(rt.metrics().snapshot()["backoff"].retried, 2);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn receive_errors_back_off_and_recover() {
    static DONE: AtomicU32 = AtomicU32::new(0);
    let t = FaultyTransport::new(memory());
    t.faults().receive_failures = 3;
    let consumer = SqsConsumer::new("recv_err", "events", |_s: AppState, _m: SqsMessage| async {
        DONE.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    t.inner
        .send(EVENTS, OutboundMessage::new("x"))
        .await
        .unwrap();
    let start = Instant::now();
    let (_state, rt) = start_with(t.clone(), config(), |p| p.consumer(consumer)).await;
    wait_until(Duration::from_secs(30), || DONE.load(Ordering::SeqCst) == 1).await;
    // Waits of 1 s, 2 s, and 4 s come first.
    assert!(start.elapsed() >= Duration::from_secs(7));
    assert_eq!(rt.metrics().snapshot()["recv_err"].receive_errors, 3);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn drain_timeout_stops_a_stuck_handler() {
    static STARTED: AtomicU32 = AtomicU32::new(0);
    let t = memory();
    let consumer = SqsConsumer::new("stuck", "events", |_s: AppState, _m: SqsMessage| async {
        STARTED.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(1_000)).await;
        Ok(())
    });
    let (_state, rt) = start_with(t.clone(), config(), |p| p.consumer(consumer)).await;
    t.send(EVENTS, OutboundMessage::new("x")).await.unwrap();
    wait_until(Duration::from_secs(10), || {
        STARTED.load(Ordering::SeqCst) == 1
    })
    .await;
    let before = Instant::now();
    rt.shutdown().await;
    let took = before.elapsed();
    assert!(
        took >= Duration::from_secs(5) && took < Duration::from_secs(7),
        "{took:?}"
    );
    assert_eq!(t.messages(EVENTS).len(), 1, "the message comes back later");
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtArgs {
    pub n: u32,
}

static AT_RUNS: AtomicU32 = AtomicU32::new(0);

#[job(name = "at_job")]
async fn at_job(_state: AppState, _args: AtArgs) -> AutumnResult<()> {
    AT_RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn enqueue_at_future_waits() {
    let t = memory();
    let clock = common::clock();
    let state = AppState::for_test();
    let rt = AwsSqsPlugin::with_config(config())
        .with_transport(t.clone())
        .with_clock(Arc::clone(&clock))
        .jobs(jobs![at_job])
        .start(&state)
        .await
        .unwrap();
    let when = clock.now() + autumn_web::reexports::chrono::TimeDelta::seconds(120);
    SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue_at("at_job", &AtArgs { n: 1 }, when)
        .await
        .unwrap();
    assert_eq!(t.queue_stats(JOBS).await.unwrap().delayed, 1);
    tokio::time::sleep(Duration::from_secs(110)).await;
    assert_eq!(AT_RUNS.load(Ordering::SeqCst), 0);
    wait_until(Duration::from_secs(30), || {
        AT_RUNS.load(Ordering::SeqCst) == 1
    })
    .await;
    rt.shutdown().await;
}

// ------------------------------------------------------------ FIFO order

#[tokio::test(start_paused = true)]
async fn fifo_group_runs_in_order_even_after_a_retry() {
    static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static FAILED_ONCE: AtomicU32 = AtomicU32::new(0);
    let t = memory();
    let consumer = SqsConsumer::new(
        "fifo_order",
        EVENTS_FIFO,
        |_s: AppState, m: SqsMessage| async move {
            if m.body == "1" && FAILED_ONCE.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(ConsumerError::retry("first try fails"));
            }
            SEEN.lock().unwrap().push(m.body.clone());
            Ok(())
        },
    )
    .backoff_ms(1_000);
    let (_state, rt) = start_with(t.clone(), config(), |p| p.consumer(consumer)).await;
    for n in 1..=3 {
        t.send(
            EVENTS_FIFO,
            OutboundMessage::new(n.to_string())
                .group_id("g")
                .dedup_id(n.to_string()),
        )
        .await
        .unwrap();
    }
    wait_until(Duration::from_secs(60), || SEEN.lock().unwrap().len() == 3).await;
    assert_eq!(*SEEN.lock().unwrap(), vec!["1", "2", "3"]);
    rt.shutdown().await;
}

// ---------------------------------------------------------------- panics

#[tokio::test(start_paused = true)]
async fn panic_before_the_future_is_poison() {
    let t = memory();
    let consumer = SqsConsumer::new("sync_panic", "events", |_s: AppState, m: SqsMessage| {
        assert!(m.body != "boom", "sync part panics");
        async { Ok(()) }
    });
    let (_state, rt) = start_with(t.clone(), config(), |p| p.consumer(consumer)).await;
    t.send(EVENTS, OutboundMessage::new("boom")).await.unwrap();
    wait_until(Duration::from_secs(10), || t.messages(DLQ).len() == 1).await;
    let m = rt.metrics().snapshot();
    assert_eq!(m["sync_panic"].poisoned, 1);
    assert_eq!(m["sync_panic"].in_flight, 0);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn long_panic_reason_is_cut_to_256_chars() {
    let t = memory();
    let consumer = SqsConsumer::new(
        "long_panic",
        "events",
        |_s: AppState, _m: SqsMessage| async {
            panic!("{}", "x".repeat(2_000));
        },
    );
    let (_state, rt) = start_with(t.clone(), config(), |p| p.consumer(consumer)).await;
    t.send(EVENTS, OutboundMessage::new("x")).await.unwrap();
    wait_until(Duration::from_secs(10), || t.messages(DLQ).len() == 1).await;
    let dead = &t.messages(DLQ)[0];
    assert_eq!(
        dead.attributes["autumn-dead-letter-reason"].chars().count(),
        256
    );
    assert_eq!(dead.attributes["autumn-source-queue"], EVENTS);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn dead_letter_keeps_ten_attributes() {
    let t = memory();
    let consumer = SqsConsumer::new(
        "many_attrs",
        "events",
        |_s: AppState, _m: SqsMessage| async { Err(ConsumerError::reject("bad")) },
    );
    let (_state, rt) = start_with(t.clone(), config(), |p| p.consumer(consumer)).await;
    let mut m = OutboundMessage::new("x");
    for i in 0..10 {
        m = m.attribute(format!("a{i}"), "v");
    }
    t.send(EVENTS, m).await.unwrap();
    wait_until(Duration::from_secs(10), || t.messages(DLQ).len() == 1).await;
    assert_eq!(t.messages(DLQ)[0].attributes.len(), 10);
    rt.shutdown().await;
}

// ----------------------------------------------------- jobs hardening

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HArgs {
    pub n: u32,
}

static CRIT_RUNS: AtomicU32 = AtomicU32::new(0);

#[job(name = "h_critical", queue = "critical")]
async fn h_critical(_state: AppState, _args: HArgs) -> AutumnResult<()> {
    CRIT_RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[job(name = "h_default")]
async fn h_default(_state: AppState, _args: HArgs) -> AutumnResult<()> {
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn job_on_the_wrong_queue_is_poison() {
    let t = memory();
    let (_state, rt) = start_with(t.clone(), config(), |p| {
        p.jobs(jobs![h_critical, h_default])
    })
    .await;
    // A sender with access to the default queue names a job of another queue.
    t.send(
        JOBS,
        OutboundMessage::new(r#"{"v":1,"job":"h_critical","payload":{"n":1},"enqueued_at":0}"#),
    )
    .await
    .unwrap();
    wait_until(Duration::from_secs(10), || t.messages(DLQ).len() == 1).await;
    assert!(t.messages(DLQ)[0].attributes["autumn-dead-letter-reason"].contains("does not belong"));
    assert_eq!(CRIT_RUNS.load(Ordering::SeqCst), 0);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn far_not_before_is_poison() {
    let t = memory();
    let (_state, rt) = start_with(t.clone(), config(), |p| p.jobs(jobs![h_default])).await;
    t.send(
        JOBS,
        OutboundMessage::new(format!(
            r#"{{"v":1,"job":"h_default","payload":{{"n":1}},"enqueued_at":0,"not_before":{}}}"#,
            i64::MAX
        )),
    )
    .await
    .unwrap();
    wait_until(Duration::from_secs(10), || t.messages(DLQ).len() == 1).await;
    assert!(t.messages(DLQ)[0].attributes["autumn-dead-letter-reason"].contains("not_before"));
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn too_long_delay_is_rejected_at_enqueue() {
    let t = memory();
    let (state, rt) = start_with(t, config(), |p| p.jobs(jobs![h_default])).await;
    let err = SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue_in(
            "h_default",
            &HArgs { n: 1 },
            Duration::from_secs(400 * 86_400),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        autumn_plugin_aws_sqs::SqsError::InvalidRequest(_)
    ));
    rt.shutdown().await;
}

/// Records interceptor calls.
#[derive(Default)]
struct Recorder {
    calls: Mutex<Vec<String>>,
}

/// Local handle, so the foreign trait can be implemented.
struct RecorderRef(Arc<Recorder>);

impl autumn_web::interceptor::JobInterceptor for RecorderRef {
    fn intercept_enqueue<'a>(
        &'a self,
        name: &'a str,
        _payload: &'a serde_json::Value,
        next: std::pin::Pin<Box<dyn std::future::Future<Output = AutumnResult<()>> + Send + 'a>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = AutumnResult<()>> + Send + 'a>> {
        Box::pin(async move {
            self.0.calls.lock().unwrap().push(format!("enqueue:{name}"));
            next.await
        })
    }

    fn intercept_execute<'a>(
        &'a self,
        name: &'a str,
        _payload: &'a serde_json::Value,
        next: std::pin::Pin<Box<dyn std::future::Future<Output = AutumnResult<()>> + Send + 'a>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = AutumnResult<()>> + Send + 'a>> {
        Box::pin(async move {
            self.0.calls.lock().unwrap().push(format!("execute:{name}"));
            next.await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn app_job_interceptor_wraps_enqueue_and_execute() {
    let t = memory();
    let recorder = Arc::new(Recorder::default());
    let state = AppState::for_test();
    let installed: Arc<dyn autumn_web::interceptor::JobInterceptor> =
        Arc::new(RecorderRef(Arc::clone(&recorder)));
    state.insert_extension(installed);
    let rt = AwsSqsPlugin::with_config(config())
        .with_transport(t.clone())
        .with_clock(common::clock())
        .jobs(jobs![h_default])
        .start(&state)
        .await
        .unwrap();
    SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue("h_default", &HArgs { n: 1 })
        .await
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        recorder.calls.lock().unwrap().len() == 2
    })
    .await;
    assert_eq!(
        *recorder.calls.lock().unwrap(),
        vec!["enqueue:h_default", "execute:h_default"]
    );
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn fifo_held_messages_keep_their_attempts() {
    static SEEN: Mutex<Vec<(String, u32)>> = Mutex::new(Vec::new());
    static M2_FAILED: AtomicU32 = AtomicU32::new(0);
    let t = memory();
    let consumer = SqsConsumer::new(
        "fifo_budget",
        EVENTS_FIFO,
        |_s: AppState, m: SqsMessage| async move {
            SEEN.lock().unwrap().push((m.body.clone(), m.attempt));
            if m.body == "1" {
                return Err(ConsumerError::retry("head always fails"));
            }
            if m.body == "2" && M2_FAILED.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(ConsumerError::retry("once"));
            }
            Ok(())
        },
    )
    .max_attempts(3)
    .backoff_ms(1_000);
    let (_state, rt) = start_with(t.clone(), config(), |p| p.consumer(consumer)).await;
    for n in 1..=2 {
        t.send(
            EVENTS_FIFO,
            OutboundMessage::new(n.to_string())
                .group_id("g")
                .dedup_id(n.to_string()),
        )
        .await
        .unwrap();
    }
    wait_until(Duration::from_secs(120), || {
        SEEN.lock()
            .unwrap()
            .iter()
            .filter(|(b, _)| b == "2")
            .count()
            == 2
    })
    .await;
    let seen = SEEN.lock().unwrap().clone();
    let m2: Vec<u32> = seen
        .iter()
        .filter(|(b, _)| b == "2")
        .map(|(_, a)| *a)
        .collect();
    // m2 starts at attempt 1 although the head failed three times first.
    assert_eq!(m2, vec![1, 2], "{seen:?}");
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn fifo_without_heartbeat_runs_each_message_once() {
    static RUNS: AtomicU32 = AtomicU32::new(0);
    let t = memory();
    let mut cfg = config();
    cfg.worker.heartbeat = false;
    let consumer = SqsConsumer::new(
        "fifo_nohb",
        EVENTS_FIFO,
        |_s: AppState, _m: SqsMessage| async {
            RUNS.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(6)).await;
            Ok(())
        },
    );
    let (_state, rt) = start_with(t.clone(), cfg, |p| p.consumer(consumer)).await;
    for n in 1..=4 {
        t.send(
            EVENTS_FIFO,
            OutboundMessage::new(n.to_string())
                .group_id("g")
                .dedup_id(n.to_string()),
        )
        .await
        .unwrap();
    }
    wait_until(Duration::from_secs(120), || {
        t.messages(EVENTS_FIFO).is_empty()
    })
    .await;
    assert_eq!(RUNS.load(Ordering::SeqCst), 4);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn bad_job_args_are_poison_without_values() {
    let t = memory();
    let (_state, rt) = start_with(t.clone(), config(), |p| p.jobs(jobs![h_default])).await;
    t.send(
        JOBS,
        OutboundMessage::new(
            r#"{"v":1,"job":"h_default","payload":{"n":"pii@example.com"},"enqueued_at":0}"#,
        ),
    )
    .await
    .unwrap();
    wait_until(Duration::from_secs(10), || t.messages(DLQ).len() == 1).await;
    let reason = &t.messages(DLQ)[0].attributes["autumn-dead-letter-reason"];
    assert_eq!(reason, "job args deserialization failed");
    assert_eq!(t.messages(DLQ)[0].attributes["autumn-attempts"], "1");
    rt.shutdown().await;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2 {
    pub name: String,
}

#[job(name = "h_versioned", version = 2)]
async fn h_versioned(_state: AppState, _args: V2) -> AutumnResult<()> {
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn bad_versioned_args_are_poison_without_values() {
    let t = memory();
    let (_state, rt) = start_with(t.clone(), config(), |p| p.jobs(jobs![h_versioned])).await;
    t.send(
        JOBS,
        OutboundMessage::new(
            r#"{"v":1,"job":"h_versioned","payload":{"__autumn_schema_version":2,"args":{"name":"pii@example.com","x":1,"name2":7}},"enqueued_at":0}"#
                .replace(r#""name":"pii@example.com","x":1,"name2":7"#, r#""name":["pii@example.com"]"#),
        ),
    )
    .await
    .unwrap();
    wait_until(Duration::from_secs(10), || t.messages(DLQ).len() == 1).await;
    let reason = &t.messages(DLQ)[0].attributes["autumn-dead-letter-reason"];
    assert!(reason.contains("stored payload version 2"), "{reason}");
    assert!(!reason.contains("pii@example.com"), "{reason}");
    assert_eq!(t.messages(DLQ)[0].attributes["autumn-attempts"], "1");
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn newer_payload_version_is_retried() {
    let t = memory();
    let (_state, rt) = start_with(t.clone(), config(), |p| p.jobs(jobs![h_versioned])).await;
    t.send(
        JOBS,
        OutboundMessage::new(
            r#"{"v":1,"job":"h_versioned","payload":{"__autumn_schema_version":3,"args":{"name":"a"}},"enqueued_at":0}"#,
        ),
    )
    .await
    .unwrap();
    wait_until(Duration::from_secs(30), || {
        rt.metrics()
            .snapshot()
            .get("default")
            .is_some_and(|c| c.retried >= 1)
    })
    .await;
    assert_eq!(rt.metrics().snapshot()["default"].poisoned, 0);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn zero_wait_does_not_spin_on_an_empty_queue() {
    // A worker that spins never yields, so time cannot move and this test hangs.
    let t = memory();
    let mut cfg = config();
    cfg.worker.wait_time_secs = 0;
    let consumer = SqsConsumer::new(
        "zero_wait",
        "events",
        |_s: AppState, _m: SqsMessage| async { Ok(()) },
    );
    let (_state, rt) = start_with(t.clone(), cfg, |p| p.consumer(consumer)).await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    rt.shutdown().await;
}
