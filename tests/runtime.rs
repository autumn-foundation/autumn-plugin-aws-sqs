//! Consumers, producer, plugin wiring, roles, drain, metrics, health
//! (AC1, AC7, AC8, AC9, AC10, AC12).
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use autumn_plugin_aws_sqs::transport::{MemoryTransport, OutboundMessage, SqsTransport};
use autumn_plugin_aws_sqs::{
    AwsSqsPlugin, ConsumerError, SqsConsumer, SqsError, SqsJobClient, SqsMessage, SqsProducer,
};
use autumn_web::actuator::{HealthIndicator, HealthStatus, MetricKind, MetricsSource};
use autumn_web::prelude::*;
use autumn_web::{AppState, jobs};
use common::{CRITICAL, DLQ, EVENTS, JOBS, config, wait_until};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Upload {
    key: String,
}

fn transport() -> MemoryTransport {
    MemoryTransport::new()
        .with_queue(JOBS)
        .with_queue(CRITICAL)
        .with_queue(DLQ)
        .with_queue(EVENTS)
}

fn plugin(t: &MemoryTransport) -> AwsSqsPlugin {
    AwsSqsPlugin::with_config(config())
        .with_transport(t.clone())
        .with_clock(common::clock())
}

// ------------------------------------------------------------- consumers

#[tokio::test(start_paused = true)]
async fn consumer_runs_typed_handler_and_acks() {
    static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let t = transport();
    let consumer = SqsConsumer::new(
        "uploads",
        "events",
        |_state: AppState, msg: SqsMessage| async move {
            let up: Upload = msg.json()?;
            SEEN.lock().unwrap().push(up.key);
            Ok(())
        },
    );
    let state = AppState::for_test();
    let rt = plugin(&t).consumer(consumer).start(&state).await.unwrap();
    t.send(EVENTS, OutboundMessage::new(r#"{"key":"a.png"}"#))
        .await
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        SEEN.lock().unwrap().contains(&"a.png".to_owned())
    })
    .await;
    wait_until(Duration::from_secs(10), || t.messages(EVENTS).is_empty()).await;
    assert_eq!(rt.metrics().snapshot()["uploads"].succeeded, 1);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn consumer_bad_json_is_rejected_to_dead_letter() {
    let t = transport();
    let consumer = SqsConsumer::new(
        "strict",
        EVENTS,
        |_s: AppState, msg: SqsMessage| async move {
            let _: Upload = msg.json()?;
            Ok(())
        },
    );
    let rt = plugin(&t)
        .consumer(consumer)
        .start(&AppState::for_test())
        .await
        .unwrap();
    t.send(EVENTS, OutboundMessage::new("{oops")).await.unwrap();
    wait_until(Duration::from_secs(10), || t.messages(DLQ).len() == 1).await;
    let m = rt.metrics().snapshot();
    assert_eq!(m["strict"].poisoned, 1);
    assert_eq!(m["strict"].dead_lettered, 1);
    assert!(t.messages(EVENTS).is_empty());
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn consumer_retry_error_uses_consumer_attempts() {
    static CALLS: AtomicU32 = AtomicU32::new(0);
    let t = transport();
    let consumer = SqsConsumer::new(
        "retrying",
        "events",
        |_s: AppState, _m: SqsMessage| async move {
            CALLS.fetch_add(1, Ordering::SeqCst);
            Err(ConsumerError::retry("downstream is down"))
        },
    )
    .max_attempts(3)
    .backoff_ms(1_000);
    let rt = plugin(&t)
        .consumer(consumer)
        .start(&AppState::for_test())
        .await
        .unwrap();
    t.send(EVENTS, OutboundMessage::new("x")).await.unwrap();
    wait_until(Duration::from_secs(60), || t.messages(DLQ).len() == 1).await;
    assert_eq!(CALLS.load(Ordering::SeqCst), 3);
    let m = rt.metrics().snapshot();
    assert_eq!(m["retrying"].retried, 2);
    assert_eq!(m["retrying"].dead_lettered, 1);
    assert!(t.messages(EVENTS).is_empty());
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn consumer_autumn_error_converts_to_retry() {
    static CALLS: AtomicU32 = AtomicU32::new(0);
    let t = transport();
    let consumer = SqsConsumer::new(
        "autumn_err",
        "events",
        |_s: AppState, _m: SqsMessage| async move {
            if CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
                let r: AutumnResult<()> = Err(AutumnError::internal_server_error_msg("db down"));
                r?;
            }
            Ok(())
        },
    );
    let rt = plugin(&t)
        .consumer(consumer)
        .start(&AppState::for_test())
        .await
        .unwrap();
    t.send(EVENTS, OutboundMessage::new("x")).await.unwrap();
    wait_until(Duration::from_secs(60), || {
        CALLS.load(Ordering::SeqCst) == 2
    })
    .await;
    wait_until(Duration::from_secs(10), || t.messages(EVENTS).is_empty()).await;
    assert_eq!(rt.metrics().snapshot()["autumn_err"].retried, 1);
    rt.shutdown().await;
}

#[test]
fn sns_envelope_unwraps() {
    let body = r#"{"Type":"Notification","MessageId":"1","TopicArn":"arn:aws:sns:us-east-1:1:t",
        "Subject":"s","Message":"{\"key\":\"b.png\"}","Timestamp":"2026-01-01T00:00:00Z"}"#;
    let msg = SqsMessage::for_test(body);
    let sns = msg.sns().unwrap();
    assert_eq!(sns.topic_arn, "arn:aws:sns:us-east-1:1:t");
    assert_eq!(sns.subject.as_deref(), Some("s"));
    let up: Upload = msg.sns_json().unwrap();
    assert_eq!(up.key, "b.png");
    assert!(matches!(
        SqsMessage::for_test("{}").sns(),
        Err(ConsumerError::Reject(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn consumer_on_unknown_queue_fails_start() {
    let t = transport();
    let consumer = SqsConsumer::new("lost", "nope", |_s: AppState, _m: SqsMessage| async {
        Ok(())
    });
    let err = plugin(&t)
        .consumer(consumer)
        .start(&AppState::for_test())
        .await
        .unwrap_err();
    assert_eq!(err, SqsError::UnknownQueue("nope".into()));
}

#[tokio::test(start_paused = true)]
async fn consumer_sharing_job_queue_fails_start() {
    let t = transport();
    let consumer = SqsConsumer::new("clash", "default", |_s: AppState, _m: SqsMessage| async {
        Ok(())
    });
    let err = plugin(&t)
        .jobs(jobs![noop_job])
        .consumer(consumer)
        .start(&AppState::for_test())
        .await
        .unwrap_err();
    assert!(
        matches!(err, SqsError::Config(ref m) if m.contains("clash")),
        "{err:?}"
    );
}

#[job(name = "noop_job")]
async fn noop_job(_state: AppState, _args: Upload) -> AutumnResult<()> {
    Ok(())
}

// -------------------------------------------------------------- producer

#[tokio::test(start_paused = true)]
async fn producer_sends_json_and_batches_over_ten() {
    let t = transport();
    let state = AppState::for_test();
    let rt = plugin(&t).start(&state).await.unwrap();
    let producer = SqsProducer::from_state(&state).unwrap();
    producer
        .send_json("events", &Upload { key: "k".into() })
        .await
        .unwrap();
    let batch: Vec<_> = (0..23)
        .map(|i| OutboundMessage::new(i.to_string()))
        .collect();
    let results = producer.send_batch("events", batch).await.unwrap();
    assert_eq!(results.len(), 23);
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(t.messages(EVENTS).len(), 24);
    assert_eq!(t.messages(EVENTS)[1].body, "0");
    assert_eq!(rt.metrics().snapshot()["events"].sent, 24);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn producer_batch_reports_each_failed_entry() {
    let t = transport();
    let state = AppState::for_test();
    let rt = plugin(&t).start(&state).await.unwrap();
    let producer = SqsProducer::from_state(&state).unwrap();
    let big = "x".repeat(autumn_plugin_aws_sqs::transport::MAX_MESSAGE_BYTES + 1);
    let results = producer
        .send_batch(
            "events",
            vec![OutboundMessage::new("ok"), OutboundMessage::new(big)],
        )
        .await
        .unwrap();
    assert!(results[0].is_ok());
    assert!(matches!(results[1], Err(SqsError::TooLarge { .. })));
    assert_eq!(rt.metrics().snapshot()["events"].send_errors, 1);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn producer_rejects_unknown_alias_and_fifo_delay() {
    let t = transport().with_queue("https://sqs.local/000/e.fifo");
    let state = AppState::for_test();
    let rt = plugin(&t).start(&state).await.unwrap();
    let producer = SqsProducer::from_state(&state).unwrap();
    assert_eq!(
        producer.send("nope", OutboundMessage::new("x")).await,
        Err(SqsError::UnknownQueue("nope".into()))
    );
    let err = producer
        .send(
            "https://sqs.local/000/e.fifo",
            OutboundMessage::new("x").group_id("g").delay_secs(5),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, SqsError::FifoDelay(_)));
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn producer_without_runtime_is_not_started() {
    assert!(matches!(
        SqsProducer::from_state(&AppState::for_test()),
        Err(SqsError::NotStarted)
    ));
}

// ------------------------------------------------------- roles and drain

#[tokio::test(start_paused = true)]
async fn web_role_enqueues_but_runs_no_workers() {
    let t = transport();
    let state = AppState::for_test();
    let rt = plugin(&t)
        .jobs(jobs![noop_job])
        .start_with_role(&state, autumn_web::ProcessRole::Web)
        .await
        .unwrap();
    assert_eq!(rt.workers_running(), 0);
    SqsJobClient::from_state(&state)
        .unwrap()
        .enqueue("noop_job", &Upload { key: "k".into() })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(t.messages(JOBS).len(), 1, "no worker drained it");
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn worker_role_runs_workers() {
    let t = transport();
    let rt = plugin(&t)
        .jobs(jobs![noop_job])
        .start_with_role(&AppState::for_test(), autumn_web::ProcessRole::Worker)
        .await
        .unwrap();
    assert_eq!(rt.workers_running(), 1);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_drains_in_flight_handler() {
    static STARTED: AtomicU32 = AtomicU32::new(0);
    static FINISHED: AtomicU32 = AtomicU32::new(0);
    let t = transport();
    let consumer = SqsConsumer::new(
        "drain",
        "events",
        |_s: AppState, _m: SqsMessage| async move {
            STARTED.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(3)).await;
            FINISHED.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    );
    let rt = plugin(&t)
        .consumer(consumer)
        .start(&AppState::for_test())
        .await
        .unwrap();
    t.send(EVENTS, OutboundMessage::new("x")).await.unwrap();
    wait_until(Duration::from_secs(10), || {
        STARTED.load(Ordering::SeqCst) == 1
    })
    .await;
    rt.shutdown().await;
    assert_eq!(
        FINISHED.load(Ordering::SeqCst),
        1,
        "drain waited for the handler"
    );
    assert!(t.messages(EVENTS).is_empty(), "handled message was deleted");
}

#[tokio::test(start_paused = true)]
async fn shutdown_stops_receiving() {
    static CALLS: AtomicU32 = AtomicU32::new(0);
    let t = transport();
    let consumer = SqsConsumer::new(
        "stopper",
        "events",
        |_s: AppState, _m: SqsMessage| async move {
            CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    );
    let rt = plugin(&t)
        .consumer(consumer)
        .start(&AppState::for_test())
        .await
        .unwrap();
    assert_eq!(rt.workers_running(), 1);
    let running = rt.running_handle();
    rt.shutdown().await;
    assert_eq!(running.load(std::sync::atomic::Ordering::SeqCst), 0);
    t.send(EVENTS, OutboundMessage::new("late")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(CALLS.load(Ordering::SeqCst), 0);
    assert_eq!(t.messages(EVENTS).len(), 1);
}

#[tokio::test(start_paused = true)]
async fn probe_drain_stops_workers() {
    let t = transport();
    let state = AppState::for_test();
    let rt = plugin(&t)
        .jobs(jobs![noop_job])
        .start(&state)
        .await
        .unwrap();
    assert_eq!(rt.workers_running(), 1);
    state.begin_shutdown_for_test();
    wait_until(Duration::from_secs(5), || rt.workers_running() == 0).await;
    rt.shutdown().await;
}

// ------------------------------------------------------ metrics, health

#[tokio::test(start_paused = true)]
async fn metrics_source_exports_families() {
    let t = transport();
    let state = AppState::for_test();
    let rt = plugin(&t).start(&state).await.unwrap();
    SqsProducer::from_state(&state)
        .unwrap()
        .send("events", OutboundMessage::new("x"))
        .await
        .unwrap();
    let families = rt.metrics().collect();
    let sent = families
        .iter()
        .find(|f| f.name == "aws_sqs_messages_sent_total")
        .expect("sent family");
    assert!(matches!(sent.kind, MetricKind::Counter));
    assert!(
        sent.samples
            .iter()
            .any(|s| s.labels.contains(&("queue".into(), "events".into())) && s.value >= 1.0)
    );
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn stats_sampler_exports_queue_depth() {
    let t = transport();
    let mut cfg = config();
    cfg.worker.stats_interval_secs = 5;
    let state = AppState::for_test();
    let rt = AwsSqsPlugin::with_config(cfg)
        .with_transport(t.clone())
        .with_clock(common::clock())
        .start_with_role(&state, autumn_web::ProcessRole::Web)
        .await
        .unwrap();
    t.send(EVENTS, OutboundMessage::new("x")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;
    let depth = rt
        .metrics()
        .collect()
        .into_iter()
        .find(|f| f.name == "aws_sqs_queue_messages")
        .expect("depth family");
    assert!(depth.samples.iter().any(|s| {
        s.labels.contains(&("queue".into(), "events".into()))
            && s.labels.contains(&("state".into(), "visible".into()))
            && (s.value - 1.0).abs() < f64::EPSILON
    }));
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn health_is_up_with_queue_details() {
    let t = transport();
    let rt = plugin(&t).start(&AppState::for_test()).await.unwrap();
    let out = rt.health().check().await;
    assert_eq!(out.status, HealthStatus::Up);
    assert!(out.details.contains_key("events"));
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn health_is_down_when_queue_missing() {
    // The fake has no "events" queue.
    let t = MemoryTransport::new().with_queue(JOBS).with_queue(DLQ);
    let rt = plugin(&t).start(&AppState::for_test()).await.unwrap();
    let out = rt.health().check().await;
    assert_eq!(out.status, HealthStatus::Down);
    rt.shutdown().await;
}

// ---------------------------------------------------------------- plugin

#[tokio::test(flavor = "multi_thread")]
async fn plugin_installs_with_one_call() {
    use autumn_web::test::TestApp;
    let t = transport();
    let client = TestApp::new()
        .plugin(plugin(&t).jobs(jobs![noop_job]))
        .build();
    let state = client.state();
    SqsJobClient::from_state(state)
        .unwrap()
        .enqueue("noop_job", &Upload { key: "k".into() })
        .await
        .unwrap();
    SqsProducer::from_state(state).unwrap();
    // Health and metrics are mounted on the actuator.
    let health = client.get("/actuator/health").send().await;
    let body = health.text();
    assert!(body.contains("aws_sqs"), "{body}");
    assert!(body.contains("UP"), "{body}");
}

#[test]
fn plugin_name_is_stable() {
    use autumn_web::plugin::Plugin;
    let p = AwsSqsPlugin::with_config(config()).with_transport(MemoryTransport::new());
    assert_eq!(p.name(), "autumn-plugin-aws-sqs");
}

#[tokio::test(start_paused = true)]
async fn custom_transport_arc_is_accepted() {
    let t: Arc<dyn SqsTransport> = Arc::new(transport());
    let state = AppState::for_test();
    let rt = AwsSqsPlugin::with_config(config())
        .with_transport_arc(t)
        .start(&state)
        .await
        .unwrap();
    SqsProducer::from_state(&state).unwrap();
    rt.shutdown().await;
}

#[test]
fn plugin_declares_autumn_contract() {
    use autumn_web::plugin::Plugin;
    let p = AwsSqsPlugin::with_config(config()).with_transport(MemoryTransport::new());
    let contract = p.contract().expect("plugin declares a contract");
    assert_eq!(contract.plugin, "autumn-plugin-aws-sqs");
    assert_eq!(
        contract.plugin_version.as_deref(),
        Some(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(contract.autumn_web.as_deref(), Some("0.8"));
}
