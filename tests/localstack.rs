//! Integration tests against a real SQS API (`LocalStack` or AWS).
//!
//! Set `AWS_SQS_IT_ENDPOINT` (for example `http://localhost:4566`) and
//! `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` to run them. Without the
//! endpoint, each test returns at once.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use autumn_plugin_aws_sqs::transport::{
    AwsSqsTransport, OutboundMessage, ReceiveOptions, SqsTransport,
};
use autumn_plugin_aws_sqs::{AwsSqsPlugin, SqsConfig, SqsError, SqsJobClient};
use autumn_web::actuator::HealthIndicator as _;
use autumn_web::prelude::*;
use autumn_web::{AppState, jobs};
use aws_sdk_sqs::types::QueueAttributeName;
use serde::{Deserialize, Serialize};

fn endpoint() -> Option<String> {
    std::env::var("AWS_SQS_IT_ENDPOINT").ok()
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn base_config(endpoint: &str) -> SqsConfig {
    // Exercise the named env var credential path.
    let mut cfg = SqsConfig {
        region: Some("us-east-1".into()),
        endpoint: Some(endpoint.into()),
        access_key_id_env: Some("AWS_ACCESS_KEY_ID".into()),
        secret_access_key_env: Some("AWS_SECRET_ACCESS_KEY".into()),
        ..SqsConfig::default()
    };
    cfg.worker.wait_time_secs = 1;
    cfg.worker.visibility_timeout_secs = 5;
    cfg.worker.max_backoff_secs = 2;
    cfg.worker.drain_timeout_secs = 5;
    cfg.worker.stats_interval_secs = 0;
    cfg
}

async fn transport(cfg: &SqsConfig) -> AwsSqsTransport {
    AwsSqsTransport::from_config(cfg).await.expect("client")
}

async fn create_queue(t: &AwsSqsTransport, name: &str, fifo: bool) -> String {
    let mut req = t.client().create_queue().queue_name(name);
    if fifo {
        req = req.attributes(QueueAttributeName::FifoQueue, "true");
    }
    req.send().await.unwrap().queue_url().unwrap().to_owned()
}

async fn wait_for(limit: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(
            start.elapsed() < limit,
            "condition not met within {limit:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transport_round_trip() {
    let Some(ep) = endpoint() else { return };
    let cfg = base_config(&ep);
    let t = transport(&cfg).await;
    let q = create_queue(&t, &unique("rt"), false).await;

    let id = t
        .send(&q, OutboundMessage::new("hello").attribute("k", "v"))
        .await
        .unwrap();
    let opts = ReceiveOptions {
        max_messages: 10,
        wait_secs: 2,
        visibility_secs: 1,
    };
    let got = t.receive(&q, opts).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].message_id, id);
    assert_eq!(got[0].body, "hello");
    assert_eq!(got[0].receive_count, Some(1));
    assert_eq!(got[0].attributes["k"], "v");

    // Visibility expiry gives a new receive count.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let again = t.receive(&q, opts).await.unwrap();
    assert_eq!(again[0].receive_count, Some(2));
    t.change_visibility(&q, &again[0].receipt_handle, 30)
        .await
        .unwrap();
    let stats = t.queue_stats(&q).await.unwrap();
    assert_eq!(stats.in_flight, 1);
    t.delete(&q, &again[0].receipt_handle).await.unwrap();

    // Batch with one bad entry: SQS reports it per entry.
    let results = t
        .send_batch(
            &q,
            vec![
                OutboundMessage::new("ok"),
                OutboundMessage::new("bad \u{0} char"),
            ],
        )
        .await
        .unwrap();
    assert!(results[0].is_ok());
    assert!(results[1].is_err(), "{results:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_queue_is_not_found() {
    let Some(ep) = endpoint() else { return };
    let cfg = base_config(&ep);
    let t = transport(&cfg).await;
    let q = format!("{ep}/000000000000/{}", unique("missing"));
    let err = t.queue_stats(&q).await.unwrap_err();
    assert!(matches!(err, SqsError::QueueNotFound(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn fifo_dedups_and_needs_group() {
    let Some(ep) = endpoint() else { return };
    let cfg = base_config(&ep);
    let t = transport(&cfg).await;
    let q = create_queue(&t, &format!("{}.fifo", unique("f")), true).await;
    let m = || OutboundMessage::new("x").group_id("g").dedup_id("d1");
    t.send(&q, m()).await.unwrap();
    t.send(&q, m()).await.unwrap();
    let got = t
        .receive(
            &q,
            ReceiveOptions {
                max_messages: 10,
                wait_secs: 1,
                visibility_secs: 30,
            },
        )
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    let err = t
        .send(&q, OutboundMessage::new("x").dedup_id("d2"))
        .await
        .unwrap_err();
    assert!(matches!(err, SqsError::InvalidRequest(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn redrive_policy_is_read() {
    let Some(ep) = endpoint() else { return };
    let cfg = base_config(&ep);
    let t = transport(&cfg).await;
    let dlq = create_queue(&t, &unique("rdlq"), false).await;
    let arn = t
        .client()
        .get_queue_attributes()
        .queue_url(&dlq)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()[&QueueAttributeName::QueueArn]
        .clone();
    let q = t
        .client()
        .create_queue()
        .queue_name(unique("rsrc"))
        .attributes(
            QueueAttributeName::RedrivePolicy,
            format!(r#"{{"deadLetterTargetArn":"{arn}","maxReceiveCount":"3"}}"#),
        )
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_owned();
    assert_eq!(t.queue_stats(&q).await.unwrap().redrive_target, Some(arn));
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItArgs {
    pub n: u32,
}

static IT_FLAKY: AtomicU32 = AtomicU32::new(0);

#[job(name = "it_flaky", max_attempts = 3, backoff_ms = 1000)]
async fn it_flaky(_state: AppState, _args: ItArgs) -> AutumnResult<()> {
    if IT_FLAKY.fetch_add(1, Ordering::SeqCst) == 0 {
        return Err(AutumnError::internal_server_error_msg("first try fails"));
    }
    Ok(())
}

#[job(name = "it_doomed", max_attempts = 2, backoff_ms = 1000)]
async fn it_doomed(_state: AppState, _args: ItArgs) -> AutumnResult<()> {
    Err(AutumnError::internal_server_error_msg("never works"))
}

static IT_DELAYED: AtomicU32 = AtomicU32::new(0);

#[job(name = "it_delayed")]
async fn it_delayed(_state: AppState, _args: ItArgs) -> AutumnResult<()> {
    IT_DELAYED.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn job_end_to_end_retry_dead_letter_and_delay() {
    let Some(ep) = endpoint() else { return };
    let mut cfg = base_config(&ep);
    let t = transport(&cfg).await;
    let jobs_q = create_queue(&t, &unique("jobs"), false).await;
    let dlq = create_queue(&t, &unique("dlq"), false).await;
    cfg.queues.insert("default".into(), jobs_q.clone());
    cfg.queues.insert("dlq".into(), dlq.clone());
    cfg.jobs.dead_letter_queue = Some("dlq".into());

    let state = AppState::for_test();
    // No custom transport: the plugin builds the AWS client from config.
    let rt = AwsSqsPlugin::new(cfg)
        .jobs(jobs![it_flaky, it_doomed, it_delayed])
        .start(&state)
        .await
        .unwrap();
    let client = SqsJobClient::from_state(&state).unwrap();

    client.enqueue("it_flaky", &ItArgs { n: 1 }).await.unwrap();
    client.enqueue("it_doomed", &ItArgs { n: 2 }).await.unwrap();
    let start = std::time::Instant::now();
    client
        .enqueue_in("it_delayed", &ItArgs { n: 3 }, Duration::from_secs(3))
        .await
        .unwrap();

    wait_for(Duration::from_secs(30), async || {
        IT_FLAKY.load(Ordering::SeqCst) >= 2
    })
    .await;
    wait_for(Duration::from_secs(30), async || {
        IT_DELAYED.load(Ordering::SeqCst) == 1
    })
    .await;
    assert!(
        start.elapsed() >= Duration::from_secs(3),
        "delay was honored"
    );

    let dead_opts = ReceiveOptions {
        max_messages: 1,
        wait_secs: 1,
        visibility_secs: 30,
    };
    let mut dead = Vec::new();
    wait_for(Duration::from_secs(30), async || {
        dead = t.receive(&dlq, dead_opts).await.unwrap();
        !dead.is_empty()
    })
    .await;
    assert!(dead[0].attributes["autumn-dead-letter-reason"].contains("never works"));
    assert_eq!(dead[0].attributes["autumn-attempts"], "2");

    let m = rt.metrics().snapshot();
    assert!(m["default"].retried >= 2, "{m:?}");
    assert_eq!(m["default"].dead_lettered, 1);
    assert_eq!(
        rt.health().check().await.status,
        autumn_web::actuator::HealthStatus::Up
    );
    rt.shutdown().await;

    wait_for(Duration::from_secs(10), async || {
        let s = t.queue_stats(&jobs_q).await.unwrap();
        s.visible + s.in_flight + s.delayed == 0
    })
    .await;
}
