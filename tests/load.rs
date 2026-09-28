//! `AwsSqsPlugin::new()` reads `[aws_sqs]` from `autumn.toml` at startup.
//!
//! This binary changes the current directory, so it holds one test only.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use autumn_plugin_aws_sqs::transport::MemoryTransport;
use autumn_plugin_aws_sqs::{AwsSqsPlugin, SqsProducer};
use autumn_web::AppState;

#[tokio::test]
async fn new_loads_autumn_toml_and_profile_file() {
    let dir = std::env::temp_dir().join(format!("aws-sqs-load-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("autumn.toml"),
        "[aws_sqs.queues]\nevents = \"https://sqs.local/000/base\"\n\
         [profile.staging.aws_sqs.queues]\nevents = \"https://sqs.local/000/inline\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("autumn-staging.toml"),
        "[aws_sqs.worker]\nmax_in_flight = 3\n",
    )
    .unwrap();
    std::env::set_current_dir(&dir).unwrap();

    let t = MemoryTransport::new().with_queue("https://sqs.local/000/inline");
    let state = AppState::for_test().with_profile("staging");
    let rt = AwsSqsPlugin::new()
        .with_transport(t.clone())
        .start(&state)
        .await
        .unwrap();
    let producer = SqsProducer::from_state(&state).unwrap();
    assert_eq!(
        producer.resolve("events").unwrap(),
        "https://sqs.local/000/inline"
    );
    producer
        .send(
            "events",
            autumn_plugin_aws_sqs::transport::OutboundMessage::new("x"),
        )
        .await
        .unwrap();
    assert_eq!(t.messages("https://sqs.local/000/inline").len(), 1);
    rt.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
