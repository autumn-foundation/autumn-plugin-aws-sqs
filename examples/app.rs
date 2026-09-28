//! Example app: a `#[job]` on SQS, a consumer, and a producer.
//!
//! Run `LocalStack`, make the queues, and set the env vars in
//! `docs/adr/0001-sqs-job-transport.md` ("Test"). Then:
//! `cargo run --example app`
#![allow(missing_docs)] // `#[job]` makes items with no docs.

use autumn_plugin_aws_sqs::{
    AwsSqsPlugin, ConsumerError, SqsConsumer, SqsJobClient, SqsMessage, SqsProducer,
};
use autumn_web::prelude::*;
use autumn_web::reexports::axum::extract::State;
use autumn_web::{AppState, jobs};
use serde::{Deserialize, Serialize};

/// Job args.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WelcomeArgs {
    /// User to greet.
    pub user_id: i64,
}

/// Sends a welcome email. Runs up to 5 times, then goes to the DLQ.
#[job(name = "send_welcome_email", max_attempts = 5, backoff_ms = 1000)]
async fn send_welcome_email(_state: AppState, args: WelcomeArgs) -> AutumnResult<()> {
    tracing::info!(user_id = args.user_id, "welcome email sent");
    Ok(())
}

/// S3 upload event (only the fields we use).
#[derive(Debug, Deserialize)]
struct Upload {
    key: String,
}

async fn on_upload(state: AppState, msg: SqsMessage) -> Result<(), ConsumerError> {
    let upload: Upload = msg.json()?;
    tracing::info!(key = %upload.key, "upload received");
    // Tell other services.
    SqsProducer::from_state(&state)?
        .send_json("events", &serde_json::json!({ "uploaded": upload.key }))
        .await?;
    Ok(())
}

/// Enqueues a job from an HTTP handler.
#[post("/signup")]
async fn signup(State(state): State<AppState>) -> AutumnResult<&'static str> {
    SqsJobClient::from_state(&state)?
        .enqueue(SendWelcomeEmailJob::NAME, &WelcomeArgs { user_id: 42 })
        .await?;
    Ok("queued")
}

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .routes(routes![signup])
        .plugin(
            AwsSqsPlugin::new()
                .jobs(jobs![send_welcome_email])
                .consumer(SqsConsumer::new("uploads", "uploads", on_upload).max_attempts(3)),
        )
        .run()
        .await;
}
