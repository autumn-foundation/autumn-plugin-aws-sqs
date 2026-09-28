//! Amazon SQS plugin for autumn-web.
//!
//! - **Jobs:** send `#[job]` work to SQS. Workers run the same handler.
//! - **Consumers:** run a handler for each message on any queue.
//! - **Producer:** send messages to any queue.
//! - **Operations:** health indicator, Prometheus metrics, drain on shutdown.
//!
//! See the README for setup.

pub mod config;
mod consumer;
pub mod envelope;
mod error;
mod health;
mod jobs;
mod metrics;
mod plugin;
pub mod policy;
mod producer;
pub mod transport;
mod worker;

pub use config::SqsConfig;
pub use consumer::{ConsumerError, SnsNotification, SqsConsumer, SqsMessage};
pub use error::SqsError;
pub use health::SqsHealth;
pub use jobs::{EnqueueOptions, SqsJobClient};
pub use metrics::{QueueCounters, SqsMetrics};
pub use plugin::{AwsSqsPlugin, PLUGIN_NAME, SqsRuntime};
pub use producer::SqsProducer;
