//! Handlers for messages from other systems.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use autumn_web::{AppState, AutumnError};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::error::SqsError;
use crate::policy::attempt_from_receive_count;
use crate::transport::{BoxFuture, ReceivedMessage};
use crate::worker::{Dispatch, Outcome, RetryRule};

/// Handler failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConsumerError {
    /// Try again later under the consumer retry rule.
    #[error("retry: {0}")]
    Retry(String),
    /// Do not try again. Dead-letter now.
    #[error("reject: {0}")]
    Reject(String),
}

impl ConsumerError {
    /// Makes a [`ConsumerError::Retry`].
    pub fn retry(reason: impl std::fmt::Display) -> Self {
        Self::Retry(reason.to_string())
    }

    /// Makes a [`ConsumerError::Reject`].
    pub fn reject(reason: impl std::fmt::Display) -> Self {
        Self::Reject(reason.to_string())
    }
}

impl From<AutumnError> for ConsumerError {
    fn from(err: AutumnError) -> Self {
        Self::Retry(err.to_string())
    }
}

impl From<SqsError> for ConsumerError {
    fn from(err: SqsError) -> Self {
        Self::Retry(err.to_string())
    }
}

/// Bad data does not get better on retry.
impl From<serde_json::Error> for ConsumerError {
    fn from(err: serde_json::Error) -> Self {
        Self::Reject(format!("json: {err}"))
    }
}

/// SNS notification envelope (raw message delivery off).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[non_exhaustive]
pub struct SnsNotification {
    /// `Notification`.
    #[serde(rename = "Type")]
    pub kind: String,
    /// SNS message ID.
    pub message_id: String,
    /// Topic ARN.
    pub topic_arn: String,
    /// Subject, if set.
    #[serde(default)]
    pub subject: Option<String>,
    /// The published message.
    pub message: String,
    /// Publish time, ISO 8601.
    #[serde(default)]
    pub timestamp: Option<String>,
}

/// A message given to a consumer handler.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SqsMessage {
    /// SQS message ID.
    pub message_id: String,
    /// Message body.
    pub body: String,
    /// 1-based attempt number.
    pub attempt: u32,
    /// String message attributes.
    pub attributes: BTreeMap<String, String>,
    /// Consumer name.
    pub consumer: String,
}

impl SqsMessage {
    /// Makes a message with this body, for handler tests.
    #[must_use]
    pub fn for_test(body: impl Into<String>) -> Self {
        Self {
            message_id: "test".to_owned(),
            body: body.into(),
            attempt: 1,
            attributes: BTreeMap::new(),
            consumer: "test".to_owned(),
        }
    }

    /// Decodes the body as JSON.
    ///
    /// # Errors
    /// Returns [`ConsumerError::Reject`] for bad JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, ConsumerError> {
        Ok(serde_json::from_str(&self.body)?)
    }

    /// Decodes the body as an SNS notification.
    ///
    /// # Errors
    /// Returns [`ConsumerError::Reject`] when the body is not one.
    pub fn sns(&self) -> Result<SnsNotification, ConsumerError> {
        serde_json::from_str(&self.body)
            .map_err(|e| ConsumerError::reject(format!("not an SNS notification: {e}")))
    }

    /// Decodes the SNS `Message` field as JSON.
    ///
    /// # Errors
    /// Returns [`ConsumerError::Reject`] for a bad envelope or bad JSON.
    pub fn sns_json<T: DeserializeOwned>(&self) -> Result<T, ConsumerError> {
        Ok(serde_json::from_str(&self.sns()?.message)?)
    }
}

type Handler = Arc<
    dyn Fn(AppState, SqsMessage) -> BoxFuture<'static, Result<(), ConsumerError>> + Send + Sync,
>;

/// A handler for one queue.
///
/// ```rust,ignore
/// SqsConsumer::new("thumbnails", "uploads", |state, msg: SqsMessage| async move {
///     let event: S3Event = msg.sns_json()?;
///     make_thumbnail(&state, event).await?;
///     Ok(())
/// })
/// .max_attempts(3)
/// ```
#[derive(Clone)]
pub struct SqsConsumer {
    pub(crate) name: String,
    pub(crate) queue: String,
    pub(crate) handler: Handler,
    pub(crate) rule: RetryRule,
}

impl std::fmt::Debug for SqsConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsConsumer")
            .field("name", &self.name)
            .field("queue", &self.queue)
            .field("rule", &self.rule)
            .finish_non_exhaustive()
    }
}

impl SqsConsumer {
    /// Makes a consumer. `queue` is an alias or a queue URL.
    ///
    /// Defaults: 5 attempts, 1 000 ms first backoff.
    pub fn new<F, Fut>(name: impl Into<String>, queue: impl Into<String>, handler: F) -> Self
    where
        F: Fn(AppState, SqsMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ConsumerError>> + Send + 'static,
    {
        Self {
            name: name.into(),
            queue: queue.into(),
            handler: Arc::new(move |state, msg| Box::pin(handler(state, msg))),
            rule: RetryRule {
                max_attempts: 5,
                initial_backoff_ms: 1_000,
            },
        }
    }

    /// Sets the attempt limit (1 or more).
    #[must_use]
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.rule.max_attempts = n.max(1);
        self
    }

    /// Sets the first retry backoff.
    #[must_use]
    pub const fn backoff_ms(mut self, ms: u64) -> Self {
        self.rule.initial_backoff_ms = ms;
        self
    }

    /// Returns the consumer name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Dispatch for SqsConsumer {
    fn dispatch(&self, state: AppState, message: ReceivedMessage) -> BoxFuture<'static, Outcome> {
        let msg = SqsMessage {
            attempt: attempt_from_receive_count(message.receive_count),
            message_id: message.message_id,
            body: message.body,
            attributes: message.attributes,
            consumer: self.name.clone(),
        };
        let run = (self.handler)(state, msg);
        let rule = self.rule;
        Box::pin(async move {
            match run.await {
                Ok(()) => Outcome::Ack,
                Err(ConsumerError::Retry(error)) => Outcome::Retry { error, rule },
                Err(ConsumerError::Reject(reason)) => Outcome::Poison(reason),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_conversions() {
        let e: ConsumerError = AutumnError::internal_server_error_msg("x").into();
        assert!(matches!(e, ConsumerError::Retry(_)));
        let e: ConsumerError = SqsError::NotStarted.into();
        assert!(matches!(e, ConsumerError::Retry(_)));
        let bad = serde_json::from_str::<u8>("x").unwrap_err();
        assert!(matches!(ConsumerError::from(bad), ConsumerError::Reject(_)));
    }

    #[test]
    fn max_attempts_is_at_least_one() {
        let c = SqsConsumer::new("c", "q", |_s: AppState, _m: SqsMessage| async { Ok(()) })
            .max_attempts(0);
        assert_eq!(c.rule.max_attempts, 1);
        assert_eq!(c.name(), "c");
    }
}
