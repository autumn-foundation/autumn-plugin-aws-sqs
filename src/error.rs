//! Error types.

use thiserror::Error;

/// Errors from this crate.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum SqsError {
    /// The configuration is not valid.
    #[error("aws_sqs config: {0}")]
    Config(String),
    /// The queue does not exist.
    #[error("queue not found: {0}")]
    QueueNotFound(String),
    /// No queue URL is set for this alias.
    #[error("unknown queue alias: {0}")]
    UnknownQueue(String),
    /// The plugin has no `#[job]` with this name.
    #[error("unknown job: {0}")]
    UnknownJob(String),
    /// SQS rejected the request as not valid.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// A FIFO queue does not accept a per-message delay.
    #[error("FIFO queue does not accept a per-message delay: {0}")]
    FifoDelay(String),
    /// The message is larger than the SQS limit.
    #[error("message is {size} bytes; the limit is {max} bytes")]
    TooLarge {
        /// Message size in bytes.
        size: usize,
        /// Limit in bytes.
        max: usize,
    },
    /// JSON encode or decode failed.
    #[error("json: {0}")]
    Json(String),
    /// The SQS service or the network failed.
    #[error("sqs service: {0}")]
    Service(String),
    /// The plugin runtime did not start.
    #[error("aws_sqs runtime is not started")]
    NotStarted,
    /// A `JobInterceptor` stopped or failed the enqueue.
    #[error("job interceptor: {0}")]
    Intercepted(String),
}

impl From<serde_json::Error> for SqsError {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(json_error_text(&err))
    }
}

/// Describes a JSON error without the input values. Message data stays out
/// of logs and dead-letter reasons.
pub(crate) fn json_error_text(err: &serde_json::Error) -> String {
    format!(
        "{:?} error at line {} column {}",
        err.classify(),
        err.line(),
        err.column()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_error_hides_values() {
        let err = serde_json::from_str::<u8>(r#""secret-value""#).unwrap_err();
        let e = SqsError::from(err);
        assert!(!e.to_string().contains("secret-value"), "{e}");
        assert!(e.to_string().contains("line 1"));
    }
}
