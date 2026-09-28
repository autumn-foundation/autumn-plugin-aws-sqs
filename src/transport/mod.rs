//! SQS transport seam.
//!
//! [`SqsTransport`] is the only code that talks to SQS.
//! [`AwsSqsTransport`] uses the AWS SDK. [`MemoryTransport`] is an in-memory fake.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use crate::error::SqsError;

mod aws;
mod memory;

pub use aws::AwsSqsTransport;
pub use memory::{MemoryTransport, MessageSnapshot};

/// Boxed `Send` future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// SQS limit for the message body plus attributes, in bytes (1 MiB).
pub const MAX_MESSAGE_BYTES: usize = 1_048_576;
/// SQS limit for message attributes on one message.
pub const MAX_ATTRIBUTES: usize = 10;
/// Data type of every attribute this crate sends.
const ATTRIBUTE_TYPE: &str = "String";

/// A message to send. Build it with [`OutboundMessage::new`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutboundMessage {
    /// Message body.
    pub body: String,
    /// `DelaySeconds`, 0 to 900. Standard queues only.
    pub delay_secs: u64,
    /// FIFO `MessageGroupId`.
    pub group_id: Option<String>,
    /// FIFO `MessageDeduplicationId`.
    pub dedup_id: Option<String>,
    /// String message attributes.
    pub attributes: BTreeMap<String, String>,
}

impl OutboundMessage {
    /// Makes a message with this body.
    #[must_use]
    pub fn new(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            ..Self::default()
        }
    }

    /// Sets `DelaySeconds`.
    #[must_use]
    pub const fn delay_secs(mut self, secs: u64) -> Self {
        self.delay_secs = secs;
        self
    }

    /// Sets the FIFO group ID.
    #[must_use]
    pub fn group_id(mut self, id: impl Into<String>) -> Self {
        self.group_id = Some(id.into());
        self
    }

    /// Sets the FIFO deduplication ID.
    #[must_use]
    pub fn dedup_id(mut self, id: impl Into<String>) -> Self {
        self.dedup_id = Some(id.into());
        self
    }

    /// Adds a string attribute.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Size that SQS counts against the limit: body, and per attribute its
    /// name, data type, and value.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.attributes.iter().fold(self.body.len(), |acc, (k, v)| {
            acc + k.len() + ATTRIBUTE_TYPE.len() + v.len()
        })
    }
}

/// A received message.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReceivedMessage {
    /// SQS message ID.
    pub message_id: String,
    /// Receipt handle for this receive.
    pub receipt_handle: String,
    /// Message body.
    pub body: String,
    /// `ApproximateReceiveCount`.
    pub receive_count: Option<u32>,
    /// String and Number message attributes. Binary attributes are not kept.
    pub attributes: BTreeMap<String, String>,
    /// FIFO `MessageGroupId`.
    pub group_id: Option<String>,
}

/// Returns `true` when SQS accepts every character of `text`.
///
/// SQS accepts `#x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF`.
#[must_use]
pub fn is_valid_sqs_text(text: &str) -> bool {
    text.chars().all(is_valid_sqs_char)
}

const fn is_valid_sqs_char(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\r' | ' '..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..)
}

/// Replaces each character that SQS does not accept with `?`.
#[must_use]
pub fn sanitize_sqs_text(text: &str) -> String {
    text.chars()
        .map(|c| if is_valid_sqs_char(c) { c } else { '?' })
        .collect()
}

/// Checks the SQS send rules for one message.
///
/// # Errors
/// Returns [`SqsError::TooLarge`], [`SqsError::FifoDelay`], or
/// [`SqsError::InvalidRequest`] with the first broken rule.
pub fn validate_outbound(queue_url: &str, message: &OutboundMessage) -> Result<(), SqsError> {
    let size = message.size_bytes();
    if size > MAX_MESSAGE_BYTES {
        return Err(SqsError::TooLarge {
            size,
            max: MAX_MESSAGE_BYTES,
        });
    }
    let bad = |m: String| Err(SqsError::InvalidRequest(m));
    if message.attributes.len() > MAX_ATTRIBUTES {
        return bad(format!(
            "{} message attributes; the limit is {MAX_ATTRIBUTES}",
            message.attributes.len()
        ));
    }
    for (k, v) in &message.attributes {
        if k.is_empty() || v.is_empty() {
            return bad(format!("attribute {k:?} has an empty name or value"));
        }
        if !is_valid_sqs_text(k) || !is_valid_sqs_text(v) {
            return bad(format!(
                "attribute {k:?} has a character that SQS does not accept"
            ));
        }
    }
    if !is_valid_sqs_text(&message.body) {
        return bad("body has a character that SQS does not accept".to_owned());
    }
    if is_fifo(queue_url) {
        if message.delay_secs > 0 {
            return Err(SqsError::FifoDelay(queue_url.to_owned()));
        }
        if message.group_id.is_none() {
            return bad("FIFO queue needs MessageGroupId".to_owned());
        }
    }
    if message.delay_secs > crate::policy::MAX_DELAY_SECS {
        return bad(format!(
            "DelaySeconds {} is over {}",
            message.delay_secs,
            crate::policy::MAX_DELAY_SECS
        ));
    }
    Ok(())
}

/// Options for one receive call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveOptions {
    /// Messages to return, 1 to 10.
    pub max_messages: u32,
    /// Long-poll wait, 0 to 20 s.
    pub wait_secs: u64,
    /// Visibility timeout for the returned messages.
    pub visibility_secs: u64,
}

/// Queue counters from `GetQueueAttributes`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueStats {
    /// `ApproximateNumberOfMessages`.
    pub visible: u64,
    /// `ApproximateNumberOfMessagesNotVisible`.
    pub in_flight: u64,
    /// `ApproximateNumberOfMessagesDelayed`.
    pub delayed: u64,
    /// Dead-letter target ARN from the redrive policy, if set.
    pub redrive_target: Option<String>,
}

/// Result of one batch entry, in input order.
pub type BatchEntryResult = Result<String, SqsError>;

/// Minimal SQS surface used by this crate.
///
/// Implement it to use a custom client. All methods take a queue URL.
pub trait SqsTransport: Send + Sync + 'static {
    /// `SendMessage`. Returns the message ID.
    fn send<'a>(
        &'a self,
        queue_url: &'a str,
        message: OutboundMessage,
    ) -> BoxFuture<'a, Result<String, SqsError>>;

    /// `SendMessageBatch` for 1 to 10 messages.
    ///
    /// The outer error is for the whole call. The inner results are per entry.
    fn send_batch<'a>(
        &'a self,
        queue_url: &'a str,
        messages: Vec<OutboundMessage>,
    ) -> BoxFuture<'a, Result<Vec<BatchEntryResult>, SqsError>>;

    /// `ReceiveMessage`.
    fn receive<'a>(
        &'a self,
        queue_url: &'a str,
        options: ReceiveOptions,
    ) -> BoxFuture<'a, Result<Vec<ReceivedMessage>, SqsError>>;

    /// `DeleteMessage`.
    fn delete<'a>(
        &'a self,
        queue_url: &'a str,
        receipt_handle: &'a str,
    ) -> BoxFuture<'a, Result<(), SqsError>>;

    /// `ChangeMessageVisibility`.
    fn change_visibility<'a>(
        &'a self,
        queue_url: &'a str,
        receipt_handle: &'a str,
        visibility_secs: u64,
    ) -> BoxFuture<'a, Result<(), SqsError>>;

    /// `GetQueueAttributes` for the counters.
    fn queue_stats<'a>(&'a self, queue_url: &'a str)
    -> BoxFuture<'a, Result<QueueStats, SqsError>>;
}

/// Returns `true` for a FIFO queue URL.
#[must_use]
#[allow(clippy::case_sensitive_file_extension_comparisons)] // SQS needs lower-case `.fifo`.
pub fn is_fifo(queue_url: &str) -> bool {
    queue_url.ends_with(".fifo")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_counts_body_names_types_and_values() {
        let m = OutboundMessage::new("abc").attribute("k", "vv");
        // 3 + 1 + "String".len() + 2
        assert_eq!(m.size_bytes(), 12);
    }

    #[test]
    fn validate_rules() {
        let url = "https://q/std";
        let fifo = "https://q/f.fifo";
        assert!(validate_outbound(url, &OutboundMessage::new("x")).is_ok());
        assert!(matches!(
            validate_outbound(fifo, &OutboundMessage::new("x").dedup_id("d")),
            Err(SqsError::InvalidRequest(_))
        ));
        assert!(matches!(
            validate_outbound(fifo, &OutboundMessage::new("x").group_id("g").delay_secs(1)),
            Err(SqsError::FifoDelay(_))
        ));
        assert!(matches!(
            validate_outbound(url, &OutboundMessage::new("x").delay_secs(901)),
            Err(SqsError::InvalidRequest(_))
        ));
        assert!(matches!(
            validate_outbound(url, &OutboundMessage::new("\u{1}")),
            Err(SqsError::InvalidRequest(_))
        ));
        assert!(matches!(
            validate_outbound(url, &OutboundMessage::new("x").attribute("k", "\u{0}")),
            Err(SqsError::InvalidRequest(_))
        ));
    }

    #[test]
    fn sqs_text_rules() {
        assert!(is_valid_sqs_text("tab\tnew\nline\r ok é 🙂"));
        assert!(!is_valid_sqs_text("\u{0}"));
        assert!(!is_valid_sqs_text("\u{fffe}"));
        assert_eq!(sanitize_sqs_text("a\u{0}b"), "a?b");
    }
}
