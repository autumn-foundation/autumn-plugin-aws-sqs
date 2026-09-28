//! Send messages to any queue.

use std::sync::Arc;

use autumn_web::AppState;
use serde::Serialize;

use crate::config::SqsConfig;
use crate::error::SqsError;
use crate::metrics::SqsMetrics;
use crate::policy::{MAX_DELAY_SECS, batch_bounds, batch_count};
use crate::transport::{
    BatchEntryResult, MAX_MESSAGE_BYTES, OutboundMessage, SqsTransport, is_fifo,
};

/// Sends messages. Get it with [`SqsProducer::from_state`].
///
/// A queue is an alias from `[aws_sqs.queues]` or a full queue URL.
#[derive(Clone)]
pub struct SqsProducer {
    inner: Arc<Inner>,
}

struct Inner {
    transport: Arc<dyn SqsTransport>,
    config: SqsConfig,
    metrics: Arc<SqsMetrics>,
}

impl std::fmt::Debug for SqsProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsProducer").finish_non_exhaustive()
    }
}

impl SqsProducer {
    /// Makes a producer.
    #[must_use]
    pub fn new(
        transport: Arc<dyn SqsTransport>,
        config: SqsConfig,
        metrics: Arc<SqsMetrics>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                transport,
                config,
                metrics,
            }),
        }
    }

    /// Returns the producer that the plugin put in the app state.
    ///
    /// # Errors
    /// Returns [`SqsError::NotStarted`] before the plugin starts.
    pub fn from_state(state: &AppState) -> Result<Self, SqsError> {
        state
            .extension::<Self>()
            .map(|p| (*p).clone())
            .ok_or(SqsError::NotStarted)
    }

    /// Returns the queue URL for an alias or URL.
    ///
    /// # Errors
    /// Returns [`SqsError::UnknownQueue`] for an unknown alias.
    pub fn resolve(&self, queue: &str) -> Result<String, SqsError> {
        self.inner.config.resolve_queue(queue)
    }

    pub(crate) fn transport(&self) -> &Arc<dyn SqsTransport> {
        &self.inner.transport
    }

    /// Sends one message. Returns the SQS message ID.
    ///
    /// # Errors
    /// Returns a validation error or the transport error.
    pub async fn send(&self, queue: &str, message: OutboundMessage) -> Result<String, SqsError> {
        let url = self.resolve(queue)?;
        self.send_to_url(queue, &url, message).await
    }

    /// Sends `value` as a JSON body.
    ///
    /// # Errors
    /// Returns [`SqsError::Json`], a validation error, or the transport error.
    pub async fn send_json<T: Serialize + Sync>(
        &self,
        queue: &str,
        value: &T,
    ) -> Result<String, SqsError> {
        self.send(queue, OutboundMessage::new(serde_json::to_string(value)?))
            .await
    }

    /// Sends any number of messages in batches of 10.
    ///
    /// Returns one result per message, in input order. A failed batch call
    /// fails each entry of that batch. Other batches still go.
    ///
    /// # Errors
    /// Returns [`SqsError::UnknownQueue`] for an unknown alias.
    pub async fn send_batch(
        &self,
        queue: &str,
        messages: Vec<OutboundMessage>,
    ) -> Result<Vec<BatchEntryResult>, SqsError> {
        let url = self.resolve(queue)?;
        let n = messages.len();
        let mut results: Vec<Option<BatchEntryResult>> = (0..n).map(|_| None).collect();
        let mut pending: Vec<Option<OutboundMessage>> = messages.into_iter().map(Some).collect();
        for k in 0..batch_count(n) {
            let Some((start, end)) = batch_bounds(n, k) else {
                break;
            };
            // Check each entry first. Send only the valid ones.
            let mut index = Vec::new();
            let mut chunk = Vec::new();
            for (i, slot) in pending.iter_mut().enumerate().take(end).skip(start) {
                let Some(m) = slot.take() else {
                    continue;
                };
                match check(&url, &m) {
                    Ok(()) => {
                        index.push(i);
                        chunk.push(m);
                    }
                    Err(e) => results[i] = Some(Err(e)),
                }
            }
            if chunk.is_empty() {
                continue;
            }
            match self.inner.transport.send_batch(&url, chunk).await {
                Ok(out) => {
                    for (i, r) in index.iter().zip(out) {
                        results[*i] = Some(r);
                    }
                }
                Err(e) => {
                    for i in &index {
                        results[*i] = Some(Err(e.clone()));
                    }
                }
            }
        }
        let results: Vec<BatchEntryResult> = results
            .into_iter()
            .map(|r| r.unwrap_or_else(|| Err(SqsError::Service("entry was not sent".to_owned()))))
            .collect();
        let ok = results.iter().filter(|r| r.is_ok()).count() as u64;
        let bad = results.len() as u64 - ok;
        self.inner.metrics.update(queue, |c| {
            c.sent += ok;
            c.send_errors += bad;
        });
        Ok(results)
    }

    /// Sends to a resolved URL. `label` names the queue in metrics.
    pub(crate) async fn send_to_url(
        &self,
        label: &str,
        url: &str,
        message: OutboundMessage,
    ) -> Result<String, SqsError> {
        let result = match check(url, &message) {
            Ok(()) => self.inner.transport.send(url, message).await,
            Err(e) => Err(e),
        };
        self.inner.metrics.update(label, |c| match &result {
            Ok(_) => c.sent += 1,
            Err(_) => c.send_errors += 1,
        });
        result
    }
}

/// Checks the SQS rules before a send.
fn check(url: &str, m: &OutboundMessage) -> Result<(), SqsError> {
    let size = m.size_bytes();
    if size > MAX_MESSAGE_BYTES {
        return Err(SqsError::TooLarge {
            size,
            max: MAX_MESSAGE_BYTES,
        });
    }
    if is_fifo(url) && m.delay_secs > 0 {
        return Err(SqsError::FifoDelay(url.to_owned()));
    }
    if m.delay_secs > MAX_DELAY_SECS {
        return Err(SqsError::InvalidRequest(format!(
            "delay {} s is over {MAX_DELAY_SECS} s",
            m.delay_secs
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_rules() {
        let url = "https://q/std";
        assert!(check(url, &OutboundMessage::new("x")).is_ok());
        assert!(matches!(
            check(url, &OutboundMessage::new("x").delay_secs(901)),
            Err(SqsError::InvalidRequest(_))
        ));
        assert!(matches!(
            check("https://q/f.fifo", &OutboundMessage::new("x").delay_secs(1)),
            Err(SqsError::FifoDelay(_))
        ));
        let big = OutboundMessage::new("x").attribute("k", "v".repeat(MAX_MESSAGE_BYTES));
        assert!(matches!(check(url, &big), Err(SqsError::TooLarge { .. })));
    }
}
