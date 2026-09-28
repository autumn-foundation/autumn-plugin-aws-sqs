//! Send messages to any queue.

use std::sync::Arc;

use autumn_web::AppState;
use serde::Serialize;

use crate::config::SqsConfig;
use crate::error::SqsError;
use crate::metrics::SqsMetrics;
use crate::policy::{MAX_BATCH_BYTES, batch_end};
use crate::transport::{BatchEntryResult, OutboundMessage, SqsTransport, validate_outbound};

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
    pub(crate) fn new(
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
        let label = self.inner.config.label_for(queue);
        self.send_to_url(&label, &url, message).await
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

    /// Sends any number of messages in batches of 10 or less and 1 MiB or less.
    ///
    /// Returns one result per message, in input order. A failed batch call
    /// fails each entry of that batch. The producer sends the other batches.
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
        // Check each entry first. Batch only the valid ones.
        let mut index = Vec::new();
        let mut valid = Vec::new();
        for (i, m) in messages.into_iter().enumerate() {
            match validate_outbound(&url, &m) {
                Ok(()) => {
                    index.push(i);
                    valid.push(m);
                }
                Err(e) => results[i] = Some(Err(e)),
            }
        }
        let sizes: Vec<usize> = valid.iter().map(OutboundMessage::size_bytes).collect();
        let mut rest = valid.into_iter();
        let mut start = 0;
        while start < sizes.len() {
            let end = batch_end(&sizes, start, MAX_BATCH_BYTES);
            let chunk: Vec<OutboundMessage> = rest.by_ref().take(end - start).collect();
            let slots = &index[start..end];
            match self.inner.transport.send_batch(&url, chunk).await {
                Ok(out) => {
                    for (i, r) in slots.iter().zip(out) {
                        results[*i] = Some(r);
                    }
                }
                Err(e) => {
                    for i in slots {
                        results[*i] = Some(Err(e.clone()));
                    }
                }
            }
            start = end;
        }
        let results: Vec<BatchEntryResult> = results
            .into_iter()
            .map(|r| r.unwrap_or_else(|| Err(SqsError::Service("entry was not sent".to_owned()))))
            .collect();
        let ok = results.iter().filter(|r| r.is_ok()).count() as u64;
        let bad = results.len() as u64 - ok;
        self.inner
            .metrics
            .update(&self.inner.config.label_for(queue), |c| {
                c.sent += ok;
                c.send_errors += bad;
            });
        Ok(results)
    }

    /// Sends to a resolved URL. `label` names the queue in metrics. Use an
    /// alias or a fixed name, never a raw URL: labels are kept forever.
    pub(crate) async fn send_to_url(
        &self,
        label: &str,
        url: &str,
        message: OutboundMessage,
    ) -> Result<String, SqsError> {
        let result = match validate_outbound(url, &message) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::MemoryTransport;

    fn producer() -> SqsProducer {
        let mut cfg = SqsConfig::default();
        cfg.queues
            .insert("events".into(), "https://q/events".into());
        let t = MemoryTransport::new().with_queue("https://q/events");
        SqsProducer::new(Arc::new(t), cfg, Arc::new(SqsMetrics::new()))
    }

    #[tokio::test]
    async fn metric_label_is_alias_or_unconfigured() {
        let p = producer();
        p.send("events", OutboundMessage::new("a")).await.unwrap();
        p.send("https://q/events", OutboundMessage::new("b"))
            .await
            .unwrap();
        let _ = p.send("https://q/other-1", OutboundMessage::new("c")).await;
        let _ = p.send("https://q/other-2", OutboundMessage::new("d")).await;
        let snap = p.inner.metrics.snapshot();
        assert_eq!(snap["events"].sent, 2);
        assert_eq!(snap["unconfigured"].send_errors, 2);
        assert_eq!(snap.len(), 2, "no label per raw URL");
    }
}
