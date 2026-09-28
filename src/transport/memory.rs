//! In-memory SQS fake for tests and local runs.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use super::{
    BatchEntryResult, BoxFuture, OutboundMessage, QueueStats, ReceiveOptions, ReceivedMessage,
    SqsTransport, is_fifo, validate_outbound,
};
use crate::error::SqsError;
use crate::policy::{MAX_BATCH, MAX_BATCH_BYTES, MAX_VISIBILITY_SECS};

/// A copy of one stored message, for test assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSnapshot {
    /// SQS message ID.
    pub message_id: String,
    /// Message body.
    pub body: String,
    /// Times the message was received.
    pub receive_count: u32,
    /// String message attributes.
    pub attributes: BTreeMap<String, String>,
    /// FIFO group ID.
    pub group_id: Option<String>,
    /// `true` while a consumer holds the message.
    pub in_flight: bool,
}

/// In-memory SQS fake.
///
/// It follows the SQS rules this crate uses: visibility timeout, receive
/// count, delay, long poll, FIFO groups and deduplication, and redrive.
/// Time comes from `tokio::time`, so `tokio::time::pause` works.
#[derive(Clone, Default)]
pub struct MemoryTransport {
    inner: Arc<Mutex<State>>,
    notify: Arc<Notify>,
}

#[derive(Default)]
struct State {
    queues: HashMap<String, Queue>,
    next_id: u64,
}

impl State {
    const fn next(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    fn queue(&mut self, url: &str) -> Result<&mut Queue, SqsError> {
        self.queues
            .get_mut(url)
            .ok_or_else(|| SqsError::QueueNotFound(url.to_owned()))
    }
}

#[derive(Default)]
struct Queue {
    messages: Vec<Stored>,
    redrive: Option<(String, u32)>,
    dedup: HashMap<String, (String, Instant)>,
    content_dedup: bool,
}

struct Stored {
    id: String,
    body: String,
    attributes: BTreeMap<String, String>,
    group_id: Option<String>,
    visible_at: Instant,
    receive_count: u32,
    receipt: Option<String>,
    /// Time of the last receive. SQS allows 12 h of visibility from it.
    received_at: Option<Instant>,
}

impl Stored {
    fn in_flight(&self, now: Instant) -> bool {
        self.receipt.is_some() && self.visible_at > now
    }

    fn delayed(&self, now: Instant) -> bool {
        self.receipt.is_none() && self.visible_at > now
    }
}

/// FIFO deduplication window.
const DEDUP_WINDOW: Duration = Duration::from_secs(300);

fn check_visibility(secs: u64) -> Result<(), SqsError> {
    if secs > MAX_VISIBILITY_SECS {
        return Err(SqsError::InvalidRequest(format!(
            "VisibilityTimeout {secs} is over {MAX_VISIBILITY_SECS}"
        )));
    }
    Ok(())
}

impl MemoryTransport {
    /// Makes an empty fake with no queues.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a queue and returns `self`.
    #[must_use]
    pub fn with_queue(self, queue_url: &str) -> Self {
        self.create_queue(queue_url);
        self
    }

    /// Adds a queue. An existing queue does not change.
    pub fn create_queue(&self, queue_url: &str) {
        self.lock().queues.entry(queue_url.to_owned()).or_default();
    }

    /// Turns on content-based deduplication for a FIFO queue.
    #[must_use]
    pub fn with_content_dedup(self, queue_url: &str) -> Self {
        self.lock()
            .queues
            .entry(queue_url.to_owned())
            .or_default()
            .content_dedup = true;
        self
    }

    /// Sets a redrive policy: after `max_receive_count` receives, the next
    /// receive moves the message to `dead_letter_url`.
    ///
    /// # Panics
    /// Panics when one queue is FIFO and the other is not. SQS rejects that.
    #[must_use]
    pub fn with_redrive(
        self,
        queue_url: &str,
        dead_letter_url: &str,
        max_receive_count: u32,
    ) -> Self {
        assert!(
            is_fifo(queue_url) == is_fifo(dead_letter_url),
            "the source queue and the dead-letter queue must be the same type (FIFO or standard)"
        );
        self.lock()
            .queues
            .entry(queue_url.to_owned())
            .or_default()
            .redrive = Some((dead_letter_url.to_owned(), max_receive_count));
        self
    }

    /// Returns a copy of all messages in the queue, in send order.
    ///
    /// Returns an empty list for an unknown queue.
    #[must_use]
    pub fn messages(&self, queue_url: &str) -> Vec<MessageSnapshot> {
        let now = Instant::now();
        self.lock()
            .queues
            .get(queue_url)
            .map_or_else(Vec::new, |q| {
                q.messages
                    .iter()
                    .map(|m| MessageSnapshot {
                        message_id: m.id.clone(),
                        body: m.body.clone(),
                        receive_count: m.receive_count,
                        attributes: m.attributes.clone(),
                        group_id: m.group_id.clone(),
                        in_flight: m.in_flight(now),
                    })
                    .collect()
            })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // A panic while the lock is held leaves plain data; keep using it.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn send_now(&self, queue_url: &str, message: OutboundMessage) -> Result<String, SqsError> {
        validate_outbound(queue_url, &message)?;
        let now = Instant::now();
        let mut state = self.lock();
        let n = state.next();
        let queue = state.queue(queue_url)?;
        if is_fifo(queue_url) {
            let key = match (&message.dedup_id, queue.content_dedup) {
                (Some(id), _) => id.clone(),
                (None, true) => format!("body:{}", message.body),
                (None, false) => {
                    return Err(SqsError::InvalidRequest(
                        "FIFO queue needs MessageDeduplicationId or content-based deduplication"
                            .to_owned(),
                    ));
                }
            };
            queue
                .dedup
                .retain(|_, (_, at)| now.duration_since(*at) < DEDUP_WINDOW);
            if let Some((id, _)) = queue.dedup.get(&key) {
                return Ok(id.clone());
            }
            queue.dedup.insert(key, (format!("m-{n}"), now));
        }
        let id = format!("m-{n}");
        queue.messages.push(Stored {
            id: id.clone(),
            body: message.body,
            attributes: message.attributes,
            group_id: message.group_id,
            visible_at: now + Duration::from_secs(message.delay_secs),
            receive_count: 0,
            receipt: None,
            received_at: None,
        });
        drop(state);
        self.notify.notify_waiters();
        Ok(id)
    }

    /// Takes up to `max` visible messages. Returns the next wake time if none.
    fn take(
        &self,
        queue_url: &str,
        options: ReceiveOptions,
    ) -> Result<(Vec<ReceivedMessage>, Option<Instant>), SqsError> {
        let now = Instant::now();
        let mut state = self.lock();
        let fifo = is_fifo(queue_url);
        let redrive = state.queue(queue_url)?.redrive.clone();

        // Move messages over the redrive limit first. A missing DLQ moves nothing.
        let mut moved = false;
        if let Some((dlq, max)) = &redrive
            && state.queues.contains_key(dlq)
        {
            let queue = state.queue(queue_url)?;
            let mut dead = Vec::new();
            let mut i = 0;
            while i < queue.messages.len() {
                let m = &queue.messages[i];
                if m.visible_at <= now && m.receive_count >= *max {
                    dead.push(queue.messages.remove(i));
                } else {
                    i += 1;
                }
            }
            moved = !dead.is_empty();
            let target = state.queue(dlq)?;
            for mut m in dead {
                m.receive_count = 0;
                m.receipt = None;
                m.received_at = None;
                m.visible_at = now;
                target.messages.push(m);
            }
        }

        let mut receipts = Vec::new();
        for _ in 0..options.max_messages {
            receipts.push(state.next());
        }
        let queue = state.queue(queue_url)?;
        // FIFO: like SQS, a group with a message in flight gives nothing. A
        // group with a delayed message gives nothing after that message.
        // Several visible messages of one group come in order.
        let mut blocked: Vec<String> = if fifo {
            queue
                .messages
                .iter()
                .filter(|m| m.in_flight(now))
                .filter_map(|m| m.group_id.clone())
                .collect()
        } else {
            Vec::new()
        };
        let mut out = Vec::new();
        let mut next_wake: Option<Instant> = None;
        let mut receipts = receipts.into_iter();
        for m in &mut queue.messages {
            if out.len() >= options.max_messages as usize {
                break;
            }
            if m.visible_at > now {
                // Wake when this message becomes visible, also in a locked group.
                next_wake = Some(next_wake.map_or(m.visible_at, |w| w.min(m.visible_at)));
            }
            let group = m.group_id.clone().filter(|_| fifo);
            if let Some(g) = &group
                && blocked.contains(g)
            {
                continue;
            }
            if m.visible_at > now {
                if let Some(g) = group {
                    blocked.push(g);
                }
                continue;
            }
            let Some(n) = receipts.next() else { break };
            let receipt = format!("rh-{}-{n}", m.id);
            m.receive_count += 1;
            m.receipt = Some(receipt.clone());
            m.received_at = Some(now);
            m.visible_at = now + Duration::from_secs(options.visibility_secs);
            out.push(ReceivedMessage {
                message_id: m.id.clone(),
                receipt_handle: receipt,
                body: m.body.clone(),
                receive_count: Some(m.receive_count),
                attributes: m.attributes.clone(),
                group_id: m.group_id.clone(),
            });
        }
        drop(state);
        if moved {
            self.notify.notify_waiters();
        }
        Ok((out, next_wake))
    }

    /// Runs `f` on the message with this current receipt handle.
    ///
    /// Returns `Ok(None)` for an old receipt handle of this fake, like SQS
    /// accepts an old handle. Returns an error for a handle it never made.
    fn with_receipt<T>(
        &self,
        queue_url: &str,
        receipt_handle: &str,
        f: impl FnOnce(&mut Queue, usize, Instant) -> T,
    ) -> Result<Option<T>, SqsError> {
        let now = Instant::now();
        let mut state = self.lock();
        let queue = state.queue(queue_url)?;
        let Some(idx) = queue
            .messages
            .iter()
            .position(|m| m.receipt.as_deref() == Some(receipt_handle))
        else {
            if receipt_handle.starts_with("rh-") {
                return Ok(None);
            }
            return Err(SqsError::InvalidRequest(format!(
                "receipt handle is not valid: {receipt_handle}"
            )));
        };
        let out = f(queue, idx, now);
        drop(state);
        self.notify.notify_waiters();
        Ok(Some(out))
    }
}

impl SqsTransport for MemoryTransport {
    fn send<'a>(
        &'a self,
        queue_url: &'a str,
        message: OutboundMessage,
    ) -> BoxFuture<'a, Result<String, SqsError>> {
        Box::pin(async move { self.send_now(queue_url, message) })
    }

    fn send_batch<'a>(
        &'a self,
        queue_url: &'a str,
        messages: Vec<OutboundMessage>,
    ) -> BoxFuture<'a, Result<Vec<BatchEntryResult>, SqsError>> {
        Box::pin(async move {
            if messages.is_empty() || messages.len() > MAX_BATCH {
                return Err(SqsError::InvalidRequest(format!(
                    "batch needs 1 to {MAX_BATCH} entries, not {}",
                    messages.len()
                )));
            }
            self.lock().queue(queue_url)?;
            let total: usize = messages.iter().map(OutboundMessage::size_bytes).sum();
            if total > MAX_BATCH_BYTES {
                return Err(SqsError::InvalidRequest(format!(
                    "BatchRequestTooLong: {total} bytes; the limit is {MAX_BATCH_BYTES}"
                )));
            }
            Ok(messages
                .into_iter()
                .map(|m| self.send_now(queue_url, m))
                .collect())
        })
    }

    fn receive<'a>(
        &'a self,
        queue_url: &'a str,
        options: ReceiveOptions,
    ) -> BoxFuture<'a, Result<Vec<ReceivedMessage>, SqsError>> {
        Box::pin(async move {
            if !(1..=10).contains(&options.max_messages) || options.wait_secs > 20 {
                return Err(SqsError::InvalidRequest(
                    "MaxNumberOfMessages must be 1 to 10; WaitTimeSeconds 0 to 20".to_owned(),
                ));
            }
            check_visibility(options.visibility_secs)?;
            let deadline = Instant::now() + Duration::from_secs(options.wait_secs);
            loop {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let (out, next_wake) = self.take(queue_url, options)?;
                if !out.is_empty() || Instant::now() >= deadline {
                    return Ok(out);
                }
                let wake = next_wake.map_or(deadline, |w| w.min(deadline));
                tokio::select! {
                    () = &mut notified => {}
                    () = tokio::time::sleep_until(wake) => {}
                }
            }
        })
    }

    fn delete<'a>(
        &'a self,
        queue_url: &'a str,
        receipt_handle: &'a str,
    ) -> BoxFuture<'a, Result<(), SqsError>> {
        Box::pin(async move {
            self.with_receipt(queue_url, receipt_handle, |queue, idx, _| {
                queue.messages.remove(idx);
            })
            .map(|_| ())
        })
    }

    fn change_visibility<'a>(
        &'a self,
        queue_url: &'a str,
        receipt_handle: &'a str,
        visibility_secs: u64,
    ) -> BoxFuture<'a, Result<(), SqsError>> {
        Box::pin(async move {
            check_visibility(visibility_secs)?;
            self.with_receipt(queue_url, receipt_handle, |queue, idx, now| {
                let m = &mut queue.messages[idx];
                if !m.in_flight(now) {
                    return Err(SqsError::InvalidRequest(
                        "message is not in flight".to_owned(),
                    ));
                }
                // SQS allows 12 h of visibility from the receive.
                let used = m
                    .received_at
                    .map_or(0, |at| now.duration_since(at).as_secs());
                if used + visibility_secs > MAX_VISIBILITY_SECS {
                    return Err(SqsError::InvalidRequest(format!(
                        "VisibilityTimeout {visibility_secs} is over the time left ({} s)",
                        MAX_VISIBILITY_SECS.saturating_sub(used)
                    )));
                }
                m.visible_at = now + Duration::from_secs(visibility_secs);
                Ok(())
            })?
            .ok_or_else(|| SqsError::InvalidRequest("receipt handle is not current".to_owned()))?
        })
    }

    fn queue_stats<'a>(
        &'a self,
        queue_url: &'a str,
    ) -> BoxFuture<'a, Result<QueueStats, SqsError>> {
        Box::pin(async move {
            let now = Instant::now();
            let mut state = self.lock();
            let queue = state.queue(queue_url)?;
            let mut counts = QueueStats {
                redrive_target: queue.redrive.as_ref().map(|(dlq, _)| dlq.clone()),
                ..QueueStats::default()
            };
            for m in &queue.messages {
                if m.in_flight(now) {
                    counts.in_flight += 1;
                } else if m.delayed(now) {
                    counts.delayed += 1;
                } else {
                    counts.visible += 1;
                }
            }
            drop(state);
            Ok(counts)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const Q: &str = "https://sqs.local/000/q";
    const DLQ: &str = "https://sqs.local/000/dlq";
    const FIFO: &str = "https://sqs.local/000/q.fifo";

    fn opts(visibility_secs: u64) -> ReceiveOptions {
        ReceiveOptions {
            max_messages: 10,
            wait_secs: 0,
            visibility_secs,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn send_then_receive_counts_one() {
        let t = MemoryTransport::new().with_queue(Q);
        let id = t
            .send(Q, OutboundMessage::new("hi").attribute("k", "v"))
            .await
            .unwrap();
        let got = t.receive(Q, opts(30)).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].message_id, id);
        assert_eq!(got[0].body, "hi");
        assert_eq!(got[0].receive_count, Some(1));
        assert_eq!(got[0].attributes.get("k").map(String::as_str), Some("v"));
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_queue_is_not_found() {
        let t = MemoryTransport::new();
        let err = t.send(Q, OutboundMessage::new("x")).await.unwrap_err();
        assert_eq!(err, SqsError::QueueNotFound(Q.to_owned()));
        assert!(matches!(
            t.queue_stats(Q).await,
            Err(SqsError::QueueNotFound(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn visibility_hides_then_redelivers() {
        let t = MemoryTransport::new().with_queue(Q);
        t.send(Q, OutboundMessage::new("a")).await.unwrap();
        let first = t.receive(Q, opts(30)).await.unwrap();
        assert!(t.receive(Q, opts(30)).await.unwrap().is_empty());
        tokio::time::advance(Duration::from_secs(31)).await;
        let second = t.receive(Q, opts(30)).await.unwrap();
        assert_eq!(second[0].receive_count, Some(2));
        assert_ne!(second[0].receipt_handle, first[0].receipt_handle);
    }

    #[tokio::test(start_paused = true)]
    async fn delete_removes_and_rejects_stale_receipt() {
        let t = MemoryTransport::new().with_queue(Q);
        t.send(Q, OutboundMessage::new("a")).await.unwrap();
        let first = t.receive(Q, opts(1)).await.unwrap();
        tokio::time::advance(Duration::from_secs(2)).await;
        let second = t.receive(Q, opts(30)).await.unwrap();
        // Like SQS: an old receipt succeeds but does not delete.
        t.delete(Q, &first[0].receipt_handle).await.unwrap();
        assert_eq!(t.messages(Q).len(), 1);
        assert!(matches!(
            t.delete(Q, "junk").await,
            Err(SqsError::InvalidRequest(_))
        ));
        t.delete(Q, &second[0].receipt_handle).await.unwrap();
        assert!(t.messages(Q).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn delay_hides_until_due() {
        let t = MemoryTransport::new().with_queue(Q);
        t.send(Q, OutboundMessage::new("a").delay_secs(60))
            .await
            .unwrap();
        assert!(t.receive(Q, opts(30)).await.unwrap().is_empty());
        assert_eq!(t.queue_stats(Q).await.unwrap().delayed, 1);
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(t.receive(Q, opts(30)).await.unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn delay_over_limit_is_invalid() {
        let t = MemoryTransport::new().with_queue(Q);
        let err = t
            .send(Q, OutboundMessage::new("a").delay_secs(901))
            .await
            .unwrap_err();
        assert!(matches!(err, SqsError::InvalidRequest(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn change_visibility_zero_makes_visible() {
        let t = MemoryTransport::new().with_queue(Q);
        t.send(Q, OutboundMessage::new("a")).await.unwrap();
        let got = t.receive(Q, opts(300)).await.unwrap();
        t.change_visibility(Q, &got[0].receipt_handle, 0)
            .await
            .unwrap();
        assert_eq!(t.receive(Q, opts(30)).await.unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn change_visibility_rejects_over_limit_and_stale() {
        let t = MemoryTransport::new().with_queue(Q);
        t.send(Q, OutboundMessage::new("a")).await.unwrap();
        let got = t.receive(Q, opts(30)).await.unwrap();
        let over = t.change_visibility(Q, &got[0].receipt_handle, 43_201).await;
        assert!(matches!(over, Err(SqsError::InvalidRequest(_))));
        let stale = t.change_visibility(Q, "nope", 10).await;
        assert!(matches!(stale, Err(SqsError::InvalidRequest(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn long_poll_wakes_on_send() {
        let t = MemoryTransport::new().with_queue(Q);
        let t2 = t.clone();
        let poll = tokio::spawn(async move {
            t2.receive(
                Q,
                ReceiveOptions {
                    max_messages: 1,
                    wait_secs: 20,
                    visibility_secs: 30,
                },
            )
            .await
        });
        tokio::time::advance(Duration::from_secs(5)).await;
        t.send(Q, OutboundMessage::new("late")).await.unwrap();
        let got = poll.await.unwrap().unwrap();
        assert_eq!(got.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn long_poll_times_out_empty() {
        let t = MemoryTransport::new().with_queue(Q);
        let start = tokio::time::Instant::now();
        let got = t
            .receive(
                Q,
                ReceiveOptions {
                    max_messages: 1,
                    wait_secs: 3,
                    visibility_secs: 30,
                },
            )
            .await
            .unwrap();
        assert!(got.is_empty());
        assert!(start.elapsed() >= Duration::from_secs(3));
    }

    #[tokio::test(start_paused = true)]
    async fn long_poll_wakes_when_delay_ends() {
        let t = MemoryTransport::new().with_queue(Q);
        t.send(Q, OutboundMessage::new("a").delay_secs(2))
            .await
            .unwrap();
        let got = t
            .receive(
                Q,
                ReceiveOptions {
                    max_messages: 1,
                    wait_secs: 20,
                    visibility_secs: 30,
                },
            )
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn receive_respects_max_messages() {
        let t = MemoryTransport::new().with_queue(Q);
        for i in 0..5 {
            t.send(Q, OutboundMessage::new(format!("m{i}")))
                .await
                .unwrap();
        }
        let got = t
            .receive(
                Q,
                ReceiveOptions {
                    max_messages: 2,
                    wait_secs: 0,
                    visibility_secs: 30,
                },
            )
            .await
            .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].body, "m0");
    }

    #[tokio::test(start_paused = true)]
    async fn fifo_requires_group_and_rejects_delay() {
        let t = MemoryTransport::new().with_queue(FIFO);
        let no_group = t.send(FIFO, OutboundMessage::new("a").dedup_id("d")).await;
        assert!(matches!(no_group, Err(SqsError::InvalidRequest(_))));
        let delayed = t
            .send(
                FIFO,
                OutboundMessage::new("a")
                    .group_id("g")
                    .dedup_id("d")
                    .delay_secs(5),
            )
            .await;
        assert!(matches!(delayed, Err(SqsError::FifoDelay(_))));
        let no_dedup = t.send(FIFO, OutboundMessage::new("a").group_id("g")).await;
        assert!(matches!(no_dedup, Err(SqsError::InvalidRequest(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn fifo_content_dedup_uses_body() {
        let t = MemoryTransport::new()
            .with_queue(FIFO)
            .with_content_dedup(FIFO);
        let m = || OutboundMessage::new("same").group_id("g");
        let a = t.send(FIFO, m()).await.unwrap();
        let b = t.send(FIFO, m()).await.unwrap();
        assert_eq!(a, b);
    }

    #[tokio::test(start_paused = true)]
    async fn fifo_dedups_within_window() {
        let t = MemoryTransport::new().with_queue(FIFO);
        let m = || OutboundMessage::new("a").group_id("g").dedup_id("same");
        let first = t.send(FIFO, m()).await.unwrap();
        let second = t.send(FIFO, m()).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(t.messages(FIFO).len(), 1);
        tokio::time::advance(Duration::from_secs(301)).await;
        let third = t.send(FIFO, m()).await.unwrap();
        assert_ne!(first, third);
    }

    #[tokio::test(start_paused = true)]
    async fn fifo_group_blocks_while_in_flight() {
        let t = MemoryTransport::new().with_queue(FIFO);
        for (g, d) in [("g1", "1"), ("g1", "2"), ("g2", "3")] {
            t.send(FIFO, OutboundMessage::new(d).group_id(g).dedup_id(d))
                .await
                .unwrap();
        }
        // Like SQS: one receive can hold several messages of one group, in order.
        let got = t.receive(FIFO, opts(30)).await.unwrap();
        let bodies: Vec<_> = got.iter().map(|m| m.body.as_str()).collect();
        assert_eq!(bodies, vec!["1", "2", "3"]);
        assert_eq!(got[0].group_id.as_deref(), Some("g1"));
        // A group with a message in flight gives nothing more.
        t.send(FIFO, OutboundMessage::new("4").group_id("g1").dedup_id("4"))
            .await
            .unwrap();
        assert!(t.receive(FIFO, opts(30)).await.unwrap().is_empty());
        // When the head comes back, the group comes back in order.
        t.change_visibility(FIFO, &got[0].receipt_handle, 0)
            .await
            .unwrap();
        t.delete(FIFO, &got[1].receipt_handle).await.unwrap();
        t.delete(FIFO, &got[2].receipt_handle).await.unwrap();
        let next = t.receive(FIFO, opts(30)).await.unwrap();
        let bodies: Vec<_> = next.iter().map(|m| m.body.as_str()).collect();
        assert_eq!(bodies, vec!["1", "4"]);
    }

    #[tokio::test(start_paused = true)]
    async fn attribute_rules_follow_sqs() {
        let t = MemoryTransport::new().with_queue(Q);
        let mut m = OutboundMessage::new("x");
        for i in 0..11 {
            m = m.attribute(format!("a{i}"), "v");
        }
        assert!(matches!(
            t.send(Q, m).await,
            Err(SqsError::InvalidRequest(_))
        ));
        let empty = OutboundMessage::new("x").attribute("k", "");
        assert!(matches!(
            t.send(Q, empty).await,
            Err(SqsError::InvalidRequest(_))
        ));
        let bad = OutboundMessage::new("bad \u{0} char");
        assert!(matches!(
            t.send(Q, bad).await,
            Err(SqsError::InvalidRequest(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn batch_over_one_mib_is_rejected() {
        let t = MemoryTransport::new().with_queue(Q);
        let big = || OutboundMessage::new("x".repeat(300_000));
        let err = t.send_batch(Q, vec![big(), big(), big(), big()]).await;
        assert!(
            matches!(err, Err(SqsError::InvalidRequest(ref m)) if m.contains("BatchRequestTooLong"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn visibility_change_respects_twelve_hour_budget() {
        let t = MemoryTransport::new().with_queue(Q);
        t.send(Q, OutboundMessage::new("a")).await.unwrap();
        let got = t.receive(Q, opts(43_200)).await.unwrap();
        tokio::time::advance(Duration::from_secs(43_000)).await;
        let over = t.change_visibility(Q, &got[0].receipt_handle, 300).await;
        assert!(matches!(over, Err(SqsError::InvalidRequest(_))));
        t.change_visibility(Q, &got[0].receipt_handle, 100)
            .await
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn long_poll_wakes_on_visibility_change() {
        let t = MemoryTransport::new().with_queue(Q);
        t.send(Q, OutboundMessage::new("a")).await.unwrap();
        let got = t.receive(Q, opts(300)).await.unwrap();
        let t2 = t.clone();
        let start = tokio::time::Instant::now();
        let poll = tokio::spawn(async move {
            t2.receive(
                Q,
                ReceiveOptions {
                    max_messages: 1,
                    wait_secs: 20,
                    visibility_secs: 30,
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        t.change_visibility(Q, &got[0].receipt_handle, 0)
            .await
            .unwrap();
        assert_eq!(poll.await.unwrap().unwrap().len(), 1);
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn redrive_to_missing_queue_keeps_message() {
        let t = MemoryTransport::new().with_queue(Q).with_redrive(Q, DLQ, 1);
        t.send(Q, OutboundMessage::new("a")).await.unwrap();
        t.receive(Q, opts(1)).await.unwrap();
        tokio::time::advance(Duration::from_secs(2)).await;
        let _ = t.receive(Q, opts(1)).await;
        assert_eq!(t.messages(Q).len(), 1, "no message is lost");
    }

    #[test]
    #[should_panic(expected = "same type")]
    fn redrive_type_must_match() {
        let _ = MemoryTransport::new().with_redrive(FIFO, DLQ, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn redrive_moves_after_max_receives() {
        let t = MemoryTransport::new()
            .with_queue(Q)
            .with_queue(DLQ)
            .with_redrive(Q, DLQ, 2);
        t.send(Q, OutboundMessage::new("poison")).await.unwrap();
        for _ in 0..2 {
            assert_eq!(t.receive(Q, opts(1)).await.unwrap().len(), 1);
            tokio::time::advance(Duration::from_secs(2)).await;
        }
        assert!(t.receive(Q, opts(1)).await.unwrap().is_empty());
        assert_eq!(t.messages(DLQ).len(), 1);
        assert_eq!(t.messages(DLQ)[0].body, "poison");
        assert_eq!(
            t.queue_stats(Q).await.unwrap().redrive_target.as_deref(),
            Some(DLQ)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn batch_reports_per_entry() {
        let t = MemoryTransport::new().with_queue(Q);
        let res = t
            .send_batch(
                Q,
                vec![OutboundMessage::new("a"), OutboundMessage::new("bad \u{0}")],
            )
            .await
            .unwrap();
        assert!(res[0].is_ok());
        assert!(matches!(res[1], Err(SqsError::InvalidRequest(_))));
        assert_eq!(t.messages(Q).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn batch_rejects_empty_and_over_ten() {
        let t = MemoryTransport::new().with_queue(Q);
        assert!(matches!(
            t.send_batch(Q, vec![]).await,
            Err(SqsError::InvalidRequest(_))
        ));
        let eleven = (0..11)
            .map(|i| OutboundMessage::new(i.to_string()))
            .collect();
        assert!(matches!(
            t.send_batch(Q, eleven).await,
            Err(SqsError::InvalidRequest(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn too_large_single_send() {
        let t = MemoryTransport::new().with_queue(Q);
        let big = "x".repeat(super::super::MAX_MESSAGE_BYTES + 1);
        assert!(matches!(
            t.send(Q, OutboundMessage::new(big)).await,
            Err(SqsError::TooLarge { .. })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn stats_count_states() {
        let t = MemoryTransport::new().with_queue(Q);
        t.send(Q, OutboundMessage::new("a")).await.unwrap();
        t.send(Q, OutboundMessage::new("b")).await.unwrap();
        t.send(Q, OutboundMessage::new("c").delay_secs(100))
            .await
            .unwrap();
        t.receive(
            Q,
            ReceiveOptions {
                max_messages: 1,
                wait_secs: 0,
                visibility_secs: 30,
            },
        )
        .await
        .unwrap();
        let s = t.queue_stats(Q).await.unwrap();
        assert_eq!((s.visible, s.in_flight, s.delayed), (1, 1, 1));
        assert_eq!(s.redrive_target, None);
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_shows_in_flight() {
        let t = MemoryTransport::new().with_queue(Q);
        t.send(Q, OutboundMessage::new("a")).await.unwrap();
        assert!(!t.messages(Q)[0].in_flight);
        t.receive(Q, opts(30)).await.unwrap();
        let snap = &t.messages(Q)[0];
        assert!(snap.in_flight);
        assert_eq!(snap.receive_count, 1);
    }
}
