//! Counters and gauges for `/actuator/prometheus`.

use std::collections::BTreeMap;
use std::sync::Mutex;

use autumn_web::actuator::{MetricFamily, MetricKind, MetricSample, MetricsSource};

use crate::transport::QueueStats;

/// Counters for one queue label.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct QueueCounters {
    /// Messages received.
    pub received: u64,
    /// Messages handled and deleted.
    pub succeeded: u64,
    /// Failures with a retry scheduled.
    pub retried: u64,
    /// Messages sent to the dead-letter queue.
    pub dead_lettered: u64,
    /// Messages that failed with no retry: bad data, unknown job, or panic.
    pub poisoned: u64,
    /// Dead letters left for the SQS redrive policy (no DLQ is set).
    pub redrive_deferred: u64,
    /// Delay hops for jobs due in more than 15 minutes.
    pub hops: u64,
    /// Visibility extensions.
    pub heartbeats: u64,
    /// Messages sent.
    pub sent: u64,
    /// Send failures.
    pub send_errors: u64,
    /// Receive failures.
    pub receive_errors: u64,
    /// Delete or visibility failures.
    pub ack_errors: u64,
    /// Handlers that run now.
    pub in_flight: u64,
    /// Last queue counters, if sampled.
    pub stats: Option<QueueStats>,
}

/// Metrics for all queues. Also a [`MetricsSource`].
#[derive(Debug, Default)]
pub struct SqsMetrics {
    queues: Mutex<BTreeMap<String, QueueCounters>>,
}

impl SqsMetrics {
    /// Makes empty metrics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Changes the counters for `queue`.
    pub(crate) fn update(&self, queue: &str, f: impl FnOnce(&mut QueueCounters)) {
        let mut map = self
            .queues
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(map.entry(queue.to_owned()).or_default());
    }

    /// Returns a copy of all counters, by queue label.
    #[must_use]
    pub fn snapshot(&self) -> BTreeMap<String, QueueCounters> {
        self.queues
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

type Pick = fn(&QueueCounters) -> u64;

const COUNTERS: &[(&str, &str, Pick)] = &[
    (
        "aws_sqs_messages_received_total",
        "Messages received.",
        |c| c.received,
    ),
    (
        "aws_sqs_messages_succeeded_total",
        "Messages handled and deleted.",
        |c| c.succeeded,
    ),
    (
        "aws_sqs_messages_retried_total",
        "Failures with a retry scheduled.",
        |c| c.retried,
    ),
    (
        "aws_sqs_messages_dead_lettered_total",
        "Messages sent to the dead-letter queue.",
        |c| c.dead_lettered,
    ),
    (
        "aws_sqs_messages_poisoned_total",
        "Messages that failed with no retry.",
        |c| c.poisoned,
    ),
    (
        "aws_sqs_messages_redrive_deferred_total",
        "Dead letters left for the SQS redrive policy.",
        |c| c.redrive_deferred,
    ),
    (
        "aws_sqs_delay_hops_total",
        "Delay hops for long delays.",
        |c| c.hops,
    ),
    ("aws_sqs_heartbeats_total", "Visibility extensions.", |c| {
        c.heartbeats
    }),
    ("aws_sqs_messages_sent_total", "Messages sent.", |c| c.sent),
    ("aws_sqs_send_errors_total", "Send failures.", |c| {
        c.send_errors
    }),
    ("aws_sqs_receive_errors_total", "Receive failures.", |c| {
        c.receive_errors
    }),
    (
        "aws_sqs_ack_errors_total",
        "Delete or visibility failures.",
        |c| c.ack_errors,
    ),
];

#[allow(clippy::cast_precision_loss)] // Counters stay far below 2^53.
const fn as_f64(v: u64) -> f64 {
    v as f64
}

fn label(queue: &str) -> Vec<(String, String)> {
    vec![("queue".to_owned(), queue.to_owned())]
}

impl MetricsSource for SqsMetrics {
    fn collect(&self) -> Vec<MetricFamily> {
        let snap = self.snapshot();
        let mut families: Vec<MetricFamily> = COUNTERS
            .iter()
            .map(|(name, help, pick)| MetricFamily {
                name: (*name).to_owned(),
                help: (*help).to_owned(),
                kind: MetricKind::Counter,
                samples: snap
                    .iter()
                    .map(|(q, c)| MetricSample {
                        labels: label(q),
                        value: as_f64(pick(c)),
                    })
                    .collect(),
            })
            .collect();
        families.push(MetricFamily {
            name: "aws_sqs_handlers_in_flight".to_owned(),
            help: "Handlers that run now.".to_owned(),
            kind: MetricKind::Gauge,
            samples: snap
                .iter()
                .map(|(q, c)| MetricSample {
                    labels: label(q),
                    value: as_f64(c.in_flight),
                })
                .collect(),
        });
        let mut depth = Vec::new();
        for (q, c) in &snap {
            if let Some(s) = &c.stats {
                for (state, v) in [
                    ("visible", s.visible),
                    ("in_flight", s.in_flight),
                    ("delayed", s.delayed),
                ] {
                    let mut labels = label(q);
                    labels.push(("state".to_owned(), state.to_owned()));
                    depth.push(MetricSample {
                        labels,
                        value: as_f64(v),
                    });
                }
            }
        }
        families.push(MetricFamily {
            name: "aws_sqs_queue_messages".to_owned(),
            help: "Approximate messages in the queue, by state.".to_owned(),
            kind: MetricKind::Gauge,
            samples: depth,
        });
        families
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_and_snapshot() {
        let m = SqsMetrics::new();
        m.update("a", |c| c.sent += 2);
        m.update("a", |c| c.sent += 1);
        assert_eq!(m.snapshot()["a"].sent, 3);
    }

    #[test]
    fn collect_has_counter_and_gauges() {
        let m = SqsMetrics::new();
        m.update("q", |c| {
            c.received = 5;
            c.in_flight = 2;
            c.stats = Some(QueueStats {
                visible: 7,
                ..QueueStats::default()
            });
        });
        let f = m.collect();
        let received = f
            .iter()
            .find(|f| f.name == "aws_sqs_messages_received_total")
            .unwrap();
        assert!((received.samples[0].value - 5.0).abs() < f64::EPSILON);
        let depth = f
            .iter()
            .find(|f| f.name == "aws_sqs_queue_messages")
            .unwrap();
        assert_eq!(depth.samples.len(), 3);
        let names: std::collections::HashSet<_> = f.iter().map(|f| f.name.clone()).collect();
        assert_eq!(names.len(), f.len(), "family names are unique");
    }
}
