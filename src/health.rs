//! Health indicator for `/actuator/health`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use autumn_web::actuator::{HealthCheckOutput, HealthIndicator, HealthStatus, IndicatorGroup};
use futures::future::BoxFuture;
use serde_json::json;

use std::time::Duration;

use crate::error::SqsError;
use crate::transport::SqsTransport;

pub(crate) struct HealthTarget {
    pub transport: Arc<dyn SqsTransport>,
    /// Label to queue URL.
    pub queues: BTreeMap<String, String>,
}

/// Checks each queue with `GetQueueAttributes`.
///
/// Status is `Up` when all queues answer and `Down` when one fails or takes
/// more than 1.5 s. It is `Unknown` before the plugin starts. Details give
/// counts and a short error class only.
pub struct SqsHealth {
    target: OnceLock<HealthTarget>,
    readiness: bool,
}

impl std::fmt::Debug for SqsHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsHealth")
            .field("started", &self.target.get().is_some())
            .field("readiness", &self.readiness)
            .finish()
    }
}

impl SqsHealth {
    pub(crate) const fn new(readiness: bool) -> Self {
        Self {
            target: OnceLock::new(),
            readiness,
        }
    }

    /// Sets the target once. Later calls do nothing.
    pub(crate) fn set(&self, target: HealthTarget) {
        let _ = self.target.set(target);
    }
}

/// Time limit per queue check. It is under the 2 s indicator timeout.
const CHECK_TIMEOUT: Duration = Duration::from_millis(1_500);

/// A short error class for public health output. It has no URL, account ID,
/// or ARN.
fn error_class(err: &SqsError) -> &'static str {
    match err {
        SqsError::QueueNotFound(_) => "not_found",
        SqsError::Service(m) if m.contains("AccessDenied") => "access_denied",
        _ => "error",
    }
}

impl HealthIndicator for SqsHealth {
    fn check(&self) -> BoxFuture<'_, HealthCheckOutput> {
        Box::pin(async move {
            let Some(target) = self.target.get() else {
                return HealthCheckOutput {
                    status: HealthStatus::Unknown,
                    details: HashMap::from([("reason".to_owned(), json!("not started"))]),
                };
            };
            let checks = target.queues.iter().map(|(label, url)| async move {
                let result =
                    tokio::time::timeout(CHECK_TIMEOUT, target.transport.queue_stats(url)).await;
                (label.clone(), result)
            });
            let mut details = HashMap::new();
            let mut up = true;
            for (label, result) in futures::future::join_all(checks).await {
                let detail = match result {
                    Ok(Ok(s)) => json!({
                        "visible": s.visible,
                        "in_flight": s.in_flight,
                        "delayed": s.delayed,
                        "redrive": s.redrive_target.is_some(),
                    }),
                    Ok(Err(e)) => {
                        up = false;
                        tracing::warn!(queue = %label, error = %e, "aws_sqs health check failed");
                        json!({ "error": error_class(&e) })
                    }
                    Err(_) => {
                        up = false;
                        json!({ "error": "timeout" })
                    }
                };
                details.insert(label, detail);
            }
            let out = if up {
                HealthCheckOutput::up()
            } else {
                HealthCheckOutput::down()
            };
            out.with_details(details)
        })
    }

    fn group(&self) -> IndicatorGroup {
        if self.readiness {
            IndicatorGroup::Readiness
        } else {
            IndicatorGroup::HealthOnly
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::MemoryTransport;

    #[tokio::test]
    async fn unknown_before_start() {
        let h = SqsHealth::new(false);
        assert_eq!(h.check().await.status, HealthStatus::Unknown);
        assert!(matches!(h.group(), IndicatorGroup::HealthOnly));
        assert!(matches!(
            SqsHealth::new(true).group(),
            IndicatorGroup::Readiness
        ));
    }

    #[tokio::test]
    async fn up_and_down() {
        let t = Arc::new(MemoryTransport::new().with_queue("https://q/a"));
        let h = SqsHealth::new(false);
        h.set(HealthTarget {
            transport: t,
            queues: BTreeMap::from([
                ("a".to_owned(), "https://q/a".to_owned()),
                ("b".to_owned(), "https://q/b".to_owned()),
            ]),
        });
        let out = h.check().await;
        assert_eq!(out.status, HealthStatus::Down);
        assert_eq!(out.details["b"]["error"], "not_found");
        assert!(
            !serde_json::to_string(&out.details)
                .unwrap()
                .contains("https://")
        );
        assert_eq!(out.details["a"]["visible"], 0);
    }
}
