//! Job message format.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Attribute with the job name.
pub const ATTR_JOB: &str = "autumn-job";
/// Attribute with the envelope kind.
pub const ATTR_KIND: &str = "autumn-kind";
/// Value of [`ATTR_KIND`] for this format.
pub const KIND_JOB_V1: &str = "job/1";
/// Attribute with the dead-letter reason.
pub const ATTR_DEAD_REASON: &str = "autumn-dead-letter-reason";
/// Attribute with the source queue of a dead letter.
pub const ATTR_DEAD_SOURCE: &str = "autumn-source-queue";
/// Attribute with the attempts before dead-lettering.
pub const ATTR_DEAD_ATTEMPTS: &str = "autumn-attempts";

/// Job message body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobEnvelope {
    /// Format version. Always 1.
    pub v: u32,
    /// Registered `#[job]` name.
    pub job: String,
    /// Job args, as the handler reads them.
    pub payload: Value,
    /// Unix seconds before which the job must not run. Set only for delays
    /// over the SQS limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<i64>,
    /// Unix seconds at first enqueue.
    pub enqueued_at: i64,
}

impl JobEnvelope {
    /// Parses a body. Returns `None` for a body that is not a v1 job envelope.
    #[must_use]
    pub fn parse(body: &str) -> Option<Self> {
        serde_json::from_str::<Self>(body).ok().filter(|e| e.v == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_omits_empty_not_before() {
        let e = JobEnvelope {
            v: 1,
            job: "j".into(),
            payload: serde_json::json!({"n": 1}),
            not_before: None,
            enqueued_at: 10,
        };
        let s = serde_json::to_string(&e).unwrap();
        assert!(!s.contains("not_before"));
        assert_eq!(JobEnvelope::parse(&s), Some(e));
    }

    #[test]
    fn parse_rejects_other_versions_and_junk() {
        assert!(JobEnvelope::parse(r#"{"v":2,"job":"j","payload":1,"enqueued_at":0}"#).is_none());
        assert!(JobEnvelope::parse("nope").is_none());
        assert!(JobEnvelope::parse(r#"{"v":1}"#).is_none());
    }
}
