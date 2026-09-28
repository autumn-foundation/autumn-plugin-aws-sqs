//! AWS SDK transport.

use std::collections::BTreeMap;

use aws_sdk_sqs::Client;
use aws_sdk_sqs::error::ProvideErrorMetadata;
use aws_sdk_sqs::types::{
    MessageAttributeValue, MessageSystemAttributeName, QueueAttributeName,
    SendMessageBatchRequestEntry,
};

use super::{
    BatchEntryResult, BoxFuture, OutboundMessage, QueueStats, ReceiveOptions, ReceivedMessage,
    SqsTransport,
};
use crate::config::SqsConfig;
use crate::error::SqsError;

/// SQS transport backed by `aws-sdk-sqs`.
#[derive(Debug, Clone)]
pub struct AwsSqsTransport {
    client: Client,
}

impl AwsSqsTransport {
    /// Wraps an SDK client. Use this for a custom region, role, or endpoint.
    #[must_use]
    pub const fn new(client: Client) -> Self {
        Self { client }
    }

    /// Builds a client from the config.
    ///
    /// Region, endpoint, and credentials come from the config when set.
    /// Otherwise the AWS default chain supplies them.
    ///
    /// # Errors
    /// Returns [`SqsError::Config`] for a bad config or a missing credential
    /// env var.
    pub async fn from_config(config: &SqsConfig) -> Result<Self, SqsError> {
        config.validate()?;
        let credentials = config.credentials(|var| std::env::var(var).ok())?;
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = &config.region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        if let Some(endpoint) = &config.endpoint {
            loader = loader.endpoint_url(endpoint);
        }
        if let Some((key, secret)) = credentials {
            loader = loader.credentials_provider(aws_credential_types::Credentials::new(
                key,
                secret,
                None,
                None,
                "autumn-plugin-aws-sqs",
            ));
        }
        Ok(Self::new(Client::new(&loader.load().await)))
    }

    /// Returns the SDK client.
    #[must_use]
    pub const fn client(&self) -> &Client {
        &self.client
    }
}

/// Maps an SQS error code to [`SqsError`].
pub(crate) fn classify(queue_url: &str, code: Option<&str>, message: Option<&str>) -> SqsError {
    let code = code.unwrap_or("unknown");
    let message = message.unwrap_or("no message");
    let short = code.rsplit('.').next().unwrap_or(code);
    match short {
        "NonExistentQueue" | "QueueDoesNotExist" => SqsError::QueueNotFound(queue_url.to_owned()),
        "InvalidParameterValue"
        | "InvalidParameterCombination"
        | "MissingParameter"
        | "ReceiptHandleIsInvalid"
        | "InvalidReceiptHandle"
        | "MessageNotInflight"
        | "InvalidMessageContents"
        | "InvalidAttributeValue"
        | "InvalidAttributeName"
        | "BatchEntryIdsNotDistinct"
        | "TooManyEntriesInBatchRequest"
        | "EmptyBatchRequest"
        | "BatchRequestTooLong"
        | "InvalidBatchEntryId"
        | "UnsupportedOperation" => SqsError::InvalidRequest(format!("{code}: {message}")),
        _ => SqsError::Service(format!("{code}: {message}")),
    }
}

/// Reads `deadLetterTargetArn` from a `RedrivePolicy` JSON value.
pub(crate) fn redrive_target(policy: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(policy)
        .ok()?
        .get("deadLetterTargetArn")?
        .as_str()
        .map(str::to_owned)
}

fn sdk_err<E: ProvideErrorMetadata>(queue_url: &str, err: &E) -> SqsError {
    classify(queue_url, err.code(), err.message())
}

fn to_i32(value: u64, what: &str) -> Result<i32, SqsError> {
    i32::try_from(value)
        .map_err(|_| SqsError::InvalidRequest(format!("{what} {value} is too large")))
}

fn attributes(
    message: &OutboundMessage,
) -> Result<Option<std::collections::HashMap<String, MessageAttributeValue>>, SqsError> {
    if message.attributes.is_empty() {
        return Ok(None);
    }
    message
        .attributes
        .iter()
        .map(|(k, v)| {
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value(v)
                .build()
                .map(|value| (k.clone(), value))
                .map_err(|e| SqsError::InvalidRequest(e.to_string()))
        })
        .collect::<Result<_, _>>()
        .map(Some)
}

impl SqsTransport for AwsSqsTransport {
    fn send<'a>(
        &'a self,
        queue_url: &'a str,
        message: OutboundMessage,
    ) -> BoxFuture<'a, Result<String, SqsError>> {
        Box::pin(async move {
            let delay = to_i32(message.delay_secs, "DelaySeconds")?;
            let out = self
                .client
                .send_message()
                .queue_url(queue_url)
                .message_body(&message.body)
                .set_delay_seconds((delay > 0).then_some(delay))
                .set_message_group_id(message.group_id.clone())
                .set_message_deduplication_id(message.dedup_id.clone())
                .set_message_attributes(attributes(&message)?)
                .send()
                .await
                .map_err(|e| sdk_err(queue_url, &e))?;
            Ok(out.message_id().unwrap_or_default().to_owned())
        })
    }

    fn send_batch<'a>(
        &'a self,
        queue_url: &'a str,
        messages: Vec<OutboundMessage>,
    ) -> BoxFuture<'a, Result<Vec<BatchEntryResult>, SqsError>> {
        Box::pin(async move {
            let mut entries = Vec::with_capacity(messages.len());
            for (i, m) in messages.iter().enumerate() {
                let delay = to_i32(m.delay_secs, "DelaySeconds")?;
                let entry = SendMessageBatchRequestEntry::builder()
                    .id(i.to_string())
                    .message_body(&m.body)
                    .set_delay_seconds((delay > 0).then_some(delay))
                    .set_message_group_id(m.group_id.clone())
                    .set_message_deduplication_id(m.dedup_id.clone())
                    .set_message_attributes(attributes(m)?)
                    .build()
                    .map_err(|e| SqsError::InvalidRequest(e.to_string()))?;
                entries.push(entry);
            }
            let out = self
                .client
                .send_message_batch()
                .queue_url(queue_url)
                .set_entries(Some(entries))
                .send()
                .await
                .map_err(|e| sdk_err(queue_url, &e))?;
            let mut results: Vec<BatchEntryResult> = (0..messages.len())
                .map(|_| Err(SqsError::Service("no result for batch entry".to_owned())))
                .collect();
            for ok in out.successful() {
                if let Some(slot) = ok
                    .id()
                    .parse::<usize>()
                    .ok()
                    .and_then(|i| results.get_mut(i))
                {
                    *slot = Ok(ok.message_id().to_owned());
                }
            }
            for failed in out.failed() {
                if let Some(slot) = failed
                    .id()
                    .parse::<usize>()
                    .ok()
                    .and_then(|i| results.get_mut(i))
                {
                    *slot = Err(classify(queue_url, Some(failed.code()), failed.message()));
                }
            }
            Ok(results)
        })
    }

    fn receive<'a>(
        &'a self,
        queue_url: &'a str,
        options: ReceiveOptions,
    ) -> BoxFuture<'a, Result<Vec<ReceivedMessage>, SqsError>> {
        Box::pin(async move {
            let out = self
                .client
                .receive_message()
                .queue_url(queue_url)
                .max_number_of_messages(to_i32(
                    u64::from(options.max_messages),
                    "MaxNumberOfMessages",
                )?)
                .wait_time_seconds(to_i32(options.wait_secs, "WaitTimeSeconds")?)
                .visibility_timeout(to_i32(options.visibility_secs, "VisibilityTimeout")?)
                .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
                .message_attribute_names("All")
                .send()
                .await
                .map_err(|e| sdk_err(queue_url, &e))?;
            Ok(out
                .messages()
                .iter()
                .map(|m| ReceivedMessage {
                    message_id: m.message_id().unwrap_or_default().to_owned(),
                    receipt_handle: m.receipt_handle().unwrap_or_default().to_owned(),
                    body: m.body().unwrap_or_default().to_owned(),
                    receive_count: m
                        .attributes()
                        .and_then(|a| a.get(&MessageSystemAttributeName::ApproximateReceiveCount))
                        .and_then(|v| v.parse().ok()),
                    attributes: m.message_attributes().map_or_else(BTreeMap::new, |a| {
                        a.iter()
                            .filter_map(|(k, v)| {
                                v.string_value().map(|s| (k.clone(), s.to_owned()))
                            })
                            .collect()
                    }),
                })
                .collect())
        })
    }

    fn delete<'a>(
        &'a self,
        queue_url: &'a str,
        receipt_handle: &'a str,
    ) -> BoxFuture<'a, Result<(), SqsError>> {
        Box::pin(async move {
            self.client
                .delete_message()
                .queue_url(queue_url)
                .receipt_handle(receipt_handle)
                .send()
                .await
                .map_err(|e| sdk_err(queue_url, &e))?;
            Ok(())
        })
    }

    fn change_visibility<'a>(
        &'a self,
        queue_url: &'a str,
        receipt_handle: &'a str,
        visibility_secs: u64,
    ) -> BoxFuture<'a, Result<(), SqsError>> {
        Box::pin(async move {
            self.client
                .change_message_visibility()
                .queue_url(queue_url)
                .receipt_handle(receipt_handle)
                .visibility_timeout(to_i32(visibility_secs, "VisibilityTimeout")?)
                .send()
                .await
                .map_err(|e| sdk_err(queue_url, &e))?;
            Ok(())
        })
    }

    fn queue_stats<'a>(
        &'a self,
        queue_url: &'a str,
    ) -> BoxFuture<'a, Result<QueueStats, SqsError>> {
        Box::pin(async move {
            let out = self
                .client
                .get_queue_attributes()
                .queue_url(queue_url)
                .attribute_names(QueueAttributeName::All)
                .send()
                .await
                .map_err(|e| sdk_err(queue_url, &e))?;
            let attrs = out.attributes();
            let num = |name: QueueAttributeName| -> u64 {
                attrs
                    .and_then(|a| a.get(&name))
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0)
            };
            Ok(QueueStats {
                visible: num(QueueAttributeName::ApproximateNumberOfMessages),
                in_flight: num(QueueAttributeName::ApproximateNumberOfMessagesNotVisible),
                delayed: num(QueueAttributeName::ApproximateNumberOfMessagesDelayed),
                redrive_target: attrs
                    .and_then(|a| a.get(&QueueAttributeName::RedrivePolicy))
                    .and_then(|p| redrive_target(p)),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const Q: &str = "https://sqs.local/000/q";

    #[test]
    fn missing_queue_codes_map_to_not_found() {
        for code in [
            "AWS.SimpleQueueService.NonExistentQueue",
            "QueueDoesNotExist",
        ] {
            assert_eq!(
                classify(Q, Some(code), Some("gone")),
                SqsError::QueueNotFound(Q.to_owned())
            );
        }
    }

    #[test]
    fn client_fault_codes_map_to_invalid_request() {
        for code in [
            "InvalidParameterValue",
            "ReceiptHandleIsInvalid",
            "AWS.SimpleQueueService.MessageNotInflight",
            "MessageNotInflight",
            "InvalidMessageContents",
            "MissingParameter",
        ] {
            let err = classify(Q, Some(code), Some("bad"));
            assert!(
                matches!(err, SqsError::InvalidRequest(ref m) if m.contains(code)),
                "{code}: {err:?}"
            );
        }
    }

    #[test]
    fn other_codes_map_to_service() {
        let err = classify(Q, Some("ThrottlingException"), Some("slow down"));
        assert_eq!(
            err,
            SqsError::Service("ThrottlingException: slow down".to_owned())
        );
        let err = classify(Q, None, None);
        assert_eq!(err, SqsError::Service("unknown: no message".to_owned()));
    }

    #[test]
    fn redrive_target_reads_arn() {
        let p = r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:dlq","maxReceiveCount":"5"}"#;
        assert_eq!(
            redrive_target(p).as_deref(),
            Some("arn:aws:sqs:us-east-1:1:dlq")
        );
        assert_eq!(redrive_target("{}"), None);
        assert_eq!(redrive_target("not json"), None);
    }

    #[test]
    fn to_i32_rejects_overflow() {
        assert_eq!(to_i32(5, "x"), Ok(5));
        assert!(matches!(
            to_i32(u64::MAX, "x"),
            Err(SqsError::InvalidRequest(_))
        ));
    }

    #[test]
    fn attributes_are_string_typed() {
        let m = OutboundMessage::new("b").attribute("k", "v");
        let a = attributes(&m).unwrap().unwrap();
        assert_eq!(a["k"].data_type(), "String");
        assert_eq!(a["k"].string_value(), Some("v"));
        assert!(attributes(&OutboundMessage::new("b")).unwrap().is_none());
    }
}
