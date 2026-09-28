# ADR 0002: Delay hops and plugin dead letters

- Status: Accepted
- Date: 2026-09-28

## Context

- SQS `DelaySeconds` is 900 s or less. Autumn `enqueue_in` has no limit.
- SQS counts attempts with `ApproximateReceiveCount`. A redrive policy moves a
  message after `maxReceiveCount` receives. Autumn counts `max_attempts` per job.

## Decision

1. **Delay.** For a delay over 900 s, the envelope gets `not_before`. The first
   send uses 900 s. A worker that receives the message early sends a copy with
   the next step, then deletes the original. The copy is a new message, so its
   receive count starts at 1.
2. **Attempts.** The worker reads `ApproximateReceiveCount` as the attempt. It
   retries with `ChangeMessageVisibility` while `attempt < max_attempts`.
3. **Dead letters.** With `jobs.dead_letter_queue` set, the worker sends the
   message there with a reason, then deletes it. Without it, the worker keeps
   the message and the SQS redrive policy moves it.
4. **Poison.** A panic, bad JSON, an unknown job, or `ConsumerError::Reject`
   goes to the dead-letter path at once.

## Reasons

- A hop costs one send and one delete per 15 minutes. It does not use attempts.
- `ChangeMessageVisibility` backoff needs no extra queue or timer.
- A plugin DLQ keeps the failure reason. A redrive policy alone loses it.
- `verus/policy.rs` proves the retry decision, the backoff bounds, and the
  delay split. Property tests check the same rules on the Rust code.

## Results

- Set the redrive `maxReceiveCount` to `max_attempts` or more. Else SQS moves a
  message before the plugin gives up.
- FIFO queues do not accept per-message delay. `enqueue_in` returns
  `SqsError::FifoDelay`.
- Delay precision is whole seconds.
