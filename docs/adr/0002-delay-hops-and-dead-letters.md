# ADR 0002: Delay hops and plugin dead letters

- Status: Accepted
- Date: 2026-09-28

## Context

- SQS `DelaySeconds` is 900 s or less. Autumn `enqueue_in` has no limit.
- SQS counts receives with `ApproximateReceiveCount`. A redrive policy moves a
  message after `maxReceiveCount` receives. Autumn counts `max_attempts` per job.
- SQS allows 12 h of visibility from one receive.

## Decision

1. **Delay.** For a delay over 900 s, the envelope gets `not_before`. The first
   send uses 900 s. A worker that receives the message early sends a copy with
   the next step, then deletes the original. The copy is a new message, so its
   receive count starts at 1.
2. **Attempts.** The worker reads `ApproximateReceiveCount` as the attempt. It
   retries with `ChangeMessageVisibility` while `attempt < max_attempts`.
   Before a run, a message with `attempt > max_attempts` goes to the
   dead-letter path. The handler does not run again.
3. **Dead letters.** With `jobs.dead_letter_queue` set, the worker sends the
   message there with a reason, then deletes it. The copy has 10 attributes or
   less and fits the size limit. Without a DLQ, or when the send fails, the
   worker keeps the message for 30 s or more. A redrive policy then moves it.
   With no redrive policy either, a warning shows at startup.
4. **Poison.** A panic, bad JSON, an unknown job, a job from another queue, or
   `ConsumerError::Reject` goes to the dead-letter path at once.
5. **12 h limit.** The worker clamps each visibility change to the time left
   from the receive, minus 5 s. The heartbeat stops when no time is left.
6. **FIFO order.** The worker receives one message at a time from a FIFO
   queue. SQS locks the group until that message settles. A batch receive
   holds later messages of the group. Each receive then adds to their receive
   count, so they can use up their attempts before they run (review round 2).

## Reasons

- A hop costs one send and one delete per 15 minutes. It does not use attempts.
- `ChangeMessageVisibility` backoff needs no extra queue or timer.
- A plugin DLQ keeps the failure reason. A redrive policy alone loses it.
- `verus/policy.rs` proves the retry decision, the backoff bounds and closed
  form, the delay split, batch bounds by count and bytes, and the visibility
  clamp. Property tests check the same rules on the Rust code.

## Results

- Set the redrive `maxReceiveCount` to `max_attempts` or more. If it is less,
  SQS moves the message before the plugin stops the retries.
- FIFO queues do not accept per-message delay. `enqueue_in` returns
  `SqsError::FifoDelay`.
- Delay precision is whole seconds.
