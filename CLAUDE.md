# CLAUDE.md

Amazon SQS plugin for autumn-web 0.7. Style for all docs and comments: ASD-STE100.

## Layout

| Path | Contents |
|---|---|
| `src/policy.rs` | Verified core: retry decision, backoff, delay split, batch bounds, visibility clamp, heartbeat. Pure. |
| `verus/policy.rs` | Verus spec and proofs for `src/policy.rs`. Keep both in step. |
| `src/transport/` | `SqsTransport` trait, `AwsSqsTransport` (SDK), `MemoryTransport` (fake). |
| `src/config.rs` | `[aws_sqs]` config, profile file, `AUTUMN_AWS_SQS__*` env overlay. |
| `src/envelope.rs` | Job message format. |
| `src/producer.rs` | `SqsProducer`. |
| `src/jobs.rs` | `SqsJobClient` and the job dispatcher. |
| `src/consumer.rs` | `SqsConsumer`, `SqsMessage`, `ConsumerError`. |
| `src/worker.rs` | Receive loop, heartbeat, retry, dead letter, drain. |
| `src/health.rs`, `src/metrics.rs` | Actuator health and Prometheus metrics. |
| `src/plugin.rs` | `AwsSqsPlugin`, `SqsRuntime`. |
| `tests/` | `jobs.rs`, `runtime.rs`, `failures.rs` (fake, paused time), `load.rs` (config files), `localstack.rs` (real API). `common/` has `FaultyTransport`. |
| `docs/` | `plan.md`, ADRs. |

## Commands

```bash
git config core.hooksPath .githooks   # pre-commit: fmt, clippy, test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
# Real SQS API:
docker run -d -p 4566:4566 -e SERVICES=sqs localstack/localstack:4
AWS_SQS_IT_ENDPOINT=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test cargo test --test localstack
cargo llvm-cov --all-targets --summary-only
verus verus/policy.rs
```

## Rules

- Write the test first. See it fail. Then write the code.
- A change to `src/policy.rs` needs the same change in `verus/policy.rs`. Run Verus.
- Only `src/transport/` calls SQS. Other code uses `SqsTransport`.
- `MemoryTransport` must follow real SQS. Check a new rule against LocalStack first.
- A metrics label is an alias or a consumer name. Never a raw URL.
- No `unwrap` or `expect` in `src/` outside tests.
- Time in tests: `#[tokio::test(start_paused = true)]` and `tests/common::clock()`.
- Workers start only when `state.role().runs_workers()` is true.
- Delivery is at least once. Do not claim exactly once.
- Do not log message bodies or credentials.
