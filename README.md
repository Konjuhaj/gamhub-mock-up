# Real-time player events: Kinesis → Braze

A small proof of concept. Player events go into a Kinesis stream, a Go consumer filters them for privacy, and sends them to Braze in real time. Everything runs locally: AWS is LocalStack and Braze is a mock.

## Architecture

![Top-level architecture](Top-level%20architecture@1x.png)

The diagram shows the target design. Trigger evaluation and GitHub Actions are not built yet, see [Not done yet](#not-done-yet).

For every batch the consumer does four things:

1. **Read** records from the shard, starting after the last checkpoint.
2. **Filter**: look up each player in `player_state` and drop events that must not leave (see [Privacy](#privacy)).
3. **Send** to Braze in chunks of 75, with retries and backoff on errors and `429`.
4. **Checkpoint** the sequence number in DynamoDB, so a restart continues where it stopped.

## Components

| Part | Where | What it does |
|---|---|---|
| Producer | `cmd/producer` | Seeds 250 players and writes fake events (`level_up`, `purchase`, …) to Kinesis |
| Consumer | `cmd/consumer` | Reads, filters, sends to Braze, checkpoints, exposes metrics |
| Braze mock | `cmd/brazemock` | Fake `/users/track`. Logs every request to `tmp/brazemock/received.jsonl`. Can add latency, errors and throttling |
| Infra | `infra/terraform` | Kinesis stream, two DynamoDB tables, IAM role |
| Prometheus | `infra/prometheus` | Scrapes the consumer |
| Load test | `tests/loadtest.py` | Produces N events and reports latency and throughput |

## Run it

You need Docker, Go, Terraform, `lstk` and a `.env` file with `LOCALSTACK_AUTH_TOKEN`.

```sh
make up        # LocalStack, Braze mock, Prometheus
make infra     # create the stream and tables
```

LocalStack forgets everything on restart, so run `make infra` after every `make up`.

In every terminal that runs Go code:

```sh
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
       AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
```

Then:

```sh
go run ./cmd/producer -seed-only          # write the players
go run ./cmd/consumer                     # terminal 1
go run ./cmd/producer -rate 10            # terminal 2
```

Producer flags: `-rate` (events per second), `-burst` (10x the rate), `-count` (stop after N events).

What to look at:

| What | Where |
|---|---|
| Events that reached Braze | `tmp/brazemock/received.jsonl` |
| Consumer metrics | http://localhost:2112/metrics |
| Prometheus | http://localhost:9090 |
| Player table | `lstk aws dynamodb scan --table-name player_state` |

Stop everything with `make down`.

## Test

```sh
make test       # unit tests
make loadtest   # needs the stack and the consumer running
```

## Metrics

| Metric | Meaning |
|---|---|
| `e2e_latency_seconds` | Time from `produced_at` until Braze accepted the event |
| `events_total{result}` | Events sent or failed |
| `braze_requests_total{status}` | Braze requests by response status |
| `suppressed_total{reason}` | Events dropped for privacy |
| `consumer_lag_ms` | How far behind the stream the consumer is |
| `inflight_batches` | Batches being sent right now |

## Privacy

Minors and deleted accounts never leave our infrastructure. The consumer looks up every event's player in `player_state` before any Braze payload is built and drops the event if the player is deleted, a minor, has an unknown age, or is not found (fail closed). Dropped events are counted in `suppressed_total{reason="deleted|minor|age_unknown"}`, are still checkpointed so they are not replayed, and are only logged as a hashed `player_id` plus the reason. `TestReplayOnlyAdultReachesBraze` proves that only the adult player reaches Braze.

These are hard rules, so they are enforced in-stream: data minimisation means data we are not allowed to use should not be sent at all. Channel consent (email, push opt-in) is different. It changes often and belongs to the user, so it stays in Braze subscription groups and is checked at send time, not built here.

## Trade-offs

| Choice | Why | In production |
|---|---|---|
| One shard, one consumer | Simple to reason about | More shards, one worker per shard (KCL or Lambda) |
| At-least-once delivery | Checkpoint is saved after the send, so a crash replays the batch | Deduplicate on `event_id` |
| 3 retries, then fail | Enough to show backoff | Retry forever with jitter, dead-letter queue |
| Braze mock | No real Braze account needed | Real endpoint and API key |
| LocalStack | Free and fast | Real AWS with the same Terraform |

## Not done yet

- First-purchase trigger (exactly one `first_purchase` per player)
- Chaos test (`make chaos`)
- CI
- Erasure across a warehouse, retention jobs, subscription-group sync
