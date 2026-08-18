# gojob

A production-style distributed job queue and worker system, built from scratch in Go.

Background job processing looks simple until you actually build one correctly. What happens when a worker dies mid-job? When two workers race for the same job? When the process gets killed while jobs are in flight? When a job fails — do you retry it, how many times, and how long do you wait between attempts? gojob is built specifically around answering those questions correctly, not around avoiding them.

## Why this exists

Most job-queue tutorials wrap a channel in a for-loop and call it done. That works until the process restarts and every queued job disappears, or two workers grab the same job at once, or a single panicking handler takes the whole service down with it. None of that is acceptable in a real system, and none of it is hard to get wrong.

This project takes the opposite approach: every piece of concurrency here — the worker pool, the queue implementations, the shutdown path — is built around Go's actual concurrency primitives (`context`, channels, `sync.WaitGroup`, mutexes) and reasoned through deliberately, not copied from a pattern. Storage is defined as an interface first, so an in-memory implementation and a Redis-backed one are interchangeable without the worker pool knowing the difference. Retry logic exists because jobs fail in the real world, and "it worked once" isn't a reliability guarantee.

## What it actually does

- **Runs a pool of workers concurrently**, coordinated with proper synchronization — no shared-state races, no goroutine leaks, no job processed by two workers at once
- **Persists every job in Redis**, so a process restart doesn't mean lost work
- **Retries failed jobs automatically**, with a configurable attempt limit and backoff delay per job type — not a blanket policy that doesn't fit every kind of work
- **Contains failures instead of propagating them** — a panic inside one job's handler is recovered and logged; it never takes down the worker, let alone the service
- **Shuts down without dropping work** — on termination, every worker finishes the job it's currently holding before exiting, and the process doesn't exit until that's true for all of them

## Where it stands

- ✅ Worker pool — concurrent processing, panic recovery, graceful shutdown, fully tested including under Go's race detector
- ✅ Redis-backed queue — persistent, atomic delivery (no duplicate job pickup), verified across real process restarts
- 🔄 Retry system — configurable per-job retry limits and backoff, currently being wired into a dead-letter queue for jobs that exhaust their attempts
- ⏳ Delayed and scheduled jobs
- ⏳ Metrics: throughput, processing latency, queue depth
- ⏳ HTTP API for external job submission

## How it's structured

```
cmd/worker      the entrypoint — assembles the queue, registry, and pool, and runs them
internal/job     what a job is, and the contract any handler must satisfy
internal/queue    the queue as an interface, with in-memory and Redis implementations
internal/worker   the worker pool and the registry mapping job types to their handlers
```

The dependency direction only ever points one way: `cmd` depends on `worker`, `worker` depends on `queue`, `queue` depends on `job`. Nothing depends back up the chain. This is what makes it possible to test the worker pool entirely against the in-memory queue — fast, no external dependencies — while running the exact same pool code against Redis in production.

## Built with

Go · Redis · Docker & Docker Compose · go-redis

## Running it

```bash
docker compose up -d redis
go run ./cmd/worker
```

Connects to Redis at `localhost:6379` by default. Set `REDIS_ADDR` to point at a different instance.

## Testing

```bash
make test        # the full test suite
make test-race    # with Go's race detector enabled — the one that actually matters for this project
```

The test suite is deliberately weighted toward concurrency correctness: concurrent enqueue/dequeue, concurrent registry access, context-cancellation behavior on every blocking call, and a full pool-level test proving every job submitted actually gets processed under real goroutine scheduling — not just that the code compiles.
