# swage

An embeddable flight recorder for application metrics in Go.

swage continuously records your app's metrics — request latencies, queue depths,
error rates, sensor readings, business KPIs — into a durable, compressed on-disk
ring. When something goes wrong, you query the window of metric history leading up
to it. Think of it as the black box for your application's numbers.

## The problem

You're running a service on a Raspberry Pi. A single-binary app in a factory. An
edge node in the field. Something goes wrong at 3am. You SSH in at 9am.

What happened?

Your options today:

- **Prometheus + Grafana** — requires a separate server, persistent infrastructure,
  network connectivity. Not viable on a Pi, in an air-gapped environment, or inside
  a single distributed binary.
- **Cloud TSDB** — requires connectivity, an account, egress costs. Not viable
  offline or in resource-constrained environments.
- **Application logging** — unstructured, unsearchable for quantitative questions
  like "what was p99 latency in the 20 minutes before the crash?"
- **Nothing** — the default. The incident happens, the history is gone.

## What swage does

swage is a Go library. No server, no infrastructure, no network required. You
record metrics with a single function call, and swage handles everything else:

```go
rec, err := ingotstore.OpenRecorder("./flight-data", swage.Options{
    Horizon: 7 * 24 * time.Hour,  // keep the last 7 days
})
defer rec.Close()

// Record metrics from anywhere in your app.
rec.Record("request_latency_ms", 12.4)
rec.Record("queue_depth", 42)
rec.Record("error_count", 1)
```

When something goes wrong, query the history:

```go
// Raw samples for the last 30 minutes.
snap, _ := rec.Snapshot(time.Now().Add(-30*time.Minute), time.Now())

// Windowed aggregates (min, max, mean, p50, p95, p99, rate) in 5-minute buckets.
sum, _ := rec.Summary(time.Now().Add(-1*time.Hour), time.Now(), 5*time.Minute)
```

## How it works

### Compressed raw retention

You say "keep 7 days." Gorilla compression achieves ~1 byte per sample, so 200
series at 1 sample/sec for 7 days is ~115 MB. swage keeps every raw sample for
the full horizon — no lossy downsampling, no rollup tiers that destroy forensic
detail. When you investigate an incident, the full-resolution data is there.

Aggregates (min, max, mean, p50, p95, p99, rate of change) are computed at query
time from the raw samples. For a 30-minute window at 1 sample/sec, that's 1,800
values per series — trivially fast to aggregate.

### Pulling the recording

swage is an in-process library, and its storage engine (ingot) is single-process.
There are two ways to get data out:

**While the process is running,** produce a `.swage` dump file. Wire up a signal
handler to trigger it on demand:

```go
go func() {
    ch := make(chan os.Signal, 1)
    signal.Notify(ch, syscall.SIGUSR1)
    for range ch {
        f, _ := os.Create(fmt.Sprintf("/var/dumps/swage-%s.swage", time.Now().Format(time.RFC3339)))
        rec.DumpTo(f, time.Now().Add(-30*time.Minute), time.Now())
        f.Close()
    }
}()
```

Then `kill -USR1 <pid>` from your SSH session.

**If the process crashed,** use `swagectl` to recover data directly from the
data directory (safe because nothing else has it open):

```sh
swagectl recover ./flight-data --last 2h -o incident.swage
```

### The `.swage` dump file

A `.swage` file is a self-contained, portable dump — NDJSON (one JSON object per
line), grep-friendly, parseable in any language. Produce it on the box, analyze it
anywhere:

```sh
swagectl ls incident.swage                           # list series
swagectl summary incident.swage --window 5m          # windowed aggregates
swagectl read incident.swage --series latency_ms     # raw samples for one series
```

### Safe for always-on use

swage is designed to run in production forever. It provides hard guarantees:

- **Bounded memory** — cardinality limits, buffer caps.
- **Bounded disk** — retention enforcement, automatic cleanup.
- **Bounded CPU** — amortized writes, no query-time surprises.

You configure a budget, and swage stays within it.

### Durable across restarts

Data is written to disk continuously. If the process crashes and restarts, the
ring reloads — no data loss within the retention window. This is the point of a
flight recorder: it survives the crash it's meant to diagnose.

## Built on ingot

swage uses [ingot](https://github.com/davidhrinaldo/ingot) as its storage engine — an
embedded, Gorilla-compressed time-series database for Go (SQLite-style, no server).
Gorilla compression achieves ~1 byte per sample, which is what makes "keep 7 days
of many series on a Raspberry Pi" feasible. Callers never interact with ingot
directly; it's an implementation detail behind swage's storage interface.

## Not to be confused with `runtime/trace.FlightRecorder`

Go 1.25 introduced `runtime/trace.FlightRecorder`, which records **runtime execution
traces** — goroutine scheduling, GC events, syscalls — in an in-memory ring buffer,
dumped to a trace file for `go tool trace`.

swage is different and complementary:

| | `runtime/trace.FlightRecorder` | swage |
|---|---|---|
| **Data** | Runtime events (goroutines, GC, scheduler) | Application metrics (float64 time series) |
| **Storage** | In-memory only | Durable on-disk, survives restart |
| **Query model** | One-shot trace snapshot | Range queries with aggregates |
| **Consumer** | `go tool trace` | Your code, incident reports |

Use the stdlib recorder to understand *how your
runtime behaved*. Use swage to understand *what your application was doing*.

## Design targets

- **Edge/IoT services** — Raspberry Pi, embedded Linux, constrained hardware.
- **Single-binary distributed apps** — no sidecar, no agent, no infrastructure.
- **Offline-first environments** — air-gapped, intermittent connectivity, field-deployed.
- **Any service without a metrics backend** — when standing up Prometheus isn't worth it.

## Non-goals

swage records and retrieves metric history. It is not:

- A dashboard or visualization tool.
- An alerting engine (trigger support is planned as a future seam).
- A metrics export agent or scrape endpoint.
- A replacement for Prometheus, Datadog, or any full observability stack.

If you have a metrics backend, use it. swage is for when that's not feasible.

## Requirements

- Go 1.26+
- [ingot](https://github.com/davidhrinaldo/ingot)
