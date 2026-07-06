# swage — Design Document

This document captures the architecture, decisions, and rationale for swage.
It is the reference for implementation. If something isn't addressed here,
it's out of scope until this document is updated.

---

## 1. Architecture Overview

```
   Your application code
          │
          │  rec.Record("latency_ms", 12.4)
          ▼
  ┌───────────────────────────────────────────┐
  │              Recorder                     │
  │                                           │
  │  ┌──────────┐         ┌────────────────┐  │
  │  │  write   │  flush  │                │  │
  │  │  buffer  │────────▶│ Store (ingot)  │  │
  │  │([]Sample)│  1s     │                │  │
  │  └──────────┘         └───────┬────────┘  │
  │                               │           │
  │  ┌──────────────┐    query    │           │
  │  │  Snapshot()  │◀────────────┘           │
  │  │  Summary()   │                         │
  │  └──────┬───────┘                         │
  │         │                                 │
  └─────────┼─────────────────────────────────┘
            │
            ▼
   Snapshot struct / .swage file / stderr dump
```

The Recorder is the single entry point. It owns a write buffer, a flush
goroutine, and a reference to a Store. Retention is the Store's
responsibility, not the Recorder's. The Store interface abstracts the
durable storage layer; the production implementation wraps ingot. Callers
never touch ingot directly.

---

## 2. Package Layout

```
swage/                              github.com/davidrinaldo/swage
├── recorder.go                     Recorder, New(), Record(), Close()
├── options.go                      Options, defaults, validation
├── snapshot.go                     Snapshot, Summary, query-time aggregation
├── store.go                        Store interface + Sample type
├── series.go                       Series handle (fast-path recording)
├── doc.go                          Package doc, stdlib distinction note
│
├── ingotstore/                     github.com/davidrinaldo/swage/ingotstore
│   └── ingotstore.go              Store backed by ingot + OpenRecorder
│
├── memstore/                       github.com/davidrinaldo/swage/memstore
│   └── memstore.go                In-memory Store for testing
│
└── cmd/
    └── swagectl/                   CLI: read .swage files, recover dead data dirs
        └── main.go
```

**Import graph:**
- `swage` imports nothing external (stdlib only).
- `swage/ingotstore` imports `ingot` + `ingot/labels`. Only package that
  touches ingot.
- `swage/memstore` imports nothing external.
- `cmd/swagectl` imports `swage` + `swage/ingotstore`.

---

## 3. Core Types and API

### Sample

```go
type Sample struct {
    Name string
    T    int64   // Unix milliseconds
    V    float64
}
```

Everything is a named float64 series. No counter/gauge/histogram
distinction at the storage layer.

**Series names must be non-empty valid UTF-8.** Invalid names (empty,
non-UTF-8) are rejected at `Record()` time — the sample is dropped and
`OnOverBudget` is called with reason `"invalid_name"`. Validation happens
before the sample enters the buffer, so one bad name never poisons a batch.

**Values must be finite.** `NaN` and `Inf` are rejected at `Record()` time
(dropped, `OnOverBudget` with reason `"invalid_value"`). These are not
representable in JSON — Go's `encoding/json` errors on them — so allowing
them into the Store would make NDJSON serialization fail. Rejecting at the
boundary is consistent with name validation: bad data never enters the
system.

### Options

```go
type Options struct {
    Horizon       time.Duration              // How far back to retain. Required.
    FlushInterval time.Duration              // Write buffer flush frequency. Default: 1s.
    MaxSeries     int                        // Max distinct series names. Default: 1000.
    MaxBufferSize int                        // Max buffered samples before forced flush. Default: 10_000.
    OnOverBudget  func(reason string, s Sample) // Called on dropped samples. Default: no-op.
    OnFlushError  func(error)                // Called on flush failure. Default: log to stderr.
    Clock         func() time.Time           // For testing. Default: time.Now.
}
```

### Recorder

```go
func New(store Store, opts Options) (*Recorder, error)

func (r *Recorder) Record(name string, value float64)
func (r *Recorder) RecordAt(name string, t time.Time, value float64)
func (r *Recorder) Series(name string) *Series

func (r *Recorder) Snapshot(from, to time.Time, names ...string) (*Snapshot, error)
func (r *Recorder) Summary(from, to time.Time, window time.Duration, names ...string) (*Summary, error)

func (r *Recorder) Flush() error
func (r *Recorder) DumpTo(w io.Writer, from, to time.Time, names ...string) error
func (r *Recorder) Close() error
```

- `New` takes a caller-provided Store. The Store must already be configured
  with retention matching `opts.Horizon`. For the common case, use
  `ingotstore.OpenRecorder(dir, opts)` which creates both and configures
  Horizon once.
- `Record` and `RecordAt` are fire-and-forget. They never return errors or
  panic. After `Close()`, they become silent no-ops.
- `Flush` sends a request to the flush goroutine and blocks until complete.
  Safe for concurrent use. Called automatically before Snapshot/Summary/DumpTo.
- `Close` flushes, stops background goroutines. If the Recorder was created
  via `OpenRecorder`, it closes the Store; if via `New`, the caller manages
  the Store lifecycle.

### Series (fast-path handle)

```go
type Series struct { /* unexported */ }

func (s *Series) Record(value float64)
func (s *Series) RecordAt(t time.Time, value float64)
```

Caches the cardinality check and name lookup. The hot path skips the map
and the budget check — just mutex, append, unlock.

### Snapshot and Summary

```go
type Snapshot struct {
    From   time.Time
    To     time.Time
    Series map[string][]Point
}

type Point struct {
    T time.Time
    V float64
}

type Summary struct {
    From   time.Time
    To     time.Time
    Window time.Duration
    Series map[string][]Bucket
}

type Bucket struct {
    Start, End       time.Time
    Min, Max, Mean   float64
    Count            int
    P50, P95, P99    float64
    Sum, Rate        float64
}
```

- `Snapshot` materializes raw samples in memory. Memory is bounded by the
  caller's time range and series filter, not the full horizon.
- `Summary` queries one series at a time, computes bucket aggregates, and
  discards raw samples before the next series. Peak memory is one series.
- `Rate` is `(last - first) / duration_seconds`. No counter-reset detection.
  See [Rate semantics](#rate-semantics).
- Percentiles are computed from raw samples per bucket (e.g., 300 values in a
  5-min window at 1/sec — trivial to sort in place).
- **Empty buckets** (no samples in a window) are omitted from the slice.
  Consumers iterating buckets may see gaps — `Start` of bucket N+1 is not
  necessarily `End` of bucket N.
- **The last bucket may be shorter than `Window`** (e.g., 5-min window on a
  7-min range produces a 5-min and a 2-min bucket). `Rate` always uses
  `End - Start` as the denominator, not `Window`.
- **Filtered series with no data** in the requested range are omitted from
  the map. A missing key means "no samples found," whether the series never
  existed or simply had no data in the window. Same behavior for Snapshot.

---

## 4. Write Path

### Design decisions

**Mutex + swap buffer.** `Record()` acquires a mutex, appends a Sample to a
slice, releases. The flush goroutine swaps the slice out (replacing it with
a pre-allocated spare) and writes to the Store outside the lock. Slices are
reused across flushes — zero steady-state allocation.

**Hard drop at capacity.** If the buffer is full (`MaxBufferSize`) or
cardinality is at limit (`MaxSeries`), the sample is dropped and
`OnOverBudget` is called. The buffer never exceeds `MaxBufferSize`. This is
a hard cap, not a soft target.

**Callbacks outside the lock.** `OnOverBudget` and `OnFlushError` are
always invoked after releasing the mutex. This prevents deadlocks if a
callback calls `Record()`.

**All writes go through the flush goroutine.** `Store.Append()` is only
ever called from the flush goroutine. The public `Flush()` method sends a
request to the flush goroutine and blocks until completion. This preserves
the single-writer invariant in the Store interface — no concurrent
`Append()` calls, ever.

**Sort before append.** Multiple goroutines recording the same series can
produce out-of-order timestamps in the buffer (goroutine A reads the clock,
gets preempted, goroutine B writes first). The flush goroutine sorts the
batch by `(Name, T)` before calling `Store.Append()`. ingot requires
ascending timestamps per series — sorting satisfies this within a batch.

**Clamp timestamps across batches.** Wall clocks go backwards — NTP step
adjustments, VM clock sync, suspend/resume. If batch N flushes with samples
up to t=1000 and the clock steps back, batch N+1 would contain t=995 for the
same series. ingot rejects out-of-order timestamps, which would drop the
entire batch via `OnFlushError`.

The flush goroutine tracks the last-flushed timestamp per series. After
sorting a batch, any sample whose timestamp is <= the last-flushed timestamp
for its series is clamped to `lastFlushed + 1ms`. This loses sub-millisecond
precision during clock adjustments but preserves data. The alternative —
dropping entire batches during clock regression — violates the flight
recorder's core promise of surviving adverse conditions.

This also makes `RecordAt` safe: callers can pass any timestamp, and the
clamp prevents cross-batch ordering violations. `RecordAt` follows the same
"never fails, never drops unless budget is exceeded" contract as `Record`.

### Crash gap

If the process is killed or loses power, samples in the write buffer (up to
`FlushInterval` worth, default 1s) are lost. Once flushed, ingot's WAL
fsyncs on `Commit()` and data survives any crash. The gap is configurable:
lower `FlushInterval` for tighter durability at the cost of more I/O.
`Flush()` is public for explicit durability points.

---

## 5. Query Path

### No rollups — raw retention, query-time aggregation

We evaluated multi-resolution rollups and rejected them for v1.

| Series | Sample rate | Horizon | Raw storage (Gorilla) |
|--------|------------|---------|----------------------|
| 50     | 1/sec      | 7 days  | ~29 MB               |
| 200    | 1/sec      | 7 days  | ~115 MB              |
| 500    | 1/sec      | 7 days  | ~288 MB              |
| 1000   | 1/sec      | 30 days | ~2.5 GB              |

Gorilla compression makes raw retention feasible for the target audience.
Rollups would destroy forensic data, require percentile sketches, and create
dual compaction with ingot. Rollups become a v2 optimization for users who
hit storage limits.

### Rate semantics

Rate is `(last - first) / duration_seconds`. No counter-reset heuristic.

A heuristic that treats value decreases as counter resets corrupts gauge data
— you can't be counter-aware and gauge-aware without type annotations. The
simple delta is correct for gauges, approximate for non-resetting counters,
and misleading for resetting counters. A `SeriesKind` hint can be added in a
future version without breaking the storage format.

### Consistency model

Only `Snapshot()` provides point-in-time consistency — it flushes once and
materializes all requested data atomically.

`Summary()` and `DumpTo()` query series sequentially from the Store while
the flush goroutine continues appending. Different series may reflect
different moments in time. The inconsistency window is the duration of the
operation (typically seconds). This is acceptable for a flight recorder —
the differences are a few seconds of data at the tail.

### DumpTo (streaming export)

`DumpTo` streams a `.swage` dump to an `io.Writer` without materializing all
samples. It queries one series at a time, writing NDJSON lines as it goes.
Peak memory is one series.

---

## 6. Retention and Storage Budget

### Retention

Retention is the Store's responsibility, configured at construction time.
The Recorder does not run a truncation loop.

- `ingotstore.New(dir, horizon)` sets `ingot.Options.Retention = horizon`.
  ingot enforces it autonomously (~1 min cycle).
- `memstore.New(horizon)` runs its own background cleanup.

**Time-based only.** Size-based retention is unpredictable. With Gorilla
compression at ~1 byte/sample, time-based is effectively size-bounded.

### Cardinality limit

`MaxSeries` (default: 1000) caps distinct series names. Excess is dropped
with `OnOverBudget` callback. `Record()` remains void — no error return.

The cardinality map tracks `map[string]time.Time` (last-recorded timestamp).
Stale entries (older than Horizon) are evicted on each flush cycle to prevent
the budget from filling permanently with abandoned series names.

A `Series` handle whose name was evicted re-registers on next `Record()`,
consuming a cardinality slot. The handle never becomes permanently broken.

### Buffer cap

`MaxBufferSize` (default: 10,000) forces a flush and hard-drops excess.
Worst-case buffer memory is ~600 KB.

---

## 7. The Dump Artifact

### `.swage` file format

NDJSON with a header line:

```
{"swage":1,"from":"2025-07-06T02:00:00Z","to":"2025-07-06T03:00:00Z","series":["latency_ms","queue_depth","errors"]}
{"s":"latency_ms","t":1720231200000,"v":12.4}
{"s":"latency_ms","t":1720231201000,"v":11.8}
{"s":"queue_depth","t":1720231200000,"v":42}
{"s":"queue_depth","t":1720231201000,"v":38}
```

**Ordering: grouped by series name (sorted), then by timestamp within each
series.** This is the natural output of streaming per-series queries —
`DumpTo` produces the canonical format directly without buffering.

Trade-offs: `grep` and `jq` work great for single-series extraction.
Global timestamp order requires re-sorting, but incident analysis typically
examines one series at a time or uses `Summary` for cross-series comparison.

**Why NDJSON:** grep-friendly, jq-friendly, parseable in any language,
streamable, compresses well with gzip. The data volumes don't justify a
binary format — a 30-min dump of 100 series is ~7 MB of NDJSON, ~1.5 MB
gzipped.

**Format versioning:** The header's `"swage"` field is the format version
(currently `1`). `ReadSnapshot` rejects any version it doesn't recognize.
This allows the format to evolve — a future version can add fields to sample
lines or change ordering — without ambiguity about backward compatibility.

### Serialization API

```go
func (s *Snapshot) WriteTo(w io.Writer) (int64, error)  // from Snapshot struct
func ReadSnapshot(r io.Reader) (*Snapshot, error)       // deserialize
func (s *Snapshot) Summary(window time.Duration) *Summary // compute aggregates
```

`Summary` on `Snapshot` is the same aggregation logic used by
`Recorder.Summary()`. This makes summary computation available standalone
— `swagectl summary` calls `ReadSnapshot` then `snap.Summary(window)`.
`Recorder.Summary()` is a convenience that flushes, queries one series at
a time (for bounded memory), and applies the same aggregation.

---

## 8. Query UX: How You Pull the Recording

### Constraint: ingot is single-process

ingot does not support concurrent access from multiple OS processes. No file
locking, no read-only mode. This means a separate CLI tool cannot open the
data directory while the application is running.

### Path A: In-process dump (process is running)

The Recorder produces `.swage` files via `DumpTo`. The caller wires the
trigger — typically a signal handler or programmatic call:

```go
// In application setup:
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

Then: `kill -USR1 <pid>` to trigger a dump.

### Path B: `swagectl recover` (process is dead)

When the process has crashed and is NOT running, `swagectl` can safely open
the ingot data directory:

```sh
swagectl recover ./flight-data --last 2h -o incident.swage
```

**Concurrent access guard:** `ingotstore` takes an advisory `flock(2)` on a
`swage.lock` file in the data directory at open time. The kernel releases it
automatically on crash. `swagectl recover` tries to acquire the same lock —
if it fails, the directory is in use and it exits with an error.

### Path C: `swagectl` on .swage files (offline analysis)

```sh
swagectl ls incident.swage
swagectl summary incident.swage --window 5m
swagectl read incident.swage --series latency_ms,error_count
```

The `.swage` file is the unit of exchange — produce it on the box, and export for analysis.

### Summary

| Scenario | How |
|---|---|
| Process running, want a dump | Signal handler or programmatic `DumpTo` |
| Process crashed | `swagectl recover` opens the dead data dir |
| Have a .swage file | `swagectl ls/read/summary` |

---

## 9. Safety Guarantees

| Resource   | Mechanism            | Bound                           | Default        |
|------------|----------------------|---------------------------------|----------------|
| Memory     | Write buffer cap     | MaxBufferSize × ~60 bytes       | ~600 KB        |
| Memory     | Cardinality limit    | MaxSeries names                 | 1000           |
| Memory     | Query (Summary)      | One series at a time            | Caller's range |
| Memory     | Query (Snapshot)     | Requested series × range        | Caller's range |
| Disk       | Time-based retention | Horizon × series × rate × ~1B  | Caller-defined |
| CPU        | Amortized flush      | 1 Store.Append per FlushInterval| 1/sec          |
| Goroutines | Fixed count          | 1 flush goroutine               | Always 1       |

Write path is bounded by construction — buffer fills, samples drop. Query
path is bounded by the caller's request. No panics. After `Close()`, writes
become no-ops.

---

## 10. Storage Interface

```go
type Store interface {
    // Append durably writes a batch of samples. Samples are sorted by
    // (Name, T) — the Store can assume per-series timestamp ordering.
    // Called from a single goroutine (the flush goroutine). Must be safe
    // for concurrent use with Query and Series (reads from other goroutines).
    Append(samples []Sample) error

    // Query returns samples for the named series in [from, to] (Unix ms).
    // Results are ordered by timestamp.
    Query(name string, from, to int64) ([]Sample, error)

    // Series returns the names of all known series.
    Series() ([]string, error)

    // Close releases resources.
    Close() error
}
```

**What's not in the interface:** `Truncate` (retention is a
construction-time concern). `QueryAll` (query one series at a time to
bound memory).

**Lifecycle:** `swage.New(store, opts)` — caller owns the Store, `Close()`
does not close it. `ingotstore.OpenRecorder(dir, opts)` — Recorder owns the
Store, `Close()` closes both. A Store must have at most one Recorder — the
single-writer invariant assumes one flush goroutine.

### ingotstore

```go
func New(dir string, horizon time.Duration) (*Store, error)
func OpenRecorder(dir string, opts swage.Options) (*swage.Recorder, error)
```

- Series name `N` → ingot label set `{__name__=N}`.
- `Append`: creates `ingot.Appender`, appends each sample, commits. Caches
  `ref` per series name for fast-path appends.
- `Query`: `ingot.Querier(from, to)` + `Select(MatchEqual("__name__", name))`.
- Retention: `ingot.Options.Retention = horizon`, enforced by ingot.
- Takes `flock(2)` on `swage.lock` in the data directory.

### memstore

```go
func New(horizon time.Duration) *Store
```

In-memory `map[string][]Sample` with `sync.RWMutex`. Background retention
goroutine if horizon > 0.

---

## 11. Restart Behavior

On restart, `ingotstore.New` calls `ingot.Open()`, which replays the WAL
and loads existing blocks. All prior data within the retention window is
immediately queryable. The Recorder starts appending — no gap other than
the time the process was down.

---

## 12. Adapter Seam

The `Record(name, value)` API is standalone — no external metric format
dependencies. A future adapter (e.g., `swage/oteladapter`) can translate
OTel or Prometheus metrics into `Record()` calls. The core write path must
remain cheap (no allocations, no interface boxing) to support this.

---

## 13. Prior Art

### vs. RRDtool

| | RRDtool | swage |
|---|---|---|
| Language | C (CGo for Go) | Pure Go |
| Integration | External tool | Embedded library |
| Schema | Fixed at creation | Dynamic |
| Rollups | Built-in | Raw retention (v1) |
| Crash safety | fsync per update | WAL-based |

swage's edge: Go library, dynamic series, no CGo.
RRDtool's edge: 25+ years battle-tested, mature rollups.

### vs. Prometheus embedded

Instrumentation without retention. Still need a server to scrape and store.

### vs. `runtime/trace.FlightRecorder`

Different on every axis. See README. Complementary.

### vs. "just use SQLite"

Missing: Gorilla compression, efficient range scans, retention management,
incident-oriented query model.

---

## 14. Build Order

| Phase | What | Tests | Deps |
|-------|------|-------|------|
| 1 | Store interface, Sample, Point types | Compile-only | stdlib |
| 2 | memstore | Append, query, series, retention, concurrency | 1 |
| 3 | Options + validation | Defaults, required fields | stdlib |
| 4 | Recorder, Series, cardinality, flush loop | Record, flush, concurrency, budget, close | 1-3 |
| 5 | Snapshot, Summary, DumpTo | Range queries, aggregation, rate, percentiles, streaming | 4 |
| 6 | .swage serialization (WriteTo, ReadSnapshot) | Round-trip serialize/deserialize | 5 |
| 7 | ingotstore | Integration: temp dirs, restart, retention, flock | 1 + ingot |
| 8 | swagectl | recover, ls, read, summary | 6 + 7 |
| 9 | doc.go + README | Human review | all |

Phases 1-3 can proceed in parallel. Phase 7 is the only one that introduces
the ingot dependency.

---

## Open Items

Deferred to future versions, with seams noted:

- **Rollups (v2):** Downsampling with percentile sketches. Summary API
  stays the same; implementation reads from rollups instead of raw data.
- **Triggers (v1.1):** Condition-based snapshot triggers registered with the
  Recorder.
- **HTTP handler (future):** Expose Snapshot/Summary over HTTP for live
  queries.
- **OTel adapter (future):** `swage/oteladapter` implementing OTel exporter.
- **Labels (v2):** Extend series identification to name + labels. ingotstore
  already supports this natively.
