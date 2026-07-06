// Package swage is an embeddable flight recorder for application metrics.
//
// swage continuously records named float64 time series into a durable,
// compressed on-disk ring backed by [ingot]. When something goes wrong,
// you query the window of metric history leading up to it. Think of it
// as the black box for your application's numbers.
//
// swage records application-domain metrics -- request latencies, queue
// depths, error rates, sensor readings, business KPIs. It is not to be
// confused with Go's [runtime/trace.FlightRecorder], which records
// runtime execution traces (goroutine scheduling, GC events, syscalls).
// The two are complementary: use the stdlib recorder to understand how
// your runtime behaved, use swage to understand what your application
// was doing.
//
// # Core API
//
// The entry point is a [Recorder], created with [New] (caller-managed Store)
// or [ingotstore.OpenRecorder] (convenience that creates both):
//
//   - [Recorder.Record] and [Recorder.RecordAt] append timestamped samples.
//     Fire-and-forget: they never return errors or panic.
//   - [Recorder.Series] returns a [Series] handle for fast-path recording
//     of a single named series, skipping per-call cardinality checks.
//   - [Recorder.Snapshot] materializes raw samples for a time range.
//     Point-in-time consistent.
//   - [Recorder.Summary] computes windowed aggregates (min, max, mean,
//     p50, p95, p99, rate) without materializing all series at once.
//   - [Recorder.DumpTo] streams a .swage dump (NDJSON) to an [io.Writer]
//     without materializing all samples.
//   - [Recorder.Close] flushes remaining samples and stops background work.
//
// # The .swage file
//
// A .swage file is a self-contained NDJSON dump: a header line followed by
// sample lines grouped by series name. It is the unit of exchange -- produce
// it on the box with [Recorder.DumpTo] or [Snapshot.WriteTo], transfer it
// anywhere, analyze it with swagectl or any JSON tool. [ReadSnapshot]
// deserializes a .swage file back into a [Snapshot].
//
// # Usage
//
// Record metrics and query the history:
//
//	rec, err := ingotstore.OpenRecorder("./flight-data", swage.Options{
//	    Horizon: 7 * 24 * time.Hour,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer rec.Close()
//
//	// Record from anywhere in your app.
//	rec.Record("request_latency_ms", 12.4)
//	rec.Record("queue_depth", 42)
//
//	// Raw samples for the last 30 minutes.
//	snap, err := rec.Snapshot(
//	    time.Now().Add(-30*time.Minute), time.Now(),
//	)
//
//	// Windowed aggregates in 5-minute buckets.
//	sum, err := rec.Summary(
//	    time.Now().Add(-1*time.Hour), time.Now(),
//	    5*time.Minute,
//	)
//
// # Safety guarantees
//
// swage is designed to run in production forever:
//
//   - Bounded memory: cardinality limits ([Options.MaxSeries]) and buffer
//     caps ([Options.MaxBufferSize]) prevent unbounded growth. Query memory
//     is bounded by the caller's requested range.
//   - Bounded disk: time-based retention ([Options.Horizon]) with automatic
//     cleanup. No unbounded accumulation.
//   - No panics: [Recorder.Record] and [Recorder.RecordAt] are silent no-ops
//     after [Recorder.Close]. Invalid names and non-finite values are dropped
//     via the [Options.OnOverBudget] callback.
package swage
