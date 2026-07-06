package swage

import "time"

// Sample is a single timestamped value in a named series.
type Sample struct {
	Name string  // Series name (non-empty, valid UTF-8).
	T    int64   // Unix milliseconds.
	V    float64 // Finite float64 value.
}

// Point is a timestamped value used in query results.
type Point struct {
	T time.Time
	V float64
}

// Store is the storage backend for a Recorder. Implementations must be safe
// for concurrent reads (Query, Series) while a single writer calls Append.
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
