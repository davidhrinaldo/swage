package swage

import (
	"math"
	"sort"
	"sync"
	"time"
	"unicode/utf8"
)

// Recorder is the single entry point for recording application metrics.
// It owns a write buffer, a flush goroutine, and a reference to a Store.
// All methods are safe for concurrent use.
type Recorder struct {
	store Store
	opts  Options

	mu    sync.Mutex
	buf   []Sample
	spare []Sample // pre-allocated spare for swap-and-reuse

	// seriesMap tracks last-recorded timestamp per series name for
	// cardinality limiting and stale eviction.
	seriesMap map[string]int64

	// lastFlushed tracks the last-flushed timestamp per series for
	// cross-batch timestamp clamping.
	lastFlushed map[string]int64

	// evictGen tracks eviction generations. Series handles compare
	// against this to know when to re-check cardinality. Starts at 1
	// so freshly created handles (lastGen=0) always do the initial check.
	evictGen uint64

	closed bool

	flushReq  chan chan error // send a channel to get notified when flush completes
	done      chan struct{}
	flushDone chan struct{} // closed when flush goroutine exits
}

// New creates a Recorder that writes to the given Store. The caller owns
// the Store — Close does not close it. Returns an error if opts are invalid.
func New(store Store, opts Options) (*Recorder, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	opts.defaults()

	r := &Recorder{
		store:       store,
		opts:        opts,
		buf:         make([]Sample, 0, opts.MaxBufferSize),
		spare:       make([]Sample, 0, opts.MaxBufferSize),
		seriesMap:   make(map[string]int64),
		lastFlushed: make(map[string]int64),
		evictGen:    1,
		flushReq:    make(chan chan error),
		done:        make(chan struct{}),
		flushDone:   make(chan struct{}),
	}
	go r.flushLoop()
	return r, nil
}

// Record appends a sample with the current timestamp. Fire-and-forget: never
// returns an error or panics. After Close, it is a silent no-op.
func (r *Recorder) Record(name string, value float64) {
	r.record(name, r.opts.Clock(), value)
}

// RecordAt appends a sample with the given timestamp. Fire-and-forget: never
// returns an error or panics. After Close, it is a silent no-op.
func (r *Recorder) RecordAt(name string, t time.Time, value float64) {
	r.record(name, t, value)
}

// record is the shared implementation for Record and RecordAt.
func (r *Recorder) record(name string, t time.Time, value float64) {
	s := Sample{Name: name, T: t.UnixMilli(), V: value}

	// Validate before acquiring the lock.
	if name == "" || !utf8.ValidString(name) {
		r.opts.OnOverBudget("invalid_name", s)
		return
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		r.opts.OnOverBudget("invalid_value", s)
		return
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}

	// Buffer cap check.
	if len(r.buf) >= r.opts.MaxBufferSize {
		r.mu.Unlock()
		r.opts.OnOverBudget("max_buffer", s)
		return
	}

	// Cardinality check.
	if _, exists := r.seriesMap[name]; !exists {
		if len(r.seriesMap) >= r.opts.MaxSeries {
			r.mu.Unlock()
			r.opts.OnOverBudget("max_series", s)
			return
		}
	}
	r.seriesMap[name] = s.T

	r.buf = append(r.buf, s)
	r.mu.Unlock()
}

// Series returns a handle for fast-path recording of the named series.
// The handle skips name validation on the hot path. If name is empty or
// invalid UTF-8, every Record call on the handle will be silently dropped
// via OnOverBudget.
func (r *Recorder) Series(name string) *Series {
	return &Series{r: r, name: name}
}

// Flush sends a request to the flush goroutine and blocks until the flush
// is complete. Safe for concurrent use.
func (r *Recorder) Flush() error {
	ch := make(chan error, 1)
	select {
	case r.flushReq <- ch:
		return <-ch
	case <-r.done:
		return nil
	}
}

// Close flushes remaining samples, stops the flush goroutine, and marks
// the Recorder as closed. After Close, Record and RecordAt are silent
// no-ops. Close does not close the Store — the caller manages its lifecycle.
func (r *Recorder) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()

	// Signal the flush goroutine to stop and wait for it.
	close(r.done)
	<-r.flushDone
	return nil
}

// flushLoop runs in a dedicated goroutine. It is the only goroutine that
// calls Store.Append, preserving the single-writer invariant.
func (r *Recorder) flushLoop() {
	defer close(r.flushDone)

	ticker := time.NewTicker(r.opts.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.doFlush()
		case ch := <-r.flushReq:
			ch <- r.doFlush()
		case <-r.done:
			// Final flush before exit.
			r.doFlush()
			return
		}
	}
}

// doFlush swaps the buffer, sorts, clamps timestamps, and appends to the Store.
func (r *Recorder) doFlush() error {
	r.mu.Lock()
	if len(r.buf) == 0 {
		r.mu.Unlock()
		return nil
	}

	// Swap buffer with spare — zero steady-state allocation.
	batch := r.buf
	r.buf = r.spare[:0]
	r.spare = batch // will be reused after this flush
	r.mu.Unlock()

	// Sort by (Name, T).
	sort.Slice(batch, func(i, j int) bool {
		if batch[i].Name != batch[j].Name {
			return batch[i].Name < batch[j].Name
		}
		return batch[i].T < batch[j].T
	})

	// Clamp timestamps to preserve per-series ordering across batches.
	for i := range batch {
		last, exists := r.lastFlushed[batch[i].Name]
		if exists && batch[i].T <= last {
			batch[i].T = last + 1
		}
		r.lastFlushed[batch[i].Name] = batch[i].T
	}

	// Evict stale series from the cardinality map.
	r.evictStaleSeries()

	err := r.store.Append(batch)
	if err != nil {
		r.opts.OnFlushError(err)
	}
	return err
}

// evictStaleSeries removes entries from the cardinality map that are older
// than the horizon, freeing cardinality slots for new series.
func (r *Recorder) evictStaleSeries() {
	cutoff := r.opts.Clock().Add(-r.opts.Horizon).UnixMilli()

	r.mu.Lock()
	defer r.mu.Unlock()

	evicted := false
	for name, lastT := range r.seriesMap {
		if lastT < cutoff {
			delete(r.seriesMap, name)
			evicted = true
		}
	}
	if evicted {
		r.evictGen++
	}
}
