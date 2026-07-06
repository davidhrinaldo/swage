// Package memstore provides an in-memory Store implementation for testing.
package memstore

import (
	"sort"
	"sync"
	"time"

	"github.com/davidrinaldo/swage"
)

// Compile-time interface check.
var _ swage.Store = (*Store)(nil)

// Store is an in-memory implementation of swage.Store. It is safe for
// concurrent use. If horizon is positive, a background goroutine removes
// samples older than horizon on a regular interval.
type Store struct {
	mu      sync.RWMutex
	data    map[string][]swage.Sample
	horizon time.Duration
	clock   func() time.Time
	done    chan struct{}
}

// New creates an in-memory Store. If horizon is positive, a background
// goroutine evicts samples older than horizon every second. Call Close to
// stop the goroutine.
func New(horizon time.Duration) *Store {
	return NewWithClock(horizon, time.Now)
}

// NewWithClock is like New but accepts a custom clock for testing retention.
func NewWithClock(horizon time.Duration, clock func() time.Time) *Store {
	s := &Store{
		data:    make(map[string][]swage.Sample),
		horizon: horizon,
		clock:   clock,
		done:    make(chan struct{}),
	}
	if horizon > 0 {
		go s.retentionLoop()
	}
	return s
}

// Append stores samples in memory. Samples are assumed to be sorted by
// (Name, T) as specified by the Store contract.
func (s *Store) Append(samples []swage.Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sample := range samples {
		s.data[sample.Name] = append(s.data[sample.Name], sample)
	}
	return nil
}

// Query returns samples for the named series in the time range [from, to]
// (Unix milliseconds, inclusive). Results are ordered by timestamp.
func (s *Store) Query(name string, from, to int64) ([]swage.Sample, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	all := s.data[name]
	if len(all) == 0 {
		return nil, nil
	}

	// Binary search for the start index.
	start := sort.Search(len(all), func(i int) bool {
		return all[i].T >= from
	})

	var result []swage.Sample
	for i := start; i < len(all); i++ {
		if all[i].T > to {
			break
		}
		result = append(result, all[i])
	}
	return result, nil
}

// Series returns the names of all series that have at least one sample.
func (s *Store) Series() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.data))
	for name, samples := range s.data {
		if len(samples) > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// Close stops the retention goroutine (if running) and releases resources.
// It is safe to call Close multiple times.
func (s *Store) Close() error {
	select {
	case <-s.done:
		// Already closed.
	default:
		close(s.done)
	}
	return nil
}

func (s *Store) retentionLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.evict()
		}
	}
}

func (s *Store) evict() {
	cutoff := s.clock().Add(-s.horizon).UnixMilli()

	s.mu.Lock()
	defer s.mu.Unlock()

	for name, samples := range s.data {
		// Binary search for the first sample at or after cutoff.
		idx := sort.Search(len(samples), func(i int) bool {
			return samples[i].T >= cutoff
		})
		if idx == len(samples) {
			delete(s.data, name)
		} else if idx > 0 {
			// Copy to release memory from the underlying array.
			kept := make([]swage.Sample, len(samples)-idx)
			copy(kept, samples[idx:])
			s.data[name] = kept
		}
	}
}
