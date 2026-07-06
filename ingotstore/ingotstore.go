// Package ingotstore provides a durable Store implementation backed by ingot.
// It is the only package in swage that imports ingot.
package ingotstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/davidhrinaldo/ingot"
	"github.com/davidhrinaldo/ingot/labels"
	"github.com/davidrinaldo/swage"
)

// Compile-time interface check.
var _ swage.Store = (*Store)(nil)

// lockFileName is the advisory lock file name in the data directory.
const lockFileName = "swage.lock"

// Store is a durable swage.Store backed by ingot. It maps series names to
// ingot label sets with a single __name__ label. Safe for concurrent reads
// (Query, Series) while a single writer calls Append.
type Store struct {
	db       *ingot.DB
	lockFile *os.File

	// refs caches ingot series references for fast-path appends.
	// Only accessed from the single Append goroutine — no mutex needed.
	refs map[string]uint64

	// closeOnce ensures Close is idempotent.
	closeOnce sync.Once
	closeErr  error
}

// New opens (or creates) an ingot database in dir with the given retention
// horizon. It takes an advisory flock(2) on swage.lock in the data directory.
// Returns an error if the lock is already held (directory in use by another
// process).
func New(dir string, horizon time.Duration) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("ingotstore: create dir: %w", err)
	}

	// Acquire advisory lock.
	lockPath := filepath.Join(dir, lockFileName)
	lockFile, err := acquireLock(lockPath)
	if err != nil {
		return nil, err
	}

	db, err := ingot.Open(dir, ingot.Options{
		Retention: horizon,
	})
	if err != nil {
		releaseLock(lockFile)
		return nil, fmt.Errorf("ingotstore: open: %w", err)
	}

	return &Store{
		db:       db,
		lockFile: lockFile,
		refs:     make(map[string]uint64),
	}, nil
}

// Recorder wraps a swage.Recorder and owns the underlying Store. Its Close
// method closes both the Recorder and the Store.
type Recorder struct {
	*swage.Recorder
	store *Store
}

// Close flushes remaining samples, stops the Recorder, and closes the
// underlying Store (releasing the flock).
func (r *Recorder) Close() error {
	recErr := r.Recorder.Close()
	storeErr := r.store.Close()
	if recErr != nil {
		return recErr
	}
	return storeErr
}

// OpenRecorder is a convenience that creates both a Store and a Recorder.
// The returned Recorder owns the Store — its Close closes both. Sets
// opts.Horizon as the ingot retention.
func OpenRecorder(dir string, opts swage.Options) (*Recorder, error) {
	store, err := New(dir, opts.Horizon)
	if err != nil {
		return nil, err
	}

	rec, err := swage.New(store, opts)
	if err != nil {
		store.Close()
		return nil, err
	}

	return &Recorder{Recorder: rec, store: store}, nil
}

// Append durably writes a batch of samples. Creates an ingot Appender,
// appends each sample, and commits. Series name N maps to ingot label set
// {__name__=N}. Cached refs provide fast-path appends for known series.
func (s *Store) Append(samples []swage.Sample) error {
	if len(samples) == 0 {
		return nil
	}

	app := s.db.Appender()

	for _, sample := range samples {
		ref := s.refs[sample.Name]
		var ls []labels.Label
		if ref == 0 {
			ls = labels.FromStrings("__name__", sample.Name)
		}

		newRef, err := app.Append(ref, ls, sample.T, sample.V)
		if err != nil {
			app.Rollback()
			return fmt.Errorf("ingotstore: append %q: %w", sample.Name, err)
		}
		if ref == 0 {
			s.refs[sample.Name] = newRef
		}
	}

	if err := app.Commit(); err != nil {
		return fmt.Errorf("ingotstore: commit: %w", err)
	}
	return nil
}

// Query returns samples for the named series in [from, to] (Unix ms).
// Results are ordered by timestamp.
func (s *Store) Query(name string, from, to int64) ([]swage.Sample, error) {
	q, err := s.db.Querier(from, to)
	if err != nil {
		return nil, fmt.Errorf("ingotstore: querier: %w", err)
	}
	defer q.Close()

	matcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", name)
	ss := q.Select(matcher)

	var result []swage.Sample
	for ss.Next() {
		iter := ss.At().Iterator()
		for iter.Next() {
			t, v := iter.At()
			result = append(result, swage.Sample{Name: name, T: t, V: v})
		}
		if err := iter.Err(); err != nil {
			return nil, fmt.Errorf("ingotstore: iterate %q: %w", name, err)
		}
	}
	if err := ss.Err(); err != nil {
		return nil, fmt.Errorf("ingotstore: select %q: %w", name, err)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].T < result[j].T
	})
	return result, nil
}

// Series returns the names of all known series.
func (s *Store) Series() ([]string, error) {
	// Query the full time range to get all series.
	q, err := s.db.Querier(0, 1<<62)
	if err != nil {
		return nil, fmt.Errorf("ingotstore: querier: %w", err)
	}
	defer q.Close()

	// Match all series that have a __name__ label.
	matcher := labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".+")
	ss := q.Select(matcher)

	var names []string
	for ss.Next() {
		lbls := ss.At().Labels()
		for _, l := range lbls {
			if l.Name == "__name__" {
				names = append(names, l.Value)
				break
			}
		}
	}
	if err := ss.Err(); err != nil {
		return nil, fmt.Errorf("ingotstore: series: %w", err)
	}

	sort.Strings(names)
	return names, nil
}

// Close releases the flock and closes ingot.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		dbErr := s.db.Close()
		lockErr := releaseLock(s.lockFile)
		if dbErr != nil {
			s.closeErr = dbErr
		} else {
			s.closeErr = lockErr
		}
	})
	return s.closeErr
}
