package ingotstore

import (
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/davidrinaldo/swage"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) string
	}{
		{
			name: "creates database in temp dir",
			setup: func(t *testing.T) string {
				return t.TempDir()
			},
		},
		{
			name: "creates nested dir if needed",
			setup: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "sub", "dir")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := tt.setup(t)
			store, err := New(dir, time.Hour)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer store.Close()

			// Lock file should exist.
			if _, err := os.Stat(filepath.Join(dir, lockFileName)); err != nil {
				t.Errorf("lock file not found: %v", err)
			}
		})
	}
}

func TestNewErrors(t *testing.T) {
	tests := []struct {
		name string
		dir  string
	}{
		{
			name: "invalid dir returns error",
			dir:  "/dev/null/invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.dir, time.Hour)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestFlockPreventsConcurrentOpen(t *testing.T) {
	dir := t.TempDir()

	store, err := New(dir, time.Hour)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	defer store.Close()

	_, err = New(dir, time.Hour)
	if err == nil {
		t.Fatal("expected error for concurrent open, got nil")
	}
}

func TestCloseReleasesLock(t *testing.T) {
	dir := t.TempDir()

	store1, err := New(dir, time.Hour)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := store1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Re-open should succeed after close.
	store2, err := New(dir, time.Hour)
	if err != nil {
		t.Fatalf("re-open after close: %v", err)
	}
	store2.Close()
}

func TestAppendAndQuery(t *testing.T) {
	tests := []struct {
		name    string
		batches [][]swage.Sample
		query   string
		from    int64
		to      int64
		want    []swage.Sample
	}{
		{
			name: "single sample round-trip",
			batches: [][]swage.Sample{
				{{Name: "cpu", T: 1000, V: 42.0}},
			},
			query: "cpu",
			from:  0,
			to:    2000,
			want: []swage.Sample{
				{Name: "cpu", T: 1000, V: 42.0},
			},
		},
		{
			name: "multiple samples ordered by timestamp",
			batches: [][]swage.Sample{
				{
					{Name: "cpu", T: 1000, V: 1.0},
					{Name: "cpu", T: 2000, V: 2.0},
					{Name: "cpu", T: 3000, V: 3.0},
				},
			},
			query: "cpu",
			from:  0,
			to:    5000,
			want: []swage.Sample{
				{Name: "cpu", T: 1000, V: 1.0},
				{Name: "cpu", T: 2000, V: 2.0},
				{Name: "cpu", T: 3000, V: 3.0},
			},
		},
		{
			name: "query filters by time range",
			batches: [][]swage.Sample{
				{
					{Name: "cpu", T: 1000, V: 1.0},
					{Name: "cpu", T: 2000, V: 2.0},
					{Name: "cpu", T: 3000, V: 3.0},
				},
			},
			query: "cpu",
			from:  1500,
			to:    2500,
			want: []swage.Sample{
				{Name: "cpu", T: 2000, V: 2.0},
			},
		},
		{
			name: "query with no matching series returns empty",
			batches: [][]swage.Sample{
				{{Name: "cpu", T: 1000, V: 1.0}},
			},
			query: "mem",
			from:  0,
			to:    2000,
			want:  nil,
		},
		{
			name:    "query on empty store returns empty",
			batches: nil,
			query:   "cpu",
			from:    0,
			to:      2000,
			want:    nil,
		},
		{
			name: "multiple series only returns requested",
			batches: [][]swage.Sample{
				{
					{Name: "cpu", T: 1000, V: 1.0},
					{Name: "mem", T: 1000, V: 99.0},
				},
			},
			query: "mem",
			from:  0,
			to:    2000,
			want: []swage.Sample{
				{Name: "mem", T: 1000, V: 99.0},
			},
		},
		{
			name: "multiple batches returns ordered results",
			batches: [][]swage.Sample{
				{{Name: "cpu", T: 1000, V: 1.0}},
				{{Name: "cpu", T: 2000, V: 2.0}},
				{{Name: "cpu", T: 3000, V: 3.0}},
				{{Name: "cpu", T: 4000, V: 4.0}},
				{{Name: "cpu", T: 5000, V: 5.0}},
			},
			query: "cpu",
			from:  0,
			to:    10000,
			want: []swage.Sample{
				{Name: "cpu", T: 1000, V: 1.0},
				{Name: "cpu", T: 2000, V: 2.0},
				{Name: "cpu", T: 3000, V: 3.0},
				{Name: "cpu", T: 4000, V: 4.0},
				{Name: "cpu", T: 5000, V: 5.0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStore(t)

			for i, batch := range tt.batches {
				if err := store.Append(batch); err != nil {
					t.Fatalf("append batch %d: %v", i, err)
				}
			}

			got, err := store.Query(tt.query, tt.from, tt.to)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAppendCachedRefs(t *testing.T) {
	store := newTestStore(t)

	// Append multiple batches to the same series to exercise the ref cache.
	batch1 := []swage.Sample{
		{Name: "cpu", T: 1000, V: 1.0},
		{Name: "mem", T: 1000, V: 10.0},
	}
	batch2 := []swage.Sample{
		{Name: "cpu", T: 2000, V: 2.0},
		{Name: "mem", T: 2000, V: 20.0},
	}
	batch3 := []swage.Sample{
		{Name: "cpu", T: 3000, V: 3.0},
	}

	for i, batch := range [][]swage.Sample{batch1, batch2, batch3} {
		if err := store.Append(batch); err != nil {
			t.Fatalf("append batch %d: %v", i+1, err)
		}
	}

	got, err := store.Query("cpu", 0, 5000)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	want := []swage.Sample{
		{Name: "cpu", T: 1000, V: 1.0},
		{Name: "cpu", T: 2000, V: 2.0},
		{Name: "cpu", T: 3000, V: 3.0},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// Verify refs were cached (should have entries for both series).
	if len(store.refs) != 2 {
		t.Errorf("expected 2 cached refs, got %d", len(store.refs))
	}
}

func TestSeries(t *testing.T) {
	tests := []struct {
		name    string
		batches [][]swage.Sample
		want    []string
	}{
		{
			name:    "empty store returns empty",
			batches: nil,
			want:    nil,
		},
		{
			name: "returns all known names sorted",
			batches: [][]swage.Sample{
				{
					{Name: "mem", T: 1000, V: 1.0},
					{Name: "cpu", T: 1000, V: 2.0},
					{Name: "disk", T: 1000, V: 3.0},
				},
			},
			want: []string{"cpu", "disk", "mem"},
		},
		{
			name: "single series",
			batches: [][]swage.Sample{
				{
					{Name: "cpu", T: 1000, V: 1.0},
					{Name: "cpu", T: 2000, V: 2.0},
				},
			},
			want: []string{"cpu"},
		},
		{
			name: "multiple batches accumulate series",
			batches: [][]swage.Sample{
				{{Name: "cpu", T: 1000, V: 1.0}},
				{{Name: "mem", T: 2000, V: 2.0}},
			},
			want: []string{"cpu", "mem"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStore(t)

			for i, batch := range tt.batches {
				if err := store.Append(batch); err != nil {
					t.Fatalf("append batch %d: %v", i, err)
				}
			}

			got, err := store.Series()
			if err != nil {
				t.Fatalf("series: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConcurrentQueryDuringAppend(t *testing.T) {
	store := newTestStore(t)

	// Seed some initial data.
	if err := store.Append([]swage.Sample{
		{Name: "cpu", T: 1000, V: 1.0},
	}); err != nil {
		t.Fatalf("seed append: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	// Writer goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(0); i < 10; i++ {
			err := store.Append([]swage.Sample{
				{Name: "cpu", T: 2000 + i*1000, V: float64(i)},
			})
			if err != nil {
				errs <- err
			}
		}
	}()

	// Reader goroutines.
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_, err := store.Query("cpu", 0, 100000)
				if err != nil {
					errs <- err
				}
				_, err = store.Series()
				if err != nil {
					errs <- err
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent error: %v", err)
	}
}

func TestOpenRecorder(t *testing.T) {
	dir := t.TempDir()

	rec, err := OpenRecorder(dir, swage.Options{
		Horizon: time.Hour,
	})
	if err != nil {
		t.Fatalf("open recorder: %v", err)
	}

	// Record a value and verify it's queryable.
	rec.Record("cpu", 42.0)
	if err := rec.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	snap, err := rec.Snapshot(time.Now().Add(-time.Minute), time.Now())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap.Series) == 0 {
		t.Error("expected at least one series in snapshot")
	}

	if err := rec.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestOpenRecorderCloseClosesBoth(t *testing.T) {
	dir := t.TempDir()

	rec, err := OpenRecorder(dir, swage.Options{
		Horizon: time.Hour,
	})
	if err != nil {
		t.Fatalf("open recorder: %v", err)
	}

	if err := rec.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The flock should be released — re-opening should succeed.
	rec2, err := OpenRecorder(dir, swage.Options{
		Horizon: time.Hour,
	})
	if err != nil {
		t.Fatalf("re-open after close: %v", err)
	}
	rec2.Close()
}

func TestOpenRecorderInvalidOpts(t *testing.T) {
	dir := t.TempDir()

	// Zero horizon is invalid.
	_, err := OpenRecorder(dir, swage.Options{})
	if err == nil {
		t.Fatal("expected error for zero horizon, got nil")
	}
}

func TestAppendEmptyBatch(t *testing.T) {
	tests := []struct {
		name    string
		samples []swage.Sample
	}{
		{name: "nil slice", samples: nil},
		{name: "empty slice", samples: []swage.Sample{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Append(tt.samples); err != nil {
				t.Fatalf("append: %v", err)
			}
		})
	}
}


func TestCloseIsIdempotent(t *testing.T) {
	store := newTestStore(t)

	if err := store.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}


// newTestStore creates a Store in a temp directory and registers cleanup.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	store, err := New(dir, time.Hour)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}
