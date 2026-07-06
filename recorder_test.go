package swage_test

import (
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/davidrinaldo/swage"
	"github.com/davidrinaldo/swage/memstore"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		opts    swage.Options
		wantErr bool
	}{
		{
			name:    "valid options",
			opts:    swage.Options{Horizon: time.Hour},
			wantErr: false,
		},
		{
			name:    "zero horizon",
			opts:    swage.Options{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := memstore.New(0)
			defer store.Close()

			rec, err := swage.New(store, tt.opts)
			if (err != nil) != tt.wantErr {
				t.Fatalf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
			if rec != nil {
				rec.Close()
			}
		})
	}
}

func TestRecordAndFlush(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rec, err := swage.New(store, swage.Options{
		Horizon: time.Hour,
		Clock:   func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	rec.Record("cpu", 1.0)
	rec.Record("cpu", 2.0)
	rec.Record("mem", 42.0)

	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := store.Query("cpu", 0, now.UnixMilli()+1000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 cpu samples, got %d", len(got))
	}
	if got[0].V != 1.0 || got[1].V != 2.0 {
		t.Errorf("cpu values = [%v, %v], want [1.0, 2.0]", got[0].V, got[1].V)
	}

	got, err = store.Query("mem", 0, now.UnixMilli()+1000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 1 || got[0].V != 42.0 {
		t.Errorf("mem samples = %v, want [{mem %d 42}]", got, now.UnixMilli())
	}
}

func TestRecordAt(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	rec, err := swage.New(store, swage.Options{Horizon: time.Hour})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	t1 := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 15, 12, 0, 1, 0, time.UTC)

	rec.RecordAt("latency", t1, 5.5)
	rec.RecordAt("latency", t2, 6.5)

	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := store.Query("latency", t1.UnixMilli(), t2.UnixMilli())
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(got))
	}
	if got[0].T != t1.UnixMilli() || got[1].T != t2.UnixMilli() {
		t.Errorf("timestamps = [%d, %d], want [%d, %d]",
			got[0].T, got[1].T, t1.UnixMilli(), t2.UnixMilli())
	}
	if got[0].V != 5.5 || got[1].V != 6.5 {
		t.Errorf("values = [%v, %v], want [5.5, 6.5]", got[0].V, got[1].V)
	}
}

func TestFlushSortsByNameThenTime(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	ts := int64(1000)
	rec, err := swage.New(store, swage.Options{
		Horizon: time.Hour,
		Clock:   func() time.Time { return time.UnixMilli(ts) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	// Record out of order: b before a, and timestamps scrambled.
	rec.RecordAt("b", time.UnixMilli(2000), 2.0)
	rec.RecordAt("a", time.UnixMilli(3000), 3.0)
	rec.RecordAt("b", time.UnixMilli(1000), 1.0)
	rec.RecordAt("a", time.UnixMilli(1000), 1.0)

	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Verify series "a" is sorted by time.
	gotA, err := store.Query("a", 0, 5000)
	if err != nil {
		t.Fatalf("Query(a) error = %v", err)
	}
	if len(gotA) != 2 {
		t.Fatalf("expected 2 samples for a, got %d", len(gotA))
	}
	if gotA[0].T >= gotA[1].T {
		t.Errorf("a samples not sorted: T=[%d, %d]", gotA[0].T, gotA[1].T)
	}

	// Verify series "b" is sorted by time.
	gotB, err := store.Query("b", 0, 5000)
	if err != nil {
		t.Fatalf("Query(b) error = %v", err)
	}
	if len(gotB) != 2 {
		t.Fatalf("expected 2 samples for b, got %d", len(gotB))
	}
	if gotB[0].T >= gotB[1].T {
		t.Errorf("b samples not sorted: T=[%d, %d]", gotB[0].T, gotB[1].T)
	}
}

func TestTimestampClampingAcrossBatches(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	rec, err := swage.New(store, swage.Options{Horizon: time.Hour})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	// Batch 1: sample at t=5000.
	rec.RecordAt("cpu", time.UnixMilli(5000), 1.0)
	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Batch 2: sample at t=3000 (clock went backwards).
	rec.RecordAt("cpu", time.UnixMilli(3000), 2.0)
	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := store.Query("cpu", 0, 10000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(got))
	}

	// First sample should be at 5000, second should be clamped to 5001.
	if got[0].T != 5000 {
		t.Errorf("first sample T = %d, want 5000", got[0].T)
	}
	if got[1].T != 5001 {
		t.Errorf("second sample T = %d, want 5001 (clamped)", got[1].T)
	}
	if got[1].V != 2.0 {
		t.Errorf("second sample V = %v, want 2.0", got[1].V)
	}
}

func TestTimestampClampingWithinBatch(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	rec, err := swage.New(store, swage.Options{Horizon: time.Hour})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	// First batch establishes lastFlushed for "cpu" at 5000.
	rec.RecordAt("cpu", time.UnixMilli(5000), 1.0)
	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Second batch: two samples both at t=3000 (both need clamping).
	rec.RecordAt("cpu", time.UnixMilli(3000), 2.0)
	rec.RecordAt("cpu", time.UnixMilli(3000), 3.0)
	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := store.Query("cpu", 0, 10000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 samples, got %d", len(got))
	}

	// Should be 5000, 5001, 5002 after clamping.
	if got[0].T != 5000 {
		t.Errorf("sample[0].T = %d, want 5000", got[0].T)
	}
	if got[1].T != 5001 {
		t.Errorf("sample[1].T = %d, want 5001", got[1].T)
	}
	if got[2].T != 5002 {
		t.Errorf("sample[2].T = %d, want 5002", got[2].T)
	}
}

func TestCardinalityLimit(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	var drops []string
	var dropMu sync.Mutex

	rec, err := swage.New(store, swage.Options{
		Horizon:   time.Hour,
		MaxSeries: 2,
		OnOverBudget: func(reason string, s swage.Sample) {
			dropMu.Lock()
			drops = append(drops, reason+":"+s.Name)
			dropMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	rec.RecordAt("a", time.UnixMilli(1000), 1.0)
	rec.RecordAt("b", time.UnixMilli(1000), 2.0)
	rec.RecordAt("c", time.UnixMilli(1000), 3.0) // should be dropped

	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := store.Query("c", 0, 5000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("series c should have been dropped, got %d samples", len(got))
	}

	dropMu.Lock()
	if len(drops) != 1 || drops[0] != "max_series:c" {
		t.Errorf("drops = %v, want [max_series:c]", drops)
	}
	dropMu.Unlock()
}

func TestCardinalityEviction(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	mu := sync.Mutex{}
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	rec, err := swage.New(store, swage.Options{
		Horizon:   10 * time.Second,
		MaxSeries: 2,
		Clock:     clock,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	// Fill both cardinality slots.
	rec.RecordAt("a", now, 1.0)
	rec.RecordAt("b", now, 2.0)

	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Advance time past horizon so both entries are stale.
	mu.Lock()
	now = now.Add(20 * time.Second)
	mu.Unlock()

	// Record "a" to keep it alive, then flush to trigger eviction.
	rec.RecordAt("a", now, 3.0)
	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Now "b" should be evicted. A new series "c" should fit.
	rec.RecordAt("c", now, 4.0)
	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := store.Query("c", 0, now.UnixMilli()+1000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 1 {
		t.Errorf("expected 1 sample for c after eviction, got %d", len(got))
	}
}

func TestMaxBufferSize(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	var drops int
	var dropMu sync.Mutex

	rec, err := swage.New(store, swage.Options{
		Horizon:       time.Hour,
		MaxBufferSize: 3,
		FlushInterval: time.Hour, // don't auto-flush
		OnOverBudget: func(reason string, s swage.Sample) {
			dropMu.Lock()
			drops++
			dropMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	// Record 5 samples — only 3 should fit.
	for i := 0; i < 5; i++ {
		rec.RecordAt("cpu", time.UnixMilli(int64(1000+i)), float64(i))
	}

	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := store.Query("cpu", 0, 10000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 samples (buffer cap), got %d", len(got))
	}

	dropMu.Lock()
	if drops != 2 {
		t.Errorf("expected 2 drops, got %d", drops)
	}
	dropMu.Unlock()
}

func TestConcurrentRecording(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	rec, err := swage.New(store, swage.Options{
		Horizon:       time.Hour,
		MaxBufferSize: 100_000,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	const (
		numGoroutines = 8
		numPerG       = 500
	)

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < numPerG; i++ {
				rec.RecordAt("series", time.UnixMilli(int64(id*numPerG+i+1)), float64(i))
			}
		}(g)
	}
	wg.Wait()

	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := store.Query("series", 0, int64(numGoroutines*numPerG+1))
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != numGoroutines*numPerG {
		t.Errorf("expected %d samples, got %d", numGoroutines*numPerG, len(got))
	}
}

func TestCloseSemantics(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	rec, err := swage.New(store, swage.Options{Horizon: time.Hour})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	rec.RecordAt("cpu", time.UnixMilli(1000), 1.0)

	if err := rec.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Final flush during Close should have persisted the sample.
	got, err := store.Query("cpu", 0, 5000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 1 {
		t.Errorf("expected 1 sample after Close flush, got %d", len(got))
	}

	// Recording after Close is a silent no-op.
	rec.Record("cpu", 2.0)
	rec.RecordAt("cpu", time.UnixMilli(2000), 3.0)

	got, err = store.Query("cpu", 0, 5000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 1 {
		t.Errorf("expected 1 sample (no-op after Close), got %d", len(got))
	}

	// Double close is safe.
	if err := rec.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}

func TestInvalidInputs(t *testing.T) {
	tests := []struct {
		name       string
		seriesName string
		value      float64
		wantReason string
	}{
		{
			name:       "empty name",
			seriesName: "",
			value:      1.0,
			wantReason: "invalid_name",
		},
		{
			name:       "invalid UTF-8 name",
			seriesName: "\xff\xfe",
			value:      1.0,
			wantReason: "invalid_name",
		},
		{
			name:       "NaN value",
			seriesName: "cpu",
			value:      math.NaN(),
			wantReason: "invalid_value",
		},
		{
			name:       "positive infinity",
			seriesName: "cpu",
			value:      math.Inf(1),
			wantReason: "invalid_value",
		},
		{
			name:       "negative infinity",
			seriesName: "cpu",
			value:      math.Inf(-1),
			wantReason: "invalid_value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := memstore.New(0)
			defer store.Close()

			var gotReason string
			rec, err := swage.New(store, swage.Options{
				Horizon: time.Hour,
				OnOverBudget: func(reason string, s swage.Sample) {
					gotReason = reason
				},
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			defer rec.Close()

			rec.Record(tt.seriesName, tt.value)

			if gotReason != tt.wantReason {
				t.Errorf("OnOverBudget reason = %q, want %q", gotReason, tt.wantReason)
			}

			// Verify nothing was buffered.
			if err := rec.Flush(); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}

			names, err := store.Series()
			if err != nil {
				t.Fatalf("Series() error = %v", err)
			}
			if len(names) != 0 {
				t.Errorf("expected no series in store, got %v", names)
			}
		})
	}
}

func TestSeriesHandle(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rec, err := swage.New(store, swage.Options{
		Horizon: time.Hour,
		Clock:   func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	cpu := rec.Series("cpu")
	cpu.Record(1.0)
	cpu.Record(2.0)
	cpu.RecordAt(now.Add(time.Second), 3.0)

	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := store.Query("cpu", 0, now.UnixMilli()+5000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 samples via Series handle, got %d", len(got))
	}
	if got[0].V != 1.0 || got[1].V != 2.0 || got[2].V != 3.0 {
		t.Errorf("values = [%v, %v, %v], want [1.0, 2.0, 3.0]",
			got[0].V, got[1].V, got[2].V)
	}
}

func TestSeriesHandleCardinalityLimit(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	var drops []string
	var dropMu sync.Mutex

	rec, err := swage.New(store, swage.Options{
		Horizon:   time.Hour,
		MaxSeries: 1,
		OnOverBudget: func(reason string, s swage.Sample) {
			dropMu.Lock()
			drops = append(drops, reason+":"+s.Name)
			dropMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	a := rec.Series("a")
	b := rec.Series("b")

	a.Record(1.0) // registers "a"
	b.Record(2.0) // should be dropped — cardinality full

	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	gotA, err := store.Query("a", 0, math.MaxInt64)
	if err != nil {
		t.Fatalf("Query(a) error = %v", err)
	}
	if len(gotA) != 1 {
		t.Errorf("expected 1 sample for a, got %d", len(gotA))
	}

	gotB, err := store.Query("b", 0, math.MaxInt64)
	if err != nil {
		t.Fatalf("Query(b) error = %v", err)
	}
	if len(gotB) != 0 {
		t.Errorf("expected 0 samples for b (dropped), got %d", len(gotB))
	}

	dropMu.Lock()
	if len(drops) != 1 || drops[0] != "max_series:b" {
		t.Errorf("drops = %v, want [max_series:b]", drops)
	}
	dropMu.Unlock()
}

func TestSeriesHandleReregistersAfterEviction(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	mu := sync.Mutex{}
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	rec, err := swage.New(store, swage.Options{
		Horizon:   10 * time.Second,
		MaxSeries: 1,
		Clock:     clock,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	a := rec.Series("a")
	a.Record(1.0)

	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Advance time past horizon to evict "a".
	mu.Lock()
	now = now.Add(20 * time.Second)
	mu.Unlock()

	// Flush to trigger eviction.
	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// The handle should re-register on next Record.
	a.Record(2.0)
	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := store.Query("a", 0, now.UnixMilli()+1000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 samples (re-registered handle), got %d", len(got))
	}
}

func TestFlushErrorCallback(t *testing.T) {
	store := &failingStore{}

	var gotErr error
	rec, err := swage.New(store, swage.Options{
		Horizon: time.Hour,
		OnFlushError: func(err error) {
			gotErr = err
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	rec.RecordAt("cpu", time.UnixMilli(1000), 1.0)
	rec.Flush()

	if gotErr == nil {
		t.Errorf("expected OnFlushError to be called")
	}
}

func TestBufferSwapAndReuse(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	rec, err := swage.New(store, swage.Options{Horizon: time.Hour})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	// Multiple flush cycles should work without issues (swap-and-reuse).
	for cycle := 0; cycle < 5; cycle++ {
		rec.RecordAt("cpu", time.UnixMilli(int64(1000+cycle*1000)), float64(cycle))
		if err := rec.Flush(); err != nil {
			t.Fatalf("Flush() cycle %d error = %v", cycle, err)
		}
	}

	got, err := store.Query("cpu", 0, 10000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 5 {
		t.Errorf("expected 5 samples after 5 flush cycles, got %d", len(got))
	}
}

func TestFlushEmptyBuffer(t *testing.T) {
	store := memstore.New(0)
	defer store.Close()

	rec, err := swage.New(store, swage.Options{Horizon: time.Hour})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	// Flushing an empty buffer should not error.
	if err := rec.Flush(); err != nil {
		t.Errorf("Flush() on empty buffer error = %v", err)
	}
}

func TestStoreReceivesSortedBatch(t *testing.T) {
	var received []swage.Sample
	store := &capturingStore{
		onAppend: func(samples []swage.Sample) {
			received = append(received, samples...)
		},
	}

	rec, err := swage.New(store, swage.Options{Horizon: time.Hour})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	rec.RecordAt("z", time.UnixMilli(2000), 2.0)
	rec.RecordAt("a", time.UnixMilli(3000), 3.0)
	rec.RecordAt("z", time.UnixMilli(1000), 1.0)
	rec.RecordAt("a", time.UnixMilli(1000), 1.0)

	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	want := []swage.Sample{
		{Name: "a", T: 1000, V: 1.0},
		{Name: "a", T: 3000, V: 3.0},
		{Name: "z", T: 1000, V: 1.0},
		{Name: "z", T: 2000, V: 2.0},
	}
	if !reflect.DeepEqual(received, want) {
		t.Errorf("Store received:\n  %v\nwant:\n  %v", received, want)
	}
}

// failingStore is a Store that always returns an error on Append.
type failingStore struct{}

func (f *failingStore) Append([]swage.Sample) error                        { return errors.New("append failed") }
func (f *failingStore) Query(string, int64, int64) ([]swage.Sample, error) { return nil, nil }
func (f *failingStore) Series() ([]string, error)                          { return nil, nil }
func (f *failingStore) Close() error                                       { return nil }

// capturingStore captures samples passed to Append for inspection.
type capturingStore struct {
	onAppend func([]swage.Sample)
}

func (c *capturingStore) Append(samples []swage.Sample) error {
	if c.onAppend != nil {
		c.onAppend(samples)
	}
	return nil
}
func (c *capturingStore) Query(string, int64, int64) ([]swage.Sample, error) { return nil, nil }
func (c *capturingStore) Series() ([]string, error)                          { return nil, nil }
func (c *capturingStore) Close() error                                       { return nil }
