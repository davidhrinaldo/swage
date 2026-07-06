package memstore

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/davidrinaldo/swage"
)

func TestAppend(t *testing.T) {
	tests := []struct {
		name    string
		batches [][]swage.Sample
		query   string
		from    int64
		to      int64
		want    []swage.Sample
	}{
		{
			name: "single batch single series",
			batches: [][]swage.Sample{
				{
					{Name: "cpu", T: 1000, V: 1.0},
					{Name: "cpu", T: 2000, V: 2.0},
				},
			},
			query: "cpu",
			from:  0,
			to:    5000,
			want: []swage.Sample{
				{Name: "cpu", T: 1000, V: 1.0},
				{Name: "cpu", T: 2000, V: 2.0},
			},
		},
		{
			name: "multiple batches accumulate",
			batches: [][]swage.Sample{
				{{Name: "mem", T: 1000, V: 10.0}},
				{{Name: "mem", T: 2000, V: 20.0}},
			},
			query: "mem",
			from:  0,
			to:    5000,
			want: []swage.Sample{
				{Name: "mem", T: 1000, V: 10.0},
				{Name: "mem", T: 2000, V: 20.0},
			},
		},
		{
			name: "multiple series in one batch",
			batches: [][]swage.Sample{
				{
					{Name: "a", T: 1000, V: 1.0},
					{Name: "b", T: 1000, V: 2.0},
				},
			},
			query: "b",
			from:  0,
			to:    5000,
			want: []swage.Sample{
				{Name: "b", T: 1000, V: 2.0},
			},
		},
		{
			name:    "empty batch",
			batches: [][]swage.Sample{{}},
			query:   "x",
			from:    0,
			to:      5000,
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(0)
			defer s.Close()

			for _, batch := range tt.batches {
				if err := s.Append(batch); err != nil {
					t.Fatalf("Append() error = %v", err)
				}
			}

			got, err := s.Query(tt.query, tt.from, tt.to)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Query() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQuery(t *testing.T) {
	samples := []swage.Sample{
		{Name: "cpu", T: 1000, V: 1.0},
		{Name: "cpu", T: 2000, V: 2.0},
		{Name: "cpu", T: 3000, V: 3.0},
		{Name: "cpu", T: 4000, V: 4.0},
		{Name: "cpu", T: 5000, V: 5.0},
	}

	tests := []struct {
		name string
		from int64
		to   int64
		want []swage.Sample
	}{
		{
			name: "full range",
			from: 0,
			to:   10000,
			want: samples,
		},
		{
			name: "exact boundaries inclusive",
			from: 2000,
			to:   4000,
			want: []swage.Sample{
				{Name: "cpu", T: 2000, V: 2.0},
				{Name: "cpu", T: 3000, V: 3.0},
				{Name: "cpu", T: 4000, V: 4.0},
			},
		},
		{
			name: "no samples in range",
			from: 6000,
			to:   9000,
			want: nil,
		},
		{
			name: "single sample",
			from: 3000,
			to:   3000,
			want: []swage.Sample{
				{Name: "cpu", T: 3000, V: 3.0},
			},
		},
		{
			name: "range before all data",
			from: 0,
			to:   500,
			want: nil,
		},
		{
			name: "nonexistent series",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(0)
			defer s.Close()

			if err := s.Append(samples); err != nil {
				t.Fatalf("Append() error = %v", err)
			}

			queryName := "cpu"
			if tt.name == "nonexistent series" {
				queryName = "nonexistent"
				tt.from = 0
				tt.to = 10000
			}

			got, err := s.Query(queryName, tt.from, tt.to)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Query() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries(t *testing.T) {
	tests := []struct {
		name    string
		samples []swage.Sample
		want    []string
	}{
		{
			name:    "no series",
			samples: nil,
			want:    []string{},
		},
		{
			name: "single series",
			samples: []swage.Sample{
				{Name: "cpu", T: 1000, V: 1.0},
			},
			want: []string{"cpu"},
		},
		{
			name: "multiple series sorted",
			samples: []swage.Sample{
				{Name: "mem", T: 1000, V: 1.0},
				{Name: "cpu", T: 1000, V: 2.0},
				{Name: "disk", T: 1000, V: 3.0},
			},
			want: []string{"cpu", "disk", "mem"},
		},
		{
			name: "duplicate names collapsed",
			samples: []swage.Sample{
				{Name: "cpu", T: 1000, V: 1.0},
				{Name: "cpu", T: 2000, V: 2.0},
			},
			want: []string{"cpu"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(0)
			defer s.Close()

			if len(tt.samples) > 0 {
				if err := s.Append(tt.samples); err != nil {
					t.Fatalf("Append() error = %v", err)
				}
			}

			got, err := s.Series()
			if err != nil {
				t.Fatalf("Series() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRetention(t *testing.T) {
	// Use a controllable clock to avoid real-time waits.
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	mu := sync.Mutex{}
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advanceClock := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}

	horizon := 10 * time.Second
	s := NewWithClock(horizon, clock)
	defer s.Close()

	// Append samples at t=0s and t=5s.
	baseMs := now.UnixMilli()
	err := s.Append([]swage.Sample{
		{Name: "cpu", T: baseMs, V: 1.0},
		{Name: "cpu", T: baseMs + 5000, V: 2.0},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Advance clock past horizon for the first sample but not the second.
	advanceClock(12 * time.Second)

	// Manually trigger eviction instead of waiting for the ticker.
	s.evict()

	got, err := s.Query("cpu", 0, baseMs+20000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 sample after retention, got %d: %v", len(got), got)
	}
	if got[0].V != 2.0 {
		t.Errorf("expected surviving sample V=2.0, got V=%v", got[0].V)
	}

	// Advance past the second sample's horizon — all data should be evicted.
	advanceClock(10 * time.Second)
	s.evict()

	got, err = s.Query("cpu", 0, baseMs+30000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 samples after full eviction, got %d", len(got))
	}

	// Series should be removed entirely after eviction.
	names, err := s.Series()
	if err != nil {
		t.Fatalf("Series() error = %v", err)
	}
	if len(names) != 0 {
		t.Errorf("expected 0 series after full eviction, got %v", names)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	s := New(0)
	defer s.Close()

	const (
		numWriters = 4
		numReaders = 4
		numOps     = 500
	)

	var wg sync.WaitGroup

	// Writers append samples concurrently.
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				_ = s.Append([]swage.Sample{
					{Name: "series", T: int64(id*numOps + i), V: float64(i)},
				})
			}
		}(w)
	}

	// Readers query and list series concurrently.
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				_, _ = s.Query("series", 0, int64(numWriters*numOps))
				_, _ = s.Series()
			}
		}()
	}

	wg.Wait()

	// Verify all samples were stored.
	got, err := s.Query("series", 0, int64(numWriters*numOps))
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	want := numWriters * numOps
	if len(got) != want {
		t.Errorf("expected %d samples, got %d", want, len(got))
	}
}

func TestCloseIdempotent(t *testing.T) {
	s := New(time.Hour)
	// Closing multiple times must not panic.
	if err := s.Close(); err != nil {
		t.Errorf("first Close() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}
