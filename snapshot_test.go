package swage_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/davidrinaldo/swage"
	"github.com/davidrinaldo/swage/memstore"
)

func TestRecorderSnapshot(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		samples    []swage.Sample
		from, to   time.Time
		filter     []string
		wantSeries map[string][]swage.Point
	}{
		{
			name: "single series",
			samples: []swage.Sample{
				{Name: "cpu", T: t0.UnixMilli(), V: 1.0},
				{Name: "cpu", T: t0.Add(time.Second).UnixMilli(), V: 2.0},
			},
			from:   t0,
			to:     t0.Add(2 * time.Second),
			filter: nil,
			wantSeries: map[string][]swage.Point{
				"cpu": {
					{T: t0, V: 1.0},
					{T: t0.Add(time.Second), V: 2.0},
				},
			},
		},
		{
			name: "multiple series",
			samples: []swage.Sample{
				{Name: "cpu", T: t0.UnixMilli(), V: 1.0},
				{Name: "mem", T: t0.UnixMilli(), V: 42.0},
				{Name: "cpu", T: t0.Add(time.Second).UnixMilli(), V: 2.0},
				{Name: "mem", T: t0.Add(time.Second).UnixMilli(), V: 43.0},
			},
			from:   t0,
			to:     t0.Add(2 * time.Second),
			filter: nil,
			wantSeries: map[string][]swage.Point{
				"cpu": {
					{T: t0, V: 1.0},
					{T: t0.Add(time.Second), V: 2.0},
				},
				"mem": {
					{T: t0, V: 42.0},
					{T: t0.Add(time.Second), V: 43.0},
				},
			},
		},
		{
			name: "name filter",
			samples: []swage.Sample{
				{Name: "cpu", T: t0.UnixMilli(), V: 1.0},
				{Name: "mem", T: t0.UnixMilli(), V: 42.0},
				{Name: "disk", T: t0.UnixMilli(), V: 100.0},
			},
			from:   t0,
			to:     t0.Add(time.Second),
			filter: []string{"cpu", "disk"},
			wantSeries: map[string][]swage.Point{
				"cpu":  {{T: t0, V: 1.0}},
				"disk": {{T: t0, V: 100.0}},
			},
		},
		{
			name: "no matching data in time range",
			samples: []swage.Sample{
				{Name: "cpu", T: t0.UnixMilli(), V: 1.0},
			},
			from:       t0.Add(10 * time.Second),
			to:         t0.Add(20 * time.Second),
			filter:     nil,
			wantSeries: map[string][]swage.Point{},
		},
		{
			name: "filter for nonexistent series",
			samples: []swage.Sample{
				{Name: "cpu", T: t0.UnixMilli(), V: 1.0},
			},
			from:       t0,
			to:         t0.Add(time.Second),
			filter:     []string{"nonexistent"},
			wantSeries: map[string][]swage.Point{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := memstore.New(0)
			defer store.Close()

			rec, err := swage.New(store, swage.Options{Horizon: time.Hour})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			defer rec.Close()

			for _, s := range tt.samples {
				rec.RecordAt(s.Name, time.UnixMilli(s.T), s.V)
			}

			snap, err := rec.Snapshot(tt.from, tt.to, tt.filter...)
			if err != nil {
				t.Fatalf("Snapshot() error = %v", err)
			}

			if snap.From != tt.from {
				t.Errorf("From = %v, want %v", snap.From, tt.from)
			}
			if snap.To != tt.to {
				t.Errorf("To = %v, want %v", snap.To, tt.to)
			}

			if len(snap.Series) != len(tt.wantSeries) {
				t.Fatalf("Series count = %d, want %d", len(snap.Series), len(tt.wantSeries))
			}

			for name, wantPoints := range tt.wantSeries {
				gotPoints, ok := snap.Series[name]
				if !ok {
					t.Errorf("missing series %q", name)
					continue
				}
				if len(gotPoints) != len(wantPoints) {
					t.Errorf("series %q: %d points, want %d", name, len(gotPoints), len(wantPoints))
					continue
				}
				for i, want := range wantPoints {
					if !gotPoints[i].T.Equal(want.T) || gotPoints[i].V != want.V {
						t.Errorf("series %q point[%d] = {%v, %v}, want {%v, %v}",
							name, i, gotPoints[i].T, gotPoints[i].V, want.T, want.V)
					}
				}
			}
		})
	}
}

func TestRecorderSnapshotStoreErrors(t *testing.T) {
	tests := []struct {
		name   string
		store  swage.Store
		filter []string
	}{
		{
			name:   "query error with filter",
			store:  &queryErrorStore{queryErr: errors.New("disk read failed")},
			filter: []string{"cpu"},
		},
		{
			name:   "series listing error without filter",
			store:  &queryErrorStore{seriesErr: errors.New("corrupt index")},
			filter: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, err := swage.New(tt.store, swage.Options{Horizon: time.Hour})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			defer rec.Close()

			_, err = rec.Snapshot(time.Now(), time.Now().Add(time.Hour), tt.filter...)
			if err == nil {
				t.Fatalf("Snapshot() expected error, got nil")
			}
		})
	}
}

func TestRecorderSummary(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		values     []float64     // recorded at 1s intervals starting at t0
		from, to   time.Time     // query range
		window     time.Duration // bucket window
		filter     []string
		wantCount  int // expected number of buckets
		checkFirst func(t *testing.T, b swage.Bucket)
		checkLast  func(t *testing.T, b swage.Bucket)
	}{
		{
			name:      "basic aggregates",
			values:    []float64{10.0, 30.0, 20.0, 50.0, 40.0},
			from:      t0,
			to:        t0.Add(10 * time.Second),
			window:    10 * time.Second,
			wantCount: 1,
			checkFirst: func(t *testing.T, b swage.Bucket) {
				t.Helper()
				if b.Count != 5 {
					t.Errorf("Count = %d, want 5", b.Count)
				}
				if b.Min != 10.0 {
					t.Errorf("Min = %v, want 10.0", b.Min)
				}
				if b.Max != 50.0 {
					t.Errorf("Max = %v, want 50.0", b.Max)
				}
				if b.Sum != 150.0 {
					t.Errorf("Sum = %v, want 150.0", b.Sum)
				}
				if b.Mean != 30.0 {
					t.Errorf("Mean = %v, want 30.0", b.Mean)
				}
			},
		},
		{
			name:      "percentiles over 100 values",
			values:    seq(1, 100),
			from:      t0,
			to:        t0.Add(time.Second),
			window:    time.Second,
			wantCount: 1,
			checkFirst: func(t *testing.T, b swage.Bucket) {
				t.Helper()
				if math.Abs(b.P50-50.5) > 0.5 {
					t.Errorf("P50 = %v, want ~50.5", b.P50)
				}
				if math.Abs(b.P95-95.05) > 0.5 {
					t.Errorf("P95 = %v, want ~95.05", b.P95)
				}
				if math.Abs(b.P99-99.01) > 0.5 {
					t.Errorf("P99 = %v, want ~99.01", b.P99)
				}
			},
		},
		{
			name:      "rate increasing counter",
			values:    []float64{0, 10, 20, 30},
			from:      t0,
			to:        t0.Add(10 * time.Second),
			window:    10 * time.Second,
			wantCount: 1,
			checkFirst: func(t *testing.T, b swage.Bucket) {
				t.Helper()
				// Rate = (30 - 0) / 10s = 3.0
				if math.Abs(b.Rate-3.0) > 0.001 {
					t.Errorf("Rate = %v, want 3.0", b.Rate)
				}
			},
		},
		{
			name:      "rate decreasing gauge",
			values:    []float64{100, 80, 60},
			from:      t0,
			to:        t0.Add(10 * time.Second),
			window:    10 * time.Second,
			wantCount: 1,
			checkFirst: func(t *testing.T, b swage.Bucket) {
				t.Helper()
				// Rate = (60 - 100) / 10s = -4.0
				if math.Abs(b.Rate-(-4.0)) > 0.001 {
					t.Errorf("Rate = %v, want -4.0", b.Rate)
				}
			},
		},
		{
			name:      "rate single sample is zero",
			values:    []float64{42},
			from:      t0,
			to:        t0.Add(10 * time.Second),
			window:    10 * time.Second,
			wantCount: 1,
			checkFirst: func(t *testing.T, b swage.Bucket) {
				t.Helper()
				if b.Rate != 0 {
					t.Errorf("Rate = %v, want 0 (single sample)", b.Rate)
				}
			},
		},
		{
			name:      "short last bucket uses actual duration for rate",
			values:    seq(0, 6), // 0,1,2,3,4,5,6 at 1s intervals
			from:      t0,
			to:        t0.Add(7 * time.Second),
			window:    5 * time.Second,
			wantCount: 2,
			checkFirst: func(t *testing.T, b swage.Bucket) {
				t.Helper()
				// First bucket [t0, t0+5s): values 0,1,2,3,4.
				if b.Count != 5 {
					t.Errorf("bucket[0].Count = %d, want 5", b.Count)
				}
				if !b.Start.Equal(t0) {
					t.Errorf("bucket[0].Start = %v, want %v", b.Start, t0)
				}
				if !b.End.Equal(t0.Add(5 * time.Second)) {
					t.Errorf("bucket[0].End = %v, want %v", b.End, t0.Add(5*time.Second))
				}
			},
			checkLast: func(t *testing.T, b swage.Bucket) {
				t.Helper()
				// Second bucket [t0+5s, t0+7s]: values 5,6. Duration 2s.
				if b.Count != 2 {
					t.Errorf("bucket[1].Count = %d, want 2", b.Count)
				}
				if !b.Start.Equal(t0.Add(5 * time.Second)) {
					t.Errorf("bucket[1].Start = %v, want %v", b.Start, t0.Add(5*time.Second))
				}
				if !b.End.Equal(t0.Add(7 * time.Second)) {
					t.Errorf("bucket[1].End = %v, want %v", b.End, t0.Add(7*time.Second))
				}
				// Rate = (6 - 5) / 2s = 0.5
				if math.Abs(b.Rate-0.5) > 0.001 {
					t.Errorf("bucket[1].Rate = %v, want 0.5", b.Rate)
				}
			},
		},
		{
			name:      "empty buckets are omitted",
			values:    []float64{1.0, -1, -1, 7.0}, // -1 is a sentinel; see below
			from:      t0,
			to:        t0.Add(8 * time.Second),
			window:    2 * time.Second,
			wantCount: 2,
			checkFirst: func(t *testing.T, b swage.Bucket) {
				t.Helper()
				if !b.Start.Equal(t0) {
					t.Errorf("bucket[0].Start = %v, want %v", b.Start, t0)
				}
			},
			checkLast: func(t *testing.T, b swage.Bucket) {
				t.Helper()
				if !b.Start.Equal(t0.Add(6 * time.Second)) {
					t.Errorf("bucket[1].Start = %v, want %v", b.Start, t0.Add(6*time.Second))
				}
			},
		},
		{
			name:      "filter for nonexistent series returns empty",
			values:    []float64{1.0},
			from:      t0,
			to:        t0.Add(time.Second),
			window:    time.Second,
			filter:    []string{"nonexistent"},
			wantCount: -1, // means expect no series in result
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := memstore.New(0)
			defer store.Close()

			rec, err := swage.New(store, swage.Options{Horizon: time.Hour})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			defer rec.Close()

			// Special handling for cases that need non-uniform spacing.
			switch tt.name {
			case "empty buckets are omitted":
				rec.RecordAt("cpu", t0, 1.0)
				rec.RecordAt("cpu", t0.Add(6*time.Second), 7.0)
			case "percentiles over 100 values":
				for i, v := range tt.values {
					rec.RecordAt("cpu", t0.Add(time.Duration(i)*time.Millisecond), v)
				}
			default:
				for i, v := range tt.values {
					rec.RecordAt("cpu", t0.Add(time.Duration(i)*time.Second), v)
				}
			}

			sum, err := rec.Summary(tt.from, tt.to, tt.window, tt.filter...)
			if err != nil {
				t.Fatalf("Summary() error = %v", err)
			}

			if tt.wantCount == -1 {
				if len(sum.Series) != 0 {
					t.Errorf("expected empty series map, got %d entries", len(sum.Series))
				}
				return
			}

			buckets := sum.Series["cpu"]
			if len(buckets) != tt.wantCount {
				t.Fatalf("bucket count = %d, want %d", len(buckets), tt.wantCount)
			}

			if tt.checkFirst != nil {
				tt.checkFirst(t, buckets[0])
			}
			if tt.checkLast != nil && len(buckets) > 1 {
				tt.checkLast(t, buckets[len(buckets)-1])
			}
		})
	}
}

func TestRecorderSummaryStoreErrors(t *testing.T) {
	tests := []struct {
		name   string
		store  swage.Store
		filter []string
	}{
		{
			name:   "query error with filter",
			store:  &queryErrorStore{queryErr: errors.New("disk read failed")},
			filter: []string{"cpu"},
		},
		{
			name:   "series listing error without filter",
			store:  &queryErrorStore{seriesErr: errors.New("corrupt index")},
			filter: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, err := swage.New(tt.store, swage.Options{Horizon: time.Hour})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			defer rec.Close()

			_, err = rec.Summary(time.Now(), time.Now().Add(time.Hour), time.Minute, tt.filter...)
			if err == nil {
				t.Fatalf("Summary() expected error, got nil")
			}
		})
	}
}

func TestSnapshotSummary(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		snap       swage.Snapshot
		window     time.Duration
		wantCount  int // number of series in result
		checkFirst func(t *testing.T, b swage.Bucket)
	}{
		{
			name: "basic aggregation",
			snap: swage.Snapshot{
				From: t0,
				To:   t0.Add(10 * time.Second),
				Series: map[string][]swage.Point{
					"cpu": {
						{T: t0, V: 10.0},
						{T: t0.Add(time.Second), V: 20.0},
						{T: t0.Add(2 * time.Second), V: 30.0},
					},
				},
			},
			window:    10 * time.Second,
			wantCount: 1,
			checkFirst: func(t *testing.T, b swage.Bucket) {
				t.Helper()
				if b.Count != 3 {
					t.Errorf("Count = %d, want 3", b.Count)
				}
				if b.Min != 10.0 {
					t.Errorf("Min = %v, want 10.0", b.Min)
				}
				if b.Max != 30.0 {
					t.Errorf("Max = %v, want 30.0", b.Max)
				}
				if b.Sum != 60.0 {
					t.Errorf("Sum = %v, want 60.0", b.Sum)
				}
				if b.Mean != 20.0 {
					t.Errorf("Mean = %v, want 20.0", b.Mean)
				}
			},
		},
		{
			name: "empty series map",
			snap: swage.Snapshot{
				From:   t0,
				To:     t0.Add(10 * time.Second),
				Series: map[string][]swage.Point{},
			},
			window:    5 * time.Second,
			wantCount: 0,
		},
		{
			name: "series with no points in range",
			snap: swage.Snapshot{
				From:   t0,
				To:     t0.Add(10 * time.Second),
				Series: map[string][]swage.Point{"cpu": {}},
			},
			window:    5 * time.Second,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sum := tt.snap.Summary(tt.window)

			if sum.From != tt.snap.From {
				t.Errorf("From = %v, want %v", sum.From, tt.snap.From)
			}
			if sum.To != tt.snap.To {
				t.Errorf("To = %v, want %v", sum.To, tt.snap.To)
			}
			if sum.Window != tt.window {
				t.Errorf("Window = %v, want %v", sum.Window, tt.window)
			}
			if len(sum.Series) != tt.wantCount {
				t.Fatalf("series count = %d, want %d", len(sum.Series), tt.wantCount)
			}

			if tt.checkFirst != nil {
				buckets := sum.Series["cpu"]
				if len(buckets) == 0 {
					t.Fatalf("expected buckets for cpu")
				}
				tt.checkFirst(t, buckets[0])
			}
		})
	}
}

func TestRecorderDumpTo(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		samples   []swage.Sample
		from, to  time.Time
		filter    []string
		wantLines int // total lines including header
	}{
		{
			name: "format and ordering",
			samples: []swage.Sample{
				{Name: "cpu", T: t0.UnixMilli(), V: 1.0},
				{Name: "mem", T: t0.UnixMilli(), V: 42.0},
				{Name: "cpu", T: t0.Add(time.Second).UnixMilli(), V: 2.0},
				{Name: "mem", T: t0.Add(time.Second).UnixMilli(), V: 43.0},
			},
			from:      t0,
			to:        t0.Add(2 * time.Second),
			filter:    nil,
			wantLines: 5, // header + 4 samples
		},
		{
			name: "with name filter",
			samples: []swage.Sample{
				{Name: "cpu", T: t0.UnixMilli(), V: 1.0},
				{Name: "mem", T: t0.UnixMilli(), V: 42.0},
			},
			from:      t0,
			to:        t0.Add(time.Second),
			filter:    []string{"cpu"},
			wantLines: 2, // header + 1 sample
		},
		{
			name:      "empty range",
			samples:   nil,
			from:      t0,
			to:        t0.Add(time.Second),
			filter:    nil,
			wantLines: 1, // header only
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := memstore.New(0)
			defer store.Close()

			rec, err := swage.New(store, swage.Options{Horizon: time.Hour})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			defer rec.Close()

			for _, s := range tt.samples {
				rec.RecordAt(s.Name, time.UnixMilli(s.T), s.V)
			}

			var buf bytes.Buffer
			if err := rec.DumpTo(&buf, tt.from, tt.to, tt.filter...); err != nil {
				t.Fatalf("DumpTo() error = %v", err)
			}

			lines := countLines(buf.Bytes())
			if lines != tt.wantLines {
				t.Errorf("line count = %d, want %d\noutput:\n%s", lines, tt.wantLines, buf.String())
			}
		})
	}
}

func TestRecorderDumpToFormatDetail(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	store := memstore.New(0)
	defer store.Close()

	rec, err := swage.New(store, swage.Options{Horizon: time.Hour})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer rec.Close()

	rec.RecordAt("cpu", t0, 1.5)
	rec.RecordAt("mem", t0, 42.0)
	rec.RecordAt("cpu", t0.Add(time.Second), 2.5)

	var buf bytes.Buffer
	if err := rec.DumpTo(&buf, t0, t0.Add(2*time.Second)); err != nil {
		t.Fatalf("DumpTo() error = %v", err)
	}

	scanner := bufio.NewScanner(&buf)

	// Parse header.
	if !scanner.Scan() {
		t.Fatalf("expected header line")
	}
	var hdr struct {
		Swage  int      `json:"swage"`
		From   string   `json:"from"`
		To     string   `json:"to"`
		Series []string `json:"series"`
	}
	if err := json.Unmarshal(scanner.Bytes(), &hdr); err != nil {
		t.Fatalf("parse header: %v", err)
	}
	if hdr.Swage != 1 {
		t.Errorf("header swage = %d, want 1", hdr.Swage)
	}
	if hdr.From != t0.UTC().Format(time.RFC3339) {
		t.Errorf("header from = %q, want %q", hdr.From, t0.UTC().Format(time.RFC3339))
	}
	if len(hdr.Series) != 2 {
		t.Fatalf("header series count = %d, want 2", len(hdr.Series))
	}

	// Parse sample lines — should be grouped by name (sorted), ascending T.
	type sampleLine struct {
		S string  `json:"s"`
		T int64   `json:"t"`
		V float64 `json:"v"`
	}
	var samples []sampleLine
	for scanner.Scan() {
		var sl sampleLine
		if err := json.Unmarshal(scanner.Bytes(), &sl); err != nil {
			t.Fatalf("parse sample line: %v", err)
		}
		samples = append(samples, sl)
	}

	if len(samples) != 3 {
		t.Fatalf("expected 3 sample lines, got %d", len(samples))
	}

	// cpu before mem (sorted), timestamps ascending within each.
	if samples[0].S != "cpu" || samples[1].S != "cpu" || samples[2].S != "mem" {
		t.Errorf("series order = [%s, %s, %s], want [cpu, cpu, mem]",
			samples[0].S, samples[1].S, samples[2].S)
	}
	if samples[0].T >= samples[1].T {
		t.Errorf("cpu timestamps not ascending: %d >= %d", samples[0].T, samples[1].T)
	}
	if samples[0].V != 1.5 || samples[1].V != 2.5 || samples[2].V != 42.0 {
		t.Errorf("values = [%v, %v, %v], want [1.5, 2.5, 42.0]",
			samples[0].V, samples[1].V, samples[2].V)
	}
}

func TestRecorderDumpToStoreErrors(t *testing.T) {
	tests := []struct {
		name   string
		store  swage.Store
		filter []string
	}{
		{
			name:   "query error with filter",
			store:  &queryErrorStore{queryErr: errors.New("disk read failed")},
			filter: []string{"cpu"},
		},
		{
			name:   "series listing error without filter",
			store:  &queryErrorStore{seriesErr: errors.New("corrupt index")},
			filter: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, err := swage.New(tt.store, swage.Options{Horizon: time.Hour})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			defer rec.Close()

			var buf bytes.Buffer
			err = rec.DumpTo(&buf, time.Now(), time.Now().Add(time.Hour), tt.filter...)
			if err == nil {
				t.Fatalf("DumpTo() expected error, got nil")
			}
		})
	}
}

func TestConcurrentQueryDuringRecording(t *testing.T) {
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

	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Pre-load some data.
	for i := 0; i < 100; i++ {
		rec.RecordAt("cpu", t0.Add(time.Duration(i)*time.Millisecond), float64(i))
	}
	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	var wg sync.WaitGroup

	// Writer goroutines keep recording.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				rec.RecordAt("cpu",
					t0.Add(time.Duration(100+id*200+i)*time.Millisecond),
					float64(100+i))
			}
		}(g)
	}

	// Reader goroutines query concurrently.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				snap, err := rec.Snapshot(t0, t0.Add(time.Hour))
				if err != nil {
					t.Errorf("Snapshot() error = %v", err)
					return
				}
				if snap == nil {
					t.Errorf("Snapshot() returned nil")
					return
				}
			}
		}()
	}

	// Summary readers.
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				sum, err := rec.Summary(t0, t0.Add(time.Hour), time.Minute)
				if err != nil {
					t.Errorf("Summary() error = %v", err)
					return
				}
				if sum == nil {
					t.Errorf("Summary() returned nil")
					return
				}
			}
		}()
	}

	// DumpTo readers.
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				var buf bytes.Buffer
				if err := rec.DumpTo(&buf, t0, t0.Add(time.Hour)); err != nil {
					t.Errorf("DumpTo() error = %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()
}

// queryErrorStore is a Store that returns configurable errors from Query and Series.
type queryErrorStore struct {
	queryErr  error
	seriesErr error
}

func (s *queryErrorStore) Append([]swage.Sample) error { return nil }

func (s *queryErrorStore) Query(string, int64, int64) ([]swage.Sample, error) {
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	return nil, nil
}

func (s *queryErrorStore) Series() ([]string, error) {
	if s.seriesErr != nil {
		return nil, s.seriesErr
	}
	return []string{"cpu"}, nil
}

func (s *queryErrorStore) Close() error { return nil }

// seq returns a slice of float64 from start to end inclusive.
func seq(start, end int) []float64 {
	out := make([]float64, 0, end-start+1)
	for i := start; i <= end; i++ {
		out = append(out, float64(i))
	}
	return out
}

// countLines counts non-empty lines in b.
func countLines(b []byte) int {
	scanner := bufio.NewScanner(bytes.NewReader(b))
	n := 0
	for scanner.Scan() {
		if len(scanner.Bytes()) > 0 {
			n++
		}
	}
	return n
}
