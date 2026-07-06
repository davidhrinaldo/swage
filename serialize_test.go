package swage_test

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/davidrinaldo/swage"
)

func TestSnapshotWriteTo(t *testing.T) {
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		snap       swage.Snapshot
		wantLines  int // total lines (header + samples)
		checkOrder func(t *testing.T, output string)
	}{
		{
			name: "single series",
			snap: swage.Snapshot{
				From: t0,
				To:   t0.Add(2 * time.Second),
				Series: map[string][]swage.Point{
					"cpu": {
						{T: t0, V: 1.5},
						{T: t0.Add(time.Second), V: 2.5},
					},
				},
			},
			wantLines: 3,
		},
		{
			name: "multiple series sorted by name",
			snap: swage.Snapshot{
				From: t0,
				To:   t0.Add(time.Second),
				Series: map[string][]swage.Point{
					"mem":  {{T: t0, V: 42.0}},
					"cpu":  {{T: t0, V: 1.0}},
					"disk": {{T: t0, V: 100.0}},
				},
			},
			wantLines: 4,
			checkOrder: func(t *testing.T, output string) {
				t.Helper()
				lines := strings.Split(strings.TrimSpace(output), "\n")
				if len(lines) < 4 {
					t.Fatalf("expected 4 lines, got %d", len(lines))
				}
				// Sample lines should be cpu, disk, mem (sorted).
				var names []string
				for _, line := range lines[1:] {
					var sl struct{ S string }
					if err := json.Unmarshal([]byte(line), &sl); err != nil {
						t.Fatalf("parse sample: %v", err)
					}
					names = append(names, sl.S)
				}
				want := []string{"cpu", "disk", "mem"}
				for i, n := range names {
					if n != want[i] {
						t.Errorf("series[%d] = %q, want %q", i, n, want[i])
					}
				}
			},
		},
		{
			name: "timestamps ascending within series",
			snap: swage.Snapshot{
				From: t0,
				To:   t0.Add(3 * time.Second),
				Series: map[string][]swage.Point{
					"cpu": {
						{T: t0, V: 1.0},
						{T: t0.Add(time.Second), V: 2.0},
						{T: t0.Add(2 * time.Second), V: 3.0},
					},
				},
			},
			wantLines: 4,
			checkOrder: func(t *testing.T, output string) {
				t.Helper()
				lines := strings.Split(strings.TrimSpace(output), "\n")
				var prevT int64
				for _, line := range lines[1:] {
					var sl struct{ T int64 }
					if err := json.Unmarshal([]byte(line), &sl); err != nil {
						t.Fatalf("parse sample: %v", err)
					}
					if sl.T < prevT {
						t.Errorf("timestamps not ascending: %d after %d", sl.T, prevT)
					}
					prevT = sl.T
				}
			},
		},
		{
			name: "empty snapshot no series",
			snap: swage.Snapshot{
				From:   t0,
				To:     t0.Add(time.Hour),
				Series: map[string][]swage.Point{},
			},
			wantLines: 1,
		},
		{
			name: "nil series map",
			snap: swage.Snapshot{
				From:   t0,
				To:     t0.Add(time.Hour),
				Series: nil,
			},
			wantLines: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tt.snap.WriteTo(&buf)
			if err != nil {
				t.Fatalf("WriteTo() error = %v", err)
			}
			if n != int64(buf.Len()) {
				t.Errorf("WriteTo() returned %d bytes, but %d were written", n, buf.Len())
			}

			lines := countNonEmptyLines(buf.String())
			if lines != tt.wantLines {
				t.Errorf("line count = %d, want %d\noutput:\n%s", lines, tt.wantLines, buf.String())
			}

			if tt.checkOrder != nil {
				tt.checkOrder(t, buf.String())
			}
		})
	}
}

func TestSnapshotWriteToHeader(t *testing.T) {
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)
	t1 := time.Date(2025, 7, 6, 3, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		snap       swage.Snapshot
		wantSwage  int
		wantFrom   string
		wantTo     string
		wantSeries []string
	}{
		{
			name: "header fields",
			snap: swage.Snapshot{
				From: t0,
				To:   t1,
				Series: map[string][]swage.Point{
					"cpu": {{T: t0, V: 1.0}},
					"mem": {{T: t0, V: 42.0}},
				},
			},
			wantSwage:  1,
			wantFrom:   "2025-07-06T02:00:00Z",
			wantTo:     "2025-07-06T03:00:00Z",
			wantSeries: []string{"cpu", "mem"},
		},
		{
			name: "empty series list in header",
			snap: swage.Snapshot{
				From:   t0,
				To:     t1,
				Series: map[string][]swage.Point{},
			},
			wantSwage:  1,
			wantFrom:   "2025-07-06T02:00:00Z",
			wantTo:     "2025-07-06T03:00:00Z",
			wantSeries: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if _, err := tt.snap.WriteTo(&buf); err != nil {
				t.Fatalf("WriteTo() error = %v", err)
			}

			// Parse header (first line).
			line := strings.SplitN(buf.String(), "\n", 2)[0]
			var hdr struct {
				Swage  int      `json:"swage"`
				From   string   `json:"from"`
				To     string   `json:"to"`
				Series []string `json:"series"`
			}
			if err := json.Unmarshal([]byte(line), &hdr); err != nil {
				t.Fatalf("parse header: %v", err)
			}

			if hdr.Swage != tt.wantSwage {
				t.Errorf("swage = %d, want %d", hdr.Swage, tt.wantSwage)
			}
			if hdr.From != tt.wantFrom {
				t.Errorf("from = %q, want %q", hdr.From, tt.wantFrom)
			}
			if hdr.To != tt.wantTo {
				t.Errorf("to = %q, want %q", hdr.To, tt.wantTo)
			}
			if len(hdr.Series) != len(tt.wantSeries) {
				t.Fatalf("series count = %d, want %d", len(hdr.Series), len(tt.wantSeries))
			}
			for i, s := range hdr.Series {
				if s != tt.wantSeries[i] {
					t.Errorf("series[%d] = %q, want %q", i, s, tt.wantSeries[i])
				}
			}
		})
	}
}

func TestReadSnapshot(t *testing.T) {
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)
	t1 := time.Date(2025, 7, 6, 3, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		input      string
		wantFrom   time.Time
		wantTo     time.Time
		wantSeries map[string][]swage.Point
	}{
		{
			name: "single series",
			input: `{"swage":1,"from":"2025-07-06T02:00:00Z","to":"2025-07-06T03:00:00Z","series":["cpu"]}
{"s":"cpu","t":1720231200000,"v":1.5}
{"s":"cpu","t":1720231201000,"v":2.5}
`,
			wantFrom: t0,
			wantTo:   t1,
			wantSeries: map[string][]swage.Point{
				"cpu": {
					{T: time.UnixMilli(1720231200000), V: 1.5},
					{T: time.UnixMilli(1720231201000), V: 2.5},
				},
			},
		},
		{
			name: "multiple series",
			input: `{"swage":1,"from":"2025-07-06T02:00:00Z","to":"2025-07-06T03:00:00Z","series":["cpu","mem"]}
{"s":"cpu","t":1720231200000,"v":1.0}
{"s":"cpu","t":1720231201000,"v":2.0}
{"s":"mem","t":1720231200000,"v":42.0}
`,
			wantFrom: t0,
			wantTo:   t1,
			wantSeries: map[string][]swage.Point{
				"cpu": {
					{T: time.UnixMilli(1720231200000), V: 1.0},
					{T: time.UnixMilli(1720231201000), V: 2.0},
				},
				"mem": {
					{T: time.UnixMilli(1720231200000), V: 42.0},
				},
			},
		},
		{
			name: "empty snapshot no samples",
			input: `{"swage":1,"from":"2025-07-06T02:00:00Z","to":"2025-07-06T03:00:00Z","series":[]}
`,
			wantFrom:   t0,
			wantTo:     t1,
			wantSeries: map[string][]swage.Point{},
		},
		{
			name: "populates series from samples not header",
			input: `{"swage":1,"from":"2025-07-06T02:00:00Z","to":"2025-07-06T03:00:00Z","series":["cpu"]}
{"s":"mem","t":1720231200000,"v":42.0}
`,
			wantFrom: t0,
			wantTo:   t1,
			wantSeries: map[string][]swage.Point{
				"mem": {
					{T: time.UnixMilli(1720231200000), V: 42.0},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap, err := swage.ReadSnapshot(strings.NewReader(tt.input))
			if err != nil {
				t.Fatalf("ReadSnapshot() error = %v", err)
			}

			if !snap.From.Equal(tt.wantFrom) {
				t.Errorf("From = %v, want %v", snap.From, tt.wantFrom)
			}
			if !snap.To.Equal(tt.wantTo) {
				t.Errorf("To = %v, want %v", snap.To, tt.wantTo)
			}
			if len(snap.Series) != len(tt.wantSeries) {
				t.Fatalf("series count = %d, want %d", len(snap.Series), len(tt.wantSeries))
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

func TestReadSnapshotErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "empty input",
			input: "",
		},
		{
			name:  "unknown format version",
			input: `{"swage":2,"from":"2025-07-06T02:00:00Z","to":"2025-07-06T03:00:00Z","series":[]}`,
		},
		{
			name:  "version zero",
			input: `{"swage":0,"from":"2025-07-06T02:00:00Z","to":"2025-07-06T03:00:00Z","series":[]}`,
		},
		{
			name:  "malformed header JSON",
			input: `{not json}`,
		},
		{
			name:  "malformed header missing from",
			input: `{"swage":1,"from":"not-a-date","to":"2025-07-06T03:00:00Z","series":[]}`,
		},
		{
			name:  "malformed header missing to",
			input: `{"swage":1,"from":"2025-07-06T02:00:00Z","to":"not-a-date","series":[]}`,
		},
		{
			name: "malformed sample line",
			input: `{"swage":1,"from":"2025-07-06T02:00:00Z","to":"2025-07-06T03:00:00Z","series":["cpu"]}
{not json}`,
		},
		{
			name: "sample line missing series name",
			input: `{"swage":1,"from":"2025-07-06T02:00:00Z","to":"2025-07-06T03:00:00Z","series":["cpu"]}
{"s":"","t":1720231200000,"v":1.0}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := swage.ReadSnapshot(strings.NewReader(tt.input))
			if err == nil {
				t.Fatalf("ReadSnapshot() expected error, got nil")
			}
		})
	}
}

func TestReadSnapshotSampleLimitExceeded(t *testing.T) {
	tests := []struct {
		name  string
		lines int // number of sample lines to produce
		limit []int
	}{
		{
			name:  "default limit exceeded",
			lines: swage.DefaultMaxReadSamples + 1,
		},
		{
			name:  "custom limit exceeded",
			lines: 101,
			limit: []int{100},
		},
		{
			name:  "custom limit of 1",
			lines: 2,
			limit: []int{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repeatingLineReader{
				header:    []byte(`{"swage":1,"from":"2025-07-06T02:00:00Z","to":"2025-07-06T03:00:00Z","series":["cpu"]}` + "\n"),
				line:      []byte(`{"s":"cpu","t":1720231200000,"v":1.0}` + "\n"),
				remaining: tt.lines,
			}

			_, err := swage.ReadSnapshot(r, tt.limit...)
			if err == nil {
				t.Fatal("ReadSnapshot() expected error, got nil")
			}
			if !strings.Contains(err.Error(), "exceeds limit") {
				t.Errorf("error = %q, want it to mention exceeds limit", err)
			}
		})
	}
}

func TestReadSnapshotSampleLimitNotExceeded(t *testing.T) {
	r := &repeatingLineReader{
		header:    []byte(`{"swage":1,"from":"2025-07-06T02:00:00Z","to":"2025-07-06T03:00:00Z","series":["cpu"]}` + "\n"),
		line:      []byte(`{"s":"cpu","t":1720231200000,"v":1.0}` + "\n"),
		remaining: 100,
	}

	_, err := swage.ReadSnapshot(r, 100)
	if err != nil {
		t.Fatalf("ReadSnapshot() unexpected error: %v", err)
	}
}

// repeatingLineReader emits a header line followed by a sample line repeated
// remaining times. Used to test ReadSnapshot's sample cap without allocating
// millions of objects.
type repeatingLineReader struct {
	header    []byte
	line      []byte
	remaining int
	buf       []byte // partial line not yet consumed
}

func (r *repeatingLineReader) Read(p []byte) (int, error) {
	if len(r.buf) == 0 {
		if len(r.header) > 0 {
			r.buf = r.header
			r.header = nil
		} else if r.remaining > 0 {
			r.buf = r.line
			r.remaining--
		} else {
			return 0, io.EOF
		}
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func TestWriteToReadSnapshotRoundTrip(t *testing.T) {
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)
	t1 := time.Date(2025, 7, 6, 3, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		snap swage.Snapshot
	}{
		{
			name: "single series",
			snap: swage.Snapshot{
				From: t0,
				To:   t1,
				Series: map[string][]swage.Point{
					"cpu": {
						{T: t0, V: 1.5},
						{T: t0.Add(time.Second), V: 2.5},
						{T: t0.Add(2 * time.Second), V: 3.5},
					},
				},
			},
		},
		{
			name: "multiple series",
			snap: swage.Snapshot{
				From: t0,
				To:   t1,
				Series: map[string][]swage.Point{
					"cpu":  {{T: t0, V: 1.0}, {T: t0.Add(time.Second), V: 2.0}},
					"mem":  {{T: t0, V: 42.0}},
					"disk": {{T: t0, V: 100.0}, {T: t0.Add(time.Second), V: 99.0}},
				},
			},
		},
		{
			name: "empty snapshot",
			snap: swage.Snapshot{
				From:   t0,
				To:     t1,
				Series: map[string][]swage.Point{},
			},
		},
		{
			name: "fractional values",
			snap: swage.Snapshot{
				From: t0,
				To:   t1,
				Series: map[string][]swage.Point{
					"latency": {{T: t0, V: 0.123456789}},
				},
			},
		},
		{
			name: "zero value",
			snap: swage.Snapshot{
				From: t0,
				To:   t1,
				Series: map[string][]swage.Point{
					"errors": {{T: t0, V: 0}},
				},
			},
		},
		{
			name: "negative values",
			snap: swage.Snapshot{
				From: t0,
				To:   t1,
				Series: map[string][]swage.Point{
					"temp": {{T: t0, V: -40.5}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tt.snap.WriteTo(&buf)
			if err != nil {
				t.Fatalf("WriteTo() error = %v", err)
			}
			if n != int64(buf.Len()) {
				t.Errorf("WriteTo() returned %d, buffer has %d bytes", n, buf.Len())
			}

			got, err := swage.ReadSnapshot(&buf)
			if err != nil {
				t.Fatalf("ReadSnapshot() error = %v", err)
			}

			if !got.From.Equal(tt.snap.From) {
				t.Errorf("From = %v, want %v", got.From, tt.snap.From)
			}
			if !got.To.Equal(tt.snap.To) {
				t.Errorf("To = %v, want %v", got.To, tt.snap.To)
			}
			if len(got.Series) != len(tt.snap.Series) {
				t.Fatalf("series count = %d, want %d", len(got.Series), len(tt.snap.Series))
			}
			for name, wantPoints := range tt.snap.Series {
				gotPoints, ok := got.Series[name]
				if !ok {
					t.Errorf("missing series %q", name)
					continue
				}
				if len(gotPoints) != len(wantPoints) {
					t.Errorf("series %q: %d points, want %d", name, len(gotPoints), len(wantPoints))
					continue
				}
				for i, want := range wantPoints {
					if !gotPoints[i].T.Equal(want.T) {
						t.Errorf("series %q point[%d].T = %v, want %v",
							name, i, gotPoints[i].T, want.T)
					}
					if gotPoints[i].V != want.V {
						t.Errorf("series %q point[%d].V = %v, want %v",
							name, i, gotPoints[i].V, want.V)
					}
				}
			}
		})
	}
}

func TestSnapshotWriteToByteCount(t *testing.T) {
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		snap swage.Snapshot
	}{
		{
			name: "with samples",
			snap: swage.Snapshot{
				From: t0,
				To:   t0.Add(time.Hour),
				Series: map[string][]swage.Point{
					"cpu": {{T: t0, V: 1.5}, {T: t0.Add(time.Second), V: 2.5}},
					"mem": {{T: t0, V: 42.0}},
				},
			},
		},
		{
			name: "empty",
			snap: swage.Snapshot{
				From:   t0,
				To:     t0.Add(time.Hour),
				Series: map[string][]swage.Point{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tt.snap.WriteTo(&buf)
			if err != nil {
				t.Fatalf("WriteTo() error = %v", err)
			}
			if n != int64(buf.Len()) {
				t.Errorf("returned byte count %d != actual bytes written %d", n, buf.Len())
			}
		})
	}
}

func TestSnapshotWriteToValidNDJSON(t *testing.T) {
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)

	snap := swage.Snapshot{
		From: t0,
		To:   t0.Add(time.Hour),
		Series: map[string][]swage.Point{
			"cpu": {{T: t0, V: 1.5}, {T: t0.Add(time.Second), V: 2.5}},
		},
	}

	var buf bytes.Buffer
	if _, err := snap.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo() error = %v", err)
	}

	// Every non-empty line must be valid JSON.
	for i, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if len(line) == 0 {
			continue
		}
		if !json.Valid([]byte(line)) {
			t.Errorf("line %d is not valid JSON: %s", i, line)
		}
	}
}

// countNonEmptyLines counts non-empty lines in s.
func countNonEmptyLines(s string) int {
	n := 0
	for _, line := range strings.Split(s, "\n") {
		if len(strings.TrimSpace(line)) > 0 {
			n++
		}
	}
	return n
}
