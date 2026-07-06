package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/davidrinaldo/swage"
	"github.com/davidrinaldo/swage/ingotstore"
)

// testSnapshot builds a Snapshot with deterministic data for testing.
func testSnapshot(t *testing.T) *swage.Snapshot {
	t.Helper()
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)
	return &swage.Snapshot{
		From: t0,
		To:   t0.Add(time.Hour),
		Series: map[string][]swage.Point{
			"cpu": {
				{T: t0, V: 1.0},
				{T: t0.Add(time.Second), V: 2.0},
				{T: t0.Add(2 * time.Second), V: 3.0},
			},
			"mem": {
				{T: t0, V: 100.0},
				{T: t0.Add(time.Second), V: 200.0},
			},
			"disk": {
				{T: t0, V: 50.0},
			},
		},
	}
}

// writeSwageFile writes a Snapshot to a temporary .swage file and returns the path.
func writeSwageFile(t *testing.T, snap *swage.Snapshot) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.swage")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create swage file: %v", err)
	}
	if _, err := snap.WriteTo(f); err != nil {
		f.Close()
		t.Fatalf("write snapshot: %v", err)
	}
	f.Close()
	return path
}

func TestRunNoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(nil, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "usage") {
		t.Errorf("stderr = %q, want usage message", stderr.String())
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"bogus"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Errorf("stderr = %q, want unknown command message", stderr.String())
	}
}

func TestLs(t *testing.T) {
	snap := testSnapshot(t)
	path := writeSwageFile(t, snap)

	tests := []struct {
		name      string
		args      []string
		wantNames []string
		wantErr   bool
	}{
		{
			name:      "lists all series sorted",
			args:      []string{"ls", path},
			wantNames: []string{"cpu", "disk", "mem"},
		},
		{
			name:    "missing file",
			args:    []string{"ls", "/nonexistent/file.swage"},
			wantErr: true,
		},
		{
			name:    "no file argument",
			args:    []string{"ls"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)

			if tt.wantErr {
				if code == 0 {
					t.Error("expected non-zero exit code")
				}
				return
			}

			if code != 0 {
				t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
			}

			got := parseLines(stdout.String())
			if !reflect.DeepEqual(got, tt.wantNames) {
				t.Errorf("got %v, want %v", got, tt.wantNames)
			}
		})
	}
}

func TestLsEmptySnapshot(t *testing.T) {
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)
	snap := &swage.Snapshot{
		From:   t0,
		To:     t0.Add(time.Hour),
		Series: map[string][]swage.Point{},
	}
	path := writeSwageFile(t, snap)

	var stdout, stderr bytes.Buffer
	code := run([]string{"ls", path}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Errorf("expected empty output, got %q", stdout.String())
	}
}

func TestRead(t *testing.T) {
	snap := testSnapshot(t)
	path := writeSwageFile(t, snap)

	tests := []struct {
		name       string
		args       []string
		wantSeries []string // series names expected in output
		wantCount  int      // total sample lines
		wantErr    bool
	}{
		{
			name:       "all series",
			args:       []string{"read", path},
			wantSeries: []string{"cpu", "disk", "mem"},
			wantCount:  6,
		},
		{
			name:       "filter single series",
			args:       []string{"read", "--series", "cpu", path},
			wantSeries: []string{"cpu"},
			wantCount:  3,
		},
		{
			name:       "filter multiple series",
			args:       []string{"read", "--series", "cpu,disk", path},
			wantSeries: []string{"cpu", "disk"},
			wantCount:  4,
		},
		{
			name:       "filter nonexistent series",
			args:       []string{"read", "--series", "nonexistent", path},
			wantSeries: nil,
			wantCount:  0,
		},
		{
			name:    "missing file",
			args:    []string{"read", "/nonexistent/file.swage"},
			wantErr: true,
		},
		{
			name:    "no file argument",
			args:    []string{"read"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)

			if tt.wantErr {
				if code == 0 {
					t.Error("expected non-zero exit code")
				}
				return
			}

			if code != 0 {
				t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
			}

			lines := parseLines(stdout.String())
			if len(lines) != tt.wantCount {
				t.Fatalf("line count = %d, want %d\noutput:\n%s", len(lines), tt.wantCount, stdout.String())
			}

			// Verify each line is valid NDJSON with expected series.
			seriesSet := make(map[string]bool)
			for _, line := range lines {
				var dl dumpLine
				if err := json.Unmarshal([]byte(line), &dl); err != nil {
					t.Fatalf("invalid NDJSON: %v\nline: %s", err, line)
				}
				if dl.S == "" {
					t.Errorf("empty series name in line: %s", line)
				}
				seriesSet[dl.S] = true
			}

			var gotSeries []string
			for s := range seriesSet {
				gotSeries = append(gotSeries, s)
			}
			sort.Strings(gotSeries)

			if tt.wantSeries == nil {
				if len(gotSeries) != 0 {
					t.Errorf("expected no series, got %v", gotSeries)
				}
			} else if !reflect.DeepEqual(gotSeries, tt.wantSeries) {
				t.Errorf("series = %v, want %v", gotSeries, tt.wantSeries)
			}
		})
	}
}

func TestReadNDJSONFormat(t *testing.T) {
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)
	snap := &swage.Snapshot{
		From: t0,
		To:   t0.Add(time.Hour),
		Series: map[string][]swage.Point{
			"cpu": {
				{T: t0, V: 1.5},
				{T: t0.Add(time.Second), V: 2.5},
			},
		},
	}
	path := writeSwageFile(t, snap)

	var stdout, stderr bytes.Buffer
	code := run([]string{"read", path}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}

	lines := parseLines(stdout.String())
	if len(lines) != 2 {
		t.Fatalf("line count = %d, want 2", len(lines))
	}

	// Verify exact NDJSON structure matches .swage sample line format.
	tests := []struct {
		wantS string
		wantT int64
		wantV float64
	}{
		{wantS: "cpu", wantT: t0.UnixMilli(), wantV: 1.5},
		{wantS: "cpu", wantT: t0.Add(time.Second).UnixMilli(), wantV: 2.5},
	}

	for i, tt := range tests {
		var dl dumpLine
		if err := json.Unmarshal([]byte(lines[i]), &dl); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
		if dl.S != tt.wantS {
			t.Errorf("line %d: s = %q, want %q", i, dl.S, tt.wantS)
		}
		if dl.T != tt.wantT {
			t.Errorf("line %d: t = %d, want %d", i, dl.T, tt.wantT)
		}
		if dl.V != tt.wantV {
			t.Errorf("line %d: v = %v, want %v", i, dl.V, tt.wantV)
		}
	}
}

func TestSummary(t *testing.T) {
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)
	snap := &swage.Snapshot{
		From: t0,
		To:   t0.Add(10 * time.Minute),
		Series: map[string][]swage.Point{
			"cpu": {
				{T: t0, V: 1.0},
				{T: t0.Add(time.Minute), V: 2.0},
				{T: t0.Add(2 * time.Minute), V: 3.0},
				{T: t0.Add(6 * time.Minute), V: 10.0},
				{T: t0.Add(7 * time.Minute), V: 20.0},
			},
		},
	}
	path := writeSwageFile(t, snap)

	tests := []struct {
		name    string
		args    []string
		wantErr bool
		check   func(t *testing.T, output string)
	}{
		{
			name: "default window",
			args: []string{"summary", path},
			check: func(t *testing.T, output string) {
				t.Helper()
				// Should contain series name header.
				if !strings.Contains(output, "--- cpu ---") {
					t.Errorf("missing series header in output:\n%s", output)
				}
				// Should contain column headers.
				if !strings.Contains(output, "COUNT") {
					t.Errorf("missing COUNT header in output:\n%s", output)
				}
				if !strings.Contains(output, "MIN") {
					t.Errorf("missing MIN header in output:\n%s", output)
				}
			},
		},
		{
			name: "custom window",
			args: []string{"summary", "--window", "10m", path},
			check: func(t *testing.T, output string) {
				t.Helper()
				if !strings.Contains(output, "--- cpu ---") {
					t.Errorf("missing series header in output:\n%s", output)
				}
				// With 10m window over 10m range, should have 1 bucket.
				// Count header + 1 data line (plus series header and blank line).
				lines := parseNonHeaderLines(output)
				if len(lines) != 1 {
					t.Errorf("expected 1 bucket line, got %d\noutput:\n%s", len(lines), output)
				}
			},
		},
		{
			name:    "missing file",
			args:    []string{"summary", "/nonexistent/file.swage"},
			wantErr: true,
		},
		{
			name:    "no file argument",
			args:    []string{"summary"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)

			if tt.wantErr {
				if code == 0 {
					t.Error("expected non-zero exit code")
				}
				return
			}

			if code != 0 {
				t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
			}

			if tt.check != nil {
				tt.check(t, stdout.String())
			}
		})
	}
}

func TestSummaryBucketValues(t *testing.T) {
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)
	snap := &swage.Snapshot{
		From: t0,
		To:   t0.Add(5 * time.Minute),
		Series: map[string][]swage.Point{
			"cpu": {
				{T: t0, V: 10.0},
				{T: t0.Add(time.Minute), V: 20.0},
				{T: t0.Add(2 * time.Minute), V: 30.0},
			},
		},
	}
	path := writeSwageFile(t, snap)

	var stdout, stderr bytes.Buffer
	code := run([]string{"summary", "--window", "5m", path}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}

	output := stdout.String()
	// Verify the output contains expected values for the single bucket.
	// Min=10, Max=30, Count=3.
	if !strings.Contains(output, "3") {
		t.Errorf("expected count 3 in output:\n%s", output)
	}
	if !strings.Contains(output, "10.0000") {
		t.Errorf("expected min 10.0000 in output:\n%s", output)
	}
	if !strings.Contains(output, "30.0000") {
		t.Errorf("expected max 30.0000 in output:\n%s", output)
	}
}

func TestSummaryEmptySnapshot(t *testing.T) {
	t0 := time.Date(2025, 7, 6, 2, 0, 0, 0, time.UTC)
	snap := &swage.Snapshot{
		From:   t0,
		To:     t0.Add(time.Hour),
		Series: map[string][]swage.Point{},
	}
	path := writeSwageFile(t, snap)

	var stdout, stderr bytes.Buffer
	code := run([]string{"summary", path}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Errorf("expected empty output for empty snapshot, got %q", stdout.String())
	}
}

func TestRecover(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Now().Add(-30 * time.Minute)

	// Seed data into an ingotstore.
	store, err := ingotstore.New(dir, time.Hour)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	samples := []swage.Sample{
		{Name: "cpu", T: t0.UnixMilli(), V: 1.0},
		{Name: "cpu", T: t0.Add(time.Second).UnixMilli(), V: 2.0},
		{Name: "mem", T: t0.UnixMilli(), V: 100.0},
	}
	if err := store.Append(samples); err != nil {
		t.Fatalf("append: %v", err)
	}
	store.Close()

	// Recover to a file.
	outPath := filepath.Join(t.TempDir(), "recovered.swage")

	var stdout, stderr bytes.Buffer
	code := run([]string{"recover", "--last", "1h", "-o", outPath, dir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}

	// Read back the recovered .swage file and verify round-trip.
	snap, err := readSwageFile(outPath)
	if err != nil {
		t.Fatalf("read recovered file: %v", err)
	}

	// Should have both series.
	if len(snap.Series) != 2 {
		t.Fatalf("series count = %d, want 2", len(snap.Series))
	}

	cpuPoints := snap.Series["cpu"]
	if len(cpuPoints) != 2 {
		t.Fatalf("cpu points = %d, want 2", len(cpuPoints))
	}
	if cpuPoints[0].V != 1.0 || cpuPoints[1].V != 2.0 {
		t.Errorf("cpu values = [%v, %v], want [1.0, 2.0]", cpuPoints[0].V, cpuPoints[1].V)
	}

	memPoints := snap.Series["mem"]
	if len(memPoints) != 1 {
		t.Fatalf("mem points = %d, want 1", len(memPoints))
	}
	if memPoints[0].V != 100.0 {
		t.Errorf("mem value = %v, want 100.0", memPoints[0].V)
	}
}

func TestRecoverToStdout(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Now().Add(-10 * time.Minute)

	store, err := ingotstore.New(dir, time.Hour)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.Append([]swage.Sample{
		{Name: "cpu", T: t0.UnixMilli(), V: 42.0},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	store.Close()

	var stdout, stderr bytes.Buffer
	code := run([]string{"recover", "--last", "1h", dir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}

	// stdout should be valid .swage NDJSON.
	snap, err := swage.ReadSnapshot(&stdout)
	if err != nil {
		t.Fatalf("parse stdout as .swage: %v", err)
	}
	if len(snap.Series["cpu"]) != 1 {
		t.Errorf("expected 1 cpu point, got %d", len(snap.Series["cpu"]))
	}
}

func TestRecoverLockedDirectory(t *testing.T) {
	dir := t.TempDir()

	// Hold the flock to simulate a running process.
	store, err := ingotstore.New(dir, time.Hour)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	var stdout, stderr bytes.Buffer
	code := run([]string{"recover", dir}, &stdout, &stderr)
	if code == 0 {
		t.Error("expected non-zero exit code for locked directory")
	}
	if !strings.Contains(stderr.String(), "locked") {
		t.Errorf("stderr = %q, want message about locked directory", stderr.String())
	}
}

func TestRecoverNoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"recover"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRecoverNonexistentDir(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"recover", "/nonexistent/dir"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestReadInvalidSwageFormat(t *testing.T) {
	// Write an invalid .swage file.
	path := filepath.Join(t.TempDir(), "bad.swage")
	if err := os.WriteFile(path, []byte("{not valid json}\n"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	tests := []struct {
		name string
		cmd  string
	}{
		{name: "ls invalid", cmd: "ls"},
		{name: "read invalid", cmd: "read"},
		{name: "summary invalid", cmd: "summary"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run([]string{tt.cmd, path}, &stdout, &stderr)
			if code == 0 {
				t.Error("expected non-zero exit code for invalid .swage file")
			}
		})
	}
}

func TestRecoverRoundTrip(t *testing.T) {
	// Full round-trip: write data to ingotstore → recover → ls/read/summary.
	dir := t.TempDir()
	t0 := time.Now().Add(-30 * time.Minute)

	store, err := ingotstore.New(dir, time.Hour)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := store.Append([]swage.Sample{
			{Name: "latency_ms", T: t0.Add(time.Duration(i) * time.Second).UnixMilli(), V: float64(i) * 1.5},
			{Name: "errors", T: t0.Add(time.Duration(i) * time.Second).UnixMilli(), V: float64(i)},
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	store.Close()

	// Recover.
	outPath := filepath.Join(t.TempDir(), "roundtrip.swage")
	var stdout, stderr bytes.Buffer
	code := run([]string{"recover", "--last", "1h", "-o", outPath, dir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("recover: exit %d, stderr = %q", code, stderr.String())
	}

	// ls should show both series.
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"ls", outPath}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("ls: exit %d, stderr = %q", code, stderr.String())
	}
	names := parseLines(stdout.String())
	wantNames := []string{"errors", "latency_ms"}
	if !reflect.DeepEqual(names, wantNames) {
		t.Errorf("ls: got %v, want %v", names, wantNames)
	}

	// read --series latency_ms should have 10 lines.
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"read", "--series", "latency_ms", outPath}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("read: exit %d, stderr = %q", code, stderr.String())
	}
	readLines := parseLines(stdout.String())
	if len(readLines) != 10 {
		t.Errorf("read: got %d lines, want 10", len(readLines))
	}

	// summary should produce output for both series.
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"summary", "--window", "1h", outPath}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("summary: exit %d, stderr = %q", code, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "--- errors ---") {
		t.Errorf("summary missing errors series header")
	}
	if !strings.Contains(output, "--- latency_ms ---") {
		t.Errorf("summary missing latency_ms series header")
	}
}

// parseLines splits output into non-empty lines.
func parseLines(s string) []string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// parseNonHeaderLines returns data lines from summary output (excluding
// series headers "--- name ---", column header lines, and blank lines).
func parseNonHeaderLines(s string) []string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "---") {
			continue
		}
		if strings.Contains(line, "COUNT") {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}
