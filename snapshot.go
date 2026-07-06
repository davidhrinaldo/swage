package swage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"time"
)

// Snapshot holds materialized raw samples for a set of series over a time range.
// It is point-in-time consistent when produced by Recorder.Snapshot.
type Snapshot struct {
	From   time.Time
	To     time.Time
	Series map[string][]Point
}

// Summary holds aggregated bucket statistics for a set of series over a time range.
type Summary struct {
	From   time.Time
	To     time.Time
	Window time.Duration
	Series map[string][]Bucket
}

// Bucket holds aggregate statistics for a single time window within a series.
type Bucket struct {
	Start, End    time.Time
	Min, Max      float64
	Mean          float64
	Count         int
	P50, P95, P99 float64
	Sum, Rate     float64
}

// Summary computes aggregated bucket statistics from a Snapshot's raw samples.
// This is the standalone aggregation path used by swagectl.
func (s *Snapshot) Summary(window time.Duration) *Summary {
	sum := &Summary{
		From:   s.From,
		To:     s.To,
		Window: window,
		Series: make(map[string][]Bucket, len(s.Series)),
	}
	for name, points := range s.Series {
		buckets := computeBuckets(s.From, s.To, window, points)
		if len(buckets) > 0 {
			sum.Series[name] = buckets
		}
	}
	return sum
}

// computeBuckets splits points into time-aligned buckets and computes
// aggregate statistics for each non-empty bucket.
func computeBuckets(from, to time.Time, window time.Duration, points []Point) []Bucket {
	if len(points) == 0 {
		return nil
	}

	var buckets []Bucket
	bucketStart := from
	for bucketStart.Before(to) {
		bucketEnd := bucketStart.Add(window)
		if bucketEnd.After(to) {
			bucketEnd = to
		}

		// Collect points in [bucketStart, bucketEnd).
		// The last bucket uses [bucketStart, bucketEnd] (inclusive end)
		// to capture points exactly at 'to'.
		var vals []float64
		var first, last float64
		for _, p := range points {
			if p.T.Before(bucketStart) {
				continue
			}
			if bucketEnd.Equal(to) {
				// Last bucket: inclusive end.
				if p.T.After(bucketEnd) {
					continue
				}
			} else {
				// Non-last bucket: exclusive end.
				if !p.T.Before(bucketEnd) {
					continue
				}
			}
			if len(vals) == 0 {
				first = p.V
			}
			last = p.V
			vals = append(vals, p.V)
		}

		if len(vals) > 0 {
			buckets = append(buckets, aggregateBucket(bucketStart, bucketEnd, first, last, vals))
		}

		bucketStart = bucketEnd
	}
	return buckets
}

// aggregateBucket computes statistics from a slice of values for one bucket.
// vals is sorted in place for percentile computation. first and last are the
// time-ordered first and last values (before sorting) for rate computation.
func aggregateBucket(start, end time.Time, first, last float64, vals []float64) Bucket {
	sort.Float64s(vals)

	n := len(vals)
	sum := 0.0
	for _, v := range vals {
		sum += v
	}

	b := Bucket{
		Start: start,
		End:   end,
		Min:   vals[0],
		Max:   vals[n-1],
		Mean:  sum / float64(n),
		Count: n,
		P50:   percentile(vals, 0.50),
		P95:   percentile(vals, 0.95),
		P99:   percentile(vals, 0.99),
		Sum:   sum,
	}

	// Rate = (last - first) / duration_seconds using time-ordered values.
	dur := end.Sub(start).Seconds()
	if dur > 0 && n > 1 {
		b.Rate = (last - first) / dur
	}

	return b
}

// percentile returns the p-th percentile using nearest-rank method.
func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	rank := p * float64(n-1)
	lower := int(math.Floor(rank))
	upper := lower + 1
	if upper >= n {
		return sorted[n-1]
	}
	frac := rank - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

// Snapshot flushes buffered samples and queries the Store for the requested
// series in the given time range. If names is empty, all series are queried.
// The result is point-in-time consistent: a single flush precedes all reads.
func (r *Recorder) Snapshot(from, to time.Time, names ...string) (*Snapshot, error) {
	if err := r.Flush(); err != nil {
		return nil, err
	}

	seriesNames, err := r.resolveNames(names)
	if err != nil {
		return nil, err
	}

	snap := &Snapshot{
		From:   from,
		To:     to,
		Series: make(map[string][]Point),
	}

	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()

	for _, name := range seriesNames {
		samples, err := r.store.Query(name, fromMs, toMs)
		if err != nil {
			return nil, fmt.Errorf("swage: query %q: %w", name, err)
		}
		if len(samples) == 0 {
			continue
		}
		points := make([]Point, len(samples))
		for i, s := range samples {
			points[i] = Point{T: time.UnixMilli(s.T), V: s.V}
		}
		snap.Series[name] = points
	}

	return snap, nil
}

// Summary flushes buffered samples and computes aggregated bucket statistics
// for the requested series. Queries one series at a time to bound peak memory.
// Not point-in-time consistent across series.
func (r *Recorder) Summary(from, to time.Time, window time.Duration, names ...string) (*Summary, error) {
	if err := r.Flush(); err != nil {
		return nil, err
	}

	seriesNames, err := r.resolveNames(names)
	if err != nil {
		return nil, err
	}

	sum := &Summary{
		From:   from,
		To:     to,
		Window: window,
		Series: make(map[string][]Bucket),
	}

	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()

	for _, name := range seriesNames {
		samples, err := r.store.Query(name, fromMs, toMs)
		if err != nil {
			return nil, fmt.Errorf("swage: query %q: %w", name, err)
		}
		if len(samples) == 0 {
			continue
		}
		points := make([]Point, len(samples))
		for i, s := range samples {
			points[i] = Point{T: time.UnixMilli(s.T), V: s.V}
		}
		buckets := computeBuckets(from, to, window, points)
		if len(buckets) > 0 {
			sum.Series[name] = buckets
		}
	}

	return sum, nil
}

// dumpHeader is the JSON header line for a .swage dump.
type dumpHeader struct {
	Swage  int      `json:"swage"`
	From   string   `json:"from"`
	To     string   `json:"to"`
	Series []string `json:"series"`
}

// dumpLine is a single sample line in a .swage dump.
type dumpLine struct {
	S string  `json:"s"`
	T int64   `json:"t"`
	V float64 `json:"v"`
}

// DumpTo flushes buffered samples and streams NDJSON to w without
// materializing all samples. Queries one series at a time. Series are
// grouped by name (sorted), timestamps ascending within each series.
func (r *Recorder) DumpTo(w io.Writer, from, to time.Time, names ...string) error {
	if err := r.Flush(); err != nil {
		return err
	}

	seriesNames, err := r.resolveNames(names)
	if err != nil {
		return err
	}

	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()

	// Determine which series actually have data and collect names for the header.
	// We need to know the series list for the header before writing any data,
	// so we query all series first. Peak memory is still one series at a time
	// since we only keep the names, not the samples.
	var presentNames []string
	for _, name := range seriesNames {
		samples, err := r.store.Query(name, fromMs, toMs)
		if err != nil {
			return fmt.Errorf("swage: query %q: %w", name, err)
		}
		if len(samples) > 0 {
			presentNames = append(presentNames, name)
		}
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	// Write header.
	hdr := dumpHeader{
		Swage:  1,
		From:   from.UTC().Format(time.RFC3339),
		To:     to.UTC().Format(time.RFC3339),
		Series: presentNames,
	}
	if hdr.Series == nil {
		hdr.Series = []string{}
	}
	if err := enc.Encode(hdr); err != nil {
		return fmt.Errorf("swage: write header: %w", err)
	}

	// Stream samples per series.
	for _, name := range presentNames {
		samples, err := r.store.Query(name, fromMs, toMs)
		if err != nil {
			return fmt.Errorf("swage: query %q: %w", name, err)
		}
		for _, s := range samples {
			if err := enc.Encode(dumpLine{S: s.Name, T: s.T, V: s.V}); err != nil {
				return fmt.Errorf("swage: write sample: %w", err)
			}
		}
	}

	return nil
}

// WriteTo serializes the Snapshot to NDJSON in the .swage format.
// The output consists of a header line followed by sample lines grouped
// by series name (sorted), with timestamps ascending within each series.
// Implements io.WriterTo.
func (s *Snapshot) WriteTo(w io.Writer) (int64, error) {
	cw := &countWriter{w: w}
	enc := json.NewEncoder(cw)
	enc.SetEscapeHTML(false)

	// Collect and sort series names.
	names := make([]string, 0, len(s.Series))
	for name := range s.Series {
		names = append(names, name)
	}
	sort.Strings(names)

	// Write header.
	hdr := dumpHeader{
		Swage:  1,
		From:   s.From.UTC().Format(time.RFC3339),
		To:     s.To.UTC().Format(time.RFC3339),
		Series: names,
	}
	if hdr.Series == nil {
		hdr.Series = []string{}
	}
	if err := enc.Encode(hdr); err != nil {
		return cw.n, fmt.Errorf("swage: write header: %w", err)
	}

	// Write sample lines per series.
	for _, name := range names {
		for _, p := range s.Series[name] {
			if err := enc.Encode(dumpLine{S: name, T: p.T.UnixMilli(), V: p.V}); err != nil {
				return cw.n, fmt.Errorf("swage: write sample: %w", err)
			}
		}
	}

	return cw.n, nil
}

// countWriter wraps an io.Writer and counts bytes written.
type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// DefaultMaxReadSamples is the maximum number of samples ReadSnapshot will
// parse before returning an error. Override per call by passing a limit to
// ReadSnapshot. 1M samples ≈ 24 MB of Point structs.
const DefaultMaxReadSamples = 1_000_000

// ReadSnapshot deserializes NDJSON from an io.Reader into a Snapshot.
// It rejects any format version other than 1. An optional maxSamples
// argument caps how many sample lines are read (default: DefaultMaxReadSamples).
func ReadSnapshot(r io.Reader, maxSamples ...int) (*Snapshot, error) {
	limit := DefaultMaxReadSamples
	if len(maxSamples) > 0 && maxSamples[0] > 0 {
		limit = maxSamples[0]
	}

	scanner := bufio.NewScanner(r)

	// Read header line.
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("swage: read header: %w", err)
		}
		return nil, fmt.Errorf("swage: empty input")
	}

	var hdr dumpHeader
	if err := json.Unmarshal(scanner.Bytes(), &hdr); err != nil {
		return nil, fmt.Errorf("swage: parse header: %w", err)
	}
	if hdr.Swage != 1 {
		return nil, fmt.Errorf("swage: unsupported format version %d", hdr.Swage)
	}

	from, err := time.Parse(time.RFC3339, hdr.From)
	if err != nil {
		return nil, fmt.Errorf("swage: parse header from: %w", err)
	}
	to, err := time.Parse(time.RFC3339, hdr.To)
	if err != nil {
		return nil, fmt.Errorf("swage: parse header to: %w", err)
	}

	snap := &Snapshot{
		From:   from,
		To:     to,
		Series: make(map[string][]Point),
	}

	// Read sample lines. Cap total samples to prevent OOM on malicious input.
	n := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if n >= limit {
			return nil, fmt.Errorf("swage: sample count exceeds limit (%d)", limit)
		}
		var sl dumpLine
		if err := json.Unmarshal(line, &sl); err != nil {
			return nil, fmt.Errorf("swage: parse sample line: %w", err)
		}
		if sl.S == "" {
			return nil, fmt.Errorf("swage: sample line missing series name")
		}
		snap.Series[sl.S] = append(snap.Series[sl.S], Point{
			T: time.UnixMilli(sl.T),
			V: sl.V,
		})
		n++
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("swage: read samples: %w", err)
	}

	return snap, nil
}

// resolveNames returns the list of series names to query. If names is empty,
// all series from the Store are returned (sorted).
func (r *Recorder) resolveNames(names []string) ([]string, error) {
	if len(names) > 0 {
		sorted := make([]string, len(names))
		copy(sorted, names)
		sort.Strings(sorted)
		return sorted, nil
	}
	seriesNames, err := r.store.Series()
	if err != nil {
		return nil, fmt.Errorf("swage: list series: %w", err)
	}
	sort.Strings(seriesNames)
	return seriesNames, nil
}
