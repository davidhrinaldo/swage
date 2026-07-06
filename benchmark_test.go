package swage_test

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/davidrinaldo/swage"
)

// nopStore discards all data. Isolates Recorder overhead from storage cost.
type nopStore struct{}

func (nopStore) Append([]swage.Sample) error                        { return nil }
func (nopStore) Query(string, int64, int64) ([]swage.Sample, error) { return nil, nil }
func (nopStore) Series() ([]string, error)                          { return nil, nil }
func (nopStore) Close() error                                       { return nil }

func BenchmarkRecord(b *testing.B) {
	rec, err := swage.New(nopStore{}, swage.Options{
		Horizon:       time.Hour,
		FlushInterval: time.Millisecond,
		MaxBufferSize: 1_000_000,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer rec.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec.Record("cpu", float64(i))
	}
}

func BenchmarkSeriesRecord(b *testing.B) {
	rec, err := swage.New(nopStore{}, swage.Options{
		Horizon:       time.Hour,
		FlushInterval: time.Millisecond,
		MaxBufferSize: 1_000_000,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer rec.Close()

	cpu := rec.Series("cpu")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cpu.Record(float64(i))
	}
}

func BenchmarkRecordParallel(b *testing.B) {
	rec, err := swage.New(nopStore{}, swage.Options{
		Horizon:       time.Hour,
		FlushInterval: time.Millisecond,
		MaxBufferSize: 10_000_000,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer rec.Close()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rec.Record("cpu", float64(i))
			i++
		}
	})
}

func BenchmarkSeriesRecordParallel(b *testing.B) {
	rec, err := swage.New(nopStore{}, swage.Options{
		Horizon:       time.Hour,
		FlushInterval: time.Millisecond,
		MaxBufferSize: 10_000_000,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer rec.Close()

	cpu := rec.Series("cpu")

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cpu.Record(float64(i))
			i++
		}
	})
}

// BenchmarkRecordMemory verifies that memory stays bounded over sustained
// recording. Records 1M samples through a Recorder with default buffer/series
// limits and a nopStore, then checks that heap growth is within expectations.
// The buffer cap (10K × ~60B ≈ 600KB) plus cardinality map overhead should
// keep steady-state heap well under 10MB.
func BenchmarkRecordMemory(b *testing.B) {
	rec, err := swage.New(nopStore{}, swage.Options{
		Horizon:       time.Hour,
		FlushInterval: time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer rec.Close()

	// Warm up — let the flush goroutine start and stabilize.
	for i := 0; i < 1000; i++ {
		rec.Record("warmup", float64(i))
	}
	rec.Flush()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	const samples = 1_000_000
	for i := 0; i < samples; i++ {
		rec.Record("cpu", float64(i))
	}
	rec.Flush()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// HeapInuse should not grow unboundedly. With a 10K buffer cap flushed
	// every 1ms, steady-state heap is dominated by the buffer (~600KB).
	// Allow 10MB as a generous ceiling.
	const maxHeapGrowth = 10 * 1024 * 1024
	growth := int64(after.HeapInuse) - int64(before.HeapInuse)
	if growth > maxHeapGrowth {
		b.Errorf("heap grew %d bytes after %d samples (before=%d, after=%d), limit=%d",
			growth, samples, before.HeapInuse, after.HeapInuse, maxHeapGrowth)
	}
	b.ReportMetric(float64(growth), "heap-growth-bytes")
	b.ReportMetric(float64(after.HeapInuse), "heap-inuse-bytes")
}

func BenchmarkSummary(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("samples=%d", n), func(b *testing.B) {
			t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			points := make([]swage.Point, n)
			for i := range points {
				points[i] = swage.Point{
					T: t0.Add(time.Duration(i) * time.Second),
					V: float64(i),
				}
			}
			snap := &swage.Snapshot{
				From:   t0,
				To:     t0.Add(time.Duration(n) * time.Second),
				Series: map[string][]swage.Point{"cpu": points},
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snap.Summary(5 * time.Minute)
			}
		})
	}
}

func BenchmarkWriteTo(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("samples=%d", n), func(b *testing.B) {
			t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			points := make([]swage.Point, n)
			for i := range points {
				points[i] = swage.Point{
					T: t0.Add(time.Duration(i) * time.Second),
					V: float64(i),
				}
			}
			snap := &swage.Snapshot{
				From:   t0,
				To:     t0.Add(time.Duration(n) * time.Second),
				Series: map[string][]swage.Point{"cpu": points},
			}

			// Measure encoded size for throughput reporting.
			var sizeBuf bytes.Buffer
			snap.WriteTo(&sizeBuf)
			b.SetBytes(int64(sizeBuf.Len()))

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snap.WriteTo(io.Discard)
			}
		})
	}
}

func BenchmarkReadSnapshot(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("samples=%d", n), func(b *testing.B) {
			t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			points := make([]swage.Point, n)
			for i := range points {
				points[i] = swage.Point{
					T: t0.Add(time.Duration(i) * time.Second),
					V: float64(i),
				}
			}
			snap := &swage.Snapshot{
				From:   t0,
				To:     t0.Add(time.Duration(n) * time.Second),
				Series: map[string][]swage.Point{"cpu": points},
			}

			var buf bytes.Buffer
			snap.WriteTo(&buf)
			data := buf.Bytes()
			b.SetBytes(int64(len(data)))

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				swage.ReadSnapshot(bytes.NewReader(data))
			}
		})
	}
}
