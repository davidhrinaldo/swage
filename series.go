package swage

import (
	"math"
	"time"
	"unicode/utf8"
)

// Series is a handle for fast-path recording of a single named series.
// It caches the cardinality check using a generation counter so the hot
// path skips the name map lookup and budget check — just mutex, append,
// unlock. When series eviction bumps the generation, the handle falls back
// to re-registration on the next call.
//
// A Series handle whose name was evicted from the cardinality map
// re-registers on the next Record call, consuming a cardinality slot.
// The handle never becomes permanently broken.
type Series struct {
	r       *Recorder
	name    string
	nameOk  bool   // true if name passed validation
	lastGen uint64 // generation when cardinality was last verified
}

// Record appends a sample with the current timestamp. Fire-and-forget: never
// returns an error or panics. After the Recorder is closed, it is a silent no-op.
func (s *Series) Record(value float64) {
	s.RecordAt(s.r.opts.Clock(), value)
}

// RecordAt appends a sample with the given timestamp. Fire-and-forget: never
// returns an error or panics. After the Recorder is closed, it is a silent no-op.
func (s *Series) RecordAt(t time.Time, value float64) {
	sample := Sample{Name: s.name, T: t.UnixMilli(), V: value}

	// Validate name once and cache the result.
	if !s.nameOk {
		if s.name == "" || !utf8.ValidString(s.name) {
			s.r.opts.OnOverBudget("invalid_name", sample)
			return
		}
		s.nameOk = true
	}

	if math.IsNaN(value) || math.IsInf(value, 0) {
		s.r.opts.OnOverBudget("invalid_value", sample)
		return
	}

	r := s.r
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}

	// Buffer cap check.
	if len(r.buf) >= r.opts.MaxBufferSize {
		r.mu.Unlock()
		r.opts.OnOverBudget("max_buffer", sample)
		return
	}

	// Fast path: if generation hasn't changed since last check, the name
	// is still in the cardinality map — skip the lookup.
	if s.lastGen != r.evictGen {
		if _, exists := r.seriesMap[s.name]; !exists {
			if len(r.seriesMap) >= r.opts.MaxSeries {
				r.mu.Unlock()
				r.opts.OnOverBudget("max_series", sample)
				return
			}
		}
		s.lastGen = r.evictGen
	}
	r.seriesMap[s.name] = sample.T

	r.buf = append(r.buf, sample)
	r.mu.Unlock()
}
