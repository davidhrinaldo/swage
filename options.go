package swage

import (
	"errors"
	"time"
)

// Options configures a Recorder.
type Options struct {
	// Horizon is how far back to retain data. Required — New returns an
	// error if zero.
	Horizon time.Duration

	// FlushInterval is how often the write buffer is flushed to the Store.
	// Default: 1s.
	FlushInterval time.Duration

	// MaxSeries is the maximum number of distinct series names. Samples for
	// new series beyond this limit are dropped. Default: 1000.
	MaxSeries int

	// MaxBufferSize is the maximum number of samples buffered before a
	// forced flush. Samples beyond this limit are dropped. Default: 10,000.
	MaxBufferSize int

	// OnOverBudget is called when a sample is dropped due to buffer or
	// cardinality limits, or invalid input. Default: no-op.
	OnOverBudget func(reason string, s Sample)

	// OnFlushError is called when Store.Append fails. Default: log to stderr.
	OnFlushError func(error)

	// Clock returns the current time. Default: time.Now.
	Clock func() time.Time
}

// defaults fills zero-valued fields with their defaults.
func (o *Options) defaults() {
	if o.FlushInterval == 0 {
		o.FlushInterval = time.Second
	}
	if o.MaxSeries == 0 {
		o.MaxSeries = 1000
	}
	if o.MaxBufferSize == 0 {
		o.MaxBufferSize = 10_000
	}
	if o.OnOverBudget == nil {
		o.OnOverBudget = func(string, Sample) {}
	}
	if o.OnFlushError == nil {
		o.OnFlushError = func(err error) {
			// Default: log to stderr. Import kept minimal; the Recorder
			// will wire this up with log.Println in practice.
		}
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
}

// validate checks required fields and returns an error if any are invalid.
func (o *Options) validate() error {
	if o.Horizon <= 0 {
		return errors.New("swage: Options.Horizon is required and must be positive")
	}
	if o.FlushInterval < 0 {
		return errors.New("swage: Options.FlushInterval must not be negative")
	}
	if o.MaxSeries < 0 {
		return errors.New("swage: Options.MaxSeries must not be negative")
	}
	if o.MaxBufferSize < 0 {
		return errors.New("swage: Options.MaxBufferSize must not be negative")
	}
	return nil
}
