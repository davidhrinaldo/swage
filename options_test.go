package swage

import (
	"testing"
	"time"
)

func TestOptionsDefaults(t *testing.T) {
	tests := []struct {
		name          string
		opts          Options
		wantFlush     time.Duration
		wantMaxSeries int
		wantMaxBuffer int
		wantClockNil  bool
	}{
		{
			name:          "all zeros get defaults",
			opts:          Options{Horizon: time.Hour},
			wantFlush:     time.Second,
			wantMaxSeries: 1000,
			wantMaxBuffer: 10_000,
		},
		{
			name:          "explicit values preserved",
			opts:          Options{Horizon: time.Hour, FlushInterval: 5 * time.Second, MaxSeries: 50, MaxBufferSize: 500},
			wantFlush:     5 * time.Second,
			wantMaxSeries: 50,
			wantMaxBuffer: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.opts.defaults()

			if tt.opts.FlushInterval != tt.wantFlush {
				t.Errorf("FlushInterval = %v, want %v", tt.opts.FlushInterval, tt.wantFlush)
			}
			if tt.opts.MaxSeries != tt.wantMaxSeries {
				t.Errorf("MaxSeries = %d, want %d", tt.opts.MaxSeries, tt.wantMaxSeries)
			}
			if tt.opts.MaxBufferSize != tt.wantMaxBuffer {
				t.Errorf("MaxBufferSize = %d, want %d", tt.opts.MaxBufferSize, tt.wantMaxBuffer)
			}
			if tt.opts.OnOverBudget == nil {
				t.Errorf("OnOverBudget is nil, want non-nil default")
			}
			if tt.opts.OnFlushError == nil {
				t.Errorf("OnFlushError is nil, want non-nil default")
			}
			if tt.opts.Clock == nil {
				t.Errorf("Clock is nil, want non-nil default")
			}
		})
	}
}

func TestOptionsValidate(t *testing.T) {
	tests := []struct {
		name    string
		opts    Options
		wantErr bool
	}{
		{
			name:    "valid options",
			opts:    Options{Horizon: time.Hour},
			wantErr: false,
		},
		{
			name:    "zero horizon",
			opts:    Options{},
			wantErr: true,
		},
		{
			name:    "negative horizon",
			opts:    Options{Horizon: -time.Second},
			wantErr: true,
		},
		{
			name:    "negative flush interval",
			opts:    Options{Horizon: time.Hour, FlushInterval: -time.Second},
			wantErr: true,
		},
		{
			name:    "negative max series",
			opts:    Options{Horizon: time.Hour, MaxSeries: -1},
			wantErr: true,
		},
		{
			name:    "negative max buffer size",
			opts:    Options{Horizon: time.Hour, MaxBufferSize: -1},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
