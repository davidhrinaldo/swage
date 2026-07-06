// Command swagectl is the CLI tool for offline analysis of .swage files
// and crash recovery of ingot data directories.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/davidrinaldo/swage"
	"github.com/davidrinaldo/swage/ingotstore"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches to the appropriate subcommand. Returns an exit code.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: swagectl <command> [arguments]")
		fmt.Fprintln(stderr, "commands: recover, ls, read, summary")
		return 1
	}

	var err error
	switch args[0] {
	case "recover":
		err = cmdRecover(args[1:], stdout, stderr)
	case "ls":
		err = cmdLs(args[1:], stdout, stderr)
	case "read":
		err = cmdRead(args[1:], stdout, stderr)
	case "summary":
		err = cmdSummary(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", args[0])
		fmt.Fprintln(stderr, "commands: recover, ls, read, summary")
		return 1
	}

	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

// cmdRecover opens a dead process's ingot data directory, builds a Snapshot,
// and writes it as a .swage file.
func cmdRecover(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	fs.SetOutput(stderr)
	last := fs.Duration("last", time.Hour, "how far back to recover")
	output := fs.String("o", "", "output file (default: stdout)")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: swagectl recover [--last duration] [-o path] <dir>")
	}
	dir := fs.Arg(0)

	// Open the ingot store. This acquires the flock — if the owning process
	// is still running, New returns an error.
	store, err := ingotstore.New(dir, *last)
	if err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	defer store.Close()

	now := time.Now()
	from := now.Add(-*last)
	to := now

	// Get all series names.
	names, err := store.Series()
	if err != nil {
		return fmt.Errorf("recover: list series: %w", err)
	}
	sort.Strings(names)

	// Build a Snapshot by querying each series.
	snap := &swage.Snapshot{
		From:   from,
		To:     to,
		Series: make(map[string][]swage.Point),
	}

	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()

	for _, name := range names {
		samples, err := store.Query(name, fromMs, toMs)
		if err != nil {
			return fmt.Errorf("recover: query %q: %w", name, err)
		}
		if len(samples) == 0 {
			continue
		}
		points := make([]swage.Point, len(samples))
		for i, s := range samples {
			points[i] = swage.Point{T: time.UnixMilli(s.T), V: s.V}
		}
		snap.Series[name] = points
	}

	// Write the .swage file.
	var w io.Writer = stdout
	if *output != "" {
		f, err := os.Create(*output)
		if err != nil {
			return fmt.Errorf("recover: create output: %w", err)
		}
		defer f.Close()
		w = f
	}

	if _, err := snap.WriteTo(w); err != nil {
		return fmt.Errorf("recover: write: %w", err)
	}
	return nil
}

// cmdLs reads a .swage file and prints series names one per line, sorted.
func cmdLs(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("ls", flag.ContinueOnError)
	fs.SetOutput(stderr)

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: swagectl ls <file.swage>")
	}

	snap, err := readSwageFile(fs.Arg(0))
	if err != nil {
		return err
	}

	names := make([]string, 0, len(snap.Series))
	for name := range snap.Series {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		fmt.Fprintln(stdout, name)
	}
	return nil
}

// dumpLine is a single sample line in a .swage dump.
type dumpLine struct {
	S string  `json:"s"`
	T int64   `json:"t"`
	V float64 `json:"v"`
}

// cmdRead reads a .swage file and prints samples as NDJSON.
func cmdRead(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("read", flag.ContinueOnError)
	fs.SetOutput(stderr)
	series := fs.String("series", "", "comma-separated series names to filter")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: swagectl read [--series name[,name...]] <file.swage>")
	}

	snap, err := readSwageFile(fs.Arg(0))
	if err != nil {
		return err
	}

	// Determine which series to print.
	var names []string
	if *series != "" {
		names = strings.Split(*series, ",")
		sort.Strings(names)
	} else {
		names = make([]string, 0, len(snap.Series))
		for name := range snap.Series {
			names = append(names, name)
		}
		sort.Strings(names)
	}

	enc := json.NewEncoder(stdout)
	enc.SetEscapeHTML(false)

	for _, name := range names {
		points, ok := snap.Series[name]
		if !ok {
			continue
		}
		for _, p := range points {
			if err := enc.Encode(dumpLine{S: name, T: p.T.UnixMilli(), V: p.V}); err != nil {
				return fmt.Errorf("read: write: %w", err)
			}
		}
	}
	return nil
}

// cmdSummary reads a .swage file, computes a Summary, and prints a
// human-readable table of bucket aggregates per series.
func cmdSummary(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("summary", flag.ContinueOnError)
	fs.SetOutput(stderr)
	window := fs.Duration("window", 5*time.Minute, "aggregation window duration")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: swagectl summary [--window duration] <file.swage>")
	}

	snap, err := readSwageFile(fs.Arg(0))
	if err != nil {
		return err
	}

	sum := snap.Summary(*window)

	names := make([]string, 0, len(sum.Series))
	for name := range sum.Series {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		fmt.Fprintf(stdout, "--- %s ---\n", name)
		fmt.Fprintf(stdout, "%-26s %-26s %8s %10s %10s %10s %10s %10s %10s %10s\n",
			"START", "END", "COUNT", "MIN", "MAX", "MEAN", "P50", "P95", "P99", "RATE")
		for _, b := range sum.Series[name] {
			fmt.Fprintf(stdout, "%-26s %-26s %8d %10.4f %10.4f %10.4f %10.4f %10.4f %10.4f %10.4f\n",
				b.Start.UTC().Format(time.RFC3339),
				b.End.UTC().Format(time.RFC3339),
				b.Count, b.Min, b.Max, b.Mean, b.P50, b.P95, b.P99, b.Rate)
		}
		fmt.Fprintln(stdout)
	}
	return nil
}

// readSwageFile opens a .swage file and parses it into a Snapshot.
func readSwageFile(path string) (*swage.Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	snap, err := swage.ReadSnapshot(f)
	if err != nil {
		return nil, err
	}
	return snap, nil
}
