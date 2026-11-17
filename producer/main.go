// The producer plays the app's part: it reads finished trips from the
// source files, splits each one into the events the app would have sent
// (requested, arrived, picked_up, dropped_off, tip_added) and sends them in
// event time order, paced by a replay clock.
//
// For now events go to standard output as JSON lines; Kafka is the next sink.
//
//	producer --input a.parquet[,b.parquet] [--speed 60] [--start-at T] [--until T] [--limit N]
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	input := flag.String("input", "", "source trip files, comma separated, in time order")
	speed := flag.Float64("speed", 1, "replay speed: 1 = real time, 60 = one hour per minute, 0 = no waiting")
	startAt := flag.String("start-at", "", "skip events before this UTC time (RFC 3339); default: the first event")
	until := flag.String("until", "", "stop at this UTC time (RFC 3339)")
	limit := flag.Int64("limit", 0, "stop after this many events (0 = no limit)")
	flag.Parse()
	if err := run(*input, *speed, *startAt, *until, *limit); err != nil {
		fmt.Fprintln(os.Stderr, "producer:", err)
		os.Exit(1)
	}
}

func parseTime(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, err
	}
	return t.UnixMicro(), nil
}

func run(input string, speed float64, startAt, until string, limit int64) error {
	if input == "" {
		return fmt.Errorf("--input is required")
	}
	from, err := parseTime(startAt)
	if err != nil {
		return fmt.Errorf("--start-at: %w", err)
	}
	to, err := parseTime(until)
	if err != nil {
		return fmt.Errorf("--until: %w", err)
	}

	tz, err := newLocalClock()
	if err != nil {
		return err
	}
	chunks, closeFiles, err := openChunks(strings.Split(input, ","), tz)
	if err != nil {
		return err
	}
	defer closeFiles()

	out := bufio.NewWriterSize(os.Stdout, 1<<20)
	defer out.Flush()
	enc := json.NewEncoder(out)

	var clock *replayClock
	var sent, last int64
	counts := map[string]int64{}
	began := time.Now()

	err = replay(chunks, tz, func(p pending) (bool, error) {
		if p.at < from {
			return true, nil
		}
		if to != 0 && p.at >= to {
			return false, nil
		}
		if clock == nil {
			// The replay clock starts at --start-at, or at the first event.
			start := p.at
			if from != 0 {
				start = from
			}
			clock = &replayClock{start: start, wallStart: time.Now(), speed: speed}
		}
		if d := clock.wait(p.at); d > time.Millisecond {
			out.Flush() // nothing stays in the buffer while we sleep
			time.Sleep(d)
		}
		ev, err := build(p)
		if err != nil {
			return false, err
		}
		if err := enc.Encode(ev); err != nil {
			return false, err
		}
		sent++
		last = p.at
		counts[ev.EventType]++
		return limit == 0 || sent < limit, nil
	})
	if err != nil {
		return err
	}

	summary, _ := json.Marshal(map[string]any{
		"events": sent, "by_type": counts, "seconds": time.Since(began).Seconds(),
		"last_event_time": time.UnixMicro(last).UTC().Format(time.RFC3339),
	})
	fmt.Fprintln(os.Stderr, string(summary))
	return nil
}
