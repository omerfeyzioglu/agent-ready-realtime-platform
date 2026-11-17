// The producer plays the app's part: it reads finished trips from the
// source files, splits each one into the events the app would have sent
// (requested, arrived, picked_up, dropped_off, tip_added) and sends them in
// event time order, paced by a replay clock.
//
// Local check (files on disk, JSON lines to stdout):
//
//	producer --input a.parquet --speed 0
//
// In the cluster (months downloaded from S3, events to Kafka, resumes where
// the topic stops):
//
//	producer --months 2026-08,2026-09 --sink kafka --brokers events-kafka-bootstrap.kafka:9092 --speed 60
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type options struct {
	input, months, bucket, region, workdir string
	sinkKind, brokers, topic               string
	speed                                  float64
	startAt, until                         string
	limit                                  int64
	resume                                 bool
}

func main() {
	var o options
	flag.StringVar(&o.input, "input", "", "local source files, comma separated, in time order")
	flag.StringVar(&o.months, "months", "", "source months to download from S3, e.g. 2026-08,2026-09 (instead of --input)")
	flag.StringVar(&o.bucket, "bucket", "nyc-taxi-raw-969788248726", "S3 bucket with source/")
	flag.StringVar(&o.region, "region", "us-east-1", "AWS region")
	flag.StringVar(&o.workdir, "workdir", "/data", "where downloaded source files are kept")
	flag.StringVar(&o.sinkKind, "sink", "stdout", "stdout or kafka")
	flag.StringVar(&o.brokers, "brokers", "events-kafka-bootstrap.kafka:9092", "Kafka bootstrap servers, comma separated")
	flag.StringVar(&o.topic, "topic", "trip-events", "Kafka topic")
	flag.Float64Var(&o.speed, "speed", 1, "replay speed: 1 = real time, 60 = one hour per minute, 0 = no waiting")
	flag.StringVar(&o.startAt, "start-at", "", "skip events before this UTC time (RFC 3339); default: the first event")
	flag.StringVar(&o.until, "until", "", "stop at this UTC time (RFC 3339)")
	flag.Int64Var(&o.limit, "limit", 0, "stop after this many events (0 = no limit)")
	flag.BoolVar(&o.resume, "resume", true, "kafka sink: continue after the newest event already in the topic")
	flag.Parse()
	if err := run(o); err != nil {
		logf("error: %v", err)
		os.Exit(1)
	}
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, time.Now().UTC().Format("15:04:05")+" "+format+"\n", args...)
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

func run(o options) error {
	from, err := parseTime(o.startAt)
	if err != nil {
		return fmt.Errorf("--start-at: %w", err)
	}
	to, err := parseTime(o.until)
	if err != nil {
		return fmt.Errorf("--until: %w", err)
	}

	// Stop cleanly on SIGTERM (pod deletion): what was sent gets flushed.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var paths []string
	switch {
	case o.input != "" && o.months != "":
		return fmt.Errorf("use --input or --months, not both")
	case o.input != "":
		paths = strings.Split(o.input, ",")
	case o.months != "":
		if paths, err = download(ctx, o.bucket, o.region, o.workdir, strings.Split(o.months, ",")); err != nil {
			return err
		}
	default:
		return fmt.Errorf("--input or --months is required")
	}

	var out sink
	switch o.sinkKind {
	case "stdout":
		out = newStdoutSink()
	case "kafka":
		brokers := strings.Split(o.brokers, ",")
		if o.resume {
			newest, err := resumePoint(brokers, o.topic)
			if err != nil {
				return fmt.Errorf("resume: %w", err)
			}
			if newest > from {
				logf("resuming at %s (newest event already in %s)", time.UnixMicro(newest).UTC().Format(time.RFC3339), o.topic)
				from = newest
			}
		}
		if out, err = newKafkaSink(brokers, o.topic); err != nil {
			return err
		}
	default:
		return fmt.Errorf("--sink must be stdout or kafka")
	}

	tz, err := newLocalClock()
	if err != nil {
		return err
	}
	chunks, closeFiles, err := openChunks(paths, tz)
	if err != nil {
		return err
	}
	defer closeFiles()

	var clock *replayClock
	var sent, last int64
	counts := map[string]int64{}
	began := time.Now()
	lastLog := began

	err = replay(chunks, tz, func(p pending) (bool, error) {
		if ctx.Err() != nil {
			return false, nil
		}
		if p.at < from {
			return true, nil
		}
		if to != 0 && p.at >= to {
			return false, nil
		}
		if clock == nil {
			// The replay clock starts at --start-at (or the resume point),
			// otherwise at the first event.
			start := p.at
			if from != 0 {
				start = from
			}
			clock = &replayClock{start: start, wallStart: time.Now(), speed: o.speed}
		}
		if d := clock.wait(p.at); d > time.Millisecond {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return false, nil
			}
		}
		ev, err := build(p)
		if err != nil {
			return false, err
		}
		if err := out.send(ev, clock.now()); err != nil {
			return false, err
		}
		sent++
		last = p.at
		counts[ev.EventType]++
		if time.Since(lastLog) >= 30*time.Second {
			lastLog = time.Now()
			logf("sent %d events, replay time %s", sent, time.UnixMicro(last).UTC().Format(time.RFC3339))
		}
		return o.limit == 0 || sent < o.limit, nil
	})
	closeErr := out.close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}

	lastTime := ""
	if sent > 0 {
		lastTime = time.UnixMicro(last).UTC().Format(time.RFC3339)
	}
	logf("done: %d events %v in %.1fs, last event time %s", sent, counts, time.Since(began).Seconds(), lastTime)
	return nil
}
