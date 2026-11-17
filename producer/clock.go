package main

import (
	"fmt"
	"time"
)

// localClock converts NYC wall-clock timestamps to UTC, the way the app
// would stamp them. It refuses days on which the clocks change: the
// fall-back hour happens twice and needs the trip-level rule that lives in
// the history backfill, and the live months (August, September) have no
// such day.
type localClock struct {
	loc        *time.Location
	changeDays map[int64]bool // day number -> clocks change on that day
}

func newLocalClock() (*localClock, error) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		return nil, err
	}
	return &localClock{loc: loc, changeDays: map[int64]bool{}}, nil
}

const microsPerDay = 86_400_000_000

func (c *localClock) toUTC(localMicros int64) (int64, error) {
	w := time.UnixMicro(localMicros).UTC() // wall-clock fields, not a real instant
	day := localMicros / microsPerDay
	changes, seen := c.changeDays[day]
	if !seen {
		_, before := time.Date(w.Year(), w.Month(), w.Day(), 0, 0, 0, 0, c.loc).Zone()
		_, after := time.Date(w.Year(), w.Month(), w.Day()+1, 0, 0, 0, 0, c.loc).Zone()
		changes = before != after
		c.changeDays[day] = changes
	}
	if changes {
		return 0, fmt.Errorf("%s is a daylight-saving change day; the producer does not replay those", w.Format("2006-01-02"))
	}
	return time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute(), w.Second(), w.Nanosecond(), c.loc).UnixMicro(), nil
}

// replayClock maps event time to wall time. Replay time starts at `start`
// and moves `speed` times faster than the wall clock; speed 0 means no
// waiting at all.
type replayClock struct {
	start     int64 // replay time at launch, UTC microseconds
	wallStart time.Time
	speed     float64
}

// now is the current replay time. With speed 0 (no waiting) replay time
// is undefined, so the start is returned.
func (c *replayClock) now() int64 {
	if c.speed <= 0 {
		return c.start
	}
	return c.start + int64(float64(time.Since(c.wallStart).Microseconds())*c.speed)
}

// wait returns how long to sleep before an event at `at` may be sent.
func (c *replayClock) wait(at int64) time.Duration {
	if c.speed <= 0 {
		return 0
	}
	due := time.Duration(float64(at-c.start)/c.speed) * time.Microsecond
	return due - time.Since(c.wallStart)
}
