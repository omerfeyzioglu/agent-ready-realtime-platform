package main

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// clockPublisher writes the replay clock to Redis once a second, so the rest
// of the platform shares one "now": the API's now() tool answers from it,
// and comparing it with the live features' heartbeat shows how far the
// pipeline lags behind the app. The key has no expiry; readers judge it by
// wall_time_utc (a stopped producer leaves an old wall time behind).
//
// The producer logs in as its own Redis user, which may only write
// replay:* keys.
type clockPublisher struct {
	client    *redis.Client
	clock     atomic.Pointer[replayClock]
	lastEvent atomic.Int64 // event time of the last event sent, UTC microseconds
	sent      atomic.Int64
}

const clockKey = "replay:v1:clock"

func newClockPublisher(addr, user, password string) *clockPublisher {
	return &clockPublisher{client: redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: user,
		Password: password,
		// Older handshake (no CLIENT SETINFO), which the restricted user's
		// command list does not include.
		DisableIdentity: true,
	})}
}

// run publishes until ctx ends. Write errors are logged, never fatal: the
// clock is for observers, the events are what matter.
func (p *clockPublisher) run(ctx context.Context, speed float64) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var lastErr string
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		c := p.clock.Load()
		if c == nil {
			continue // replay has not started yet
		}
		err := p.client.HSet(ctx, clockKey, map[string]any{
			"replay_time_utc":     iso(c.now()),
			"last_event_time_utc": iso(p.lastEvent.Load()),
			"wall_time_utc":       time.Now().UTC().Format(time.RFC3339Nano),
			"speed":               speed,
			"events_sent":         p.sent.Load(),
		}).Err()
		if err != nil && err.Error() != lastErr {
			logf("replay clock to redis: %v", err)
		}
		if err != nil {
			lastErr = err.Error()
		} else {
			lastErr = ""
		}
	}
}

func (p *clockPublisher) close() { p.client.Close() }

func iso(micros int64) string {
	return time.UnixMicro(micros).UTC().Format(time.RFC3339)
}
