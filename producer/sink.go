package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// sink is where events go: standard output for local checks, Kafka in the
// cluster.
type sink interface {
	send(ev Event, replayAt int64) error
	close() error
}

// stdoutSink writes one JSON line per event.
type stdoutSink struct {
	w   *bufio.Writer
	enc *json.Encoder
}

func newStdoutSink() *stdoutSink {
	w := bufio.NewWriterSize(os.Stdout, 1<<20)
	return &stdoutSink{w: w, enc: json.NewEncoder(w)}
}

func (s *stdoutSink) send(ev Event, _ int64) error { return s.enc.Encode(ev) }
func (s *stdoutSink) close() error                 { return s.w.Flush() }

// kafkaSink produces to one topic. Delivery settings:
//   - key = trip_id: one trip's events land in one partition, in order
//     (Kafka's default murmur2 partitioner, same as the Java client);
//   - acks = all and idempotence (franz-go defaults): a write counts only
//     when the broker has stored it, and a retried batch is not stored twice;
//   - zstd compression per batch.
//
// Records are sent asynchronously; the first delivery error stops the run.
type kafkaSink struct {
	client *kgo.Client
	mu     sync.Mutex
	err    error
}

func newKafkaSink(brokers []string, topic string) (*kafkaSink, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerLinger(20*time.Millisecond),
		// Never give up on a record while the broker is briefly away
		// (restart, node replacement); the run stops only on real errors.
		kgo.RecordRetries(1<<30),
		kgo.ClientID("trip-event-producer"),
	)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka not reachable at %v: %w", brokers, err)
	}
	return &kafkaSink{client: client}, nil
}

func (s *kafkaSink) send(ev Event, replayAt int64) error {
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return err
	}
	value, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	rec := &kgo.Record{
		Key:   []byte(ev.TripID),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte(ev.EventType)},
			// When the event was sent on the replay clock. At speed > 1 the
			// wall-clock record timestamp is compressed; this one is not.
			{Key: "replay_time", Value: []byte(time.UnixMicro(replayAt).UTC().Format(time.RFC3339Nano))},
		},
	}
	s.client.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
		if err != nil {
			s.mu.Lock()
			if s.err == nil {
				s.err = err
			}
			s.mu.Unlock()
		}
	})
	return nil
}

func (s *kafkaSink) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	flushErr := s.client.Flush(ctx)
	s.client.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(s.err, flushErr)
}

// resumePoint returns the largest event time already in the topic (UTC
// microseconds), or 0 when the topic is empty. Events are produced in event
// time order, so the newest record of each partition tells where a stopped
// run had got to. Events at exactly that time may be sent again; their
// event_id is derived, so staging recognises the duplicates.
func resumePoint(brokers []string, topic string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return 0, err
	}
	defer admin.Close()
	ends, err := kadm.NewClient(admin).ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, err
	}
	if err := ends.Error(); err != nil {
		return 0, err
	}

	last := map[int32]kgo.Offset{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			last[o.Partition] = kgo.NewOffset().At(o.Offset - 1)
		}
	})
	if len(last) == 0 {
		return 0, nil
	}

	reader, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: last}),
	)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	var newest int64
	seen := map[int32]bool{}
	for len(seen) < len(last) {
		fetches := reader.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("reading the last records: %w", err)
		}
		var ferr error
		fetches.EachError(func(_ string, _ int32, err error) { ferr = err })
		if ferr != nil {
			return 0, ferr
		}
		for _, r := range fetches.Records() {
			seen[r.Partition] = true
			var ev struct {
				EventTime time.Time `json:"event_time"`
			}
			if err := json.Unmarshal(r.Value, &ev); err != nil {
				return 0, fmt.Errorf("partition %d offset %d: %w", r.Partition, r.Offset, err)
			}
			if t := ev.EventTime.UnixMicro(); t > newest {
				newest = t
			}
		}
	}
	return newest, nil
}
