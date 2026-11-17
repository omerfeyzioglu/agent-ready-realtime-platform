package main

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/parquet-go/parquet-go"
)

// eventHeap keeps pending events with the earliest on top. Ties are broken
// by trip and kind so that a replay always produces the same order.
type eventHeap []pending

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	if c := bytes.Compare(h[i].trip.TripID[:], h[j].trip.TripID[:]); c != 0 {
		return c < 0
	}
	return h[i].kind < h[j].kind
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(pending)) }
func (h *eventHeap) Pop() any {
	old := *h
	p := old[len(old)-1]
	*h = old[:len(old)-1]
	return p
}

// chunk is one Parquet row group: the unit the producer reads at a time.
type chunk struct {
	rows     parquet.RowGroup
	earliest int64 // earliest event it can contain, UTC microseconds
}

// openChunks lists the row groups of all input files in order. For each one
// it takes the smallest timestamp from the column statistics in the file
// footer, so no data has to be read to know where a row group starts.
func openChunks(paths []string, tz *localClock) ([]chunk, func(), error) {
	var chunks []chunk
	var files []*os.File
	closeAll := func() {
		for _, f := range files {
			f.Close()
		}
	}
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		files = append(files, f)
		st, err := f.Stat()
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		pf, err := parquet.OpenFile(f, st.Size())
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("%s: %w", path, err)
		}
		meta := pf.Metadata()
		for i, rg := range pf.RowGroups() {
			local := int64(math.MaxInt64)
			for _, col := range meta.RowGroups[i].Columns {
				if len(col.MetaData.PathInSchema) != 1 || !isTimeColumn(col.MetaData.PathInSchema[0]) {
					continue
				}
				if col.MetaData.NumValues == col.MetaData.Statistics.NullCount {
					continue // column is all NULL in this row group
				}
				min := col.MetaData.Statistics.MinValue
				if len(min) != 8 {
					closeAll()
					return nil, nil, fmt.Errorf("%s: row group %d has no min statistic for %s", path, i, col.MetaData.PathInSchema[0])
				}
				if v := int64(binary.LittleEndian.Uint64(min)); v < local {
					local = v
				}
			}
			if local == math.MaxInt64 {
				closeAll()
				return nil, nil, fmt.Errorf("%s: row group %d has no timestamps", path, i)
			}
			utc, err := tz.toUTC(local)
			if err != nil {
				closeAll()
				return nil, nil, err
			}
			chunks = append(chunks, chunk{rows: rg, earliest: utc})
		}
	}
	if len(chunks) == 0 {
		closeAll()
		return nil, nil, errors.New("no row groups in the input")
	}
	return chunks, closeAll, nil
}

func isTimeColumn(name string) bool {
	for _, c := range timeColumns {
		if c == name {
			return true
		}
	}
	return false
}

// replay reads the chunks in order and hands every event to emit in event
// time order.
//
// A trip row holds events that lie minutes or hours apart, and row groups
// overlap in time, so events wait in a heap. After reading chunk i, an event
// is safe to send only if it is earlier than anything the unread chunks can
// still contain: that limit is the smallest `earliest` among chunks i+1...
func replay(chunks []chunk, tz *localClock, emit func(pending) (bool, error)) error {
	// safeBefore[i] = smallest earliest of chunks i.. (suffix minimum)
	safeBefore := make([]int64, len(chunks)+1)
	safeBefore[len(chunks)] = math.MaxInt64
	for i := len(chunks) - 1; i >= 0; i-- {
		safeBefore[i] = min(chunks[i].earliest, safeBefore[i+1])
	}

	h := &eventHeap{}
	buf := make([]Trip, 4096)
	var events []pending
	for i, c := range chunks {
		r := parquet.NewGenericRowGroupReader[Trip](c.rows)
		for {
			n, err := r.Read(buf)
			for k := 0; k < n; k++ {
				t := buf[k] // copy: the buffer is reused
				var serr error
				if events, serr = split(&t, tz, events[:0]); serr != nil {
					r.Close()
					return serr
				}
				for _, p := range events {
					heap.Push(h, p)
				}
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				r.Close()
				return err
			}
		}
		r.Close()

		for h.Len() > 0 && (*h)[0].at < safeBefore[i+1] {
			more, err := emit(heap.Pop(h).(pending))
			if err != nil || !more {
				return err
			}
		}
	}
	return nil
}
