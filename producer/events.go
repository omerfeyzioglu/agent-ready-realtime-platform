package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Event kinds, in the order a trip goes through them. The number is also the
// tie-breaker when two events of one trip share a timestamp.
const (
	kindRequested = iota
	kindArrived
	kindPickedUp
	kindDroppedOff
	kindTipAdded
)

var kindNames = [...]string{"requested", "arrived", "picked_up", "dropped_off", "tip_added"}

// pending is an event waiting for its turn: just the time, the kind and the
// trip it belongs to. The payload is built only when the event is sent.
type pending struct {
	at   int64 // event time, UTC microseconds
	kind uint8
	trip *Trip
}

type requestedPayload struct {
	Provider        *string `json:"provider"`
	OriginatingBase *string `json:"originating_base"`
	PickupZone      *int32  `json:"pickup_zone"`
	SharedRequest   *string `json:"shared_request"`
	WavRequest      *string `json:"wav_request"`
	AccessARide     *string `json:"access_a_ride"`
}

type arrivedPayload struct {
	DispatchingBase *string `json:"dispatching_base"`
	WavMatch        *string `json:"wav_match"`
}

type droppedOffPayload struct {
	DropoffZone         *int32   `json:"dropoff_zone"`
	TripMiles           *float64 `json:"trip_miles"`
	TripTime            *int64   `json:"trip_time"`
	BasePassengerFare   *float64 `json:"base_passenger_fare"`
	Tolls               *float64 `json:"tolls"`
	BCF                 *float64 `json:"bcf"`
	SalesTax            *float64 `json:"sales_tax"`
	CongestionSurcharge *float64 `json:"congestion_surcharge"`
	AirportFee          *float64 `json:"airport_fee"`
	CBDCongestionFee    *float64 `json:"cbd_congestion_fee"`
	DriverPay           *float64 `json:"driver_pay"`
	SharedMatch         *string  `json:"shared_match"`
}

type tipAddedPayload struct {
	Tips *float64 `json:"tips"`
}

// Event is what leaves the producer: the envelope plus the payload.
type Event struct {
	EventID   string          `json:"event_id"`
	TripID    string          `json:"trip_id"`
	EventType string          `json:"event_type"`
	EventTime string          `json:"event_time"`
	Payload   json.RawMessage `json:"payload"`
}

// split turns a trip into its pending events. A missing timestamp means a
// missing event (no on_scene -> no arrived). Each field travels in the first
// event where its value is final, so nothing leaks from the future.
func split(t *Trip, tz *localClock, out []pending) ([]pending, error) {
	add := func(local *int64, kind uint8) (int64, bool, error) {
		if local == nil {
			return 0, false, nil
		}
		utc, err := tz.toUTC(*local)
		if err != nil {
			return 0, false, err
		}
		out = append(out, pending{at: utc, kind: kind, trip: t})
		return utc, true, nil
	}
	if _, _, err := add(t.RequestAt, kindRequested); err != nil {
		return out, err
	}
	if _, _, err := add(t.OnSceneAt, kindArrived); err != nil {
		return out, err
	}
	if _, _, err := add(t.PickupAt, kindPickedUp); err != nil {
		return out, err
	}
	dropUTC, ok, err := add(t.DropoffAt, kindDroppedOff)
	if err != nil {
		return out, err
	}
	if ok && t.Tips != nil && *t.Tips > 0 {
		out = append(out, pending{at: dropUTC + tipDelayMinutes(t.TripID)*60_000_000, kind: kindTipAdded, trip: t})
	}
	return out, nil
}

// tipDelayMinutes is 1-60 minutes taken from the last four hex digits of the
// trip_id (random bits), the same rule the history backfill uses. A replay
// therefore delivers every tip at the same moment each time.
func tipDelayMinutes(id [16]byte) int64 {
	return 1 + int64(uint16(id[14])<<8|uint16(id[15]))%60
}

// eventID builds a UUID v7 whose time part is the event time and whose
// "random" part is derived from the trip and the event kind. The same event
// gets the same id on every send, as if the app had stored it once.
func eventID(tripID [16]byte, kind uint8, atMicros int64) string {
	h := sha256.New()
	h.Write(tripID[:])
	h.Write([]byte{kind})
	sum := h.Sum(nil)

	var b [16]byte
	ms := uint64(atMicros / 1000)
	b[0], b[1], b[2] = byte(ms>>40), byte(ms>>32), byte(ms>>24)
	b[3], b[4], b[5] = byte(ms>>16), byte(ms>>8), byte(ms)
	copy(b[6:], sum[:10])
	b[6] = 0x70 | b[6]&0x0f // version 7
	b[8] = 0x80 | b[8]&0x3f // RFC 4122 variant
	return formatUUID(b)
}

func formatUUID(b [16]byte) string {
	s := hex.EncodeToString(b[:])
	return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32]
}

// build materialises a pending event into the message that is sent.
func build(p pending) (Event, error) {
	t := p.trip
	var payload any
	switch p.kind {
	case kindRequested:
		payload = requestedPayload{t.Provider, t.OriginatingBase, t.PickupZone, t.SharedRequest, t.WavRequest, t.AccessARide}
	case kindArrived:
		payload = arrivedPayload{t.DispatchingBase, t.WavMatch}
	case kindPickedUp:
		payload = struct{}{}
	case kindDroppedOff:
		payload = droppedOffPayload{t.DropoffZone, t.TripMiles, t.TripTime, t.BasePassengerFare, t.Tolls, t.BCF,
			t.SalesTax, t.CongestionSurcharge, t.AirportFee, t.CBDCongestionFee, t.DriverPay, t.SharedMatch}
	case kindTipAdded:
		payload = tipAddedPayload{t.Tips}
	default:
		return Event{}, fmt.Errorf("unknown event kind %d", p.kind)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	return Event{
		EventID:   eventID(t.TripID, p.kind, p.at),
		TripID:    formatUUID(t.TripID),
		EventType: kindNames[p.kind],
		EventTime: time.UnixMicro(p.at).UTC().Format("2006-01-02T15:04:05.000000Z"),
		Payload:   raw,
	}, nil
}
