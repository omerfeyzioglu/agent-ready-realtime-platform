package live;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stage 1, keyed by trip_id: pairs a trip's later events with its request.
 *
 * The `arrived` event has no zone or provider, so the request is kept in
 * state until the trip is closed. Kafka is keyed by trip_id, so a trip's
 * events arrive in order on one partition.
 */
public class TripMatcher extends KeyedProcessFunction<String, TripEvent, Signal> {

    private static final byte OPEN = 1;
    private static final byte CLOSED = 2;
    private static final byte SKIPPED = 3;

    /** State is dropped this long after the request; every real trip is closed well before. */
    private static final long KEEP_MS = 3 * 3600_000L;

    private transient ValueState<Integer> zone;
    private transient ValueState<String> provider;
    private transient ValueState<Long> requestedAt;
    private transient ValueState<Byte> status;

    @Override
    public void open(OpenContext ctx) {
        zone = getRuntimeContext().getState(new ValueStateDescriptor<>("zone", Integer.class));
        provider = getRuntimeContext().getState(new ValueStateDescriptor<>("provider", String.class));
        requestedAt = getRuntimeContext().getState(new ValueStateDescriptor<>("requested_at", Long.class));
        status = getRuntimeContext().getState(new ValueStateDescriptor<>("status", Byte.class));
    }

    @Override
    public void processElement(TripEvent e, Context ctx, Collector<Signal> out) throws Exception {
        Byte st = status.value();
        switch (e.eventType) {
            case "requested" -> {
                if (st != null) {
                    return; // a resent duplicate
                }
                // Same exclusions as batch: unknown and outside-NYC zones.
                if (e.provider == null || e.pickupZone < 1 || e.pickupZone > 263) {
                    status.update(SKIPPED);
                } else {
                    zone.update(e.pickupZone);
                    provider.update(e.provider);
                    requestedAt.update(e.eventTimeMs);
                    status.update(OPEN);
                    emit(out, Signal.REQUEST, e, e.eventTimeMs, 0);
                }
                ctx.timerService().registerEventTimeTimer(e.eventTimeMs + KEEP_MS);
            }
            case "arrived" -> {
                if (st == null) {
                    // Arrival before its own request: out of order, as staging flags it.
                    status.update(SKIPPED);
                    ctx.timerService().registerEventTimeTimer(e.eventTimeMs + KEEP_MS);
                } else if (st == OPEN) {
                    long waitMs = e.eventTimeMs - requestedAt.value();
                    if (waitMs >= 0) {
                        emit(out, Signal.WAIT, e, e.eventTimeMs, waitMs / 1000);
                    }
                    emit(out, Signal.CLOSE, e, e.eventTimeMs, 0);
                    status.update(CLOSED);
                }
            }
            case "picked_up" -> {
                if (st == null) {
                    status.update(SKIPPED);
                    ctx.timerService().registerEventTimeTimer(e.eventTimeMs + KEEP_MS);
                } else if (st == OPEN) {
                    // No arrival event (missing on-scene time): the request ends here.
                    emit(out, Signal.CLOSE, e, e.eventTimeMs, 0);
                    status.update(CLOSED);
                }
            }
            default -> { } // dropped_off, tip_added: not used by these features
        }
    }

    private void emit(Collector<Signal> out, int kind, TripEvent e, long at, long waitS) throws Exception {
        int z = zone.value();
        out.collect(new Signal(kind, z, provider.value(), e.tripId, at, waitS));
        out.collect(new Signal(kind, z, "all", e.tripId, at, waitS));
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Signal> out) {
        zone.clear();
        provider.clear();
        requestedAt.clear();
        status.clear();
    }
}
