package live;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.nio.charset.StandardCharsets;

/** Kafka value -> TripEvent; unusable records become null and are skipped. */
public class TripEventSchema extends AbstractDeserializationSchema<TripEvent> {
    @Override
    public TripEvent deserialize(byte[] message) {
        return TripEvent.parse(new String(message, StandardCharsets.UTF_8));
    }
}
