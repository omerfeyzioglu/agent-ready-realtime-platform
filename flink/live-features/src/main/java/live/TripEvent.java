package live;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;

/**
 * The fields of a trip event this job needs. Only `requested` carries the
 * pickup zone and the provider; the other events are matched to it by
 * trip_id.
 */
public class TripEvent {
    public String tripId;
    public String eventType;
    public long eventTimeMs;
    public int pickupZone;   // requested only
    public String provider;  // requested only: uber, lyft or null

    public TripEvent() {}

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Returns null for a record that is not a usable event (the raw writer sends those to the DLQ). */
    public static TripEvent parse(String value) {
        try {
            JsonNode n = JSON.readTree(value);
            TripEvent e = new TripEvent();
            e.tripId = n.path("trip_id").asText(null);
            e.eventType = n.path("event_type").asText(null);
            String t = n.path("event_time").asText(null);
            if (e.tripId == null || e.eventType == null || t == null) {
                return null;
            }
            e.eventTimeMs = Instant.parse(t).toEpochMilli();
            if ("requested".equals(e.eventType)) {
                JsonNode p = n.path("payload");
                e.pickupZone = p.path("pickup_zone").asInt(0);
                switch (p.path("provider").asText("")) {
                    case "HV0003" -> e.provider = "uber";
                    case "HV0005" -> e.provider = "lyft";
                    default -> e.provider = null;
                }
            }
            return e;
        } catch (Exception ex) {
            return null;
        }
    }
}
