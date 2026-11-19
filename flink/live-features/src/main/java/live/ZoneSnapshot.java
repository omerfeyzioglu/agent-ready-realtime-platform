package live;

/**
 * One row for Redis. kind = "window" (15m or 30m values) or "open" (state
 * at the minute). Null means "no value" (no wait measured).
 */
public class ZoneSnapshot {
    public String kind;
    public int zoneId;
    public String provider;
    public String window;          // "15m" / "30m"; window rows only
    public long windowStartMs;     // window rows only
    public long asOfMs;            // window end (exclusive) / open as-of

    public long requests;
    public long waitN;
    public Double waitP50S;
    public Double waitP90S;

    public long openRequests;
    public Long openRequestMaxAgeS;

    public ZoneSnapshot() {}
}
