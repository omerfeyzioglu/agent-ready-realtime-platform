package live;

/**
 * Everything one zone and provider looks like at one minute (event time):
 * both windows and the open requests. Computed once; Redis and Iceberg
 * both store this same record.
 */
public class ZoneMinute {
    public int zoneId;
    public String provider;
    public long asOfMs;          // the minute boundary; windows end here (exclusive)
    public long computedAtMs;    // wall clock when the record was made

    public WindowValues w15 = new WindowValues();
    public WindowValues w30 = new WindowValues();

    public long openRequests;
    public Long openRequestMaxAgeS;   // null when nothing is open

    public ZoneMinute() {}

    /** Values over [asOf - minutes, asOf). */
    public static class WindowValues {
        public long requests;
        public long waitN;
        public Double waitP50S;   // null when no wait was measured
        public Double waitP90S;

        public WindowValues() {}

        public boolean isLowSample() {
            return waitN < 10;
        }
    }
}
