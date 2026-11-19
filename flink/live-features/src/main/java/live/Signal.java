package live;

/**
 * What one trip contributes to its zone: a request was made, a wait was
 * measured, or an open request was closed. Emitted once for the trip's
 * provider and once more for "all".
 */
public class Signal {
    public static final int REQUEST = 1;
    public static final int WAIT = 2;
    public static final int CLOSE = 3;

    public int kind;
    public int zoneId;
    public String provider;
    public String tripId;
    public long atMs;     // event time the signal belongs to
    public long waitS;    // WAIT only

    public Signal() {}

    public Signal(int kind, int zoneId, String provider, String tripId, long atMs, long waitS) {
        this.kind = kind;
        this.zoneId = zoneId;
        this.provider = provider;
        this.tripId = tripId;
        this.atMs = atMs;
        this.waitS = waitS;
    }

    public String zoneKey() {
        return zoneId + "|" + provider;
    }
}
