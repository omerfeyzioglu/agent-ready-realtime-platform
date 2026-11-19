package live;

import java.util.Arrays;

/** Quantiles the way DuckDB's quantile_cont computes them, so live and batch agree. */
public final class Quantiles {
    private Quantiles() {}

    /** Linear interpolation between the closest ranks; values must be sorted. */
    public static double cont(long[] sorted, double q) {
        double pos = (sorted.length - 1) * q;
        int lo = (int) Math.floor(pos);
        int hi = (int) Math.ceil(pos);
        return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
    }

    public static long[] sorted(long[] values) {
        long[] copy = values.clone();
        Arrays.sort(copy);
        return copy;
    }
}
