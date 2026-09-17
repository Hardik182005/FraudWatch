package org.fraudwatch.flink;

/**
 * Faithful Java port of {@code packages/fwcore/fwcore/features.py} (feature spec fs-1.0).
 *
 * <p>Must produce bit-identical raw features to the Python reference:
 * entries are iterated in insertion order, the running mean uses {@code mean += (x - mean) / n},
 * pruning keeps entries with {@code ts >= max_ts - 1h} and at most {@link #MAX_ENTRIES} (the newest),
 * and {@code secs_since_last} is measured from the maximum processed event time.
 * Verified by {@code FeatureParityTest} against fixtures generated with fwcore.
 */
public final class Features {
    public static final long W_60S = 60_000L;
    public static final long W_10M = 600_000L;
    public static final long W_1H = 3_600_000L;
    public static final double SMALL_AMOUNT_INR = 200.0;
    public static final int MAX_ENTRIES = 500;

    private Features() {}

    /** Per-key state. Flink POJO (public no-arg constructor, public fields). */
    public static class AccountState {
        public long[] entryTs = new long[0];
        public double[] entryAmt = new double[0];
        public long totalCount = 0L;
        public double amountMean = 0.0;
        public long maxTs = -1L;

        public AccountState() {}
    }

    /** Raw (untransformed) features. Nullable fields mirror Python {@code None}. */
    public static class RawFeatures {
        public double amountInr;
        public long priorCount60s;
        public long priorCount10m;
        public long priorCount1h;
        public double priorAmountSum1h;
        public long smallCount10m;
        public Double secsSinceLast;
        public long priorCountTotal;
        public Double priorMeanAmount;
        public Double amountRatio;

        public RawFeatures() {}
    }

    /** Port of {@code compute_raw}. Does not mutate state. */
    public static RawFeatures computeRaw(AccountState state, long eventTime, double amountInr) {
        long c60 = 0, c10m = 0, c1h = 0, small10m = 0;
        double sum1h = 0.0;
        final int n = state.entryTs.length;
        for (int i = 0; i < n; i++) {
            long ts = state.entryTs[i];
            double amt = state.entryAmt[i];
            if (ts > eventTime) {
                continue;
            }
            long age = eventTime - ts;
            if (age <= W_1H) {
                c1h += 1;
                sum1h += amt;
                if (age <= W_10M) {
                    c10m += 1;
                    if (amt < SMALL_AMOUNT_INR) {
                        small10m += 1;
                    }
                    if (age <= W_60S) {
                        c60 += 1;
                    }
                }
            }
        }
        RawFeatures r = new RawFeatures();
        r.amountInr = amountInr;
        r.priorCount60s = c60;
        r.priorCount10m = c10m;
        r.priorCount1h = c1h;
        r.priorAmountSum1h = sum1h;
        r.smallCount10m = small10m;
        r.secsSinceLast = state.maxTs < 0 ? null : ((double) Math.max(0L, eventTime - state.maxTs)) / 1000.0;
        r.priorCountTotal = state.totalCount;
        r.priorMeanAmount = state.totalCount != 0 ? state.amountMean : null;
        r.amountRatio = (state.totalCount >= 1 && state.amountMean > 0) ? amountInr / state.amountMean : null;
        return r;
    }

    /** Port of {@code update}: apply a non-late event to state (mutates in place). */
    public static void update(AccountState state, long eventTime, double amountInr) {
        int n = state.entryTs.length;
        long[] ts = new long[n + 1];
        double[] amt = new double[n + 1];
        System.arraycopy(state.entryTs, 0, ts, 0, n);
        System.arraycopy(state.entryAmt, 0, amt, 0, n);
        ts[n] = eventTime;
        amt[n] = amountInr;
        state.entryTs = ts;
        state.entryAmt = amt;
        state.totalCount += 1;
        state.amountMean += (amountInr - state.amountMean) / (double) state.totalCount;
        if (eventTime > state.maxTs) {
            state.maxTs = eventTime;
        }
        long cutoff = state.maxTs - W_1H;
        int len = ts.length;
        if (len > 0 && (ts[0] < cutoff || len > MAX_ENTRIES)) {
            int kept = 0;
            for (int i = 0; i < len; i++) {
                if (ts[i] >= cutoff) {
                    kept++;
                }
            }
            long[] kts = new long[kept];
            double[] kamt = new double[kept];
            int j = 0;
            for (int i = 0; i < len; i++) {
                if (ts[i] >= cutoff) {
                    kts[j] = ts[i];
                    kamt[j] = amt[i];
                    j++;
                }
            }
            if (kept > MAX_ENTRIES) {
                int from = kept - MAX_ENTRIES;
                long[] t2 = new long[MAX_ENTRIES];
                double[] a2 = new double[MAX_ENTRIES];
                System.arraycopy(kts, from, t2, 0, MAX_ENTRIES);
                System.arraycopy(kamt, from, a2, 0, MAX_ENTRIES);
                kts = t2;
                kamt = a2;
            }
            state.entryTs = kts;
            state.entryAmt = kamt;
        }
    }
}
