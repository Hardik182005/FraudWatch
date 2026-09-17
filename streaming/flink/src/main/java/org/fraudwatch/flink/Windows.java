package org.fraudwatch.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** 60 s tumbling event-time window aggregation per demo session over non-late scored records. */
public final class Windows {
    private Windows() {}

    /**
     * unscored_count counts records whose policy action is "unscored" or whose scoring failed
     * (no action available).
     */
    public static class Agg implements AggregateFunction<Records.ScoredRecord, Records.WindowAgg, Records.WindowAgg> {
        private static final long serialVersionUID = 1L;

        @Override
        public Records.WindowAgg createAccumulator() {
            return new Records.WindowAgg();
        }

        @Override
        public Records.WindowAgg add(Records.ScoredRecord s, Records.WindowAgg acc) {
            acc.txnCount += 1;
            acc.amountSum += s.f.input.amount;
            if ("alert".equals(s.action)) {
                acc.alertCount += 1;
            } else if ("review".equals(s.action)) {
                acc.reviewCount += 1;
            } else if (s.action == null || "unscored".equals(s.action)) {
                acc.unscoredCount += 1;
            }
            return acc;
        }

        @Override
        public Records.WindowAgg getResult(Records.WindowAgg acc) {
            return acc;
        }

        @Override
        public Records.WindowAgg merge(Records.WindowAgg a, Records.WindowAgg b) {
            a.txnCount += b.txnCount;
            a.amountSum += b.amountSum;
            a.alertCount += b.alertCount;
            a.reviewCount += b.reviewCount;
            a.unscoredCount += b.unscoredCount;
            return a;
        }
    }

    public static class Finish extends ProcessWindowFunction<Records.WindowAgg, Records.WindowAgg, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(String session, Context ctx, Iterable<Records.WindowAgg> aggs, Collector<Records.WindowAgg> out) {
            for (Records.WindowAgg a : aggs) {
                Records.WindowAgg w = new Records.WindowAgg();
                w.sessionId = session;
                w.windowStart = ctx.window().getStart();
                w.windowEnd = ctx.window().getEnd();
                w.txnCount = a.txnCount;
                w.amountSum = a.amountSum;
                w.alertCount = a.alertCount;
                w.reviewCount = a.reviewCount;
                w.unscoredCount = a.unscoredCount;
                out.collect(w);
            }
        }
    }

    /** Counts records that arrived after their window was finalized (side output). */
    public static class LateCounter extends RichMapFunction<Records.ScoredRecord, String> {
        private static final long serialVersionUID = 1L;
        private transient Counter counter;

        @Override
        public void open(OpenContext openContext) {
            counter = getRuntimeContext().getMetricGroup().counter("window_late_records");
        }

        @Override
        public String map(Records.ScoredRecord s) {
            counter.inc();
            return s.f.input.eventId;
        }
    }
}
