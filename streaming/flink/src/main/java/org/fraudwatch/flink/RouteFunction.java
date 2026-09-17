package org.fraudwatch.flink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Runs after timestamps/watermarks are assigned in the source, so heartbeats and txns for other
 * engines still advance the watermark. Invalid records go to the DLQ side output, heartbeats are
 * dropped, txns whose target_engines does not contain "flink" are dropped.
 */
public class RouteFunction extends ProcessFunction<Records.InputRecord, Records.InputRecord> {
    private static final long serialVersionUID = 1L;
    public static final OutputTag<Records.InputRecord> INVALID = new OutputTag<Records.InputRecord>("invalid") {
        private static final long serialVersionUID = 1L;
    };

    private transient Counter valid;
    private transient Counter invalid;
    private transient Counter heartbeats;
    private transient Counter notTargeted;

    @Override
    public void open(OpenContext openContext) {
        valid = getRuntimeContext().getMetricGroup().counter("events_valid");
        invalid = getRuntimeContext().getMetricGroup().counter("events_invalid");
        heartbeats = getRuntimeContext().getMetricGroup().counter("heartbeats");
        notTargeted = getRuntimeContext().getMetricGroup().counter("events_not_targeted");
    }

    @Override
    public void processElement(Records.InputRecord r, Context ctx, Collector<Records.InputRecord> out) {
        if (Records.KIND_INVALID.equals(r.kind)) {
            invalid.inc();
            ctx.output(INVALID, r);
        } else if (Records.KIND_HEARTBEAT.equals(r.kind)) {
            heartbeats.inc();
        } else {
            valid.inc();
            if (r.targetsFlink) {
                out.collect(r);
            } else {
                notTargeted.inc();
            }
        }
    }
}
