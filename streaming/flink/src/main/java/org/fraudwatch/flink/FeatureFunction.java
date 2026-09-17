package org.fraudwatch.flink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Keyed by demo_session_id:account_id. Late = event_time &lt; this operator's current watermark.
 * Late events are scored against a read-only view of state; on-time events update it.
 */
public class FeatureFunction extends KeyedProcessFunction<String, Records.InputRecord, Records.FeatureRecord> {
    private static final long serialVersionUID = 1L;

    private final long stateTtlHours;
    private transient ValueState<Features.AccountState> state;
    private transient Counter lateCounter;
    private transient Counter processed;

    public FeatureFunction(long stateTtlHours) {
        this.stateTtlHours = stateTtlHours;
    }

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Features.AccountState> desc =
                new ValueStateDescriptor<>("account-state", Features.AccountState.class);
        if (stateTtlHours > 0) {
            desc.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofHours(stateTtlHours))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build());
        }
        state = getRuntimeContext().getState(desc);
        lateCounter = getRuntimeContext().getMetricGroup().counter("events_late");
        processed = getRuntimeContext().getMetricGroup().counter("events_featurized");
    }

    @Override
    public void processElement(Records.InputRecord in, Context ctx, Collector<Records.FeatureRecord> out) throws Exception {
        long wm = ctx.timerService().currentWatermark();
        boolean late = wm != Long.MIN_VALUE && in.eventTime < wm;
        Features.AccountState st = state.value();
        if (st == null) {
            st = new Features.AccountState();
        }
        Features.RawFeatures raw = Features.computeRaw(st, in.eventTime, in.amountInr);
        if (late) {
            lateCounter.inc();
        } else {
            Features.update(st, in.eventTime, in.amountInr);
            state.update(st);
        }
        processed.inc();
        Records.FeatureRecord fr = new Records.FeatureRecord();
        fr.input = in;
        fr.late = late;
        fr.watermarkMs = wm == Long.MIN_VALUE ? null : wm;
        fr.raw = raw;
        out.collect(fr);
    }
}
