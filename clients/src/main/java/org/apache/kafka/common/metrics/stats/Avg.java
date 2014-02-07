package org.apache.kafka.common.metrics.stats;

import java.util.List;

import org.apache.kafka.common.metrics.MetricConfig;


/**
 * A {@link SampledStat} that maintains a simple average over its samples.
 */
public class Avg extends SampledStat {

    public Avg() {
        super(0.0);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value += value;
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        double total = 0.0;
        long count = 0;
        for (int i = 0; i < samples.size(); i++) {
            Sample s = samples.get(i);
            total += s.value;
            count += s.eventCount;
        }
        return total / count;
    }

}
