package org.apache.kafka.common.metrics.stats;

import java.util.List;

import org.apache.kafka.common.metrics.MetricConfig;


/**
 * A {@link SampledStat} that gives the min over its samples.
 */
public class Min extends SampledStat {

    public Min() {
        super(Double.MIN_VALUE);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value = Math.min(sample.value, value);
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        double max = Double.MAX_VALUE;
        for (int i = 0; i < samples.size(); i++)
            max = Math.min(max, samples.get(i).value);
        return max;
    }

}
