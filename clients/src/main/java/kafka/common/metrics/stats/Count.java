package kafka.common.metrics.stats;

import java.util.List;

import kafka.common.metrics.MetricConfig;

/**
 * A {@link SampledStat} that maintains a simple count of what it has seen.
 */
public class Count extends SampledStat {

    public Count() {
        super(0);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value += 1.0;
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        double total = 0.0;
        for (int i = 0; i < samples.size(); i++)
            total += samples.get(i).value;
        return total;
    }

}
