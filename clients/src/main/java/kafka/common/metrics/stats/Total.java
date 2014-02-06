package kafka.common.metrics.stats;

import kafka.common.metrics.MeasurableStat;
import kafka.common.metrics.MetricConfig;

/**
 * An un-windowed cumulative total maintained over all time.
 */
public class Total implements MeasurableStat {

    private double total;

    public Total() {
        this.total = 0.0;
    }

    public Total(double value) {
        this.total = value;
    }

    @Override
    public void record(MetricConfig config, double value, long time) {
        this.total += value;
    }

    @Override
    public double measure(MetricConfig config, long now) {
        return this.total;
    }

}
