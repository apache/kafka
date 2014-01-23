package kafka.common.metrics.stats;

import java.util.List;
import java.util.concurrent.TimeUnit;

import kafka.common.metrics.MeasurableStat;
import kafka.common.metrics.MetricConfig;

/**
 * The rate of the given quanitity. By default this is the total observed over a set of samples from a sampled statistic
 * divided by the ellapsed time over the sample windows. Alternative {@link SampledStat} implementations can be
 * provided, however, to record the rate of occurences (e.g. the count of values measured over the time interval) or
 * other such values.
 */
public class Rate implements MeasurableStat {

    private final TimeUnit unit;
    private final SampledStat stat;

    public Rate(TimeUnit unit) {
        this(unit, new SampledTotal());
    }

    public Rate(TimeUnit unit, SampledStat stat) {
        this.stat = stat;
        this.unit = unit;
    }

    public String unitName() {
        return unit.name().substring(0, unit.name().length() - 2).toLowerCase();
    }

    @Override
    public void record(MetricConfig config, double value, long time) {
        this.stat.record(config, value, time);
    }

    @Override
    public double measure(MetricConfig config, long now) {
        double ellapsed = convert(now - stat.oldest().lastWindow);
        return stat.measure(config, now) / ellapsed;
    }

    private double convert(long time) {
        switch (unit) {
            case NANOSECONDS:
                return time;
            case MICROSECONDS:
                return time / 1000.0;
            case MILLISECONDS:
                return time / (1000.0 * 1000.0);
            case SECONDS:
                return time / (1000.0 * 1000.0 * 1000.0);
            case MINUTES:
                return time / (60.0 * 1000.0 * 1000.0 * 1000.0);
            case HOURS:
                return time / (60.0 * 60.0 * 1000.0 * 1000.0 * 1000.0);
            case DAYS:
                return time / (24.0 * 60.0 * 60.0 * 1000.0 * 1000.0 * 1000.0);
            default:
                throw new IllegalStateException("Unknown unit: " + unit);
        }
    }

    public static class SampledTotal extends SampledStat {

        public SampledTotal() {
            super(0.0d);
        }

        @Override
        protected void update(Sample sample, MetricConfig config, double value, long now) {
            sample.value += value;
        }

        @Override
        public double combine(List<Sample> samples, MetricConfig config, long now) {
            double total = 0.0;
            for (int i = 0; i < samples.size(); i++)
                total += samples.get(i).value;
            return total;
        }

    }
}
