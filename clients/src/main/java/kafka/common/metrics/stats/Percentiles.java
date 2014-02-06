package kafka.common.metrics.stats;

import java.util.ArrayList;
import java.util.List;

import kafka.common.metrics.CompoundStat;
import kafka.common.metrics.Measurable;
import kafka.common.metrics.MetricConfig;
import kafka.common.metrics.stats.Histogram.BinScheme;
import kafka.common.metrics.stats.Histogram.ConstantBinScheme;
import kafka.common.metrics.stats.Histogram.LinearBinScheme;

/**
 * A compound stat that reports one or more percentiles
 */
public class Percentiles implements CompoundStat {

    public static enum BucketSizing {
        CONSTANT, LINEAR
    }

    private final Percentile[] percentiles;
    private Histogram current;
    private Histogram shadow;
    private long lastWindow;
    private long eventCount;

    public Percentiles(int sizeInBytes, double max, BucketSizing bucketing, Percentile... percentiles) {
        this(sizeInBytes, 0.0, max, bucketing, percentiles);
    }

    public Percentiles(int sizeInBytes, double min, double max, BucketSizing bucketing, Percentile... percentiles) {
        this.percentiles = percentiles;
        BinScheme scheme = null;
        if (bucketing == BucketSizing.CONSTANT) {
            scheme = new ConstantBinScheme(sizeInBytes / 4, min, max);
        } else if (bucketing == BucketSizing.LINEAR) {
            if (min != 0.0d)
                throw new IllegalArgumentException("Linear bucket sizing requires min to be 0.0.");
            scheme = new LinearBinScheme(sizeInBytes / 4, max);
        }
        this.current = new Histogram(scheme);
        this.shadow = new Histogram(scheme);
        this.eventCount = 0L;
    }

    @Override
    public List<NamedMeasurable> stats() {
        List<NamedMeasurable> ms = new ArrayList<NamedMeasurable>(this.percentiles.length);
        for (Percentile percentile : this.percentiles) {
            final double pct = percentile.percentile();
            ms.add(new NamedMeasurable(percentile.name(), percentile.description(), new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return current.value(pct / 100.0);
                }
            }));
        }
        return ms;
    }

    @Override
    public void record(MetricConfig config, double value, long time) {
        long ellapsed = time - this.lastWindow;
        if (ellapsed > config.timeWindowNs() / 2 || this.eventCount > config.eventWindow() / 2)
            this.shadow.clear();
        if (ellapsed > config.timeWindowNs() || this.eventCount > config.eventWindow()) {
            Histogram tmp = this.current;
            this.current = this.shadow;
            this.shadow = tmp;
            this.shadow.clear();
        }
        this.current.record(value);
        this.shadow.record(value);
    }

}
