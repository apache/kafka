package kafka.common.metrics;

import java.util.concurrent.TimeUnit;

/**
 * Configuration values for metrics
 */
public class MetricConfig {

    private Quota quota;
    private int samples;
    private long eventWindow;
    private long timeWindowNs;
    private TimeUnit unit;

    public MetricConfig() {
        super();
        this.quota = null;
        this.samples = 2;
        this.eventWindow = Long.MAX_VALUE;
        this.timeWindowNs = TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
        this.unit = TimeUnit.SECONDS;
    }

    public Quota quota() {
        return this.quota;
    }

    public MetricConfig quota(Quota quota) {
        this.quota = quota;
        return this;
    }

    public long eventWindow() {
        return eventWindow;
    }

    public MetricConfig eventWindow(long window) {
        this.eventWindow = window;
        return this;
    }

    public long timeWindowNs() {
        return timeWindowNs;
    }

    public MetricConfig timeWindow(long window, TimeUnit unit) {
        this.timeWindowNs = TimeUnit.NANOSECONDS.convert(window, unit);
        return this;
    }

    public int samples() {
        return this.samples;
    }

    public MetricConfig samples(int samples) {
        if (samples < 1)
            throw new IllegalArgumentException("The number of samples must be at least 1.");
        this.samples = samples;
        return this;
    }

    public TimeUnit timeUnit() {
        return unit;
    }

    public MetricConfig timeUnit(TimeUnit unit) {
        this.unit = unit;
        return this;
    }
}
