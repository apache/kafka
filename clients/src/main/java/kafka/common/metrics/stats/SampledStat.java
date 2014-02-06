package kafka.common.metrics.stats;

import java.util.ArrayList;
import java.util.List;

import kafka.common.metrics.MeasurableStat;
import kafka.common.metrics.MetricConfig;

/**
 * A SampledStat records a single scalar value measured over one or more samples. Each sample is recorded over a
 * configurable window. The window can be defined by number of events or ellapsed time (or both, if both are given the
 * window is complete when <i>either</i> the event count or ellapsed time criterion is met).
 * <p>
 * All the samples are combined to produce the measurement. When a window is complete the oldest sample is cleared and
 * recycled to begin recording the next sample.
 * 
 * Subclasses of this class define different statistics measured using this basic pattern.
 */
public abstract class SampledStat implements MeasurableStat {

    private double initialValue;
    private int current = 0;
    protected List<Sample> samples;

    public SampledStat(double initialValue) {
        this.initialValue = initialValue;
        this.samples = new ArrayList<Sample>(2);
    }

    @Override
    public void record(MetricConfig config, double value, long now) {
        Sample sample = current(now);
        if (sample.isComplete(now, config))
            sample = advance(config, now);
        update(sample, config, value, now);
        sample.eventCount += 1;
    }

    private Sample advance(MetricConfig config, long now) {
        this.current = (this.current + 1) % config.samples();
        if (this.current >= samples.size()) {
            Sample sample = newSample(now);
            this.samples.add(sample);
            return sample;
        } else {
            Sample sample = current(now);
            sample.reset(now);
            return sample;
        }
    }

    protected Sample newSample(long now) {
        return new Sample(this.initialValue, now);
    }

    @Override
    public double measure(MetricConfig config, long now) {
        timeoutObsoleteSamples(config, now);
        return combine(this.samples, config, now);
    }

    public Sample current(long now) {
        if (samples.size() == 0)
            this.samples.add(newSample(now));
        return this.samples.get(this.current);
    }

    public Sample oldest() {
        return this.samples.get((this.current + 1) % this.samples.size());
    }

    protected abstract void update(Sample sample, MetricConfig config, double value, long now);

    public abstract double combine(List<Sample> samples, MetricConfig config, long now);

    /* Timeout any windows that have expired in the absense of any events */
    protected void timeoutObsoleteSamples(MetricConfig config, long now) {
        for (int i = 0; i < samples.size(); i++) {
            int idx = (this.current + i) % samples.size();
            Sample sample = this.samples.get(idx);
            if (now - sample.lastWindow >= (i + 1) * config.timeWindowNs())
                sample.reset(now);
        }
    }

    protected static class Sample {
        public double initialValue;
        public long eventCount;
        public long lastWindow;
        public double value;

        public Sample(double initialValue, long now) {
            this.initialValue = initialValue;
            this.eventCount = 0;
            this.lastWindow = now;
            this.value = initialValue;
        }

        public void reset(long now) {
            this.eventCount = 0;
            this.lastWindow = now;
            this.value = initialValue;
        }

        public boolean isComplete(long now, MetricConfig config) {
            return now - lastWindow >= config.timeWindowNs() || eventCount >= config.eventWindow();
        }
    }

}
