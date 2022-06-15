/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.metrics.stats;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;

/**
 * A SampledStat records a single scalar value measured over one or more samples. Each sample is recorded over a
 * configurable window. The window can be defined by number of events or elapsed time (or both, if both are given the
 * window is complete when <i>either</i> the event count or elapsed time criterion is met).
 * <p>
 * All the samples are combined to produce the measurement. When a window is complete the oldest sample is cleared and
 * recycled to begin recording the next sample.
 * 
 * Subclasses of this class define different statistics measured using this basic pattern.
 */
public abstract class SampledStat implements MeasurableStat {

    private final double initialValue;
    /**
     * Index of the latest stored sample.
     */
    private int current = 0;
    /**
     * Stores the recorded samples. Older samples are overwritten using {@link Sample#reset(long)}
     */
    protected List<Sample> samples;

    public SampledStat(double initialValue) {
        this.initialValue = initialValue;
        this.samples = new ArrayList<>(2);
    }

    /**
     * {@inheritDoc}
     *
     * On every record, do the following:
     * 1. Check if the current window has expired
     * 2. If yes, then advance the current pointer to new window. The start time of the new window is set to nearest
     *    possible starting point for the new window. The nearest starting point occurs at config.timeWindowMs intervals
     *    from the end time of last known window.
     * 3. Update the recorded value for the current window
     * 4. Increase the number of event count
     */
    @Override
    public void record(MetricConfig config, double value, long recordingTimeMs) {
        Sample sample = current(recordingTimeMs);
        if (sample.isComplete(recordingTimeMs, config)) {
            final long previousWindowStartTime = sample.lastWindowMs;
            final long previousWindowEndTime = previousWindowStartTime + config.timeWindowMs();
            final long startTimeOfNewWindow = recordingTimeMs - ((recordingTimeMs - previousWindowEndTime) % config.timeWindowMs());
            sample = advance(config, startTimeOfNewWindow);
        }
        update(sample, config, value, recordingTimeMs);
        sample.eventCount++;
    }

    private Sample advance(MetricConfig config, long timeMs) {
        this.current = (this.current + 1) % config.samples();
        Sample sample;
        if (this.current >= samples.size()) {
            sample = newSample(timeMs);
            this.samples.add(sample);
        } else {
            sample = current(timeMs);
            sample.reset(timeMs);
        }
        return sample;
    }

    protected Sample newSample(long timeMs) {
        return new Sample(this.initialValue, timeMs);
    }

    @Override
    public double measure(MetricConfig config, long now) {
        purgeObsoleteSamples(config, now);
        return combine(this.samples, config, now);
    }

    public Sample current(long timeMs) {
        if (samples.size() == 0)
            this.samples.add(newSample(timeMs));
        return this.samples.get(this.current);
    }

    public Sample oldest(long now) {
        if (samples.size() == 0)
            this.samples.add(newSample(now));
        return samples.stream().min(Comparator.comparingLong(s -> s.lastWindowMs)).orElse(samples.get(0));
    }

    @Override
    public String toString() {
        return "SampledStat(" +
            "initialValue=" + initialValue +
            ", current=" + current +
            ", samples=" + samples +
            ')';
    }

    protected abstract void update(Sample sample, MetricConfig config, double value, long timeMs);

    public abstract double combine(List<Sample> samples, MetricConfig config, long now);

    /* Timeout any windows that have expired in the absence of any events */
    protected void purgeObsoleteSamples(MetricConfig config, long now) {
        long expireAge = config.samples() * config.timeWindowMs();
        for (Sample sample : samples) {
            if (now - sample.lastWindowMs >= expireAge)
                sample.reset(now);
        }
    }

    protected static class Sample {
        public double initialValue;
        public long eventCount;
        public long lastWindowMs;
        public double value;

        public Sample(double initialValue, long now) {
            this.initialValue = initialValue;
            this.eventCount = 0;
            this.lastWindowMs = now;
            this.value = initialValue;
        }

        public void reset(long now) {
            this.eventCount = 0;
            this.lastWindowMs = now;
            this.value = initialValue;
        }

        public boolean isComplete(long timeMs, MetricConfig config) {
            return timeMs - lastWindowMs >= config.timeWindowMs() || eventCount >= config.eventWindow();
        }

        @Override
        public String toString() {
            return "Sample(" +
                "value=" + value +
                ", eventCount=" + eventCount +
                ", lastWindowMs=" + lastWindowMs +
                ", initialValue=" + initialValue +
                ')';
        }
    }

}
