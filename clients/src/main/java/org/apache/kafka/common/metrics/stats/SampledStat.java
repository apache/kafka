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

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;

import java.util.ArrayList;
import java.util.List;

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
    private int current = 0;
    protected List<Sample> samples;

    public SampledStat(double initialValue) {
        this.initialValue = initialValue;
        // keep one extra placeholder for "overlapping sample" (see purgeObsoleteSamples() logic)
        this.samples = new ArrayList<>(MetricConfig.DEFAULT_NUM_SAMPLES + 1);
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        Sample sample = current(timeMs);
        if (sample.isComplete(timeMs, config))
            sample = advance(config, timeMs);
        update(sample, config, value, timeMs);
        sample.eventCount += 1;
        sample.lastEventMs = timeMs;
    }

    private Sample advance(MetricConfig config, long timeMs) {
        // keep one extra placeholder for "overlapping sample" (see purgeObsoleteSamples() logic)
        int maxSamples = config.samples() + 1;
        this.current = (this.current + 1) % maxSamples;
        if (this.current >= samples.size()) {
            Sample sample = newSample(timeMs);
            this.samples.add(sample);
            return sample;
        } else {
            Sample sample = current(timeMs);
            sample.reset(timeMs);
            return sample;
        }
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
        if (samples.isEmpty())
            this.samples.add(newSample(timeMs));
        return this.samples.get(this.current);
    }

    public Sample oldest(long now) {
        if (samples.isEmpty())
            this.samples.add(newSample(now));
        Sample oldest = this.samples.get(0);
        for (int i = 1; i < this.samples.size(); i++) {
            Sample curr = this.samples.get(i);
            if (curr.startTimeMs < oldest.startTimeMs)
                oldest = curr;
        }
        return oldest;
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

    // purge any samples that lack observed events within the monitored window
    protected void purgeObsoleteSamples(MetricConfig config, long now) {
        long expireAge = config.samples() * config.timeWindowMs();
        for (Sample sample : samples) {
            // samples overlapping the monitored window are kept,
            // even if they started before it
            if (now - sample.lastEventMs >= expireAge) {
                sample.reset(now);
            }
        }
    }

    protected static class Sample {
        public double initialValue;
        public long eventCount;
        public long startTimeMs;
        public long lastEventMs;
        public double value;

        public Sample(double initialValue, long now) {
            this.initialValue = initialValue;
            this.eventCount = 0;
            this.startTimeMs = now;
            this.lastEventMs = now;
            this.value = initialValue;
        }

        public void reset(long now) {
            this.eventCount = 0;
            this.startTimeMs = now;
            this.lastEventMs = now;
            this.value = initialValue;
        }

        public boolean isComplete(long timeMs, MetricConfig config) {
            return timeMs - startTimeMs >= config.timeWindowMs() || eventCount >= config.eventWindow();
        }

        @Override
        public String toString() {
            return "Sample(" +
                "value=" + value +
                ", eventCount=" + eventCount +
                ", startTimeMs=" + startTimeMs +
                ", lastEventMs=" + lastEventMs +
                ", initialValue=" + initialValue +
                ')';
        }
    }

}
