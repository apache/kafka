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
    private int current = 0;

    protected List<Sample> samples;

    public SampledStat(double initialValue) {
        this.initialValue = initialValue;
        this.samples = new ArrayList<>(2);
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        Sample sample = current(timeMs);
        if (sample.isComplete(timeMs, config))
            sample = advance(config, timeMs);
        update(sample, config, value, timeMs);
        sample.setEventCount(sample.getEventCount() + 1);
    }

    private Sample advance(MetricConfig config, long timeMs) {
        this.current = (this.current + 1) % config.samples();
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
        if (samples.size() == 0)
            this.samples.add(newSample(timeMs));
        return this.samples.get(this.current);
    }

    /**
     * Determines if the latest sample belongs to the first ever window.
     *
     * This scenario is true iff all of the following are true:
     * 1. There is only 1 sample available. Since each window produces one sample, presence of more than one sample
     *    means that other windows have been processed, hence, current cannot be the first window.
     * 2. The current sample is active i.e. it has not been reset.
     * 3. The current sample has not been reused by another window.
     */
    public boolean isCurrentSampleInFirstWindow() {
        return this.samples.size() == 1 && // should have exactly one sample
                this.samples.get(0).isActive() && // sample should be active
                !this.samples.get(0).isObjectReused(); // sample should never have been reused
    }

    public Sample oldest(long now) {
        if (samples.size() == 0)
            this.samples.add(newSample(now));
        Sample oldest = this.samples.get(0);
        for (int i = 1; i < this.samples.size(); i++) {
            Sample curr = this.samples.get(i);
            if (curr.getLastWindowMs() < oldest.getLastWindowMs())
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

    /* Timeout any windows that have expired in the absence of any events */
    protected void purgeObsoleteSamples(MetricConfig config, long now) {
        long expireAge = config.samples() * config.timeWindowMs();
        for (Sample sample : samples) {
            if (now - sample.getLastWindowMs() >= expireAge)
                sample.reset(now);
        }
    }

    protected static class Sample {
        private double initialValue;
        private long eventCount;
        private long lastWindowMs;
        private double value;
        private boolean isReused = false;

        /**
         * A Sample object could be re-used to store future samples for space efficiency. Thus, a sample could be in
         * either of the following lifecycle states:
         * NOT_INITIALIZED: Sample has not been initialized.
         * ACTIVE: Sample has values and is currently
         * RESET: Sample has been reset and the object is not destroyed so that it could be used for storing future
         *        samples.
         */
        private enum LifecycleState {
            NOT_INITIALIZED, ACTIVE, RESET
        }
        private LifecycleState currentLifecycleState;

        public Sample(double initialValue, long now) {
            this.initialValue = initialValue;
            this.eventCount = 0;
            this.lastWindowMs = now;
            this.value = initialValue;
            this.currentLifecycleState = LifecycleState.ACTIVE;
        }

        public void reset(long now) {
            this.currentLifecycleState = LifecycleState.RESET;
            this.eventCount = 0;
            this.lastWindowMs = now;
            this.isReused = true;
            this.value = initialValue;
        }

        /**
         * Has this sample object ever been re-used
         */
        public boolean isObjectReused() {
            return isReused;
        }

        public boolean isComplete(long timeMs, MetricConfig config) {
            return timeMs - lastWindowMs >= config.timeWindowMs() || eventCount >= config.eventWindow();
        }

        public boolean isActive() {
            return currentLifecycleState == LifecycleState.ACTIVE;
        }

        public double getInitialValue() {
            return initialValue;
        }

        public void setInitialValue(final double initialValue) {
            this.currentLifecycleState = LifecycleState.ACTIVE;
            this.initialValue = initialValue;
        }

        public long getEventCount() {
            return eventCount;
        }

        public void setEventCount(final long eventCount) {
            this.currentLifecycleState = LifecycleState.ACTIVE;
            this.eventCount = eventCount;
        }

        public long getLastWindowMs() {
            return lastWindowMs;
        }

        public void setLastWindowMs(final long lastWindowMs) {
            this.currentLifecycleState = LifecycleState.ACTIVE;
            this.lastWindowMs = lastWindowMs;
        }

        public double getValue() {
            return value;
        }

        public void setValue(final double value) {
            this.currentLifecycleState = LifecycleState.ACTIVE;
            this.value = value;
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
