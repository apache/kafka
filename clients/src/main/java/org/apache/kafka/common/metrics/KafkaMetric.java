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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.SuppressKafkaInternalApiUsage;
import org.apache.kafka.common.utils.Time;

import java.util.Objects;
import java.util.function.BooleanSupplier;

@InterfaceAudience.Public
public final class KafkaMetric implements Metric {

    private final MetricName metricName;
    private final Object lock;
    private final Time time;
    private final MetricValueProvider<?> metricValueProvider;
    private volatile MetricConfig config;
    private final BooleanSupplier recordingEnabled;
    // public for testing
    /**
     * Create a metric to monitor an object that implements MetricValueProvider.
     * @param lock The lock used to prevent race condition
     * @param metricName The name of the metric
     * @param valueProvider The metric value provider associated with this metric
     * @param config The configuration of the metric
     * @param time The time instance to use with the metrics
     */
    @SuppressKafkaInternalApiUsage("KIP-1265: ctor leaks internal Time for test injection — pending KIP review to promote Time or refactor")
    public KafkaMetric(Object lock, MetricName metricName, MetricValueProvider<?> valueProvider,
                       MetricConfig config, Time time) {
        this(lock, metricName, valueProvider, config, time, null);
    }

    /**
     * Create a metric with a supplier for recording eligibility.
     * @param lock The lock used to prevent race condition
     * @param metricName The name of the metric
     * @param valueProvider The metric value provider associated with this metric
     * @param config The configuration of the metric
     * @param time The time instance to use with the metrics
     * @param recordingEnabled supplies reference to determine if the metric is enabled or not
     */
    KafkaMetric(Object lock, MetricName metricName, MetricValueProvider<?> valueProvider,
                MetricConfig config, Time time, BooleanSupplier recordingEnabled) {
        this.metricName = metricName;
        this.lock = lock;
        this.metricValueProvider = Objects.requireNonNull(valueProvider, "valueProvider must not be null");
        this.config = config;
        this.time = time;
        this.recordingEnabled = recordingEnabled != null ? recordingEnabled : () -> true;
    }

    /**
     * Get the configuration of this metric.
     * This is supposed to be used by server only.
     * @return Return the config of this metric
     */
    public MetricConfig config() {
        return this.config;
    }

    /**
     * Get the metric name
     * @return Return the name of this metric
     */
    @Override
    public MetricName metricName() {
        return this.metricName;
    }

    /**
     * Take the metric and return the value via {@link MetricValueProvider#value(MetricConfig, long)}.
     *
     * @return Return the metric value
     */
    @Override
    public Object metricValue() {
        long now = time.milliseconds();
        synchronized (this.lock) {
            return metricValueProvider.value(config, now);
        }
    }

    /**
     * Determine whether recording is enabled by the owning sensor, or true for standalone metrics.
     * @return true if the metric is active, false otherwise
     */
    private boolean isActive() {
        return recordingEnabled.getAsBoolean();
    }

    /**
     * The method determines if the metric value provider is of type Measurable.
     *
     * @return true if the metric value provider is of type Measurable, false otherwise.
     */
    public boolean isMeasurable() {
        return this.metricValueProvider instanceof Measurable;
    }

    /**
     * Get the underlying metric provider, which should be a {@link Measurable}
     * @return Return the metric provider
     * @throws IllegalStateException if the underlying metric is not a {@link Measurable}.
     */
    public Measurable measurable() {
        if (isMeasurable())
            return (Measurable) metricValueProvider;
        else
            throw new IllegalStateException("Not a measurable: " + this.metricValueProvider.getClass());
    }

    /**
     * Take the metric and return the value, where the underlying metric provider should be a {@link Measurable}
     * @param timeMs The time that this metric is taken
     * @return Return the metric value if it's measurable, otherwise 0
     */
    double measurableValue(long timeMs) {
        synchronized (this.lock) {
            if (isMeasurable())
                return ((Measurable) metricValueProvider).measure(config, timeMs);
            else
                return 0;
        }
    }

    /**
     * Set the metric config.
     * This is supposed to be used by server only.
     * @param config configuration for this metrics
     */
    public void config(MetricConfig config) {
        synchronized (lock) {
            this.config = config;
        }
    }

    /**
     * Internal access to KafkaMetric for use by the metrics system. This is not intended for use by end users.
     */
    @InterfaceAudience.Private
    public static final class InternalAccess {
        private InternalAccess() {
        }

        /**
         * Returns whether the metrics recording level is enabled
         * @param metric the metric to check
         * @return true if the metric is enabled, false otherwise
         */
        public static boolean isEnabled(KafkaMetric metric) {
            return metric.isActive();
        }
    }

    /**
     * Returns a human-readable representation of this metric.
     *
     * <p>The metricValueProvider is represented by its class name rather than its full
     * toString() to avoid dumping internal stat state (e.g. SampledStat's samples list) into
     * logs, which could be verbose and is rarely useful for identifying which metric changed.
     */
    @Override
    public String toString() {
        Class<?> cls = metricValueProvider.getClass();
        if (cls.isSynthetic() || cls.isAnonymousClass()) {
            return "KafkaMetric [metricName=" + metricName + "]";
        }

        return "KafkaMetric [" +
                "metricName=" + metricName +
                ", metricValueProvider=" + metricValueProvider.getClass().getName() +
                ']';
    }
}
