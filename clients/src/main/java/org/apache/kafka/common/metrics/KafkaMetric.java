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
import org.apache.kafka.common.utils.Time;

/**
 * An implementation of {@link Metric} interface.
 * <p>
 * A KafkaMetric is a named metric for monitoring purpose. The metric value can be a {@link Measurable} or a {@link Gauge}.
 * <pre>
 * <b>metricName</b> The name of the metric
 * <b>lock</b> A lock used for reading the metric value in case of race condition
 * <b>time</b> The POSIX time in milliseconds the metric is being taken
 * <b>metricValueProvider</b> The metric collecting implementation that implements {@link MetricValueProvider}
 * <b>config</b> The metric configuration which is a {@link MetricConfig}
 * </pre>
 * <p>
 * Usage looks something like this:
 *
 * <pre>{@code
 * // set up metrics:
 *
 * Map<String, String> tags = new HashMap<>();
 * tags.put("key1", "value1");
 *
 * MetricConfig config = new MetricConfig().tags(metricTags);
 * Time time = new SystemTime();
 * metricName = new MetricName(&quot;message-size-max&quot;, &quot;producer-metrics&quot;);
 *
 * KafkaMetric m = new KafkaMetric(new Object(),
 *                                 Objects.requireNonNull(metricName),
 *                                 (Gauge<Integer>) (config, now) -> 1,
 *                                 config,
 *                                 time);
 * }</pre>
 */
public final class KafkaMetric implements Metric {

    private final MetricName metricName;
    private final Object lock;
    private final Time time;
    private final MetricValueProvider<?> metricValueProvider;
    private MetricConfig config;

    // public for testing
    public KafkaMetric(Object lock, MetricName metricName, MetricValueProvider<?> valueProvider,
            MetricConfig config, Time time) {
        this.metricName = metricName;
        this.lock = lock;
        if (!(valueProvider instanceof Measurable) && !(valueProvider instanceof Gauge))
            throw new IllegalArgumentException("Unsupported metric value provider of class " + valueProvider.getClass());
        this.metricValueProvider = valueProvider;
        this.config = config;
        this.time = time;
    }

    public MetricConfig config() {
        return this.config;
    }

    @Override
    public MetricName metricName() {
        return this.metricName;
    }

    @Override
    public Object metricValue() {
        long now = time.milliseconds();
        synchronized (this.lock) {
            if (this.metricValueProvider instanceof Measurable)
                return ((Measurable) metricValueProvider).measure(config, now);
            else if (this.metricValueProvider instanceof Gauge)
                return ((Gauge<?>) metricValueProvider).value(config, now);
            else
                throw new IllegalStateException("Not a valid metric: " + this.metricValueProvider.getClass());
        }
    }

    public Measurable measurable() {
        if (this.metricValueProvider instanceof Measurable)
            return (Measurable) metricValueProvider;
        else
            throw new IllegalStateException("Not a measurable: " + this.metricValueProvider.getClass());
    }

    double measurableValue(long timeMs) {
        synchronized (this.lock) {
            if (this.metricValueProvider instanceof Measurable)
                return ((Measurable) metricValueProvider).measure(config, timeMs);
            else
                return 0;
        }
    }

    public void config(MetricConfig config) {
        synchronized (lock) {
            this.config = config;
        }
    }
}
