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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A compound stat providing various metrics based on an internally maintained {@link HdrHistogram}.
 * It should be more performant, more precise, and much more flexible regarding concurrent usage
 * compared to either the most commonly used in the Kafka codebase Yammer histograms or the existing
 * Kafka Metrics alternatives like {@link Percentiles}.
 */
public final class KafkaMetricHistogram implements CompoundStat {

    /**
     * The number of significant digits used for the histogram.
     */
    public static final int NUM_SIG_FIGS = 3;

    /**
     * Max latency value.
     */
    public static final long MAX_LATENCY_MS = Duration.ofMinutes(1).toMillis();

    /**
     * Suffix used for the histogram's max value.
     */
    private static final String MAX_NAME = "max";

    /**
     * Set list of percentiles we will provide metrics for.
     */
    private static final Map<Double, String> PERCENTILE_NAMES =
        Utils.mkMap(
            Utils.mkEntry(50.0, "p50"),
            Utils.mkEntry(95.0, "p95"),
            Utils.mkEntry(99.0, "p99"),
            Utils.mkEntry(99.9, "p999")
        );

    /**
     * Factory for creating a {@link MetricName} based on a metric name with suffix.
     */
    private final Function<String, MetricName> metricNameFactory;
    private final HdrHistogram hdrHistogram;

    /**
     * Creates a new histogram with the purpose of tracking latency values. As such, the histogram
     * allows concurrent reads and writes and 3-digit precision across its range (higher precision significantly
     * increases the memory footprint of the histogram and is rarely needed).
     *
     * @param metricNameFactory A factory for creating a {@link MetricName} based on a metric name
     *                          string.
     * @return A new {@link KafkaMetricHistogram} with the configured properties.
     */
    public static KafkaMetricHistogram newLatencyHistogram(
        Function<String, MetricName> metricNameFactory
    ) {
        return new KafkaMetricHistogram(
            metricNameFactory,
            MAX_LATENCY_MS,
            NUM_SIG_FIGS);
    }

    /**
     * Creates a new histogram with the purpose of tracking latency values. As such, the histogram
     * allows concurrent reads and writes and 3-digit precision across its range (higher precision significantly
     * increases the memory footprint of the histogram and is rarely needed).
     *
     * @param metricNameFactory              A factory for creating a {@link MetricName} based on a metric name
     *                                          string.
     * @param highestTrackableValue          The maximum trackable value for the histogram, e.g. something
     *                                          with some safety margin above the configured request timeout for
     *                                          request latencies.
     * @param numberOfSignificantValueDigits The number of significant digits used.
     */
    private KafkaMetricHistogram(
        Function<String, MetricName> metricNameFactory,
        long highestTrackableValue,
        int numberOfSignificantValueDigits
    ) {
        this.metricNameFactory = metricNameFactory;
        this.hdrHistogram = new HdrHistogram(highestTrackableValue, numberOfSignificantValueDigits);
    }

    @Override
    public List<NamedMeasurable> stats() {
        List<NamedMeasurable> stats = new ArrayList<>();
        stats.add(new NamedMeasurable(
            metricNameFactory.apply(MAX_NAME),
            (config, now) -> hdrHistogram.max(now)));
        PERCENTILE_NAMES
            .entrySet()
            .stream()
            .map(e -> new NamedMeasurable(
                metricNameFactory.apply(e.getValue()),
                (config, now) -> hdrHistogram.measurePercentile(now, e.getKey())))
            .forEachOrdered(stats::add);
        return stats;
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        hdrHistogram.record((long) value);
    }
}
