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
package org.apache.kafka.common.telemetry.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Frequencies;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.SimpleRate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.telemetry.internals.LastValueTracker.InstantAndValue;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * All metrics implement the {@link MetricValueProvider} interface. They are divided into
 * two base types:
 *
 * <ol>
 *     <li>{@link Gauge}</li>
 *     <li>{@link Measurable}</li>
 * </ol>
 *
 * {@link Gauge Gauges} can have any value, but we only collect metrics with number values.
 * {@link Measurable Measurables} are divided into simple types with single values
 * ({@link Avg}, {@link CumulativeCount}, {@link Min}, {@link Max}, {@link Rate},
 * {@link SimpleRate}, and {@link CumulativeSum}) and compound types ({@link Frequencies},
 * {@link Meter}, and {@link Percentiles}).
 *
 * <p>
 *
 * We can safely assume that a {@link CumulativeCount count} always increases in steady state. It
 * should be a bug if a count metric decreases.
 *
 * <p>
 *
 * Total and Sum are treated as a monotonically increasing counter. The javadocs for Total metric type
 * say "An un-windowed cumulative total maintained over all time.". The standalone Total metrics in
 * the codebase seem to be cumulative metrics that will always increase. The Total metric underlying
 * Meter type is mostly a Total of a Count metric.
 * We can assume that a Total metric always increases (but it is not guaranteed as the sample values might be both
 * negative or positive).
 * For now, Total is converted to CUMULATIVE_DOUBLE unless we find a valid counter-example.
 *
 * <p>
 *
 * The Sum as it is a sample sum which is not a cumulative metric. It is converted to GAUGE_DOUBLE.
 *
 * <p>
 *
 * The compound metrics are virtual metrics. They are composed of simple types or anonymous measurable types
 * which are reported. A compound metric is never reported as-is.
 *
 * <p>
 *
 * A Meter metric is always created with and reported as 2 metrics: a rate and a count. For eg:
 * org.apache.kafka.common.network.Selector has Meter metric for "connection-close" but it has to be
 * created with a "connection-close-rate" metric of type rate and a "connection-close-total"
 * metric of type total.
 *
 * <p>
 *
 * Frequencies is created with an array of Frequency objects. When a Frequencies metric is registered, each
 * member Frequency object is converted into an anonymous Measurable and registered. So, a Frequencies metric
 * is reported with a set of measurables with name = Frequency.name(). As there is no way to figure out the
 * compound type, each component measurables is converted to a GAUGE_DOUBLE.
 *
 * <p>
 *
 * Percentiles work the same way as Frequencies. The only difference is that it is composed of Percentile
 * types instead. So, we should treat the component measurable as GAUGE_DOUBLE.
 *
 * <p>
 *
 * Some metrics are defined as either anonymous inner classes or lambdas implementing the Measurable
 * interface. As we do not have any information on how to treat them, we should fallback to treating
 * them as GAUGE_DOUBLE.
 *
 * <p>
 *
 * OpenTelemetry mapping for measurables:
 * Avg / Rate / Min / Max / Total / Sum -> Gauge
 * Count -> Sum
 * Meter has 2 elements :
 *  Total -> Sum
 *  Rate -> Gauge
 * Frequencies -> each component is Gauge
 * Percentiles -> each component is Gauge
 */
public class KafkaMetricsCollector implements MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(KafkaMetricsCollector.class);

    private final StateLedger ledger;
    private final Time time;
    private final MetricNamingStrategy<MetricName> metricNamingStrategy;
    private final Set<String> excludeLabels;

    public KafkaMetricsCollector(MetricNamingStrategy<MetricName> metricNamingStrategy, Set<String> excludeLabels) {
        this(metricNamingStrategy, Time.SYSTEM, excludeLabels);
    }

    // Visible for testing
    KafkaMetricsCollector(MetricNamingStrategy<MetricName> metricNamingStrategy, Time time, Set<String> excludeLabels) {
        this.metricNamingStrategy = metricNamingStrategy;
        this.time = time;
        this.ledger = new StateLedger();
        this.excludeLabels = excludeLabels;
    }

    public void init(List<KafkaMetric> metrics) {
        ledger.init(metrics);
    }

    /**
     * This is called whenever a metric is updated or added.
     */
    public void metricChange(KafkaMetric metric) {
        ledger.metricChange(metric);
    }

    /**
     * This is called whenever a metric is removed.
     */
    public void metricRemoval(KafkaMetric metric) {
        ledger.metricRemoval(metric);
    }

    /**
     * This is called whenever temporality changes, resets the value tracker for metrics.
     */
    public void metricsReset() {
        ledger.metricsStateReset();
    }

    // Visible for testing
    Set<MetricKey> getTrackedMetrics() {
        return ledger.metricMap.keySet();
    }

    @Override
    public void collect(MetricsEmitter metricsEmitter) {
        for (Map.Entry<MetricKey, KafkaMetric> entry : ledger.getMetrics()) {
            MetricKey metricKey = entry.getKey();
            KafkaMetric metric = entry.getValue();

            try {
                collectMetric(metricsEmitter, metricKey, metric);
            } catch (Exception e) {
                // catch and log to continue processing remaining metrics
                log.error("Error processing Kafka metric {}", metricKey, e);
            }
        }
    }

    protected void collectMetric(MetricsEmitter metricsEmitter, MetricKey metricKey, KafkaMetric metric) {
        Object metricValue;

        try {
            metricValue = metric.metricValue();
        } catch (Exception e) {
            // If an exception occurs when retrieving value, log warning and continue to process the rest of metrics
            log.warn("Failed to retrieve metric value {}", metricKey.name(), e);
            return;
        }

        Instant now = Instant.ofEpochMilli(time.milliseconds());
        if (metric.isMeasurable()) {
            Measurable measurable = metric.measurable();
            Double value = (Double) metricValue;

            if (measurable instanceof WindowedCount || measurable instanceof CumulativeSum) {
                collectSum(metricKey, value, metricsEmitter, now);
            } else {
                collectGauge(metricKey, value, metricsEmitter, now);
            }
        } else {
            // It is non-measurable Gauge metric.
            // Collect the metric only if its value is a number.
            if (metricValue instanceof Number) {
                Number value = (Number) metricValue;
                collectGauge(metricKey, value, metricsEmitter, now);
            } else {
                // skip non-measurable metrics
                log.debug("Skipping non-measurable gauge metric {}", metricKey.name());
            }
        }
    }

    private void collectSum(MetricKey metricKey, double value, MetricsEmitter metricsEmitter, Instant timestamp) {
        if (!metricsEmitter.shouldEmitMetric(metricKey)) {
            return;
        }

        if (metricsEmitter.shouldEmitDeltaMetrics()) {
            InstantAndValue<Double> instantAndValue = ledger.delta(metricKey, timestamp, value);

            metricsEmitter.emitMetric(
                SinglePointMetric.deltaSum(metricKey, instantAndValue.getValue(), true, timestamp,
                    instantAndValue.getIntervalStart(), excludeLabels)
            );
        } else {
            metricsEmitter.emitMetric(
                SinglePointMetric.sum(metricKey, value, true, timestamp, ledger.instantAdded(metricKey), excludeLabels)
            );
        }
    }

    private void collectGauge(MetricKey metricKey, Number value, MetricsEmitter metricsEmitter, Instant timestamp) {
        if (!metricsEmitter.shouldEmitMetric(metricKey)) {
            return;
        }

        metricsEmitter.emitMetric(
            SinglePointMetric.gauge(metricKey, value, timestamp, excludeLabels)
        );
    }

    /**
     * Keeps track of the state of metrics, e.g. when they were added, what their getAndSet value is,
     * and clearing them out when they're removed.
     */
    private class StateLedger {

        private final Map<MetricKey, KafkaMetric> metricMap = new ConcurrentHashMap<>();
        private final LastValueTracker<Double> doubleDeltas = new LastValueTracker<>();
        private final Map<MetricKey, Instant> metricAdded = new ConcurrentHashMap<>();

        private Instant instantAdded(MetricKey metricKey) {
            // lookup when the metric was added to use it as the interval start. That should always
            // exist, but if it doesn't (e.g. changed metrics temporality) then we use now.
            return metricAdded.computeIfAbsent(metricKey, x -> Instant.ofEpochMilli(time.milliseconds()));
        }

        private void init(List<KafkaMetric> metrics) {
            log.info("initializing Kafka metrics collector");
            for (KafkaMetric m : metrics) {
                metricMap.put(metricNamingStrategy.metricKey(m.metricName()), m);
            }
        }

        private void metricChange(KafkaMetric metric) {
            MetricKey metricKey = metricNamingStrategy.metricKey(metric.metricName());
            metricMap.put(metricKey, metric);
            if (doubleDeltas.contains(metricKey)) {
                log.warn("Registering a new metric {} which already has a last value tracked. " +
                    "Removing metric from delta register.", metric.metricName(), new Exception());

                /*
                 This scenario shouldn't occur while registering a metric since it should
                 have already been cleared out on cleanup/shutdown.
                 We remove the metric here to clear out the delta register because we are running
                 into an issue where old metrics are being re-registered which causes us to
                 record a negative delta
                */
                doubleDeltas.remove(metricKey);
            }
            metricAdded.put(metricKey, Instant.ofEpochMilli(time.milliseconds()));
        }

        private void metricRemoval(KafkaMetric metric) {
            log.debug("removing kafka metric : {}", metric.metricName());
            MetricKey metricKey = metricNamingStrategy.metricKey(metric.metricName());
            metricMap.remove(metricKey);
            doubleDeltas.remove(metricKey);
            metricAdded.remove(metricKey);
        }

        private Iterable<? extends Entry<MetricKey, KafkaMetric>> getMetrics() {
            return metricMap.entrySet();
        }

        private InstantAndValue<Double> delta(MetricKey metricKey, Instant now, Double value) {
            Optional<InstantAndValue<Double>> lastValue = doubleDeltas.getAndSet(metricKey, now, value);

            return lastValue
                .map(last -> new InstantAndValue<>(last.getIntervalStart(), value - last.getValue()))
                .orElse(new InstantAndValue<>(instantAdded(metricKey), value));
        }

        private void metricsStateReset() {
            metricAdded.clear();
            doubleDeltas.reset();
        }
    }

}
