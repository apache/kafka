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
package org.apache.kafka.common.telemetry.collector;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.MetricsReporter;
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
import org.apache.kafka.common.telemetry.emitter.Emitter;
import org.apache.kafka.common.telemetry.metrics.MetricKey;
import org.apache.kafka.common.telemetry.metrics.MetricNamingStrategy;
import org.apache.kafka.common.telemetry.metrics.MetricType;
import org.apache.kafka.common.telemetry.metrics.Metric;
import org.apache.kafka.common.utils.Time;

/**
 * All metrics implement the {@link MetricValueProvider} interface. They are divided into
 * two base types:
 *
 * <ol>
 *     <li>{@link Gauge}</li>
 *     <li>{@link Measurable}</li>
 * </ol>
 *
 * {@link Gauge Gauges} can have any value but we only collect metrics with number values.
 * {@link Measurable Measurables} are divided into simple types with single values
 * ({@link Avg}, {@link CumulativeCount}, {@link Min}, {@link Max}, {@link Rate},
 * {@link SimpleRate}, and {@link CumulativeSum}) and compound types ({@link Frequencies},
 * {@link Meter}, and {@link Percentiles}).
 *
 * <p/>
 *
 * We can safely assume that a {@link CumulativeCount count} always increases in steady state. It
 * should be a bug if a count metric decreases.
 *
 * <p/>
 *
 * Should total and sum be treated as a monotonically increasing counter ?
 * The javadocs for Total metric type say "An un-windowed cumulative total maintained over all time.".
 * The standalone Total metrics in the KafkaExporter codebase seem to be cumulative metrics that will always increase.
 * The Total metric underlying Meter type is mostly a Total of a Count metric.
 * We can assume that a Total metric always increases (but it is not guaranteed as the sample values might be both
 * negative or positive).
 * For now, Total is converted to CUMULATIVE_DOUBLE unless we find a valid counter-example.
 *
 * <p/>
 *
 * The Sum as it is a sample sum which is not a cumulative metric. It is converted to GAUGE_DOUBLE.
 *
 * <p/>
 *
 * The compound metrics are virtual metrics. They are composed of simple types or anonymous measurable types
 * which are reported. A compound metric is never reported as-is.
 *
 * <p/>
 *
 * A Meter metric is always created with and reported as 2 KafkaExporter metrics: a rate and a
 * count. For eg: org.apache.kafka.common.network.Selector has Meter metric for "connection-close" but it
 * has to be created with a "connection-close-rate" metric of type rate and a "connection-close-total"
 * metric of type total. So, we will never get a KafkaExporter metric with type Meter.
 *
 * <p/>
 *
 * Frequencies is created with a array of Frequency objects. When a Frequencies metric is registered, each
 * member Frequency object is converted into an anonymous Measurable and registered. So, a Frequencies metric
 * is reported with a set of measurables with name = Frequency.name(). As there is no way to figure out the
 * compound type, each component measurables is converted to a GAUGE_DOUBLE.
 *
 * <p/>
 *
 * Percentiles work the same way as Frequencies. The only difference is that it is composed of Percentile
 * types instead. So, we should treat the component measurable as GAUGE_DOUBLE.
 *
 * <p/>
 *
 * Some metrics are defined as either anonymous inner classes or lambdas implementing the Measurable
 * interface. As we do not have any information on how to treat them, we should fallback to treating
 * them as GAUGE_DOUBLE.
 *
 * <p/>
 *
 * KafkaExporter -> OpenTelemetry mapping for measurables
 * Avg / Rate / Min / Max / Total / Sum -> Gauge
 * Count -> Sum
 * Meter has 2 elements :
 *  Total -> Sum
 *  Rate -> Gauge
 * Frequencies -> each component is Gauge
 * Percentiles -> each component is Gauge
 */
public class KafkaMetricsCollector extends AbstractMetricsCollector implements MetricsReporter {

    protected final StateLedger ledger;

    protected final MetricNamingStrategy<MetricName> metricNamingStrategy;

    protected final Predicate<KafkaMetric> metricFilter;

    private final static Field METRIC_VALUE_PROVIDER_FIELD;

    static {
        try {
            METRIC_VALUE_PROVIDER_FIELD = KafkaMetric.class.getDeclaredField("metricValueProvider");
            METRIC_VALUE_PROVIDER_FIELD.setAccessible(true);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    public KafkaMetricsCollector(MetricNamingStrategy<MetricName> metricNamingStrategy) {
        this(Time.SYSTEM, metricNamingStrategy);
    }

    public KafkaMetricsCollector(Time time, MetricNamingStrategy<MetricName> metricNamingStrategy) {
        this(time, metricNamingStrategy, m -> true);
    }

    public KafkaMetricsCollector(Time time,
        MetricNamingStrategy<MetricName> metricNamingStrategy,
        Predicate<KafkaMetric> metricFilter) {
        super(time);
        this.ledger = new StateLedger(time, metricNamingStrategy);
        this.metricNamingStrategy = metricNamingStrategy;
        this.metricFilter = metricFilter;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        List<KafkaMetric> tracked = metrics.stream().filter(metricFilter).collect(Collectors.toList());
        ledger.init(tracked);
    }

    /**
     * This is called whenever a metric is updated or added
     */
    @Override
    public void metricChange(KafkaMetric metric) {
        if (metricFilter.test(metric))
            ledger.metricChange(metric);
    }

    /**
     * This is called whenever a metric is removed
     */
    @Override
    public void metricRemoval(KafkaMetric metric) {
        if (metricFilter.test(metric))
            ledger.metricRemoval(metric);
    }

    @Override
    public void close() {
    }

    @Override
    public void collect(Emitter emitter) {
        for (Map.Entry<MetricKey, KafkaMetric> entry : ledger.getMetrics()) {
            MetricKey metricKey = entry.getKey();
            KafkaMetric metric = entry.getValue();

            try {
                collectMetric(emitter, metricKey, metric);
            } catch (Exception e) {
                // catch and log to continue processing remaining metrics
                log.error("Unexpected error processing Kafka metric {}", metric.metricName(), e);
            }
        }
    }

    protected void collectMetric(Emitter emitter, MetricKey metricKey, KafkaMetric metric) {
        Object metricValue;

        try {
            metricValue = metric.metricValue();
        } catch (Exception e) {
             // If exception happens when retrieve value, log warning and continue to process the rest of metrics
            log.warn("Failed to retrieve metric value {}", metricKey.name(), e);
            return;
        }

        if (isMeasurable(metric)) {
            Measurable measurable = metric.measurable();
            double value = (Double) metricValue;

            if (measurable instanceof WindowedCount || measurable instanceof CumulativeSum) {
                maybeEmitDouble(emitter, metricKey, MetricType.sum, value);
                maybeEmitDelta(emitter, metricKey, value);
            } else {
                maybeEmitDouble(emitter, metricKey, MetricType.sum, value);
            }
        } else if (metricValue instanceof Number) {
            // For non-measurable Gauge metrics, collect the metric only if its value is a number.
            Number value = (Number) metricValue;

            // Map integer types to long and any other number type to double.
            if (value instanceof Integer || value instanceof Long)
                maybeEmitLong(emitter, metricKey, MetricType.gauge, value::longValue);
            else
                maybeEmitDouble(emitter, metricKey, MetricType.gauge, value::doubleValue);
        } else {
            // skip non-measurable metrics
            log.debug("Skipping non-measurable gauge metric {}", metricKey.name());
        }
    }

    protected boolean maybeEmitDelta(Emitter emitter, MetricKey originalMetricKey, double value) {
        MetricKey metricKey = metricNamingStrategy.derivedMetricKey(originalMetricKey, "delta");

        if (!emitter.shouldEmitMetric(metricKey))
            return false;

        // calculate a getAndSet, and add to out if non-empty
        Instant timestamp = now();
        InstantAndValue<Double> instantAndValue = ledger.delta(originalMetricKey, timestamp, value);

        Metric metric = new Metric(metricKey,
            MetricType.sum,
            instantAndValue.value(),
            timestamp,
            instantAndValue.intervalStart(),
            true);
        return emitter.emitMetric(metric);
    }

    private static boolean isMeasurable(KafkaMetric metric) {
        // KafkaMetric does not expose the internal MetricValueProvider and throws an IllegalStateException exception
        // if .measurable() is called for a Gauge.
        // There are 2 ways to find the type of internal MetricValueProvider for a KafkaMetric - use reflection or
        // get the information based on whether or not a IllegalStateException exception is thrown.
        // We use reflection so that we can avoid the cost of generating the stack trace when it's
        // not a measurable.
        try {
            Object provider = METRIC_VALUE_PROVIDER_FIELD.get(metric);
            return provider instanceof Measurable;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

}
