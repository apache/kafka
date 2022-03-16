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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.telemetry.metrics.MetricKey;
import org.apache.kafka.common.telemetry.metrics.MetricNamingStrategy;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps track of the state of metrics, e.g. when they were added, what their getAndSet value is,
 * and clearing them out when they're removed.
 *
 * <p>Note that this class doesn't have a context object, so it can't use the real
 * MetricKey (with context.labels()). The StateLedger is created earlier in the process so
 * that it can handle the MetricsReporter methods (init/metricChange,metricRemoval).</p>
 */
public class StateLedger {

    private final static Logger log = LoggerFactory.getLogger(StateLedger.class);

    protected final Time time;

    protected final MetricNamingStrategy<MetricName> metricNamingStrategy;

    protected final Map<MetricKey, KafkaMetric> metricMap = new ConcurrentHashMap<>();

    protected final LastValueTracker<Double> doubleDeltas = new LastValueTracker<>();

    protected final ConcurrentMap<MetricKey, Instant> metricAdded = new ConcurrentHashMap<>();

    public StateLedger(Time time, MetricNamingStrategy<MetricName> metricNamingStrategy) {
        this.time = time;
        this.metricNamingStrategy = metricNamingStrategy;
    }

    private Instant instantAdded(MetricKey metricKey) {
        // lookup when the metric was added to use it as the interval start. That should always
        // exist, but if it doesn't (e.g. if there's a race) then we use now.
        return metricAdded.computeIfAbsent(metricKey, x -> Instant.ofEpochMilli(time.milliseconds()));
    }

    public void init(List<KafkaMetric> metrics) {
        log.debug("initializing Kafka metrics");
        for (KafkaMetric m : metrics) {
            metricMap.put(metricNamingStrategy.metricKey(m.metricName()), m);
        }
    }

    public void metricChange(KafkaMetric metric) {
        MetricKey metricKey = metricNamingStrategy.metricKey(metric.metricName());
        metricMap.put(metricKey, metric);

        if (doubleDeltas.contains(metricKey)) {
            log.warn("Registering a new metric {} which already has a last value tracked. " +
                "Removing metric from delta register.", metricKey, new Exception());

            // This scenario shouldn't occur while registering a metric since it should
            // have already been cleared out on cleanup/shutdown.
            // We remove the metric here to clear out the delta register because we are running
            // into an issue where old metrics are being re-registered which causes us to
            // record a negative delta
            doubleDeltas.remove(metricKey);
        }

        metricAdded.put(metricKey, Instant.ofEpochMilli(time.milliseconds()));
    }

    public void metricRemoval(KafkaMetric metric) {
        MetricKey metricKey = metricNamingStrategy.metricKey(metric.metricName());
        log.debug("removing kafka metric : {}", metricKey);
        metricMap.remove(metricKey);
        doubleDeltas.remove(metricKey);
        metricAdded.remove(metricKey);
    }

    public Iterable<? extends Entry<MetricKey, KafkaMetric>> getMetrics() {
        return metricMap.entrySet();
    }

    public InstantAndValue<Double> delta(MetricKey metricKey, Instant now, Double value) {
        Optional<InstantAndValue<Double>> lastValue = doubleDeltas.getAndSet(metricKey, now, value);

        return lastValue
            .map(last -> new InstantAndValue<>(last.intervalStart(), value - last.value()))
            .orElse(new InstantAndValue<>(instantAdded(metricKey), value));
    }
}
