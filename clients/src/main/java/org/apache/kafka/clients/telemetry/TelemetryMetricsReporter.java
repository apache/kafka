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
package org.apache.kafka.clients.telemetry;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>TelemetryMetricsReporter</code> serves to tie into the the {@link MetricsReporter} logic
 * by which our code can be notified of updates to the set of collected metrics.
 */
public class TelemetryMetricsReporter implements MetricsReporter {

    private static final Logger log = LoggerFactory.getLogger(TelemetryMetricsReporter.class);

    private final Map<MetricName, KafkaMetric> metricsMap = new HashMap<>();

    private final DeltaValueStore deltaValueStore;

    public TelemetryMetricsReporter(DeltaValueStore deltaValueStore) {
        this.deltaValueStore = deltaValueStore;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.trace("configure - configs: {}", configs);
    }

    @Override
    public void close() {
        log.trace("close");
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        log.trace("init - metrics: {}", metrics);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        MetricName metricName = metric.metricName();

        if (metricName.name().startsWith("org.apache.kafka")) {
            metricsMap.put(metricName, metric);
            log.trace("metricChange - metricName: {}", metricName);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        MetricName metricName = metric.metricName();

        if (metricName.name().startsWith("org.apache.kafka")) {
            metricsMap.remove(metricName);
            deltaValueStore.remove(metricName);
            log.trace("metricRemoval - metricName: {}", metricName);
        }
    }

    public Collection<KafkaMetric> current() {
        return Collections.unmodifiableCollection(metricsMap.values());
    }

}
