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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRICS_SUFFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.FETCH_MANAGER_METRICS_SUFFIX;

/**
 * This class encapsulates the creation of metrics for the kafka consumer. When constructing a metrics object, you
 * need to provide the suffix of the metric group.  These are:
 * <li>
 *     <ul>Group Coordinator: {@link MetricSuffix#COORDINATOR}</ul>
 *     <ul>Consumer metrics: {@link MetricSuffix#CONSUMER}</ul>
 *     <ul>Fetch manager metrics: {@link MetricSuffix#FETCH_MANAGER}</ul>
 * </li>
 *
 * If no prefix is provided, we will use the default prefix {@link ConsumerUtils#CONSUMER_METRIC_GROUP_PREFIX}.
 */
public abstract class AbstractConsumerMetrics {
    protected final String groupMetricsName;

    public enum MetricSuffix {
        COORDINATOR(COORDINATOR_METRICS_SUFFIX),
        CONSUMER(CONSUMER_METRICS_SUFFIX),
        FETCH_MANAGER(FETCH_MANAGER_METRICS_SUFFIX);

        private final String suffix;

        MetricSuffix(String suffix) {
            this.suffix = suffix;
        }

        public String getSuffix() {
            return suffix;
        }
    }

    public AbstractConsumerMetrics(MetricSuffix suffix) {
        this(Optional.empty(), suffix.getSuffix());
    }

    public AbstractConsumerMetrics(Optional<String> grpMetricsPrefix, String suffix) {
        Objects.requireNonNull(suffix, "suffix cannot be null");
        String prefix = grpMetricsPrefix.orElse(CONSUMER_METRIC_GROUP_PREFIX);
        groupMetricsName = prefix + suffix;
    }

    public String groupMetricsName() {
        return groupMetricsName;
    }

    public Meter createMeter(Metrics metrics, String baseName, String descriptiveName) {
        return new Meter(new WindowedCount(),
            metrics.metricName(baseName + "-rate",
                groupMetricsName,
                String.format("The number of %s per second", descriptiveName)),
            metrics.metricName(baseName + "-total",
                groupMetricsName,
                String.format("The total number of %s", descriptiveName)));
    }

    public void addMetric(Metrics metrics,
                          String name,
                          String description,
                          Measurable measurable) {
        metrics.addMetric(
            metrics.metricName(name, groupMetricsName, description),
            measurable);
    }
}

