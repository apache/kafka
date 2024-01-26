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

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRICS_SUFFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.FETCH_MANAGER_METRICS_SUFFIX;

/**
 * Base class for different consumer metrics to extend. This class helps to construct the logical group name from the
 * given prefix and suffix, and provides a few common utilities.
 *
 * <p>
 * Prefix can be provided by the user and is "consumer-" by default (see {@link ConsumerUtils#CONSUMER_METRIC_GROUP_PREFIX}).
 * </p>
 *
 * <p>
 * The suffix can be one of the following:
 * <ul>
 *     <li><code>-coordinator-metrics</code>: {@link MetricGroupSuffix#COORDINATOR}</li>
 *     <li><code>-metrics</code>: {@link MetricGroupSuffix#CONSUMER}</li>
 *     <li><code>-fetch-manager-metrics</code>: {@link MetricGroupSuffix#FETCH_MANAGER}</li>
 * </ul>
 * </p>
 */
public abstract class AbstractConsumerMetrics {
    protected final String metricGroupName;

    public enum MetricGroupSuffix {
        COORDINATOR(COORDINATOR_METRICS_SUFFIX),
        CONSUMER(CONSUMER_METRICS_SUFFIX),
        FETCH_MANAGER(FETCH_MANAGER_METRICS_SUFFIX);

        private final String suffix;

        MetricGroupSuffix(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public String toString() {
            return suffix;
        }
    }

    public AbstractConsumerMetrics(MetricGroupSuffix suffix) {
        this(CONSUMER_METRIC_GROUP_PREFIX, suffix.toString());
    }

    public AbstractConsumerMetrics(String prefix, String suffix) {
        if (suffix == null)
            throw new IllegalArgumentException("metric group suffix cannot be null");
        if (prefix == null)
            throw new IllegalArgumentException("metric group prefix cannot be null");
        metricGroupName = prefix + suffix;
    }

    public String metricGroupName() {
        return metricGroupName;
    }

    public Meter createMeter(Metrics metrics, String baseName, String descriptiveName) {
        return new Meter(new WindowedCount(),
            metrics.metricName(baseName + "-rate",
                metricGroupName,
                String.format("The number of %s per second", descriptiveName)),
            metrics.metricName(baseName + "-total",
                metricGroupName,
                String.format("The total number of %s", descriptiveName)));
    }

    public void addMetric(Metrics metrics,
                          String metricName,
                          String description,
                          Measurable measurable) {
        metrics.addMetric(
            metrics.metricName(metricName, metricGroupName, description),
            measurable);
    }
}

