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

import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import java.util.Optional;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;

public abstract class AbstractConsumerMetrics {
    protected String groupMetricsName = CONSUMER_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX;

    public AbstractConsumerMetrics() {}
    public AbstractConsumerMetrics(Optional<String> grpMetricsPrefix) {
        grpMetricsPrefix.ifPresent(s -> this.groupMetricsName = s + COORDINATOR_METRICS_SUFFIX);
    }

    public static Meter createMeter(Metrics metrics, String groupName, String baseName, String descriptiveName) {
        return new Meter(new WindowedCount(),
                metrics.metricName(baseName + "-rate", groupName,
                        String.format("The number of %s per second", descriptiveName)),
                metrics.metricName(baseName + "-total", groupName,
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
