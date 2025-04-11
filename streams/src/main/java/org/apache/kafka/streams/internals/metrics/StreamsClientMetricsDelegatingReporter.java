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

package org.apache.kafka.streams.internals.metrics;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StreamsClientMetricsDelegatingReporter implements MetricsReporter {

    private static final Logger log = LoggerFactory.getLogger(StreamsClientMetricsDelegatingReporter.class);
    private final Admin adminClient;

    public StreamsClientMetricsDelegatingReporter(final Admin adminClient, final String streamsClientId) {
        this.adminClient = Objects.requireNonNull(adminClient);
        log.debug("Creating Client Metrics reporter for streams client {}", streamsClientId);
    }

    @Override
    public void init(final List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public void metricChange(final KafkaMetric metric) {
        if (isStreamsClientMetric(metric)) {
            log.debug("Registering metric {}", metric.metricName());
            adminClient.registerMetricForSubscription(metric);
        }
    }

    private boolean isStreamsClientMetric(final KafkaMetric metric) {
        final boolean shouldInclude = metric.metricName().group().equals("stream-metrics");
        if (!shouldInclude) {
            log.trace("Rejecting thread metric {}", metric.metricName());
        }
        return shouldInclude;
    }

    @Override
    public void metricRemoval(final KafkaMetric metric) {
        if (isStreamsClientMetric(metric)) {
            log.debug("Unregistering metric {}", metric.metricName());
            adminClient.unregisterMetricFromSubscription(metric);
        }
    }

    @Override
    public void close() {
        // No op
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // No op
    }
}
