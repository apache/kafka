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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StreamsThreadDelegatingMetricsReporter implements MetricsReporter {


    private static final Logger LOG = LoggerFactory.getLogger(StreamsThreadDelegatingMetricsReporter.class);
    private Consumer<?, ?> consumer;
    final String threadId;
    final String stateUpdaterThreadId;
    private static final String THREAD_ID_TAG = "thread-id";


    public StreamsThreadDelegatingMetricsReporter(final Consumer<?, ?> consumer, final String threadId, final String stateUpdaterThreadId) {
        this.consumer = Objects.requireNonNull(consumer);
        this.threadId = Objects.requireNonNull(threadId);
        this.stateUpdaterThreadId = Objects.requireNonNull(stateUpdaterThreadId);
        LOG.info("Creating MetricsReporter for threadId {} and stateUpdaterId {}", threadId, stateUpdaterThreadId);
    }

    @Override
    public void init(final List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public void metricChange(final KafkaMetric metric) {
        if (tagMatchStreamOrStateUpdaterThreadId(metric)) {
            LOG.info("Registering metric {}", metric.metricName());
            consumer.registerMetric(metric);
        }
    }

    boolean tagMatchStreamOrStateUpdaterThreadId(final KafkaMetric metric) {
        final Map<String, String> tags = metric.metricName().tags();
        final boolean shouldInclude = tags.containsKey(THREAD_ID_TAG) && (tags.get(THREAD_ID_TAG).equals(threadId) || tags.get(THREAD_ID_TAG).equals(stateUpdaterThreadId));
        if (!shouldInclude) {
            LOG.warn("Rejecting metric {}", metric.metricName());
        }
        return shouldInclude;
    }

    @Override
    public void metricRemoval(final KafkaMetric metric) {
        if (tagMatchStreamOrStateUpdaterThreadId(metric)) {
            LOG.info("Unregistering metric {}", metric.metricName());
            consumer.unregisterMetric(metric);
        }
    }

    @Override
    public void close() {
        this.consumer = null;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // No op
    }
}
