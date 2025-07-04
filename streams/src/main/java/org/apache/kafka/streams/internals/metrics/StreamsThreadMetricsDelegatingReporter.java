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
import java.util.Optional;

public class StreamsThreadMetricsDelegatingReporter implements MetricsReporter {
    
    private static final Logger log = LoggerFactory.getLogger(StreamsThreadMetricsDelegatingReporter.class);
    private static final String THREAD_ID_TAG = "thread-id";
    private final Consumer<byte[], byte[]> consumer;
    private final String threadId;
    private final Optional<String> stateUpdaterThreadId;


    public StreamsThreadMetricsDelegatingReporter(final Consumer<byte[], byte[]> consumer, final String threadId, final Optional<String> stateUpdaterThreadId) {
        this.consumer = Objects.requireNonNull(consumer);
        this.threadId = Objects.requireNonNull(threadId);
        this.stateUpdaterThreadId = stateUpdaterThreadId;
        log.debug("Creating MetricsReporter for threadId {} and stateUpdaterId {}", threadId, stateUpdaterThreadId);
    }

    @Override
    public void init(final List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public void metricChange(final KafkaMetric metric) {
        if (tagMatchStreamOrStateUpdaterThreadId(metric)) {
            log.debug("Registering metric {}", metric.metricName());
            consumer.registerMetricForSubscription(metric);
        }
    }

    /*
       The StreamMetrics object is a singleton shared by all StreamThread instances.
       So we need to make sure we only pass metrics for the current StreamThread that contains this
       MetricsReporter instance, which will register metrics with the embedded KafkaConsumer to pass
       through the telemetry pipeline.
       Otherwise, Kafka Streams would register multiple metrics for all StreamThreads.
     */
    private boolean tagMatchStreamOrStateUpdaterThreadId(final KafkaMetric metric) {
        final Map<String, String> tags = metric.metricName().tags();
        final boolean shouldInclude = tags.containsKey(THREAD_ID_TAG) && (tags.get(THREAD_ID_TAG).equals(threadId) ||
                Optional.ofNullable(tags.get(THREAD_ID_TAG)).equals(stateUpdaterThreadId));
        if (!shouldInclude) {
            log.trace("Rejecting metric {}", metric.metricName());
        }
        return shouldInclude;
    }

    @Override
    public void metricRemoval(final KafkaMetric metric) {
        if (tagMatchStreamOrStateUpdaterThreadId(metric)) {
            log.debug("Unregistering metric {}", metric.metricName());
            consumer.unregisterMetricFromSubscription(metric);
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
