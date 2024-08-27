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
import java.util.Optional;

public class StreamsDelegatingMetricsReporter implements MetricsReporter {


    private static final Logger LOG = LoggerFactory.getLogger(org.apache.kafka.streams.internals.metrics.StreamsDelegatingMetricsReporter.class);
    private Consumer<?, ?> consumer;
    final String threadId;
    private static final String THREAD_ID_TAG = "thread-id";


    public StreamsDelegatingMetricsReporter(final Consumer<?, ?> consumer, final String threadId) {
        this.consumer = consumer;
        this.threadId = threadId;
    }

    @Override
    public void init(final List<KafkaMetric> metrics) {

    }

    @Override
    public void metricChange(final KafkaMetric metric) {
        if (filteredMetric(metric).isPresent()) {
            LOG.info("Registering metric {} for thread={}", metric.metricName().name(), threadId);
            consumer.registerMetric(metric);
        }
    }

    Optional<KafkaMetric> filteredMetric(final KafkaMetric kafkaMetric) {
        final Map<String, String> tags = kafkaMetric.metricName().tags();
        KafkaMetric maybeKafkaMetric = null;
        if (tags.containsKey(THREAD_ID_TAG) && tags.get(THREAD_ID_TAG).equals(threadId)) {
            maybeKafkaMetric = kafkaMetric;
        }
        return Optional.ofNullable(maybeKafkaMetric);
    }

    @Override
    public void metricRemoval(final KafkaMetric metric) {
        if (filteredMetric(metric).isPresent()) {
            LOG.info("Unregistering metric {} for thread={}", metric.metricName().name(), threadId);
            consumer.unregisterMetric(metric);
        }
    }

    @Override
    public void close() {
        this.consumer = null;
    }

    @Override
    public void configure(final Map<String, ?> configs) {

    }
}
