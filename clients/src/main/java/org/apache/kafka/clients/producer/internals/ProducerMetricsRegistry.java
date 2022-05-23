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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ClientTelemetryMetricsRegistry;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;

/**
 * Metrics corresponding to the producer-level per
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-714:+Client+metrics+and+observability#KIP714:Clientmetricsandobservability-Clientinstance-levelProducermetrics">KIP-714</a>.
 */
public class ProducerMetricsRegistry extends ClientTelemetryMetricsRegistry {

    private final static String GROUP_NAME = "producer-telemetry";

    private final static String PREFIX = "org.apache.kafka.client.producer.";

    private final static String RECORD_QUEUE_BYTES_NAME = PREFIX + "record.queue.bytes";

    private final static String RECORD_QUEUE_BYTES_DESCRIPTION = "Current amount of memory used in producer record queues.";

    private final static String RECORD_QUEUE_MAX_BYTES_NAME = PREFIX + "record.queue.max.bytes";

    private final static String RECORD_QUEUE_MAX_BYTES_DESCRIPTION = "Total amount of queue/buffer memory allowed on the producer queue(s).";

    private final static String RECORD_QUEUE_COUNT_NAME = PREFIX + "record.queue.count";

    private final static String RECORD_QUEUE_COUNT_DESCRIPTION = "Current number of records on the producer queue(s).";

    private final static String RECORD_QUEUE_MAX_COUNT_NAME = PREFIX + "record.queue.max.count";

    private final static String RECORD_QUEUE_MAX_COUNT_DESCRIPTION = "Maximum amount of records allowed on the producer queue(s).";

    public final MetricName recordQueueBytes;

    public final MetricName recordQueueMaxBytes;

    public final MetricName recordQueueCount;

    public final MetricName recordQueueMaxCount;

    public ProducerMetricsRegistry(Metrics metrics) {
        super(metrics);

        this.recordQueueBytes = createMetricName(RECORD_QUEUE_BYTES_NAME, RECORD_QUEUE_BYTES_DESCRIPTION);
        this.recordQueueMaxBytes = createMetricName(RECORD_QUEUE_MAX_BYTES_NAME, RECORD_QUEUE_MAX_BYTES_DESCRIPTION);
        this.recordQueueCount = createMetricName(RECORD_QUEUE_COUNT_NAME, RECORD_QUEUE_COUNT_DESCRIPTION);
        this.recordQueueMaxCount = createMetricName(RECORD_QUEUE_MAX_COUNT_NAME, RECORD_QUEUE_MAX_COUNT_DESCRIPTION);
    }

    private MetricName createMetricName(String name, String description) {
        return createMetricName(name, GROUP_NAME, description);
    }

}
