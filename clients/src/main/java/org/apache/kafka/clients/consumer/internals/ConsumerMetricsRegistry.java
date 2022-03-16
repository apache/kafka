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
package org.apache.kafka.clients.consumer.internals;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.ClientTelemetryMetricsRegistry;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;

/**
 * Metrics corresponding to the consumer-level per
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-714:+Client+metrics+and+observability#KIP714:Clientmetricsandobservability-Clientinstance-levelConsumermetrics">KIP-714</a>.
 */
public class ConsumerMetricsRegistry extends ClientTelemetryMetricsRegistry {

    private final static String GROUP_NAME = "consumer-telemetry";

    private final static String PREFIX = "org.apache.kafka.client.consumer.";

    private final static String POLL_LAST_NAME = PREFIX + "poll.last";

    private final static String POLL_LAST_DESCRIPTION = "The number of seconds since the last poll() invocation.";

    private final static String COMMIT_COUNT_NAME = PREFIX + "commit.count";

    private final static String COMMIT_COUNT_DESCRIPTION = "Number of commit requests sent.";

    private final static String GROUP_ASSIGNMENT_PARTITION_COUNT_NAME = PREFIX + "group.assignment.partition.count";

    private final static String GROUP_ASSIGNMENT_PARTITION_COUNT_DESCRIPTION = "Number of currently assigned partitions to this consumer by the group leader.";

    private final static String ASSIGNMENT_PARTITION_COUNT_NAME = PREFIX + "assignment.partition.count";

    private final static String ASSIGNMENT_PARTITION_COUNT_DESCRIPTION = "Number of currently assigned partitions to this consumer, either through the group protocol or through assign().";

    private final static String GROUP_REBALANCE_COUNT_NAME = PREFIX + "group.error.count";

    private final static String GROUP_REBALANCE_COUNT_DESCRIPTION = "Number of group rebalances.";

    private final static String GROUP_ERROR_COUNT_NAME = PREFIX + "group.error.count";

    private final static String GROUP_ERROR_COUNT_DESCRIPTION = "Consumer group error counts. The error label depicts the actual error, e.g., \"MaxPollExceeded\", \"HeartbeatTimeout\", etc.";

    private final static String RECORD_QUEUE_COUNT_NAME = PREFIX + "record.queue.count";

    private final static String RECORD_QUEUE_COUNT_DESCRIPTION = "Number of records in consumer pre-fetch queue.";

    private final static String RECORD_QUEUE_BYTES_NAME = PREFIX + "record.queue.bytes";

    private final static String RECORD_QUEUE_BYTES_DESCRIPTION = "Amount of record memory in consumer pre-fetch queue. This may also include per-record overhead.";

    private final static String RECORD_APPLICATION_COUNT_NAME = PREFIX + "record.application.count";

    private final static String RECORD_APPLICATION_COUNT_DESCRIPTION = "Number of records consumed by application.";

    private final static String RECORD_APPLICATION_BYTES_NAME = PREFIX + "record.application.bytes";

    private final static String RECORD_APPLICATION_BYTES_DESCRIPTION = "Memory of records consumed by application.";

    private final static String REASON_LABEL = "reason";

    public final MetricName pollLast;

    public final MetricName commitCount;

    public final MetricName groupAssignmentPartitionCount;

    public final MetricName assignmentPartitionCount;

    public final MetricName groupRebalanceCount;

    private final MetricNameTemplate groupErrorCount;

    public final MetricName recordQueueCount;

    public final MetricName recordQueueBytes;

    public final MetricName recordApplicationCount;

    public final MetricName recordApplicationBytes;

    public ConsumerMetricsRegistry(Metrics metrics) {
        super(metrics);

        Set<String> reasonTags = appendTags(tags, REASON_LABEL);

        this.pollLast = createMetricName(POLL_LAST_NAME, POLL_LAST_DESCRIPTION);
        this.commitCount = createMetricName(COMMIT_COUNT_NAME, COMMIT_COUNT_DESCRIPTION);
        this.groupAssignmentPartitionCount = createMetricName(GROUP_ASSIGNMENT_PARTITION_COUNT_NAME, GROUP_ASSIGNMENT_PARTITION_COUNT_DESCRIPTION);
        this.assignmentPartitionCount = createMetricName(ASSIGNMENT_PARTITION_COUNT_NAME, ASSIGNMENT_PARTITION_COUNT_DESCRIPTION);
        this.groupRebalanceCount = createMetricName(GROUP_REBALANCE_COUNT_NAME, GROUP_REBALANCE_COUNT_DESCRIPTION);
        this.groupErrorCount = createMetricNameTemplate(GROUP_ERROR_COUNT_NAME, GROUP_ERROR_COUNT_DESCRIPTION, reasonTags);
        this.recordQueueCount = createMetricName(RECORD_QUEUE_COUNT_NAME, RECORD_QUEUE_COUNT_DESCRIPTION);
        this.recordQueueBytes = createMetricName(RECORD_QUEUE_BYTES_NAME, RECORD_QUEUE_BYTES_DESCRIPTION);
        this.recordApplicationCount = createMetricName(RECORD_APPLICATION_COUNT_NAME, RECORD_APPLICATION_COUNT_DESCRIPTION);
        this.recordApplicationBytes = createMetricName(RECORD_APPLICATION_BYTES_NAME, RECORD_APPLICATION_BYTES_DESCRIPTION);
    }

    private MetricName createMetricName(String name, String description) {
        return createMetricName(name, GROUP_NAME, description);
    }

    private MetricNameTemplate createMetricNameTemplate(String name, String description, Set<String> tags) {
        return new MetricNameTemplate(name, GROUP_NAME, description, tags);
    }

    public MetricName groupErrorCount(String reason) {
        Map<String, String> metricsTags = Collections.singletonMap(REASON_LABEL, reason);
        return this.metrics.metricInstance(groupErrorCount, metricsTags);
    }

}
