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

import org.apache.kafka.common.TopicPartition;

/**
 * A {@link ClientMetricRecorder} that exposes methods to record the topic-level metrics.
 *
 * @see ProducerMetricRecorder for details on the producer-level metrics.
 */

public interface ProducerTopicMetricRecorder extends ClientMetricRecorder {

    String ACKS_LABEL = "acks";

    String PARTITION_LABEL = "partition";

    String REASON_LABEL = "reason";

    String TOPIC_LABEL = "topic";

    String PREFIX = ProducerMetricRecorder.PREFIX + "partition.";

    String RECORD_QUEUE_BYTES_NAME = PREFIX + "record.queue.bytes";

    String RECORD_QUEUE_BYTES_DESCRIPTION = "Number of bytes queued on partition queue.";

    String RECORD_QUEUE_COUNT_NAME = PREFIX + "record.queue.count";

    String RECORD_QUEUE_COUNT_DESCRIPTION = "Number of records queued on partition queue.";

    String RECORD_LATENCY_NAME = PREFIX + "record.latency";

    String RECORD_LATENCY_DESCRIPTION = "Total produce record latency, from application calling send()/produce() to ack received from broker.";

    String RECORD_QUEUE_LATENCY_NAME = PREFIX + "record.queue.latency";

    String RECORD_QUEUE_LATENCY_DESCRIPTION = "Time between send()/produce() and record being sent to broker.";

    String RECORD_RETRIES_NAME = PREFIX + "record.retries";

    String RECORD_RETRIES_DESCRIPTION = "Number of ProduceRequest retries.";

    String RECORD_FAILURES_NAME = PREFIX + "record.failures";

    String RECORD_FAILURES_DESCRIPTION = "Number of records that permanently failed delivery. Reason is a short string representation of the reason, which is typically the name of a Kafka protocol error code, e.g., “RequestTimedOut”.";

    String RECORD_SUCCESS_NAME = PREFIX + "record.success";

    String RECORD_SUCCESS_DESCRIPTION = "Number of records that have been successfully produced.";

    void incrementRecordQueueBytes(TopicPartition topicPartition, short acks, long amount);

    default void decrementRecordQueueBytes(TopicPartition topicPartition, short acks, long amount) {
        incrementRecordQueueBytes(topicPartition, acks, -amount);
    }

    void incrementRecordQueueCount(TopicPartition topicPartition, short acks, long amount);

    default void decrementRecordQueueCount(TopicPartition topicPartition, short acks, long amount) {
        incrementRecordQueueCount(topicPartition, acks, -amount);
    }

    void recordRecordLatency(TopicPartition topicPartition, short acks, long amount);

    void recordRecordQueueLatency(TopicPartition topicPartition, short acks, long amount);

    void addRecordRetries(TopicPartition topicPartition, short acks, long amount);

    void addRecordFailures(TopicPartition topicPartition, short acks, Throwable error, long amount);

    void addRecordSuccess(TopicPartition topicPartition, short acks, long amount);
}
