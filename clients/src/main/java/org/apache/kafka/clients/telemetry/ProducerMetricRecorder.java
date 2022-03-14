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

/**
 * A {@link ClientMetricRecorder} that exposes methods to record the producer-level metrics.
 *
 * @see ProducerTopicMetricRecorder for details on the topic-level metrics.
 */
public interface ProducerMetricRecorder extends ClientMetricRecorder {

    String PREFIX = ClientMetricRecorder.PREFIX + "producer.";

    String RECORD_QUEUE_BYTES_NAME = PREFIX + "record.queue.bytes";

    String RECORD_QUEUE_BYTES_DESCRIPTION = "Current amount of memory used in producer record queues.";

    String RECORD_QUEUE_MAX_BYTES_NAME = PREFIX + "record.queue.max.bytes";

    String RECORD_QUEUE_MAX_BYTES_DESCRIPTION = "Total amount of queue/buffer memory allowed on the producer queue(s).";

    String RECORD_QUEUE_COUNT_NAME = PREFIX + "record.queue.count";

    String RECORD_QUEUE_COUNT_DESCRIPTION = "Current number of records on the producer queue(s).";

    String RECORD_QUEUE_MAX_COUNT_NAME = PREFIX + "record.queue.max.count";

    String RECORD_QUEUE_MAX_COUNT_DESCRIPTION = "Maximum amount of records allowed on the producer queue(s).";

    void incrementRecordQueueBytes(long amount);

    default void decrementRecordQueueBytes(long amount) {
        incrementRecordQueueBytes(-amount);
    }

    void incrementRecordQueueMaxBytes(long amount);

    default void decrementRecordQueueMaxBytes(long amount) {
        incrementRecordQueueMaxBytes(-amount);
    }

    void incrementRecordQueueCount(long amount);

    default void decrementRecordQueueCount(long amount) {
        incrementRecordQueueCount(-amount);
    }

    void incrementRecordQueueMaxCount(long amount);

    default void decrementRecordQueueMaxCount(long amount) {
        incrementRecordQueueMaxCount(-amount);
    }

}
