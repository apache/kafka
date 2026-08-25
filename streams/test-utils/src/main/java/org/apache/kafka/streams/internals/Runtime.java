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
package org.apache.kafka.streams.internals;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorContext;

import java.time.Duration;
import java.util.Map;

/**
 * Runs the task(s) backing a {@link TopologyTestDriver}: accepts input records, drives them to
 * quiescence, and resolves state stores.
 *
 */
public interface Runtime {

    /**
     * Pipe a single record into the topology and drive processing until no more work is immediately
     * processable.
     */
    void pipeRecord(String topicName, long timestamp, byte[] key, byte[] value, Headers headers);

    /**
     * Advances the internally mocked wall-clock time by the given duration, then fires any
     * eligible wall-clock punctuators, commits, and drains any newly processable work.
     *
     * @param advance the amount of time to advance the wall-clock time by
     * @throws NullPointerException if {@code advance} is null
     */
    void handleWallClockTimeAdvance(final Duration advance);

    /**
     * Drain every currently processable record: re-enqueue produced records that loop back into a
     * source or internal topic, and process until nothing more is immediately processable.
     */
    void completeAllProcessableWork();

    /** Suspend, commit, and cleanly close the underlying task(s). Does not drain remaining work. */
    void suspendAndCloseTaskCleanly();

    /** @return whether the runtime still has records queued that it could not process. */
    boolean hasRecordsQueued();

    /**
     * @param name                  the store name
     * @param throwForBuiltInStores whether to throw if the store is a built-in store type that has a
     *                              dedicated typed accessor
     * @return the store, or {@code null} if no store with this name is registered
     */
    StateStore getStateStore(String name, boolean throwForBuiltInStores);

    /**
     * For test-only introspection
     * @return the processor context of the underlying task, or {@code null} if there is no task
     */
    ProcessorContext<?, ?> taskProcessorContext();

    /** Driver-level callbacks a runtime needs but does not own. */
    interface Host {

        /** Commit the given offsets (transactionally or via the mock consumer, per processing mode). */
        void commit(Map<TopicPartition, OffsetAndMetadata> offsets);

        /** Feed a record produced to a global-store source topic into the global state update task. */
        void processGlobalRecord(TopicPartition globalInputTopicPartition, long timestamp, byte[] key, byte[] value, Headers headers);

        /** @return the global-store source partition for {@code topic}, or {@code null} if none. */
        TopicPartition globalPartitionOrNull(String topic);

        /** Record that {@code record} was produced to {@code topic}, for later retrieval via {@code readRecord}. */
        void recordOutput(String topic, ProducerRecord<byte[], byte[]> record);
    }
}
