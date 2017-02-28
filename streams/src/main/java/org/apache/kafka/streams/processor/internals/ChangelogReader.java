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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * Performs bulk read operations from a set of partitions. Used to
 * restore  {@link org.apache.kafka.streams.processor.StateStore}s from their
 * change logs
 */
public interface ChangelogReader {
    /**
     * Validate that the partition exists on the cluster.
     * @param topicPartition    partition to validate.
     * @param storeName         name of the store the partition is for.
     * @throws org.apache.kafka.streams.errors.StreamsException if partition doesn't exist
     */
    void validatePartitionExists(final TopicPartition topicPartition, final String storeName);

    /**
     * Register a state store and it's partition for later restoration.
     * @param restorationInfo
     */
    void register(final StateRestorer restorationInfo);

    /**
     * Restore all registered state stores by reading from their changelogs.
     */
    void restore();

    /**
     * @return the restored offsets for all persistent stores.
     */
    Map<TopicPartition, Long> restoredOffsets();
}
