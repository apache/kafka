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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Task {
    void resume();

    void commit();

    void suspend();

    void close(boolean clean, boolean isZombie);

    TaskId id();

    String applicationId();

    Set<TopicPartition> partitions();

    ProcessorTopology topology();

    ProcessorContext context();

    StateStore getStore(String name);

    void closeSuspended(boolean clean, boolean isZombie, RuntimeException e);

    Map<TopicPartition, Long> checkpointedOffsets();

    boolean process();

    boolean commitNeeded();

    boolean maybePunctuateStreamTime();

    boolean maybePunctuateSystemTime();

    List<ConsumerRecord<byte[], byte[]>> update(TopicPartition partition, List<ConsumerRecord<byte[], byte[]>> remaining);

    String toString(String indent);

    int addRecords(TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records);

    boolean hasStateStores();

    /**
     * initialize the task and return true if the task is ready to run, i.e, it has not state stores
     * @return true if this task has no state stores that may need restoring.
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    boolean initialize();

    /**
     * @return any changelog partitions associated with this task
     */
    Collection<TopicPartition> changelogPartitions();
}
