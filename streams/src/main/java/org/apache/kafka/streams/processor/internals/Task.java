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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.Set;

public interface Task {
    /**
     * Initialize the task and return {@code true} if the task is ready to run, i.e, it has not state stores
     * @return true if this task has no state stores that may need restoring.
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    boolean initializeStateStores();

    boolean commitNeeded();

    void initializeTopology();

    void commit();

    void suspend();

    void resume();

    void closeSuspended(final boolean clean,
                        final boolean isZombie,
                        final RuntimeException e);

    void close(final boolean clean,
               final boolean isZombie);

    StateStore getStore(final String name);

    String applicationId();

    ProcessorTopology topology();

    ProcessorContext context();

    TaskId id();

    Set<TopicPartition> partitions();

    /**
     * @return any changelog partitions associated with this task
     */
    Collection<TopicPartition> changelogPartitions();

    boolean hasStateStores();

    String toString(final String indent);

}
