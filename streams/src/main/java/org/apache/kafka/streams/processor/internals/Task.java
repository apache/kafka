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
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public interface Task {
    enum State {
        CREATED, RESTORING, RUNNING, SUSPENDED, CLOSED;

        static void validateTransition(final State oldState, final State newState) {
            if (oldState == CREATED && (newState == RESTORING || newState == CLOSED)) {
                return;
            } else if (oldState == RESTORING && (newState == RUNNING || newState == SUSPENDED || newState == CLOSED)) {
                return;
            } else if (oldState == RUNNING && (newState == SUSPENDED || newState == CLOSED)) {
                return;
            } else if (oldState == SUSPENDED && (newState == RUNNING || newState == CLOSED)) {
                return;
            } else {
                throw new IllegalStateException("Invalid transition from " + oldState + " to " + newState);
            }
        }
    }

    State state();

    void transitionTo(State newState);

    void initializeIfNeeded();

    void startRunning();

    default Map<TopicPartition, Long> purgableOffsets() {
        return Collections.emptyMap();
    }

    default boolean process(final long wallClockTime) {
        return false;
    }

    boolean commitNeeded();

    default boolean commitRequested() {
        return false;
    }

    void commit();

    void suspend();

    void resume();

    /**
     * Close a task that we still own. Commit all progress and close the task gracefully.
     * Throws an exception if this couldn't be done.
     */
    void closeClean();

    /**
     * Close a task that we may not own. Discard any uncommitted progress and close the task.
     * Never throws an exception, but just makes all attempts to release resources while closing.
     */
    void closeDirty();

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

    boolean isActive();

    String toString(final String indent);
}
