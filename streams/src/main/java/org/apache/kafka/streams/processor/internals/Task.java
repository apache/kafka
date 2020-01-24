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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public interface Task {

    long LATEST_OFFSET = -2L;

    enum State {
        CREATED {
            @Override
            public boolean hasBeenRunning() {
                return false;
            }
        },
        RESTORING {
            @Override
            public boolean hasBeenRunning() {
                return false;
            }
        },
        RUNNING {
            @Override
            public boolean hasBeenRunning() {
                return true;
            }
        },
        SUSPENDED {
            @Override
            public boolean hasBeenRunning() {
                return true;
            }
        },
        CLOSED {
            @Override
            public boolean hasBeenRunning() {
                return true;
            }
        },
        CLOSING {
            @Override
            public boolean hasBeenRunning() {
                return true;
            }
        };

        public abstract boolean hasBeenRunning();
    }

    enum TaskType {
        ACTIVE("ACTIVE"),

        STANDBY("STANDBY"),

        GLOBAL("GLOBAL");

        public final String name;

        TaskType(final String name) {
            this.name = name;
        }
    }

    State state();

    void initializeIfNeeded();

    void completeInitializationAfterRestore();

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

    default boolean maybePunctuateStreamTime() {
        return false;
    }

    default boolean maybePunctuateSystemTime() {
        return false;
    }

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

    TaskId id();

    Set<TopicPartition> inputPartitions();

    /**
     * @return any changelog partitions associated with this task
     */
    Collection<TopicPartition> changelogPartitions();

    /**
     * @return the offsets of all the changelog partitions associated with this task,
     *         indicating the current positions of the logged state stores of the task.
     */
    Map<TopicPartition, Long> changelogOffsets();

    boolean isActive();
}
