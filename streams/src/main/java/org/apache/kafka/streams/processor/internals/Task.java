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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public interface Task {

    // this must be negative to distinguish a running active task from other kinds of tasks
    // which may be caught up to the same offsets
    long LATEST_OFFSET = -2L;

    /*
     * <pre>
     *                 +-------------+
     *          +<---- | Created (0) | <----------------------+
     *          |      +-----+-------+                        |
     *          |            |                                |
     *          |            v                                |
     *          |      +-----+-------+                        |
     *          +<---- | Restoring(1)|<---------------+       |
     *          |      +-----+-------+                |       |
     *          |            |                        |       |
     *          |            +--------------------+   |       |
     *          |            |                    |   |       |
     *          |            v                    v   |       |
     *          |      +-----+-------+       +----+---+----+  |
     *          |      | Running (2) | ----> | Suspended(3)|  |    //TODO Suspended(3) could be removed after we've stable on KIP-429
     *          |      +-----+-------+       +------+------+  |
     *          |            |                      |         |
     *          |            |                      |         |
     *          |            v                      |         |
     *          |      +-----+-------+ <------------+         |
     *          +----> | Closed (4)  |                        |
     *                 +-------------+ <----------------------+
     * </pre>
     */
    enum State {
        CREATED(1, 4),         // 0
        RESTORING(2, 3, 4),    // 1
        RUNNING(3, 4),         // 2
        SUSPENDED(1, 4),       // 3
        CLOSED(0);             // 4, we allow CLOSED to transit to CREATED to handle corrupted tasks

        private final Set<Integer> validTransitions = new HashSet<>();

        State(final Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isValidTransition(final State newState) {
            return validTransitions.contains(newState.ordinal());
        }
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

    TaskId id();

    State state();

    boolean isActive();

    boolean isClosed();

    /**
     * @throws LockException could happen when multi-threads within the single instance, could retry
     * @throws StreamsException fatal error, should close the thread
     */
    void initializeIfNeeded();

    /**
     * @throws StreamsException fatal error, should close the thread
     */
    void completeRestoration();

    void addRecords(TopicPartition partition, Iterable<ConsumerRecord<byte[], byte[]>> records);

    boolean commitNeeded();

    /**
     * @throws StreamsException fatal error, should close the thread
     */
    void prepareCommit();

    void postCommit();

    /**
     * @throws TaskMigratedException all the task has been migrated
     * @throws StreamsException fatal error, should close the thread
     */
    void prepareSuspend();

    void suspend();
    /**
     *
     * @throws StreamsException fatal error, should close the thread
     */
    void resume();

    /**
     * Prepare to close a task that we still own and prepare it for committing
     * Throws an exception if this couldn't be done.
     * Must be idempotent.
     *
     * @throws StreamsException fatal error, should close the thread
     */
    void prepareCloseClean();

    /**
     * Must be idempotent.
     */
    void closeClean();

    /**
     * Prepare to close a task that we may not own. Discard any uncommitted progress and close the task.
     * Never throws an exception, but just makes all attempts to release resources while closing.
     * Must be idempotent.
     */
    void prepareCloseDirty();

    /**
     * Must be idempotent.
     */
    void closeDirty();

    /**
     * Updates input partitions and topology after rebalance
     */
    void update(final Set<TopicPartition> topicPartitions, final ProcessorTopology processorTopology);

    /**
     * Attempt a clean close but do not close the underlying state
     */
    void closeAndRecycleState();

    /**
     * Revive a closed task to a created one; should never throw an exception
     */
    void revive();

    StateStore getStore(final String name);

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

    void markChangelogAsCorrupted(final Collection<TopicPartition> partitions);

    default Map<TopicPartition, Long> purgeableOffsets() {
        return Collections.emptyMap();
    }

    default Map<TopicPartition, OffsetAndMetadata> committableOffsetsAndMetadata() {
        return Collections.emptyMap();
    }

    default void recordProcessBatchTime(final long processBatchTime) {}

    default void recordProcessTimeRatioAndBufferSize(final long allTaskProcessMs, final long now) {}

    default boolean process(final long wallClockTime) {
        return false;
    }

    default boolean commitRequested() {
        return false;
    }

    default boolean maybePunctuateStreamTime() {
        return false;
    }

    default boolean maybePunctuateSystemTime() {
        return false;
    }

}
