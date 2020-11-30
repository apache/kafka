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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Task {

    // this must be negative to distinguish a running active task from other kinds of tasks
    // which may be caught up to the same offsets
    long LATEST_OFFSET = -2L;

    /*
     * <pre>
     *                 +-------------+
     *          +----- | Created (0) | <----------+
     *          |      +-----+-------+            |
     *          |            |                    |
     *          |            v                    |
     *          |      +-----+-------+            |
     *          +----- | Restoring(1)| <----+     |
     *          |      +-----+-------+      |     |
     *          |            |              |     |
     *          |            v              |     |
     *          |      +-----+-------+      |     |
     *          |      | Running (2) |      |     |
     *          |      +-----+-------+      |     |
     *          |            |              |     |
     *          |            v              |     |
     *          |     +------+--------+     |     |
     *          +---> | Suspended (3) | ----+     |    //TODO Suspended(3) could be removed after we've stable on KIP-429
     *                +------+--------+           |
     *                       |                    |
     *                       v                    |
     *                 +-----+-------+            |
     *                 | Closed (4)  | -----------+
     *                 +-------------+
     * </pre>
     */
    enum State {
        CREATED(1, 3),            // 0
        RESTORING(2, 3),          // 1
        RUNNING(3),               // 2
        SUSPENDED(1, 4),          // 3
        CLOSED(0);                // 4, we allow CLOSED to transit to CREATED to handle corrupted tasks

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

    default boolean needsInitializationOrRestoration() {
        return state() == State.CREATED || state() == State.RESTORING;
    }

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
    Map<TopicPartition, OffsetAndMetadata> prepareCommit();

    void postCommit(boolean enforceCheckpoint);

    void suspend();

    /**
     *
     * @throws StreamsException fatal error, should close the thread
     */
    void resume();

    /**
     * Must be idempotent.
     */
    void closeClean();

    /**
     * Must be idempotent.
     */
    void closeDirty();

    /**
     * Updates input partitions and topology after rebalance
     */
    void update(final Set<TopicPartition> topicPartitions, final Map<String, List<String>> allTopologyNodesToSourceTopics);

    /**
     * Attempt a clean close but do not close the underlying state
     */
    void closeCleanAndRecycleState();

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

    /**
     * @throws TimeoutException if {@code currentWallClockMs > task-timeout-deadline}
     */
    void maybeInitTaskTimeoutOrThrow(final long currentWallClockMs,
                                     final Exception cause);

    void clearTaskTimeout();
}
