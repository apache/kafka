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
package org.apache.kafka.connect.integration;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A handle to an executing task in a worker. Use this class to record progress, for example: number of records seen
 * by the task using so far, or waiting for partitions to be assigned to the task.
 */
public class TaskHandle {

    private static final Logger log = LoggerFactory.getLogger(TaskHandle.class);

    private final String taskId;
    private final ConnectorHandle connectorHandle;
    private final ConcurrentMap<TopicPartition, PartitionHistory> partitions = new ConcurrentHashMap<>();
    private final StartAndStopCounter startAndStopCounter = new StartAndStopCounter();
    private final Consumer<SinkRecord> consumer;

    private CountDownLatch recordsRemainingLatch;
    private CountDownLatch recordsToCommitLatch;
    private int expectedRecords = -1;
    private int expectedCommits = -1;

    public TaskHandle(ConnectorHandle connectorHandle, String taskId, Consumer<SinkRecord> consumer) {
        this.taskId = taskId;
        this.connectorHandle = connectorHandle;
        this.consumer = consumer;
    }

    public String taskId() {
        return taskId;
    }

    public void record() {
        record(null);
    }

    /**
     * Record a message arrival at the task and the connector overall.
     */
    public void record(SinkRecord record) {
        if (consumer != null && record != null) {
            consumer.accept(record);
        }
        if (recordsRemainingLatch != null) {
            recordsRemainingLatch.countDown();
        }
        connectorHandle.record();
    }

    /**
     * Record arrival of a batch of messages at the task and the connector overall.
     *
     * @param batchSize the number of messages
     */
    public void record(int batchSize) {
        if (recordsRemainingLatch != null) {
            IntStream.range(0, batchSize).forEach(i -> recordsRemainingLatch.countDown());
        }
        connectorHandle.record(batchSize);
    }

    /**
     * Record a message commit from the task and the connector overall.
     */
    public void commit() {
        if (recordsToCommitLatch != null) {
            recordsToCommitLatch.countDown();
        }
        connectorHandle.commit();
    }

    /**
     * Record commit on a batch of messages from the task and the connector overall.
     *
     * @param batchSize the number of messages
     */
    public void commit(int batchSize) {
        if (recordsToCommitLatch != null) {
            IntStream.range(0, batchSize).forEach(i -> recordsToCommitLatch.countDown());
        }
        connectorHandle.commit(batchSize);
    }

    /**
     * Set the number of expected records for this task.
     *
     * @param expected number of records
     */
    public void expectedRecords(int expected) {
        expectedRecords = expected;
        recordsRemainingLatch = new CountDownLatch(expected);
    }

    /**
     * Set the number of expected record commits performed by this task.
     *
     * @param expected number of commits
     */
    public void expectedCommits(int expected) {
        expectedRecords = expected;
        recordsToCommitLatch = new CountDownLatch(expected);
    }

    /**
     * Adds a set of partitions to the (sink) task's assignment
     *
     * @param partitions the newly-assigned partitions
     */
    public void partitionsAssigned(Collection<TopicPartition> partitions) {
        partitions.forEach(partition -> this.partitions.computeIfAbsent(partition, PartitionHistory::new).assigned());
    }

    /**
     * Removes a set of partitions to the (sink) task's assignment
     *
     * @param partitions the newly-revoked partitions
     */
    public void partitionsRevoked(Collection<TopicPartition> partitions) {
        partitions.forEach(partition -> this.partitions.computeIfAbsent(partition, PartitionHistory::new).revoked());
    }

    /**
     * Records offset commits for a (sink) task's partitions
     *
     * @param partitions the committed partitions
     */
    public void partitionsCommitted(Collection<TopicPartition> partitions) {
        partitions.forEach(partition -> this.partitions.computeIfAbsent(partition, PartitionHistory::new).committed());
    }

    /**
     * @return the complete set of partitions currently assigned to this (sink) task
     */
    public Collection<TopicPartition> assignment() {
        return partitions.values().stream()
                .filter(PartitionHistory::isAssigned)
                .map(PartitionHistory::topicPartition)
                .collect(Collectors.toSet());
    }

    /**
     * @return the number of topic partitions assigned to this (sink) task.
     */
    public int numPartitionsAssigned() {
        return assignment().size();
    }

    /**
     * Returns the number of times the partition has been assigned to this (sink) task.
     * @param partition the partition
     * @return the number of times it has been assigned; may be 0 if never assigned
     */
    public int timesAssigned(TopicPartition partition) {
        return partitions.computeIfAbsent(partition, PartitionHistory::new).timesAssigned();
    }

    /**
     * Returns the number of times the partition has been revoked from this (sink) task.
     * @param partition the partition
     * @return the number of times it has been revoked; may be 0 if never revoked
     */
    public int timesRevoked(TopicPartition partition) {
        return partitions.computeIfAbsent(partition, PartitionHistory::new).timesRevoked();
    }

    /**
     * Returns the number of times the framework has committed offsets for this partition
     * @param partition the partition
     * @return the number of times it has been committed; may be 0 if never committed
     */
    public int timesCommitted(TopicPartition partition) {
        return partitions.computeIfAbsent(partition, PartitionHistory::new).timesCommitted();
    }

    /**
     * Wait up to the specified number of milliseconds for this task to meet the expected number of
     * records as defined by {@code expectedRecords}.
     *
     * @param timeoutMillis number of milliseconds to wait for records
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(long timeoutMillis) throws InterruptedException {
        awaitRecords(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Wait up to the specified timeout for this task to meet the expected number of records as
     * defined by {@code expectedRecords}.
     *
     * @param timeout duration to wait for records
     * @param unit    the unit of duration; may not be null
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(long timeout, TimeUnit unit) throws InterruptedException {
        if (recordsRemainingLatch == null) {
            throw new IllegalStateException("Illegal state encountered. expectedRecords() was not set for this task?");
        }
        if (!recordsRemainingLatch.await(timeout, unit)) {
            String msg = String.format(
                    "Insufficient records seen by task %s in %d millis. Records expected=%d, actual=%d",
                    taskId,
                    unit.toMillis(timeout),
                    expectedRecords,
                    expectedRecords - recordsRemainingLatch.getCount());
            throw new DataException(msg);
        }
        log.debug("Task {} saw {} records, expected {} records",
                  taskId, expectedRecords - recordsRemainingLatch.getCount(), expectedRecords);
    }

    /**
     * Wait up to the specified timeout in milliseconds for this task to meet the expected number
     * of commits as defined by {@code expectedCommits}.
     *
     * @param timeoutMillis number of milliseconds to wait for commits
     * @throws InterruptedException if another threads interrupts this one while waiting for commits
     */
    public void awaitCommits(long timeoutMillis) throws InterruptedException {
        awaitCommits(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Wait up to the specified timeout for this task to meet the expected number of commits as
     * defined by {@code expectedCommits}.
     *
     * @param timeout duration to wait for commits
     * @param unit    the unit of duration; may not be null
     * @throws InterruptedException if another threads interrupts this one while waiting for commits
     */
    public void awaitCommits(long timeout, TimeUnit unit) throws InterruptedException {
        if (recordsToCommitLatch == null) {
            throw new IllegalStateException("Illegal state encountered. expectedRecords() was not set for this task?");
        }
        if (!recordsToCommitLatch.await(timeout, unit)) {
            String msg = String.format(
                    "Insufficient records seen by task %s in %d millis. Records expected=%d, actual=%d",
                    taskId,
                    unit.toMillis(timeout),
                    expectedCommits,
                    expectedCommits - recordsToCommitLatch.getCount());
            throw new DataException(msg);
        }
        log.debug("Task {} saw {} records, expected {} records",
                  taskId, expectedCommits - recordsToCommitLatch.getCount(), expectedCommits);
    }

    /**
     * Gets the start and stop counter corresponding to this handle.
     *
     * @return the start and stop counter
     */
    public StartAndStopCounter startAndStopCounter() {
        return startAndStopCounter;
    }

    /**
     * Record that this task has been stopped. This should be called by the task.
     */
    public void recordTaskStart() {
        startAndStopCounter.recordStart();
    }

    /**
     * Record that this task has been stopped. This should be called by the task.
     */
    public void recordTaskStop() {
        startAndStopCounter.recordStop();
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until this task has completed the
     * expected number of starts.
     *
     * @param expectedStarts    the expected number of starts
     * @return the latch; never null
     */
    public StartAndStopLatch expectedStarts(int expectedStarts) {
        return startAndStopCounter.expectedStarts(expectedStarts);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until this task has completed the
     * expected number of starts.
     *
     * @param expectedStops    the expected number of stops
     * @return the latch; never null
     */
    public StartAndStopLatch expectedStops(int expectedStops) {
        return startAndStopCounter.expectedStops(expectedStops);
    }

    @Override
    public String toString() {
        return "Handle{" +
                "taskId='" + taskId + '\'' +
                '}';
    }

    private static class PartitionHistory {
        private final TopicPartition topicPartition;
        private boolean assigned = false;
        private int timesAssigned = 0;
        private int timesRevoked = 0;
        private int timesCommitted = 0;

        public PartitionHistory(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
        }

        public void assigned() {
            timesAssigned++;
            assigned = true;
        }

        public void revoked() {
            timesRevoked++;
            assigned = false;
        }

        public void committed() {
            timesCommitted++;
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        public boolean isAssigned() {
            return assigned;
        }

        public int timesAssigned() {
            return timesAssigned;
        }

        public int timesRevoked() {
            return timesRevoked;
        }

        public int timesCommitted() {
            return timesCommitted;
        }
    }
}
