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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A handle to an executing task in a worker. Use this class to record progress, for example: number of records seen
 * by the task using so far, or waiting for partitions to be assigned to the task.
 */
public class TaskHandle {

    private static final Logger log = LoggerFactory.getLogger(TaskHandle.class);

    private final String taskId;
    private final ConnectorHandle connectorHandle;
    private final AtomicInteger partitionsAssigned = new AtomicInteger(0);
    private final StartAndStopCounter startAndStopCounter = new StartAndStopCounter();
    private final RecordLatches recordLatches;

    public TaskHandle(ConnectorHandle connectorHandle, String taskId) {
        log.info("Created task {} for connector {}", taskId, connectorHandle);
        this.taskId = taskId;
        this.connectorHandle = connectorHandle;
        this.recordLatches = new RecordLatches("Task " + taskId);
    }

    /**
     * Record a message arrival at the task and the connector overall.
     */
    public void record() {
        recordLatches.record();
        connectorHandle.record();
    }

    /**
     * Record arrival of a batch of messages at the task and the connector overall.
     *
     * @param batchSize the number of messages
     */
    public void record(int batchSize) {
        recordLatches.record(batchSize);
        connectorHandle.record(batchSize);
    }

    /**
     * Record a message arrival at the task and the connector overall.
     *
     * @param topic the name of the topic
     */
    public void record(String topic) {
        recordLatches.record(topic);
        connectorHandle.record(topic);
    }

    /**
     * Record arrival of a batch of messages at the task and the connector overall.
     *
     * @param topic     the name of the topic
     * @param batchSize the number of messages
     */
    public void record(String topic, int batchSize) {
        recordLatches.record(topic, batchSize);
        connectorHandle.record(topic, batchSize);
    }

    /**
     * Record a message commit from the task and the connector overall.
     */
    public void commit() {
        recordLatches.commit();
        connectorHandle.commit();
    }

    /**
     * Record commit on a batch of messages from the task and the connector overall.
     *
     * @param batchSize the number of messages
     */
    public void commit(int batchSize) {
        recordLatches.commit(batchSize);
        connectorHandle.commit(batchSize);
    }

    /**
     * Set the number of expected records for this task.
     *
     * @param expected number of records
     */
    public void expectedRecords(int expected) {
        recordLatches.expectedRecords(expected);
    }

    /**
     * Set the number of expected records for this task.
     *
     * @param topic    the name of the topic onto which the records are expected
     * @param expected number of records
     */
    public void expectedRecords(String topic, int expected) {
        recordLatches.expectedRecords(topic, expected);
    }

    /**
     * Set the number of expected record commits performed by this task.
     *
     * @param expected number of commits
     */
    public void expectedCommits(int expected) {
        recordLatches.expectedCommits(expected);
    }

    /**
     * Set the number of partitions assigned to this task.
     *
     * @param numPartitions number of partitions
     */
    public void partitionsAssigned(int numPartitions) {
        partitionsAssigned.set(numPartitions);
    }

    /**
     * @return the number of topic partitions assigned to this task.
     */
    public int partitionsAssigned() {
        return partitionsAssigned.get();
    }

    /**
     * Wait up to the specified number of milliseconds for this task to meet the expected number of
     * records as defined by {@code expectedRecords}.
     *
     * @param timeoutMillis number of milliseconds to wait for records
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(long timeoutMillis) throws InterruptedException {
        recordLatches.awaitRecords(timeoutMillis);
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
        recordLatches.awaitRecords(timeout, unit);
    }

    /**
     * Wait for this connector to meet the expected number of records as defined by {@code
     * expectedRecords}.
     *
     * @param topic   the name of the topic
     * @param timeout max duration to wait for records
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(String topic, long timeout) throws InterruptedException {
        recordLatches.awaitRecords(topic, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Wait up to the specified timeout in milliseconds for this task to meet the expected number
     * of commits as defined by {@code expectedCommits}.
     *
     * @param timeoutMillis number of milliseconds to wait for commits
     * @throws InterruptedException if another threads interrupts this one while waiting for commits
     */
    public void awaitCommits(long timeoutMillis) throws InterruptedException {
        recordLatches.awaitCommits(timeoutMillis);
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
        recordLatches.awaitCommits(timeout, unit);
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
}
