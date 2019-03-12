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

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * A handle to a connector executing in a Connect cluster.
 */
public class ConnectorHandle {

    private static final Logger log = LoggerFactory.getLogger(ConnectorHandle.class);

    private final String connectorName;
    private final Map<String, TaskHandle> taskHandles = new ConcurrentHashMap<>();

    private CountDownLatch recordsRemainingLatch;
    private CountDownLatch recordsToCommitLatch;
    private int expectedRecords = -1;
    private int expectedCommits = -1;

    public ConnectorHandle(String connectorName) {
        this.connectorName = connectorName;
    }

    /**
     * Get or create a task handle for a given task id. The task need not be created when this method is called. If the
     * handle is called before the task is created, the task will bind to the handle once it starts (or restarts).
     *
     * @param taskId the task id
     * @return a non-null {@link TaskHandle}
     */
    public TaskHandle taskHandle(String taskId) {
        return taskHandles.computeIfAbsent(taskId, k -> new TaskHandle(this, taskId));
    }

    /**
     * Get the connector's name corresponding to this handle.
     *
     * @return the connector's name
     */
    public String name() {
        return connectorName;
    }

    /**
     * Get the list of tasks handles monitored by this connector handle.
     *
     * @return the task handle list
     */
    public Collection<TaskHandle> tasks() {
        return taskHandles.values();
    }

    /**
     * Delete the task handle for this task id.
     *
     * @param taskId the task id.
     */
    public void deleteTask(String taskId) {
        log.info("Removing handle for {} task in connector {}", taskId, connectorName);
        taskHandles.remove(taskId);
    }

    /**
     * Set the number of expected records for this connector.
     *
     * @param expected number of records
     */
    public void expectedRecords(int expected) {
        expectedRecords = expected;
        recordsRemainingLatch = new CountDownLatch(expected);
    }

    /**
     * Set the number of expected commits performed by this connector.
     *
     * @param expected number of commits
     */
    public void expectedCommits(int expected) {
        expectedCommits = expected;
        recordsToCommitLatch = new CountDownLatch(expected);
    }

    /**
     * Record a message arrival at the connector.
     */
    public void record() {
        if (recordsRemainingLatch != null) {
            recordsRemainingLatch.countDown();
        }
    }

    /**
     * Record arrival of a batch of messages at the connector.
     *
     * @param batchSize the number of messages
     */
    public void record(int batchSize) {
        if (recordsRemainingLatch != null) {
            IntStream.range(0, batchSize).forEach(i -> recordsRemainingLatch.countDown());
        }
    }

    /**
     * Record a message commit from the connector.
     */
    public void commit() {
        if (recordsToCommitLatch != null) {
            recordsToCommitLatch.countDown();
        }
    }

    /**
     * Record commit on a batch of messages from the connector.
     *
     * @param batchSize the number of messages
     */
    public void commit(int batchSize) {
        if (recordsToCommitLatch != null) {
            IntStream.range(0, batchSize).forEach(i -> recordsToCommitLatch.countDown());
        }
    }

    /**
     * Wait for this connector to meet the expected number of records as defined by {@code
     * expectedRecords}.
     *
     * @param timeout max duration to wait for records
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(int timeout) throws InterruptedException {
        if (recordsRemainingLatch == null || expectedRecords < 0) {
            throw new IllegalStateException("expectedRecords() was not set for this connector?");
        }
        if (!recordsRemainingLatch.await(timeout, TimeUnit.MILLISECONDS)) {
            String msg = String.format(
                    "Insufficient records seen by connector %s in %d millis. Records expected=%d, actual=%d",
                    connectorName,
                    timeout,
                    expectedRecords,
                    expectedRecords - recordsRemainingLatch.getCount());
            throw new DataException(msg);
        }
    }

     /**
     * Wait for this connector to meet the expected number of commits as defined by {@code
     * expectedCommits}.
     *
     * @param  timeout duration to wait for commits
     * @throws InterruptedException if another threads interrupts this one while waiting for commits
     */
    public void awaitCommits(int timeout) throws InterruptedException {
        if (recordsToCommitLatch == null || expectedCommits < 0) {
            throw new IllegalStateException("expectedCommits() was not set for this connector?");
        }
        if (!recordsToCommitLatch.await(timeout, TimeUnit.MILLISECONDS)) {
            String msg = String.format(
                    "Insufficient records committed by connector %s in %d millis. Records expected=%d, actual=%d",
                    connectorName,
                    timeout,
                    expectedCommits,
                    expectedCommits - recordsToCommitLatch.getCount());
            throw new DataException(msg);
        }
    }

    @Override
    public String toString() {
        return "ConnectorHandle{" +
                "connectorName='" + connectorName + '\'' +
                '}';
    }
}
