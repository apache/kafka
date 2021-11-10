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
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A handle to a connector executing in a Connect cluster.
 */
public class ConnectorHandle {

    private static final Logger log = LoggerFactory.getLogger(ConnectorHandle.class);

    private final String connectorName;
    private final Map<String, TaskHandle> taskHandles = new ConcurrentHashMap<>();
    private final StartAndStopCounter startAndStopCounter = new StartAndStopCounter();

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
        return taskHandle(taskId, null);
    }

    /**
     * Get or create a task handle for a given task id. The task need not be created when this method is called. If the
     * handle is called before the task is created, the task will bind to the handle once it starts (or restarts).
     *
     * @param taskId the task id
     * @param consumer A callback invoked when a sink task processes a record.
     * @return a non-null {@link TaskHandle}
     */
    public TaskHandle taskHandle(String taskId, Consumer<SinkRecord> consumer) {
        return taskHandles.computeIfAbsent(taskId, k -> new TaskHandle(this, taskId, consumer));
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
    public void awaitRecords(long timeout) throws InterruptedException {
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
    public void awaitCommits(long timeout) throws InterruptedException {
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

    /**
     * Record that this connector has been started. This should be called by the connector under
     * test.
     *
     * @see #expectedStarts(int)
     */
    public void recordConnectorStart() {
        startAndStopCounter.recordStart();
    }

    /**
     * Record that this connector has been stopped. This should be called by the connector under
     * test.
     *
     * @see #expectedStarts(int)
     */
    public void recordConnectorStop() {
        startAndStopCounter.recordStop();
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the connector using this handle
     * and all tasks using {@link TaskHandle} have completed the expected number of
     * starts, starting the counts at the time this method is called.
     *
     * <p>A test can call this method, specifying the number of times the connector and tasks
     * will each be stopped and started from that point (typically {@code expectedStarts(1)}).
     * The test should then change the connector or otherwise cause the connector to restart one or
     * more times, and then can call {@link StartAndStopLatch#await(long, TimeUnit)} to wait up to a
     * specified duration for the connector and all tasks to be started at least the specified
     * number of times.
     *
     * <p>This method does not track the number of times the connector and tasks are stopped, and
     * only tracks the number of times the connector and tasks are <em>started</em>.
     *
     * @param expectedStarts the minimum number of starts that are expected once this method is
     *                       called
     * @return the latch that can be used to wait for the starts to complete; never null
     */
    public StartAndStopLatch expectedStarts(int expectedStarts) {
        return expectedStarts(expectedStarts, true);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the connector using this handle
     * and optionally all tasks using {@link TaskHandle} have completed the expected number of
     * starts, starting the counts at the time this method is called.
     *
     * <p>A test can call this method, specifying the number of times the connector and tasks
     * will each be stopped and started from that point (typically {@code expectedStarts(1)}).
     * The test should then change the connector or otherwise cause the connector to restart one or
     * more times, and then can call {@link StartAndStopLatch#await(long, TimeUnit)} to wait up to a
     * specified duration for the connector and all tasks to be started at least the specified
     * number of times.
     *
     * <p>This method does not track the number of times the connector and tasks are stopped, and
     * only tracks the number of times the connector and tasks are <em>started</em>.
     *
     * @param expectedStarts the minimum number of starts that are expected once this method is
     *                       called
     * @param includeTasks  true if the latch should also wait for the tasks to be stopped the
     *                      specified minimum number of times
     * @return the latch that can be used to wait for the starts to complete; never null
     */
    public StartAndStopLatch expectedStarts(int expectedStarts, boolean includeTasks) {
        List<StartAndStopLatch> taskLatches = includeTasks
                ? taskHandles.values().stream()
                .map(task -> task.expectedStarts(expectedStarts))
                .collect(Collectors.toList())
                : Collections.emptyList();
        return startAndStopCounter.expectedStarts(expectedStarts, taskLatches);
    }

    public StartAndStopLatch expectedStarts(int expectedStarts, Map<String, Integer> expectedTasksStarts, boolean includeTasks) {
        List<StartAndStopLatch> taskLatches = includeTasks
                ? taskHandles.values().stream()
                .map(task -> task.expectedStarts(expectedTasksStarts.get(task.taskId())))
                .collect(Collectors.toList())
                : Collections.emptyList();
        return startAndStopCounter.expectedStarts(expectedStarts, taskLatches);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the connector using this handle
     * and optionally all tasks using {@link TaskHandle} have completed the minimum number of
     * stops, starting the counts at the time this method is called.
     *
     * <p>A test can call this method, specifying the number of times the connector and tasks
     * will each be stopped from that point (typically {@code expectedStops(1)}).
     * The test should then change the connector or otherwise cause the connector to stop (or
     * restart) one or more times, and then can call
     * {@link StartAndStopLatch#await(long, TimeUnit)} to wait up to a specified duration for the
     * connector and all tasks to be started at least the specified number of times.
     *
     * <p>This method does not track the number of times the connector and tasks are started, and
     * only tracks the number of times the connector and tasks are <em>stopped</em>.
     *
     * @param expectedStops the minimum number of starts that are expected once this method is
     *                      called
     * @return the latch that can be used to wait for the starts to complete; never null
     */
    public StartAndStopLatch expectedStops(int expectedStops) {
        return expectedStops(expectedStops, true);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the connector using this handle
     * and optionally all tasks using {@link TaskHandle} have completed the minimum number of
     * stops, starting the counts at the time this method is called.
     *
     * <p>A test can call this method, specifying the number of times the connector and tasks
     * will each be stopped from that point (typically {@code expectedStops(1)}).
     * The test should then change the connector or otherwise cause the connector to stop (or
     * restart) one or more times, and then can call
     * {@link StartAndStopLatch#await(long, TimeUnit)} to wait up to a specified duration for the
     * connector and all tasks to be started at least the specified number of times.
     *
     * <p>This method does not track the number of times the connector and tasks are started, and
     * only tracks the number of times the connector and tasks are <em>stopped</em>.
     *
     * @param expectedStops the minimum number of starts that are expected once this method is
     *                      called
     * @param includeTasks  true if the latch should also wait for the tasks to be stopped the
     *                      specified minimum number of times
     * @return the latch that can be used to wait for the starts to complete; never null
     */
    public StartAndStopLatch expectedStops(int expectedStops, boolean includeTasks) {
        List<StartAndStopLatch> taskLatches = includeTasks
                ? taskHandles.values().stream()
                .map(task -> task.expectedStops(expectedStops))
                .collect(Collectors.toList())
                : Collections.emptyList();
        return startAndStopCounter.expectedStops(expectedStops, taskLatches);
    }

    public StartAndStopLatch expectedStops(int expectedStops, Map<String, Integer> expectedTasksStops, boolean includeTasks) {
        List<StartAndStopLatch> taskLatches = includeTasks
                ? taskHandles.values().stream()
                .map(task -> task.expectedStops(expectedTasksStops.get(task.taskId())))
                .collect(Collectors.toList())
                : Collections.emptyList();
        return startAndStopCounter.expectedStops(expectedStops, taskLatches);
    }

    @Override
    public String toString() {
        return "ConnectorHandle{" +
                "connectorName='" + connectorName + '\'' +
                '}';
    }
}
